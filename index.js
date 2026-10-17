/**
 * ISS Daily Backup
 * ----------------
 * Runs on Cloud Scheduler (every 5 minutes). Each tick reads the admin's settings from
 * Firestore (_meta/backupSettings) and, if backup is enabled and today's backup at the
 * configured time hasn't happened yet, copies EVERY top-level Firestore collection into ONE
 * Cloud Storage object: ISS_Daily_Backup.json (overwritten each time; never a dated copy).
 *
 * Source data is only READ. The only Firestore documents written are the two bookkeeping
 * docs: _meta/backupStatus (written here) and _meta/backupSettings (written by the web app).
 */
const { onSchedule } = require('firebase-functions/v2/scheduler');
const logger = require('firebase-functions/logger');
const admin = require('firebase-admin');

const BUCKET = 'issreport.firebasestorage.app'; // same bucket as storageBucket in the web app
admin.initializeApp({ storageBucket: BUCKET });
const db = admin.firestore();
const { Timestamp, FieldValue } = admin.firestore;

const BACKUP_FILE = 'ISS_Daily_Backup.json';   // the ONE backup file
const SETTINGS_REF = db.doc('_meta/backupSettings');
const STATUS_REF = db.doc('_meta/backupStatus');
const BOOKKEEPING_DOCS = { _meta: ['backupSettings', 'backupStatus'] }; // not part of the backup
const DEFAULT_TZ = 'Asia/Manila';
const DEFAULT_TIME = '02:00';
const MAX_ATTEMPTS_PER_DAY = 5;                // scheduled attempts, incl. retries after a failure
const RETRY_AFTER_MS = 15 * 60 * 1000;         // wait before retrying a failed attempt
const LOCK_MS = 15 * 60 * 1000;                // a crashed run's lock expires after this

/* ---------- time helpers ---------- */
function localParts(date, tz) {
  const p = {};
  new Intl.DateTimeFormat('en-CA', {
    timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hour12: false,
  }).formatToParts(date).forEach(x => { p[x.type] = x.value; });
  return { date: `${p.year}-${p.month}-${p.day}`, minutes: (Number(p.hour) % 24) * 60 + Number(p.minute) };
}

/* ---------- Firestore -> JSON ---------- */
function serialize(v) {
  if (v === null || v === undefined) return null;
  if (v instanceof Timestamp) return { __type: 'timestamp', iso: v.toDate().toISOString() };
  if (v instanceof admin.firestore.GeoPoint) return { __type: 'geopoint', latitude: v.latitude, longitude: v.longitude };
  if (v instanceof admin.firestore.DocumentReference) return { __type: 'reference', path: v.path };
  if (Buffer.isBuffer(v)) return { __type: 'bytes', base64: v.toString('base64') };
  if (Array.isArray(v)) return v.map(serialize);
  if (typeof v === 'object') {
    const out = {};
    Object.keys(v).forEach(k => { out[k] = serialize(v[k]); });
    return out;
  }
  return v;
}

async function buildBackup(tz) {
  const collections = {};
  const counts = {};
  let totalDocuments = 0;
  const refs = await db.listCollections();      // discovers EVERY top-level collection
  for (const col of refs) {
    const snap = await col.get();               // read-only
    const skip = BOOKKEEPING_DOCS[col.id] || [];
    const docs = {};
    snap.forEach(d => { if (!skip.includes(d.id)) docs[d.id] = serialize(d.data()); });
    collections[col.id] = docs;
    counts[col.id] = Object.keys(docs).length;
    totalDocuments += counts[col.id];
  }
  return {
    backupInfo: {
      file: BACKUP_FILE,
      createdAt: new Date().toISOString(),
      projectId: process.env.GCLOUD_PROJECT || null,
      timezone: tz,
      totalCollections: refs.length,
      totalDocuments,
      collectionCounts: counts,
    },
    collections,
  };
}

/* ---------- one backup run ---------- */
async function claim(reason, today, settings) {
  const now = Timestamp.now();
  return db.runTransaction(async tx => {
    const st = (await tx.get(STATUS_REF)).data() || {};
    if (st.runningSince && now.toMillis() - st.runningSince.toMillis() < LOCK_MS) return false; // another run active
    const sameDay = st.lastAttemptDate === today;
    const update = { runningSince: now, status: 'Running', lastTrigger: reason };
    if (reason === 'scheduled') {
      update.lastAttemptAt = now;
      update.lastAttemptDate = today;
      update.attemptsToday = (sameDay ? (st.attemptsToday || 0) : 0) + 1;
    } else if (settings.runNowRequestedAt) {
      update.lastManualHandledAt = settings.runNowRequestedAt;
    }
    tx.set(STATUS_REF, update, { merge: true });
    return true;
  });
}

async function runBackup(reason, today, tz, settings) {
  if (!(await claim(reason, today, settings))) { logger.info('Backup already running; skipping.'); return; }
  try {
    const backup = await buildBackup(tz);
    const json = JSON.stringify(backup, null, 2);
    const file = admin.storage().bucket().file(BACKUP_FILE);
    // Single upload to the SAME object name => replaces it. If this throws, the previous file is untouched.
    await file.save(json, { resumable: false, contentType: 'application/json', metadata: { cacheControl: 'no-store' } });
    const [meta] = await file.getMetadata();
    const done = {
      status: 'Success', message: 'Backup completed.', runningSince: FieldValue.delete(),
      lastSuccessAt: Timestamp.now(), sizeBytes: Number(meta.size),
      totalDocuments: backup.backupInfo.totalDocuments, totalCollections: backup.backupInfo.totalCollections,
      collectionCounts: backup.backupInfo.collectionCounts, fileName: BACKUP_FILE,
    };
    if (reason === 'scheduled') done.lastSuccessDate = today; // a manual run doesn't cancel today's scheduled run
    await STATUS_REF.set(done, { merge: true });
    logger.info('Backup OK', { reason, docs: backup.backupInfo.totalDocuments, bytes: done.sizeBytes });
  } catch (err) {
    logger.error('Backup FAILED', err);
    await STATUS_REF.set({
      status: 'Failed', message: String(err && err.message || err).slice(0, 300),
      runningSince: FieldValue.delete(), lastFailureAt: Timestamp.now(),
    }, { merge: true });
  }
}

/* ---------- the scheduled tick ---------- */
exports.dailyBackup = onSchedule(
  { schedule: 'every 5 minutes', timeZone: DEFAULT_TZ, region: 'asia-southeast1',
    memory: '512MiB', timeoutSeconds: 540, retryCount: 0 },
  async () => {
    const settings = (await SETTINGS_REF.get()).data() || {};
    const st = (await STATUS_REF.get()).data() || {};
    const tz = settings.timezone || DEFAULT_TZ;
    const nowDate = new Date();
    const { date: today, minutes } = localParts(nowDate, tz);

    let reason = null;
    const manualNew = settings.runNowRequestedAt &&
      (!st.lastManualHandledAt || settings.runNowRequestedAt.toMillis() > st.lastManualHandledAt.toMillis());
    if (manualNew) {
      reason = 'manual';
    } else if (settings.enabled === true) {
      const [h, m] = String(settings.time || DEFAULT_TIME).split(':').map(Number);
      const due = minutes >= h * 60 + m;
      const doneToday = st.lastSuccessDate === today;
      const attempts = st.lastAttemptDate === today ? (st.attemptsToday || 0) : 0;
      const lastMs = st.lastAttemptAt ? st.lastAttemptAt.toMillis() : 0;
      if (due && !doneToday && attempts < MAX_ATTEMPTS_PER_DAY && nowDate.getTime() - lastMs >= RETRY_AFTER_MS) reason = 'scheduled';
    }
    if (!reason) return;
    await runBackup(reason, today, tz, settings);
  }
);
