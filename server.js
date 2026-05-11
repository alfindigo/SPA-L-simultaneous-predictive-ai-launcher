const express    = require("express");
const { spawn, exec } = require("child_process");
const path       = require("path");
const fs         = require("fs");
require("dotenv").config();

// ── SQLite via sql.js ─────────────────────────────────────────────────────────
const initSqlJs = require("sql.js");
const DB_PATH   = path.join(__dirname, "jobs.db");
let db;

// ── Config from environment — no hardcoding ───────────────────────────────────
const PORT           = parseInt(process.env.LAUNCHER_PORT     || "5557");
const MINIO_ENDPOINT = process.env.MINIO_ENDPOINT             || "localhost";
const MINIO_PORT     = parseInt(process.env.MINIO_PORT        || "9000");
const MINIO_ACCESS   = process.env.MINIO_ACCESS_KEY           || "";
const MINIO_SECRET   = process.env.MINIO_SECRET_KEY           || "";
const AIRFLOW_URL    = process.env.AIRFLOW_URL                || "http://localhost:8080";
const AIRFLOW_USER   = process.env.AIRFLOW_USERNAME           || "admin";
const AIRFLOW_PASS   = process.env.AIRFLOW_PASSWORD           || "admin";

// ── Metric parsing ────────────────────────────────────────────────────────────
function parseMetrics(logText) {
  const grab = (tag) => {
    const m = logText.match(new RegExp(`\\[${tag}\\]\\s+([\\d.]+)`, "i"));
    return m ? parseFloat(m[1]) : null;
  };
  const smape    = grab("SMAPE");
  const accuracy = grab("ACCURACY");
  const rmse     = grab("RMSE");
  const derivedAccuracy = accuracy !== null
    ? accuracy
    : smape !== null ? Math.max(0, Math.min(100, 100 - smape)) : null;
  return { smape, accuracy: derivedAccuracy, rmse };
}

function recordAccuracy(modelName, datasetName, smape, rmse, accuracy, jobId, trainJobId = null) {
  if (accuracy === null && smape !== null) {
    accuracy = parseFloat(Math.max(0, Math.min(100, 100 - smape)).toFixed(2));
  }
  dbInsert(
    `INSERT INTO accuracy (model_name, dataset_name, accuracypercentage, smape, rmse, job_id, train_job_id, recorded_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    [modelName, datasetName, accuracy, smape, rmse, jobId, trainJobId, Date.now()]
  );
  console.log(`[accuracy] model="${modelName}" dataset="${datasetName}" accuracy=${accuracy?.toFixed(2)}% smape=${smape?.toFixed(4)}`);
}

// ── MinIO ─────────────────────────────────────────────────────────────────────
const Minio = require("minio");
const minioClient = new Minio.Client({
  endPoint:  MINIO_ENDPOINT,
  port:      MINIO_PORT,
  useSSL:    false,
  accessKey: MINIO_ACCESS,
  secretKey: MINIO_SECRET,
});
const BUCKETS = { models: "models", datasets: "datasets", logs: "logs", outputs: "outputs" };

async function minioUpload(bucket, objectName, filePath) {
  await minioClient.fPutObject(bucket, objectName, filePath);
}
async function minioDownload(bucket, objectName, destPath) {
  await minioClient.fGetObject(bucket, objectName, destPath);
}
async function minioPutBuffer(bucket, objectName, buffer) {
  const buf = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer, "utf8");
  await minioClient.putObject(bucket, objectName, buf, buf.length);
}
async function minioGetString(bucket, objectName) {
  return new Promise((resolve, reject) => {
    let data = "";
    minioClient.getObject(bucket, objectName, (err, stream) => {
      if (err) return reject(err);
      stream.on("data", chunk => data += chunk.toString());
      stream.on("end",  () => resolve(data));
      stream.on("error", reject);
    });
  });
}
async function minioExists(bucket, objectName) {
  try { await minioClient.statObject(bucket, objectName); return true; }
  catch (_) { return false; }
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────
const https = require("https");
const http  = require("http");

async function fetchDatasetFromUrl(datasetName, url) {
  console.log(`[scheduler] fetching dataset "${datasetName}" from ${url}`);
  const folderName = datasetName.toLowerCase().replace(/\s+/g, "-");
  const ext        = url.includes(".csv") ? ".csv" : url.includes(".json") ? ".json" : ".csv";
  const tmpFile    = path.join(require("os").tmpdir(), `${folderName}_fresh${ext}`);
  await new Promise((resolve, reject) => {
    const proto = url.startsWith("https") ? https : http;
    const file  = fs.createWriteStream(tmpFile);
    proto.get(url, res => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        file.close();
        return fetchDatasetFromUrl(datasetName, res.headers.location).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode} from ${url}`));
      res.pipe(file);
      file.on("finish", () => file.close(resolve));
      file.on("error", reject);
    }).on("error", reject);
  });
  const objectName = `${folderName}/latest${ext}`;
  await minioUpload(BUCKETS.datasets, objectName, tmpFile);
  const localDir  = path.join(__dirname, "datasets", folderName);
  fs.mkdirSync(localDir, { recursive: true });
  const localFile = path.join(localDir, `latest${ext}`);
  fs.copyFileSync(tmpFile, localFile);
  fs.unlinkSync(tmpFile);
  return localFile;
}

// ── Scheduled pipeline ────────────────────────────────────────────────────────
async function runScheduledPipeline(modelId, datasetId, extraArgs = "", trainExtraArgs = "", runExtraArgs = "") {
  console.log(`[scheduler] pipeline starting — model=${modelId} dataset=${datasetId}`);
  const model   = dbGet(`SELECT * FROM models   WHERE id = ?`, [modelId]);
  const dataset = dbGet(`SELECT * FROM datasets WHERE id = ?`, [datasetId]);
  if (!model)   throw new Error("model not found");
  if (!dataset) throw new Error("dataset not found");

  let freshDataPath = dataset.path || dataset.name;
  if (dataset.url) {
    try {
      freshDataPath = await fetchDatasetFromUrl(dataset.name, dataset.url);
      dbRun(`UPDATE datasets SET path = ? WHERE id = ?`, [freshDataPath, datasetId]);
    } catch (e) {
      console.error(`[scheduler] dataset fetch failed, using existing: ${e.message}`);
    }
  }

  const cfg = loadModelConfig(model.path);
  if (!cfg) throw new Error("could not load model config");
  const trainScript = path.join(model.path, cfg.train_script);
  if (!fs.existsSync(trainScript)) throw new Error("train script not found");

  const effectiveTrainArgs = trainExtraArgs.trim() || extraArgs.trim();
  const trainArgs = ["--dataset", dataset.name];
  if (freshDataPath && freshDataPath !== dataset.name) trainArgs.push("--dataset_path", freshDataPath);
  if (effectiveTrainArgs) trainArgs.push(...effectiveTrainArgs.split(/\s+/));

  const trainJobId = await startJob("train", trainScript, trainArgs, model.path, {
    modelName: model.name, modelPath: model.path, dataset: dataset.name, extraArgs,
  });
  await new Promise(resolve => {
    const check = setInterval(() => { if (!procs.has(trainJobId)) { clearInterval(check); resolve(); } }, 5000);
  });
  const trainJob = dbGet(`SELECT status FROM jobs WHERE id = ?`, [trainJobId]);
  if (trainJob?.status !== "done") throw new Error(`train job ${trainJobId} did not complete (${trainJob?.status})`);

  const runScript = path.join(model.path, cfg.run_script);
  if (!fs.existsSync(runScript)) throw new Error("run script not found");
  const testPath = dataset.test_path || freshDataPath;
  const effectiveRunArgs = runExtraArgs.trim() || extraArgs.trim();
  const runArgs = ["--dataset", dataset.name];
  if (testPath && testPath !== dataset.name) runArgs.push("--dataset_path", testPath);
  if (effectiveRunArgs) runArgs.push(...effectiveRunArgs.split(/\s+/));

  const runJobId = await startJob("run", runScript, runArgs, model.path, {
    modelName: model.name, modelPath: model.path, dataset: dataset.name, extraArgs,
  });
  await new Promise(resolve => {
    const check = setInterval(() => { if (!procs.has(runJobId)) { clearInterval(check); resolve(); } }, 5000);
  });

  let logText = "";
  const lp = logPath(runJobId);
  if (fs.existsSync(lp)) {
    logText = fs.readFileSync(lp, "utf8");
  } else {
    try { logText = await minioGetString(BUCKETS.logs, `job_${runJobId}.log`); } catch (e) {
      console.error(`[scheduler] could not read log:`, e.message);
    }
  }

  const { smape, accuracy, rmse } = parseMetrics(logText);
  if (smape !== null || accuracy !== null) {
    try { recordAccuracy(model.name, dataset.name, smape, rmse, accuracy, runJobId, trainJobId); }
    catch (e) { console.error(`[scheduler] recordAccuracy failed:`, e.message); }
  }
}

// ── MinIO model sync ──────────────────────────────────────────────────────────
async function syncModelFromMinio(modelName, localModelPath) {
  const folderName = modelName.toLowerCase().replace(/\s+/g, "-");
  const objects = await new Promise((resolve, reject) => {
    const items = [];
    const stream = minioClient.listObjects(BUCKETS.models, `${folderName}/`, true);
    stream.on("data", obj => items.push(obj));
    stream.on("end",  () => resolve(items));
    stream.on("error", reject);
  });
  for (const obj of objects) {
    const fileName = path.basename(obj.name);
    const destPath = path.join(localModelPath, fileName);
    await minioDownload(BUCKETS.models, obj.name, destPath);
  }
  console.log(`[minio] synced ${objects.length} file(s) into ${localModelPath}`);
}

// ── Cron scheduler ────────────────────────────────────────────────────────────
const cron = require("node-cron");
function registerSchedule(s) {
  if (cronTasks.has(s.id)) { cronTasks.get(s.id).stop(); cronTasks.delete(s.id); }
  if (!s.enabled) return;
  if (!cron.validate(s.cron_time)) { console.error(`[scheduler] invalid cron "${s.cron_time}"`); return; }
  const task = cron.schedule(s.cron_time, () => {
    dbRun(`UPDATE schedules SET last_run=?, last_status='running' WHERE id=?`, [Date.now(), s.id]);
    runScheduledPipeline(s.model_id, s.dataset_id, s.extra_args || "", s.train_extra_args || "", s.run_extra_args || "")
      .then(() => dbRun(`UPDATE schedules SET last_status='done' WHERE id=?`, [s.id]))
      .catch(e => { console.error(`[scheduler] error:`, e.message); dbRun(`UPDATE schedules SET last_status='failed' WHERE id=?`, [s.id]); });
  });
  cronTasks.set(s.id, task);
  console.log(`[scheduler] registered schedule ${s.id} @ "${s.cron_time}"`);
}
function initScheduler() {
  const schedules = dbAll(`SELECT * FROM schedules WHERE enabled=1`);
  schedules.forEach(registerSchedule);
  console.log(`[scheduler] ${schedules.length} schedule(s) loaded`);
}

// ── DB init ───────────────────────────────────────────────────────────────────
async function initDb() {
  const SQL = await initSqlJs();
  db = fs.existsSync(DB_PATH) ? new SQL.Database(fs.readFileSync(DB_PATH)) : new SQL.Database();
  db.run(`CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL, model_name TEXT NOT NULL,
    model_path TEXT NOT NULL, dataset TEXT NOT NULL DEFAULT '', extra_args TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'running', exit_code INTEGER, fail_reason TEXT, log_file TEXT,
    started_at INTEGER NOT NULL, ended_at INTEGER, duration_ms INTEGER)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_started ON jobs(started_at DESC)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_status  ON jobs(status)`);
  db.run(`CREATE TABLE IF NOT EXISTS models (
    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
    path TEXT NOT NULL, created_at INTEGER NOT NULL)`);
  db.run(`CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT, model_id INTEGER NOT NULL, dataset_id INTEGER NOT NULL,
    cron_time TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, last_run INTEGER,
    last_status TEXT, created_at INTEGER NOT NULL)`);
  try { db.run(`ALTER TABLE schedules ADD COLUMN extra_args TEXT NOT NULL DEFAULT ''`); } catch (_) {}
  try { db.run(`ALTER TABLE schedules ADD COLUMN train_extra_args TEXT NOT NULL DEFAULT ''`); } catch (_) {}
  try { db.run(`ALTER TABLE schedules ADD COLUMN run_extra_args TEXT NOT NULL DEFAULT ''`); } catch (_) {}
  db.run(`CREATE TABLE IF NOT EXISTS datasets (
    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE,
    path TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL)`);
  try { db.run(`ALTER TABLE datasets ADD COLUMN url       TEXT NOT NULL DEFAULT ''`); } catch (_) {}
  try { db.run(`ALTER TABLE datasets ADD COLUMN test_path TEXT NOT NULL DEFAULT ''`); } catch (_) {}
  db.run(`CREATE TABLE IF NOT EXISTS accuracy (
    id INTEGER PRIMARY KEY AUTOINCREMENT, model_name TEXT NOT NULL, dataset_name TEXT NOT NULL,
    accuracypercentage REAL, smape REAL, rmse REAL, job_id INTEGER NOT NULL,
    train_job_id INTEGER, recorded_at INTEGER NOT NULL)`);
  try { db.run(`ALTER TABLE accuracy ADD COLUMN smape REAL`); } catch (_) {}
  try { db.run(`ALTER TABLE accuracy ADD COLUMN rmse  REAL`); } catch (_) {}
  try { db.run(`ALTER TABLE accuracy ADD COLUMN accuracypercentage REAL`); } catch (_) {}
  db.run(`CREATE INDEX IF NOT EXISTS idx_accuracy_model ON accuracy(model_name)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_accuracy_time  ON accuracy(recorded_at DESC)`);
  persist();
  console.log("[db] ready →", DB_PATH);
}

// ── DB helpers ────────────────────────────────────────────────────────────────
function persist() { fs.writeFileSync(DB_PATH, Buffer.from(db.export())); }
function dbRun(sql, params = []) { db.run(sql, params); persist(); }
function dbInsert(sql, params = []) {
  db.run(sql, params);
  const result = db.exec("SELECT last_insert_rowid()");
  persist();
  return result[0].values[0][0];
}
function dbGet(sql, params = []) {
  const stmt = db.prepare(sql); stmt.bind(params);
  const row  = stmt.step() ? stmt.getAsObject() : null;
  stmt.free(); return row;
}
function dbAll(sql, params = []) {
  const stmt = db.prepare(sql); stmt.bind(params);
  const rows = [];
  while (stmt.step()) rows.push(stmt.getAsObject());
  stmt.free(); return rows;
}

// ── Logs ──────────────────────────────────────────────────────────────────────
const LOGS_DIR = path.join(__dirname, "logs");
if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR, { recursive: true });
const logPath      = (id) => path.join(LOGS_DIR, `job_${id}.log`);
const jobOutputDir = (id) => path.join(LOGS_DIR, `job_${id}`);
const logBuffers   = new Map();

function writeLog(file, text) { if (!file) return; try { fs.appendFileSync(file, text); } catch (_) {} }
function writeLogMinio(jobId, text) { logBuffers.set(jobId, (logBuffers.get(jobId) || "") + text); }

setInterval(async () => {
  for (const [jobId, text] of logBuffers.entries()) {
    if (!text) continue;
    try {
      const objectName = `job_${jobId}.log`;
      let existing = "";
      try { existing = await minioGetString(BUCKETS.logs, objectName); } catch (_) {}
      await minioPutBuffer(BUCKETS.logs, objectName, existing + text);
      logBuffers.set(jobId, "");
    } catch (e) { console.error(`[minio] log flush failed for job ${jobId}:`, e.message); }
  }
}, 3000);

// ── Runtime state ─────────────────────────────────────────────────────────────
const procs     = new Map();
const progress  = new Map();
const cronTasks = new Map();

// ── Express ───────────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

function fmtMs(ms) { const s = Math.floor(ms / 1000), m = Math.floor(s / 60); return m ? `${m}m ${s % 60}s` : `${s}s`; }
function enrichJob(j) {
  const running = procs.has(j.id);
  j.status      = running ? "running" : j.status;
  j.duration_ms = running ? Date.now() - j.started_at : j.duration_ms;
  return j;
}
function parseProgress(text, jobId) {
  const p = progress.get(jobId); if (!p) return;
  const sm = text.match(/Series\s+(\d+)\/(\d+)\s+done\s+\((\d+(?:\.\d+)?)%\)/);
  if (sm) { p.percent = Math.min(100, parseFloat(sm[3])); p.detail = `Series ${sm[1]} / ${sm[2]}`; }
  const em = text.match(/Epoch\s+(\d+):\s+\S+\s+(\d+)\/(\d+)/);
  if (em) {
    const ep = parseInt(em[1]), step = parseInt(em[2]), tot = parseInt(em[3]);
    p.percent = Math.min(99, Math.round(((ep + step / tot) / p.totalEpochs) * 100));
    p.detail  = `Epoch ${ep + 1} / ${p.totalEpochs}`;
  }
  const me = text.match(/Max epochs:\s+(\d+)/); if (me) p.totalEpochs = parseInt(me[1]);
}

// ── Model config ──────────────────────────────────────────────────────────────
function detectModelConfig(modelPath) {
  if (fs.existsSync(path.join(modelPath, "model.json")))  return "model.json";
  if (fs.existsSync(path.join(modelPath, "config.json"))) return "config.json";
  const files = fs.readdirSync(modelPath);
  if (files.some(f => f === "model.py" || f.startsWith("modeling_"))) return "model.py";
  return null;
}
function loadModelConfig(modelPath) {
  const kind = detectModelConfig(modelPath); if (!kind) return null;
  if (kind === "model.json") {
    const cfg = JSON.parse(fs.readFileSync(path.join(modelPath, "model.json"), "utf8"));
    cfg.model_type = cfg.model_type || "custom"; return cfg;
  }
  if (kind === "config.json") {
    const hfCfg    = JSON.parse(fs.readFileSync(path.join(modelPath, "config.json"), "utf8"));
    const modelType = hfCfg.model_type || "unknown";
    const wrapperMap = {
      "chronos": { train: "train_wrapper.py", run: "run_wrapper.py" },
      "chronos_bolt": { train: "train_wrapper.py", run: "run_wrapper.py" },
      "t5":      { train: "train_wrapper.py", run: "run_wrapper.py" },
      "moirai":  { train: "train_wrapper.py", run: "run_wrapper.py" },
      "timesfm": { train: "train_wrapper.py", run: "run_wrapper.py" },
    };
    const scripts = wrapperMap[modelType] || { train: "train_wrapper.py", run: "run_wrapper.py" };
    return { name: hfCfg.name || modelType, model_type: modelType,
             train_script: scripts.train, run_script: scripts.run,
             dataset_param: "--dataset", model_param: "", _hf: hfCfg };
  }
  if (kind === "model.py") {
    return { name: path.basename(modelPath), model_type: "research",
             train_script: "train_wrapper.py", run_script: "run_wrapper.py",
             dataset_param: "--dataset", model_param: "--model" };
  }
  return null;
}

// ── Core job runner ───────────────────────────────────────────────────────────
async function startJob(type, scriptPath, args, cwd, meta) {
  const { modelName, modelPath } = meta;
  const startedAt = Date.now();
  const jobId = dbInsert(
    `INSERT INTO jobs (type, model_name, model_path, dataset, extra_args, status, started_at) VALUES (?,?,?,?,?,'running',?)`,
    [type, modelName, modelPath, meta.dataset || "", meta.extraArgs || "", startedAt]
  );
  const lp = logPath(jobId), outDir = jobOutputDir(jobId);
  dbRun(`UPDATE jobs SET log_file = ? WHERE id = ?`, [lp, jobId]);
  try { fs.mkdirSync(outDir, { recursive: true }); } catch (_) {}
  progress.set(jobId, { percent: 0, detail: "", totalEpochs: 50 });
  try { await syncModelFromMinio(modelName, modelPath); }
  catch (e) { console.warn(`[minio] weight sync skipped: ${e.message}`); }
  fs.writeFileSync(lp, [`[launcher] job #${jobId} — ${type} — ${new Date().toISOString()}`,
    `[launcher] $ python ${path.basename(scriptPath)} ${args.join(" ")}`, ""].join("\n"));
  const python = process.platform === "win32" ? "python" : "python3";
  const child  = spawn(python, ["-u", scriptPath, ...args], {
    cwd, stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, PYTHONUNBUFFERED: "1", PYTHONIOENCODING: "utf-8",
           JOB_OUTPUT_DIR: outDir, JOB_ID: String(jobId) },
  });
  procs.set(jobId, child);
  console.log(`[job ${jobId}] started PID ${child.pid}`);
  child.stdout.on("data", d => { const t = d.toString(); writeLog(lp, t); writeLogMinio(jobId, t); parseProgress(t, jobId); });
  child.stderr.on("data", d => { const t = d.toString(); writeLog(lp, t); writeLogMinio(jobId, t); });
  const cleanup = () => {
    try { if (fs.existsSync(outDir)) fs.rmSync(outDir, { recursive: true, force: true }); } catch (_) {}
    setTimeout(() => {
      logBuffers.delete(jobId);
      try { if (fs.existsSync(lp)) fs.unlinkSync(lp); } catch (_) {}
    }, 30000);
  };
  child.on("close", async (code) => {
    const endedAt = Date.now(), duration_ms = endedAt - startedAt;
    const row = dbGet(`SELECT status FROM jobs WHERE id = ?`, [jobId]);
    procs.delete(jobId); progress.delete(jobId);
    if (row?.status === "cancelled") {
      dbRun(`UPDATE jobs SET ended_at=?, duration_ms=? WHERE id=?`, [endedAt, duration_ms, jobId]);
      writeLog(lp, `\n[CANCELLED] Duration: ${fmtMs(duration_ms)}\n`);
      writeLogMinio(jobId, `\n[CANCELLED] Duration: ${fmtMs(duration_ms)}\n`);
    } else if (code === 0) {
      dbRun(`UPDATE jobs SET status='done', exit_code=0, ended_at=?, duration_ms=? WHERE id=?`, [endedAt, duration_ms, jobId]);
      writeLog(lp, `\n[DONE] Duration: ${fmtMs(duration_ms)}\n`);
      writeLogMinio(jobId, `\n[DONE] Duration: ${fmtMs(duration_ms)}\n`);
      const jobRow = dbGet(`SELECT model_path FROM jobs WHERE id = ?`, [jobId]);
      const forecastFile = findForecastImage(jobId, jobRow?.model_path, startedAt);
      if (forecastFile) {
        try { await minioUpload(BUCKETS.outputs, `job_${jobId}/forecast.png`, forecastFile); }
        catch (e) { console.error(`[minio] forecast upload failed:`, e.message); }
      }
    } else if (code !== null) {
      const reason = `Exited with code ${code}`;
      dbRun(`UPDATE jobs SET status='failed', exit_code=?, fail_reason=?, ended_at=?, duration_ms=? WHERE id=?`,
        [code, reason, endedAt, duration_ms, jobId]);
      writeLog(lp, `\n[ERROR] ${reason}. Duration: ${fmtMs(duration_ms)}\n`);
      writeLogMinio(jobId, `\n[ERROR] ${reason}. Duration: ${fmtMs(duration_ms)}\n`);
    } else {
      dbRun(`UPDATE jobs SET status='cancelled', ended_at=?, duration_ms=? WHERE id=?`, [endedAt, duration_ms, jobId]);
      writeLog(lp, `\n[CANCELLED] Duration: ${fmtMs(duration_ms)}\n`);
      writeLogMinio(jobId, `\n[CANCELLED] Duration: ${fmtMs(duration_ms)}\n`);
    }
    cleanup();
  });
  child.on("error", err => {
    procs.delete(jobId); progress.delete(jobId);
    const endedAt = Date.now();
    dbRun(`UPDATE jobs SET status='failed', fail_reason=?, ended_at=?, duration_ms=? WHERE id=?`,
      [err.message, endedAt, endedAt - startedAt, jobId]);
    writeLog(lp, `\n[ERROR] ${err.message}\n`);
    writeLogMinio(jobId, `\n[ERROR] ${err.message}\n`);
    cleanup();
  });
  return jobId;
}

// ── Forecast image ────────────────────────────────────────────────────────────
function findForecastImage(jobId, modelPath, startedAt) {
  const outDir = jobOutputDir(jobId);
  if (fs.existsSync(outDir)) {
    const explicit = path.join(outDir, "forecast.png");
    if (fs.existsSync(explicit)) return explicit;
    try {
      const pngs = fs.readdirSync(outDir).filter(f => f.toLowerCase().endsWith(".png"));
      if (pngs.length) return path.join(outDir, pngs[0]);
    } catch (_) {}
  }
  if (modelPath) {
    for (const candidate of [
      path.join(modelPath, "forecast.png"),
      path.join(modelPath, "outputs", "forecast.png"),
      path.join(modelPath, `forecast_${jobId}.png`),
    ]) {
      if (fs.existsSync(candidate)) {
        try { const st = fs.statSync(candidate); if (!startedAt || st.mtimeMs >= startedAt - 60000) return candidate; } catch (_) {}
      }
    }
  }
  return null;
}

// ── REST endpoints ────────────────────────────────────────────────────────────
app.get("/api/models", (req, res) => res.json(dbAll(`SELECT * FROM models ORDER BY name ASC`)));
app.post("/api/models", async (req, res) => {
  const { name, path: mpath } = req.body;
  if (!name || !mpath) return res.status(400).json({ error: "name and path required" });
  try {
    const detected = detectModelConfig(mpath);
    if (!detected) return res.status(400).json({ error: "No recognised config found." });
  } catch (e) { return res.status(400).json({ error: e.message }); }
  try {
    dbRun(`INSERT INTO models (name, path, created_at) VALUES (?, ?, ?)`, [name, mpath, Date.now()]);
    const row = dbGet(`SELECT * FROM models WHERE name = ?`, [name]);
    const folderName = name.toLowerCase().replace(/\s+/g, "-");
    for (const file of fs.readdirSync(mpath)) {
      const fullPath = path.join(mpath, file);
      if (fs.statSync(fullPath).isFile()) {
        try { await minioUpload(BUCKETS.models, `${folderName}/${file}`, fullPath); } catch (e) {}
      }
    }
    res.status(201).json(row);
  } catch (e) { res.status(409).json({ error: "Model name already exists" }); }
});
app.delete("/api/models/:id", (req, res) => { dbRun(`DELETE FROM models WHERE id = ?`, [parseInt(req.params.id)]); res.json({ ok: true }); });

app.get("/api/datasets", (req, res) => res.json(dbAll(`SELECT * FROM datasets ORDER BY name ASC`)));
app.get("/api/dataset-info", (req, res) => {
  const ds = dbGet(`SELECT * FROM datasets WHERE id = ?`, [parseInt(req.query.datasetId)]);
  if (!ds) return res.status(404).json({ error: "Dataset not found" });
  const isGluon = !ds.path;
  res.json({ name: ds.name, path: ds.path || null, isGluon,
    type: isGluon ? "gluonts" : ds.path.endsWith(".csv") ? "csv" : ds.path.endsWith(".json") ? "json" : "unknown" });
});
app.post("/api/datasets", async (req, res) => {
  const { name, path: dpath = "" } = req.body;
  if (!name) return res.status(400).json({ error: "name required" });
  try {
    dbRun(`INSERT INTO datasets (name, path, created_at) VALUES (?, ?, ?)`, [name, dpath, Date.now()]);
    const row = dbGet(`SELECT * FROM datasets WHERE name = ?`, [name]);
    if (dpath && fs.existsSync(dpath)) {
      const folderName = name.toLowerCase().replace(/\s+/g, "-");
      const stat = fs.statSync(dpath);
      if (stat.isDirectory()) {
        for (const file of fs.readdirSync(dpath)) {
          const fullPath = path.join(dpath, file);
          if (fs.statSync(fullPath).isFile()) {
            try { await minioUpload(BUCKETS.datasets, `${folderName}/${file}`, fullPath); } catch (e) {}
          }
        }
      } else { await minioUpload(BUCKETS.datasets, `${folderName}/${path.basename(dpath)}`, dpath); }
    }
    res.status(201).json(row);
  } catch (e) { res.status(409).json({ error: "Dataset name already exists" }); }
});
app.post("/api/datasets/upsert", (req, res) => {
  const { name, path: dpath = "" } = req.body;
  if (!name) return res.status(400).json({ error: "name required" });
  const existing = dbGet(`SELECT * FROM datasets WHERE name = ?`, [name]);
  if (existing) { dbRun(`UPDATE datasets SET path = ? WHERE id = ?`, [dpath, existing.id]); return res.json(dbGet(`SELECT * FROM datasets WHERE id = ?`, [existing.id])); }
  try { dbRun(`INSERT INTO datasets (name, path, created_at) VALUES (?, ?, ?)`, [name, dpath, Date.now()]); res.status(201).json(dbGet(`SELECT * FROM datasets WHERE name = ?`, [name])); }
  catch (e) { res.status(500).json({ error: e.message }); }
});
app.delete("/api/datasets/:id", (req, res) => { dbRun(`DELETE FROM datasets WHERE id = ?`, [parseInt(req.params.id)]); res.json({ ok: true }); });
app.patch("/api/datasets/:id", (req, res) => {
  const id = parseInt(req.params.id);
  const ds = dbGet(`SELECT * FROM datasets WHERE id = ?`, [id]);
  if (!ds) return res.status(404).json({ error: "Dataset not found" });
  if (req.body.path      !== undefined) dbRun(`UPDATE datasets SET path=?      WHERE id=?`, [req.body.path,      id]);
  if (req.body.url       !== undefined) dbRun(`UPDATE datasets SET url=?       WHERE id=?`, [req.body.url,       id]);
  if (req.body.test_path !== undefined) dbRun(`UPDATE datasets SET test_path=? WHERE id=?`, [req.body.test_path, id]);
  res.json(dbGet(`SELECT * FROM datasets WHERE id = ?`, [id]));
});
app.get("/api/dataset-files", (req, res) => {
  const { path: dpath } = req.query; if (!dpath) return res.json({ files: [] });
  try {
    if (!fs.existsSync(dpath)) return res.json({ files: [], error: "Path not found" });
    const stat = fs.statSync(dpath);
    if (stat.isDirectory()) return res.json({ files: fs.readdirSync(dpath, { withFileTypes: true }).filter(e => e.isFile()).map(e => e.name).sort() });
    return res.json({ files: [path.basename(dpath)] });
  } catch (e) { return res.json({ files: [], error: e.message }); }
});

app.post("/api/pipeline", async (req, res) => {
  const { modelId, datasetId, table = "energy_consumption", gluontsName = "electricity" } = req.body;
  if (!modelId) return res.status(400).json({ error: "modelId required" });
  const model   = dbGet(`SELECT * FROM models   WHERE id = ?`, [modelId]);
  const dataset = datasetId ? dbGet(`SELECT * FROM datasets WHERE id = ?`, [datasetId]) : null;
  if (!model) return res.status(400).json({ error: "Model not found" });
  const conf = {
    model_name:   model.name,
    dataset_name: dataset?.name || "energy_data",
    table,
    gluonts_name: gluontsName,
  };
  try {
    const auth    = Buffer.from(`${AIRFLOW_USER}:${AIRFLOW_PASS}`).toString("base64");
    const trigger = await fetch(`${AIRFLOW_URL}/api/v1/dags/launcher_pipeline/dagRuns`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Basic ${auth}` },
      body: JSON.stringify({ conf }),
    });
    if (!trigger.ok) { const err = await trigger.text(); return res.status(502).json({ error: `Airflow error: ${err}` }); }
    const data = await trigger.json();
    res.json({ ok: true, dagRunId: data.dag_run_id, conf });
  } catch (e) { res.status(500).json({ error: `Could not reach Airflow at ${AIRFLOW_URL}: ${e.message}` }); }
});

app.get("/api/jobs", (req, res) => {
  const q = (req.query.search || "").trim(), like = `%${q}%`;
  const jobs = q
    ? dbAll(`SELECT * FROM jobs WHERE model_name LIKE ? OR dataset LIKE ? OR type LIKE ? OR status LIKE ? ORDER BY started_at DESC LIMIT 200`, [like, like, like, like])
    : dbAll(`SELECT * FROM jobs ORDER BY started_at DESC LIMIT 200`);
  res.json({ jobs: jobs.map(j => enrichJob({ ...j, duration_ms: procs.has(j.id) ? Date.now() - j.started_at : j.duration_ms })) });
});
app.get("/api/jobs/:id", (req, res) => {
  const id = parseInt(req.params.id);
  const job = dbGet(`SELECT * FROM jobs WHERE id = ?`, [id]);
  if (!job) return res.status(404).json({ error: "Not found" });
  try { const cfg = loadModelConfig(job.model_path); if (cfg) { job.train_script = cfg.train_script; job.run_script = cfg.run_script; job.dataset_param = cfg.dataset_param; job.model_type = cfg.model_type; } } catch (_) {}
  res.json(enrichJob({ ...job, duration_ms: procs.has(id) ? Date.now() - job.started_at : job.duration_ms }));
});
app.get("/api/jobs/:id/log", async (req, res) => {
  const id = parseInt(req.params.id);
  if (procs.has(id)) {
    const job = dbGet(`SELECT log_file FROM jobs WHERE id = ?`, [id]);
    if (!job?.log_file || !fs.existsSync(job.log_file)) return res.json({ log: "", size: 0 });
    try {
      const stat = fs.statSync(job.log_file), offset = Math.max(0, parseInt(req.query.offset || "0"));
      if (offset >= stat.size) return res.json({ log: "", size: stat.size });
      const buf = Buffer.alloc(stat.size - offset), fd = fs.openSync(job.log_file, "r");
      fs.readSync(fd, buf, 0, buf.length, offset); fs.closeSync(fd);
      return res.json({ log: buf.toString("utf8"), size: stat.size });
    } catch { return res.json({ log: "", size: 0 }); }
  }
  try { const text = await minioGetString(BUCKETS.logs, `job_${id}.log`); return res.json({ log: text, size: Buffer.byteLength(text, "utf8") }); }
  catch (_) {
    const job = dbGet(`SELECT log_file FROM jobs WHERE id = ?`, [id]);
    if (job?.log_file && fs.existsSync(job.log_file)) {
      try { const text = fs.readFileSync(job.log_file, "utf8"); return res.json({ log: text, size: Buffer.byteLength(text, "utf8") }); } catch {}
    }
    return res.json({ log: "", size: 0 });
  }
});
app.get("/api/jobs/:id/progress", (req, res) => {
  const id = parseInt(req.params.id), p = progress.get(id);
  res.json(p ? { percent: p.percent, detail: p.detail, running: procs.has(id) } : { percent: 0, detail: "", running: false });
});
app.get("/api/validate", (req, res) => {
  const { modelPath } = req.query; if (!modelPath) return res.json({ valid: false, error: "No path" });
  try { const cfg = loadModelConfig(modelPath); if (!cfg) return res.json({ valid: false, error: "No recognised config found." }); res.json({ valid: true, config: cfg }); }
  catch (e) { res.json({ valid: false, error: e.message }); }
});

app.get("/api/schedules", (req, res) => res.json(dbAll(`SELECT s.*, m.name as model_name, d.name as dataset_name FROM schedules s LEFT JOIN models m ON m.id=s.model_id LEFT JOIN datasets d ON d.id=s.dataset_id ORDER BY s.created_at DESC`)));
app.post("/api/schedules", (req, res) => {
  const { modelId, datasetId, cronTime, extraArgs = "", trainExtraArgs = "", runExtraArgs = "" } = req.body;
  if (!modelId || !datasetId || !cronTime) return res.status(400).json({ error: "modelId, datasetId and cronTime required" });
  if (!cron.validate(cronTime)) return res.status(400).json({ error: `Invalid cron: "${cronTime}"` });
  const m = dbGet(`SELECT * FROM models WHERE id=?`, [modelId]), d = dbGet(`SELECT * FROM datasets WHERE id=?`, [datasetId]);
  if (!m) return res.status(400).json({ error: "Model not found" });
  if (!d) return res.status(400).json({ error: "Dataset not found" });
  const id = dbInsert(`INSERT INTO schedules (model_id, dataset_id, cron_time, extra_args, train_extra_args, run_extra_args, enabled, created_at) VALUES (?,?,?,?,?,?,1,?)`,
    [modelId, datasetId, cronTime, extraArgs, trainExtraArgs, runExtraArgs, Date.now()]);
  const row = dbGet(`SELECT s.*, m.name as model_name, d.name as dataset_name FROM schedules s LEFT JOIN models m ON m.id=s.model_id LEFT JOIN datasets d ON d.id=s.dataset_id WHERE s.id=?`, [id]);
  registerSchedule(row); res.status(201).json(row);
});
app.patch("/api/schedules/:id", (req, res) => {
  const id = parseInt(req.params.id), row = dbGet(`SELECT * FROM schedules WHERE id=?`, [id]);
  if (!row) return res.status(404).json({ error: "Not found" });
  const enabled = req.body.enabled !== undefined ? (req.body.enabled ? 1 : 0) : row.enabled;
  const cronTime = req.body.cronTime || row.cron_time;
  if (!cron.validate(cronTime)) return res.status(400).json({ error: `Invalid cron: "${cronTime}"` });
  dbRun(`UPDATE schedules SET enabled=?, cron_time=? WHERE id=?`, [enabled, cronTime, id]);
  const updated = dbGet(`SELECT s.*, m.name as model_name, d.name as dataset_name FROM schedules s LEFT JOIN models m ON m.id=s.model_id LEFT JOIN datasets d ON d.id=s.dataset_id WHERE s.id=?`, [id]);
  registerSchedule(updated); res.json(updated);
});
app.delete("/api/schedules/:id", (req, res) => {
  const id = parseInt(req.params.id);
  if (cronTasks.has(id)) { cronTasks.get(id).stop(); cronTasks.delete(id); }
  dbRun(`DELETE FROM schedules WHERE id=?`, [id]); res.json({ ok: true });
});
app.post("/api/schedules/:id/run", async (req, res) => {
  const id = parseInt(req.params.id), row = dbGet(`SELECT * FROM schedules WHERE id=?`, [id]);
  if (!row) return res.status(404).json({ error: "Not found" });
  res.json({ ok: true, message: "Pipeline triggered" });
  dbRun(`UPDATE schedules SET last_run=?, last_status='running' WHERE id=?`, [Date.now(), id]);
  runScheduledPipeline(row.model_id, row.dataset_id, row.extra_args || "", row.train_extra_args || "", row.run_extra_args || "")
    .then(() => dbRun(`UPDATE schedules SET last_status='done' WHERE id=?`, [id]))
    .catch(e => { console.error(`[scheduler] error:`, e.message); dbRun(`UPDATE schedules SET last_status='failed' WHERE id=?`, [id]); });
});
app.post("/api/schedule/run", async (req, res) => {
  const { modelId, datasetId } = req.body;
  if (!modelId || !datasetId) return res.status(400).json({ error: "modelId and datasetId required" });
  res.json({ ok: true, message: "Pipeline started" });
  runScheduledPipeline(parseInt(modelId), parseInt(datasetId)).catch(e => console.error(`[scheduler] error:`, e.message));
});

app.get("/api/accuracy", (req, res) => res.json(dbAll(`SELECT * FROM accuracy ORDER BY recorded_at DESC LIMIT 200`)));
app.get("/api/accuracy/model/:modelName", (req, res) => res.json(dbAll(`SELECT * FROM accuracy WHERE model_name = ? ORDER BY recorded_at DESC LIMIT 100`, [req.params.modelName])));
app.post("/api/accuracy", (req, res) => {
  let { model_name, dataset_name, smape, rmse, accuracy, job_id, train_job_id } = req.body;
  if (!model_name || !dataset_name) return res.status(400).json({ error: "model_name and dataset_name required" });
  if (accuracy === null || accuracy === undefined) {
    accuracy = smape !== null && smape !== undefined ? parseFloat(Math.max(0, Math.min(100, 100 - smape)).toFixed(2)) : 0;
  }
  dbInsert(`INSERT INTO accuracy (model_name, dataset_name, accuracypercentage, smape, rmse, job_id, train_job_id, recorded_at) VALUES (?,?,?,?,?,?,?,?)`,
    [model_name, dataset_name, accuracy, smape ?? null, rmse ?? null, job_id, train_job_id ?? null, Date.now()]);
  res.json({ ok: true });
});

app.get("/api/pick-folder", (req, res) => {
  let cmd;
  if (process.platform === "win32") cmd = `powershell -Command "Add-Type -AssemblyName System.Windows.Forms; $d = New-Object System.Windows.Forms.FolderBrowserDialog; if ($d.ShowDialog() -eq 'OK') { $d.SelectedPath }"`;
  else if (process.platform === "darwin") cmd = `osascript -e 'POSIX path of (choose folder)'`;
  else cmd = `zenity --file-selection --directory`;
  exec(cmd, (err, stdout) => res.json(err || !stdout.trim() ? { cancelled: true } : { path: stdout.trim() }));
});
app.get("/api/pick-file", (req, res) => {
  let cmd;
  if (process.platform === "win32") cmd = `powershell -Command "Add-Type -AssemblyName System.Windows.Forms; $d = New-Object System.Windows.Forms.OpenFileDialog; if ($d.ShowDialog() -eq 'OK') { $d.FileName }"`;
  else if (process.platform === "darwin") cmd = `osascript -e 'POSIX path of (choose file)'`;
  else cmd = `zenity --file-selection`;
  exec(cmd, (err, stdout) => res.json(err || !stdout.trim() ? { cancelled: true } : { path: stdout.trim() }));
});

app.post("/api/train", async (req, res) => {
  try {
    let modelPath, modelName, datasetName = "", datasetPath = "";
    if (req.body.modelId) {
      const m = dbGet(`SELECT * FROM models WHERE id = ?`, [req.body.modelId]);
      if (!m) return res.status(400).json({ error: "Model not found" });
      modelPath = m.path; modelName = m.name;
    } else { modelPath = req.body.modelPath; if (!modelPath) return res.status(400).json({ error: "modelId or modelPath required" }); }
    if (req.body.datasetId) {
      const d = dbGet(`SELECT * FROM datasets WHERE id = ?`, [req.body.datasetId]);
      if (!d) return res.status(400).json({ error: "Dataset not found" });
      datasetName = d.name; datasetPath = d.path || "";
    } else if (req.body.datasetPath) { datasetPath = req.body.datasetPath; datasetName = datasetPath; }
    const cfg = loadModelConfig(modelPath); if (!cfg) return res.status(400).json({ error: "No recognised model config found" });
    if (!modelName) modelName = cfg.name;
    const scriptPath = path.join(modelPath, cfg.train_script);
    if (!fs.existsSync(scriptPath)) return res.status(400).json({ error: "train_script not found" });
    const extraArgs = req.body.extraArgs || "";
    const args = ["--dataset", datasetName];
    if (datasetPath) args.push("--dataset_path", datasetPath);
    if (extraArgs.trim()) args.push(...extraArgs.trim().split(/\s+/));
    const jobId = await startJob("train", scriptPath, args, modelPath, { modelName, modelPath, dataset: datasetName, extraArgs });
    res.status(202).json({ ok: true, jobId });
  } catch (e) { res.status(400).json({ error: e.message }); }
});
app.post("/api/run", async (req, res) => {
  try {
    let modelPath, modelName, datasetName = "", datasetPath = "";
    if (req.body.modelId) {
      const m = dbGet(`SELECT * FROM models WHERE id = ?`, [req.body.modelId]);
      if (!m) return res.status(400).json({ error: "Model not found" });
      modelPath = m.path; modelName = m.name;
    } else { modelPath = req.body.modelPath; if (!modelPath) return res.status(400).json({ error: "modelId or modelPath required" }); }
    if (req.body.datasetId) {
      const d = dbGet(`SELECT * FROM datasets WHERE id = ?`, [req.body.datasetId]);
      if (!d) return res.status(400).json({ error: "Dataset not found" });
      datasetName = d.name; datasetPath = d.path || "";
    }
    const cfg = loadModelConfig(modelPath); if (!cfg) return res.status(400).json({ error: "No recognised model config found" });
    if (!modelName) modelName = cfg.name;
    const scriptPath = path.join(modelPath, cfg.run_script);
    if (!fs.existsSync(scriptPath)) return res.status(400).json({ error: "run_script not found" });
    const extraArgs = req.body.extraArgs || "";
    const args = ["--dataset", datasetName || "unknown"];
    if (datasetPath) args.push("--dataset_path", datasetPath);
    if (cfg.model_param) args.push(cfg.model_param, modelPath);
    if (extraArgs.trim()) args.push(...extraArgs.trim().split(/\s+/));
    const jobId = await startJob("run", scriptPath, args, modelPath, { modelName, modelPath, dataset: datasetName, extraArgs });
    res.status(202).json({ ok: true, jobId });
  } catch (e) { res.status(400).json({ error: e.message }); }
});
app.post("/api/cancel", (req, res) => {
  const id = parseInt(req.body.jobId), child = procs.get(id);
  if (!child) return res.json({ ok: true, message: "Not running" });
  dbRun(`UPDATE jobs SET status='cancelled', fail_reason='cancelled by user' WHERE id=?`, [id]);
  if (process.platform === "win32") { exec(`taskkill /F /T /PID ${child.pid}`); }
  else { try { process.kill(-child.pid, "SIGTERM"); } catch (_) { child.kill("SIGTERM"); } setTimeout(() => { try { child.kill("SIGKILL"); } catch (_) {} }, 3000); }
  res.json({ ok: true });
});
app.get("/api/jobs/:id/forecast.png", async (req, res) => {
  const id = parseInt(req.params.id), job = dbGet(`SELECT model_path, started_at FROM jobs WHERE id = ?`, [id]);
  if (!job) return res.status(404).end();
  try { const stream = await minioClient.getObject(BUCKETS.outputs, `job_${id}/forecast.png`); res.setHeader("Content-Type", "image/png"); res.setHeader("Cache-Control", "no-store"); return stream.pipe(res); } catch (_) {}
  const file = findForecastImage(id, job.model_path, job.started_at);
  if (!file) return res.status(404).end();
  res.setHeader("Content-Type", "image/png"); res.setHeader("Cache-Control", "no-store");
  fs.createReadStream(file).pipe(res);
});

const PY_PREVIEW = path.join(__dirname, "_dataset_preview.py");
app.get("/api/dataset-preview", (req, res) => {
  const id = parseInt(req.query.datasetId), limit = Math.min(200, Math.max(1, parseInt(req.query.limit || "20")));
  const ds = dbGet(`SELECT * FROM datasets WHERE id = ?`, [id]);
  if (!ds) return res.status(404).json({ error: "Dataset not found" });
  if (!fs.existsSync(PY_PREVIEW)) return res.status(500).json({ error: `_dataset_preview.py missing` });
  const python = process.platform === "win32" ? "python" : "python3";
  const child  = spawn(python, [PY_PREVIEW, ds.name, ds.path || "", String(limit)], { env: { ...process.env, PYTHONIOENCODING: "utf-8" } });
  let out = "", err = "";
  child.stdout.on("data", d => out += d.toString());
  child.stderr.on("data", d => err += d.toString());
  child.on("close", code => {
    if (code !== 0) return res.status(500).json({ error: (err || `exit ${code}`).trim() });
    try { res.json(JSON.parse(out)); } catch (e) { res.status(500).json({ error: "Bad preview output", raw: out.slice(0, 500) }); }
  });
  child.on("error", e => res.status(500).json({ error: e.message }));
});

app.get("/api/model-readme", (req, res) => {
  const { modelPath } = req.query; if (!modelPath) return res.json({ readme: null });
  const candidates = ["README.md", "readme.md", "README.txt"];
  for (const f of candidates) {
    const fp = path.join(modelPath, f);
    if (fs.existsSync(fp)) { try { return res.json({ readme: fs.readFileSync(fp, "utf8") }); } catch (_) {} }
  }
  res.json({ readme: null });
});

// ── Boot ──────────────────────────────────────────────────────────────────────
initDb().then(() => {
  const orphans = dbAll(`SELECT id FROM jobs WHERE status='running'`);
  if (orphans.length) {
    console.log(`[db] marking ${orphans.length} orphaned job(s) as failed`);
    dbRun(`UPDATE jobs SET status='failed', fail_reason='server restarted', ended_at=?, duration_ms=0 WHERE status='running'`, [Date.now()]);
  }
  app.listen(PORT, "0.0.0.0", () => console.log(`AI Launcher → http://localhost:${PORT}`));
  initScheduler();
});