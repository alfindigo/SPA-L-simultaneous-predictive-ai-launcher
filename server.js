const express = require("express");
const { spawn, exec } = require("child_process");
const path = require("path");
const fs = require("fs");

// ── SQLite via sql.js ─────────────────────────────────────────────────────────
const initSqlJs = require("sql.js");
const DB_PATH = path.join(__dirname, "jobs.db");
let db;

const Minio = require("minio");
const minioClient = new Minio.Client({
  endPoint: "localhost",
  port: 9000,
  useSSL: false,
  accessKey: "user1",
  secretKey: "freshtohome",
});

const BUCKETS = {
  models:   "models",
  datasets: "datasets",
  logs:     "logs",
  outputs:  "outputs",
};

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
      stream.on("end", () => resolve(data));
      stream.on("error", reject);
    });
  });
}

async function minioExists(bucket, objectName) {
  try { await minioClient.statObject(bucket, objectName); return true; }
  catch (_) { return false; }
}

// ── Download model weights from MinIO INTO local modelPath ───────────────────
// Only downloads files that already exist in MinIO — non-fatal if MinIO
// is unavailable (falls back to whatever is already local).
async function syncModelFromMinio(modelName, localModelPath) {
  const folderName = modelName.toLowerCase().replace(/\s+/g, "-");
  const objects = await new Promise((resolve, reject) => {
    const items = [];
    const stream = minioClient.listObjects(BUCKETS.models, `${folderName}/`, true);
    stream.on("data", obj => items.push(obj));
    stream.on("end", () => resolve(items));
    stream.on("error", reject);
  });
  for (const obj of objects) {
    const fileName = path.basename(obj.name);
    const destPath = path.join(localModelPath, fileName);
    await minioDownload(BUCKETS.models, obj.name, destPath);
  }
  console.log(`[minio] synced ${objects.length} file(s) into ${localModelPath}`);
}

// ── DB init ───────────────────────────────────────────────────────────────────
async function initDb() {
  const SQL = await initSqlJs();
  db = fs.existsSync(DB_PATH)
    ? new SQL.Database(fs.readFileSync(DB_PATH))
    : new SQL.Database();
  db.run(`
    CREATE TABLE IF NOT EXISTS jobs (
      id          INTEGER PRIMARY KEY AUTOINCREMENT,
      type        TEXT    NOT NULL,
      model_name  TEXT    NOT NULL,
      model_path  TEXT    NOT NULL,
      dataset     TEXT    NOT NULL DEFAULT '',
      extra_args  TEXT    NOT NULL DEFAULT '',
      status      TEXT    NOT NULL DEFAULT 'running',
      exit_code   INTEGER,
      fail_reason TEXT,
      log_file    TEXT,
      started_at  INTEGER NOT NULL,
      ended_at    INTEGER,
      duration_ms INTEGER
    )
  `);
  db.run(`CREATE INDEX IF NOT EXISTS idx_started ON jobs(started_at DESC)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_status  ON jobs(status)`);
  db.run(`
    CREATE TABLE IF NOT EXISTS models (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      name       TEXT NOT NULL UNIQUE,
      path       TEXT NOT NULL,
      created_at INTEGER NOT NULL
    )
  `);
  db.run(`
    CREATE TABLE IF NOT EXISTS datasets (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      name       TEXT NOT NULL UNIQUE,
      path       TEXT NOT NULL DEFAULT '',
      created_at INTEGER NOT NULL
    )
  `);
  persist();
  console.log("[db] ready →", DB_PATH);
}

// ── DB helpers ────────────────────────────────────────────────────────────────
function persist() {
  fs.writeFileSync(DB_PATH, Buffer.from(db.export()));
}

function dbRun(sql, params = []) {
  db.run(sql, params);
  persist();
}

function dbInsert(sql, params = []) {
  db.run(sql, params);
  const result = db.exec("SELECT last_insert_rowid()");
  persist();
  return result[0].values[0][0];
}

function dbGet(sql, params = []) {
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const row = stmt.step() ? stmt.getAsObject() : null;
  stmt.free();
  return row;
}

function dbAll(sql, params = []) {
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const rows = [];
  while (stmt.step()) rows.push(stmt.getAsObject());
  stmt.free();
  return rows;
}

// ── Log directory ─────────────────────────────────────────────────────────────
const LOGS_DIR = path.join(__dirname, "logs");
if (!fs.existsSync(LOGS_DIR)) fs.mkdirSync(LOGS_DIR, { recursive: true });
const logPath = (id) => path.join(LOGS_DIR, `job_${id}.log`);
const jobOutputDir = (id) => path.join(LOGS_DIR, `job_${id}`);

// In-memory log buffers per job, flushed to MinIO every 3 seconds
const logBuffers = new Map();

function writeLog(file, text) {
  if (!file) return;
  try { fs.appendFileSync(file, text); } catch (_) {}
}

function writeLogMinio(jobId, text) {
  const current = logBuffers.get(jobId) || "";
  logBuffers.set(jobId, current + text);
}

// Flush all pending log buffers to MinIO every 3 seconds
setInterval(async () => {
  for (const [jobId, text] of logBuffers.entries()) {
    if (!text) continue;
    try {
      const objectName = `job_${jobId}.log`;
      let existing = "";
      try { existing = await minioGetString(BUCKETS.logs, objectName); } catch (_) {}
      await minioPutBuffer(BUCKETS.logs, objectName, existing + text);
      logBuffers.set(jobId, "");
    } catch (e) {
      console.error(`[minio] log flush failed for job ${jobId}:`, e.message);
    }
  }
}, 3000);

// ── Runtime state ─────────────────────────────────────────────────────────────
const procs    = new Map(); // jobId → child process
const progress = new Map(); // jobId → { percent, detail, totalEpochs }

// ── Express ───────────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

// ── Helpers ───────────────────────────────────────────────────────────────────
function fmtMs(ms) {
  const s = Math.floor(ms / 1000), m = Math.floor(s / 60);
  return m ? `${m}m ${s % 60}s` : `${s}s`;
}

function enrichJob(j) {
  const running = procs.has(j.id);
  j.status = running ? "running" : j.status;
  j.duration_ms = running ? Date.now() - j.started_at : j.duration_ms;
  return j;
}

function parseProgress(text, jobId) {
  const p = progress.get(jobId);
  if (!p) return;
  const sm = text.match(/Series\s+(\d+)\/(\d+)\s+done\s+\((\d+(?:\.\d+)?)%\)/);
  if (sm) {
    p.percent = Math.min(100, parseFloat(sm[3]));
    p.detail = `Series ${sm[1]} / ${sm[2]}`;
  }
  const em = text.match(/Epoch\s+(\d+):\s+\S+\s+(\d+)\/(\d+)/);
  if (em) {
    const ep = parseInt(em[1]), step = parseInt(em[2]), tot = parseInt(em[3]);
    p.percent = Math.min(99, Math.round(((ep + step / tot) / p.totalEpochs) * 100));
    p.detail = `Epoch ${ep + 1} / ${p.totalEpochs}`;
  }
  const me = text.match(/Max epochs:\s+(\d+)/);
  if (me) p.totalEpochs = parseInt(me[1]);
}

// ── Model config detection ────────────────────────────────────────────────────
function detectModelConfig(modelPath) {
  const modelJson = path.join(modelPath, "model.json");
  if (fs.existsSync(modelJson)) return "model.json";
  const configJson = path.join(modelPath, "config.json");
  if (fs.existsSync(configJson)) return "config.json";
  const files = fs.readdirSync(modelPath);
  if (files.some(f => f === "model.py" || f.startsWith("modeling_"))) return "model.py";
  return null;
}

function loadModelConfig(modelPath) {
  const kind = detectModelConfig(modelPath);
  if (!kind) return null;

  if (kind === "model.json") {
    const cfg = JSON.parse(fs.readFileSync(path.join(modelPath, "model.json"), "utf8"));
    cfg.model_type = cfg.model_type || "custom";
    return cfg;
  }

  if (kind === "config.json") {
    const hfCfg = JSON.parse(fs.readFileSync(path.join(modelPath, "config.json"), "utf8"));
    const modelType = hfCfg.model_type || "unknown";
    const wrapperMap = {
      "chronos":      { train: "train_wrapper.py", run: "run_wrapper.py" },
      "chronos_bolt": { train: "train_wrapper.py", run: "run_wrapper.py" },
      "t5":           { train: "train_wrapper.py", run: "run_wrapper.py" },
      "moirai":       { train: "train_wrapper.py", run: "run_wrapper.py" },
      "timesfm":      { train: "train_wrapper.py", run: "run_wrapper.py" },
    };
    const scripts = wrapperMap[modelType] || { train: "train_wrapper.py", run: "run_wrapper.py" };
    return {
      name: hfCfg.name || modelType,
      model_type: modelType,
      train_script: scripts.train,
      run_script: scripts.run,
      dataset_param: "--dataset",
      model_param: "",
      _hf: hfCfg,
    };
  }

  if (kind === "model.py") {
    return {
      name: path.basename(modelPath),
      model_type: "research",
      train_script: "train_wrapper.py",
      run_script: "run_wrapper.py",
      dataset_param: "--dataset",
      model_param: "--model",
    };
  }

  return null;
}

// ── Core job runner ───────────────────────────────────────────────────────────
// FIX: scriptPath, args, cwd are passed directly (so Python runs from the
//      local modelPath where packages are installed).
//      MinIO is still used to sync the latest weights INTO that local folder
//      before the job starts — keeping MinIO as the source of truth for files.
async function startJob(type, scriptPath, args, cwd, meta) {
  const { modelName, modelPath } = meta;
  const startedAt = Date.now();

  const jobId = dbInsert(
    `INSERT INTO jobs (type, model_name, model_path, dataset, extra_args, status, started_at)
     VALUES (?, ?, ?, ?, ?, 'running', ?)`,
    [type, modelName, modelPath, meta.dataset || "", meta.extraArgs || "", startedAt]
  );

  const lp = logPath(jobId);
  dbRun(`UPDATE jobs SET log_file = ? WHERE id = ?`, [lp, jobId]);

  const outDir = jobOutputDir(jobId);
  try { fs.mkdirSync(outDir, { recursive: true }); } catch (_) {}

  progress.set(jobId, { percent: 0, detail: "", totalEpochs: 50 });

  // FIX: Sync latest weights from MinIO INTO the local modelPath.
  // Python still runs from modelPath so all package imports resolve correctly.
  // Non-fatal — if MinIO is down or has no files, local files are used as-is.
  try {
    await syncModelFromMinio(modelName, modelPath);
  } catch (e) {
    console.warn(`[minio] weight sync skipped, using local files: ${e.message}`);
  }

  const header = [
    `[launcher] job #${jobId} — ${type} — ${new Date().toISOString()}`,
    `[launcher] $ python ${path.basename(scriptPath)} ${args.join(" ")}`,
    ""
  ].join("\n");
  fs.writeFileSync(lp, header);

  const python = process.platform === "win32" ? "python" : "python3";
  const child = spawn(python, ["-u", scriptPath, ...args], {
    cwd,
    stdio: ["ignore", "pipe", "pipe"],
    env: {
      ...process.env,
      PYTHONUNBUFFERED: "1",
      PYTHONIOENCODING: "utf-8",
      JOB_OUTPUT_DIR: outDir,
      JOB_ID: String(jobId),
    },
  });

  procs.set(jobId, child);
  console.log(`[job ${jobId}] started PID ${child.pid}`);

  child.stdout.on("data", d => {
    const t = d.toString();
    writeLog(lp, t);
    writeLogMinio(jobId, t);
    parseProgress(t, jobId);
  });

  child.stderr.on("data", d => {
    const t = d.toString();
    writeLog(lp, t);
    writeLogMinio(jobId, t);
  });

  const cleanup = () => {
    // Only clean up the per-job output dir — do NOT delete the local log here,
    // since the final log flush to MinIO may still be in-flight (3s interval).
    // The local log file will be cleaned up after the interval flushes it.
    try {
      if (fs.existsSync(outDir)) {
        fs.rmSync(outDir, { recursive: true, force: true });
        console.log(`[cleanup] removed local output dir ${outDir}`);
      }
    } catch (e) { console.warn(`[cleanup] could not remove output dir:`, e.message); }

    // Clear the buffer — the interval will flush whatever is pending once more
    // then the key won't exist on the next tick.
    // We give it one extra flush cycle before deleting the local log.
    setTimeout(() => {
      logBuffers.delete(jobId);
      try {
        if (fs.existsSync(lp)) {
          fs.unlinkSync(lp);
          console.log(`[cleanup] removed local log ${lp}`);
        }
      } catch (e) { console.warn(`[cleanup] could not remove log:`, e.message); }
    }, 5000); // 5s — enough for the 3s interval to flush the final chunk
  };

  child.on("close", async (code) => {
    const endedAt = Date.now();
    const duration_ms = endedAt - startedAt;
    const row = dbGet(`SELECT status FROM jobs WHERE id = ?`, [jobId]);
    procs.delete(jobId);
    progress.delete(jobId);
    console.log(`[job ${jobId}] closed code=${code} status=${row?.status}`);

    if (row?.status === "cancelled") {
      dbRun(`UPDATE jobs SET ended_at=?, duration_ms=? WHERE id=?`, [endedAt, duration_ms, jobId]);
      writeLog(lp, `\n[CANCELLED] Duration: ${fmtMs(duration_ms)}\n`);
      writeLogMinio(jobId, `\n[CANCELLED] Duration: ${fmtMs(duration_ms)}\n`);
    } else if (code === 0) {
      dbRun(`UPDATE jobs SET status='done', exit_code=0, ended_at=?, duration_ms=? WHERE id=?`,
        [endedAt, duration_ms, jobId]);
      writeLog(lp, `\n[DONE] Duration: ${fmtMs(duration_ms)}\n`);
      writeLogMinio(jobId, `\n[DONE] Duration: ${fmtMs(duration_ms)}\n`);

      // Upload forecast image to MinIO if one was produced
      const jobRow = dbGet(`SELECT model_path FROM jobs WHERE id = ?`, [jobId]);
      const forecastFile = findForecastImage(jobId, jobRow?.model_path, startedAt);
      if (forecastFile) {
        try {
          await minioUpload(BUCKETS.outputs, `job_${jobId}/forecast.png`, forecastFile);
          console.log(`[minio] uploaded forecast for job ${jobId}`);
        } catch (e) { console.error(`[minio] forecast upload failed:`, e.message); }
      }
    } else if (code !== null) {
      const reason = `Exited with code ${code}`;
      dbRun(`UPDATE jobs SET status='failed', exit_code=?, fail_reason=?, ended_at=?, duration_ms=? WHERE id=?`,
        [code, reason, endedAt, duration_ms, jobId]);
      writeLog(lp, `\n[ERROR] ${reason}. Duration: ${fmtMs(duration_ms)}\n`);
      writeLogMinio(jobId, `\n[ERROR] ${reason}. Duration: ${fmtMs(duration_ms)}\n`);
    } else {
      dbRun(`UPDATE jobs SET status='cancelled', ended_at=?, duration_ms=? WHERE id=?`,
        [endedAt, duration_ms, jobId]);
      writeLog(lp, `\n[CANCELLED] Duration: ${fmtMs(duration_ms)}\n`);
      writeLogMinio(jobId, `\n[CANCELLED] Duration: ${fmtMs(duration_ms)}\n`);
    }

    cleanup();
  });

  child.on("error", err => {
    procs.delete(jobId);
    progress.delete(jobId);
    const endedAt = Date.now();
    dbRun(`UPDATE jobs SET status='failed', fail_reason=?, ended_at=?, duration_ms=? WHERE id=?`,
      [err.message, endedAt, endedAt - startedAt, jobId]);
    writeLog(lp, `\n[ERROR] ${err.message}\n`);
    writeLogMinio(jobId, `\n[ERROR] ${err.message}\n`);
    console.error(`[job ${jobId}] error: ${err.message}`);
    cleanup();
  });

  return jobId;
}

// ── REST endpoints ────────────────────────────────────────────────────────────

// GET /api/models
app.get("/api/models", (req, res) => {
  res.json(dbAll(`SELECT * FROM models ORDER BY name ASC`));
});

// POST /api/models  { name, path }
app.post("/api/models", async (req, res) => {
  const { name, path: mpath } = req.body;
  if (!name || !mpath) return res.status(400).json({ error: "name and path required" });
  try {
    const detected = detectModelConfig(mpath);
    if (!detected) return res.status(400).json({ error: "No recognised config found. Expected model.json, config.json, or model.py" });
  } catch (e) {
    return res.status(400).json({ error: e.message });
  }
  try {
    dbRun(`INSERT INTO models (name, path, created_at) VALUES (?, ?, ?)`, [name, mpath, Date.now()]);
    const row = dbGet(`SELECT * FROM models WHERE name = ?`, [name]);

    // Upload entire model folder to MinIO for future syncing
    const folderName = name.toLowerCase().replace(/\s+/g, "-");
    const files = fs.readdirSync(mpath);
    for (const file of files) {
      const fullPath = path.join(mpath, file);
      if (fs.statSync(fullPath).isFile()) {
        try {
          await minioUpload(BUCKETS.models, `${folderName}/${file}`, fullPath);
        } catch (e) {
          console.error(`[minio] failed to upload ${file}:`, e.message);
        }
      }
    }
    console.log(`[minio] uploaded model "${name}" to models/${folderName}/`);
    res.status(201).json(row);
  } catch (e) {
    res.status(409).json({ error: "Model name already exists" });
  }
});

// DELETE /api/models/:id
app.delete("/api/models/:id", (req, res) => {
  dbRun(`DELETE FROM models WHERE id = ?`, [parseInt(req.params.id)]);
  res.json({ ok: true });
});

// GET /api/datasets
app.get("/api/datasets", (req, res) => {
  res.json(dbAll(`SELECT * FROM datasets ORDER BY name ASC`));
});

// POST /api/datasets  { name, path }
app.post("/api/datasets", async (req, res) => {
  const { name, path: dpath = "" } = req.body;
  if (!name) return res.status(400).json({ error: "name required" });
  try {
    dbRun(`INSERT INTO datasets (name, path, created_at) VALUES (?, ?, ?)`, [name, dpath, Date.now()]);
    const row = dbGet(`SELECT * FROM datasets WHERE name = ?`, [name]);

    if (dpath && fs.existsSync(dpath)) {
      const stat = fs.statSync(dpath);
      const folderName = name.toLowerCase().replace(/\s+/g, "-");
      if (stat.isDirectory()) {
        const files = fs.readdirSync(dpath);
        for (const file of files) {
          const fullPath = path.join(dpath, file);
          if (fs.statSync(fullPath).isFile()) {
            try {
              await minioUpload(BUCKETS.datasets, `${folderName}/${file}`, fullPath);
            } catch (e) {
              console.error(`[minio] failed to upload dataset file ${file}:`, e.message);
            }
          }
        }
      } else {
        await minioUpload(BUCKETS.datasets, `${folderName}/${path.basename(dpath)}`, dpath);
      }
      console.log(`[minio] uploaded dataset "${name}" to datasets/${folderName}/`);
    }
    res.status(201).json(row);
  } catch (e) {
    res.status(409).json({ error: "Dataset name already exists" });
  }
});

// DELETE /api/datasets/:id
app.delete("/api/datasets/:id", (req, res) => {
  dbRun(`DELETE FROM datasets WHERE id = ?`, [parseInt(req.params.id)]);
  res.json({ ok: true });
});

// GET /api/dataset-files?path=
app.get("/api/dataset-files", (req, res) => {
  const { path: dpath } = req.query;
  if (!dpath) return res.json({ files: [] });
  try {
    if (!fs.existsSync(dpath)) return res.json({ files: [], error: "Path not found" });
    const stat = fs.statSync(dpath);
    if (stat.isDirectory()) {
      const entries = fs.readdirSync(dpath, { withFileTypes: true });
      const files = entries.filter(e => e.isFile()).map(e => e.name).sort();
      return res.json({ files });
    }
    return res.json({ files: [path.basename(dpath)] });
  } catch (e) {
    return res.json({ files: [], error: e.message });
  }
});

// GET /api/jobs
app.get("/api/jobs", (req, res) => {
  const q = (req.query.search || "").trim();
  const like = `%${q}%`;
  const jobs = q
    ? dbAll(`SELECT * FROM jobs WHERE model_name LIKE ? OR dataset LIKE ? OR type LIKE ? OR status LIKE ? ORDER BY started_at DESC LIMIT 200`,
        [like, like, like, like])
    : dbAll(`SELECT * FROM jobs ORDER BY started_at DESC LIMIT 200`);
  const now = Date.now();
  res.json({ jobs: jobs.map(j => enrichJob({ ...j, duration_ms: procs.has(j.id) ? now - j.started_at : j.duration_ms })) });
});

// GET /api/jobs/:id
app.get("/api/jobs/:id", (req, res) => {
  const id = parseInt(req.params.id);
  const job = dbGet(`SELECT * FROM jobs WHERE id = ?`, [id]);
  if (!job) return res.status(404).json({ error: "Not found" });
  try {
    const cfg = loadModelConfig(job.model_path);
    if (cfg) {
      job.train_script = cfg.train_script || null;
      job.run_script   = cfg.run_script   || null;
      job.dataset_param = cfg.dataset_param || null;
      job.model_type   = cfg.model_type   || "custom";
    }
  } catch (_) {}
  res.json(enrichJob({ ...job, duration_ms: procs.has(id) ? Date.now() - job.started_at : job.duration_ms }));
});

// GET /api/jobs/:id/log?offset=N
// FIX: Running jobs → local file (fast). Completed jobs → MinIO (persistent).
app.get("/api/jobs/:id/log", async (req, res) => {
  const id = parseInt(req.params.id);

  // Running — serve from local file for low-latency incremental reads
  if (procs.has(id)) {
    const job = dbGet(`SELECT log_file FROM jobs WHERE id = ?`, [id]);
    if (!job?.log_file || !fs.existsSync(job.log_file))
      return res.json({ log: "", size: 0 });
    try {
      const stat = fs.statSync(job.log_file);
      const offset = Math.max(0, parseInt(req.query.offset || "0"));
      if (offset >= stat.size) return res.json({ log: "", size: stat.size });
      const buf = Buffer.alloc(stat.size - offset);
      const fd = fs.openSync(job.log_file, "r");
      fs.readSync(fd, buf, 0, buf.length, offset);
      fs.closeSync(fd);
      return res.json({ log: buf.toString("utf8"), size: stat.size });
    } catch {
      return res.json({ log: "", size: 0 });
    }
  }

  // Completed — serve from MinIO (local file may have been cleaned up)
  try {
    const text = await minioGetString(BUCKETS.logs, `job_${id}.log`);
    return res.json({ log: text, size: Buffer.byteLength(text, "utf8") });
  } catch (_) {
    // MinIO not yet flushed or unavailable — fall back to local if it still exists
    const job = dbGet(`SELECT log_file FROM jobs WHERE id = ?`, [id]);
    if (job?.log_file && fs.existsSync(job.log_file)) {
      try {
        const text = fs.readFileSync(job.log_file, "utf8");
        return res.json({ log: text, size: Buffer.byteLength(text, "utf8") });
      } catch {}
    }
    return res.json({ log: "", size: 0 });
  }
});

// GET /api/jobs/:id/progress
app.get("/api/jobs/:id/progress", (req, res) => {
  const id = parseInt(req.params.id);
  const p = progress.get(id);
  res.json(p
    ? { percent: p.percent, detail: p.detail, running: procs.has(id) }
    : { percent: 0, detail: "", running: false });
});

// GET /api/validate?modelPath=
app.get("/api/validate", (req, res) => {
  const { modelPath } = req.query;
  if (!modelPath) return res.json({ valid: false, error: "No path" });
  try {
    const cfg = loadModelConfig(modelPath);
    if (!cfg) return res.json({ valid: false, error: "No recognised config found. Expected model.json, config.json with model_type, or model.py" });
    res.json({ valid: true, config: cfg });
  } catch (e) {
    res.json({ valid: false, error: e.message });
  }
});

// GET /api/pick-folder
app.get("/api/pick-folder", (req, res) => {
  let cmd;
  if (process.platform === "win32")
    cmd = `powershell -Command "Add-Type -AssemblyName System.Windows.Forms; $d = New-Object System.Windows.Forms.FolderBrowserDialog; if ($d.ShowDialog() -eq 'OK') { $d.SelectedPath }"`;
  else if (process.platform === "darwin")
    cmd = `osascript -e 'POSIX path of (choose folder)'`;
  else
    cmd = `zenity --file-selection --directory`;
  exec(cmd, (err, stdout) =>
    res.json(err || !stdout.trim() ? { cancelled: true } : { path: stdout.trim() }));
});

// GET /api/pick-file
app.get("/api/pick-file", (req, res) => {
  let cmd;
  if (process.platform === "win32")
    cmd = `powershell -Command "Add-Type -AssemblyName System.Windows.Forms; $d = New-Object System.Windows.Forms.OpenFileDialog; if ($d.ShowDialog() -eq 'OK') { $d.FileName }"`;
  else if (process.platform === "darwin")
    cmd = `osascript -e 'POSIX path of (choose file)'`;
  else
    cmd = `zenity --file-selection`;
  exec(cmd, (err, stdout) =>
    res.json(err || !stdout.trim() ? { cancelled: true } : { path: stdout.trim() }));
});

// POST /api/train
app.post("/api/train", async (req, res) => {
  try {
    let modelPath, modelName, datasetName = "";
    if (req.body.modelId) {
      const m = dbGet(`SELECT * FROM models WHERE id = ?`, [req.body.modelId]);
      if (!m) return res.status(400).json({ error: "Model not found" });
      modelPath = m.path; modelName = m.name;
    } else {
      modelPath = req.body.modelPath;
      if (!modelPath) return res.status(400).json({ error: "modelId or modelPath required" });
    }

    let datasetPath = "";
    if (req.body.datasetId) {
      const d = dbGet(`SELECT * FROM datasets WHERE id = ?`, [req.body.datasetId]);
      if (!d) return res.status(400).json({ error: "Dataset not found" });
      datasetName = d.name; datasetPath = d.path || d.name;
    } else if (req.body.datasetPath) {
      datasetPath = req.body.datasetPath; datasetName = datasetPath;
    }

    const cfg = loadModelConfig(modelPath);
    if (!cfg) return res.status(400).json({ error: "No recognised model config found" });
    if (!modelName) modelName = cfg.name;

    const scriptPath = path.join(modelPath, cfg.train_script);
    if (!fs.existsSync(scriptPath)) return res.status(400).json({ error: "train_script not found" });

    const extraArgs = req.body.extraArgs || "";
    const args = datasetPath ? [cfg.dataset_param || "--dataset", datasetPath] : [];
    if (extraArgs.trim()) args.push(...extraArgs.trim().split(/\s+/));

    if (!datasetName) {
      const dsMatch = extraArgs.match(/--dataset[= ](\S+)/);
      if (dsMatch) datasetName = dsMatch[1];
    }

    const jobId = await startJob("train", scriptPath, args, modelPath, {
      modelName, modelPath, dataset: datasetName, extraArgs,
    });
    res.status(202).json({ ok: true, jobId });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// POST /api/run
app.post("/api/run", async (req, res) => {
  try {
    let modelPath, modelName, datasetName = "";
    if (req.body.modelId) {
      const m = dbGet(`SELECT * FROM models WHERE id = ?`, [req.body.modelId]);
      if (!m) return res.status(400).json({ error: "Model not found" });
      modelPath = m.path; modelName = m.name;
    } else {
      modelPath = req.body.modelPath;
      if (!modelPath) return res.status(400).json({ error: "modelId or modelPath required" });
    }

    if (req.body.datasetId) {
      const d = dbGet(`SELECT * FROM datasets WHERE id = ?`, [req.body.datasetId]);
      if (!d) return res.status(400).json({ error: "Dataset not found" });
      datasetName = d.name;
    }

    const cfg = loadModelConfig(modelPath);
    if (!cfg) return res.status(400).json({ error: "No recognised model config found" });
    if (!modelName) modelName = cfg.name;

    const scriptPath = path.join(modelPath, cfg.run_script);
    if (!fs.existsSync(scriptPath)) return res.status(400).json({ error: "run_script not found" });

    const extraArgs = req.body.extraArgs || "";
    const args = [];
    if (cfg.model_param)
      args.push(cfg.model_param, cfg.model_path ? path.join(modelPath, cfg.model_path) : modelPath);
    if (extraArgs.trim()) args.push(...extraArgs.trim().split(/\s+/));

    if (!datasetName) {
      const dsMatch = extraArgs.match(/--dataset[= ](\S+)/);
      if (dsMatch) datasetName = dsMatch[1];
    }

    const jobId = await startJob("run", scriptPath, args, modelPath, {
      modelName, modelPath, dataset: datasetName, extraArgs,
    });
    res.status(202).json({ ok: true, jobId });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// POST /api/cancel
app.post("/api/cancel", (req, res) => {
  const id = parseInt(req.body.jobId);
  const child = procs.get(id);
  if (!child) return res.json({ ok: true, message: "Not running" });
  console.log(`[job ${id}] cancelling PID ${child.pid}`);
  dbRun(`UPDATE jobs SET status='cancelled', fail_reason='cancelled by user' WHERE id=?`, [id]);
  if (process.platform === "win32") {
    exec(`taskkill /F /T /PID ${child.pid}`, (err, stdout, stderr) => {
      console.log(`[job ${id}] taskkill: ${stdout} ${stderr}`);
    });
  } else {
    try { process.kill(-child.pid, "SIGTERM"); } catch (_) { child.kill("SIGTERM"); }
    setTimeout(() => { try { child.kill("SIGKILL"); } catch (_) {} }, 3000);
  }
  res.json({ ok: true });
});

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
    const candidates = [
      path.join(modelPath, "forecast.png"),
      path.join(modelPath, "outputs", "forecast.png"),
      path.join(modelPath, `forecast_${jobId}.png`),
    ];
    for (const p of candidates) {
      if (fs.existsSync(p)) {
        try {
          const st = fs.statSync(p);
          if (!startedAt || st.mtimeMs >= startedAt - 1000) return p;
        } catch (_) {}
      }
    }
  }
  return null;
}

// GET /api/jobs/:id/forecast.png
// FIX: Try MinIO first (works after cleanup), fall back to local (works during job)
app.get("/api/jobs/:id/forecast.png", async (req, res) => {
  const id = parseInt(req.params.id);
  const job = dbGet(`SELECT model_path, started_at FROM jobs WHERE id = ?`, [id]);
  if (!job) return res.status(404).end();

  // Try MinIO first — persistent after local cleanup
  try {
    const stream = await minioClient.getObject(BUCKETS.outputs, `job_${id}/forecast.png`);
    res.setHeader("Content-Type", "image/png");
    res.setHeader("Cache-Control", "no-store");
    return stream.pipe(res);
  } catch (_) {}

  // Fall back to local file (job still running, or MinIO upload pending)
  const file = findForecastImage(id, job.model_path, job.started_at);
  if (!file) return res.status(404).end();
  res.setHeader("Content-Type", "image/png");
  res.setHeader("Cache-Control", "no-store");
  fs.createReadStream(file).pipe(res);
});

// ── Dataset preview ───────────────────────────────────────────────────────────
const PY_PREVIEW = path.join(__dirname, "_dataset_preview.py");

app.get("/api/dataset-preview", (req, res) => {
  const id = parseInt(req.query.datasetId);
  const limit = Math.min(200, Math.max(1, parseInt(req.query.limit || "20")));
  const ds = dbGet(`SELECT * FROM datasets WHERE id = ?`, [id]);
  if (!ds) return res.status(404).json({ error: "Dataset not found" });
  if (!fs.existsSync(PY_PREVIEW)) {
    return res.status(500).json({ error: `_dataset_preview.py missing next to server.js (${PY_PREVIEW})` });
  }
  const python = process.platform === "win32" ? "python" : "python3";
  const child = spawn(python, [PY_PREVIEW, ds.name, ds.path || "", String(limit)], {
    env: { ...process.env, PYTHONIOENCODING: "utf-8" },
  });
  let out = "", err = "";
  child.stdout.on("data", d => out += d.toString());
  child.stderr.on("data", d => err += d.toString());
  child.on("close", code => {
    if (code !== 0) return res.status(500).json({ error: (err || `exit ${code}`).trim() });
    try { res.json(JSON.parse(out)); }
    catch (e) { res.status(500).json({ error: "Bad preview output", raw: out.slice(0, 500) }); }
  });
  child.on("error", e => res.status(500).json({ error: e.message }));
});

// ── Boot ──────────────────────────────────────────────────────────────────────
const PORT = 5557;
initDb().then(() => {
  const orphans = dbAll(`SELECT id FROM jobs WHERE status='running'`);
  if (orphans.length) {
    console.log(`[db] marking ${orphans.length} orphaned job(s) as failed`);
    dbRun(`UPDATE jobs SET status='failed', fail_reason='server restarted', ended_at=?, duration_ms=0 WHERE status='running'`,
      [Date.now()]);
  }
  app.listen(PORT, "0.0.0.0", () =>
    console.log(`AI Launcher → http://localhost:${PORT}`));
});