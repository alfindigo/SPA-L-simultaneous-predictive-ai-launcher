const express = require("express");
const { spawn, exec } = require("child_process");
const path = require("path");
const fs = require("fs");

// ── SQLite via sql.js ─────────────────────────────────────────────────────────
const initSqlJs = require("sql.js");
const DB_PATH = path.join(__dirname, "jobs.db");
let db;

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

// Returns the last inserted row id after an INSERT
function dbInsert(sql, params = []) {
  db.run(sql, params);
  // sql.js way to get last insert rowid
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

function writeLog(file, text) {
  if (!file) return;
  try { fs.appendFileSync(file, text); } catch (_) { }
}

// ── Runtime state ─────────────────────────────────────────────────────────────
// keyed by integer jobId
const procs = new Map();  // jobId → child process
const progress = new Map();  // jobId → { percent, detail, totalEpochs }

// ── Express ───────────────────────────────────────────────────────────────────
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));
app.get("/", (req, res) => res.redirect("/list.html"));

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

  // run_wrapper: "[run] Series 3/5 done (60.0%)"
  const sm = text.match(/Series\s+(\d+)\/(\d+)\s+done\s+\((\d+(?:\.\d+)?)%\)/);
  if (sm) {
    p.percent = Math.min(100, parseFloat(sm[3]));
    p.detail = `Series ${sm[1]} / ${sm[2]}`;
  }

  // Lightning: "Epoch 3: |██| 45/100"
  const em = text.match(/Epoch\s+(\d+):\s+\S+\s+(\d+)\/(\d+)/);
  if (em) {
    const ep = parseInt(em[1]), step = parseInt(em[2]), tot = parseInt(em[3]);
    p.percent = Math.min(99, Math.round(((ep + step / tot) / p.totalEpochs) * 100));
    p.detail = `Epoch ${ep + 1} / ${p.totalEpochs}`;
  }

  const me = text.match(/Max epochs:\s+(\d+)/);
  if (me) p.totalEpochs = parseInt(me[1]);
}

// ── Core job runner ───────────────────────────────────────────────────────────
function startJob(type, scriptPath, args, cwd, meta) {
  const startedAt = Date.now();

  const jobId = dbInsert(
    `INSERT INTO jobs (type, model_name, model_path, dataset, extra_args, status, started_at)
     VALUES (?, ?, ?, ?, ?, 'running', ?)`,
    [type, meta.modelName, meta.modelPath, meta.dataset || "", meta.extraArgs || "", startedAt]
  );

  const lp = logPath(jobId);
  dbRun(`UPDATE jobs SET log_file = ? WHERE id = ?`, [lp, jobId]);

  progress.set(jobId, { percent: 0, detail: "", totalEpochs: 50 });

  // Write header immediately so first poll sees content
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
    env: { ...process.env, PYTHONUNBUFFERED: "1", PYTHONIOENCODING: "utf-8" },
  });

  procs.set(jobId, child);
  console.log(`[job ${jobId}] started PID ${child.pid}`);

  child.stdout.on("data", d => {
    const t = d.toString();
    writeLog(lp, t);
    parseProgress(t, jobId);
  });

  child.stderr.on("data", d => {
    writeLog(lp, d.toString());
  });

  child.on("close", code => {
    const endedAt = Date.now();
    const duration_ms = endedAt - startedAt;

    // Read status BEFORE removing from procs
    const row = dbGet(`SELECT status FROM jobs WHERE id = ?`, [jobId]);

    procs.delete(jobId);
    progress.delete(jobId);

    console.log(`[job ${jobId}] closed code=${code} status=${row?.status}`);

    if (row?.status === "cancelled") {
      // Already marked — just write duration
      dbRun(`UPDATE jobs SET ended_at=?, duration_ms=? WHERE id=?`,
        [endedAt, duration_ms, jobId]);
      writeLog(lp, `\n[CANCELLED] Duration: ${fmtMs(duration_ms)}\n`);
    } else if (code === 0) {
      dbRun(`UPDATE jobs SET status='done', exit_code=0, ended_at=?, duration_ms=? WHERE id=?`,
        [endedAt, duration_ms, jobId]);
      writeLog(lp, `\n[DONE] Duration: ${fmtMs(duration_ms)}\n`);
    } else if (code !== null) {
      const reason = `Exited with code ${code}`;
      dbRun(`UPDATE jobs SET status='failed', exit_code=?, fail_reason=?, ended_at=?, duration_ms=? WHERE id=?`,
        [code, reason, endedAt, duration_ms, jobId]);
      writeLog(lp, `\n[ERROR] ${reason}. Duration: ${fmtMs(duration_ms)}\n`);
    } else {
      // code===null means killed — treat as cancelled if not already
      dbRun(`UPDATE jobs SET status='cancelled', ended_at=?, duration_ms=? WHERE id=?`,
        [endedAt, duration_ms, jobId]);
      writeLog(lp, `\n[CANCELLED] Duration: ${fmtMs(duration_ms)}\n`);
    }
  });

  child.on("error", err => {
    procs.delete(jobId);
    progress.delete(jobId);
    const endedAt = Date.now();
    dbRun(`UPDATE jobs SET status='failed', fail_reason=?, ended_at=?, duration_ms=? WHERE id=?`,
      [err.message, endedAt, endedAt - startedAt, jobId]);
    writeLog(lp, `\n[ERROR] ${err.message}\n`);
    console.error(`[job ${jobId}] error: ${err.message}`);
  });

  return jobId;
}

// ── REST endpoints ────────────────────────────────────────────────────────────

// GET /api/jobs  — list all jobs (with optional ?search=)
app.get("/ai/jobs", (req, res) => {
  const q = (req.query.search || "").trim();
  const like = `%${q}%`;
  const jobs = q
    ? dbAll(`SELECT * FROM jobs WHERE model_name LIKE ? OR dataset LIKE ? OR type LIKE ? OR status LIKE ? ORDER BY started_at DESC LIMIT 200`,
      [like, like, like, like])
    : dbAll(`SELECT * FROM jobs ORDER BY started_at DESC LIMIT 200`);

  const now = Date.now();
  res.json({ jobs: jobs.map(j => enrichJob({ ...j, duration_ms: procs.has(j.id) ? now - j.started_at : j.duration_ms })) });
});

// GET /api/jobs/:id  — single job detail
app.get("/ai/jobs/:id", (req, res) => {
  const id = parseInt(req.params.id);
  const job = dbGet(`SELECT * FROM jobs WHERE id = ?`, [id]);
  if (!job) return res.status(404).json({ error: "Not found" });

  // Attach model script info if available
  try {
    const cfg = JSON.parse(fs.readFileSync(path.join(job.model_path, "model.json"), "utf8"));
    job.train_script = cfg.train_script || null;
    job.run_script = cfg.run_script || null;
    job.dataset_param = cfg.dataset_param || null;
  } catch (_) { }

  res.json(enrichJob({ ...job, duration_ms: procs.has(id) ? Date.now() - job.started_at : job.duration_ms }));
});

// GET /api/jobs/:id/log?offset=N  — incremental log fetch
app.get("/ai/jobs/:id/log", (req, res) => {
  const job = dbGet(`SELECT log_file FROM jobs WHERE id = ?`, [req.params.id]);
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
    res.json({ log: buf.toString("utf8"), size: stat.size });
  } catch {
    res.json({ log: "", size: 0 });
  }
});

// GET /api/jobs/:id/progress
app.get("/ai/progress/:id", (req, res) => {
  const id = parseInt(req.params.id);
  const p = progress.get(id);
  res.json(p
    ? { percent: p.percent, detail: p.detail, busy: procs.has(id) }
    : { percent: 0, detail: "", busy: false });
});

// GET /api/validate?modelPath=
app.get("/ai/validate", (req, res) => {
  const { modelPath } = req.query;
  if (!modelPath) return res.json({ valid: false, error: "No path" });
  try {
    const cfgPath = path.join(modelPath, "model.json");
    if (!fs.existsSync(cfgPath)) return res.json({ valid: false, error: "model.json not found" });
    const cfg = JSON.parse(fs.readFileSync(cfgPath, "utf8"));
    const missing = ["name", "train_script", "run_script"].filter(k => !cfg[k]);
    if (missing.length) return res.json({ valid: false, error: `Missing: ${missing.join(", ")}` });
    res.json({ valid: true, config: cfg });
  } catch { res.json({ valid: false, error: "Invalid model.json" }); }
});

// GET /api/pick-folder
app.get("/pick-folder", (req, res) => {
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
app.get("/pick-file", (req, res) => {
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
app.post("/ai/train", (req, res) => {
  const { modelPath, datasetPath, extraArgs } = req.body;
  if (!modelPath) return res.status(400).json({ error: "modelPath required" });
  try {
    const cfg = JSON.parse(fs.readFileSync(path.join(modelPath, "model.json"), "utf8"));
    const scriptPath = path.join(modelPath, cfg.train_script);
    if (!fs.existsSync(scriptPath)) return res.status(400).json({ error: "train_script not found" });

    const args = datasetPath ? [cfg.dataset_param || "--dataset", datasetPath] : [];
    if (extraArgs?.trim()) args.push(...extraArgs.trim().split(/\s+/));

    const dsMatch = extraArgs?.match(/--dataset[= ](\S+)/);
    const dataset = datasetPath || (dsMatch ? dsMatch[1] : "");

    const jobId = startJob("train", scriptPath, args, modelPath, {
      modelName: cfg.name, modelPath, dataset, extraArgs: extraArgs || "",
    });
    res.status(202).json({ ok: true, jobId });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// POST /api/run
app.post("/ai/run", (req, res) => {
  const { modelPath, extraArgs } = req.body;
  if (!modelPath) return res.status(400).json({ error: "modelPath required" });
  try {
    const cfg = JSON.parse(fs.readFileSync(path.join(modelPath, "model.json"), "utf8"));
    const scriptPath = path.join(modelPath, cfg.run_script);
    if (!fs.existsSync(scriptPath)) return res.status(400).json({ error: "run_script not found" });

    const args = [];
    if (cfg.model_param)
      args.push(cfg.model_param, path.join(modelPath, cfg.model_path || "lag-llama.ckpt"));
    if (extraArgs?.trim()) args.push(...extraArgs.trim().split(/\s+/));

    const dsMatch = extraArgs?.match(/--dataset[= ](\S+)/);
    const dataset = dsMatch ? dsMatch[1] : "";

    const jobId = startJob("run", scriptPath, args, modelPath, {
      modelName: cfg.name, modelPath, dataset, extraArgs: extraArgs || "",
    });
    res.status(202).json({ ok: true, jobId });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// POST /api/cancel  { jobId }
app.post("/ai/cancel", (req, res) => {
  const id = parseInt(req.body.jobId);
  const child = procs.get(id);

  if (!child) return res.json({ ok: true, message: "Not running" });

  console.log(`[job ${id}] cancelling PID ${child.pid}`);

  // Mark BEFORE kill so close handler sees cancelled status
  dbRun(`UPDATE jobs SET status='cancelled', fail_reason='cancelled by user' WHERE id=?`, [id]);

  if (process.platform === "win32") {
    // taskkill /F forces, /T kills the whole tree (Python + children)
    exec(`taskkill /F /T /PID ${child.pid}`, (err, stdout, stderr) => {
      console.log(`[job ${id}] taskkill: ${stdout} ${stderr}`);
    });
  } else {
    try { process.kill(-child.pid, "SIGTERM"); } catch (_) { child.kill("SIGTERM"); }
    setTimeout(() => { try { child.kill("SIGKILL"); } catch (_) { } }, 3000);
  }

  res.json({ ok: true });
});

// ── Boot ──────────────────────────────────────────────────────────────────────
const PORT = 5557;
initDb().then(() => {
  // Clean up orphaned running jobs from previous server session
  const orphans = dbAll(`SELECT id FROM jobs WHERE status='running'`);
  if (orphans.length) {
    console.log(`[db] marking ${orphans.length} orphaned job(s) as failed`);
    dbRun(`UPDATE jobs SET status='failed', fail_reason='server restarted', ended_at=?, duration_ms=0 WHERE status='running'`,
      [Date.now()]);
  }
  app.listen(PORT, "0.0.0.0", () =>
    console.log(`AI Launcher → http://localhost:${PORT}`));
});