"""
dag_launcher_pipeline.py
Flow:
  1. Pull data from Postgres → export CSV → register in launcher
  2. If Postgres unreachable → fall back to GluonTS built-in
  3. Train → poll until done
  4. Run   → poll until done
  5. Report metrics to /api/accuracy

All config comes from environment variables — nothing hardcoded.
Key env vars (set in docker-compose via .env):
  LAUNCHER_URL        e.g. http://host.docker.internal:5557
  DATA_DB_CONN        SQLAlchemy conn string for data Postgres
  HOST_DAGS_PATH      Host-side path that maps to /opt/airflow/dags
  DEFAULT_MODEL_NAME  Model to use for scheduled (non-manual) runs
"""
import os
import re
import time
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

LAUNCHER_URL       = os.environ.get("LAUNCHER_URL",        "http://host.docker.internal:5557")
DATA_DB_CONN       = os.environ.get("DATA_DB_CONN",        "")
HOST_DAGS_PATH     = os.environ.get("HOST_DAGS_PATH",      "")
DEFAULT_MODEL_NAME = os.environ.get("DEFAULT_MODEL_NAME",  "")
POLL_INTERVAL      = 10
JOB_TIMEOUT        = 7200

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def _conf(ctx, key, default=""):
    return (ctx["dag_run"].conf or {}).get(key, default)

def _get_model_id(model_name):
    resp = requests.get(f"{LAUNCHER_URL}/api/models", timeout=10)
    resp.raise_for_status()
    match = next((m for m in resp.json() if m["name"] == model_name), None)
    if not match:
        available = [m["name"] for m in resp.json()]
        raise ValueError(f"Model '{model_name}' not found. Available: {available}")
    return match["id"]

def _poll_job(job_id, label="job"):
    deadline = time.time() + JOB_TIMEOUT
    while time.time() < deadline:
        resp = requests.get(f"{LAUNCHER_URL}/api/jobs/{job_id}", timeout=10)
        resp.raise_for_status()
        job    = resp.json()
        status = job["status"]
        print(f"[{label}] job {job_id} status={status}")
        if status == "done":
            return job
        if status in ("failed", "cancelled"):
            raise RuntimeError(f"{label} job {job_id} ended '{status}': {job.get('fail_reason', '')}")
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"{label} job {job_id} did not finish within {JOB_TIMEOUT}s")

def _upsert_dataset(name, path=""):
    resp = requests.get(f"{LAUNCHER_URL}/api/datasets", timeout=10)
    resp.raise_for_status()
    existing = next((d for d in resp.json() if d["name"] == name), None)
    if existing:
        requests.patch(f"{LAUNCHER_URL}/api/datasets/{existing['id']}", json={"path": path}, timeout=10)
        print(f"[dataset] Updated '{name}' id={existing['id']} path={path!r}")
        return existing["id"]
    r = requests.post(f"{LAUNCHER_URL}/api/datasets", json={"name": name, "path": path}, timeout=10)
    r.raise_for_status()
    did = r.json()["id"]
    print(f"[dataset] Registered '{name}' id={did} path={path!r}")
    return did

def task_build_dataset(**ctx):
    ti           = ctx["ti"]
    dataset_name = _conf(ctx, "dataset_name", "energy_data")
    table        = _conf(ctx, "table",        "energy_consumption")
    gluonts_name = _conf(ctx, "gluonts_name", "electricity")

    if DATA_DB_CONN:
        try:
            from sqlalchemy import create_engine, text
            import pandas as pd
            print(f"[dataset] Connecting to Postgres, querying '{table}'...")
            engine = create_engine(DATA_DB_CONN)
            with engine.connect() as conn:
                df_raw = pd.read_sql(
                    text("SELECT timestamp, household, consumption FROM energy_consumption ORDER BY timestamp, household ASC"),
                    conn,
                )
                df = df_raw.pivot(index="timestamp", columns="household", values="consumption")
                df.columns = [f"household_{int(c)}" for c in df.columns]
                df = df.reset_index()
            if df.empty:
                raise ValueError(f"Table '{table}' returned no rows.")
            print(f"[dataset] Got {len(df)} rows, {len(df.columns)-1} series.")
            container_dags = os.path.dirname(os.path.abspath(__file__))
            csv_filename   = f"{dataset_name}.csv"
            container_csv  = os.path.join(container_dags, csv_filename)
            df.to_csv(container_csv, index=False)
            print(f"[dataset] Wrote CSV → {container_csv}")
            host_csv = os.path.join(HOST_DAGS_PATH, csv_filename) if HOST_DAGS_PATH else container_csv
            print(f"[dataset] Host-side CSV path: {host_csv}")
            dataset_id = _upsert_dataset(dataset_name, host_csv)
            ti.xcom_push(key="dataset_id",   value=dataset_id)
            ti.xcom_push(key="dataset_name", value=dataset_name)
            ti.xcom_push(key="row_count",    value=len(df))
            return
        except Exception as exc:
            print(f"[dataset] Postgres failed: {exc}")
            print(f"[dataset] Falling back to GluonTS built-in '{gluonts_name}'")

    if not gluonts_name:
        raise ValueError("No reachable Postgres and no gluonts_name provided.")
    print(f"[dataset] Using GluonTS built-in: '{gluonts_name}'")
    dataset_id = _upsert_dataset(gluonts_name, "")
    ti.xcom_push(key="dataset_id",   value=dataset_id)
    ti.xcom_push(key="dataset_name", value=gluonts_name)
    ti.xcom_push(key="row_count",    value=0)

def task_train(**ctx):
    ti         = ctx["ti"]
    model_name = _conf(ctx, "model_name") or DEFAULT_MODEL_NAME
    if not model_name:
        raise ValueError("model_name is required — set DEFAULT_MODEL_NAME env var or pass in trigger conf.")
    dataset_id = ti.xcom_pull(key="dataset_id", task_ids="build_dataset")
    model_id   = _get_model_id(model_name)
    print(f"[train] Starting — model={model_name} dataset_id={dataset_id}")
    resp = requests.post(f"{LAUNCHER_URL}/api/train", json={"modelId": model_id, "datasetId": dataset_id}, timeout=30)
    resp.raise_for_status()
    job_id = resp.json()["jobId"]
    print(f"[train] Job {job_id} started")
    ti.xcom_push(key="train_job_id", value=job_id)
    _poll_job(job_id, label="train")
    print(f"[train] Job {job_id} complete ✓")

def task_run(**ctx):
    ti         = ctx["ti"]
    model_name = _conf(ctx, "model_name") or DEFAULT_MODEL_NAME
    if not model_name:
        raise ValueError("model_name is required — set DEFAULT_MODEL_NAME env var or pass in trigger conf.")
    dataset_id = ti.xcom_pull(key="dataset_id", task_ids="build_dataset")
    model_id   = _get_model_id(model_name)
    print(f"[run] Starting — model={model_name} dataset_id={dataset_id}")
    resp = requests.post(f"{LAUNCHER_URL}/api/run", json={"modelId": model_id, "datasetId": dataset_id}, timeout=30)
    resp.raise_for_status()
    job_id = resp.json()["jobId"]
    print(f"[run] Job {job_id} started")
    ti.xcom_push(key="run_job_id", value=job_id)
    _poll_job(job_id, label="run")
    print(f"[run] Job {job_id} complete ✓")

def task_report(**ctx):
    ti         = ctx["ti"]
    model_name = _conf(ctx, "model_name") or DEFAULT_MODEL_NAME or "unknown"
    run_job_id = ti.xcom_pull(key="run_job_id", task_ids="run")
    row_count  = ti.xcom_pull(key="row_count",  task_ids="build_dataset") or 0
    if not run_job_id:
        print("[report] No run_job_id available, skipping.")
        return
    try:
        resp = requests.get(f"{LAUNCHER_URL}/api/jobs/{run_job_id}/log", timeout=15)
        resp.raise_for_status()
        log_text = resp.json().get("log", "")
        smape    = re.search(r'\[SMAPE\]\s+([\d.]+)',    log_text)
        accuracy = re.search(r'\[ACCURACY\]\s+([\d.]+)', log_text)
        rmse     = re.search(r'\[RMSE\]\s+([\d.]+)',     log_text)
        smape_val    = float(smape.group(1))    if smape    else None
        accuracy_val = float(accuracy.group(1)) if accuracy else None
        rmse_val     = float(rmse.group(1))     if rmse     else None
        if accuracy_val is None and smape_val is not None:
            accuracy_val = round(max(0, min(100, 100 - smape_val)), 2)
        requests.post(f"{LAUNCHER_URL}/api/accuracy", json={
            "model_name":   model_name,
            "dataset_name": ti.xcom_pull(key="dataset_name", task_ids="build_dataset"),
            "smape":        smape_val,
            "rmse":         rmse_val,
            "accuracy":     accuracy_val,
            "job_id":       run_job_id,
            "train_job_id": ti.xcom_pull(key="train_job_id", task_ids="train"),
        }, timeout=10)
        print("=" * 50)
        print(f"[report] model='{model_name}' rows={row_count} smape={smape_val} rmse={rmse_val} accuracy={accuracy_val}%")
        print("=" * 50)
    except Exception as exc:
        print(f"[report] Could not fetch/post metrics: {exc}")

with DAG(
    dag_id="launcher_pipeline",
    default_args=default_args,
    description="Postgres → CSV → register → train → run → report",
    schedule_interval=os.environ.get("PIPELINE_CRON", "0 9 * * *"),
    start_date=datetime(2026, 5, 10),
    catchup=False,
    tags=["launcher", "timeseries"],
    params={
        "model_name":   "",
        "dataset_name": "energy_data",
        "table":        "energy_consumption",
        "gluonts_name": "electricity",
    },
) as dag:
    build_dataset = PythonOperator(task_id="build_dataset", python_callable=task_build_dataset)
    train         = PythonOperator(task_id="train",         python_callable=task_train)
    run           = PythonOperator(task_id="run",           python_callable=task_run)
    report        = PythonOperator(task_id="report",        python_callable=task_report)
    build_dataset >> train >> run >> report