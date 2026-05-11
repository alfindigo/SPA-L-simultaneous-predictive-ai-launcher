"""
dag_eia_ingest.py
Fetches latest hourly electricity demand from the EIA API and appends
new rows to energy_consumption in data-db.

All config from environment — nothing hardcoded.
Key env vars (set in docker-compose via .env):
  EIA_API_KEY      from eia.gov/opendata
  DATA_DB_CONN     SQLAlchemy conn string for data Postgres
  EIA_INGEST_CRON  cron schedule (default: 30 8 * * *)
"""
import os
import requests
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

EIA_API_KEY  = os.environ.get("EIA_API_KEY",  "")
DATA_DB_CONN = os.environ.get("DATA_DB_CONN", "")

EIA_REGIONS = {
    1: "CAISO", 2: "NYISO", 3: "MISO",  4: "PJM",
    5: "ISNE",  6: "SWPP",  7: "ERCO",  8: "NEVP",
    9: "PACW",  10: "BPAT",
}
EIA_URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

def fetch_eia_region(region_code, start_dt, end_dt):
    if not EIA_API_KEY:
        raise ValueError("EIA_API_KEY environment variable is not set.")
    params = {
        "api_key":               EIA_API_KEY,
        "data[]":                "value",
        "facets[respondent][]":  region_code,
        "facets[type][]":        "D",
        "start":                 start_dt.strftime("%Y-%m-%dT%H"),
        "end":                   end_dt.strftime("%Y-%m-%dT%H"),
        "frequency":             "hourly",
        "sort[0][column]":       "period",
        "sort[0][direction]":    "asc",
        "length":                5000,
    }
    resp = requests.get(EIA_URL, params=params, timeout=30)
    resp.raise_for_status()
    rows = resp.json().get("response", {}).get("data", [])
    results = []
    for row in rows:
        try:
            ts  = datetime.strptime(row["period"], "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
            val = float(row["value"]) if row["value"] is not None else None
            if val is not None:
                results.append((ts, val))
        except (ValueError, KeyError):
            continue
    return results

def task_ingest(**ctx):
    if not DATA_DB_CONN:
        raise ValueError("DATA_DB_CONN is not set.")
    from sqlalchemy import create_engine, text
    engine = create_engine(DATA_DB_CONN)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT MAX(timestamp) FROM energy_consumption"))
        latest = result.scalar()
    if latest is None:
        start_dt = datetime.now(timezone.utc) - timedelta(days=30)
    else:
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        start_dt = latest + timedelta(hours=1)
    end_dt = datetime.now(timezone.utc)
    if start_dt >= end_dt:
        print(f"[ingest] DB already up to date (latest={latest}). Nothing to fetch.")
        return
    print(f"[ingest] Fetching EIA data from {start_dt} to {end_dt}")
    total_inserted = 0
    with engine.connect() as conn:
        for household_id, region_code in EIA_REGIONS.items():
            print(f"[ingest] Region {region_code} (household {household_id})...")
            try:
                rows = fetch_eia_region(region_code, start_dt, end_dt)
                if not rows:
                    print(f"[ingest]   No new data for {region_code}")
                    continue
                insert_rows = [{"ts": ts, "household": household_id, "consumption": round(val, 4)} for ts, val in rows]
                conn.execute(text("""
                    INSERT INTO energy_consumption (timestamp, household, consumption)
                    VALUES (:ts, :household, :consumption)
                    ON CONFLICT DO NOTHING
                """), insert_rows)
                conn.commit()
                print(f"[ingest]   Inserted {len(insert_rows)} rows for {region_code}")
                total_inserted += len(insert_rows)
            except Exception as e:
                print(f"[ingest]   ERROR fetching {region_code}: {e}")
                continue
    print(f"[ingest] Done. Total rows inserted: {total_inserted}")

with DAG(
    dag_id="eia_ingest",
    default_args=default_args,
    description="Fetch latest EIA hourly electricity demand → append to energy_consumption",
    schedule_interval=os.environ.get("EIA_INGEST_CRON", "30 8 * * *"),
    start_date=datetime(2026, 5, 10),
    catchup=False,
    tags=["launcher", "ingest", "eia"],
) as dag:
    ingest = PythonOperator(task_id="ingest", python_callable=task_ingest)