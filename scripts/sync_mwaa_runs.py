#!/usr/bin/env python3
"""
sync_mwaa_runs.py
Fetches recent DAG runs from MWAA and writes them to the pipeline_runs table.

Environment variables (all required):
  MWAA_HOST      - MWAA webserver hostname (no https://)
  MWAA_TOKEN     - MWAA web login token (from aws mwaa create-web-login-token)
  DB_URL         - PostgreSQL connection URL
  PIPELINE_IDS   - JSON array of pipeline IDs, e.g. '["wwi_sales_etl", ...]'
"""
import json
import os
import sys
import uuid
import urllib.request

import psycopg2

mwaa_host    = os.environ["MWAA_HOST"]
token        = os.environ["MWAA_TOKEN"]
db_url       = os.environ["DB_URL"].replace("postgresql+asyncpg://", "postgresql://")
pipeline_ids = json.loads(os.environ["PIPELINE_IDS"])

STATUS_MAP = {
    "success": "success",
    "failed":  "failed",
    "running": "running",
    "queued":  "queued",
}

conn = psycopg2.connect(db_url)
cur  = conn.cursor()
total = 0

for pipeline_id in pipeline_ids:
    url = (
        f"https://{mwaa_host}/api/v1/dags/{pipeline_id}"
        f"/dagRuns?limit=50&order_by=-start_date"
    )
    req = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {token}"}
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as exc:
        print(f"  Could not fetch runs for {pipeline_id}: {exc}")
        continue

    runs = data.get("dag_runs", [])
    print(f"  {pipeline_id}: {len(runs)} run(s) found in MWAA")

    for run in runs:
        run_id   = run.get("dag_run_id", "")
        state    = run.get("state", "success")
        start    = run.get("start_date") or None
        end      = run.get("end_date") or None
        run_type = run.get("run_type", "scheduled")

        if not run_id:
            continue

        status       = STATUS_MAP.get(state, "success")
        trigger_type = "manual" if "manual" in run_type.lower() else "scheduled"

        try:
            cur.execute(
                """
                INSERT INTO pipeline_runs
                    (id, run_id, pipeline_id, status, trigger_type, start_time, end_time)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO NOTHING
                """,
                (str(uuid.uuid4()), run_id, pipeline_id, status, trigger_type, start, end),
            )
            total += 1
        except Exception as exc:
            print(f"    Insert error for {run_id}: {exc}")
            conn.rollback()

conn.commit()
cur.close()
conn.close()
print(f"Synced {total} run(s) total.")
