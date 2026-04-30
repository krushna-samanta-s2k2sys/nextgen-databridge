#!/usr/bin/env python3
"""
setup_databases.py
Restores WideWorldImporters from S3 and creates TargetDB with all ETL output tables.

Required environment variables:
  SQLSERVER_HOST      - RDS SQL Server endpoint
  MSSQL_PASSWORD      - sqladmin password
  ARTIFACTS_BUCKET    - S3 bucket to stage the .bak file
  AWS_DEFAULT_REGION  - e.g. us-east-1
  AWS_ACCOUNT_ID      - AWS account ID (for S3 ARN)
"""
import os
import sys
import time
import urllib.request
import logging

import boto3
import pymssql

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("setup_databases")

HOST       = os.environ["SQLSERVER_HOST"]
PASSWORD   = os.environ["MSSQL_PASSWORD"]
BUCKET     = os.environ["ARTIFACTS_BUCKET"]
REGION     = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
ACCOUNT_ID = os.environ["AWS_ACCOUNT_ID"]

WWI_BAK_KEY = "wwi/WideWorldImporters-Full.bak"
WWI_BAK_URL = (
    "https://github.com/microsoft/sql-server-samples/releases/download/"
    "wide-world-importers-v1.0/WideWorldImporters-Full.bak"
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def connect(database="master"):
    return pymssql.connect(
        server=HOST, port=1433,
        user="sqladmin", password=PASSWORD,
        database=database, as_dict=True,
        autocommit=True,
        login_timeout=30, timeout=300,
    )


def db_exists(name: str) -> bool:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DB_ID(%s) AS db_id", (name,))
            row = cur.fetchone()
            return row and row["db_id"] is not None


def exec_script(path: str, database="master"):
    """Execute a .sql file, splitting on GO statements."""
    with open(path) as f:
        sql = f.read()

    batches = [b.strip() for b in sql.split("\nGO") if b.strip()]
    with connect(database) as conn:
        with conn.cursor() as cur:
            for batch in batches:
                if batch:
                    cur.execute(batch)


# ── Step 1: TargetDB ──────────────────────────────────────────────────────────

def setup_targetdb():
    log.info("Setting up TargetDB …")
    script = os.path.join(os.path.dirname(__file__), "sql", "setup_targetdb.sql")
    exec_script(script, database="master")
    log.info("TargetDB setup complete.")


# ── Step 2: WideWorldImporters restore ───────────────────────────────────────

def upload_bak_to_s3():
    s3 = boto3.client("s3", region_name=REGION)

    # Skip if already uploaded
    try:
        s3.head_object(Bucket=BUCKET, Key=WWI_BAK_KEY)
        log.info("WideWorldImporters-Full.bak already in S3 — skipping upload.")
        return
    except s3.exceptions.ClientError:
        pass

    local_bak = "/tmp/WideWorldImporters-Full.bak"
    log.info(f"Downloading WideWorldImporters-Full.bak ({WWI_BAK_URL}) …")
    urllib.request.urlretrieve(WWI_BAK_URL, local_bak)
    log.info("Upload to S3 …")
    s3.upload_file(local_bak, BUCKET, WWI_BAK_KEY)
    log.info(f"Uploaded to s3://{BUCKET}/{WWI_BAK_KEY}")


def restore_wwi():
    if db_exists("WideWorldImporters"):
        log.info("WideWorldImporters already exists — skipping restore.")
        return

    upload_bak_to_s3()

    s3_arn = f"arn:aws:s3:::{BUCKET}/{WWI_BAK_KEY}"
    log.info(f"Initiating RDS restore from {s3_arn} …")

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "EXEC msdb.dbo.rds_restore_database "
                "@restore_db_name = 'WideWorldImporters', "
                "@s3_arn_to_restore_from = %s",
                (s3_arn,),
            )

    log.info("Restore task submitted. Polling for completion (timeout 30 min) …")
    wait_for_restore()


def wait_for_restore(timeout_secs=1800, poll_secs=20):
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT TOP 1 lifecycle, task_type, task_info,
                                 [% complete] AS pct_complete
                    FROM   msdb.dbo.rds_fn_task_status(NULL, 0)
                    WHERE  task_type = 'RESTORE_DB'
                    ORDER  BY created_at DESC
                """)
                row = cur.fetchone()

        if not row:
            log.info("  No restore task found yet — waiting …")
        else:
            status = row["lifecycle"]
            pct    = row.get("pct_complete") or 0
            log.info(f"  Restore status: {status} ({pct:.1f}% complete)")
            if status == "SUCCESS":
                log.info("WideWorldImporters restore SUCCESS.")
                return
            if status == "ERROR":
                raise RuntimeError(
                    f"WideWorldImporters restore FAILED: {row.get('task_info')}"
                )

        time.sleep(poll_secs)

    raise TimeoutError("WideWorldImporters restore did not complete within 30 minutes.")


# ── Entrypoint ────────────────────────────────────────────────────────────────

def main():
    log.info(f"Target SQL Server: {HOST}")

    setup_targetdb()
    restore_wwi()

    log.info("Database setup finished successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error(f"Setup failed: {e}")
        sys.exit(1)
