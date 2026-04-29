#!/usr/bin/env python3
"""
NextGenDatabridge EKS Job Entrypoint

Dispatches on OPERATION_TYPE env var:
  "transform" (default) — DuckDB SQL transform on staged DuckDB files from S3
  "extract"              — stream rows from a source DB directly to Parquet on S3
"""
import json
import logging
import os
import sys
import traceback
import uuid
from datetime import datetime, timezone

import boto3
import duckdb
import psycopg2
import pyarrow as pa

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("nextgen_databridge.eks_job")


# ─── AWS helpers ──────────────────────────────────────────────────────────────

def get_s3_client():
    kwargs = dict(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    endpoint = os.getenv("AWS_ENDPOINT_URL")
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)


def get_secret(connection_name: str) -> dict:
    """Fetch connection credentials from Secrets Manager."""
    secret_id = f"nextgen-databridge/connections/{connection_name}"
    client = boto3.client("secretsmanager", region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    response = client.get_secret_value(SecretId=secret_id)
    return json.loads(response["SecretString"])


# ─── DB connection factory ────────────────────────────────────────────────────

def get_db_connection(creds: dict):
    """Return a DB-API 2.0 connection built from a Secrets Manager creds dict."""
    conn_type = creds.get("conn_type", "mssql").lower()
    host      = creds["host"]
    port      = creds.get("port")
    login     = creds["login"]
    password  = creds["password"]
    schema    = creds.get("schema", "")

    if conn_type == "mssql":
        import pymssql
        return pymssql.connect(
            server=host,
            port=int(port or 1433),
            user=login,
            password=password,
            database=schema or "master",
            as_dict=False,
        )
    elif conn_type in ("postgresql", "postgres"):
        return psycopg2.connect(
            host=host,
            port=int(port or 5432),
            user=login,
            password=password,
            dbname=schema or "postgres",
        )
    elif conn_type == "mysql":
        import pymysql
        return pymysql.connect(
            host=host,
            port=int(port or 3306),
            user=login,
            password=password,
            database=schema or "",
        )
    else:
        raise ValueError(f"Unsupported conn_type: {conn_type!r}")


# ─── S3 / DuckDB helpers (used by transform) ──────────────────────────────────

def download_duckdb(s3_path: str, local_path: str):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3 = get_s3_client()
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    logger.info(f"Downloading s3://{bucket}/{key} -> {local_path}")
    s3.download_file(bucket, key, local_path)


def upload_duckdb(local_path: str, s3_path: str):
    s3 = get_s3_client()
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    logger.info(f"Uploading {local_path} -> s3://{bucket}/{key}")
    s3.upload_file(local_path, bucket, key)


# ─── Audit DB helper ──────────────────────────────────────────────────────────

def write_task_run(status: str, **fields):
    url = os.getenv("AUDIT_DB_URL", "").replace("postgresql+asyncpg://", "postgresql://")
    if not url:
        return
    try:
        conn = psycopg2.connect(url)
        cur  = conn.cursor()
        task_run_id = fields.get("task_run_id")
        if not task_run_id:
            return
        cur.execute("""
            INSERT INTO task_runs (
                id, task_run_id, run_id, pipeline_id, task_id, task_type,
                status, output_duckdb_path, output_table, output_row_count, output_size_bytes,
                start_time, end_time, duration_seconds,
                error_message, error_traceback, worker_host, created_at, updated_at
            ) VALUES (
                %s,%s,%s,%s,%s,%s, %s,%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, NOW(),NOW()
            )
            ON CONFLICT (task_run_id) DO UPDATE SET
                status=EXCLUDED.status,
                output_duckdb_path=EXCLUDED.output_duckdb_path,
                output_table=EXCLUDED.output_table,
                output_row_count=EXCLUDED.output_row_count,
                end_time=EXCLUDED.end_time,
                duration_seconds=EXCLUDED.duration_seconds,
                error_message=EXCLUDED.error_message,
                error_traceback=EXCLUDED.error_traceback,
                updated_at=NOW()
        """, (
            str(uuid.uuid4()), task_run_id,
            fields.get("run_id"), fields.get("pipeline_id"),
            fields.get("task_id"), fields.get("task_type"),
            status,
            fields.get("output_duckdb_path"),
            fields.get("output_table"),
            fields.get("output_row_count"),
            fields.get("output_size_bytes"),
            fields.get("start_time"), fields.get("end_time"),
            fields.get("duration_seconds"),
            fields.get("error_message"), fields.get("error_traceback"),
            os.getenv("HOSTNAME", "eks-job"),
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Audit write failed: {e}")


# ─── Operation: extract ───────────────────────────────────────────────────────

def execute_extract(task_config: dict, output_path: str, run_id: str):
    """
    Stream rows from a source database into a DuckDB file on S3.

    Processes EXTRACT_CHUNK_SIZE rows at a time via PyArrow so the full result
    set is never in memory at once.  Output is a standard .duckdb file so
    downstream duckdb_transform tasks can ATTACH it with no format changes.
    """
    source       = task_config.get("source", {})
    conn_name    = source.get("connection")
    query        = source.get("query", "")
    output_table = task_config.get("output", {}).get("table", "raw_data")
    chunk_size   = int(os.getenv("EXTRACT_CHUNK_SIZE", "500000"))

    if not conn_name:
        raise ValueError("task_config.source.connection is required for extract")
    if not query:
        raise ValueError("task_config.source.query is required for extract")

    query = query.replace("{{ run_id }}", run_id).replace("{{run_id}}", run_id)

    logger.info(f"Fetching credentials for connection: {conn_name}")
    creds = get_secret(conn_name)
    conn  = get_db_connection(creds)

    logger.info(f"Executing source query ({len(query)} chars)")
    cursor = conn.cursor()
    cursor.execute(query)
    col_names = [d[0] for d in cursor.description]

    local_output  = f"/tmp/extract_{run_id}.duckdb"
    db            = duckdb.connect(local_output)
    db.execute(f"SET memory_limit='{os.getenv('DUCKDB_MEMORY_LIMIT', '8GB')}'")
    table_created = False
    total_rows    = 0

    try:
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break

            arrays = [pa.array([r[i] for r in rows]) for i in range(len(col_names))]
            chunk  = pa.table(dict(zip(col_names, arrays)))

            # DuckDB reads Arrow tables natively — no full-dataset copy in memory
            db.register("_chunk", chunk)
            if not table_created:
                db.execute(f"CREATE TABLE {output_table} AS SELECT * FROM _chunk")
                table_created = True
            else:
                db.execute(f"INSERT INTO {output_table} SELECT * FROM _chunk")
            db.unregister("_chunk")

            total_rows += len(rows)
            logger.info(f"Extracted {total_rows:,} rows...")

        # Query returned 0 rows — create the empty table so downstream ATTACH
        # can still reference the table name without a catalog error.
        if not table_created:
            col_defs = ", ".join(f'"{name}" VARCHAR' for name in col_names)
            db.execute(f"CREATE TABLE {output_table} ({col_defs})")
            logger.info(f"Created empty table {output_table} (query returned 0 rows)")
    finally:
        db.close()
        cursor.close()
        conn.close()

    size_bytes = os.path.getsize(local_output)
    upload_duckdb(local_output, output_path)
    try:
        os.unlink(local_output)
    except Exception:
        pass

    logger.info(f"Extract complete: {total_rows:,} rows -> {output_path}")
    return total_rows, size_bytes


# ─── Operation: transform ─────────────────────────────────────────────────────

def execute_transform(task_config: dict, input_paths: dict, output_path: str, run_id: str):
    """
    Download upstream DuckDB files from S3, run SQL transform, upload result.

    DUCKDB_MEMORY_LIMIT env var caps in-process memory; spill goes to
    /tmp/duckdb_spill_<run_id> so the container needs enough ephemeral disk
    for the spill only, not the full dataset.
    """
    local_output = f"/tmp/output_{run_id}.duckdb"
    local_inputs = {}

    for task_id, s3_path in input_paths.items():
        local_path = f"/tmp/input_{task_id}_{run_id}.duckdb"
        download_duckdb(s3_path, local_path)
        local_inputs[task_id] = local_path
        logger.info(f"Downloaded input {task_id}: {s3_path}")

    db = duckdb.connect(local_output)
    db.execute(f"SET memory_limit='{os.getenv('DUCKDB_MEMORY_LIMIT', '8GB')}'")
    db.execute(f"SET temp_directory='/tmp/duckdb_spill_{run_id}'")

    for alias, lpath in local_inputs.items():
        db.execute(f"ATTACH '{lpath}' AS {alias.replace('-', '_')} (READ_ONLY)")
        logger.info(f"Attached {alias}")

    sql          = task_config.get("sql", "")
    output_table = task_config.get("output", {}).get("table", "result")
    if not sql:
        raise ValueError("No SQL provided in task config")

    sql = sql.replace("{{ run_id }}", run_id).replace("{{run_id}}", run_id)
    logger.info(f"Executing transform SQL ({len(sql)} chars)")
    db.execute(f"CREATE OR REPLACE TABLE {output_table} AS ({sql})")

    row_count = db.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
    logger.info(f"Transform complete: {row_count:,} rows in {output_table}")
    db.close()

    size_bytes = os.path.getsize(local_output)
    upload_duckdb(local_output, output_path)
    os.unlink(local_output)
    for lpath in local_inputs.values():
        try: os.unlink(lpath)
        except: pass

    return row_count, size_bytes


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    pipeline_id    = os.getenv("PIPELINE_ID", "")
    run_id         = os.getenv("RUN_ID", "")
    task_id        = os.getenv("TASK_ID", "")
    output_path    = os.getenv("OUTPUT_DUCKDB_PATH", "")
    input_paths    = json.loads(os.getenv("INPUT_PATHS", "{}"))
    task_config    = json.loads(os.getenv("TASK_CONFIG", "{}"))
    operation_type = os.getenv("OPERATION_TYPE", "transform")
    task_run_id    = f"{run_id}_{task_id}_attempt1"

    if not all([pipeline_id, run_id, task_id, output_path]):
        logger.error("Missing required env vars: PIPELINE_ID, RUN_ID, TASK_ID, OUTPUT_DUCKDB_PATH")
        sys.exit(1)

    start_time = datetime.now(timezone.utc)
    logger.info(f"Starting EKS job [{operation_type}]: {pipeline_id}/{task_id} run={run_id}")

    write_task_run(
        "running",
        task_run_id=task_run_id, run_id=run_id, pipeline_id=pipeline_id,
        task_id=task_id, task_type=f"eks_{operation_type}", start_time=start_time,
    )

    try:
        if operation_type == "extract":
            row_count, size_bytes = execute_extract(task_config, output_path, run_id)
        else:
            row_count, size_bytes = execute_transform(task_config, input_paths, output_path, run_id)

        output_table = task_config.get("output", {}).get("table")
        end_time     = datetime.now(timezone.utc)
        duration     = (end_time - start_time).total_seconds()

        write_task_run(
            "success",
            task_run_id=task_run_id, run_id=run_id, pipeline_id=pipeline_id,
            task_id=task_id, task_type=f"eks_{operation_type}",
            start_time=start_time, end_time=end_time, duration_seconds=duration,
            output_duckdb_path=output_path, output_table=output_table,
            output_row_count=row_count, output_size_bytes=size_bytes,
        )
        logger.info(f"Job SUCCESS: {row_count:,} rows -> {output_path} in {duration:.1f}s")
        sys.exit(0)

    except Exception as e:
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        write_task_run(
            "failed",
            task_run_id=task_run_id, run_id=run_id, pipeline_id=pipeline_id,
            task_id=task_id, task_type=f"eks_{operation_type}",
            start_time=start_time, end_time=end_time, duration_seconds=duration,
            error_message=str(e)[:2000], error_traceback=traceback.format_exc()[:5000],
        )
        logger.error(f"Job FAILED: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
