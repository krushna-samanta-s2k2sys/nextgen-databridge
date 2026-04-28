#!/usr/bin/env python3
"""
NextGenDatabridge EKS Job Entrypoint
Runs as a Kubernetes Job container for heavy transform workloads.
Reads task config from env, executes the transform, writes DuckDB to S3.
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("nextgen_databridge.eks_job")


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


def download_duckdb(s3_path: str, local_path: str):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3 = get_s3_client()
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    logger.info(f"Downloading s3://{bucket}/{key} → {local_path}")
    s3.download_file(bucket, key, local_path)


def upload_duckdb(local_path: str, s3_path: str):
    s3 = get_s3_client()
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    logger.info(f"Uploading {local_path} → s3://{bucket}/{key}")
    s3.upload_file(local_path, bucket, key)


def write_task_run(status: str, **fields):
    """Update task_run status in audit DB"""
    url = os.getenv("AUDIT_DB_URL", "").replace("postgresql+asyncpg://", "postgresql://")
    if not url:
        return
    try:
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        task_run_id = fields.get("task_run_id")
        if not task_run_id:
            return

        cur.execute("""
            INSERT INTO task_runs (
                id, task_run_id, run_id, pipeline_id, task_id, task_type,
                status, output_duckdb_path, output_row_count, output_size_bytes,
                start_time, end_time, duration_seconds,
                error_message, error_traceback, worker_host, created_at, updated_at
            ) VALUES (
                %s,%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,%s, NOW(),NOW()
            )
            ON CONFLICT (task_run_id) DO UPDATE SET
                status=EXCLUDED.status,
                output_duckdb_path=EXCLUDED.output_duckdb_path,
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


def execute_transform(task_config: dict, input_paths: dict, output_path: str, run_id: str):
    """Execute the heavy DuckDB transform"""
    local_output = f"/tmp/output_{run_id}.duckdb"
    local_inputs = {}

    # Download input DuckDB files
    for task_id, s3_path in input_paths.items():
        local_path = f"/tmp/input_{task_id}_{run_id}.duckdb"
        download_duckdb(s3_path, local_path)
        local_inputs[task_id] = local_path
        logger.info(f"Downloaded input {task_id}: {s3_path}")

    db = duckdb.connect(local_output)

    # Attach all inputs
    for alias, lpath in local_inputs.items():
        safe_alias = alias.replace("-", "_")
        db.execute(f"ATTACH '{lpath}' AS {safe_alias} (READ_ONLY)")
        logger.info(f"Attached {alias}")

    sql = task_config.get("sql", "")
    output_table = task_config.get("output", {}).get("table", "result")

    if not sql:
        raise ValueError("No SQL provided in task config")

    # Render basic template vars
    sql = sql.replace("{{ run_id }}", run_id)
    sql = sql.replace("{{run_id}}", run_id)

    logger.info(f"Executing transform SQL ({len(sql)} chars)")
    db.execute(f"CREATE OR REPLACE TABLE {output_table} AS ({sql})")

    row_count = db.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
    logger.info(f"Transform complete: {row_count} rows in {output_table}")
    db.close()

    size_bytes = os.path.getsize(local_output)
    upload_duckdb(local_output, output_path)
    os.unlink(local_output)

    # Cleanup inputs
    for lpath in local_inputs.values():
        try: os.unlink(lpath)
        except: pass

    return row_count, size_bytes


def main():
    pipeline_id  = os.getenv("PIPELINE_ID", "")
    run_id       = os.getenv("RUN_ID", "")
    task_id      = os.getenv("TASK_ID", "")
    output_path  = os.getenv("OUTPUT_DUCKDB_PATH", "")
    input_paths  = json.loads(os.getenv("INPUT_PATHS", "{}"))
    task_config  = json.loads(os.getenv("TASK_CONFIG", "{}"))
    task_run_id  = f"{run_id}_{task_id}_attempt1"

    if not all([pipeline_id, run_id, task_id, output_path]):
        logger.error("Missing required env vars: PIPELINE_ID, RUN_ID, TASK_ID, OUTPUT_DUCKDB_PATH")
        sys.exit(1)

    start_time = datetime.now(timezone.utc)
    logger.info(f"Starting EKS transform job: {pipeline_id}/{task_id} run={run_id}")

    write_task_run(
        "running",
        task_run_id=task_run_id,
        run_id=run_id,
        pipeline_id=pipeline_id,
        task_id=task_id,
        task_type="eks_job",
        start_time=start_time,
    )

    try:
        row_count, size_bytes = execute_transform(task_config, input_paths, output_path, run_id)
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        write_task_run(
            "success",
            task_run_id=task_run_id,
            run_id=run_id,
            pipeline_id=pipeline_id,
            task_id=task_id,
            task_type="eks_job",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            output_duckdb_path=output_path,
            output_row_count=row_count,
            output_size_bytes=size_bytes,
        )
        logger.info(f"Job SUCCESS: {row_count} rows → {output_path} in {duration:.1f}s")
        sys.exit(0)

    except Exception as e:
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()
        err_msg = str(e)
        err_tb  = traceback.format_exc()

        write_task_run(
            "failed",
            task_run_id=task_run_id,
            run_id=run_id,
            pipeline_id=pipeline_id,
            task_id=task_id,
            task_type="eks_job",
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration,
            error_message=err_msg[:2000],
            error_traceback=err_tb[:5000],
        )
        logger.error(f"Job FAILED: {err_msg}")
        sys.exit(1)


if __name__ == "__main__":
    main()
