"""
NextGenDatabridge Base Operator
Shared state, audit helpers, S3/DuckDB utilities, and secret resolution
inherited by all NextGenDatabridge operator classes.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
import psycopg2
from airflow.models import BaseOperator
from airflow.utils.context import Context

logger = logging.getLogger("nextgen_databridge.operators")


class NextGenDatabridgeBaseOperator(BaseOperator):
    """
    Base class for all NextGenDatabridge operators and action triggers.
    Provides: audit DB logging, task-run record upserts, S3/DuckDB helpers,
    and AWS Secrets Manager resolution.
    """

    def __init__(
        self,
        pipeline_id: str,
        task_config: dict,
        pipeline_config: dict,
        duckdb_bucket: str,
        audit_db_url: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.pipeline_id     = pipeline_id
        self.task_config     = task_config
        self.pipeline_config = pipeline_config
        self.duckdb_bucket   = duckdb_bucket
        self.audit_db_url    = audit_db_url
        self._task_run_id: Optional[str] = None

    # ── Secrets Manager ────────────────────────────────────────────────────────
    def _resolve_secret(self, value: str) -> str:
        """
        If *value* starts with "secret:", fetch the remainder from AWS Secrets Manager.
        Syntax: "secret:<secret-name>"  or  "secret:<secret-name>/<json-key>"
        Returns the value unchanged when it does not start with "secret:".
        """
        if not isinstance(value, str) or not value.startswith("secret:"):
            return value
        ref   = value[len("secret:"):]
        parts = ref.split("/", 1)
        sm    = boto3.client("secretsmanager",
                             region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        raw   = sm.get_secret_value(SecretId=parts[0])["SecretString"]
        return json.loads(raw)[parts[1]] if len(parts) == 2 else raw

    # ── S3 helpers ─────────────────────────────────────────────────────────────
    def get_s3_client(self):
        kwargs = dict(region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        endpoint = os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            kwargs["endpoint_url"] = endpoint
        return boto3.client("s3", **kwargs)

    def duckdb_s3_path(self, run_id: str, task_id: str) -> str:
        return f"s3://{self.duckdb_bucket}/pipelines/{self.pipeline_id}/runs/{run_id}/{task_id}.duckdb"

    def local_duckdb_path(self, run_id: str, task_id: str) -> str:
        return f"/tmp/nextgen_databridge/{self.pipeline_id}/{run_id}/{task_id}.duckdb"

    def ensure_local_dir(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def download_duckdb(self, s3_path: str, local_path: str):
        self.ensure_local_dir(local_path)
        s3 = self.get_s3_client()
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        logger.info(f"Downloading s3://{bucket}/{key} → {local_path}")
        s3.download_file(bucket, key, local_path)

    def upload_duckdb(self, local_path: str, s3_path: str):
        s3 = self.get_s3_client()
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        logger.info(f"Uploading {local_path} → s3://{bucket}/{key}")
        s3.upload_file(local_path, bucket, key)

    # ── Audit DB helpers ───────────────────────────────────────────────────────
    def _audit_db_conn(self):
        raw = self.audit_db_url or os.getenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "")
        if not raw:
            try:
                import boto3 as _boto3, json as _json
                _sm = _boto3.client(
                    "secretsmanager",
                    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
                )
                _secret = _sm.get_secret_value(
                    SecretId="nextgen-databridge/connections/audit_db"
                )
                raw = _json.loads(_secret["SecretString"]).get("url", "")
            except Exception as _e:
                raise RuntimeError(
                    f"NEXTGEN_DATABRIDGE_AUDIT_DB_URL not set and Secrets Manager fallback failed: {_e}"
                )
        if not raw:
            raise RuntimeError(
                "NEXTGEN_DATABRIDGE_AUDIT_DB_URL is not set and secret 'url' field is empty"
            )
        url = raw.replace("postgresql+asyncpg://", "postgresql://")
        return psycopg2.connect(url)

    def _write_task_run(self, run_id: str, status: str, **fields):
        try:
            conn = self._audit_db_conn()
            cur  = conn.cursor()
            task_run_id = f"{run_id}_{self.task_config['task_id']}_attempt{fields.get('attempt_number', 1)}"
            self._task_run_id = task_run_id

            task_type_raw = self.task_config.get("type", "").lower().replace("-", "_")
            task_type_val = task_type_raw if task_type_raw in self._VALID_TASK_TYPES else None
            _status_lower = status.lower()

            # Ensure pipeline row exists (separate commit so FK is visible to pipeline_runs insert).
            cur.execute("""
                INSERT INTO pipelines
                    (id, pipeline_id, name, status, created_by, created_at, updated_at)
                VALUES (%s, %s, %s, 'active', 'airflow', NOW(), NOW())
                ON CONFLICT (pipeline_id) DO NOTHING
            """, (str(uuid.uuid4()), self.pipeline_id,
                  self.pipeline_config.get("name", self.pipeline_id)))
            conn.commit()

            # Ensure pipeline_run row exists (separate commit so FK is visible to task_runs insert).
            cur.execute("""
                INSERT INTO pipeline_runs
                    (id, run_id, pipeline_id, status, trigger_type,
                     total_tasks, completed_tasks, failed_tasks, total_rows_processed,
                     created_at)
                VALUES (%s, %s, %s, 'running', 'airflow', 0, 0, 0, 0, NOW())
                ON CONFLICT (run_id) DO NOTHING
            """, (str(uuid.uuid4()), run_id, self.pipeline_id))
            conn.commit()

            # All remaining writes in a single transaction so task_runs and
            # pipeline_runs counters/status are committed atomically.
            total_tasks = len(self.pipeline_config.get("tasks", []))
            cur.execute("""
                UPDATE pipeline_runs
                SET start_time  = COALESCE(start_time, NOW()),
                    total_tasks = GREATEST(total_tasks, %s)
                WHERE run_id = %s
            """, (total_tasks, run_id))

            if _status_lower == "success":
                cur.execute("""
                    UPDATE pipeline_runs
                    SET completed_tasks      = COALESCE(completed_tasks, 0) + 1,
                        total_rows_processed = COALESCE(total_rows_processed, 0) + %s
                    WHERE run_id = %s
                """, (fields.get("output_row_count") or 0, run_id))
            elif _status_lower == "failed":
                cur.execute("""
                    UPDATE pipeline_runs
                    SET failed_tasks = COALESCE(failed_tasks, 0) + 1
                    WHERE run_id = %s
                """, (run_id,))

            cur.execute("""
                INSERT INTO task_runs (
                    id, task_run_id, run_id, pipeline_id, task_id, task_type,
                    status, attempt_number, max_attempts, input_sources, output_duckdb_path,
                    output_table, output_row_count, output_size_bytes, output_schema,
                    queued_at, start_time, end_time, duration_seconds,
                    qc_results, qc_passed, qc_warnings, qc_failures,
                    error_message, error_traceback, task_config_snapshot,
                    metrics, worker_host, created_at, updated_at
                ) VALUES (
                    %(id)s, %(task_run_id)s, %(run_id)s, %(pipeline_id)s, %(task_id)s, %(task_type)s,
                    %(status)s, %(attempt_number)s, %(max_attempts)s, %(input_sources)s, %(output_duckdb_path)s,
                    %(output_table)s, %(output_row_count)s, %(output_size_bytes)s, %(output_schema)s,
                    %(queued_at)s, %(start_time)s, %(end_time)s, %(duration_seconds)s,
                    %(qc_results)s, %(qc_passed)s, %(qc_warnings)s, %(qc_failures)s,
                    %(error_message)s, %(error_traceback)s, %(task_config_snapshot)s,
                    %(metrics)s, %(worker_host)s, NOW(), NOW()
                )
                ON CONFLICT (task_run_id) DO UPDATE SET
                    status             = EXCLUDED.status,
                    output_duckdb_path = COALESCE(EXCLUDED.output_duckdb_path, task_runs.output_duckdb_path),
                    output_table       = COALESCE(EXCLUDED.output_table,       task_runs.output_table),
                    output_row_count   = COALESCE(EXCLUDED.output_row_count,   task_runs.output_row_count),
                    output_size_bytes  = COALESCE(EXCLUDED.output_size_bytes,  task_runs.output_size_bytes),
                    output_schema      = COALESCE(EXCLUDED.output_schema,      task_runs.output_schema),
                    end_time           = EXCLUDED.end_time,
                    duration_seconds   = EXCLUDED.duration_seconds,
                    qc_results         = EXCLUDED.qc_results,
                    qc_passed          = EXCLUDED.qc_passed,
                    qc_warnings        = EXCLUDED.qc_warnings,
                    qc_failures        = EXCLUDED.qc_failures,
                    error_message      = EXCLUDED.error_message,
                    error_traceback    = EXCLUDED.error_traceback,
                    metrics            = EXCLUDED.metrics,
                    updated_at         = NOW()
            """, {
                "id":                   str(uuid.uuid4()),
                "task_run_id":          task_run_id,
                "run_id":               run_id,
                "pipeline_id":          self.pipeline_id,
                "task_id":              self.task_config["task_id"],
                "task_type":            task_type_val,
                "status":               _status_lower,
                "attempt_number":       fields.get("attempt_number", 1),
                "max_attempts":         self.task_config.get("retries", self.retries if hasattr(self, "retries") else 3) + 1,
                "input_sources":        json.dumps(fields.get("input_sources", [])),
                "output_duckdb_path":   fields.get("output_duckdb_path"),
                "output_table":         fields.get("output_table"),
                "output_row_count":     fields.get("output_row_count"),
                "output_size_bytes":    fields.get("output_size_bytes"),
                "output_schema":        json.dumps(fields.get("output_schema")) if fields.get("output_schema") else None,
                "queued_at":            fields.get("queued_at"),
                "start_time":           fields.get("start_time"),
                "end_time":             fields.get("end_time"),
                "duration_seconds":     fields.get("duration_seconds"),
                "qc_results":           json.dumps(fields.get("qc_results")) if fields.get("qc_results") else None,
                "qc_passed":            fields.get("qc_passed"),
                "qc_warnings":          fields.get("qc_warnings", 0),
                "qc_failures":          fields.get("qc_failures", 0),
                "error_message":        fields.get("error_message"),
                "error_traceback":      fields.get("error_traceback"),
                "task_config_snapshot": json.dumps(self.task_config),
                "metrics":              json.dumps(fields.get("metrics", {})),
                "worker_host":          os.getenv("HOSTNAME", "unknown"),
            })

            # Check completion only after task_runs is written (same transaction).
            if _status_lower in ("success", "failed"):
                cur.execute("""
                    SELECT total_tasks, completed_tasks, failed_tasks
                    FROM pipeline_runs WHERE run_id = %s
                """, (run_id,))
                row = cur.fetchone()
                if row:
                    total_t, completed_t, failed_t = row
                    if total_t > 0 and (completed_t + failed_t) >= total_t:
                        final_status = "failed" if failed_t > 0 else "success"
                        cur.execute("""
                            UPDATE pipeline_runs
                            SET status           = CAST(%s AS runstatus),
                                end_time         = NOW(),
                                duration_seconds = EXTRACT(EPOCH FROM (NOW() - start_time))
                            WHERE run_id = %s AND status = 'running'
                        """, (final_status, run_id))

            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to write task_run audit record: {e}")

    def _write_audit_log(self, event_type: str, run_id: str, details: dict, severity: str = "info"):
        try:
            conn = self._audit_db_conn()
            cur  = conn.cursor()
            cur.execute("""
                INSERT INTO audit_logs
                    (id, event_type, pipeline_id, run_id, task_id, "user", details, severity, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                str(uuid.uuid4()), event_type, self.pipeline_id, run_id,
                self.task_config["task_id"], "airflow", json.dumps(details), severity,
            ))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Audit log write failed: {e}")

    # ── DuckDB producer resolution ─────────────────────────────────────────────
    # Task types that pass through DuckDB files without producing new ones.
    _PASSTHROUGH_TASK_TYPES = {"data_quality", "schema_validate", "schema_validation"}

    # Task types that produce a DuckDB output file.
    _PRODUCER_TASK_TYPES = {
        "sql_extract", "cdc_extract", "duckdb_transform", "duckdb_query",
        "file_ingest", "eks_job", "eks_extract", "eks_transform",
    }

    # All valid values for the tasktype PostgreSQL enum.
    _VALID_TASK_TYPES = {
        "sql_extract", "sql_transform", "duckdb_transform", "duckdb_query",
        "schema_validate", "data_quality", "cdc_extract", "file_ingest",
        "kafka_consume", "kafka_produce", "pubsub_consume", "pubsub_publish",
        "eks_job", "eks_extract", "eks_transform",
        "python_callable", "conditional_branch", "load_target", "notification",
        "api_call", "autosys_job", "stored_proc",
    }

    def _find_duckdb_producer_task(self, ti, task_id: str, visited: set = None) -> str | None:
        """Walk the DAG upward through passthrough tasks to find the DuckDB-producing task."""
        if visited is None:
            visited = set()
        if task_id in visited:
            return None
        visited.add(task_id)

        all_tasks = {t["task_id"]: t for t in self.pipeline_config.get("tasks", [])}
        task_def  = all_tasks.get(task_id, {})
        task_type = task_def.get("type", "")

        if task_type in self._PASSTHROUGH_TASK_TYPES:
            for parent in task_def.get("depends_on", []):
                producer = self._find_duckdb_producer_task(ti, parent, visited)
                if producer:
                    return producer
            return None

        if task_type in self._PRODUCER_TASK_TYPES:
            return task_id

        return None

    def execute(self, context: Context) -> Any:
        raise NotImplementedError
