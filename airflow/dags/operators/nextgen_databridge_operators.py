"""
NextGenDatabridge Custom Airflow Operators
All task types implemented as Airflow BaseOperator subclasses.
Each operator logs to the audit table before and after execution.
"""
from __future__ import annotations

import json
import logging
import os
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
import psycopg2
from airflow.models import BaseOperator
from airflow.utils.context import Context

logger = logging.getLogger("nextgen_databridge.operators")


# ─────────────────────────────────────────────────────────────────────────────
# Base NextGenDatabridge Operator
# ─────────────────────────────────────────────────────────────────────────────
class NextGenDatabridgeBaseOperator(BaseOperator):
    """
    Base class for all NextGenDatabridge operators.
    Handles: audit logging, task run record creation/update,
    DuckDB S3 path templating, error capture.
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

    # ── S3 helpers ─────────────────────────────────────────────────────
    def get_s3_client(self):
        # Credentials come from the MWAA execution role via instance metadata.
        # AWS_ENDPOINT_URL is honoured when set (e.g. local dev with LocalStack).
        kwargs = dict(region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
        endpoint = os.getenv("AWS_ENDPOINT_URL")
        if endpoint:
            kwargs["endpoint_url"] = endpoint
        return boto3.client("s3", **kwargs)

    def duckdb_s3_path(self, run_id: str, task_id: str) -> str:
        """Canonical S3 path for a task's output DuckDB file"""
        return f"s3://{self.duckdb_bucket}/pipelines/{self.pipeline_id}/runs/{run_id}/{task_id}.duckdb"

    def local_duckdb_path(self, run_id: str, task_id: str) -> str:
        return f"/tmp/nextgen_databridge/{self.pipeline_id}/{run_id}/{task_id}.duckdb"

    def ensure_local_dir(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)

    # ── S3 DuckDB download / upload ─────────────────────────────────────
    def download_duckdb(self, s3_path: str, local_path: str):
        """Download a DuckDB file from S3 to /tmp"""
        self.ensure_local_dir(local_path)
        s3 = self.get_s3_client()
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        logger.info(f"Downloading s3://{bucket}/{key} → {local_path}")
        s3.download_file(bucket, key, local_path)

    def upload_duckdb(self, local_path: str, s3_path: str):
        """Upload a DuckDB file from /tmp to S3"""
        s3 = self.get_s3_client()
        bucket, key = s3_path.replace("s3://", "").split("/", 1)
        logger.info(f"Uploading {local_path} → s3://{bucket}/{key}")
        s3.upload_file(local_path, bucket, key)

    # ── Audit DB helpers ────────────────────────────────────────────────
    def _audit_db_conn(self):
        """Sync psycopg2 connection for audit writes from operator"""
        url = self.audit_db_url or os.environ["NEXTGEN_DATABRIDGE_AUDIT_DB_URL"]
        url = url.replace("postgresql+asyncpg://", "postgresql://")
        return psycopg2.connect(url)

    def _write_task_run(self, run_id: str, status: str, **fields):
        """Upsert a task_runs record"""
        try:
            conn = self._audit_db_conn()
            cur = conn.cursor()
            task_run_id = f"{run_id}_{self.task_config['task_id']}_attempt{fields.get('attempt_number', 1)}"
            self._task_run_id = task_run_id

            # Ensure the pipeline exists (FK: pipeline_runs → pipelines).
            cur.execute("""
                INSERT INTO pipelines
                    (id, pipeline_id, name, status, created_by, created_at, updated_at)
                VALUES (%s, %s, %s, 'active', 'airflow', NOW(), NOW())
                ON CONFLICT (pipeline_id) DO NOTHING
            """, (str(uuid.uuid4()), self.pipeline_id,
                  self.pipeline_config.get("name", self.pipeline_id)))
            conn.commit()

            # Ensure a pipeline_runs row exists so the FK on task_runs is satisfied.
            # When Airflow triggers the DAG directly (not via the NextGenDatabridge API), no
            # pipeline_runs record is created beforehand — we upsert a minimal one here.
            cur.execute("""
                INSERT INTO pipeline_runs
                    (id, run_id, pipeline_id, status, trigger_type,
                     total_tasks, completed_tasks, failed_tasks, total_rows_processed,
                     created_at)
                VALUES (%s, %s, %s, 'running', 'airflow', 0, 0, 0, 0, NOW())
                ON CONFLICT (run_id) DO NOTHING
            """, (str(uuid.uuid4()), run_id, self.pipeline_id))
            conn.commit()

            # Set start_time and total_tasks on the first call for this run
            total_tasks = len(self.pipeline_config.get("tasks", []))
            cur.execute("""
                UPDATE pipeline_runs
                SET start_time = NOW(), total_tasks = %s
                WHERE run_id = %s AND start_time IS NULL
            """, (total_tasks, run_id))

            # Increment per-task counters on completion
            _status_upper = status.upper()
            if _status_upper == "SUCCESS":
                cur.execute("""
                    UPDATE pipeline_runs
                    SET completed_tasks = completed_tasks + 1,
                        total_rows_processed = total_rows_processed + %s
                    WHERE run_id = %s
                """, (fields.get("output_row_count") or 0, run_id))
            elif _status_upper == "FAILED":
                cur.execute("""
                    UPDATE pipeline_runs SET failed_tasks = failed_tasks + 1
                    WHERE run_id = %s
                """, (run_id,))

            # Finalize run status when all tasks have reported in
            if _status_upper in ("SUCCESS", "FAILED"):
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
                            SET status = CAST(%s AS runstatus),
                                end_time = NOW(),
                                duration_seconds = EXTRACT(EPOCH FROM (NOW() - start_time))
                            WHERE run_id = %s AND status = 'running'
                        """, (final_status, run_id))

            conn.commit()

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
                    status = EXCLUDED.status,
                    output_duckdb_path = EXCLUDED.output_duckdb_path,
                    output_row_count = EXCLUDED.output_row_count,
                    output_size_bytes = EXCLUDED.output_size_bytes,
                    output_schema = EXCLUDED.output_schema,
                    end_time = EXCLUDED.end_time,
                    duration_seconds = EXCLUDED.duration_seconds,
                    qc_results = EXCLUDED.qc_results,
                    qc_passed = EXCLUDED.qc_passed,
                    qc_warnings = EXCLUDED.qc_warnings,
                    qc_failures = EXCLUDED.qc_failures,
                    error_message = EXCLUDED.error_message,
                    error_traceback = EXCLUDED.error_traceback,
                    metrics = EXCLUDED.metrics,
                    updated_at = NOW()
            """, {
                "id": str(uuid.uuid4()),
                "task_run_id": task_run_id,
                "run_id": run_id,
                "pipeline_id": self.pipeline_id,
                "task_id": self.task_config["task_id"],
                "task_type": self.task_config.get("type", "").lower().replace("-", "_"),
                "status": status.lower(),
                "attempt_number": fields.get("attempt_number", 1),
                "max_attempts": self.task_config.get("retries", self.retries if hasattr(self, "retries") else 3) + 1,
                "input_sources": json.dumps(fields.get("input_sources", [])),
                "output_duckdb_path": fields.get("output_duckdb_path"),
                "output_table": fields.get("output_table"),
                "output_row_count": fields.get("output_row_count"),
                "output_size_bytes": fields.get("output_size_bytes"),
                "output_schema": json.dumps(fields.get("output_schema")) if fields.get("output_schema") else None,
                "queued_at": fields.get("queued_at"),
                "start_time": fields.get("start_time"),
                "end_time": fields.get("end_time"),
                "duration_seconds": fields.get("duration_seconds"),
                "qc_results": json.dumps(fields.get("qc_results")) if fields.get("qc_results") else None,
                "qc_passed": fields.get("qc_passed"),
                "qc_warnings": fields.get("qc_warnings", 0),
                "qc_failures": fields.get("qc_failures", 0),
                "error_message": fields.get("error_message"),
                "error_traceback": fields.get("error_traceback"),
                "task_config_snapshot": json.dumps(self.task_config),
                "metrics": json.dumps(fields.get("metrics", {})),
                "worker_host": os.getenv("HOSTNAME", "unknown"),
            })
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to write task_run audit record: {e}")

    def _write_audit_log(self, event_type: str, run_id: str, details: dict, severity: str = "info"):
        try:
            conn = self._audit_db_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO audit_logs
                    (id, event_type, pipeline_id, run_id, task_id, "user", details, severity, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                str(uuid.uuid4()),
                event_type,
                self.pipeline_id,
                run_id,
                self.task_config["task_id"],
                "airflow",
                json.dumps(details),
                severity,
            ))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Audit log write failed: {e}")

    # Task types that are transparent — they validate/inspect DuckDB files but do not
    # produce new ones. When resolving which DuckDB to attach, we look through these.
    _PASSTHROUGH_TASK_TYPES = {"data_quality", "schema_validation"}

    def _find_duckdb_producer_task(self, ti, task_id: str, visited: set = None) -> str | None:
        """Find the task_id of the actual DuckDB-producing task reachable from task_id.
        Quality and schema-check tasks are transparent — we walk through them to their source.
        Returns the producing task_id, or None if no DuckDB output is found.
        The caller should reconstruct the S3 path via duckdb_s3_path(run_id, producer_id)
        rather than using the XCom value, which may contain unresolved {{ run_id }} templates."""
        if visited is None:
            visited = set()
        if task_id in visited:
            return None
        visited.add(task_id)

        all_tasks = {t["task_id"]: t for t in self.pipeline_config.get("tasks", [])}
        task_def  = all_tasks.get(task_id, {})
        task_type = task_def.get("type", "")

        if task_type in self._PASSTHROUGH_TASK_TYPES:
            # Look through quality/schema tasks to find their DuckDB source
            for parent in task_def.get("depends_on", []):
                producer = self._find_duckdb_producer_task(ti, parent, visited)
                if producer:
                    return producer
            return None

        # Extract / transform tasks: confirm via XCom presence then return this task_id
        if ti.xcom_pull(task_ids=task_id, key="duckdb_path") is not None:
            return task_id

        return None

    def execute(self, context: Context) -> Any:
        raise NotImplementedError


# ─────────────────────────────────────────────────────────────────────────────
# SQL Extract Operator
# ─────────────────────────────────────────────────────────────────────────────
class SQLExtractOperator(NextGenDatabridgeBaseOperator):
    """
    Extract data from SQL Server / Oracle / PostgreSQL into DuckDB.
    Supports full load, incremental, and CDC modes.
    """

    def execute(self, context: Context) -> str:
        run_id   = context["run_id"]
        task_id  = self.task_config["task_id"]
        source   = self.task_config.get("source", {})
        output   = self.task_config.get("output", {})

        s3_path    = output.get("duckdb_path") or self.duckdb_s3_path(run_id, task_id)
        # Resolve any Jinja-style {{ run_id }} placeholders left in config-supplied paths
        s3_path    = s3_path.replace("{{ run_id }}", run_id).replace("{{run_id}}", run_id)
        local_path = self.local_duckdb_path(run_id, task_id)
        self.ensure_local_dir(local_path)

        start_ts = datetime.now(timezone.utc)
        input_sources = [{
            "type": source.get("connection_type", "sql"),
            "connection": source.get("connection"),
            "query": source.get("query", "")[:200],
            "table": source.get("table"),
        }]

        self._write_task_run(
            run_id, "running",
            start_time=start_ts,
            input_sources=input_sources,
        )
        self._write_audit_log("TASK_STARTED", run_id, {
            "task_type": "sql_extract",
            "source": source.get("connection"),
        })

        try:
            # Get connection credentials from Airflow connections / Secrets Manager
            conn_id   = source.get("connection")
            query     = source.get("query", f"SELECT * FROM {source.get('table', 'unknown')}")
            batch_size = source.get("batch_size", 100_000)

            # Render Jinja-style template variables in the query before sending to the DB.
            # Airflow does NOT auto-render operator fields that aren't in template_fields.
            ds = context.get("ds", "")
            query = query.replace("{{ ds }}", ds).replace("{{ds}}", ds)
            query = query.replace("{{ run_id }}", run_id).replace("{{run_id}}", run_id)

            # Resolve Airflow connection
            try:
                from airflow.hooks.base import BaseHook
                airflow_conn = BaseHook.get_connection(conn_id)
                conn_type    = airflow_conn.conn_type
                host         = airflow_conn.host
                port         = airflow_conn.port
                login        = airflow_conn.login
                password     = airflow_conn.password
                schema       = airflow_conn.schema
            except Exception:
                # Fallback: read from env/secrets
                conn_type = source.get("connection_type", "sqlserver")
                host, port, login, password, schema = self._get_connection_from_env(conn_id)

            # Build source connection and extract
            df = self._extract_data(conn_type, host, port, login, password, schema, query, batch_size)

            # Write to local DuckDB
            import duckdb
            row_count = len(df) if hasattr(df, '__len__') else 0
            output_table = output.get("table", "raw_data")

            db = duckdb.connect(local_path)
            db.execute(f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM df")

            # Capture schema
            schema_info = db.execute(f"DESCRIBE {output_table}").fetchall()
            schema_dict = {col[0]: col[1] for col in schema_info}
            size_bytes  = os.path.getsize(local_path)
            db.close()

            # Upload to S3
            self.upload_duckdb(local_path, s3_path)

            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()

            self._write_task_run(
                run_id, "success",
                start_time=start_ts,
                end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                input_sources=input_sources,
                output_duckdb_path=s3_path,
                output_table=output_table,
                output_row_count=row_count,
                output_size_bytes=size_bytes,
                output_schema=schema_dict,
            )
            self._write_audit_log("TASK_COMPLETED", run_id, {
                "output_path": s3_path,
                "row_count": row_count,
                "duration_seconds": round(duration, 2),
            })

            # Push to XCom for downstream tasks
            context["ti"].xcom_push(key="duckdb_path", value=s3_path)
            context["ti"].xcom_push(key="row_count", value=row_count)

            logger.info(f"SQLExtract: {task_id} extracted {row_count} rows → {s3_path}")
            return s3_path

        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            err_msg  = str(e)
            err_tb   = traceback.format_exc()
            self._write_task_run(
                run_id, "failed",
                start_time=start_ts,
                end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                error_message=err_msg[:2000],
                error_traceback=err_tb[:5000],
            )
            self._write_audit_log("TASK_FAILED", run_id, {"error": err_msg[:500]}, severity="error")
            raise

    def _extract_data(self, conn_type, host, port, login, password, schema, query, batch_size):
        """Extract using appropriate driver based on conn_type"""
        import pandas as pd

        if conn_type in ("mssql", "sqlserver"):
            try:
                import pyodbc
                conn_str = (
                    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                    f"SERVER={host},{port or 1433};DATABASE={schema};"
                    f"UID={login};PWD={password};TrustServerCertificate=yes"
                )
                conn = pyodbc.connect(conn_str)
            except ImportError:
                import pymssql
                conn = pymssql.connect(host, login, password, schema, port=port or 1433)
            df = pd.read_sql(query, conn, chunksize=None)
            conn.close()

        elif conn_type == "oracle":
            import cx_Oracle
            dsn = cx_Oracle.makedsn(host, port or 1521, service_name=schema)
            conn = cx_Oracle.connect(login, password, dsn)
            df = pd.read_sql(query, conn)
            conn.close()

        elif conn_type in ("postgres", "postgresql"):
            import psycopg2
            conn = psycopg2.connect(
                host=host, port=port or 5432,
                user=login, password=password, dbname=schema
            )
            df = pd.read_sql(query, conn)
            conn.close()

        else:
            # Generic SQLAlchemy fallback
            from sqlalchemy import create_engine
            engine = create_engine(
                f"{conn_type}://{login}:{password}@{host}:{port}/{schema}"
            )
            df = pd.read_sql(query, engine)

        return df

    def _get_connection_from_env(self, conn_id: str):
        prefix = conn_id.upper().replace("-", "_")
        return (
            os.getenv(f"{prefix}_HOST", "localhost"),
            int(os.getenv(f"{prefix}_PORT", "1433")),
            os.getenv(f"{prefix}_USER", ""),
            os.getenv(f"{prefix}_PASS", ""),
            os.getenv(f"{prefix}_DB", ""),
        )


# ─────────────────────────────────────────────────────────────────────────────
# DuckDB Transform Operator
# ─────────────────────────────────────────────────────────────────────────────
class DuckDBTransformOperator(NextGenDatabridgeBaseOperator):
    """
    Execute SQL transformations using DuckDB.
    Can join multiple upstream DuckDB files, apply complex SQL,
    and produce a new DuckDB output.
    """

    def execute(self, context: Context) -> str:
        run_id   = context["run_id"]
        task_id  = self.task_config["task_id"]
        ti       = context["ti"]

        sql    = self.task_config.get("sql", "")
        source = self.task_config.get("source", {})
        output = self.task_config.get("output", {})

        out_s3_path    = output.get("duckdb_path") or self.duckdb_s3_path(run_id, task_id)
        out_s3_path    = out_s3_path.replace("{{ run_id }}", run_id).replace("{{run_id}}", run_id)
        out_local_path = self.local_duckdb_path(run_id, task_id)
        self.ensure_local_dir(out_local_path)

        start_ts = datetime.now(timezone.utc)

        # Collect input sources from XComs or config
        input_sources = []
        depends_on = self.task_config.get("depends_on", [])

        # Resolve input DuckDB files by finding the actual producing task for each dep.
        # Quality/schema tasks are transparent — _find_duckdb_producer_task walks through
        # them. We always reconstruct the canonical S3 path from (run_id, producer_task_id)
        # rather than using the XCom value directly, which may contain unresolved
        # {{ run_id }} Jinja templates copied verbatim from the pipeline JSON config.
        local_inputs: Dict[str, str] = {}  # producer_task_id -> local path
        for dep_task_id in depends_on:
            producer_id = self._find_duckdb_producer_task(ti, dep_task_id)
            if producer_id and producer_id not in local_inputs:
                dep_s3_path = self.duckdb_s3_path(run_id, producer_id)
                dep_local   = self.local_duckdb_path(run_id, producer_id)
                if not os.path.exists(dep_local):
                    self.download_duckdb(dep_s3_path, dep_local)
                local_inputs[producer_id] = dep_local
                input_sources.append({
                    "type": "duckdb",
                    "task_id": producer_id,
                    "path": dep_s3_path,
                })

        # Also handle explicit source config
        if source.get("duckdb_path"):
            explicit_path = source["duckdb_path"]
            explicit_local = f"/tmp/nextgen_databridge/{run_id}/explicit_input.duckdb"
            if explicit_path.startswith("s3://") and not os.path.exists(explicit_local):
                self.download_duckdb(explicit_path, explicit_local)
            local_inputs["_explicit"] = explicit_local
            input_sources.append({"type": "duckdb", "path": explicit_path})

        self._write_task_run(
            run_id, "running",
            start_time=start_ts,
            input_sources=input_sources,
        )

        try:
            import duckdb
            db = duckdb.connect(out_local_path)

            # Attach all input DuckDB files
            for alias, local_path in local_inputs.items():
                safe_alias = alias.replace("-", "_").replace("/", "_")
                db.execute(f"ATTACH '{local_path}' AS {safe_alias} (READ_ONLY)")

            # Apply SQL template substitutions
            sql_rendered = self._render_sql(sql, context, local_inputs)

            output_table = output.get("table", "result")
            final_sql = f"CREATE OR REPLACE TABLE {output_table} AS ({sql_rendered})"

            logger.info(f"DuckDB transform: executing SQL for {task_id}")
            db.execute(final_sql)

            row_count   = db.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
            schema_info = db.execute(f"DESCRIBE {output_table}").fetchall()
            schema_dict = {col[0]: col[1] for col in schema_info}
            db.close()

            size_bytes = os.path.getsize(out_local_path)
            self.upload_duckdb(out_local_path, out_s3_path)

            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(
                run_id, "success",
                start_time=start_ts,
                end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                input_sources=input_sources,
                output_duckdb_path=out_s3_path,
                output_table=output_table,
                output_row_count=row_count,
                output_size_bytes=size_bytes,
                output_schema=schema_dict,
            )
            self._write_audit_log("TASK_COMPLETED", run_id, {
                "output_path": out_s3_path,
                "row_count": row_count,
                "duration": round(duration, 2),
            })

            context["ti"].xcom_push(key="duckdb_path", value=out_s3_path)
            context["ti"].xcom_push(key="row_count", value=row_count)

            logger.info(f"DuckDBTransform: {task_id} produced {row_count} rows → {out_s3_path}")
            return out_s3_path

        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            err_msg  = str(e)
            self._write_task_run(
                run_id, "failed",
                start_time=start_ts,
                end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                error_message=err_msg[:2000],
                error_traceback=traceback.format_exc()[:5000],
            )
            self._write_audit_log("TASK_FAILED", run_id, {"error": err_msg[:500]}, severity="error")
            raise

    def _render_sql(self, sql: str, context: Context, local_inputs: dict) -> str:
        """Render Jinja-style SQL with context variables"""
        ds = context.get("ds", "")
        run_id = context.get("run_id", "")
        sql = sql.replace("{{ ds }}", ds)
        sql = sql.replace("{{ run_id }}", run_id)
        sql = sql.replace("{{ds}}", ds)
        sql = sql.replace("{{run_id}}", run_id)
        return sql


# ─────────────────────────────────────────────────────────────────────────────
# Data Quality Operator
# ─────────────────────────────────────────────────────────────────────────────
class DataQualityOperator(NextGenDatabridgeBaseOperator):
    """
    Execute configurable data quality checks against a DuckDB table.
    Supports: not_null, unique, row_count_min/max, freshness,
              value_range, regex_match, referential_integrity, custom_sql.
    """

    def execute(self, context: Context) -> dict:
        run_id   = context["run_id"]
        task_id  = self.task_config["task_id"]
        ti       = context["ti"]
        checks   = self.task_config.get("checks", [])

        # Resolve input DuckDB
        depends_on  = self.task_config.get("depends_on", [])
        input_s3    = ti.xcom_pull(task_ids=depends_on[0], key="duckdb_path") if depends_on else None
        input_local = self.local_duckdb_path(run_id, depends_on[0] if depends_on else task_id)

        if input_s3 and not os.path.exists(input_local):
            self.download_duckdb(input_s3, input_local)

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts,
                             input_sources=[{"type": "duckdb", "path": input_s3}])

        results = []
        failures = 0
        warnings = 0
        overall_pass = True

        try:
            import duckdb
            db = duckdb.connect(input_local, read_only=True)
            input_table = self.task_config.get("source", {}).get("table", "raw_data")

            for check in checks:
                check_type  = check.get("type")
                action      = check.get("action", "fail")
                column      = check.get("column")
                description = check.get("description", f"{check_type} on {column or 'table'}")

                try:
                    passed, message, count = self._run_check(db, input_table, check)
                except Exception as e:
                    passed = False
                    message = str(e)
                    count   = None

                result = {
                    "check_type": check_type,
                    "column": column,
                    "description": description,
                    "passed": passed,
                    "message": message,
                    "count": count,
                    "action": action,
                }
                results.append(result)

                if not passed:
                    if action == "fail":
                        failures += 1
                        overall_pass = False
                    elif action == "warn":
                        warnings += 1

                logger.info(f"QC check {check_type}/{column}: {'PASS' if passed else 'FAIL'} — {message}")

            db.close()

        except Exception as e:
            failures += 1
            overall_pass = False
            results.append({
                "check_type": "internal_error",
                "passed": False,
                "message": str(e),
                "action": "fail",
            })

        duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
        status = "success" if overall_pass else "failed"

        self._write_task_run(
            run_id, status,
            start_time=start_ts,
            end_time=datetime.now(timezone.utc),
            duration_seconds=duration,
            qc_results={"checks": results},
            qc_passed=overall_pass,
            qc_warnings=warnings,
            qc_failures=failures,
            error_message=f"{failures} QC check(s) failed" if not overall_pass else None,
        )
        self._write_audit_log(
            "TASK_COMPLETED" if overall_pass else "TASK_FAILED",
            run_id,
            {"qc_passed": overall_pass, "failures": failures, "warnings": warnings},
            severity="info" if overall_pass else "error",
        )

        context["ti"].xcom_push(key="qc_passed", value=overall_pass)
        context["ti"].xcom_push(key="qc_results", value=results)
        # Pass through the upstream DuckDB path so downstream transform tasks
        # that depend on this quality task can still attach the correct file.
        if input_s3:
            context["ti"].xcom_push(key="duckdb_path", value=input_s3)

        if not overall_pass and self.task_config.get("fail_pipeline_on_error", True):
            raise ValueError(f"Data quality failed: {failures} checks failed. Results: {results}")

        return {"passed": overall_pass, "results": results, "failures": failures, "warnings": warnings}

    def _run_check(self, db, table: str, check: dict):
        check_type = check["type"]
        column     = check.get("column")

        if check_type == "not_null":
            count = db.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL").fetchone()[0]
            passed = count == 0
            return passed, f"{count} null values in '{column}'", count

        elif check_type == "unique":
            count = db.execute(
                f"SELECT COUNT(*) FROM (SELECT {column}, COUNT(*) c FROM {table} GROUP BY {column} HAVING c > 1)"
            ).fetchone()[0]
            passed = count == 0
            return passed, f"{count} duplicate values in '{column}'", count

        elif check_type == "row_count_min":
            min_val = check.get("value", 1)
            count   = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            passed  = count >= min_val
            return passed, f"Row count {count} < minimum {min_val}", count

        elif check_type == "row_count_max":
            max_val = check.get("value", 1_000_000)
            count   = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            passed  = count <= max_val
            return passed, f"Row count {count} > maximum {max_val}", count

        elif check_type == "freshness":
            max_hours  = check.get("max_hours", 24)
            date_col   = check.get("column", "updated_at")
            result     = db.execute(
                f"SELECT MAX({date_col}) FROM {table}"
            ).fetchone()[0]
            if result is None:
                return False, f"No data in '{date_col}'", None
            from datetime import datetime, timezone, timedelta
            now  = datetime.now(timezone.utc)
            diff = now - result.replace(tzinfo=timezone.utc) if hasattr(result, 'replace') else timedelta(days=999)
            passed = diff.total_seconds() / 3600 <= max_hours
            return passed, f"Most recent data is {diff.total_seconds()/3600:.1f}h old (max {max_hours}h)", None

        elif check_type == "value_range":
            min_v = check.get("min")
            max_v = check.get("max")
            where_parts = []
            if min_v is not None: where_parts.append(f"{column} < {min_v}")
            if max_v is not None: where_parts.append(f"{column} > {max_v}")
            where = " OR ".join(where_parts) if where_parts else "1=0"
            count = db.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}").fetchone()[0]
            passed = count == 0
            return passed, f"{count} values out of range [{min_v}, {max_v}] in '{column}'", count

        elif check_type == "regex_match":
            pattern = check.get("pattern", ".*")
            count   = db.execute(
                f"SELECT COUNT(*) FROM {table} WHERE NOT regexp_matches(CAST({column} AS VARCHAR), '{pattern}')"
            ).fetchone()[0]
            passed = count == 0
            return passed, f"{count} values in '{column}' don't match pattern '{pattern}'", count

        elif check_type == "custom_sql":
            sql     = check.get("sql", "SELECT 0")
            count   = db.execute(sql).fetchone()[0]
            passed  = count == 0
            return passed, f"Custom SQL check returned {count} failing rows", count

        elif check_type == "schema_match":
            expected_cols = check.get("columns", {})
            actual_schema = db.execute(f"DESCRIBE {table}").fetchall()
            actual_map    = {col[0]: col[1] for col in actual_schema}
            mismatches    = []
            for col_name, col_type in expected_cols.items():
                if col_name not in actual_map:
                    mismatches.append(f"Missing column '{col_name}'")
                elif col_type.upper() not in actual_map[col_name].upper():
                    mismatches.append(f"Column '{col_name}' type mismatch: expected {col_type}, got {actual_map[col_name]}")
            passed = len(mismatches) == 0
            return passed, "; ".join(mismatches) if mismatches else "Schema matches", len(mismatches)

        else:
            return True, f"Unknown check type '{check_type}' — skipped", None


# ─────────────────────────────────────────────────────────────────────────────
# Schema Validate Operator
# ─────────────────────────────────────────────────────────────────────────────
class SchemaValidateOperator(NextGenDatabridgeBaseOperator):
    def execute(self, context: Context) -> dict:
        run_id  = context["run_id"]
        task_id = self.task_config["task_id"]
        ti      = context["ti"]

        depends_on = self.task_config.get("depends_on", [])
        input_s3   = ti.xcom_pull(task_ids=depends_on[0], key="duckdb_path") if depends_on else None
        input_local= self.local_duckdb_path(run_id, depends_on[0] if depends_on else task_id)

        if input_s3 and not os.path.exists(input_local):
            self.download_duckdb(input_s3, input_local)

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts)

        try:
            import duckdb
            expected_schema = self.task_config.get("expected_schema", {})
            input_table     = self.task_config.get("source", {}).get("table", "raw_data")

            db = duckdb.connect(input_local, read_only=True)
            actual_schema = {col[0]: col[1] for col in db.execute(f"DESCRIBE {input_table}").fetchall()}
            db.close()

            issues = []
            for col_name, col_type in expected_schema.items():
                if col_name not in actual_schema:
                    issues.append({"column": col_name, "issue": "missing"})
                elif col_type.upper() not in actual_schema[col_name].upper():
                    issues.append({
                        "column": col_name,
                        "issue": "type_mismatch",
                        "expected": col_type,
                        "actual": actual_schema[col_name],
                    })

            passed = len(issues) == 0
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()

            self._write_task_run(
                run_id, "success" if passed else "failed",
                start_time=start_ts,
                end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                qc_results={"schema_issues": issues},
                qc_passed=passed,
                qc_failures=len(issues),
            )

            context["ti"].xcom_push(key="schema_valid", value=passed)
            context["ti"].xcom_push(key="schema_issues", value=issues)
            context["ti"].xcom_push(key="duckdb_path", value=input_s3)  # pass through

            if not passed and self.task_config.get("fail_on_error", True):
                raise ValueError(f"Schema validation failed: {issues}")

            return {"valid": passed, "issues": issues, "actual_schema": actual_schema}

        except ValueError:
            raise
        except Exception as e:
            self._write_task_run(run_id, "failed", start_time=start_ts,
                                 error_message=str(e)[:2000])
            raise


# ─────────────────────────────────────────────────────────────────────────────
# Kafka Operator
# ─────────────────────────────────────────────────────────────────────────────
class KafkaOperator(NextGenDatabridgeBaseOperator):
    def execute(self, context: Context) -> str:
        run_id   = context["run_id"]
        task_id  = self.task_config["task_id"]
        task_type= self.task_config.get("type")
        output   = self.task_config.get("output", {})

        out_s3_path    = output.get("duckdb_path") or self.duckdb_s3_path(run_id, task_id)
        out_local_path = self.local_duckdb_path(run_id, task_id)
        self.ensure_local_dir(out_local_path)

        conn_id       = self.task_config.get("connection")
        topic         = self.task_config.get("topic")
        group_id      = self.task_config.get("group_id", f"nextgen-databridge-{self.pipeline_id}")
        max_messages  = self.task_config.get("max_messages", 10_000)
        poll_timeout  = self.task_config.get("poll_timeout_seconds", 30)

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts,
                             input_sources=[{"type": "kafka", "topic": topic}])

        try:
            from kafka import KafkaConsumer
            from kafka.errors import NoBrokersAvailable
            import json as json_lib

            bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset=self.task_config.get("auto_offset_reset", "earliest"),
                enable_auto_commit=False,
                consumer_timeout_ms=poll_timeout * 1000,
                value_deserializer=lambda m: json_lib.loads(m.decode("utf-8")),
                max_poll_records=min(max_messages, 500),
            )

            records = []
            for msg in consumer:
                records.append(msg.value)
                if len(records) >= max_messages:
                    break

            consumer.close()

            import duckdb
            import pandas as pd
            df = pd.DataFrame(records) if records else pd.DataFrame()
            row_count = len(df)

            db = duckdb.connect(out_local_path)
            output_table = output.get("table", "kafka_messages")
            db.execute(f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM df")
            schema_info = db.execute(f"DESCRIBE {output_table}").fetchall()
            schema_dict = {col[0]: col[1] for col in schema_info}
            db.close()

            self.upload_duckdb(out_local_path, out_s3_path)
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()

            self._write_task_run(
                run_id, "success",
                start_time=start_ts,
                end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                output_duckdb_path=out_s3_path,
                output_table=output_table,
                output_row_count=row_count,
                output_schema=schema_dict,
            )

            context["ti"].xcom_push(key="duckdb_path", value=out_s3_path)
            context["ti"].xcom_push(key="row_count", value=row_count)
            return out_s3_path

        except Exception as e:
            self._write_task_run(run_id, "failed", start_time=start_ts,
                                 error_message=str(e)[:2000],
                                 error_traceback=traceback.format_exc()[:5000])
            raise


# ─────────────────────────────────────────────────────────────────────────────
# GCP PubSub Operator
# ─────────────────────────────────────────────────────────────────────────────
class PubSubOperator(NextGenDatabridgeBaseOperator):
    def execute(self, context: Context) -> str:
        run_id     = context["run_id"]
        task_id    = self.task_config["task_id"]
        task_type  = self.task_config.get("type")
        output     = self.task_config.get("output", {})
        project_id = self.task_config.get("project_id") or os.getenv("GCP_PROJECT_ID")
        subscription = self.task_config.get("subscription")
        topic        = self.task_config.get("topic")
        max_messages = self.task_config.get("max_messages", 10_000)

        out_s3_path    = output.get("duckdb_path") or self.duckdb_s3_path(run_id, task_id)
        out_local_path = self.local_duckdb_path(run_id, task_id)
        self.ensure_local_dir(out_local_path)

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts,
                             input_sources=[{"type": "pubsub", "topic": topic, "subscription": subscription}])
        try:
            from google.cloud import pubsub_v1
            import json as json_lib

            subscriber = pubsub_v1.SubscriberClient()
            sub_path   = subscriber.subscription_path(project_id, subscription)

            records = []
            def callback(message):
                try:
                    data = json_lib.loads(message.data.decode("utf-8"))
                    records.append(data)
                except Exception:
                    records.append({"raw": message.data.decode("utf-8")})
                message.ack()

            future = subscriber.subscribe(sub_path, callback=callback)
            import concurrent.futures
            try:
                future.result(timeout=self.task_config.get("poll_timeout_seconds", 60))
            except concurrent.futures.TimeoutError:
                future.cancel()

            import duckdb
            import pandas as pd
            df = pd.DataFrame(records[:max_messages]) if records else pd.DataFrame()
            row_count = len(df)

            db = duckdb.connect(out_local_path)
            output_table = output.get("table", "pubsub_messages")
            db.execute(f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM df")
            db.close()

            self.upload_duckdb(out_local_path, out_s3_path)
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()

            self._write_task_run(
                run_id, "success",
                start_time=start_ts,
                end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                output_duckdb_path=out_s3_path,
                output_row_count=row_count,
            )
            context["ti"].xcom_push(key="duckdb_path", value=out_s3_path)
            return out_s3_path

        except Exception as e:
            self._write_task_run(run_id, "failed", start_time=start_ts, error_message=str(e)[:2000])
            raise


# ─────────────────────────────────────────────────────────────────────────────
# File Ingest Operator
# ─────────────────────────────────────────────────────────────────────────────
class FileIngestOperator(NextGenDatabridgeBaseOperator):
    def execute(self, context: Context) -> str:
        run_id   = context["run_id"]
        task_id  = self.task_config["task_id"]
        source   = self.task_config.get("source", {})
        output   = self.task_config.get("output", {})

        out_s3_path    = output.get("duckdb_path") or self.duckdb_s3_path(run_id, task_id)
        out_local_path = self.local_duckdb_path(run_id, task_id)
        self.ensure_local_dir(out_local_path)

        file_path  = source.get("path") or source.get("s3_path")
        file_format= source.get("format", "csv").lower()

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts,
                             input_sources=[{"type": "file", "path": file_path, "format": file_format}])
        try:
            import pandas as pd

            if file_path and file_path.startswith("s3://"):
                local_file = f"/tmp/nextgen_databridge/{run_id}/input.{file_format}"
                self.ensure_local_dir(local_file)
                s3 = self.get_s3_client()
                bucket, key = file_path.replace("s3://", "").split("/", 1)
                s3.download_file(bucket, key, local_file)
                file_path = local_file

            if file_format in ("csv", "tsv"):
                sep = "\t" if file_format == "tsv" else source.get("delimiter", ",")
                df  = pd.read_csv(file_path, sep=sep, **source.get("read_options", {}))
            elif file_format == "parquet":
                df = pd.read_parquet(file_path)
            elif file_format in ("json", "jsonl", "ndjson"):
                df = pd.read_json(file_path, lines=True)
            elif file_format in ("xlsx", "xls"):
                df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")

            import duckdb
            row_count = len(df)
            output_table = output.get("table", "file_data")

            db = duckdb.connect(out_local_path)
            db.execute(f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM df")
            schema_info = db.execute(f"DESCRIBE {output_table}").fetchall()
            db.close()

            self.upload_duckdb(out_local_path, out_s3_path)
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()

            self._write_task_run(
                run_id, "success",
                start_time=start_ts,
                end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                output_duckdb_path=out_s3_path,
                output_row_count=row_count,
            )
            context["ti"].xcom_push(key="duckdb_path", value=out_s3_path)
            return out_s3_path

        except Exception as e:
            self._write_task_run(run_id, "failed", start_time=start_ts, error_message=str(e)[:2000])
            raise


# ─────────────────────────────────────────────────────────────────────────────
# EKS Job Operator — submits a Kubernetes Job for heavy compute
# ─────────────────────────────────────────────────────────────────────────────
class EKSJobOperator(NextGenDatabridgeBaseOperator):
    def __init__(self, image: str, cpu_request: str, memory_request: str,
                 cpu_limit: str, memory_limit: str, namespace: str,
                 eks_cluster_name: str, service_account: str, env_vars: dict, **kwargs):
        super().__init__(**kwargs)
        self.image            = image
        self.cpu_request      = cpu_request
        self.memory_request   = memory_request
        self.cpu_limit        = cpu_limit
        self.memory_limit     = memory_limit
        self.namespace        = namespace
        self.eks_cluster_name = eks_cluster_name
        self.service_account  = service_account
        self.env_vars         = env_vars or {}

    def execute(self, context: Context) -> str:
        run_id   = context["run_id"]
        task_id  = self.task_config["task_id"]
        ti       = context["ti"]

        # Kubernetes names must be RFC 1123 subdomains: [a-z0-9-], max 63 chars,
        # start/end alphanumeric. run_id contains '+' and ':' from ISO timestamps.
        # Attempt number is included so each Airflow retry creates a distinct Job
        # (re-using the same name on retry causes a 409 Conflict from the API server).
        import re as _re
        attempt = getattr(ti, "try_number", 1)
        _raw = f"df-{self.pipeline_id[:18]}-{task_id[:18]}-{run_id[-12:]}-a{attempt}".lower()
        job_name = _re.sub(r"-+", "-", _re.sub(r"[^a-z0-9-]", "-", _raw)).strip("-")[:63].rstrip("-")
        out_s3   = self.duckdb_s3_path(run_id, task_id)

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts,
                             input_sources=[],
                             metrics={"cpu_request": self.cpu_request, "memory_request": self.memory_request})

        # Collect input DuckDB paths from upstream tasks.
        # Walk through passthrough tasks (data_quality, schema_validation) to the
        # actual producing task so aliases match the SQL database references.
        input_paths = {}
        for dep in self.task_config.get("depends_on", []):
            producer_id = self._find_duckdb_producer_task(ti, dep)
            if producer_id and producer_id not in input_paths:
                input_paths[producer_id] = self.duckdb_s3_path(run_id, producer_id)

        try:
            import base64
            import tempfile
            import boto3
            from botocore.signers import RequestSigner
            from kubernetes import client as k8s_client

            region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
            cluster_name = os.getenv("EKS_CLUSTER_NAME", "nextgen-databridge")

            # Describe cluster to get endpoint and CA
            eks_boto = boto3.client("eks", region_name=region)
            cluster_info = eks_boto.describe_cluster(name=cluster_name)["cluster"]

            # Generate a presigned STS GetCallerIdentity URL — this is the EKS bearer token
            boto_session = boto3.session.Session()
            sts_client = boto_session.client("sts", region_name=region)
            service_id = sts_client.meta.service_model.service_id
            signer = RequestSigner(
                service_id, region, "sts", "v4",
                boto_session.get_credentials(), boto_session.events,
            )
            presigned = signer.generate_presigned_url(
                {
                    "method": "GET",
                    "url": f"https://sts.{region}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
                    "body": {},
                    "headers": {"x-k8s-aws-id": cluster_name},
                    "context": {},
                },
                region_name=region,
                expires_in=60,
                operation_name="GetCallerIdentity",
            )
            k8s_token = "k8s-aws-v1." + base64.urlsafe_b64encode(
                presigned.encode("utf-8")
            ).decode("utf-8").rstrip("=")

            # Write the CA cert to a temp file so the k8s client can verify TLS
            ca_bytes = base64.b64decode(cluster_info["certificateAuthority"]["data"])
            ca_file = tempfile.NamedTemporaryFile(delete=False, suffix=".crt")
            ca_file.write(ca_bytes)
            ca_file.flush()
            ca_file.close()

            configuration = k8s_client.Configuration()
            configuration.host = cluster_info["endpoint"]
            configuration.ssl_ca_cert = ca_file.name
            configuration.api_key = {"authorization": f"Bearer {k8s_token}"}
            k8s_client.Configuration.set_default(configuration)

            batch_v1 = k8s_client.BatchV1Api()

            # Build env vars
            env = [
                k8s_client.V1EnvVar(name="PIPELINE_ID", value=self.pipeline_id),
                k8s_client.V1EnvVar(name="RUN_ID", value=run_id),
                k8s_client.V1EnvVar(name="TASK_ID", value=task_id),
                k8s_client.V1EnvVar(name="DS", value=context.get("ds", "")),
                k8s_client.V1EnvVar(name="OUTPUT_DUCKDB_PATH", value=out_s3),
                k8s_client.V1EnvVar(name="INPUT_PATHS", value=json.dumps(input_paths)),
                k8s_client.V1EnvVar(name="TASK_CONFIG", value=json.dumps(self.task_config)),
                k8s_client.V1EnvVar(name="AUDIT_DB_URL", value=self.audit_db_url or ""),
                k8s_client.V1EnvVar(name="DUCKDB_BUCKET", value=self.duckdb_bucket),
                k8s_client.V1EnvVar(name="AWS_ENDPOINT_URL", value=os.getenv("AWS_ENDPOINT_URL", "")),
            ]
            for k, v in self.env_vars.items():
                env.append(k8s_client.V1EnvVar(name=k, value=str(v)))

            manifest = k8s_client.V1Job(
                api_version="batch/v1",
                kind="Job",
                metadata=k8s_client.V1ObjectMeta(
                    name=job_name,
                    namespace=self.namespace,
                    labels={
                        "app": "nextgen-databridge",
                        "pipeline": self.pipeline_id,
                        "task": task_id,
                    },
                ),
                spec=k8s_client.V1JobSpec(
                    backoff_limit=self.task_config.get("retries", 3),
                    ttl_seconds_after_finished=3600,
                    template=k8s_client.V1PodTemplateSpec(
                        spec=k8s_client.V1PodSpec(
                            restart_policy="Never",
                            service_account_name=self.service_account,
                            tolerations=[
                                k8s_client.V1Toleration(
                                    key="nextgen-databridge-job",
                                    operator="Equal",
                                    value="true",
                                    effect="NoSchedule",
                                )
                            ],
                            containers=[
                                k8s_client.V1Container(
                                    name="transform",
                                    image=self.image,
                                    env=env,
                                    resources=k8s_client.V1ResourceRequirements(
                                        requests={"cpu": self.cpu_request, "memory": self.memory_request},
                                        limits={"cpu": self.cpu_limit, "memory": self.memory_limit},
                                    ),
                                )
                            ],
                        )
                    ),
                ),
            )

            batch_v1.create_namespaced_job(namespace=self.namespace, body=manifest)
            logger.info(f"EKS Job submitted: {job_name}")

            # Poll until complete
            timeout_secs = self.task_config.get("timeout_minutes", 120) * 60
            elapsed = 0
            while elapsed < timeout_secs:
                time.sleep(15)
                elapsed += 15
                job = batch_v1.read_namespaced_job(name=job_name, namespace=self.namespace)
                if job.status.succeeded:
                    break
                if job.status.failed and job.status.failed >= manifest.spec.backoff_limit:
                    raise RuntimeError(f"EKS Job {job_name} failed after {job.status.failed} attempts")

            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(
                run_id, "success",
                start_time=start_ts,
                end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                output_duckdb_path=out_s3,
                eks_job_name=job_name,
            )

            context["ti"].xcom_push(key="duckdb_path", value=out_s3)
            return out_s3

        except Exception as e:
            self._write_task_run(run_id, "failed", start_time=start_ts,
                                 error_message=str(e)[:2000], eks_job_name=job_name)
            raise


# ─────────────────────────────────────────────────────────────────────────────
# Load Target Operator — write DuckDB output to target system
# ─────────────────────────────────────────────────────────────────────────────
class LoadTargetOperator(NextGenDatabridgeBaseOperator):
    def execute(self, context: Context) -> str:
        run_id  = context["run_id"]
        task_id = self.task_config["task_id"]
        ti      = context["ti"]
        target  = self.task_config.get("target", {})

        depends_on = self.task_config.get("depends_on", [])
        input_s3   = ti.xcom_pull(task_ids=depends_on[0], key="duckdb_path") if depends_on else None
        input_local= self.local_duckdb_path(run_id, depends_on[0] if depends_on else task_id)

        if input_s3 and not os.path.exists(input_local):
            self.download_duckdb(input_s3, input_local)

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts,
                             input_sources=[{"type": "duckdb", "path": input_s3}])
        try:
            import duckdb
            target_type  = target.get("type", "s3")
            target_table = target.get("table", "output")
            write_mode   = target.get("mode", "append")  # append, overwrite, upsert

            db = duckdb.connect(input_local, read_only=True)
            source_table = self.task_config.get("source", {}).get("table", "result")
            df = db.execute(f"SELECT * FROM {source_table}").df()
            db.close()
            row_count = len(df)

            if target_type in ("sqlserver", "mssql", "oracle", "postgresql", "postgres"):
                self._write_to_rdbms(df, target, write_mode)
            elif target_type in ("s3", "parquet", "csv"):
                self._write_to_s3(df, target)
            elif target_type == "kafka":
                self._write_to_kafka(df, target)
            elif target_type == "pubsub":
                self._write_to_pubsub(df, target)
            else:
                # Default: write as Parquet to S3
                self._write_to_s3(df, target)

            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(
                run_id, "success",
                start_time=start_ts,
                end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                output_row_count=row_count,
            )
            return f"Loaded {row_count} rows to {target_type}:{target.get('path', target_table)}"

        except Exception as e:
            self._write_task_run(run_id, "failed", start_time=start_ts, error_message=str(e)[:2000])
            raise

    def _to_sql_drop_rowversion(self, df, table: str, engine, schema: str, if_exists: str):
        """Write df to SQL Server, automatically dropping rowversion/timestamp columns.
        Passes an active Connection (not Engine) to df.to_sql so pandas defers transaction
        management to the caller, avoiding pymssql error 3902 (COMMIT with no BEGIN)."""
        import re
        from sqlalchemy import text as sa_text

        def _write(frame, conn):
            frame.to_sql(table, conn, schema=schema, if_exists=if_exists,
                         index=False, chunksize=5000)

        try:
            with engine.begin() as conn:
                _write(df, conn)
        except Exception as first_err:
            err_str = str(first_err)
            # SQL Server error 273: cannot insert explicit value into timestamp/rowversion
            if "273" not in err_str and "timestamp column" not in err_str.lower():
                raise

            # Strategy 1: INFORMATION_SCHEMA (covers standard timestamp/rowversion)
            dropped = []
            try:
                with engine.connect() as conn:
                    rows = conn.execute(sa_text(
                        f"SELECT c.name FROM sys.columns c "
                        f"JOIN sys.types t ON c.user_type_id = t.user_type_id "
                        f"JOIN sys.tables tbl ON c.object_id = tbl.object_id "
                        f"JOIN sys.schemas s ON tbl.schema_id = s.schema_id "
                        f"WHERE s.name = '{schema}' AND tbl.name = '{table}' "
                        f"AND t.name IN ('timestamp', 'rowversion')"
                    )).fetchall()
                    dropped = [r[0] for r in rows]
            except Exception:
                pass

            # Strategy 2: parse column name from error message
            if not dropped:
                match = re.search(r"\[([^\]]+)\].*timestamp", err_str, re.IGNORECASE)
                if match:
                    dropped = [match.group(1)]

            # Strategy 3: drop any DataFrame datetime column that the table lists as
            # timestamp/rowversion via INFORMATION_SCHEMA (broader fallback)
            if not dropped:
                try:
                    with engine.connect() as conn:
                        rows = conn.execute(sa_text(
                            f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                            f"WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}' "
                            f"AND DATA_TYPE IN ('timestamp','rowversion')"
                        )).fetchall()
                        dropped = [r[0] for r in rows]
                except Exception:
                    pass

            if not dropped:
                raise

            df2 = df.drop(columns=[c for c in dropped if c in df.columns], errors="ignore")
            logger.info(f"Retrying load after dropping rowversion columns: {dropped}")
            with engine.begin() as conn:
                _write(df2, conn)

    def _write_to_rdbms(self, df, target: dict, mode: str):
        from sqlalchemy import create_engine, text
        conn_id = target.get("connection")
        schema  = target.get("schema", "dbo")
        table   = target.get("table", "output")

        db_url = self._resolve_rdbms_url(conn_id, target)
        engine = create_engine(db_url)

        # etl_loaded_at is declared DEFAULT SYSDATETIME() on every target table; excluding
        # it from the INSERT lets SQL Server auto-populate it and avoids error 273 when the
        # column was originally created with type timestamp/rowversion instead of DATETIME2.
        df = df.drop(columns=["etl_loaded_at"], errors="ignore")

        if mode == "overwrite":
            # TRUNCATE + append instead of if_exists='replace'.
            # 'replace' issues DROP+CREATE (DDL), which auto-commits the pymssql
            # transaction, causing error 3902 when SQLAlchemy tries to COMMIT.
            with engine.begin() as conn:
                exists = conn.execute(text(
                    f"SELECT OBJECT_ID('{schema}.{table}')"
                )).scalar()
                if exists:
                    conn.execute(text(f"TRUNCATE TABLE [{schema}].[{table}]"))
            self._to_sql_drop_rowversion(df, table, engine, schema, "append")
        else:
            self._to_sql_drop_rowversion(df, table, engine, schema, "append")

    def _resolve_rdbms_url(self, conn_id: str, target: dict) -> str:
        """Build a SQLAlchemy URL, preferring Airflow connection over target dict fields."""
        if conn_id:
            try:
                from airflow.hooks.base import BaseHook
                ac = BaseHook.get_connection(conn_id)
                conn_type = ac.conn_type or target.get("type", "mssql")
                host      = ac.host     or target.get("_host", "localhost")
                port      = ac.port     or target.get("_port", 1433)
                login     = ac.login
                password  = ac.password
                # target "database" key overrides the connection's schema/database
                schema = target.get("database") or ac.schema or target.get("_database", "master")

                driver_map = {
                    "mssql":     "mssql+pymssql",
                    "sqlserver": "mssql+pymssql",
                    "oracle":    "oracle+cx_oracle",
                    "postgres":  "postgresql",
                    "postgresql":"postgresql",
                }
                dialect = driver_map.get(conn_type.lower(), conn_type)
                return f"{dialect}://{login}:{password}@{host}:{port}/{schema}"
            except Exception as e:
                logger.warning(f"BaseHook lookup failed for '{conn_id}', falling back to target dict: {e}")

        return self._build_db_url(conn_id, target)

    def _write_to_s3(self, df, target: dict):
        import pyarrow as pa
        import pyarrow.parquet as pq
        bucket  = target.get("bucket") or self.duckdb_bucket
        key     = target.get("path", f"output/{self.pipeline_id}/output.parquet")
        local_f = f"/tmp/nextgen_databridge/output_{uuid.uuid4().hex[:8]}.parquet"
        pq.write_table(pa.Table.from_pandas(df), local_f)
        s3 = self.get_s3_client()
        s3.upload_file(local_f, bucket, key)
        os.unlink(local_f)

    def _write_to_kafka(self, df, target: dict):
        from kafka import KafkaProducer
        import json as json_lib
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            value_serializer=lambda v: json_lib.dumps(v).encode("utf-8"),
        )
        topic = target.get("topic", self.pipeline_id)
        for record in df.to_dict("records"):
            producer.send(topic, record)
        producer.flush()
        producer.close()

    def _write_to_pubsub(self, df, target: dict):
        from google.cloud import pubsub_v1
        import json as json_lib
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            target.get("project_id", os.getenv("GCP_PROJECT_ID")),
            target.get("topic", self.pipeline_id)
        )
        for record in df.to_dict("records"):
            publisher.publish(topic_path, json_lib.dumps(record).encode("utf-8"))

    def _build_db_url(self, conn_id: str, target: dict) -> str:
        t = target.get("type", "postgresql")
        h = target.get("host", "localhost")
        p = target.get("port", 5432)
        u = target.get("username", "")
        pw= target.get("password", "")
        db= target.get("database", "")
        return f"{t}://{u}:{pw}@{h}:{p}/{db}"


# ─────────────────────────────────────────────────────────────────────────────
# Notification Operator
# ─────────────────────────────────────────────────────────────────────────────
class NotificationOperator(NextGenDatabridgeBaseOperator):
    def execute(self, context: Context) -> str:
        run_id  = context["run_id"]
        task_id = self.task_config["task_id"]
        channels= self.task_config.get("channels", [])
        message = self.task_config.get("message", f"Pipeline {self.pipeline_id} - Task {task_id} completed")
        subject = self.task_config.get("subject", f"NextGenDatabridge Notification: {self.pipeline_id}")

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts)

        sent = []
        for channel in channels:
            try:
                if channel.startswith("slack:"):
                    self._send_slack(channel.replace("slack:", ""), message, context)
                    sent.append(channel)
                elif channel.startswith("email:"):
                    self._send_email(channel.replace("email:", ""), subject, message)
                    sent.append(channel)
            except Exception as e:
                logger.warning(f"Notification to {channel} failed: {e}")

        duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
        self._write_task_run(run_id, "success", start_time=start_ts,
                             end_time=datetime.now(timezone.utc),
                             duration_seconds=duration,
                             metrics={"notified_channels": sent})
        return f"Notified: {sent}"

    def _send_slack(self, channel: str, message: str, context: dict):
        import urllib.request
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not set, skipping Slack notification")
            return
        payload = json.dumps({"channel": channel, "text": message}).encode()
        req = urllib.request.Request(webhook_url, data=payload, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)

    def _send_email(self, recipient: str, subject: str, body: str):
        import smtplib
        from email.mime.text import MIMEText
        smtp_host = os.getenv("SMTP_HOST", "localhost")
        smtp_port = int(os.getenv("SMTP_PORT", "25"))
        from_addr = os.getenv("SMTP_FROM", "nextgen-databridge@platform.internal")
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"]    = from_addr
        msg["To"]      = recipient
        with smtplib.SMTP(smtp_host, smtp_port) as s:
            s.sendmail(from_addr, [recipient], msg.as_string())
