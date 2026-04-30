"""
SQL Extract Operator
Extracts data from SQL Server, Oracle, or PostgreSQL into a DuckDB file on S3.
Supports full-load, incremental, and CDC modes.

Oracle driver precedence: python-oracledb (thin mode, no client required) → cx_Oracle fallback.
"""
from __future__ import annotations

import logging
import os
import traceback
from datetime import datetime, timezone

from airflow.utils.context import Context

from operators.base import NextGenDatabridgeBaseOperator

logger = logging.getLogger("nextgen_databridge.operators.sql_extract")


class SQLExtractOperator(NextGenDatabridgeBaseOperator):

    def execute(self, context: Context) -> str:
        run_id  = context["run_id"]
        task_id = self.task_config["task_id"]
        source  = self.task_config.get("source", {})
        output  = self.task_config.get("output", {})

        s3_path    = output.get("duckdb_path") or self.duckdb_s3_path(run_id, task_id)
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

        self._write_task_run(run_id, "running", start_time=start_ts, input_sources=input_sources)
        self._write_audit_log("TASK_STARTED", run_id, {
            "task_type": "sql_extract",
            "source": source.get("connection"),
        })

        try:
            conn_id    = source.get("connection")
            query      = source.get("query", f"SELECT * FROM {source.get('table', 'unknown')}")
            batch_size = source.get("batch_size", 100_000)

            ds = context.get("ds", "")
            query = (query
                     .replace("{{ ds }}", ds).replace("{{ds}}", ds)
                     .replace("{{ run_id }}", run_id).replace("{{run_id}}", run_id))

            try:
                from airflow.hooks.base import BaseHook
                airflow_conn = BaseHook.get_connection(conn_id)
                conn_type = airflow_conn.conn_type
                host      = airflow_conn.host
                port      = airflow_conn.port
                login     = airflow_conn.login
                password  = airflow_conn.password
                schema    = airflow_conn.schema
            except Exception:
                conn_type = source.get("connection_type", "sqlserver")
                host, port, login, password, schema = self._conn_from_env(conn_id)

            df = self._extract_data(conn_type, host, port, login, password, schema, query, batch_size)

            import duckdb
            row_count    = len(df) if hasattr(df, "__len__") else 0
            output_table = output.get("table", "raw_data")

            db = duckdb.connect(local_path)
            db.execute(f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM df")
            schema_info = db.execute(f"DESCRIBE {output_table}").fetchall()
            schema_dict = {col[0]: col[1] for col in schema_info}
            size_bytes  = os.path.getsize(local_path)
            db.close()

            self.upload_duckdb(local_path, s3_path)

            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(
                run_id, "success",
                start_time=start_ts, end_time=datetime.now(timezone.utc),
                duration_seconds=duration, input_sources=input_sources,
                output_duckdb_path=s3_path, output_table=output_table,
                output_row_count=row_count, output_size_bytes=size_bytes,
                output_schema=schema_dict,
            )
            self._write_audit_log("TASK_COMPLETED", run_id, {
                "output_path": s3_path, "row_count": row_count,
                "duration_seconds": round(duration, 2),
            })

            context["ti"].xcom_push(key="duckdb_path", value=s3_path)
            context["ti"].xcom_push(key="row_count", value=row_count)
            logger.info(f"SQLExtract: {task_id} extracted {row_count} rows → {s3_path}")
            return s3_path

        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(
                run_id, "failed",
                start_time=start_ts, end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                error_message=str(e)[:2000],
                error_traceback=traceback.format_exc()[:5000],
            )
            self._write_audit_log("TASK_FAILED", run_id, {"error": str(e)[:500]}, severity="error")
            raise

    # ── Driver dispatch ────────────────────────────────────────────────────────

    def _extract_data(self, conn_type, host, port, login, password, schema, query, batch_size):
        import pandas as pd

        ct = (conn_type or "").lower()

        if ct in ("mssql", "sqlserver"):
            return self._extract_sqlserver(host, port, login, password, schema, query)

        if ct == "oracle":
            return self._extract_oracle(host, port, login, password, schema, query)

        if ct in ("postgres", "postgresql"):
            return self._extract_postgresql(host, port, login, password, schema, query)

        # Generic SQLAlchemy fallback
        from sqlalchemy import create_engine
        engine = create_engine(f"{ct}://{login}:{password}@{host}:{port}/{schema}")
        return pd.read_sql(query, engine)

    def _extract_sqlserver(self, host, port, login, password, schema, query):
        import pandas as pd
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
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def _extract_oracle(self, host, port, login, password, schema, query):
        """
        Connects via python-oracledb in thin mode (no Oracle Client required).
        Falls back to cx_Oracle if oracledb is not installed.
        *schema* is used as the service_name; set it to the service name or SID.
        """
        import pandas as pd
        try:
            import oracledb
            conn = oracledb.connect(
                user=login, password=password,
                dsn=f"{host}:{port or 1521}/{schema}",
            )
        except ImportError:
            import cx_Oracle
            dsn  = cx_Oracle.makedsn(host, port or 1521, service_name=schema)
            conn = cx_Oracle.connect(login, password, dsn)
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def _extract_postgresql(self, host, port, login, password, schema, query):
        import pandas as pd
        import psycopg2
        conn = psycopg2.connect(
            host=host, port=port or 5432,
            user=login, password=password, dbname=schema,
        )
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    def _conn_from_env(self, conn_id: str):
        prefix = conn_id.upper().replace("-", "_")
        return (
            os.getenv(f"{prefix}_HOST", "localhost"),
            int(os.getenv(f"{prefix}_PORT", "1433")),
            os.getenv(f"{prefix}_USER", ""),
            os.getenv(f"{prefix}_PASS", ""),
            os.getenv(f"{prefix}_DB", ""),
        )
