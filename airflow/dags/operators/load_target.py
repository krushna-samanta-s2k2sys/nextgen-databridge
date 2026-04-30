"""
Load Target Operator
Reads a DuckDB table produced by an upstream task and writes it to a target:
SQL Server, Oracle, PostgreSQL, S3/Parquet, Kafka, or GCP PubSub.

Oracle connectivity uses python-oracledb (thin mode) with cx_Oracle as fallback.
"""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone

from airflow.utils.context import Context

from operators.base import NextGenDatabridgeBaseOperator

logger = logging.getLogger("nextgen_databridge.operators.load_target")


class LoadTargetOperator(NextGenDatabridgeBaseOperator):

    def execute(self, context: Context) -> str:
        run_id  = context["run_id"]
        task_id = self.task_config["task_id"]
        ti      = context["ti"]
        target  = self.task_config.get("target", {})

        depends_on  = self.task_config.get("depends_on", [])
        input_s3    = ti.xcom_pull(task_ids=depends_on[0], key="duckdb_path") if depends_on else None
        input_local = self.local_duckdb_path(run_id, depends_on[0] if depends_on else task_id)

        if input_s3 and not os.path.exists(input_local):
            self.download_duckdb(input_s3, input_local)

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts,
                             input_sources=[{"type": "duckdb", "path": input_s3}])
        try:
            import duckdb
            target_type  = target.get("type", "s3")
            target_table = target.get("table", "output")
            write_mode   = target.get("mode", "append")

            db           = duckdb.connect(input_local, read_only=True)
            source_table = self.task_config.get("source", {}).get("table", "result")
            df           = db.execute(f"SELECT * FROM {source_table}").df()
            db.close()
            row_count = len(df)

            if target_type in ("sqlserver", "mssql"):
                self._write_to_rdbms(df, target, write_mode, dialect="mssql+pymssql")
            elif target_type == "oracle":
                self._write_to_rdbms(df, target, write_mode, dialect="oracle+oracledb")
            elif target_type in ("postgresql", "postgres"):
                self._write_to_rdbms(df, target, write_mode, dialect="postgresql+psycopg2")
            elif target_type in ("s3", "parquet", "csv"):
                self._write_to_s3(df, target)
            elif target_type == "kafka":
                self._write_to_kafka(df, target)
            elif target_type == "pubsub":
                self._write_to_pubsub(df, target)
            else:
                self._write_to_s3(df, target)

            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(
                run_id, "success",
                start_time=start_ts, end_time=datetime.now(timezone.utc),
                duration_seconds=duration, output_row_count=row_count,
            )
            return f"Loaded {row_count} rows to {target_type}:{target.get('path', target_table)}"

        except Exception as e:
            self._write_task_run(run_id, "failed", start_time=start_ts,
                                 error_message=str(e)[:2000])
            raise

    # ── RDBMS write ────────────────────────────────────────────────────────────

    def _write_to_rdbms(self, df, target: dict, mode: str, dialect: str):
        from sqlalchemy import create_engine, text
        conn_id = target.get("connection")
        schema  = target.get("schema", "dbo")
        table   = target.get("table", "output")

        db_url = self._resolve_rdbms_url(conn_id, target, dialect)
        engine = create_engine(db_url)

        df = df.drop(columns=["etl_loaded_at"], errors="ignore")

        if mode == "overwrite":
            with engine.begin() as conn:
                exists = conn.execute(text(
                    f"SELECT OBJECT_ID('{schema}.{table}')"
                )).scalar()
                if exists:
                    conn.execute(text(f"TRUNCATE TABLE [{schema}].[{table}]"))
            self._to_sql_safe(df, table, engine, schema, "append", dialect)
        else:
            self._to_sql_safe(df, table, engine, schema, "append", dialect)

    def _to_sql_safe(self, df, table: str, engine, schema: str, if_exists: str, dialect: str):
        """Write df to RDBMS, automatically dropping rowversion/timestamp columns on SQL Server."""
        import re
        from sqlalchemy import text as sa_text

        def _write(frame, conn):
            frame.to_sql(table, conn, schema=schema, if_exists=if_exists,
                         index=False, chunksize=5000)

        try:
            with engine.begin() as conn:
                _write(df, conn)
        except Exception as first_err:
            # Only attempt rowversion retry for SQL Server
            if "mssql" not in dialect:
                raise
            err_str = str(first_err)
            if "273" not in err_str and "timestamp column" not in err_str.lower():
                raise

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

            if not dropped:
                match = re.search(r"\[([^\]]+)\].*timestamp", err_str, re.IGNORECASE)
                if match:
                    dropped = [match.group(1)]

            if not dropped:
                raise

            df2 = df.drop(columns=[c for c in dropped if c in df.columns], errors="ignore")
            logger.info(f"Retrying load after dropping rowversion columns: {dropped}")
            with engine.begin() as conn:
                _write(df2, conn)

    def _resolve_rdbms_url(self, conn_id: str, target: dict, default_dialect: str) -> str:
        if conn_id:
            try:
                from airflow.hooks.base import BaseHook
                ac         = BaseHook.get_connection(conn_id)
                conn_type  = ac.conn_type or ""
                host       = ac.host     or target.get("_host", "localhost")
                port       = ac.port     or target.get("_port", 1433)
                login      = ac.login
                password   = ac.password
                schema     = target.get("database") or ac.schema or "master"

                _dialect_map = {
                    "mssql":      "mssql+pymssql",
                    "sqlserver":  "mssql+pymssql",
                    "oracle":     "oracle+oracledb",
                    "postgres":   "postgresql+psycopg2",
                    "postgresql": "postgresql+psycopg2",
                }
                dialect = _dialect_map.get(conn_type.lower(), default_dialect)
                return f"{dialect}://{login}:{password}@{host}:{port}/{schema}"
            except Exception as e:
                logger.warning(f"BaseHook lookup failed for '{conn_id}': {e}")

        t  = target.get("type", "postgresql")
        h  = target.get("host", "localhost")
        p  = target.get("port", 5432)
        u  = target.get("username", "")
        pw = target.get("password", "")
        db = target.get("database", "")
        return f"{default_dialect}://{u}:{pw}@{h}:{p}/{db}"

    # ── S3 / streaming writes ──────────────────────────────────────────────────

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
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        topic = target.get("topic", self.pipeline_id)
        for record in df.to_dict("records"):
            producer.send(topic, record)
        producer.flush()
        producer.close()

    def _write_to_pubsub(self, df, target: dict):
        from google.cloud import pubsub_v1
        publisher  = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(
            target.get("project_id", os.getenv("GCP_PROJECT_ID")),
            target.get("topic", self.pipeline_id),
        )
        for record in df.to_dict("records"):
            publisher.publish(topic_path, json.dumps(record).encode("utf-8"))
