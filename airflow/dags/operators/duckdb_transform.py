"""
DuckDB Transform Operator
Executes SQL transformations using DuckDB, attaching multiple upstream DuckDB
files from S3 as read-only databases and producing a new output DuckDB file.
"""
from __future__ import annotations

import logging
import os
import traceback
from datetime import datetime, timezone
from typing import Dict

from airflow.utils.context import Context

from operators.base import NextGenDatabridgeBaseOperator

logger = logging.getLogger("nextgen_databridge.operators.duckdb_transform")


class DuckDBTransformOperator(NextGenDatabridgeBaseOperator):

    def execute(self, context: Context) -> str:
        run_id  = context["run_id"]
        task_id = self.task_config["task_id"]
        ti      = context["ti"]

        sql    = self.task_config.get("sql", "")
        source = self.task_config.get("source", {})
        output = self.task_config.get("output", {})

        out_s3    = output.get("duckdb_path") or self.duckdb_s3_path(run_id, task_id)
        out_s3    = out_s3.replace("{{ run_id }}", run_id).replace("{{run_id}}", run_id)
        out_local = self.local_duckdb_path(run_id, task_id)
        self.ensure_local_dir(out_local)

        start_ts     = datetime.now(timezone.utc)
        input_sources = []
        depends_on   = self.task_config.get("depends_on", [])

        # Resolve upstream DuckDB files, walking through passthrough tasks
        local_inputs: Dict[str, str] = {}
        for dep_task_id in depends_on:
            producer_id = self._find_duckdb_producer_task(ti, dep_task_id)
            if producer_id and producer_id not in local_inputs:
                dep_s3    = self.duckdb_s3_path(run_id, producer_id)
                dep_local = self.local_duckdb_path(run_id, producer_id)
                if not os.path.exists(dep_local):
                    self.download_duckdb(dep_s3, dep_local)
                local_inputs[producer_id] = dep_local
                input_sources.append({"type": "duckdb", "task_id": producer_id, "path": dep_s3})

        if source.get("duckdb_path"):
            explicit_path  = source["duckdb_path"]
            explicit_local = f"/tmp/nextgen_databridge/{run_id}/explicit_input.duckdb"
            if explicit_path.startswith("s3://") and not os.path.exists(explicit_local):
                self.download_duckdb(explicit_path, explicit_local)
            local_inputs["_explicit"] = explicit_local
            input_sources.append({"type": "duckdb", "path": explicit_path})

        self._write_task_run(run_id, "running", start_time=start_ts, input_sources=input_sources)

        try:
            import duckdb
            db = duckdb.connect(out_local)

            for alias, local_path in local_inputs.items():
                safe_alias = alias.replace("-", "_").replace("/", "_")
                db.execute(f"ATTACH '{local_path}' AS {safe_alias} (READ_ONLY)")

            sql_rendered = self._render_sql(sql, context)
            output_table = output.get("table", "result")
            db.execute(f"CREATE OR REPLACE TABLE {output_table} AS ({sql_rendered})")

            row_count   = db.execute(f"SELECT COUNT(*) FROM {output_table}").fetchone()[0]
            schema_info = db.execute(f"DESCRIBE {output_table}").fetchall()
            schema_dict = {col[0]: col[1] for col in schema_info}
            db.close()

            size_bytes = os.path.getsize(out_local)
            self.upload_duckdb(out_local, out_s3)

            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(
                run_id, "success",
                start_time=start_ts, end_time=datetime.now(timezone.utc),
                duration_seconds=duration, input_sources=input_sources,
                output_duckdb_path=out_s3, output_table=output_table,
                output_row_count=row_count, output_size_bytes=size_bytes,
                output_schema=schema_dict,
            )
            self._write_audit_log("TASK_COMPLETED", run_id, {
                "output_path": out_s3, "row_count": row_count,
                "duration": round(duration, 2),
            })

            context["ti"].xcom_push(key="duckdb_path", value=out_s3)
            context["ti"].xcom_push(key="row_count", value=row_count)
            logger.info(f"DuckDBTransform: {task_id} produced {row_count} rows → {out_s3}")
            return out_s3

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

    def _render_sql(self, sql: str, context: Context) -> str:
        ds     = context.get("ds", "")
        run_id = context.get("run_id", "")
        return (sql
                .replace("{{ ds }}", ds).replace("{{ds}}", ds)
                .replace("{{ run_id }}", run_id).replace("{{run_id}}", run_id))
