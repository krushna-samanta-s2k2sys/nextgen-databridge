"""
File Ingest Operator
Reads CSV, TSV, Parquet, JSON/JSONL, or Excel files from local disk or S3
and writes them as a DuckDB table on S3.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from airflow.utils.context import Context

from operators.base import NextGenDatabridgeBaseOperator

logger = logging.getLogger("nextgen_databridge.operators.file_ingest")


class FileIngestOperator(NextGenDatabridgeBaseOperator):

    def execute(self, context: Context) -> str:
        run_id  = context["run_id"]
        task_id = self.task_config["task_id"]
        source  = self.task_config.get("source", {})
        output  = self.task_config.get("output", {})

        out_s3    = output.get("duckdb_path") or self.duckdb_s3_path(run_id, task_id)
        out_local = self.local_duckdb_path(run_id, task_id)
        self.ensure_local_dir(out_local)

        file_path   = source.get("path") or source.get("s3_path")
        file_format = source.get("format", "csv").lower()

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts,
                             input_sources=[{"type": "file", "path": file_path,
                                             "format": file_format}])
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
            row_count    = len(df)
            output_table = output.get("table", "file_data")

            db = duckdb.connect(out_local)
            db.execute(f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM df")
            db.close()

            self.upload_duckdb(out_local, out_s3)
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()

            self._write_task_run(
                run_id, "success",
                start_time=start_ts, end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                output_duckdb_path=out_s3, output_row_count=row_count,
            )
            context["ti"].xcom_push(key="duckdb_path", value=out_s3)
            return out_s3

        except Exception as e:
            self._write_task_run(run_id, "failed", start_time=start_ts,
                                 error_message=str(e)[:2000])
            raise
