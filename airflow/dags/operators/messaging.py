"""
Messaging Operators — Kafka and GCP PubSub
KafkaOperator  — consume messages from a Kafka topic into DuckDB.
PubSubOperator — consume messages from a GCP PubSub subscription into DuckDB.
"""
from __future__ import annotations

import json
import logging
import os
import traceback
from datetime import datetime, timezone

from airflow.utils.context import Context

from operators.base import NextGenDatabridgeBaseOperator

logger = logging.getLogger("nextgen_databridge.operators.messaging")


class KafkaOperator(NextGenDatabridgeBaseOperator):

    def execute(self, context: Context) -> str:
        run_id  = context["run_id"]
        task_id = self.task_config["task_id"]
        output  = self.task_config.get("output", {})

        out_s3    = output.get("duckdb_path") or self.duckdb_s3_path(run_id, task_id)
        out_local = self.local_duckdb_path(run_id, task_id)
        self.ensure_local_dir(out_local)

        topic        = self.task_config.get("topic")
        group_id     = self.task_config.get("group_id", f"nextgen-databridge-{self.pipeline_id}")
        max_messages = self.task_config.get("max_messages", 10_000)
        poll_timeout = self.task_config.get("poll_timeout_seconds", 30)

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts,
                             input_sources=[{"type": "kafka", "topic": topic}])

        try:
            from kafka import KafkaConsumer

            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                group_id=group_id,
                auto_offset_reset=self.task_config.get("auto_offset_reset", "earliest"),
                enable_auto_commit=False,
                consumer_timeout_ms=poll_timeout * 1000,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
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
            df           = pd.DataFrame(records) if records else pd.DataFrame()
            row_count    = len(df)
            output_table = output.get("table", "kafka_messages")

            db = duckdb.connect(out_local)
            if records:
                db.execute(f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM df")
            else:
                db.execute(f"CREATE OR REPLACE TABLE {output_table} (message VARCHAR)")
            schema_dict = {col[0]: col[1] for col in db.execute(f"DESCRIBE {output_table}").fetchall()}
            db.close()

            self.upload_duckdb(out_local, out_s3)
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()

            self._write_task_run(
                run_id, "success",
                start_time=start_ts, end_time=datetime.now(timezone.utc),
                duration_seconds=duration,
                output_duckdb_path=out_s3, output_table=output_table,
                output_row_count=row_count, output_schema=schema_dict,
            )
            context["ti"].xcom_push(key="duckdb_path", value=out_s3)
            context["ti"].xcom_push(key="row_count", value=row_count)
            return out_s3

        except Exception as e:
            self._write_task_run(run_id, "failed", start_time=start_ts,
                                 error_message=str(e)[:2000],
                                 error_traceback=traceback.format_exc()[:5000])
            raise


class PubSubOperator(NextGenDatabridgeBaseOperator):

    def execute(self, context: Context) -> str:
        run_id       = context["run_id"]
        task_id      = self.task_config["task_id"]
        output       = self.task_config.get("output", {})
        project_id   = self.task_config.get("project_id") or os.getenv("GCP_PROJECT_ID")
        subscription = self.task_config.get("subscription")
        topic        = self.task_config.get("topic")
        max_messages = self.task_config.get("max_messages", 10_000)

        out_s3    = output.get("duckdb_path") or self.duckdb_s3_path(run_id, task_id)
        out_local = self.local_duckdb_path(run_id, task_id)
        self.ensure_local_dir(out_local)

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts,
                             input_sources=[{"type": "pubsub", "topic": topic,
                                             "subscription": subscription}])
        try:
            import concurrent.futures
            from google.cloud import pubsub_v1

            subscriber = pubsub_v1.SubscriberClient()
            sub_path   = subscriber.subscription_path(project_id, subscription)
            records    = []

            def callback(message):
                try:
                    data = json.loads(message.data.decode("utf-8"))
                    records.append(data)
                except Exception:
                    records.append({"raw": message.data.decode("utf-8")})
                message.ack()

            future = subscriber.subscribe(sub_path, callback=callback)
            try:
                future.result(timeout=self.task_config.get("poll_timeout_seconds", 60))
            except concurrent.futures.TimeoutError:
                future.cancel()

            import duckdb
            import pandas as pd
            df           = pd.DataFrame(records[:max_messages]) if records else pd.DataFrame()
            row_count    = len(df)
            output_table = output.get("table", "pubsub_messages")

            db = duckdb.connect(out_local)
            if records:
                db.execute(f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM df")
            else:
                db.execute(f"CREATE OR REPLACE TABLE {output_table} (message VARCHAR)")
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
