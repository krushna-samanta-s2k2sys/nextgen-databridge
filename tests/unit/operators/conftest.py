"""
Shared fixtures for operator unit tests.
"""
import os
import sys
import json
import duckdb
import pytest
from unittest.mock import MagicMock, patch

# Ensure airflow/dags is on the path for operator imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../airflow/dags"))


def make_op(cls, task_config, pipeline_config=None, **kwargs):
    """Instantiate any NextGenDatabridge operator with minimal boilerplate."""
    return cls(
        pipeline_id="test_pipeline",
        task_config={"task_id": "test_task", **task_config},
        pipeline_config=pipeline_config or {
            "pipeline_id": "test_pipeline",
            "tasks": [{"task_id": "test_task", "type": task_config.get("type", "sql_extract")}],
        },
        duckdb_bucket="test-bucket",
        audit_db_url="postgresql://u:p@localhost/testdb",
        task_id=task_config.get("task_id", "test_task"),
        **kwargs,
    )


def make_ctx(run_id="run_001", ds="2026-05-04", xcom_data=None):
    """Build a minimal Airflow context mock."""
    ti = MagicMock()
    xcom_data = xcom_data or {}
    ti.xcom_pull.side_effect = lambda task_ids=None, key=None: xcom_data.get(
        (task_ids, key)
    )
    return {"run_id": run_id, "ds": ds, "ti": ti, "dag_run": MagicMock()}


def seed_duckdb(path: str, table: str, rows: list, columns: list):
    """Create a DuckDB file with a single table for testing."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = duckdb.connect(path)
    if rows:
        col_defs = ", ".join(f"{c} VARCHAR" for c in columns)
        conn.execute(f"CREATE TABLE {table} ({col_defs})")
        for row in rows:
            vals = ", ".join(f"'{v}'" if v is not None else "NULL" for v in row)
            conn.execute(f"INSERT INTO {table} VALUES ({vals})")
    else:
        col_defs = ", ".join(f"{c} VARCHAR" for c in columns)
        conn.execute(f"CREATE TABLE {table} ({col_defs})")
    conn.close()


@pytest.fixture
def noop_audit(monkeypatch):
    """Patch audit DB calls so tests don't need a real DB."""
    from operators.base import NextGenDatabridgeBaseOperator
    monkeypatch.setattr(NextGenDatabridgeBaseOperator, "_write_task_run", lambda *a, **kw: None)
    monkeypatch.setattr(NextGenDatabridgeBaseOperator, "_write_audit_log", lambda *a, **kw: None)


@pytest.fixture
def noop_s3(monkeypatch):
    """Patch S3 helpers so tests don't need real AWS."""
    from operators.base import NextGenDatabridgeBaseOperator
    monkeypatch.setattr(NextGenDatabridgeBaseOperator, "download_duckdb", lambda *a, **kw: None)
    monkeypatch.setattr(NextGenDatabridgeBaseOperator, "upload_duckdb", lambda *a, **kw: None)
    monkeypatch.setattr(NextGenDatabridgeBaseOperator, "get_s3_client", lambda self: MagicMock())
