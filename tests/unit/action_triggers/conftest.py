"""Shared fixtures for action_trigger unit tests."""
import os
import sys
import pytest
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../airflow/dags"))


def make_op(cls, task_config, pipeline_config=None, **kwargs):
    return cls(
        pipeline_id="test_pipeline",
        task_config={"task_id": "test_task", **task_config},
        pipeline_config=pipeline_config or {
            "pipeline_id": "test_pipeline",
            "tasks": [{"task_id": "test_task", "type": task_config.get("type", "api_call")}],
        },
        duckdb_bucket="test-bucket",
        audit_db_url="postgresql://u:p@localhost/testdb",
        task_id=task_config.get("task_id", "test_task"),
        **kwargs,
    )


def make_ctx(run_id="run_001", ds="2026-05-04", xcom_data=None):
    ti = MagicMock()
    xcom_data = xcom_data or {}
    ti.xcom_pull.side_effect = lambda task_ids=None, key=None: xcom_data.get((task_ids, key))
    return {"run_id": run_id, "ds": ds, "ti": ti, "dag_run": MagicMock()}


@pytest.fixture
def noop_audit(monkeypatch):
    from operators.base import NextGenDatabridgeBaseOperator
    monkeypatch.setattr(NextGenDatabridgeBaseOperator, "_write_task_run", lambda *a, **kw: None)
    monkeypatch.setattr(NextGenDatabridgeBaseOperator, "_write_audit_log", lambda *a, **kw: None)


@pytest.fixture
def noop_s3(monkeypatch):
    from operators.base import NextGenDatabridgeBaseOperator
    monkeypatch.setattr(NextGenDatabridgeBaseOperator, "download_duckdb", lambda *a, **kw: None)
    monkeypatch.setattr(NextGenDatabridgeBaseOperator, "upload_duckdb",   lambda *a, **kw: None)
    monkeypatch.setattr(NextGenDatabridgeBaseOperator, "get_s3_client",   lambda self: MagicMock())
