"""Unit tests for NextGenDatabridgeBaseOperator."""
import json
import os
import pytest
from unittest.mock import MagicMock, patch, call

from tests.unit.operators.conftest import make_op, make_ctx


def _op(**kwargs):
    from operators.base import NextGenDatabridgeBaseOperator
    return make_op(NextGenDatabridgeBaseOperator, {"type": "sql_extract"}, **kwargs)


# ── _resolve_secret ────────────────────────────────────────────────────────────

class TestResolveSecret:
    def test_non_secret_string_passthrough(self):
        assert _op()._resolve_secret("plain") == "plain"

    def test_non_string_passthrough(self):
        op = _op()
        assert op._resolve_secret(42) == 42
        assert op._resolve_secret(None) is None

    def test_secret_no_json_key(self):
        op = _op()
        mock_sm = MagicMock()
        mock_sm.get_secret_value.return_value = {"SecretString": "s3cr3t"}
        with patch("boto3.client", return_value=mock_sm):
            result = op._resolve_secret("secret:my-secret")
        mock_sm.get_secret_value.assert_called_once_with(SecretId="my-secret")
        assert result == "s3cr3t"

    def test_secret_with_json_key(self):
        op = _op()
        mock_sm = MagicMock()
        mock_sm.get_secret_value.return_value = {
            "SecretString": json.dumps({"password": "p4ss", "user": "bob"})
        }
        with patch("boto3.client", return_value=mock_sm):
            result = op._resolve_secret("secret:my-secret/password")
        assert result == "p4ss"


# ── Path helpers ───────────────────────────────────────────────────────────────

class TestPathHelpers:
    def test_duckdb_s3_path(self):
        path = _op().duckdb_s3_path("run_001", "ext")
        assert path == "s3://test-bucket/pipelines/test_pipeline/runs/run_001/ext.duckdb"

    def test_local_duckdb_path(self):
        path = _op().local_duckdb_path("run_001", "ext")
        assert path == "/tmp/nextgen_databridge/test_pipeline/run_001/ext.duckdb"

    def test_get_s3_client_no_endpoint(self, monkeypatch):
        monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
        op = _op()
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()
            op.get_s3_client()
        args, kwargs = mock_client.call_args
        assert args[0] == "s3"
        assert "endpoint_url" not in kwargs

    def test_get_s3_client_with_endpoint(self, monkeypatch):
        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
        op = _op()
        with patch("boto3.client") as mock_client:
            mock_client.return_value = MagicMock()
            op.get_s3_client()
        _, kwargs = mock_client.call_args
        assert kwargs.get("endpoint_url") == "http://minio:9000"


# ── download / upload ──────────────────────────────────────────────────────────

class TestDownloadUpload:
    def test_download_duckdb(self, tmp_path):
        op = _op()
        local_path = str(tmp_path / "sub" / "file.duckdb")
        mock_s3 = MagicMock()
        with patch.object(op, "get_s3_client", return_value=mock_s3):
            op.download_duckdb("s3://bkt/a/b.duckdb", local_path)
        mock_s3.download_file.assert_called_once_with("bkt", "a/b.duckdb", local_path)
        assert os.path.isdir(str(tmp_path / "sub"))

    def test_upload_duckdb(self, tmp_path):
        op = _op()
        local = str(tmp_path / "out.duckdb")
        open(local, "wb").close()
        mock_s3 = MagicMock()
        with patch.object(op, "get_s3_client", return_value=mock_s3):
            op.upload_duckdb(local, "s3://bkt/a/b.duckdb")
        mock_s3.upload_file.assert_called_once_with(local, "bkt", "a/b.duckdb")


# ── Audit DB writes ────────────────────────────────────────────────────────────

class TestAuditDB:
    def _mock_conn(self):
        conn = MagicMock()
        cur  = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.return_value = (5, 3, 1)
        return conn, cur

    def test_write_task_run_commits(self):
        op = _op()
        conn, _ = self._mock_conn()
        with patch("psycopg2.connect", return_value=conn):
            op._write_task_run("run_001", "running")
        assert conn.commit.called

    def test_write_task_run_success_status(self):
        op = _op()
        conn, cur = self._mock_conn()
        with patch("psycopg2.connect", return_value=conn):
            op._write_task_run("run_001", "success", output_row_count=100)
        assert conn.commit.called

    def test_write_task_run_swallows_db_error(self):
        op = _op()
        with patch("psycopg2.connect", side_effect=Exception("DB down")):
            op._write_task_run("run_001", "running")  # must not raise

    def test_write_audit_log_commits(self):
        op = _op()
        conn, _ = self._mock_conn()
        with patch("psycopg2.connect", return_value=conn):
            op._write_audit_log("TASK_STARTED", "run_001", {"k": "v"}, severity="info")
        assert conn.commit.called

    def test_write_audit_log_swallows_db_error(self):
        op = _op()
        with patch("psycopg2.connect", side_effect=Exception("DB down")):
            op._write_audit_log("TASK_STARTED", "run_001", {})  # must not raise

    def test_audit_db_conn_raises_when_no_url(self, monkeypatch):
        monkeypatch.delenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", raising=False)
        from operators.base import NextGenDatabridgeBaseOperator
        op = make_op(NextGenDatabridgeBaseOperator, {"type": "sql_extract"},
                     pipeline_config={"pipeline_id": "p", "tasks": []})
        op.audit_db_url = ""
        mock_sm = MagicMock()
        mock_sm.get_secret_value.side_effect = Exception("no secret")
        with patch("boto3.client", return_value=mock_sm):
            with pytest.raises(RuntimeError, match="NEXTGEN_DATABRIDGE_AUDIT_DB_URL"):
                op._audit_db_conn()


# ── _find_duckdb_producer_task ─────────────────────────────────────────────────

class TestFindProducerTask:
    def _op_with_tasks(self, tasks):
        from operators.base import NextGenDatabridgeBaseOperator
        return make_op(
            NextGenDatabridgeBaseOperator,
            {"type": "load_target"},
            pipeline_config={"pipeline_id": "test_pipeline", "tasks": tasks},
        )

    def test_direct_producer(self):
        tasks = [{"task_id": "ext", "type": "sql_extract"}]
        result = self._op_with_tasks(tasks)._find_duckdb_producer_task(MagicMock(), "ext")
        assert result == "ext"

    def test_walks_through_passthrough(self):
        tasks = [
            {"task_id": "ext", "type": "sql_extract"},
            {"task_id": "dq",  "type": "data_quality", "depends_on": ["ext"]},
        ]
        result = self._op_with_tasks(tasks)._find_duckdb_producer_task(MagicMock(), "dq")
        assert result == "ext"

    def test_unknown_task_returns_none(self):
        result = self._op_with_tasks([])._find_duckdb_producer_task(MagicMock(), "ghost")
        assert result is None

    def test_non_producer_non_passthrough_returns_none(self):
        tasks = [{"task_id": "notif", "type": "notification"}]
        result = self._op_with_tasks(tasks)._find_duckdb_producer_task(MagicMock(), "notif")
        assert result is None

    def test_prevents_cycle(self):
        tasks = [
            {"task_id": "a", "type": "data_quality", "depends_on": ["b"]},
            {"task_id": "b", "type": "data_quality", "depends_on": ["a"]},
        ]
        result = self._op_with_tasks(tasks)._find_duckdb_producer_task(MagicMock(), "a")
        assert result is None

    def test_execute_not_implemented(self):
        op = _op()
        with pytest.raises(NotImplementedError):
            op.execute({})
