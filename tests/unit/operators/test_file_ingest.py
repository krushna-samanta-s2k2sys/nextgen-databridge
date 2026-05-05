"""Unit tests for FileIngestOperator."""
import os
import duckdb
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from tests.unit.operators.conftest import make_op, make_ctx


def _make_op(source, monkeypatch, noop_audit, noop_s3, out_path):
    from operators.file_ingest import FileIngestOperator
    op = make_op(FileIngestOperator, {
        "type": "file_ingest",
        "source": source,
        "output": {"table": "file_data"},
    })
    monkeypatch.setattr(op, "local_duckdb_path", lambda *_: out_path)
    monkeypatch.setattr(op, "duckdb_s3_path",    lambda *_: "s3://bkt/out.duckdb")
    return op


# ── CSV ingestion ──────────────────────────────────────────────────────────────

def test_ingest_csv(tmp_path, monkeypatch, noop_audit, noop_s3):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id,name\n1,Alice\n2,Bob\n")
    out_path = str(tmp_path / "out.duckdb")

    op = _make_op({"path": str(csv_file), "format": "csv"}, monkeypatch, noop_audit, noop_s3, out_path)
    result = op.execute(make_ctx())

    assert result == "s3://bkt/out.duckdb"
    conn = duckdb.connect(out_path, read_only=True)
    rows = conn.execute("SELECT COUNT(*) FROM file_data").fetchone()[0]
    conn.close()
    assert rows == 2


def test_ingest_tsv(tmp_path, monkeypatch, noop_audit, noop_s3):
    tsv_file = tmp_path / "data.tsv"
    tsv_file.write_text("id\tname\n1\tAlice\n2\tBob\n")
    out_path = str(tmp_path / "out.duckdb")

    op = _make_op({"path": str(tsv_file), "format": "tsv"}, monkeypatch, noop_audit, noop_s3, out_path)
    result = op.execute(make_ctx())

    conn = duckdb.connect(out_path, read_only=True)
    rows = conn.execute("SELECT COUNT(*) FROM file_data").fetchone()[0]
    conn.close()
    assert rows == 2


# ── JSON ingestion ─────────────────────────────────────────────────────────────

def test_ingest_jsonl(tmp_path, monkeypatch, noop_audit, noop_s3):
    jsonl_file = tmp_path / "data.jsonl"
    jsonl_file.write_text('{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n')
    out_path = str(tmp_path / "out.duckdb")

    op = _make_op({"path": str(jsonl_file), "format": "jsonl"}, monkeypatch, noop_audit, noop_s3, out_path)
    result = op.execute(make_ctx())

    conn = duckdb.connect(out_path, read_only=True)
    rows = conn.execute("SELECT COUNT(*) FROM file_data").fetchone()[0]
    conn.close()
    assert rows == 2


# ── Parquet ingestion ──────────────────────────────────────────────────────────

def test_ingest_parquet(tmp_path, monkeypatch, noop_audit, noop_s3):
    parquet_file = str(tmp_path / "data.parquet")
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
    df.to_parquet(parquet_file)
    out_path = str(tmp_path / "out.duckdb")

    op = _make_op({"path": parquet_file, "format": "parquet"}, monkeypatch, noop_audit, noop_s3, out_path)
    result = op.execute(make_ctx())

    conn = duckdb.connect(out_path, read_only=True)
    rows = conn.execute("SELECT COUNT(*) FROM file_data").fetchone()[0]
    conn.close()
    assert rows == 3


# ── S3 file download ───────────────────────────────────────────────────────────

def test_ingest_from_s3(tmp_path, monkeypatch, noop_audit, noop_s3):
    local_csv = str(tmp_path / "downloaded.csv")
    out_path  = str(tmp_path / "out.duckdb")

    def fake_s3_download(bucket, key, dest):
        pd.DataFrame({"id": [1]}).to_csv(dest, index=False)

    mock_s3 = MagicMock()
    mock_s3.download_file.side_effect = fake_s3_download

    op = _make_op({"s3_path": "s3://mybucket/data.csv", "format": "csv"},
                  monkeypatch, noop_audit, noop_s3, out_path)
    monkeypatch.setattr(op, "get_s3_client", lambda: mock_s3)

    result = op.execute(make_ctx())
    mock_s3.download_file.assert_called_once()
    assert result == "s3://bkt/out.duckdb"


# ── Unsupported format ─────────────────────────────────────────────────────────

def test_unsupported_format_raises(tmp_path, monkeypatch, noop_audit, noop_s3):
    out_path = str(tmp_path / "out.duckdb")

    op = _make_op({"path": "/some/file.xml", "format": "xml"},
                  monkeypatch, noop_audit, noop_s3, out_path)
    with pytest.raises(ValueError, match="Unsupported file format"):
        op.execute(make_ctx())


# ── XCom push ─────────────────────────────────────────────────────────────────

def test_xcom_push_duckdb_path(tmp_path, monkeypatch, noop_audit, noop_s3):
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("id\n1\n2\n")
    out_path = str(tmp_path / "out.duckdb")

    op = _make_op({"path": str(csv_file), "format": "csv"}, monkeypatch, noop_audit, noop_s3, out_path)
    ctx = make_ctx()
    op.execute(ctx)
    ctx["ti"].xcom_push.assert_called_with(key="duckdb_path", value="s3://bkt/out.duckdb")


# ── execute failure path ───────────────────────────────────────────────────────

def test_execute_reraises_on_missing_file(tmp_path, monkeypatch, noop_audit, noop_s3):
    out_path = str(tmp_path / "out.duckdb")
    op = _make_op({"path": "/nonexistent/file.csv", "format": "csv"},
                  monkeypatch, noop_audit, noop_s3, out_path)
    with pytest.raises(Exception):
        op.execute(make_ctx())
