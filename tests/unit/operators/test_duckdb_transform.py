"""Unit tests for DuckDBTransformOperator."""
import duckdb
import os
import pytest
from unittest.mock import MagicMock, patch

from tests.unit.operators.conftest import make_op, make_ctx


def _setup_input_db(path, table="raw_data", rows=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = duckdb.connect(path)
    conn.execute(f"CREATE TABLE {table} (id INTEGER, val VARCHAR)")
    for row in (rows or [(1, "a"), (2, "b")]):
        conn.execute(f"INSERT INTO {table} VALUES {row}")
    conn.close()


def _make_transform_op(sql, depends_on=None, extra=None, monkeypatch=None, noop_audit=None, noop_s3=None):
    from operators.duckdb_transform import DuckDBTransformOperator
    cfg = {
        "type": "duckdb_transform",
        "sql": sql,
        "output": {"table": "result"},
    }
    if depends_on:
        cfg["depends_on"] = depends_on
    if extra:
        cfg.update(extra)
    return make_op(DuckDBTransformOperator, cfg)


class TestRenderSql:
    def test_renders_ds_and_run_id(self):
        from operators.duckdb_transform import DuckDBTransformOperator
        op = make_op(DuckDBTransformOperator, {"type": "duckdb_transform", "sql": ""})
        ctx = {"ds": "2026-05-04", "run_id": "run_001"}
        rendered = op._render_sql("SELECT '{{ ds }}' AS d, '{{run_id}}' AS r", ctx)
        assert rendered == "SELECT '2026-05-04' AS d, 'run_001' AS r"

    def test_both_brace_styles(self):
        from operators.duckdb_transform import DuckDBTransformOperator
        op = make_op(DuckDBTransformOperator, {"type": "duckdb_transform", "sql": ""})
        rendered = op._render_sql("{{ds}} {{ ds }}", {"ds": "X", "run_id": ""})
        assert rendered == "X X"


class TestExecute:
    def test_basic_sql_transform(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        from operators.duckdb_transform import DuckDBTransformOperator

        input_path = str(tmp_path / "input.duckdb")
        out_path   = str(tmp_path / "output.duckdb")
        _setup_input_db(input_path)

        op = make_op(DuckDBTransformOperator, {
            "type": "duckdb_transform",
            "sql": "SELECT id, val FROM raw.raw_data WHERE id > 0",
            "output": {"table": "result"},
            "depends_on": ["raw"],
        })

        pipeline_config = {
            "pipeline_id": "test_pipeline",
            "tasks": [
                {"task_id": "raw", "type": "sql_extract"},
                {"task_id": "test_task", "type": "duckdb_transform", "depends_on": ["raw"]},
            ],
        }
        op.pipeline_config = pipeline_config

        monkeypatch.setattr(op, "local_duckdb_path",
                            lambda run_id, tid: input_path if tid == "raw" else out_path)
        monkeypatch.setattr(op, "duckdb_s3_path",
                            lambda run_id, tid: f"s3://test-bucket/{tid}.duckdb")
        monkeypatch.setattr(op, "_find_duckdb_producer_task",
                            lambda ti, dep, visited=None: "raw")

        ctx = make_ctx()
        result = op.execute(ctx)

        assert result.startswith("s3://")
        ctx["ti"].xcom_push.assert_any_call(key="duckdb_path", value=result)
        assert os.path.exists(out_path)
        out_conn = duckdb.connect(out_path, read_only=True)
        rows = out_conn.execute("SELECT COUNT(*) FROM result").fetchone()[0]
        out_conn.close()
        assert rows == 2

    def test_execute_raises_on_bad_sql(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        from operators.duckdb_transform import DuckDBTransformOperator

        input_path = str(tmp_path / "input.duckdb")
        out_path   = str(tmp_path / "output.duckdb")
        _setup_input_db(input_path)

        op = make_op(DuckDBTransformOperator, {
            "type": "duckdb_transform",
            "sql": "SELECT * FROM nonexistent_table_xyz",
            "output": {"table": "result"},
            "depends_on": ["raw"],
        })
        pipeline_config = {
            "pipeline_id": "test_pipeline",
            "tasks": [
                {"task_id": "raw", "type": "sql_extract"},
                {"task_id": "test_task", "type": "duckdb_transform", "depends_on": ["raw"]},
            ],
        }
        op.pipeline_config = pipeline_config

        monkeypatch.setattr(op, "local_duckdb_path",
                            lambda run_id, tid: input_path if tid == "raw" else out_path)
        monkeypatch.setattr(op, "duckdb_s3_path",
                            lambda run_id, tid: f"s3://test-bucket/{tid}.duckdb")
        monkeypatch.setattr(op, "_find_duckdb_producer_task",
                            lambda ti, dep, visited=None: "raw")

        with pytest.raises(Exception):
            op.execute(make_ctx())

    def test_explicit_source_path(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        import shutil
        from operators.duckdb_transform import DuckDBTransformOperator

        explicit_input = str(tmp_path / "explicit.duckdb")
        out_path       = str(tmp_path / "output.duckdb")
        explicit_local = str(tmp_path / "explicit_local.duckdb")
        _setup_input_db(explicit_input)

        op = make_op(DuckDBTransformOperator, {
            "type": "duckdb_transform",
            "sql": "SELECT id FROM _explicit.raw_data",
            "source": {"duckdb_path": "s3://bkt/explicit.duckdb"},
            "output": {"table": "result"},
        })

        monkeypatch.setattr(op, "local_duckdb_path", lambda *_: out_path)
        monkeypatch.setattr(op, "duckdb_s3_path", lambda *_: "s3://bkt/out.duckdb")

        # Intercept download_duckdb so the explicit source lands in a real temp path.
        # Also patch the hardcoded explicit_local path used by the operator.
        def fake_download(s3_path, local_path):
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            shutil.copy(explicit_input, local_path)

        monkeypatch.setattr(op, "download_duckdb", fake_download)

        # Patch the hardcoded explicit_local variable used inside execute():
        # The operator resolves it as /tmp/nextgen_databridge/{run_id}/explicit_input.duckdb
        # We redirect it to our tmp copy.
        import operators.duckdb_transform as _mod
        orig_path_exists = os.path.exists
        shutil.copy(explicit_input, explicit_local)

        real_local = f"/tmp/nextgen_databridge/run_001/explicit_input.duckdb"
        os.makedirs(os.path.dirname(real_local) if "/" in real_local else ".", exist_ok=True)

        # Patch os.path.exists so the operator doesn't re-download, and copy
        # the file to the real_local path so ATTACH can succeed.
        try:
            os.makedirs(os.path.dirname(real_local), exist_ok=True)
            shutil.copy(explicit_input, real_local)
        except OSError:
            pytest.skip("Cannot create /tmp path on this platform")

        ctx = make_ctx()
        result = op.execute(ctx)
        assert result == "s3://bkt/out.duckdb"
