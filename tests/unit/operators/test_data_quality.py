"""Unit tests for DataQualityOperator and SchemaValidateOperator."""
import duckdb
import pytest

from tests.unit.operators.conftest import make_op, make_ctx


def _setup_db(tmp_path, table="raw_data", ddl=None, rows=None):
    path = str(tmp_path / "input.duckdb")
    conn = duckdb.connect(path)
    if ddl:
        conn.execute(ddl)
    else:
        conn.execute(f"CREATE TABLE {table} (name VARCHAR, age INTEGER, score DOUBLE, ts TIMESTAMP)")
    if rows is not None:
        for row in rows:
            vals = ", ".join("NULL" if v is None else f"'{v}'" if isinstance(v, str) else str(v) for v in row)
            conn.execute(f"INSERT INTO {table} VALUES ({vals})")
    conn.close()
    return path


def _op(task_config_extra, monkeypatch, db_path, noop_audit, noop_s3):
    from operators.data_quality import DataQualityOperator
    op = make_op(DataQualityOperator, {
        "type": "data_quality",
        "source": {"table": "raw_data"},
        "fail_pipeline_on_error": False,
        **task_config_extra,
    })
    monkeypatch.setattr(op, "local_duckdb_path", lambda *_: db_path)
    return op


# ── not_null ───────────────────────────────────────────────────────────────────

def test_not_null_pass(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("Alice", 30, 9.5, "2026-01-01")])
    op = _op({"checks": [{"type": "not_null", "column": "name", "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is True
    assert r["failures"] == 0


def test_not_null_fail(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("Alice", 30, 9.5, "2026-01-01"), (None, 25, 8.0, "2026-01-02")])
    op = _op({"checks": [{"type": "not_null", "column": "name", "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is False
    assert r["failures"] == 1


def test_not_null_warn_action(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[(None, 25, 8.0, "2026-01-02")])
    op = _op({"checks": [{"type": "not_null", "column": "name", "action": "warn"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is True  # warn does not fail pipeline
    assert r["warnings"] == 1
    assert r["failures"] == 0


# ── unique ─────────────────────────────────────────────────────────────────────

def test_unique_pass(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("Alice", 30, 9.5, "2026-01-01"), ("Bob", 25, 8.0, "2026-01-02")])
    op = _op({"checks": [{"type": "unique", "column": "name", "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is True


def test_unique_fail(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("Alice", 30, 9.5, "2026-01-01"), ("Alice", 25, 8.0, "2026-01-02")])
    op = _op({"checks": [{"type": "unique", "column": "name", "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is False


# ── row_count_min / row_count_max ──────────────────────────────────────────────

def test_row_count_min_pass(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("A", 1, 1.0, "2026-01-01"), ("B", 2, 2.0, "2026-01-02")])
    op = _op({"checks": [{"type": "row_count_min", "value": 2, "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is True


def test_row_count_min_fail(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("A", 1, 1.0, "2026-01-01")])
    op = _op({"checks": [{"type": "row_count_min", "value": 5, "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is False


def test_row_count_max_pass(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("A", 1, 1.0, "2026-01-01")])
    op = _op({"checks": [{"type": "row_count_max", "value": 10, "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is True


def test_row_count_max_fail(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[
        ("A", 1, 1.0, "2026-01-01"), ("B", 2, 2.0, "2026-01-02"), ("C", 3, 3.0, "2026-01-03")
    ])
    op = _op({"checks": [{"type": "row_count_max", "value": 2, "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is False


# ── value_range ────────────────────────────────────────────────────────────────

def test_value_range_pass(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("A", 5, 5.0, "2026-01-01")])
    op = _op({"checks": [{"type": "value_range", "column": "age", "min": 1, "max": 10, "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is True


def test_value_range_fail(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("A", 150, 5.0, "2026-01-01")])
    op = _op({"checks": [{"type": "value_range", "column": "age", "min": 1, "max": 100, "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is False


# ── regex_match ────────────────────────────────────────────────────────────────

def test_regex_match_pass(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("ABC123", 1, 1.0, "2026-01-01")])
    op = _op({"checks": [{"type": "regex_match", "column": "name", "pattern": "^[A-Z]+[0-9]+$", "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is True


def test_regex_match_fail(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("abc", 1, 1.0, "2026-01-01")])
    op = _op({"checks": [{"type": "regex_match", "column": "name", "pattern": "^[0-9]+$", "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is False


# ── custom_sql ─────────────────────────────────────────────────────────────────

def test_custom_sql_pass(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("A", 5, 5.0, "2026-01-01")])
    op = _op({"checks": [{"type": "custom_sql", "sql": "SELECT COUNT(*) FROM raw_data WHERE age < 0", "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is True


def test_custom_sql_fail(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("A", -5, 5.0, "2026-01-01")])
    op = _op({"checks": [{"type": "custom_sql", "sql": "SELECT COUNT(*) FROM raw_data WHERE age < 0", "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is False


# ── schema_match ───────────────────────────────────────────────────────────────

def test_schema_match_pass(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("A", 5, 5.0, "2026-01-01")])
    op = _op({"checks": [{"type": "schema_match", "columns": {"name": "VARCHAR"}, "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is True


def test_schema_match_missing_column(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("A", 5, 5.0, "2026-01-01")])
    op = _op({"checks": [{"type": "schema_match", "columns": {"missing_col": "VARCHAR"}, "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is False


# ── unknown check type ─────────────────────────────────────────────────────────

def test_unknown_check_type_skipped(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("A", 5, 5.0, "2026-01-01")])
    op = _op({"checks": [{"type": "unsupported_check", "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    r = op.execute(make_ctx())
    assert r["passed"] is True  # unknown check returns (True, ..., None)


# ── fail_pipeline_on_error ─────────────────────────────────────────────────────

def test_fail_pipeline_on_error_true_raises(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[(None, 5, 5.0, "2026-01-01")])
    from operators.data_quality import DataQualityOperator
    op = make_op(DataQualityOperator, {
        "type": "data_quality",
        "source": {"table": "raw_data"},
        "fail_pipeline_on_error": True,
        "checks": [{"type": "not_null", "column": "name", "action": "fail"}],
    })
    monkeypatch.setattr(op, "local_duckdb_path", lambda *_: db)
    with pytest.raises(ValueError, match="Data quality failed"):
        op.execute(make_ctx())


# ── XCom push ─────────────────────────────────────────────────────────────────

def test_xcom_push_qc_passed(tmp_path, monkeypatch, noop_audit, noop_s3):
    db = _setup_db(tmp_path, rows=[("A", 5, 5.0, "2026-01-01")])
    op = _op({"checks": [{"type": "row_count_min", "value": 1, "action": "fail"}]},
             monkeypatch, db, noop_audit, noop_s3)
    ctx = make_ctx()
    op.execute(ctx)
    ctx["ti"].xcom_push.assert_any_call(key="qc_passed", value=True)


# ── SchemaValidateOperator ─────────────────────────────────────────────────────

class TestSchemaValidateOperator:
    def _make(self, expected_schema, db_path, monkeypatch, noop_audit, noop_s3):
        from operators.data_quality import SchemaValidateOperator
        op = make_op(SchemaValidateOperator, {
            "type": "schema_validate",
            "source": {"table": "raw_data"},
            "expected_schema": expected_schema,
            "fail_on_error": False,
        })
        monkeypatch.setattr(op, "local_duckdb_path", lambda *_: db_path)
        return op

    def test_valid_schema(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        db = _setup_db(tmp_path, rows=[("A", 5, 5.0, "2026-01-01")])
        op = self._make({"name": "VARCHAR", "age": "INTEGER"}, db, monkeypatch, noop_audit, noop_s3)
        result = op.execute(make_ctx())
        assert result["valid"] is True
        assert result["issues"] == []

    def test_missing_column_detected(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        db = _setup_db(tmp_path, rows=[("A", 5, 5.0, "2026-01-01")])
        op = self._make({"nonexistent": "VARCHAR"}, db, monkeypatch, noop_audit, noop_s3)
        result = op.execute(make_ctx())
        assert result["valid"] is False
        assert any(i["issue"] == "missing" for i in result["issues"])

    def test_fail_on_error_true_raises(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        db = _setup_db(tmp_path, rows=[("A", 5, 5.0, "2026-01-01")])
        from operators.data_quality import SchemaValidateOperator
        op = make_op(SchemaValidateOperator, {
            "type": "schema_validate",
            "source": {"table": "raw_data"},
            "expected_schema": {"ghost_col": "VARCHAR"},
            "fail_on_error": True,
        })
        monkeypatch.setattr(op, "local_duckdb_path", lambda *_: db)
        with pytest.raises(ValueError, match="Schema validation failed"):
            op.execute(make_ctx())

    def test_xcom_push_on_success(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        db = _setup_db(tmp_path, rows=[("A", 5, 5.0, "2026-01-01")])
        op = self._make({"name": "VARCHAR"}, db, monkeypatch, noop_audit, noop_s3)
        ctx = make_ctx()
        op.execute(ctx)
        ctx["ti"].xcom_push.assert_any_call(key="schema_valid", value=True)
