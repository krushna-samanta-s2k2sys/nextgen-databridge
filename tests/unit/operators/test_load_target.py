"""Unit tests for LoadTargetOperator."""
import duckdb
import os
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch, call

from tests.unit.operators.conftest import make_op, make_ctx


def _setup_input_db(path, table="result"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = duckdb.connect(path)
    conn.execute(f"CREATE TABLE {table} (id INTEGER, name VARCHAR)")
    conn.execute(f"INSERT INTO {table} VALUES (1, 'Alice'), (2, 'Bob')")
    conn.close()


def _make_op(target, monkeypatch, noop_audit, noop_s3, db_path):
    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {
        "type": "load_target",
        "source": {"table": "result"},
        "target": target,
    })
    monkeypatch.setattr(op, "local_duckdb_path", lambda *_: db_path)
    return op


# ── _resolve_rdbms_url ─────────────────────────────────────────────────────────

def test_resolve_rdbms_url_no_conn_id():
    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {"type": "load_target", "target": {}})
    url = op._resolve_rdbms_url(None, {
        "host": "h", "port": 5432, "username": "u", "password": "p", "database": "db"
    }, "postgresql+psycopg2")
    assert "h:5432/db" in url
    assert "postgresql+psycopg2" in url


# ── RDBMS append mode ──────────────────────────────────────────────────────────

def test_write_to_rdbms_append(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    op = _make_op(
        {"type": "postgresql", "host": "h", "port": 5432, "username": "u",
         "password": "p", "database": "db", "schema": "public", "table": "out", "mode": "append"},
        monkeypatch, noop_audit, noop_s3, db_path,
    )

    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin().__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin().__exit__  = MagicMock(return_value=False)

    with patch("sqlalchemy.create_engine", return_value=mock_engine):
        with patch.object(op, "_to_sql_safe") as mock_to_sql:
            op.execute(make_ctx())

    mock_to_sql.assert_called_once()
    args = mock_to_sql.call_args[0]
    assert isinstance(args[0], pd.DataFrame)
    assert len(args[0]) == 2


# ── RDBMS overwrite mode ───────────────────────────────────────────────────────

def test_write_to_rdbms_overwrite(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    op = _make_op(
        {"type": "sqlserver", "host": "h", "port": 1433, "username": "u",
         "password": "p", "database": "db", "schema": "dbo", "table": "out", "mode": "overwrite"},
        monkeypatch, noop_audit, noop_s3, db_path,
    )

    mock_engine = MagicMock()
    mock_ctx_mgr = MagicMock()
    mock_ctx_mgr.__enter__ = MagicMock(return_value=MagicMock(
        execute=MagicMock(return_value=MagicMock(scalar=MagicMock(return_value=1)))
    ))
    mock_ctx_mgr.__exit__ = MagicMock(return_value=False)
    mock_engine.begin.return_value = mock_ctx_mgr

    with patch("sqlalchemy.create_engine", return_value=mock_engine):
        with patch.object(op, "_to_sql_safe") as mock_to_sql:
            op.execute(make_ctx())

    mock_to_sql.assert_called_once()


# ── RDBMS merge mode ───────────────────────────────────────────────────────────

def test_write_to_rdbms_merge_calls_merge(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    op = _make_op(
        {"type": "postgresql", "host": "h", "port": 5432, "username": "u",
         "password": "p", "database": "db", "schema": "public", "table": "out",
         "mode": "merge", "keys": ["id"]},
        monkeypatch, noop_audit, noop_s3, db_path,
    )

    with patch("sqlalchemy.create_engine") as mock_ce:
        with patch.object(op, "_merge_to_rdbms") as mock_merge:
            op.execute(make_ctx())

    mock_merge.assert_called_once()
    _, _, _, _, keys, _ = mock_merge.call_args[0]
    assert keys == ["id"]


def test_write_to_rdbms_merge_missing_keys_raises(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {
        "type": "load_target",
        "source": {"table": "result"},
        "target": {
            "type": "postgresql", "host": "h", "port": 5432,
            "username": "u", "password": "p", "database": "db",
            "schema": "public", "table": "out", "mode": "merge",
        },
    })
    monkeypatch.setattr(op, "local_duckdb_path", lambda *_: db_path)

    with patch("sqlalchemy.create_engine"):
        with pytest.raises(ValueError, match="target.keys is required"):
            op.execute(make_ctx())


# ── S3 write ───────────────────────────────────────────────────────────────────

def test_write_to_s3(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    op = _make_op(
        {"type": "s3", "bucket": "my-bucket", "path": "output/data.parquet"},
        monkeypatch, noop_audit, noop_s3, db_path,
    )

    mock_s3 = MagicMock()
    with patch.object(op, "get_s3_client", return_value=mock_s3):
        with patch("pyarrow.parquet.write_table"):
            with patch("os.unlink"):  # file was never created since write_table is mocked
                result = op.execute(make_ctx())

    assert "2 rows" in result or "s3" in result.lower() or "output" in result


def test_write_to_s3_default_bucket(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    op = _make_op(
        {"type": "parquet"},
        monkeypatch, noop_audit, noop_s3, db_path,
    )
    mock_s3 = MagicMock()
    with patch.object(op, "get_s3_client", return_value=mock_s3):
        with patch("pyarrow.parquet.write_table"):
            with patch("os.unlink"):
                op.execute(make_ctx())
    mock_s3.upload_file.assert_called_once()


# ── Kafka write ────────────────────────────────────────────────────────────────

def test_write_to_kafka(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    op = _make_op(
        {"type": "kafka", "topic": "my-topic"},
        monkeypatch, noop_audit, noop_s3, db_path,
    )

    mock_producer = MagicMock()
    import sys
    sys.modules["kafka"].KafkaProducer.return_value = mock_producer

    result = op.execute(make_ctx())
    assert "kafka" in result.lower() or "my-topic" in result


# ── PubSub write ───────────────────────────────────────────────────────────────

def test_write_to_pubsub(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    op = _make_op(
        {"type": "pubsub", "project_id": "my-project", "topic": "my-topic"},
        monkeypatch, noop_audit, noop_s3, db_path,
    )

    mock_publisher = MagicMock()
    mock_publisher.topic_path.return_value = "projects/my-project/topics/my-topic"
    import sys
    sys.modules["google.cloud.pubsub_v1"].PublisherClient.return_value = mock_publisher

    result = op.execute(make_ctx())
    assert "pubsub" in result.lower() or mock_publisher.publish.called


# ── execute failure path ───────────────────────────────────────────────────────

def test_execute_reraises_on_error(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    op = _make_op(
        {"type": "postgresql", "host": "h", "port": 5432, "username": "u",
         "password": "p", "database": "db", "schema": "public", "table": "out", "mode": "append"},
        monkeypatch, noop_audit, noop_s3, db_path,
    )
    with patch("sqlalchemy.create_engine", side_effect=Exception("engine error")):
        with pytest.raises(Exception, match="engine error"):
            op.execute(make_ctx())


# ── _merge_to_rdbms dialect branches ──────────────────────────────────────────

def test_merge_unsupported_dialect_raises():
    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {"type": "load_target", "target": {}})
    df = pd.DataFrame({"id": [1], "val": ["a"]})
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin().__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin().__exit__ = MagicMock(return_value=False)
    with patch.object(mock_conn, "execute"):
        with patch("pandas.DataFrame.to_sql"):
            with pytest.raises(ValueError, match="merge mode is not supported"):
                op._merge_to_rdbms(df, "t", mock_engine, "s", ["id"], "sqlite")


def test_merge_postgresql_dialect(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {
        "type": "load_target",
        "source": {"table": "result"},
        "target": {
            "type": "postgresql", "host": "h", "port": 5432,
            "username": "u", "password": "p", "database": "db",
            "schema": "public", "table": "out", "mode": "merge", "keys": ["id"],
        },
    })
    monkeypatch.setattr(op, "local_duckdb_path", lambda *_: db_path)

    mock_engine = MagicMock()
    mock_ctx_mgr = MagicMock()
    mock_conn = MagicMock()
    mock_ctx_mgr.__enter__ = MagicMock(return_value=mock_conn)
    mock_ctx_mgr.__exit__ = MagicMock(return_value=False)
    mock_engine.begin.return_value = mock_ctx_mgr

    with patch("sqlalchemy.create_engine", return_value=mock_engine):
        with patch("pandas.DataFrame.to_sql"):
            op.execute(make_ctx())

    # The postgresql ON CONFLICT path should have called execute
    assert mock_conn.execute.called


def test_merge_mssql_dialect(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {
        "type": "load_target",
        "source": {"table": "result"},
        "target": {
            "type": "sqlserver", "host": "h", "port": 1433,
            "username": "u", "password": "p", "database": "db",
            "schema": "dbo", "table": "out", "mode": "merge", "keys": ["id"],
        },
    })
    monkeypatch.setattr(op, "local_duckdb_path", lambda *_: db_path)

    mock_engine = MagicMock()
    mock_ctx_mgr = MagicMock()
    mock_conn = MagicMock()
    mock_ctx_mgr.__enter__ = MagicMock(return_value=mock_conn)
    mock_ctx_mgr.__exit__ = MagicMock(return_value=False)
    mock_engine.begin.return_value = mock_ctx_mgr

    with patch("sqlalchemy.create_engine", return_value=mock_engine):
        with patch("pandas.DataFrame.to_sql"):
            op.execute(make_ctx())

    assert mock_conn.execute.called


def test_merge_oracle_dialect(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {
        "type": "load_target",
        "source": {"table": "result"},
        "target": {
            "type": "oracle", "host": "h", "port": 1521,
            "username": "u", "password": "p", "database": "db",
            "schema": "sch", "table": "out", "mode": "merge", "keys": ["id"],
        },
    })
    monkeypatch.setattr(op, "local_duckdb_path", lambda *_: db_path)

    mock_engine = MagicMock()
    mock_ctx_mgr = MagicMock()
    mock_conn = MagicMock()
    mock_ctx_mgr.__enter__ = MagicMock(return_value=mock_conn)
    mock_ctx_mgr.__exit__ = MagicMock(return_value=False)
    mock_engine.begin.return_value = mock_ctx_mgr

    with patch("sqlalchemy.create_engine", return_value=mock_engine):
        with patch("pandas.DataFrame.to_sql"):
            op.execute(make_ctx())

    assert mock_conn.execute.called


# ── _to_sql_safe ────────────────────────────────────────────────────────────────

def test_to_sql_safe_basic_write():
    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {"type": "load_target", "target": {}})
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})

    mock_engine = MagicMock()
    mock_ctx_mgr = MagicMock()
    mock_ctx_mgr.__enter__ = MagicMock(return_value=MagicMock())
    mock_ctx_mgr.__exit__ = MagicMock(return_value=False)
    mock_engine.begin.return_value = mock_ctx_mgr

    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        op._to_sql_safe(df, "my_table", mock_engine, "public", "replace", "postgresql+psycopg2")

    mock_to_sql.assert_called_once()


def test_to_sql_safe_non_mssql_error_reraises():
    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {"type": "load_target", "target": {}})
    df = pd.DataFrame({"id": [1]})

    mock_engine = MagicMock()
    mock_ctx_mgr = MagicMock()
    mock_ctx_mgr.__enter__ = MagicMock(return_value=MagicMock())
    mock_ctx_mgr.__exit__ = MagicMock(return_value=False)
    mock_engine.begin.return_value = mock_ctx_mgr

    with patch("pandas.DataFrame.to_sql", side_effect=Exception("pg error")):
        with pytest.raises(Exception, match="pg error"):
            op._to_sql_safe(df, "t", mock_engine, "s", "append", "postgresql+psycopg2")


def test_to_sql_safe_mssql_rowversion_retry():
    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {"type": "load_target", "target": {}})
    df = pd.DataFrame({"id": [1], "ts_col": [b"rowver"]})

    mock_engine = MagicMock()
    mock_begin_ctx = MagicMock()
    mock_conn_obj = MagicMock()
    mock_begin_ctx.__enter__ = MagicMock(return_value=mock_conn_obj)
    mock_begin_ctx.__exit__ = MagicMock(return_value=False)
    mock_engine.begin.return_value = mock_begin_ctx

    mock_connect_ctx = MagicMock()
    mock_connect_conn = MagicMock()
    mock_connect_conn.execute.return_value.fetchall.return_value = [("ts_col",)]
    mock_connect_ctx.__enter__ = MagicMock(return_value=mock_connect_conn)
    mock_connect_ctx.__exit__ = MagicMock(return_value=False)
    mock_engine.connect.return_value = mock_connect_ctx

    call_count = [0]

    def to_sql_first_fail(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise Exception("Error 273 timestamp column cannot be modified")

    with patch("pandas.DataFrame.to_sql", side_effect=to_sql_first_fail):
        op._to_sql_safe(df, "t", mock_engine, "dbo", "append", "mssql+pymssql")

    assert call_count[0] == 2  # first failed, second succeeded


def test_to_sql_safe_mssql_non_rowversion_error_reraises():
    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {"type": "load_target", "target": {}})
    df = pd.DataFrame({"id": [1]})

    mock_engine = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=MagicMock())
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_engine.begin.return_value = mock_ctx

    with patch("pandas.DataFrame.to_sql", side_effect=Exception("connection lost")):
        with pytest.raises(Exception, match="connection lost"):
            op._to_sql_safe(df, "t", mock_engine, "dbo", "append", "mssql+pymssql")


# ── _resolve_rdbms_url with conn_id ────────────────────────────────────────────

def test_resolve_rdbms_url_with_conn_id():
    import sys
    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {"type": "load_target", "target": {}})

    mock_conn = MagicMock()
    mock_conn.conn_type = "sqlserver"
    mock_conn.host = "sqlhost"
    mock_conn.port = 1433
    mock_conn.login = "sa"
    mock_conn.password = "pass"
    mock_conn.schema = "master"

    _ahb = sys.modules["airflow.hooks.base"]
    with patch.object(_ahb.BaseHook, "get_connection", return_value=mock_conn):
        url = op._resolve_rdbms_url("my_conn_id", {"database": "mydb"}, "mssql+pymssql")

    assert "mssql+pymssql" in url
    assert "sqlhost" in url


def test_resolve_rdbms_url_conn_id_basehook_fails_fallback():
    import sys
    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {"type": "load_target", "target": {}})

    _ahb = sys.modules["airflow.hooks.base"]
    with patch.object(_ahb.BaseHook, "get_connection", side_effect=Exception("no hook")):
        url = op._resolve_rdbms_url(
            "bad_conn",
            {"host": "h", "port": 5432, "username": "u", "password": "p", "database": "db"},
            "postgresql+psycopg2",
        )

    assert "postgresql+psycopg2" in url
    assert "h:5432/db" in url


# ── oracle execute path ────────────────────────────────────────────────────────

def test_write_to_oracle(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    op = _make_op(
        {"type": "oracle", "host": "h", "port": 1521, "username": "u",
         "password": "p", "database": "db", "schema": "sch", "table": "out", "mode": "append"},
        monkeypatch, noop_audit, noop_s3, db_path,
    )
    mock_engine = MagicMock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = MagicMock(return_value=MagicMock())
    mock_ctx.__exit__ = MagicMock(return_value=False)
    mock_engine.begin.return_value = mock_ctx

    with patch("sqlalchemy.create_engine", return_value=mock_engine):
        with patch.object(op, "_to_sql_safe") as mock_to_sql:
            op.execute(make_ctx())

    mock_to_sql.assert_called_once()
    assert "oracle" in mock_to_sql.call_args[0][5]  # dialect arg


# ── execute — unknown target type falls through to s3 ─────────────────────────

def test_write_unknown_target_type_falls_through_to_s3(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)
    op = _make_op(
        {"type": "unknown_format"},
        monkeypatch, noop_audit, noop_s3, db_path,
    )
    with patch.object(op, "get_s3_client", return_value=MagicMock()):
        with patch("pyarrow.parquet.write_table"):
            with patch("os.unlink"):
                result = op.execute(make_ctx())

    assert result is not None


# ── execute — download triggered by input_s3 ──────────────────────────────────

def test_execute_downloads_input_when_input_s3_set(tmp_path, monkeypatch, noop_audit, noop_s3):
    db_path = str(tmp_path / "input.duckdb")
    _setup_input_db(db_path)

    from operators.load_target import LoadTargetOperator
    op = make_op(LoadTargetOperator, {
        "type": "load_target",
        "source": {"table": "result"},
        "depends_on": ["upstream"],
        "target": {"type": "s3", "bucket": "my-bucket", "path": "out.parquet"},
    })
    # local_duckdb_path returns a non-existing path to trigger download
    non_existing = str(tmp_path / "nonexistent.duckdb")
    monkeypatch.setattr(op, "local_duckdb_path", lambda *_: non_existing)

    download_calls = []

    def fake_download(s3_path, local_path):
        # Copy the real db to the non-existing path so the op can read it
        import shutil
        shutil.copy(db_path, non_existing)
        download_calls.append((s3_path, local_path))

    monkeypatch.setattr(op, "download_duckdb", fake_download)

    ctx = make_ctx(xcom_data={("upstream", "duckdb_path"): "s3://bkt/upstream.duckdb"})
    with patch.object(op, "get_s3_client", return_value=MagicMock()):
        with patch("pyarrow.parquet.write_table"):
            with patch("os.unlink"):
                op.execute(ctx)

    assert len(download_calls) == 1
    assert download_calls[0][0] == "s3://bkt/upstream.duckdb"
