"""Unit tests for config_loader functions."""
import json
import os
import sys
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../airflow/dags"))


# ── _build_substitution_map ────────────────────────────────────────────────────

def test_build_substitution_map_empty():
    from operators.config_loader import _build_substitution_map
    subs = _build_substitution_map({})
    assert subs == {}


def test_build_substitution_map_buckets():
    from operators.config_loader import _build_substitution_map
    env = {"s3_buckets": {"duckdb_store": "my-duckdb-bucket", "pipeline_configs": "my-configs"}}
    subs = _build_substitution_map(env)
    assert subs["duckdb_store"] == "my-duckdb-bucket"
    assert subs["pipeline_configs"] == "my-configs"


def test_build_substitution_map_registries():
    from operators.config_loader import _build_substitution_map
    env = {"container_registries": {"transform": "123.dkr.ecr.us-east-1.amazonaws.com"}}
    subs = _build_substitution_map(env)
    assert subs["registry.transform"] == "123.dkr.ecr.us-east-1.amazonaws.com"


def test_build_substitution_map_secrets():
    from operators.config_loader import _build_substitution_map
    env = {"secrets": {"wwi_sqlserver": "arn:aws:secretsmanager:us-east-1:123:secret:wwi"}}
    subs = _build_substitution_map(env)
    assert subs["secret.wwi_sqlserver"] == "arn:aws:secretsmanager:us-east-1:123:secret:wwi"


# ── _resolve_string ────────────────────────────────────────────────────────────

def test_resolve_string_no_placeholders():
    from operators.config_loader import _resolve_string
    assert _resolve_string("plain_value", {}) == "plain_value"


def test_resolve_string_replaces_known():
    from operators.config_loader import _resolve_string
    subs = {"my_bucket": "actual-bucket-name"}
    result = _resolve_string("s3://${my_bucket}/path", subs)
    assert result == "s3://actual-bucket-name/path"


def test_resolve_string_leaves_unknown():
    from operators.config_loader import _resolve_string
    result = _resolve_string("${unknown_key}/path", {})
    assert result == "${unknown_key}/path"


def test_resolve_string_multiple_placeholders():
    from operators.config_loader import _resolve_string
    subs = {"a": "X", "b": "Y"}
    result = _resolve_string("${a}-${b}", subs)
    assert result == "X-Y"


# ── _resolve_value ─────────────────────────────────────────────────────────────

def test_resolve_value_string():
    from operators.config_loader import _resolve_value
    subs = {"k": "v"}
    assert _resolve_value("${k}/path", subs) == "v/path"


def test_resolve_value_dict():
    from operators.config_loader import _resolve_value
    subs = {"k": "v"}
    result = _resolve_value({"key": "${k}/path", "num": 42}, subs)
    assert result == {"key": "v/path", "num": 42}


def test_resolve_value_list():
    from operators.config_loader import _resolve_value
    subs = {"k": "v"}
    result = _resolve_value(["${k}", "literal"], subs)
    assert result == ["v", "literal"]


def test_resolve_value_non_string_passthrough():
    from operators.config_loader import _resolve_value
    assert _resolve_value(42, {}) == 42
    assert _resolve_value(None, {}) is None
    assert _resolve_value(True, {}) is True


# ── _resolve_connections ───────────────────────────────────────────────────────

def test_resolve_connections_known_connection():
    from operators.config_loader import _resolve_connections
    config = {
        "pipeline_id": "p1",
        "tasks": [{
            "task_id": "t1",
            "source": {"connection": "wwi_sqlserver"},
        }],
    }
    env_config = {
        "connections": {
            "wwi_sqlserver": {
                "airflow_conn_id": "wwi_mssql_prod",
                "type": "mssql",
                "host": "db.internal",
                "port": 1433,
                "database": "WWI",
            }
        }
    }
    _resolve_connections(config, env_config)
    source = config["tasks"][0]["source"]
    assert source["connection"] == "wwi_mssql_prod"
    assert source["connection_type"] == "mssql"
    assert source["_host"] == "db.internal"
    assert source["_database"] == "WWI"


def test_resolve_connections_unknown_connection_unchanged():
    from operators.config_loader import _resolve_connections
    config = {
        "tasks": [{"task_id": "t1", "source": {"connection": "missing_conn"}}]
    }
    _resolve_connections(config, {"connections": {}})
    assert config["tasks"][0]["source"]["connection"] == "missing_conn"


def test_resolve_connections_no_connection_key():
    from operators.config_loader import _resolve_connections
    config = {"tasks": [{"task_id": "t1", "source": {"table": "MyTable"}}]}
    _resolve_connections(config, {"connections": {"x": {}}})
    assert config["tasks"][0]["source"] == {"table": "MyTable"}


# ── resolve_pipeline_config ────────────────────────────────────────────────────

def test_resolve_pipeline_config_full():
    from operators.config_loader import resolve_pipeline_config
    pipeline = {
        "pipeline_id": "my_pipeline",
        "tasks": [{
            "task_id": "extract",
            "type": "sql_extract",
            "source": {
                "connection": "wwi_db",
                "query": "SELECT * FROM ${schema}.orders",
            },
            "output": {"duckdb_path": "s3://${duckdb_store}/output.duckdb"},
        }],
    }
    env_config = {
        "s3_buckets": {"duckdb_store": "my-duckdb-bucket", "schema": "dbo"},
        "connections": {
            "wwi_db": {
                "airflow_conn_id": "wwi_prod",
                "type": "mssql",
                "host": "db.host",
                "port": 1433,
                "database": "WWI",
            }
        },
    }
    result = resolve_pipeline_config(pipeline, env_config)
    # DuckDB path placeholder resolved
    assert "my-duckdb-bucket" in result["tasks"][0]["output"]["duckdb_path"]
    # Connection resolved
    assert result["tasks"][0]["source"]["connection"] == "wwi_prod"
    # Original not mutated
    assert pipeline["tasks"][0]["source"]["connection"] == "wwi_db"


# ── load_environment_config ────────────────────────────────────────────────────

def test_load_environment_config_no_bucket(monkeypatch):
    import operators.config_loader as cl
    cl._env_config_cache = None
    monkeypatch.delenv("NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET", raising=False)
    result = cl.load_environment_config()
    assert result == {}
    cl._env_config_cache = None  # reset after test


def test_load_environment_config_from_s3(monkeypatch):
    import operators.config_loader as cl
    cl._env_config_cache = None
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET", "my-bucket")
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_ENV", "dev")

    mock_s3 = MagicMock()
    env_data = {"environment": "dev", "connections": {}}
    mock_s3.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=json.dumps(env_data).encode()))}

    with patch("boto3.client", return_value=mock_s3):
        result = cl.load_environment_config()

    assert result["environment"] == "dev"
    cl._env_config_cache = None


def test_load_environment_config_caches(monkeypatch):
    import operators.config_loader as cl
    cached = {"environment": "cached"}
    cl._env_config_cache = cached
    result = cl.load_environment_config()
    assert result is cached
    cl._env_config_cache = None


def test_load_environment_config_s3_error_returns_empty(monkeypatch):
    import operators.config_loader as cl
    cl._env_config_cache = None
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET", "my-bucket")

    with patch("boto3.client", side_effect=Exception("S3 down")):
        result = cl.load_environment_config()

    assert result == {}
    cl._env_config_cache = None


# ── load_active_pipeline_configs ───────────────────────────────────────────────

def test_load_active_pipeline_configs_from_db(monkeypatch):
    import operators.config_loader as cl
    cl._env_config_cache = {}

    config = {"pipeline_id": "p1", "tasks": []}
    mock_conn = MagicMock()
    mock_cur  = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchall.return_value = [(config,)]

    with patch("operators.config_loader._resolve_audit_db_url", return_value="postgresql://u:p@h/db"):
        with patch("psycopg2.connect", return_value=mock_conn):
            result = cl.load_active_pipeline_configs()

    assert len(result) == 1
    assert result[0]["pipeline_id"] == "p1"
    cl._env_config_cache = None


def test_load_active_pipeline_configs_fallback_to_s3(monkeypatch):
    import operators.config_loader as cl
    cl._env_config_cache = {}

    config = {"pipeline_id": "s3_pipeline", "tasks": []}
    mock_s3 = MagicMock()
    mock_s3.get_paginator.return_value.paginate.return_value = iter([{
        "Contents": [{"Key": "active/s3_pipeline.json"}]
    }])
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=json.dumps(config).encode()))
    }

    with patch("operators.config_loader._resolve_audit_db_url", side_effect=Exception("DB down")):
        with patch("psycopg2.connect", side_effect=Exception("DB down")):
            with patch("boto3.client", return_value=mock_s3):
                with patch.dict(os.environ, {"NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET": "my-bucket"}):
                    result = cl.load_active_pipeline_configs()

    assert len(result) == 1
    assert result[0]["pipeline_id"] == "s3_pipeline"
    cl._env_config_cache = None


# ── _load_from_db ──────────────────────────────────────────────────────────────

def test_load_from_db_returns_configs():
    from operators.config_loader import _load_from_db
    config = {"pipeline_id": "p1"}
    mock_conn = MagicMock()
    mock_cur  = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchall.return_value = [(config,), ({"pipeline_id": "p2"},)]

    with patch("operators.config_loader._resolve_audit_db_url", return_value="postgresql://u:p@h/db"):
        with patch("psycopg2.connect", return_value=mock_conn):
            result = _load_from_db()

    assert len(result) == 2


# ── load_pipeline_config (single) ─────────────────────────────────────────────

def test_load_pipeline_config_from_db():
    from operators.config_loader import load_pipeline_config
    config = {"pipeline_id": "p1", "tasks": []}
    mock_conn = MagicMock()
    mock_cur  = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchone.return_value = (config,)

    with patch("operators.config_loader.load_environment_config", return_value={}):
        with patch("operators.config_loader._resolve_audit_db_url", return_value="postgresql://u:p@h/db"):
            with patch("psycopg2.connect", return_value=mock_conn):
                result = load_pipeline_config("p1")

    assert result["pipeline_id"] == "p1"


def test_load_pipeline_config_returns_none_when_missing():
    from operators.config_loader import load_pipeline_config
    mock_conn = MagicMock()
    mock_cur  = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_cur.fetchone.return_value = None

    with patch("operators.config_loader.load_environment_config", return_value={}):
        with patch("operators.config_loader._resolve_audit_db_url", return_value="postgresql://u:p@h/db"):
            with patch("psycopg2.connect", return_value=mock_conn):
                result = load_pipeline_config("nonexistent")

    assert result is None
