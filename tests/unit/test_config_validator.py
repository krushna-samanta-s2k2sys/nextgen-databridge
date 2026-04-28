"""
Unit tests for PipelineConfigValidator
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../backend'))

import pytest
from services.config_validator import PipelineConfigValidator


@pytest.fixture
def v():
    return PipelineConfigValidator()


MINIMAL_VALID = {
    "pipeline_id": "test_pipeline",
    "version": "1.0.0",
    "tasks": [
        {
            "task_id": "extract",
            "type": "sql_extract",
            "source": {"connection": "sqlserver_prod", "query": "SELECT 1"},
            "output": {"duckdb_path": "s3://bucket/test.duckdb"}
        }
    ]
}


def test_valid_minimal(v):
    r = v.validate(MINIMAL_VALID)
    assert r.valid


def test_missing_pipeline_id(v):
    cfg = {**MINIMAL_VALID}
    del cfg["pipeline_id"]
    r = v.validate(cfg)
    assert not r.valid
    assert any("pipeline_id" in e for e in r.errors)


def test_missing_tasks(v):
    r = v.validate({"pipeline_id": "test", "tasks": []})
    assert not r.valid


def test_invalid_pipeline_id_uppercase(v):
    cfg = {**MINIMAL_VALID, "pipeline_id": "MyPipeline"}
    r = v.validate(cfg)
    assert not r.valid


def test_unknown_task_type(v):
    cfg = {**MINIMAL_VALID, "tasks": [{"task_id": "x", "type": "not_a_type",
                                        "source": {"connection": "c", "query": "q"},
                                        "output": {"duckdb_path": "s3://b/f.duckdb"}}]}
    r = v.validate(cfg)
    assert not r.valid
    assert any("not_a_type" in e for e in r.errors)


def test_duplicate_task_id(v):
    cfg = {**MINIMAL_VALID, "tasks": [
        {"task_id": "same", "type": "sql_extract", "source": {"connection": "c", "query": "q"}, "output": {"duckdb_path": "s3://b/1.duckdb"}},
        {"task_id": "same", "type": "sql_extract", "source": {"connection": "c", "query": "q"}, "output": {"duckdb_path": "s3://b/2.duckdb"}},
    ]}
    r = v.validate(cfg)
    assert not r.valid
    assert any("Duplicate" in e for e in r.errors)


def test_missing_dependency(v):
    cfg = {**MINIMAL_VALID, "tasks": [
        {"task_id": "t1", "type": "sql_extract", "source": {"connection": "c", "query": "q"}, "output": {"duckdb_path": "s3://b/t1.duckdb"}},
        {"task_id": "t2", "type": "duckdb_transform", "depends_on": ["does_not_exist"],
         "sql": "SELECT 1", "output": {"duckdb_path": "s3://b/t2.duckdb"}},
    ]}
    r = v.validate(cfg)
    assert not r.valid
    assert any("does_not_exist" in e for e in r.errors)


def test_cycle_detection(v):
    cfg = {**MINIMAL_VALID, "tasks": [
        {"task_id": "a", "type": "duckdb_transform", "depends_on": ["b"], "sql": "S", "output": {"duckdb_path": "s3://b/a.duckdb"}},
        {"task_id": "b", "type": "duckdb_transform", "depends_on": ["a"], "sql": "S", "output": {"duckdb_path": "s3://b/b.duckdb"}},
    ]}
    r = v.validate(cfg)
    assert not r.valid
    assert any("Circular" in e for e in r.errors)


def test_valid_linear_dag(v):
    cfg = {**MINIMAL_VALID, "tasks": [
        {"task_id": "extract", "type": "sql_extract", "source": {"connection": "c", "query": "q"}, "output": {"duckdb_path": "s3://b/e.duckdb"}},
        {"task_id": "transform", "type": "duckdb_transform", "depends_on": ["extract"], "sql": "SELECT 1", "output": {"duckdb_path": "s3://b/t.duckdb"}},
        {"task_id": "validate", "type": "data_quality", "depends_on": ["transform"],
         "checks": [{"type": "not_null", "column": "id", "action": "fail"}]},
        {"task_id": "load", "type": "load_target", "depends_on": ["validate"],
         "source": {"table": "result"}, "target": {"type": "s3"}},
    ]}
    r = v.validate(cfg)
    assert r.valid, r.errors


def test_invalid_qc_check(v):
    cfg = {**MINIMAL_VALID, "tasks": [
        {"task_id": "qc", "type": "data_quality",
         "checks": [{"type": "not_a_check", "column": "x", "action": "fail"}]}
    ]}
    r = v.validate(cfg)
    assert not r.valid
    assert any("not_a_check" in e for e in r.errors)


def test_empty_qc_checks(v):
    cfg = {**MINIMAL_VALID, "tasks": [
        {"task_id": "qc", "type": "data_quality", "checks": []}
    ]}
    r = v.validate(cfg)
    assert not r.valid


def test_valid_cron(v):
    cfg = {**MINIMAL_VALID, "schedule": "0 6 * * *"}
    r = v.validate(cfg)
    assert r.valid


def test_invalid_retries(v):
    cfg = {**MINIMAL_VALID, "retries": -1}
    r = v.validate(cfg)
    assert not r.valid


def test_valid_sla(v):
    cfg = {**MINIMAL_VALID, "sla_minutes": 120}
    r = v.validate(cfg)
    assert r.valid


def test_branch_task_needs_branches(v):
    cfg = {**MINIMAL_VALID, "tasks": [
        {"task_id": "branch", "type": "conditional_branch", "branches": [{"label": "a", "task_id": "x"}]}
    ]}
    r = v.validate(cfg)
    assert not r.valid


def test_sql_task_needs_connection_or_duckdb(v):
    cfg = {**MINIMAL_VALID, "tasks": [
        {"task_id": "t", "type": "sql_extract", "source": {}, "output": {"duckdb_path": "s3://b/t.duckdb"}}
    ]}
    r = v.validate(cfg)
    assert not r.valid


def test_validation_result_to_dict(v):
    r = v.validate(MINIMAL_VALID)
    d = r.to_dict()
    assert "valid" in d
    assert "errors" in d
    assert "warnings" in d
    assert "info" in d


def test_large_valid_pipeline(v):
    tasks = [{"task_id": "extract", "type": "sql_extract", "source": {"connection": "c", "query": "q"}, "output": {"duckdb_path": "s3://b/0.duckdb"}}]
    prev = "extract"
    for i in range(1, 10):
        tid = f"transform_{i}"
        tasks.append({"task_id": tid, "type": "duckdb_transform", "depends_on": [prev], "sql": "SELECT 1", "output": {"duckdb_path": f"s3://b/{i}.duckdb"}})
        prev = tid
    cfg = {**MINIMAL_VALID, "tasks": tasks}
    r = v.validate(cfg)
    assert r.valid
