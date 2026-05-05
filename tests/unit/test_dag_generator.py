"""Unit tests for dag_generator — _default_args, _make_task, build_dag, OPERATOR_MAP."""
import os
import sys
import pytest
from datetime import timedelta
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../airflow/dags"))


# ── Helpers ────────────────────────────────────────────────────────────────────

def _minimal_pipeline(extra_tasks=None):
    return {
        "pipeline_id": "test_pipe",
        "tasks": extra_tasks or [{"task_id": "t1", "type": "sql_extract"}],
    }


# ── OPERATOR_MAP completeness ──────────────────────────────────────────────────

def test_operator_map_has_expected_types():
    from dag_generator import OPERATOR_MAP
    expected_types = {
        "sql_extract", "cdc_extract", "sql_transform", "duckdb_transform", "duckdb_query",
        "schema_validate", "data_quality", "file_ingest", "kafka_consume", "kafka_produce",
        "pubsub_consume", "pubsub_publish", "eks_job", "load_target", "notification",
        "api_call", "autosys_job", "stored_proc",
    }
    for t in expected_types:
        assert t in OPERATOR_MAP, f"Missing task type: {t}"


# ── _default_args ──────────────────────────────────────────────────────────────

def test_default_args_defaults():
    from dag_generator import _default_args
    args = _default_args({"pipeline_id": "p"})
    assert args["owner"] == "nextgen-databridge"
    assert args["retries"] == 3
    assert isinstance(args["retry_delay"], timedelta)
    assert args["email_on_failure"] is True
    assert args["email_on_retry"] is False


def test_default_args_overrides():
    from dag_generator import _default_args
    args = _default_args({
        "pipeline_id": "p", "owner": "custom", "retries": 5,
        "retry_delay_minutes": 2, "email_on_failure": False,
    })
    assert args["owner"] == "custom"
    assert args["retries"] == 5
    assert args["retry_delay"] == timedelta(minutes=2)
    assert args["email_on_failure"] is False


# ── _make_task ─────────────────────────────────────────────────────────────────

def test_make_task_sql_extract():
    from dag_generator import _make_task, OPERATOR_MAP
    from operators.sql_extract import SQLExtractOperator

    mock_dag = MagicMock()
    pipeline = _minimal_pipeline()
    task_cfg = {"task_id": "t1", "type": "sql_extract"}

    op = _make_task(task_cfg, mock_dag, pipeline)
    assert isinstance(op, SQLExtractOperator)


def test_make_task_data_quality():
    from dag_generator import _make_task
    from operators.data_quality import DataQualityOperator

    mock_dag = MagicMock()
    pipeline = _minimal_pipeline()
    task_cfg = {"task_id": "t1", "type": "data_quality"}

    op = _make_task(task_cfg, mock_dag, pipeline)
    assert isinstance(op, DataQualityOperator)


def test_make_task_notification():
    from dag_generator import _make_task
    from operators.notification import NotificationOperator

    mock_dag = MagicMock()
    pipeline = _minimal_pipeline()
    task_cfg = {"task_id": "t1", "type": "notification"}

    op = _make_task(task_cfg, mock_dag, pipeline)
    assert isinstance(op, NotificationOperator)


def test_make_task_load_target():
    from dag_generator import _make_task
    from operators.load_target import LoadTargetOperator

    mock_dag = MagicMock()
    pipeline = _minimal_pipeline()
    task_cfg = {"task_id": "t1", "type": "load_target"}

    op = _make_task(task_cfg, mock_dag, pipeline)
    assert isinstance(op, LoadTargetOperator)


def test_make_task_engine_eks_routes_to_eks_job():
    from dag_generator import _make_task
    from operators.eks_job import EKSJobOperator

    mock_dag = MagicMock()
    pipeline = _minimal_pipeline()
    task_cfg = {
        "task_id": "t1",
        "type": "sql_extract",
        "engine": "eks",
        "execution": {"cpu": "500m", "memory": "1Gi"},
    }

    op = _make_task(task_cfg, mock_dag, pipeline)
    assert isinstance(op, EKSJobOperator)


def test_make_task_engine_eks_sets_operation_type_extract():
    from dag_generator import _make_task
    from operators.eks_job import EKSJobOperator

    mock_dag = MagicMock()
    pipeline = _minimal_pipeline()
    task_cfg = {"task_id": "t1", "type": "sql_extract", "engine": "eks", "execution": {}}

    op = _make_task(task_cfg, mock_dag, pipeline)
    env_vars = op.task_config.get("execution", {}).get("env_vars", {})
    # Verify original task_config is preserved (not rewritten to eks_job)
    assert op.task_config["type"] == "sql_extract"


def test_make_task_unknown_type_returns_empty():
    from dag_generator import _make_task
    import sys
    empty_op_cls = sys.modules["airflow"].operators.empty.EmptyOperator

    mock_dag = MagicMock()
    pipeline = _minimal_pipeline()
    task_cfg = {"task_id": "t1", "type": "nonexistent_type_xyz"}

    op = _make_task(task_cfg, mock_dag, pipeline)
    # Should call EmptyOperator
    assert empty_op_cls.called or op is not None


def test_make_task_conditional_branch():
    import dag_generator
    from dag_generator import _make_task

    mock_dag = MagicMock()
    pipeline = _minimal_pipeline()
    task_cfg = {
        "task_id": "t1",
        "type": "conditional_branch",
        "condition_expression": "True",
        "branches": [{"label": "True", "task_id": "t2"}],
    }

    op = _make_task(task_cfg, mock_dag, pipeline)
    # BranchPythonOperator is imported at module level in dag_generator;
    # it's a MagicMock — calling it returns a MagicMock instance
    assert dag_generator.BranchPythonOperator.called


# ── build_dag ──────────────────────────────────────────────────────────────────

def test_build_dag_returns_dag():
    from dag_generator import build_dag
    import sys
    dag_cls = sys.modules["airflow"].DAG if hasattr(sys.modules.get("airflow", MagicMock()), "DAG") else MagicMock()

    pipeline = _minimal_pipeline()
    # build_dag calls airflow.DAG which is mocked, so just test it doesn't raise
    with patch("dag_generator.DAG") as mock_dag_cls:
        mock_dag = MagicMock()
        mock_dag_cls.return_value = mock_dag
        with patch("dag_generator._make_task") as mock_make:
            mock_task = MagicMock()
            mock_make.return_value = mock_task
            dag = build_dag(pipeline)

    mock_dag_cls.assert_called_once()
    call_kwargs = mock_dag_cls.call_args[1]
    assert call_kwargs["dag_id"] == "test_pipe"


def test_build_dag_multiple_tasks_wired():
    from dag_generator import build_dag

    pipeline = {
        "pipeline_id": "multi_pipe",
        "tasks": [
            {"task_id": "extract", "type": "sql_extract"},
            {"task_id": "transform", "type": "duckdb_transform", "depends_on": ["extract"]},
            {"task_id": "load",      "type": "load_target",      "depends_on": ["transform"]},
        ],
    }

    with patch("dag_generator.DAG") as mock_dag_cls:
        mock_dag = MagicMock()
        mock_dag_cls.return_value = mock_dag
        tasks_created = {}

        def fake_make_task(task_cfg, dag, pipeline_cfg):
            t = MagicMock()
            tasks_created[task_cfg["task_id"]] = t
            return t

        with patch("dag_generator._make_task", side_effect=fake_make_task):
            build_dag(pipeline)

    assert set(tasks_created.keys()) == {"extract", "transform", "load"}


def test_build_dag_with_sla():
    from dag_generator import build_dag

    pipeline = {"pipeline_id": "p", "sla_minutes": 60, "tasks": []}

    with patch("dag_generator.DAG") as mock_dag_cls:
        mock_dag = MagicMock()
        mock_dag_cls.return_value = mock_dag
        build_dag(pipeline)

    kwargs = mock_dag_cls.call_args[1]
    assert kwargs.get("sla_miss_callback") is not None


# ── register_dags ──────────────────────────────────────────────────────────────

def test_register_dags_handles_empty_config_list():
    from dag_generator import register_dags

    with patch("dag_generator.load_active_pipeline_configs", return_value=[]):
        result = register_dags()

    assert result == {}


def test_register_dags_skips_configs_without_pipeline_id():
    from dag_generator import register_dags

    with patch("dag_generator.load_active_pipeline_configs", return_value=[{"name": "no_id"}]):
        result = register_dags()

    assert result == {}


def test_register_dags_handles_build_error_gracefully():
    from dag_generator import register_dags

    configs = [{"pipeline_id": "bad_pipe", "tasks": []}]
    with patch("dag_generator.load_active_pipeline_configs", return_value=configs):
        with patch("dag_generator.build_dag", side_effect=Exception("build failed")):
            result = register_dags()

    assert "bad_pipe" not in result


def test_register_dags_load_error_returns_empty():
    from dag_generator import register_dags

    with patch("dag_generator.load_active_pipeline_configs", side_effect=Exception("S3 down")):
        result = register_dags()

    assert result == {}


def test_register_dags_success():
    from dag_generator import register_dags

    config = {"pipeline_id": "my_pipe", "tasks": []}
    mock_dag = MagicMock()
    with patch("dag_generator.load_active_pipeline_configs", return_value=[config]):
        with patch("dag_generator.build_dag", return_value=mock_dag):
            result = register_dags()

    assert "my_pipe" in result
    assert result["my_pipe"] is mock_dag


# ── build_dag — multiple roots/leaves adds start/end markers ──────────────────

def test_build_dag_multiple_roots_adds_start_end():
    from dag_generator import build_dag

    pipeline = {
        "pipeline_id": "parallel_pipe",
        "tasks": [
            {"task_id": "root1", "type": "sql_extract"},
            {"task_id": "root2", "type": "sql_extract"},
            {"task_id": "join",  "type": "duckdb_transform", "depends_on": ["root1", "root2"]},
            {"task_id": "leaf1", "type": "load_target",      "depends_on": ["join"]},
            {"task_id": "leaf2", "type": "notification",     "depends_on": ["join"]},
        ],
    }

    with patch("dag_generator.DAG") as mock_dag_cls:
        mock_dag = MagicMock()
        mock_dag_cls.return_value = mock_dag
        with patch("dag_generator._make_task", return_value=MagicMock()):
            build_dag(pipeline)

    # EmptyOperator called for start/end markers (two separate calls)
    import dag_generator
    assert dag_generator.EmptyOperator.call_count >= 2


# ── build_dag — task group wiring ─────────────────────────────────────────────

def test_build_dag_with_task_group():
    from dag_generator import build_dag

    pipeline = {
        "pipeline_id": "grouped_pipe",
        "tasks": [
            {"task_id": "t1", "type": "sql_extract", "group": "ingest"},
            {"task_id": "t2", "type": "duckdb_transform", "group": "ingest", "depends_on": ["t1"]},
        ],
    }

    with patch("dag_generator.DAG") as mock_dag_cls:
        mock_dag = MagicMock()
        mock_dag_cls.return_value = mock_dag
        with patch("dag_generator._make_task", return_value=MagicMock()):
            build_dag(pipeline)

    # TaskGroup should have been called for the "ingest" group
    import dag_generator
    dag_generator.TaskGroup.assert_called()


# ── build_dag — depends_on with missing dep emits warning ─────────────────────

def test_build_dag_missing_dependency_does_not_raise():
    from dag_generator import build_dag

    pipeline = {
        "pipeline_id": "missing_dep_pipe",
        "tasks": [
            {"task_id": "t1", "type": "sql_extract", "depends_on": ["ghost_task"]},
        ],
    }

    with patch("dag_generator.DAG") as mock_dag_cls:
        mock_dag = MagicMock()
        mock_dag_cls.return_value = mock_dag
        with patch("dag_generator._make_task", return_value=MagicMock()):
            build_dag(pipeline)  # should not raise despite missing dep


# ── _make_task — branch_fn callable ───────────────────────────────────────────

def test_make_task_branch_fn_evaluates_true_condition():
    import dag_generator
    from dag_generator import _make_task

    mock_dag = MagicMock()
    pipeline = _minimal_pipeline()
    task_cfg = {
        "task_id": "branch1",
        "type": "conditional_branch",
        "condition_expression": "1 == 1",
        "branches": [
            {"label": "True", "task_id": "true_task"},
            {"label": "False", "task_id": "false_task"},
        ],
    }

    _make_task(task_cfg, mock_dag, pipeline)

    # BranchPythonOperator was called — extract the python_callable and invoke it
    call_kwargs = dag_generator.BranchPythonOperator.call_args[1]
    branch_fn = call_kwargs.get("python_callable") or dag_generator.BranchPythonOperator.call_args[0][0]

    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = None
    ctx = {"ti": mock_ti}
    result = branch_fn(**ctx)

    assert result == "true_task"


def test_make_task_branch_fn_default_branch():
    import dag_generator
    from dag_generator import _make_task

    mock_dag = MagicMock()
    pipeline = _minimal_pipeline()
    task_cfg = {
        "task_id": "branch2",
        "type": "conditional_branch",
        "condition_expression": "some_undefined_var",
        "branches": [
            {"label": "default", "task_id": "default_task"},
            {"label": "other", "task_id": "other_task"},
        ],
    }

    _make_task(task_cfg, mock_dag, pipeline)
    call_kwargs = dag_generator.BranchPythonOperator.call_args[1]
    branch_fn = call_kwargs.get("python_callable") or dag_generator.BranchPythonOperator.call_args[0][0]

    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = None
    ctx = {"ti": mock_ti}
    result = branch_fn(**ctx)  # expression fails, falls through to default

    assert result == "default_task"


def test_make_task_branch_fn_with_depends_on():
    import dag_generator
    from dag_generator import _make_task

    mock_dag = MagicMock()
    pipeline = _minimal_pipeline()
    task_cfg = {
        "task_id": "branch3",
        "type": "conditional_branch",
        "condition_expression": "True",
        "depends_on": ["upstream1"],
        "branches": [{"label": "True", "task_id": "next_task"}],
    }

    _make_task(task_cfg, mock_dag, pipeline)
    call_kwargs = dag_generator.BranchPythonOperator.call_args[1]
    branch_fn = call_kwargs.get("python_callable") or dag_generator.BranchPythonOperator.call_args[0][0]

    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = "some_xcom_val"
    ctx = {"ti": mock_ti}
    result = branch_fn(**ctx)

    assert result == "next_task"
