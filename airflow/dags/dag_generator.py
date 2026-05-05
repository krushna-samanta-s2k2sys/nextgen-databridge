"""
Dynamic DAG Generator
Reads pipeline configs from S3/DB and generates Airflow DAGs at runtime.
This module is imported by Airflow's DagBag — it scans for pipeline configs
and generates a DAG object for each active pipeline.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.models import Variable

# NextGenDatabridge imports
from operators import (
    SQLExtractOperator,
    DuckDBTransformOperator,
    DataQualityOperator,
    SchemaValidateOperator,
    KafkaOperator,
    PubSubOperator,
    FileIngestOperator,
    EKSJobOperator,
    LoadTargetOperator,
    NotificationOperator,
)
from action_triggers import (
    APICallOperator,
    AutosysJobOperator,
    StoredProcOperator,
)
from operators.nextgen_databridge_callbacks import (
    on_success_callback,
    on_failure_callback,
    on_retry_callback,
    sla_miss_callback,
    dag_run_success_callback,
    dag_run_failure_callback,
)
from operators.config_loader import load_active_pipeline_configs

logger = logging.getLogger("nextgen_databridge.dag_generator")

# Resolved once at DAG-bag import time from the MWAA startup-script env vars.
_ECR_REGISTRY = os.getenv("NEXTGEN_DATABRIDGE_ECR_REGISTRY", "")
_ENV          = os.getenv("NEXTGEN_DATABRIDGE_ENV", "dev")

# ─────────────────────────────────────────────────────────────────────────────
# Operator factory
# ─────────────────────────────────────────────────────────────────────────────
OPERATOR_MAP: Dict[str, type] = {
    "sql_extract":       SQLExtractOperator,
    "cdc_extract":       SQLExtractOperator,
    "sql_transform":     DuckDBTransformOperator,
    "duckdb_transform":  DuckDBTransformOperator,
    "duckdb_query":      DuckDBTransformOperator,
    "schema_validate":   SchemaValidateOperator,
    "data_quality":      DataQualityOperator,
    "file_ingest":       FileIngestOperator,
    "kafka_consume":     KafkaOperator,
    "kafka_produce":     KafkaOperator,
    "pubsub_consume":    PubSubOperator,
    "pubsub_publish":    PubSubOperator,
    "eks_job":           EKSJobOperator,
    "load_target":       LoadTargetOperator,
    "notification":      NotificationOperator,
    "api_call":          APICallOperator,
    "autosys_job":       AutosysJobOperator,
    "stored_proc":       StoredProcOperator,
}


def _default_args(pipeline_config: dict) -> dict:
    """Build Airflow default_args from pipeline config"""
    return {
        "owner": pipeline_config.get("owner", "nextgen-databridge"),
        "depends_on_past": pipeline_config.get("depends_on_past", False),
        "email": pipeline_config.get("alert_emails", []),
        "email_on_failure": pipeline_config.get("email_on_failure", True),
        "email_on_retry": pipeline_config.get("email_on_retry", False),
        "retries": pipeline_config.get("retries", 3),
        "retry_delay": timedelta(minutes=pipeline_config.get("retry_delay_minutes", 5)),
        "retry_exponential_backoff": pipeline_config.get("retry_exponential_backoff", False),
        "max_retry_delay": timedelta(minutes=pipeline_config.get("max_retry_delay_minutes", 60)),
        "execution_timeout": timedelta(minutes=pipeline_config.get("task_timeout_minutes", 120)),
        "on_failure_callback": on_failure_callback,
        "on_retry_callback": on_retry_callback,
        "on_success_callback": on_success_callback,
    }


def _make_task(task_config: dict, dag: DAG, pipeline_config: dict):
    """
    Factory: create the right Airflow operator from a task config dict.
    Returns an Airflow BaseOperator subclass instance.
    """
    task_id       = task_config["task_id"]
    task_type     = task_config["type"]
    original_type = task_type
    execution     = task_config.get("execution", {})

    # Route to EKS when the task sets "engine": "eks" but isn't already eks_job.
    # The container dispatches on OPERATION_TYPE so the same image handles both.
    if task_config.get("engine") == "eks" and task_type != "eks_job":
        task_type = "eks_job"

    # Common kwargs passed to every operator
    common_kwargs = dict(
        task_id=task_id,
        dag=dag,
        pipeline_id=pipeline_config["pipeline_id"],
        task_config=task_config,
        pipeline_config=pipeline_config,
        duckdb_bucket=os.getenv("NEXTGEN_DATABRIDGE_DUCKDB_BUCKET", "nextgen-databridge-duckdb-store"),
        audit_db_url=os.getenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", ""),
        retries=task_config.get("retries", pipeline_config.get("retries", 3)),
        retry_delay=timedelta(
            minutes=task_config.get("retry_delay_minutes", pipeline_config.get("retry_delay_minutes", 5))
        ),
        execution_timeout=timedelta(
            minutes=task_config.get("timeout_minutes", pipeline_config.get("task_timeout_minutes", 120))
        ),
        on_failure_callback=on_failure_callback,
        on_retry_callback=on_retry_callback,
    )

    # Conditional branch uses BranchPythonOperator
    if task_type == "conditional_branch":
        branches = task_config.get("branches", [])
        condition_expr = task_config.get("condition_expression", "True")

        def branch_fn(**context):
            # Evaluate expression with XCom values available
            ti = context["ti"]
            # pull upstream XComs into local scope for eval
            xcom_values = {}
            for dep in task_config.get("depends_on", []):
                try:
                    xcom_values[dep] = ti.xcom_pull(task_ids=dep)
                except Exception:
                    pass
            try:
                result = eval(condition_expr, {"__builtins__": {}}, {**xcom_values, **context})
                for branch in branches:
                    if branch.get("label") == str(result) or branch.get("condition") == str(result):
                        return branch["task_id"]
            except Exception as e:
                logger.warning(f"Branch evaluation failed for {task_id}: {e}")
            # Default branch
            for branch in branches:
                if branch.get("label") == "default":
                    return branch["task_id"]
            return branches[0]["task_id"] if branches else None

        return BranchPythonOperator(
            task_id=task_id,
            python_callable=branch_fn,
            dag=dag,
        )

    # EKS job gets additional k8s parameters
    if task_type == "eks_job":
        # Determine what the container should do:
        #   extract tasks  -> OPERATION_TYPE=extract  (stream DB rows to Parquet on S3)
        #   everything else -> OPERATION_TYPE=transform (DuckDB SQL on staged files)
        operation_type = (
            "extract" if original_type in ("sql_extract", "cdc_extract") else "transform"
        )
        eks_env = {
            **execution.get("env_vars", {}),
            "OPERATION_TYPE": operation_type,
        }
        default_image = (
            f"{_ECR_REGISTRY}/nextgen-databridge/transform:{_ENV}"
            if _ECR_REGISTRY
            else f"nextgen-databridge/transform:{_ENV}"
        )
        common_kwargs.update({
            "image": execution.get("image", default_image),
            "cpu_request": execution.get("cpu", "1"),
            "memory_request": execution.get("memory", "2Gi"),
            "cpu_limit": execution.get("cpu_limit", execution.get("cpu", "2")),
            "memory_limit": execution.get("memory_limit", execution.get("memory", "4Gi")),
            "namespace": execution.get("namespace", "nextgen-databridge-jobs"),
            "eks_cluster_name": os.getenv("EKS_CLUSTER_NAME", "nextgen-databridge"),
            "service_account": execution.get("service_account", "nextgen-databridge-task-runner"),
            "env_vars": eks_env,
        })

    OperatorClass = OPERATOR_MAP.get(task_type)
    if not OperatorClass:
        logger.warning(f"Unknown task type '{task_type}' for task '{task_id}', using EmptyOperator")
        return EmptyOperator(task_id=task_id, dag=dag)

    return OperatorClass(**common_kwargs)


def build_dag(pipeline_config: dict) -> DAG:
    """
    Build a complete Airflow DAG from a pipeline config dict.
    Sets up all tasks, dependencies, task groups, and callbacks.
    """
    pid       = pipeline_config["pipeline_id"]
    schedule  = pipeline_config.get("schedule")
    sla_mins  = pipeline_config.get("sla_minutes")
    max_runs  = pipeline_config.get("max_active_runs", 1)
    catchup   = pipeline_config.get("catchup", False)
    tags      = pipeline_config.get("tags", [])

    sla = timedelta(minutes=sla_mins) if sla_mins else None

    dag = DAG(
        dag_id=pid,
        default_args=_default_args(pipeline_config),
        description=pipeline_config.get("description", f"NextGenDatabridge pipeline: {pid}"),
        schedule_interval=schedule,
        start_date=days_ago(1),
        catchup=catchup,
        max_active_runs=max_runs,
        tags=["nextgen-databridge"] + tags,
        sla_miss_callback=sla_miss_callback,
        on_success_callback=dag_run_success_callback,
        on_failure_callback=dag_run_failure_callback,
        dagrun_timeout=timedelta(minutes=pipeline_config.get("dag_timeout_minutes", 480)),
        doc_md=f"""
## Pipeline: {pid}

{pipeline_config.get('description', '')}

**Version**: {pipeline_config.get('version', 'unknown')}
**Owner**: {pipeline_config.get('owner', 'nextgen-databridge')}
**Schedule**: `{schedule or 'manual'}`
**SLA**: {f'{sla_mins} minutes' if sla_mins else 'None'}
        """,
    )

    tasks_by_id: Dict[str, Any] = {}
    tasks_config = pipeline_config.get("tasks", [])

    # ── Build task groups for parallel sections ──────────────────────────
    # Detect parallelizable groups: tasks with same depth in the DAG
    dep_map: Dict[str, List[str]] = {
        t["task_id"]: t.get("depends_on", [])
        for t in tasks_config
        if isinstance(t, dict)
    }

    # Assign group labels if task config specifies them
    groups: Dict[str, TaskGroup] = {}
    for task_config in tasks_config:
        group_name = task_config.get("group")
        if group_name and group_name not in groups:
            groups[group_name] = TaskGroup(group_id=group_name, dag=dag)

    # ── Create operators ─────────────────────────────────────────────────
    for task_config in tasks_config:
        if not isinstance(task_config, dict):
            continue
        task_id = task_config.get("task_id")
        if not task_id:
            continue

        group_name = task_config.get("group")
        if group_name and group_name in groups:
            with groups[group_name]:
                op = _make_task(task_config, dag, pipeline_config)
        else:
            op = _make_task(task_config, dag, pipeline_config)

        tasks_by_id[task_id] = op

    # ── Wire dependencies ─────────────────────────────────────────────────
    for task_config in tasks_config:
        if not isinstance(task_config, dict):
            continue
        task_id = task_config.get("task_id")
        deps    = task_config.get("depends_on", [])
        trigger = task_config.get("trigger_rule", "all_success")

        if task_id not in tasks_by_id:
            continue

        op = tasks_by_id[task_id]

        # Set trigger rule
        if hasattr(op, "trigger_rule"):
            op.trigger_rule = trigger

        # Set upstream dependencies
        for dep_id in deps:
            if dep_id in tasks_by_id:
                tasks_by_id[dep_id] >> op
            else:
                logger.warning(f"DAG '{pid}': task '{task_id}' depends on missing '{dep_id}'")

    # ── Pipeline start/end markers ────────────────────────────────────────
    roots  = [tid for tid, deps in dep_map.items() if not deps]
    leaves = [tid for tid in dep_map if not any(tid in d for d in dep_map.values())]

    if len(roots) > 1 or len(leaves) > 1:
        # Add explicit start/end nodes when multiple roots/leaves
        start = EmptyOperator(task_id="_pipeline_start", dag=dag)
        end   = EmptyOperator(task_id="_pipeline_end", dag=dag, trigger_rule="all_done")
        tasks_by_id["_pipeline_start"] = start
        tasks_by_id["_pipeline_end"]   = end

        for root_id in roots:
            if root_id in tasks_by_id:
                start >> tasks_by_id[root_id]

        for leaf_id in leaves:
            if leaf_id in tasks_by_id:
                tasks_by_id[leaf_id] >> end

    # ── Cross-DAG upstream sensors ────────────────────────────────────────
    # Each upstream entry creates an ExternalTaskSensor that blocks until the
    # upstream DAG reaches an allowed state before any task in this DAG starts.
    dag_deps        = pipeline_config.get("dag_dependencies", {})
    upstream_deps   = dag_deps.get("upstream", [])
    downstream_deps = dag_deps.get("downstream", [])

    for upstream_dep in upstream_deps:
        upstream_pid   = upstream_dep["pipeline_id"]
        allowed_states = upstream_dep.get("allowed_states", ["success"])
        timeout_secs   = int(upstream_dep.get("timeout_minutes", 240) * 60)

        sensor = ExternalTaskSensor(
            task_id=f"wait_for_{upstream_pid}",
            external_dag_id=upstream_pid,
            external_task_id=None,  # None = wait for the entire upstream DAG
            allowed_states=allowed_states,
            poke_interval=60,
            timeout=timeout_secs,
            mode="reschedule",      # free the worker slot between pokes
            dag=dag,
        )
        if "_pipeline_start" in tasks_by_id:
            sensor >> tasks_by_id["_pipeline_start"]
        else:
            for root_id in roots:
                if root_id in tasks_by_id:
                    sensor >> tasks_by_id[root_id]

    # ── Cross-DAG downstream triggers ────────────────────────────────────
    # Each downstream entry triggers the target DAG after all tasks in this
    # pipeline succeed, then waits for it to complete before marking this run done.
    for downstream_dep in downstream_deps:
        downstream_pid = downstream_dep["pipeline_id"]
        conf           = downstream_dep.get("conf") or {}

        trigger = TriggerDagRunOperator(
            task_id=f"trigger_{downstream_pid}",
            trigger_dag_id=downstream_pid,
            wait_for_completion=True,
            conf=conf,
            trigger_rule="all_success",
            dag=dag,
        )
        if "_pipeline_end" in tasks_by_id:
            tasks_by_id["_pipeline_end"] >> trigger
        else:
            for leaf_id in leaves:
                if leaf_id in tasks_by_id:
                    tasks_by_id[leaf_id] >> trigger

    logger.info(f"Built DAG '{pid}' with {len(tasks_by_id)} tasks")
    return dag


# ─────────────────────────────────────────────────────────────────────────────
# DagBag registration — called by Airflow when it scans this file
# ─────────────────────────────────────────────────────────────────────────────
def register_dags() -> Dict[str, DAG]:
    """
    Load all active pipeline configs and register DAGs.
    Returns dict of dag_id -> DAG for Airflow DagBag.
    """
    generated: Dict[str, DAG] = {}
    try:
        configs = load_active_pipeline_configs()
        for config in configs:
            pid = config.get("pipeline_id")
            if not pid:
                continue
            try:
                dag = build_dag(config)
                generated[pid] = dag
                logger.info(f"Registered DAG: {pid}")
            except Exception as e:
                logger.error(f"Failed to build DAG '{pid}': {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Failed to load pipeline configs: {e}", exc_info=True)

    return generated


# Register in global scope for Airflow DagBag discovery
try:
    _dags = register_dags()
    for _dag_id, _dag in _dags.items():
        globals()[_dag_id] = _dag
except Exception as _e:
    logger.error(f"DAG registration failed: {_e}", exc_info=True)
