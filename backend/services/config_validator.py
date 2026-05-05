"""
Pipeline Configuration Validator
Validates pipeline JSON configs against schema, checks DAG is acyclic,
verifies connections exist, checks EKS resources.
"""
from __future__ import annotations
import re
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field


VALID_TASK_TYPES = {
    "sql_extract", "sql_transform", "duckdb_transform", "duckdb_query",
    "schema_validate", "data_quality", "cdc_extract", "file_ingest",
    "kafka_consume", "kafka_produce", "pubsub_consume", "pubsub_publish",
    "eks_job", "python_callable", "conditional_branch", "load_target", "notification",
    "api_call", "autosys_job", "stored_proc",
}

VALID_QC_CHECKS = {
    "not_null", "unique", "row_count_min", "row_count_max", "freshness",
    "value_range", "regex_match", "referential_integrity", "schema_match",
    "custom_sql"
}

VALID_ALERT_CHANNELS = re.compile(
    r"^(slack:#.+|email:.+@.+|pagerduty:.+|teams:.+|webhook:https?://.+)$"
)

CRON_PATTERN = re.compile(
    r"^(\*|[0-9]{1,2}|\*/[0-9]+|[0-9]+-[0-9]+)"
    r"( (\*|[0-9]{1,2}|\*/[0-9]+|[0-9]+-[0-9]+)){4}$"
)


@dataclass
class ValidationResult:
    valid: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    info: List[str] = field(default_factory=list)

    def error(self, msg: str):
        self.errors.append(msg)
        self.valid = False

    def warn(self, msg: str):
        self.warnings.append(msg)

    def note(self, msg: str):
        self.info.append(msg)

    def to_dict(self) -> Dict:
        return {
            "valid": self.valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "info": self.info,
        }


class PipelineConfigValidator:
    """
    Full pipeline config validator.
    Checks schema, DAG structure, task types, QC rules, etc.
    """

    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult()

        # ── Top-level required fields ──────────────────────────────────────
        for field in ("pipeline_id", "tasks"):
            if field not in config:
                result.error(f"Missing required top-level field: '{field}'")

        if not result.valid:
            return result

        # ── pipeline_id format ────────────────────────────────────────────
        pid = config.get("pipeline_id", "")
        if not re.match(r"^[a-z][a-z0-9_]{1,198}[a-z0-9]$", pid):
            result.error(
                f"pipeline_id '{pid}' must be lowercase snake_case, 3-200 chars, "
                "start with a letter"
            )

        # ── version ───────────────────────────────────────────────────────
        version = config.get("version", "")
        if version and not re.match(r"^\d+\.\d+\.\d+$", version):
            result.warn(f"version '{version}' should follow semver (e.g. 1.0.0)")

        # ── schedule ─────────────────────────────────────────────────────
        schedule = config.get("schedule")
        if schedule and not CRON_PATTERN.match(schedule.strip()):
            result.warn(
                f"schedule '{schedule}' may not be a valid cron expression"
            )

        # ── retries / SLA ─────────────────────────────────────────────────
        retries = config.get("retries", 0)
        if not isinstance(retries, int) or retries < 0 or retries > 10:
            result.error("retries must be an integer between 0 and 10")

        sla = config.get("sla_minutes")
        if sla is not None and (not isinstance(sla, (int, float)) or sla <= 0):
            result.error("sla_minutes must be a positive number")

        # ── alerting ──────────────────────────────────────────────────────
        alerting = config.get("alerting", {})
        for event_key in ("on_failure", "on_success", "on_sla_breach", "on_retry"):
            channels = alerting.get(event_key, [])
            if not isinstance(channels, list):
                result.error(f"alerting.{event_key} must be a list")
                continue
            for ch in channels:
                if not VALID_ALERT_CHANNELS.match(ch):
                    result.warn(
                        f"alerting.{event_key}: channel '{ch}' format not recognised "
                        "(use slack:#channel, email:addr, pagerduty:key, teams:webhook, webhook:url)"
                    )

        # ── DAG dependencies (cross-pipeline) ────────────────────────────
        if "dag_dependencies" in config:
            self._validate_dag_dependencies(config, result)

        # ── tasks ────────────────────────────────────────────────────────
        tasks = config.get("tasks", [])
        if not isinstance(tasks, list) or len(tasks) == 0:
            result.error("'tasks' must be a non-empty list")
            return result

        task_ids = set()
        for i, task in enumerate(tasks):
            self._validate_task(task, i, task_ids, result)

        # ── DAG acyclicity check ──────────────────────────────────────────
        self._check_dag_acyclic(tasks, result)

        # ── Warn about unreachable tasks ──────────────────────────────────
        self._check_unreachable_tasks(tasks, result)

        result.note(f"Config validated: {len(tasks)} tasks, pipeline_id='{pid}'")
        return result

    # ─────────────────────────────────────────────────────────────────────
    def _validate_task(
        self,
        task: Any,
        index: int,
        task_ids: set,
        result: ValidationResult,
    ):
        if not isinstance(task, dict):
            result.error(f"Task at index {index} must be an object")
            return

        # Required
        task_id = task.get("task_id")
        if not task_id:
            result.error(f"Task[{index}] missing 'task_id'")
            return

        if not re.match(r"^[a-z][a-z0-9_]*$", task_id):
            result.error(
                f"Task '{task_id}' id must be lowercase snake_case"
            )

        if task_id in task_ids:
            result.error(f"Duplicate task_id: '{task_id}'")
        task_ids.add(task_id)

        task_type = task.get("type")
        if not task_type:
            result.error(f"Task '{task_id}' missing 'type'")
        elif task_type not in VALID_TASK_TYPES:
            result.error(
                f"Task '{task_id}' unknown type '{task_type}'. "
                f"Valid: {sorted(VALID_TASK_TYPES)}"
            )

        # depends_on
        deps = task.get("depends_on", [])
        if not isinstance(deps, list):
            result.error(f"Task '{task_id}'.depends_on must be a list")

        # Type-specific validation
        if task_type == "data_quality":
            self._validate_qc_task(task, result)
        elif task_type == "eks_job":
            self._validate_eks_task(task, result)
        elif task_type in ("sql_extract", "sql_transform", "cdc_extract"):
            self._validate_sql_task(task, result)
        elif task_type in ("kafka_consume", "kafka_produce"):
            self._validate_kafka_task(task, result)
        elif task_type == "conditional_branch":
            self._validate_branch_task(task, result)
        elif task_type == "api_call":
            self._validate_api_call_task(task, result)
        elif task_type == "autosys_job":
            self._validate_autosys_task(task, result)
        elif task_type == "stored_proc":
            self._validate_stored_proc_task(task, result)

        # output is required for most tasks (not validation/notification/trigger tasks)
        non_output_types = {
            "schema_validate", "data_quality", "notification", "conditional_branch",
            "api_call", "autosys_job",
        }
        if task_type not in non_output_types:
            output = task.get("output", {})
            if not output.get("duckdb_path") and not output.get("target"):
                result.warn(
                    f"Task '{task_id}' has no output.duckdb_path or output.target defined"
                )

    def _validate_qc_task(self, task: dict, result: ValidationResult):
        task_id = task["task_id"]
        checks = task.get("checks", [])
        if not isinstance(checks, list) or len(checks) == 0:
            result.error(f"Task '{task_id}' (data_quality) must have a non-empty 'checks' list")
            return
        for check in checks:
            check_type = check.get("type")
            if check_type not in VALID_QC_CHECKS:
                result.error(
                    f"Task '{task_id}': unknown QC check type '{check_type}'. "
                    f"Valid: {sorted(VALID_QC_CHECKS)}"
                )
            action = check.get("action", "fail")
            if action not in ("fail", "warn", "skip"):
                result.error(
                    f"Task '{task_id}' check '{check_type}' action must be fail/warn/skip"
                )

    def _validate_eks_task(self, task: dict, result: ValidationResult):
        task_id = task["task_id"]
        execution = task.get("execution", {})
        if execution.get("mode") != "eks_job":
            result.warn(
                f"Task '{task_id}' type is 'eks_job' but execution.mode is not 'eks_job'"
            )
        cpu = execution.get("cpu")
        memory = execution.get("memory")
        if cpu and not re.match(r"^\d+(\.\d+)?m?$", str(cpu)):
            result.warn(f"Task '{task_id}' execution.cpu format looks wrong: '{cpu}'")
        if memory and not re.match(r"^\d+(Ki|Mi|Gi|Ti|m)?$", str(memory)):
            result.warn(f"Task '{task_id}' execution.memory format looks wrong: '{memory}'")

    def _validate_sql_task(self, task: dict, result: ValidationResult):
        task_id = task["task_id"]
        source = task.get("source", {})
        if not source.get("connection") and not source.get("duckdb_path"):
            result.error(
                f"Task '{task_id}': sql task must have source.connection or source.duckdb_path"
            )
        if not source.get("query") and not source.get("table"):
            result.error(
                f"Task '{task_id}': sql task must have source.query or source.table"
            )

    def _validate_kafka_task(self, task: dict, result: ValidationResult):
        task_id = task["task_id"]
        if not task.get("connection"):
            result.error(f"Task '{task_id}': kafka task must have 'connection'")
        if not task.get("topic"):
            result.error(f"Task '{task_id}': kafka task must have 'topic'")

    def _validate_branch_task(self, task: dict, result: ValidationResult):
        task_id = task["task_id"]
        branches = task.get("branches", [])
        if len(branches) < 2:
            result.error(
                f"Task '{task_id}' (conditional_branch) must define at least 2 branches"
            )
        for b in branches:
            if not b.get("condition") and b.get("label") != "default":
                result.warn(
                    f"Task '{task_id}' branch '{b.get('label', '?')}' has no condition"
                )

    def _validate_api_call_task(self, task: dict, result: ValidationResult):
        task_id = task["task_id"]
        if not task.get("url"):
            result.error(f"Task '{task_id}' (api_call) must have 'url'")
        method = task.get("method", "GET").upper()
        if method not in ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"):
            result.error(f"Task '{task_id}' (api_call) invalid method '{method}'")
        auth = task.get("auth") or {}
        auth_type = auth.get("type", "none")
        if auth_type not in ("none", "basic", "bearer", "api_key",
                             "oauth2_client_credentials"):
            result.error(
                f"Task '{task_id}' (api_call) unknown auth.type '{auth_type}'"
            )
        if auth_type == "basic" and (not auth.get("username") or not auth.get("password")):
            result.error(f"Task '{task_id}' (api_call) basic auth requires username and password")
        if auth_type == "bearer" and not auth.get("token"):
            result.error(f"Task '{task_id}' (api_call) bearer auth requires token")
        if auth_type == "api_key" and (not auth.get("key") or not auth.get("value")):
            result.error(f"Task '{task_id}' (api_call) api_key auth requires key and value")
        if auth_type == "oauth2_client_credentials":
            for field in ("token_url", "client_id", "client_secret"):
                if not auth.get(field):
                    result.error(
                        f"Task '{task_id}' (api_call) oauth2_client_credentials requires '{field}'"
                    )

    def _validate_autosys_task(self, task: dict, result: ValidationResult):
        task_id = task["task_id"]
        if not task.get("autosys_url"):
            result.error(f"Task '{task_id}' (autosys_job) must have 'autosys_url'")
        if not task.get("job_name"):
            result.error(f"Task '{task_id}' (autosys_job) must have 'job_name'")
        auth = task.get("auth") or {}
        auth_type = auth.get("type", "none")
        if auth_type not in ("none", "basic", "bearer", "autosys_token"):
            result.error(
                f"Task '{task_id}' (autosys_job) unknown auth.type '{auth_type}'"
            )
        if auth_type == "autosys_token" and (not auth.get("username") or not auth.get("password")):
            result.error(
                f"Task '{task_id}' (autosys_job) autosys_token auth requires username and password"
            )
        timeout = task.get("timeout_seconds")
        if timeout is not None and (not isinstance(timeout, int) or timeout <= 0):
            result.error(f"Task '{task_id}' (autosys_job) timeout_seconds must be a positive int")

    def _validate_stored_proc_task(self, task: dict, result: ValidationResult):
        task_id = task["task_id"]
        if not task.get("connection"):
            result.error(f"Task '{task_id}' (stored_proc) must have 'connection'")
        if not task.get("procedure"):
            result.error(f"Task '{task_id}' (stored_proc) must have 'procedure'")
        for param in task.get("parameters") or []:
            if not param.get("name"):
                result.error(f"Task '{task_id}' (stored_proc) parameter missing 'name'")
            direction = param.get("direction", "in")
            if direction not in ("in", "out", "inout"):
                result.error(
                    f"Task '{task_id}' (stored_proc) parameter '{param.get('name')}' "
                    f"direction must be in/out/inout"
                )
        if task.get("capture_resultset") and not (task.get("output") or {}).get("duckdb_path"):
            result.warn(
                f"Task '{task_id}' (stored_proc) has capture_resultset=true "
                "but no output.duckdb_path defined"
            )

    def _validate_dag_dependencies(self, config: dict, result: ValidationResult):
        pid      = config.get("pipeline_id", "")
        dag_deps = config.get("dag_dependencies", {})

        if not isinstance(dag_deps, dict):
            result.error("dag_dependencies must be an object with 'upstream' and/or 'downstream' keys")
            return

        _VALID_STATES = {"success", "failed", "skipped", "upstream_failed"}

        for direction in ("upstream", "downstream"):
            entries = dag_deps.get(direction, [])
            if not entries:
                continue
            if not isinstance(entries, list):
                result.error(f"dag_dependencies.{direction} must be a list")
                continue

            for i, entry in enumerate(entries):
                if not isinstance(entry, dict):
                    result.error(f"dag_dependencies.{direction}[{i}] must be an object")
                    continue

                dep_pid = entry.get("pipeline_id")
                if not dep_pid or not isinstance(dep_pid, str):
                    result.error(f"dag_dependencies.{direction}[{i}] must have a non-empty 'pipeline_id'")
                elif dep_pid == pid:
                    result.error(f"dag_dependencies.{direction}[{i}]: pipeline cannot depend on itself ('{pid}')")

                if direction == "upstream":
                    await_status = entry.get("await_status", "success")
                    if await_status not in _VALID_STATES:
                        result.error(
                            f"dag_dependencies.upstream[{i}].await_status '{await_status}' is not valid. "
                            f"Valid: {sorted(_VALID_STATES)}"
                        )

    def _check_dag_acyclic(self, tasks: List[dict], result: ValidationResult):
        """Topological sort (Kahn's algorithm) to detect cycles"""
        task_ids = {t["task_id"] for t in tasks if isinstance(t, dict) and "task_id" in t}
        graph: Dict[str, List[str]] = {t["task_id"]: [] for t in tasks if isinstance(t, dict) and "task_id" in t}
        in_degree: Dict[str, int] = {tid: 0 for tid in task_ids}

        for task in tasks:
            if not isinstance(task, dict):
                continue
            tid = task.get("task_id")
            for dep in task.get("depends_on", []):
                if dep not in task_ids:
                    result.error(
                        f"Task '{tid}' depends on '{dep}' which does not exist"
                    )
                    continue
                graph[dep].append(tid)
                in_degree[tid] += 1

        # Kahn's
        queue = [t for t, d in in_degree.items() if d == 0]
        visited = 0
        while queue:
            node = queue.pop(0)
            visited += 1
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if visited != len(task_ids):
            cycle_nodes = [t for t, d in in_degree.items() if d > 0]
            result.error(
                f"Circular dependency detected among tasks: {cycle_nodes}"
            )
        else:
            result.note("DAG is acyclic — topological order is valid")

    def _check_unreachable_tasks(self, tasks: List[dict], result: ValidationResult):
        """Warn about tasks with no path from root and no dependents"""
        task_ids = {t["task_id"] for t in tasks if isinstance(t, dict) and "task_id" in t}
        has_dependents = set()
        for task in tasks:
            for dep in task.get("depends_on", []):
                has_dependents.add(dep)

        roots = task_ids - {
            t["task_id"]
            for t in tasks
            if isinstance(t, dict) and t.get("depends_on")
        }
        leaves = task_ids - has_dependents

        result.note(f"DAG roots (no upstream deps): {sorted(roots)}")
        result.note(f"DAG leaves (no downstream deps): {sorted(leaves)}")
