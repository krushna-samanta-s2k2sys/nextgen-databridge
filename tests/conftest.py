"""
Root conftest — stubs airflow and optional heavy dependencies before any
test module is imported.  Must live here (tests/) so pytest loads it first.
"""
import sys
from unittest.mock import MagicMock


# ── Minimal BaseOperator stub ─────────────────────────────────────────────────
class _BaseOperator:
    template_fields: list = []
    template_ext: list = []
    ui_color: str = "#fff"
    retries: int = 3

    def __init__(
        self,
        task_id: str = "stub_task",
        dag=None,
        retries: int = 3,
        retry_delay=None,
        execution_timeout=None,
        on_failure_callback=None,
        on_retry_callback=None,
        on_success_callback=None,
        **kwargs,
    ):
        self.task_id = task_id
        self.dag = dag
        self.retries = retries


_am = MagicMock()

# airflow.models — needs a real BaseOperator class so operator inheritance works
_am_models = MagicMock()
_am_models.BaseOperator = _BaseOperator
_am_models.Variable = MagicMock()

# airflow.utils.context — just needs to be importable
_am_context = MagicMock()

_am.models.BaseOperator = _BaseOperator
_am.operators.python.BranchPythonOperator = MagicMock(return_value=MagicMock())
_am.operators.empty.EmptyOperator = MagicMock(return_value=MagicMock())
_am.operators.trigger_dagrun.TriggerDagRunOperator = MagicMock(return_value=MagicMock())
_am.utils.task_group.TaskGroup = MagicMock()
_am.utils.dates.days_ago = lambda n: None
_am.exceptions.AirflowException = Exception

_AIRFLOW_MODULES = [
    "airflow",
    "airflow.models.dag",
    "airflow.models.variable",
    "airflow.utils",
    "airflow.utils.context",
    "airflow.utils.dates",
    "airflow.utils.task_group",
    "airflow.hooks",
    "airflow.hooks.base",
    "airflow.operators",
    "airflow.operators.python",
    "airflow.operators.empty",
    "airflow.operators.trigger_dagrun",
    "airflow.exceptions",
]
for _mod in _AIRFLOW_MODULES:
    sys.modules.setdefault(_mod, _am)

# Register airflow.models with a dedicated stub so BaseOperator is a real class
sys.modules.setdefault("airflow.models", _am_models)

# ── Stub optional heavy dependencies ─────────────────────────────────────────
_OPTIONAL = [
    "kubernetes", "kubernetes.client", "kubernetes.client.models",
    "kubernetes.config",
    "google", "google.cloud", "google.cloud.pubsub_v1",
    "kafka", "kafka.errors",
    "oracledb", "cx_Oracle",
    "pymssql",
    "sqlalchemy", "sqlalchemy.orm", "sqlalchemy.engine",
    "airflow.sensors", "airflow.sensors.external_task",
    "opentelemetry", "opentelemetry.trace", "opentelemetry.metrics",
    "opentelemetry.sdk", "opentelemetry.sdk.trace", "opentelemetry.sdk.metrics",
    "prometheus_client",
]
for _mod in _OPTIONAL:
    if _mod not in sys.modules:
        sys.modules[_mod] = MagicMock()
