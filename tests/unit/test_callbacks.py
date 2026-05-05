"""Unit tests for nextgen_databridge_callbacks."""
import json
import os
import sys
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../airflow/dags"))


# ── Helpers ────────────────────────────────────────────────────────────────────

def _mock_db_conn():
    conn = MagicMock()
    cur  = MagicMock()
    conn.cursor.return_value = cur
    cur.rowcount = 1
    cur.fetchone.return_value = (3, 1, 1)  # total, completed, failed
    return conn, cur


def _make_ti(dag_id="my_dag", task_id="my_task", run_id="run_1"):
    ti = MagicMock()
    ti.dag_id     = dag_id
    ti.task_id    = task_id
    ti.run_id     = run_id
    ti.try_number = 1
    ti.duration   = 5.0
    ti.start_date = datetime.now(timezone.utc)
    ti.end_date   = datetime.now(timezone.utc)
    return ti


def _ctx(ti=None, dag=None, dag_run=None, run_id="run_1"):
    return {
        "task_instance": ti or _make_ti(),
        "run_id": run_id,
        "exception": RuntimeError("boom"),
        "dag": dag,
        "dag_run": dag_run,
        "task_instance_key_str": "my_dag.my_task.run_1",
    }


# ── _audit_db_conn ─────────────────────────────────────────────────────────────

def test_audit_db_conn_with_env_var(monkeypatch):
    from operators.nextgen_databridge_callbacks import _audit_db_conn
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")
    mock_conn = MagicMock()
    with patch("psycopg2.connect", return_value=mock_conn) as mock_pg:
        result = _audit_db_conn()
    mock_pg.assert_called_once_with("postgresql://u:p@h/db")
    assert result is mock_conn


def test_audit_db_conn_asyncpg_url_normalized(monkeypatch):
    from operators.nextgen_databridge_callbacks import _audit_db_conn
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql+asyncpg://u:p@h/db")
    mock_conn = MagicMock()
    with patch("psycopg2.connect", return_value=mock_conn) as mock_pg:
        _audit_db_conn()
    called_url = mock_pg.call_args[0][0]
    assert "postgresql://" in called_url
    assert "+asyncpg" not in called_url


def test_audit_db_conn_no_env_falls_back_to_sm(monkeypatch):
    from operators.nextgen_databridge_callbacks import _audit_db_conn
    monkeypatch.delenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", raising=False)

    mock_sm = MagicMock()
    mock_sm.get_secret_value.return_value = {
        "SecretString": json.dumps({"url": "postgresql://u:p@h/db"})
    }
    mock_conn = MagicMock()
    with patch("boto3.client", return_value=mock_sm):
        with patch("psycopg2.connect", return_value=mock_conn):
            result = _audit_db_conn()
    assert result is mock_conn


def test_audit_db_conn_no_env_sm_fails_raises(monkeypatch):
    from operators.nextgen_databridge_callbacks import _audit_db_conn
    monkeypatch.delenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", raising=False)

    with patch("boto3.client", side_effect=Exception("SM unavailable")):
        with pytest.raises(RuntimeError, match="Secrets Manager fallback failed"):
            _audit_db_conn()


def test_audit_db_conn_sm_returns_empty_url_raises(monkeypatch):
    from operators.nextgen_databridge_callbacks import _audit_db_conn
    monkeypatch.delenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", raising=False)

    mock_sm = MagicMock()
    mock_sm.get_secret_value.return_value = {
        "SecretString": json.dumps({"url": ""})
    }
    with patch("boto3.client", return_value=mock_sm):
        with pytest.raises(RuntimeError, match="not set and secret"):
            _audit_db_conn()


# ── _write_alert ───────────────────────────────────────────────────────────────

def test_write_alert_success(monkeypatch):
    from operators.nextgen_databridge_callbacks import _write_alert
    conn, cur = _mock_db_conn()
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    with patch("psycopg2.connect", return_value=conn):
        _write_alert("TASK_FAILURE", "error", "Task Failed", "Details",
                     pipeline_id="p1", run_id="r1", task_id="t1")

    cur.execute.assert_called_once()
    conn.commit.assert_called_once()


def test_write_alert_db_error_swallowed(monkeypatch):
    from operators.nextgen_databridge_callbacks import _write_alert
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    with patch("psycopg2.connect", side_effect=Exception("DB down")):
        _write_alert("x", "error", "title", "msg")  # should not raise


# ── _upsert_pipeline_run ───────────────────────────────────────────────────────

def test_upsert_pipeline_run_success(monkeypatch):
    from operators.nextgen_databridge_callbacks import _upsert_pipeline_run
    conn, cur = _mock_db_conn()
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    with patch("psycopg2.connect", return_value=conn):
        _upsert_pipeline_run("my_dag", "run_1", "success")

    cur.execute.assert_called_once()
    conn.commit.assert_called_once()


def test_upsert_pipeline_run_error_swallowed(monkeypatch):
    from operators.nextgen_databridge_callbacks import _upsert_pipeline_run
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    with patch("psycopg2.connect", side_effect=Exception("DB down")):
        _upsert_pipeline_run("dag", "run", "failed")  # should not raise


# ── _ensure_run_started ────────────────────────────────────────────────────────

def test_ensure_run_started_success(monkeypatch):
    from operators.nextgen_databridge_callbacks import _ensure_run_started
    conn, cur = _mock_db_conn()
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    with patch("psycopg2.connect", return_value=conn):
        _ensure_run_started("my_dag", "run_1")

    cur.execute.assert_called_once()
    conn.commit.assert_called_once()


def test_ensure_run_started_error_swallowed(monkeypatch):
    from operators.nextgen_databridge_callbacks import _ensure_run_started
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    with patch("psycopg2.connect", side_effect=Exception("DB down")):
        _ensure_run_started("dag", "run")  # should not raise


# ── _write_audit_log ───────────────────────────────────────────────────────────

def test_write_audit_log_success(monkeypatch):
    from operators.nextgen_databridge_callbacks import _write_audit_log
    conn, cur = _mock_db_conn()
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    with patch("psycopg2.connect", return_value=conn):
        _write_audit_log("TASK_FAILED", "dag1", "run1", "task1", {"k": "v"}, "error")

    cur.execute.assert_called_once()
    conn.commit.assert_called_once()


def test_write_audit_log_error_swallowed(monkeypatch):
    from operators.nextgen_databridge_callbacks import _write_audit_log
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    with patch("psycopg2.connect", side_effect=Exception("DB down")):
        _write_audit_log("EVT", "dag", "run", "task", {}, "info")  # should not raise


# ── _send_email_notification ───────────────────────────────────────────────────

def test_send_email_notification_success(monkeypatch):
    from operators.nextgen_databridge_callbacks import _send_email_notification

    mock_smtp = MagicMock()
    mock_smtp.__enter__ = MagicMock(return_value=mock_smtp)
    mock_smtp.__exit__ = MagicMock(return_value=False)

    with patch("smtplib.SMTP", return_value=mock_smtp):
        _send_email_notification("user@example.com", "Subject", "<p>Body</p>")

    mock_smtp.sendmail.assert_called_once()


def test_send_email_notification_error_swallowed(monkeypatch):
    from operators.nextgen_databridge_callbacks import _send_email_notification

    with patch("smtplib.SMTP", side_effect=Exception("SMTP down")):
        _send_email_notification("user@example.com", "Subject", "Body")  # should not raise


# ── _send_slack_notification ───────────────────────────────────────────────────

def test_send_slack_notification_success():
    from operators.nextgen_databridge_callbacks import _send_slack_notification

    with patch("urllib.request.urlopen") as mock_open:
        with patch("urllib.request.Request") as mock_req:
            _send_slack_notification("https://hooks.slack.com/fake", "Hello!")

    mock_open.assert_called_once()


def test_send_slack_notification_error_swallowed():
    from operators.nextgen_databridge_callbacks import _send_slack_notification

    with patch("urllib.request.urlopen", side_effect=Exception("network error")):
        with patch("urllib.request.Request"):
            _send_slack_notification("https://bad.url/", "msg")  # should not raise


# ── _fire_pipeline_alerts ──────────────────────────────────────────────────────

def test_fire_pipeline_alerts_email_channel(monkeypatch):
    from operators.nextgen_databridge_callbacks import _fire_pipeline_alerts

    pipeline_config = {
        "pipeline_id": "p1",
        "alerting": {"on_failure": ["email:ops@company.com"]},
    }
    with patch("operators.nextgen_databridge_callbacks._send_email_notification") as mock_email:
        _fire_pipeline_alerts(pipeline_config, "on_failure", {"run_id": "r1"})

    mock_email.assert_called_once()
    args = mock_email.call_args[0]
    assert "ops@company.com" in args[0]


def test_fire_pipeline_alerts_slack_channel_with_webhook(monkeypatch):
    from operators.nextgen_databridge_callbacks import _fire_pipeline_alerts
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/fake")

    pipeline_config = {
        "pipeline_id": "p1",
        "alerting": {"on_failure": ["slack:general"]},
    }
    with patch("operators.nextgen_databridge_callbacks._send_slack_notification") as mock_slack:
        _fire_pipeline_alerts(pipeline_config, "on_failure", {"run_id": "r1"})

    mock_slack.assert_called_once()


def test_fire_pipeline_alerts_slack_no_webhook(monkeypatch):
    from operators.nextgen_databridge_callbacks import _fire_pipeline_alerts
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

    pipeline_config = {
        "pipeline_id": "p1",
        "alerting": {"on_failure": ["slack:general"]},
    }
    with patch("operators.nextgen_databridge_callbacks._send_slack_notification") as mock_slack:
        _fire_pipeline_alerts(pipeline_config, "on_failure", {})

    mock_slack.assert_not_called()


def test_fire_pipeline_alerts_pagerduty(monkeypatch):
    from operators.nextgen_databridge_callbacks import _fire_pipeline_alerts

    pipeline_config = {
        "pipeline_id": "p1",
        "alerting": {"on_failure": ["pagerduty:integrationkey12345"]},
    }
    # should not raise; PagerDuty just logs
    _fire_pipeline_alerts(pipeline_config, "on_failure", {})


def test_fire_pipeline_alerts_no_channels():
    from operators.nextgen_databridge_callbacks import _fire_pipeline_alerts
    _fire_pipeline_alerts({"pipeline_id": "p1", "alerting": {}}, "on_failure", {})  # no-op


def test_fire_pipeline_alerts_success_event_slack_color(monkeypatch):
    from operators.nextgen_databridge_callbacks import _fire_pipeline_alerts
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.com/fake")

    pipeline_config = {
        "pipeline_id": "p1",
        "alerting": {"on_success": ["slack:general"]},
    }
    with patch("operators.nextgen_databridge_callbacks._send_slack_notification") as mock_slack:
        _fire_pipeline_alerts(pipeline_config, "on_success", {})

    # success event uses green color
    color_arg = mock_slack.call_args[0][2]
    assert color_arg == "#36a64f"


# ── _mark_task_run_failed ──────────────────────────────────────────────────────

def test_mark_task_run_failed_newly_failed(monkeypatch):
    from operators.nextgen_databridge_callbacks import _mark_task_run_failed
    conn, cur = _mock_db_conn()
    cur.rowcount = 1  # UPDATE affected a row (newly failed)
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    with patch("psycopg2.connect", return_value=conn):
        _mark_task_run_failed("run_1", "dag_1", "task_1", 1, "err msg")

    # Should have executed: UPDATE + SELECT pipeline_runs + possibly UPDATE pipeline_runs
    assert cur.execute.call_count >= 2
    conn.commit.assert_called_once()


def test_mark_task_run_failed_already_failed_inserts(monkeypatch):
    from operators.nextgen_databridge_callbacks import _mark_task_run_failed
    conn, cur = _mock_db_conn()
    cur.rowcount = 0  # UPDATE didn't affect row (already failed or doesn't exist)
    cur.fetchone.return_value = (3, 1, 3)  # all tasks done
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    with patch("psycopg2.connect", return_value=conn):
        _mark_task_run_failed("run_1", "dag_1", "task_1", 1, "err")

    # Should attempt INSERT as fallback
    assert cur.execute.call_count >= 2


def test_mark_task_run_failed_error_swallowed(monkeypatch):
    from operators.nextgen_databridge_callbacks import _mark_task_run_failed
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    with patch("psycopg2.connect", side_effect=Exception("DB down")):
        _mark_task_run_failed("run", "dag", "task", 1, "err")  # should not raise


# ── on_failure_callback ────────────────────────────────────────────────────────

def test_on_failure_callback_with_ti(monkeypatch):
    from operators.nextgen_databridge_callbacks import on_failure_callback
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    ctx = _ctx()
    with patch("operators.nextgen_databridge_callbacks._ensure_run_started"):
        with patch("operators.nextgen_databridge_callbacks._mark_task_run_failed"):
            with patch("operators.nextgen_databridge_callbacks._write_audit_log"):
                with patch("operators.nextgen_databridge_callbacks._write_alert"):
                    on_failure_callback(ctx)  # should not raise


def test_on_failure_callback_without_ti(monkeypatch):
    from operators.nextgen_databridge_callbacks import on_failure_callback
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    ctx = _ctx(ti=None)
    ctx["task_instance"] = None
    with patch("operators.nextgen_databridge_callbacks._ensure_run_started"):
        with patch("operators.nextgen_databridge_callbacks._mark_task_run_failed"):
            with patch("operators.nextgen_databridge_callbacks._write_audit_log"):
                with patch("operators.nextgen_databridge_callbacks._write_alert"):
                    on_failure_callback(ctx)  # should not raise, uses "unknown" defaults


def test_on_failure_callback_fires_pipeline_alerts(monkeypatch):
    from operators.nextgen_databridge_callbacks import on_failure_callback
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    mock_dag = MagicMock()
    mock_dag.params = {"pipeline_config": {"pipeline_id": "p1", "alerting": {}}}
    ctx = _ctx(dag=mock_dag)

    with patch("operators.nextgen_databridge_callbacks._ensure_run_started"):
        with patch("operators.nextgen_databridge_callbacks._mark_task_run_failed"):
            with patch("operators.nextgen_databridge_callbacks._write_audit_log"):
                with patch("operators.nextgen_databridge_callbacks._write_alert"):
                    with patch("operators.nextgen_databridge_callbacks._fire_pipeline_alerts") as mock_fire:
                        on_failure_callback(ctx)

    mock_fire.assert_called_once()


# ── on_success_callback ────────────────────────────────────────────────────────

def test_on_success_callback_with_ti(monkeypatch):
    from operators.nextgen_databridge_callbacks import on_success_callback
    monkeypatch.setenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "postgresql://u:p@h/db")

    ctx = _ctx()
    with patch("operators.nextgen_databridge_callbacks._ensure_run_started"):
        with patch("operators.nextgen_databridge_callbacks._write_audit_log"):
            on_success_callback(ctx)  # should not raise


def test_on_success_callback_without_ti(monkeypatch):
    from operators.nextgen_databridge_callbacks import on_success_callback

    ctx = _ctx()
    ctx["task_instance"] = None
    with patch("operators.nextgen_databridge_callbacks._ensure_run_started"):
        with patch("operators.nextgen_databridge_callbacks._write_audit_log"):
            on_success_callback(ctx)  # should not raise


# ── on_retry_callback ──────────────────────────────────────────────────────────

def test_on_retry_callback_with_ti(monkeypatch):
    from operators.nextgen_databridge_callbacks import on_retry_callback

    ctx = _ctx()
    with patch("operators.nextgen_databridge_callbacks._write_audit_log") as mock_log:
        on_retry_callback(ctx)

    mock_log.assert_called_once()
    assert mock_log.call_args[0][0] == "TASK_RETRIED"


def test_on_retry_callback_without_ti(monkeypatch):
    from operators.nextgen_databridge_callbacks import on_retry_callback

    ctx = _ctx()
    ctx["task_instance"] = None
    with patch("operators.nextgen_databridge_callbacks._write_audit_log"):
        on_retry_callback(ctx)  # should not raise


# ── dag_run_success_callback ───────────────────────────────────────────────────

def test_dag_run_success_callback_with_dag_run(monkeypatch):
    from operators.nextgen_databridge_callbacks import dag_run_success_callback

    mock_dag = MagicMock()
    mock_dag.dag_id = "my_dag"

    mock_dag_run = MagicMock()
    mock_dag_run.dag_id  = "my_dag"
    mock_dag_run.run_id  = "run_1"
    mock_dag_run.run_type = "scheduled"
    mock_dag_run.start_date = datetime.now(timezone.utc)
    mock_dag_run.end_date   = datetime.now(timezone.utc)

    ti1 = MagicMock(); ti1.state = "success"
    ti2 = MagicMock(); ti2.state = "success"
    mock_dag_run.get_task_instances.return_value = [ti1, ti2]

    ctx = {"dag": mock_dag, "dag_run": mock_dag_run, "run_id": "run_1"}

    with patch("operators.nextgen_databridge_callbacks._upsert_pipeline_run") as mock_upsert:
        with patch("operators.nextgen_databridge_callbacks._write_audit_log"):
            dag_run_success_callback(ctx)

    mock_upsert.assert_called_once()
    call_kwargs = mock_upsert.call_args[1]
    assert call_kwargs["status"] == "success"
    assert call_kwargs["completed_tasks"] == 2


def test_dag_run_success_callback_missing_ids(monkeypatch):
    from operators.nextgen_databridge_callbacks import dag_run_success_callback

    ctx = {"dag": None, "dag_run": None, "dag_id": "", "run_id": ""}
    # Should return early without calling helpers
    with patch("operators.nextgen_databridge_callbacks._upsert_pipeline_run") as mock_up:
        dag_run_success_callback(ctx)
    mock_up.assert_not_called()


def test_dag_run_success_callback_manual_trigger(monkeypatch):
    from operators.nextgen_databridge_callbacks import dag_run_success_callback

    mock_dag_run = MagicMock()
    mock_dag_run.dag_id   = "my_dag"
    mock_dag_run.run_id   = "run_1"
    mock_dag_run.run_type = "manual"
    mock_dag_run.start_date = None
    mock_dag_run.end_date   = None
    mock_dag_run.get_task_instances.return_value = []

    ctx = {"dag": None, "dag_run": mock_dag_run, "run_id": "run_1"}

    with patch("operators.nextgen_databridge_callbacks._upsert_pipeline_run") as mock_up:
        with patch("operators.nextgen_databridge_callbacks._write_audit_log"):
            dag_run_success_callback(ctx)

    assert mock_up.call_args[1]["trigger_type"] == "manual"


# ── dag_run_failure_callback ───────────────────────────────────────────────────

def test_dag_run_failure_callback_with_dag_run(monkeypatch):
    from operators.nextgen_databridge_callbacks import dag_run_failure_callback

    mock_dag_run = MagicMock()
    mock_dag_run.dag_id   = "my_dag"
    mock_dag_run.run_id   = "run_1"
    mock_dag_run.run_type = "scheduled"
    mock_dag_run.start_date = datetime.now(timezone.utc)
    mock_dag_run.end_date   = None

    ti1 = MagicMock(); ti1.state = "failed"
    mock_dag_run.get_task_instances.return_value = [ti1]

    ctx = {"dag": None, "dag_run": mock_dag_run, "run_id": "run_1", "reason": "upstream failed"}

    with patch("operators.nextgen_databridge_callbacks._upsert_pipeline_run") as mock_up:
        with patch("operators.nextgen_databridge_callbacks._write_audit_log"):
            dag_run_failure_callback(ctx)

    mock_up.assert_called_once()
    assert mock_up.call_args[1]["status"] == "failed"


def test_dag_run_failure_callback_missing_ids(monkeypatch):
    from operators.nextgen_databridge_callbacks import dag_run_failure_callback

    ctx = {"dag": None, "dag_run": None, "dag_id": "", "run_id": ""}
    with patch("operators.nextgen_databridge_callbacks._upsert_pipeline_run") as mock_up:
        dag_run_failure_callback(ctx)
    mock_up.assert_not_called()


def test_dag_run_failure_callback_get_task_instances_error(monkeypatch):
    from operators.nextgen_databridge_callbacks import dag_run_failure_callback

    mock_dag_run = MagicMock()
    mock_dag_run.dag_id   = "my_dag"
    mock_dag_run.run_id   = "run_1"
    mock_dag_run.run_type = "scheduled"
    mock_dag_run.start_date = None
    mock_dag_run.end_date   = None
    mock_dag_run.get_task_instances.side_effect = Exception("DB err")

    ctx = {"dag": None, "dag_run": mock_dag_run, "run_id": "run_1", "reason": ""}

    with patch("operators.nextgen_databridge_callbacks._upsert_pipeline_run"):
        with patch("operators.nextgen_databridge_callbacks._write_audit_log"):
            dag_run_failure_callback(ctx)  # exception swallowed in try/except


# ── sla_miss_callback ──────────────────────────────────────────────────────────

def test_sla_miss_callback(monkeypatch):
    from operators.nextgen_databridge_callbacks import sla_miss_callback

    mock_dag = MagicMock()
    mock_dag.dag_id = "my_dag"

    t1 = MagicMock(); t1.task_id = "t1"
    t2 = MagicMock(); t2.task_id = "t2"

    with patch("operators.nextgen_databridge_callbacks._write_alert") as mock_alert:
        sla_miss_callback(mock_dag, [t1], [t2], [], [])

    mock_alert.assert_called_once()
    kwargs = mock_alert.call_args[1]
    assert kwargs["alert_type"] == "SLA_BREACH"
    assert kwargs["pipeline_id"] == "my_dag"
