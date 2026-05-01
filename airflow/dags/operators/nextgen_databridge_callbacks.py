"""
NextGenDatabridge Airflow Callbacks
Handles on_failure, on_success, on_retry, and SLA miss callbacks.
Each callback writes to the audit table and fires alerts.
"""
from __future__ import annotations

import json
import logging
import os
import smtplib
import traceback
import urllib.request
import uuid
from datetime import datetime, timezone
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Any, Dict, Optional

import psycopg2

logger = logging.getLogger("nextgen_databridge.callbacks")


def _audit_db_conn():
    raw = os.getenv("NEXTGEN_DATABRIDGE_AUDIT_DB_URL", "")
    if not raw:
        # Startup script may not have propagated the env var to Celery workers;
        # fetch directly from Secrets Manager — the MWAA execution role allows this.
        try:
            import boto3 as _boto3, json as _json
            _sm = _boto3.client(
                "secretsmanager",
                region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            )
            _secret = _sm.get_secret_value(
                SecretId="nextgen-databridge/connections/audit_db"
            )
            raw = _json.loads(_secret["SecretString"]).get("url", "")
        except Exception as _e:
            raise RuntimeError(
                f"NEXTGEN_DATABRIDGE_AUDIT_DB_URL not set and Secrets Manager fallback failed: {_e}"
            )
    if not raw:
        raise RuntimeError(
            "NEXTGEN_DATABRIDGE_AUDIT_DB_URL is not set and secret 'url' field is empty"
        )
    url = raw.replace("postgresql+asyncpg://", "postgresql://")
    return psycopg2.connect(url)


def _write_alert(
    alert_type: str,
    severity: str,
    title: str,
    message: str,
    pipeline_id: Optional[str] = None,
    run_id: Optional[str] = None,
    task_id: Optional[str] = None,
    details: Optional[dict] = None,
):
    try:
        conn = _audit_db_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO alerts (id, alert_type, severity, title, message, pipeline_id, run_id, task_id, details, fired_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            str(uuid.uuid4()), alert_type, severity, title, message,
            pipeline_id, run_id, task_id, json.dumps(details or {}),
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to write alert: {e}")


def _upsert_pipeline_run(
    pipeline_id: str,
    run_id: str,
    status: str,
    trigger_type: str = "scheduled",
    triggered_by: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    duration_seconds: Optional[float] = None,
    total_tasks: int = 0,
    completed_tasks: int = 0,
    failed_tasks: int = 0,
    error_message: Optional[str] = None,
):
    try:
        conn = _audit_db_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_runs
                (id, run_id, pipeline_id, status, trigger_type, triggered_by,
                 start_time, end_time, duration_seconds,
                 total_tasks, completed_tasks, failed_tasks, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                status           = EXCLUDED.status,
                end_time         = COALESCE(EXCLUDED.end_time, pipeline_runs.end_time),
                duration_seconds = COALESCE(EXCLUDED.duration_seconds, pipeline_runs.duration_seconds),
                total_tasks      = GREATEST(EXCLUDED.total_tasks, pipeline_runs.total_tasks),
                completed_tasks  = EXCLUDED.completed_tasks,
                failed_tasks     = EXCLUDED.failed_tasks,
                error_message    = COALESCE(EXCLUDED.error_message, pipeline_runs.error_message)
        """, (
            str(uuid.uuid4()), run_id, pipeline_id, status, trigger_type, triggered_by,
            start_time, end_time, duration_seconds,
            total_tasks, completed_tasks, failed_tasks, error_message,
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to upsert pipeline run {run_id}: {e}")


def _ensure_run_started(dag_id: str, run_id: str, start_time: Optional[datetime] = None, trigger_type: str = "scheduled"):
    """Insert a 'running' pipeline_runs row if one doesn't exist yet. No-op on conflict."""
    try:
        conn = _audit_db_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO pipeline_runs
                (id, run_id, pipeline_id, status, trigger_type, start_time,
                 total_tasks, completed_tasks, failed_tasks, total_rows_processed)
            VALUES (%s, %s, %s, 'running', %s, %s, 0, 0, 0, 0)
            ON CONFLICT (run_id) DO NOTHING
        """, (str(uuid.uuid4()), run_id, dag_id, trigger_type, start_time or datetime.now(timezone.utc)))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to ensure pipeline run started {run_id}: {e}")


def _write_audit_log(event_type: str, pipeline_id: str, run_id: str, task_id: str, details: dict, severity: str):
    try:
        conn = _audit_db_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO audit_logs (id, event_type, pipeline_id, run_id, task_id, "user", details, severity, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            str(uuid.uuid4()), event_type, pipeline_id, run_id, task_id,
            "airflow", json.dumps(details), severity,
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Audit log write failed: {e}")


def _send_email_notification(recipient: str, subject: str, html_body: str):
    """Send email via configured SMTP"""
    try:
        smtp_host = os.getenv("SMTP_HOST", "mailhog")
        smtp_port = int(os.getenv("SMTP_PORT", "1025"))
        from_addr = os.getenv("SMTP_FROM", "nextgen-databridge-noreply@platform.internal")

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = from_addr
        msg["To"]      = recipient
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as s:
            s.sendmail(from_addr, [recipient], msg.as_string())
        logger.info(f"Email sent to {recipient}: {subject}")
    except Exception as e:
        logger.error(f"Email send failed: {e}")


def _send_slack_notification(webhook_url: str, text: str, color: str = "#ff0000"):
    """Post to Slack via incoming webhook"""
    try:
        payload = json.dumps({
            "attachments": [{
                "color": color,
                "text": text,
                "mrkdwn_in": ["text"],
            }]
        }).encode()
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        logger.error(f"Slack notification failed: {e}")


def _fire_pipeline_alerts(pipeline_config: dict, event: str, context: dict):
    """Read alert config from pipeline config and dispatch notifications"""
    alerting = pipeline_config.get("alerting", {})
    channels = alerting.get(event, [])

    pipeline_id = pipeline_config.get("pipeline_id", "unknown")
    run_id      = context.get("run_id", "")
    task_id     = context.get("task_instance_key_str", "")

    for channel in channels:
        if channel.startswith("email:"):
            recipient = channel.replace("email:", "")
            subject   = f"[NextGenDatabridge] {event.replace('on_', '').upper()}: {pipeline_id}"
            body      = f"""
<h2>NextGenDatabridge Pipeline {event.replace('on_', '').title()}</h2>
<p><b>Pipeline:</b> {pipeline_id}</p>
<p><b>Run ID:</b> {run_id}</p>
<p><b>Task:</b> {task_id}</p>
<p><b>Time:</b> {datetime.now(timezone.utc).isoformat()}</p>
<p><a href="{os.getenv('NEXTGEN_DATABRIDGE_UI_URL', 'http://localhost:3000')}/pipelines/{pipeline_id}/runs/{run_id}">
  View in NextGenDatabridge UI
</a></p>
            """
            _send_email_notification(recipient, subject, body)

        elif channel.startswith("slack:"):
            webhook = os.getenv("SLACK_WEBHOOK_URL", "")
            if webhook:
                color = "#ff0000" if event == "on_failure" else "#36a64f"
                _send_slack_notification(
                    webhook,
                    f"*NextGenDatabridge*: `{pipeline_id}` — {event.replace('on_', '')} | Run: `{run_id}` | Task: `{task_id}`",
                    color,
                )

        elif channel.startswith("pagerduty:"):
            # Would integrate with PagerDuty Events API v2
            pd_key = channel.replace("pagerduty:", "")
            logger.info(f"PagerDuty alert would fire for key {pd_key[:8]}...")


def _mark_task_run_failed(
    run_id: str, dag_id: str, task_id: str, attempt: int,
    err_msg: str, end_time: Optional[datetime] = None,
):
    """
    Ensure task_runs reflects a failure.  Called from on_failure_callback so
    that tasks manually marked failed from the Airflow UI — where the operator's
    except block never executes — are still recorded correctly.

    Uses a conditional UPDATE so pipeline_runs.failed_tasks is only incremented
    when the row was not already in 'failed' state, preventing double-counting
    when the operator's own except block already wrote the failure.
    """
    try:
        conn        = _audit_db_conn()
        cur         = conn.cursor()
        task_run_id = f"{run_id}_{task_id}_attempt{attempt}"
        _end        = end_time or datetime.now(timezone.utc)

        # Update only if the record exists and is not already failed.
        cur.execute("""
            UPDATE task_runs
            SET status        = 'failed',
                end_time      = COALESCE(end_time, %s),
                error_message = COALESCE(error_message, %s),
                updated_at    = NOW()
            WHERE task_run_id = %s
              AND status != 'failed'
        """, (_end, err_msg[:2000] if err_msg else None, task_run_id))

        newly_failed = cur.rowcount

        if newly_failed == 0:
            # Record may not exist yet (task was manually failed before it ever ran).
            cur.execute("""
                INSERT INTO task_runs
                    (id, task_run_id, run_id, pipeline_id, task_id, status,
                     end_time, error_message, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, 'failed', %s, %s, NOW(), NOW())
                ON CONFLICT (task_run_id) DO NOTHING
            """, (
                str(uuid.uuid4()), task_run_id, run_id, dag_id, task_id,
                _end, err_msg[:2000] if err_msg else None,
            ))
            newly_failed = cur.rowcount

        # Only bump the pipeline_runs counter when this callback is the first
        # to record the failure (avoids double-counting with operator writes).
        if newly_failed > 0:
            cur.execute("""
                UPDATE pipeline_runs
                SET failed_tasks = COALESCE(failed_tasks, 0) + 1
                WHERE run_id = %s
            """, (run_id,))

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to mark task_run as failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Airflow Callback Functions
# ─────────────────────────────────────────────────────────────────────────────

def on_failure_callback(context: dict):
    """Called by Airflow when any task fails"""
    ti          = context.get("task_instance")
    task_id     = ti.task_id if ti else "unknown"
    dag_id      = ti.dag_id if ti else "unknown"
    run_id      = context.get("run_id", "")
    exception   = context.get("exception", "Unknown error")
    err_msg     = str(exception)
    attempt     = ti.try_number if ti else 1

    logger.error(f"Task FAILED: {dag_id}/{task_id} — {err_msg[:200]}")

    _ensure_run_started(dag_id, run_id, start_time=ti.start_date if ti else None)

    # Ensure task_runs reflects the failure regardless of whether the operator's
    # except block ran (handles manual failures from the Airflow UI).
    _mark_task_run_failed(
        run_id=run_id, dag_id=dag_id, task_id=task_id, attempt=attempt,
        err_msg=err_msg, end_time=ti.end_date if ti else None,
    )

    _write_audit_log(
        "TASK_FAILED", dag_id, run_id, task_id,
        {"error": err_msg[:2000], "attempt": attempt},
        severity="error",
    )

    _write_alert(
        alert_type="TASK_FAILURE",
        severity="error",
        title=f"Task Failed: {task_id}",
        message=f"Pipeline `{dag_id}` task `{task_id}` failed: {err_msg[:500]}",
        pipeline_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        details={"error": err_msg[:2000], "attempt": attempt},
    )

    # Fire pipeline-level alerts
    try:
        dag = context.get("dag")
        if dag and hasattr(dag, "params") and "pipeline_config" in dag.params:
            _fire_pipeline_alerts(dag.params["pipeline_config"], "on_failure", context)
    except Exception as e:
        logger.debug(f"Could not fire pipeline alerts: {e}")


def on_success_callback(context: dict):
    """Called by Airflow when a task succeeds"""
    ti      = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown"
    dag_id  = ti.dag_id if ti else "unknown"
    run_id  = context.get("run_id", "")
    duration= ti.duration if ti else 0

    logger.info(f"Task SUCCESS: {dag_id}/{task_id} in {duration:.1f}s")
    _ensure_run_started(dag_id, run_id, start_time=ti.start_date if ti else None)
    _write_audit_log(
        "TASK_COMPLETED", dag_id, run_id, task_id,
        {"duration_seconds": duration},
        severity="info",
    )


def on_retry_callback(context: dict):
    """Called by Airflow when a task retries"""
    ti      = context.get("task_instance")
    task_id = ti.task_id if ti else "unknown"
    dag_id  = ti.dag_id if ti else "unknown"
    run_id  = context.get("run_id", "")
    attempt = ti.try_number if ti else 1

    logger.warning(f"Task RETRY: {dag_id}/{task_id} attempt {attempt}")
    _write_audit_log(
        "TASK_RETRIED", dag_id, run_id, task_id,
        {"attempt": attempt},
        severity="warning",
    )


def dag_run_success_callback(context: dict):
    """Called by Airflow when a full DAG run completes successfully"""
    dag     = context.get("dag")
    dag_run = context.get("dag_run")
    if not dag or not dag_run:
        return

    dag_id = dag.dag_id
    run_id = dag_run.run_id

    try:
        tis       = dag_run.get_task_instances()
        total     = len(tis)
        completed = sum(1 for ti in tis if getattr(ti, "state", "") == "success")
        failed    = sum(1 for ti in tis if getattr(ti, "state", "") in ("failed", "upstream_failed"))
    except Exception:
        total = completed = failed = 0

    start = dag_run.start_date
    end   = dag_run.end_date or datetime.now(timezone.utc)
    dur   = (end - start).total_seconds() if start and end else None

    run_type = getattr(dag_run, "run_type", None)
    trigger_type = "manual" if run_type and "manual" in str(run_type).lower() else "scheduled"

    _upsert_pipeline_run(
        pipeline_id=dag_id,
        run_id=run_id,
        status="success",
        trigger_type=trigger_type,
        start_time=start,
        end_time=end,
        duration_seconds=dur,
        total_tasks=total,
        completed_tasks=completed,
        failed_tasks=failed,
    )
    _write_audit_log("RUN_COMPLETED", dag_id, run_id, "_dag",
                     {"duration_seconds": dur, "total_tasks": total}, severity="info")


def dag_run_failure_callback(context: dict):
    """Called by Airflow when a DAG run fails"""
    dag     = context.get("dag")
    dag_run = context.get("dag_run")
    if not dag or not dag_run:
        return

    dag_id  = dag.dag_id
    run_id  = dag_run.run_id
    reason  = str(context.get("reason", ""))

    try:
        tis       = dag_run.get_task_instances()
        total     = len(tis)
        failed    = sum(1 for ti in tis if getattr(ti, "state", "") in ("failed", "upstream_failed"))
        completed = sum(1 for ti in tis if getattr(ti, "state", "") == "success")
    except Exception:
        total = completed = failed = 0

    start = dag_run.start_date
    end   = dag_run.end_date or datetime.now(timezone.utc)
    dur   = (end - start).total_seconds() if start and end else None

    run_type = getattr(dag_run, "run_type", None)
    trigger_type = "manual" if run_type and "manual" in str(run_type).lower() else "scheduled"

    _upsert_pipeline_run(
        pipeline_id=dag_id,
        run_id=run_id,
        status="failed",
        trigger_type=trigger_type,
        start_time=start,
        end_time=end,
        duration_seconds=dur,
        total_tasks=total,
        completed_tasks=completed,
        failed_tasks=failed,
        error_message=reason[:2000] if reason else None,
    )
    _write_audit_log("RUN_FAILED", dag_id, run_id, "_dag",
                     {"duration_seconds": dur, "reason": reason[:500]}, severity="error")


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Called by Airflow when a DAG misses its SLA"""
    dag_id = dag.dag_id
    logger.warning(f"SLA MISS: {dag_id}")

    _write_alert(
        alert_type="SLA_BREACH",
        severity="warning",
        title=f"SLA Breach: {dag_id}",
        message=f"Pipeline `{dag_id}` has breached its SLA. Tasks behind: {[t.task_id for t in blocking_task_list]}",
        pipeline_id=dag_id,
        details={
            "sla_tasks": [str(s) for s in slas],
            "blocking_tasks": [t.task_id for t in blocking_task_list],
        },
    )
