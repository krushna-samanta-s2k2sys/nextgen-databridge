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
        raise RuntimeError("NEXTGEN_DATABRIDGE_AUDIT_DB_URL is not set")
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

    logger.error(f"Task FAILED: {dag_id}/{task_id} — {err_msg[:200]}")

    _write_audit_log(
        "TASK_FAILED", dag_id, run_id, task_id,
        {"error": err_msg[:2000], "attempt": ti.try_number if ti else 1},
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
        details={"error": err_msg[:2000], "attempt": ti.try_number if ti else 1},
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
