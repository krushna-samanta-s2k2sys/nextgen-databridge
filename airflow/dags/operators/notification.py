"""
Notification Operator
Sends notifications to Slack channels or email recipients.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from airflow.utils.context import Context

from operators.base import NextGenDatabridgeBaseOperator

logger = logging.getLogger("nextgen_databridge.operators.notification")


class NotificationOperator(NextGenDatabridgeBaseOperator):

    def execute(self, context: Context) -> str:
        run_id  = context["run_id"]
        task_id = self.task_config["task_id"]
        channels = self.task_config.get("channels", [])
        message  = self.task_config.get("message",
                       f"Pipeline {self.pipeline_id} - Task {task_id} completed")
        subject  = self.task_config.get("subject",
                       f"NextGenDatabridge Notification: {self.pipeline_id}")

        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts)

        sent = []
        for channel in channels:
            try:
                if channel.startswith("slack:"):
                    self._send_slack(channel.replace("slack:", ""), message, context)
                    sent.append(channel)
                elif channel.startswith("email:"):
                    self._send_email(channel.replace("email:", ""), subject, message)
                    sent.append(channel)
            except Exception as e:
                logger.warning(f"Notification to {channel} failed: {e}")

        duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
        self._write_task_run(run_id, "success", start_time=start_ts,
                             end_time=datetime.now(timezone.utc),
                             duration_seconds=duration,
                             metrics={"notified_channels": sent})
        return f"Notified: {sent}"

    def _send_slack(self, channel: str, message: str, context: dict):
        import urllib.request
        webhook_url = os.getenv("SLACK_WEBHOOK_URL")
        if not webhook_url:
            logger.warning("SLACK_WEBHOOK_URL not set, skipping Slack notification")
            return
        payload = json.dumps({"channel": channel, "text": message}).encode()
        req = urllib.request.Request(
            webhook_url, data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)

    def _send_email(self, recipient: str, subject: str, body: str):
        import smtplib
        from email.mime.text import MIMEText
        smtp_host = os.getenv("SMTP_HOST", "localhost")
        smtp_port = int(os.getenv("SMTP_PORT", "25"))
        from_addr = os.getenv("SMTP_FROM", "nextgen-databridge@platform.internal")
        msg           = MIMEText(body)
        msg["Subject"] = subject
        msg["From"]    = from_addr
        msg["To"]      = recipient
        with smtplib.SMTP(smtp_host, smtp_port) as s:
            s.sendmail(from_addr, [recipient], msg.as_string())
