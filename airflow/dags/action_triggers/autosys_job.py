"""
Autosys Job Action Trigger
Triggers and optionally polls a CA Workload Automation (Autosys) job via REST API.
"""
from __future__ import annotations

import json
import logging
import time
import traceback
from datetime import datetime, timezone

from airflow.utils.context import Context

from operators.base import NextGenDatabridgeBaseOperator

logger = logging.getLogger("nextgen_databridge.action_triggers.autosys_job")


class AutosysJobOperator(NextGenDatabridgeBaseOperator):
    """
    task_config keys:
      autosys_url            (str, required) — base URL, e.g. https://autosys.corp:9443
      job_name               (str, required) — job name as known to Autosys
      auth                   (dict)          — auth block (see below)
      job_params             (dict)          — optional key/value overrides sent with trigger
      wait_for_completion    (bool)          — poll until terminal status; default false
      poll_interval_seconds  (int)           — seconds between status polls; default 30
      timeout_seconds        (int)           — max wait time when polling; default 3600

    auth block variants:
      {"type": "autosys_token", "username": "...", "password": "secret:..."}
        — authenticates via POST /v1/security/tokens and uses the returned bearer token
      {"type": "bearer", "token": "secret:..."}
      {"type": "basic",  "username": "...", "password": "secret:..."}

    Terminal statuses:
      SU → success
      FA, TE, OH, OI, IN → failure
    """

    _TERMINAL_SUCCESS = {"SU"}
    _TERMINAL_FAILURE = {"FA", "TE", "OH", "OI", "IN"}

    def execute(self, context: Context):
        cfg      = self.task_config
        run_id   = context["run_id"]
        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts)

        try:
            base_url   = cfg["autosys_url"].rstrip("/")
            job_name   = cfg["job_name"]
            auth       = cfg.get("auth") or {}
            job_params = cfg.get("job_params") or {}
            wait       = cfg.get("wait_for_completion", False)
            poll_secs  = int(cfg.get("poll_interval_seconds", 30))
            timeout    = int(cfg.get("timeout_seconds", 3600))

            headers    = self._make_headers(base_url, auth)
            job_run_id = self._trigger_job(base_url, headers, job_name, job_params)
            logger.info(f"Autosys job '{job_name}' triggered — run_id={job_run_id}")

            context["ti"].xcom_push(key="job_run_id", value=job_run_id)
            job_status = "ST"

            if wait:
                deadline = time.time() + timeout
                while time.time() < deadline:
                    job_status = self._poll_status(base_url, headers, job_run_id)
                    logger.info(f"Autosys '{job_name}' status: {job_status}")
                    if job_status in self._TERMINAL_SUCCESS:
                        break
                    if job_status in self._TERMINAL_FAILURE:
                        raise Exception(
                            f"Autosys job '{job_name}' ended with status '{job_status}'"
                        )
                    time.sleep(poll_secs)
                else:
                    raise Exception(
                        f"Autosys job '{job_name}' did not reach terminal status within {timeout}s"
                    )

            context["ti"].xcom_push(key="job_status", value=job_status)

            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(run_id, "success", start_time=start_ts,
                                 end_time=datetime.now(timezone.utc),
                                 duration_seconds=duration,
                                 metrics={"job_name": job_name, "job_run_id": job_run_id,
                                          "job_status": job_status})
            return {"job_run_id": job_run_id, "job_status": job_status}

        except Exception as exc:
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(run_id, "failed", start_time=start_ts,
                                 end_time=datetime.now(timezone.utc),
                                 duration_seconds=duration,
                                 error_message=str(exc),
                                 error_traceback=traceback.format_exc())
            raise

    # ── Auth / HTTP helpers ────────────────────────────────────────────────────

    def _make_headers(self, base_url: str, auth: dict) -> dict:
        import base64
        auth_type = (auth or {}).get("type", "none")
        base = {"Content-Type": "application/json"}

        if auth_type == "autosys_token":
            token = self._get_autosys_token(base_url, auth)
            return {**base, "Authorization": f"Bearer {token}"}
        if auth_type == "bearer":
            return {**base, "Authorization": f"Bearer {self._resolve_secret(auth['token'])}"}
        if auth_type == "basic":
            user  = self._resolve_secret(auth["username"])
            pwd   = self._resolve_secret(auth["password"])
            tok   = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            return {**base, "Authorization": f"Basic {tok}"}
        return base

    def _get_autosys_token(self, base_url: str, auth: dict) -> str:
        import urllib.request as ureq
        payload = json.dumps({
            "username": self._resolve_secret(auth["username"]),
            "password": self._resolve_secret(auth["password"]),
        }).encode()
        req = ureq.Request(f"{base_url}/v1/security/tokens", data=payload,
                           headers={"Content-Type": "application/json"}, method="POST")
        with ureq.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())["access_token"]

    def _trigger_job(self, base_url: str, headers: dict, job_name: str, job_params: dict) -> str:
        import urllib.request as ureq
        payload = {"jobName": job_name}
        if job_params:
            payload["jobParameters"] = job_params
        req = ureq.Request(f"{base_url}/v1/jobruns",
                           data=json.dumps(payload).encode(), headers=headers, method="POST")
        with ureq.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())
        return str(body.get("jobRunId") or body.get("runId") or body.get("id", "unknown"))

    def _poll_status(self, base_url: str, headers: dict, job_run_id: str) -> str:
        import urllib.request as ureq
        req = ureq.Request(f"{base_url}/v1/jobruns/{job_run_id}", headers=headers)
        with ureq.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read())
        return str(body.get("status") or body.get("jobStatus") or "UN")
