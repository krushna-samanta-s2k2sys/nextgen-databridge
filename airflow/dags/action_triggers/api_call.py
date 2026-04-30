"""
API Call Action Trigger
Calls an external HTTP endpoint as a DAG task.
Supports all common auth schemes and AWS Secrets Manager value resolution.
"""
from __future__ import annotations

import json
import logging
import os
import traceback
from datetime import datetime, timezone
from typing import Optional

from airflow.utils.context import Context

from operators.base import NextGenDatabridgeBaseOperator

logger = logging.getLogger("nextgen_databridge.action_triggers.api_call")


class APICallOperator(NextGenDatabridgeBaseOperator):
    """
    task_config keys:
      url              (str, required)  — target URL; supports {{ run_id }}/{{ ds }}/{{ pipeline_id }}
      method           (str)            — HTTP verb, default GET
      headers          (dict)           — extra request headers; values may be "secret:<name>[/key]"
      params           (dict)           — URL query parameters
      body             (dict|str)       — request body; dicts are JSON-serialised
      auth             (dict)           — see auth block variants below
      assert_status    (list[int])      — acceptable status codes; defaults to [200..299]
      retry_on_status  (list[int])      — codes that trigger Airflow retry, e.g. [429, 503]
      timeout_seconds  (int)            — request timeout, default 30
      response_xcom_key (str)           — XCom key to push response body into

    auth block variants:
      {"type": "none"}
      {"type": "basic",   "username": "...", "password": "secret:name/key"}
      {"type": "bearer",  "token": "secret:name/key"}
      {"type": "api_key", "key": "X-Api-Key", "value": "secret:name/key", "in": "header|query"}
      {"type": "oauth2_client_credentials",
       "token_url": "...", "client_id": "...", "client_secret": "secret:name/key", "scope": "..."}
    """

    def execute(self, context: Context):
        import urllib.request as ureq

        cfg      = self.task_config
        run_id   = context["run_id"]
        start_ts = datetime.now(timezone.utc)
        self._write_task_run(run_id, "running", start_time=start_ts)

        try:
            url     = self._render_url(cfg["url"], context)
            method  = cfg.get("method", "GET").upper()
            timeout = int(cfg.get("timeout_seconds", 30))
            assert_status   = cfg.get("assert_status")
            retry_on_status = cfg.get("retry_on_status", [])

            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            for k, v in (cfg.get("headers") or {}).items():
                headers[k] = self._resolve_secret(v)

            auth = cfg.get("auth") or {}
            if auth.get("type") == "oauth2_client_credentials":
                headers["Authorization"] = f"Bearer {self._oauth2_token(auth)}"
            elif not (auth.get("type") == "api_key" and auth.get("in") == "query"):
                headers.update(self._build_auth_header(auth))

            params = dict(cfg.get("params") or {})
            if auth.get("type") == "api_key" and auth.get("in") == "query":
                params[auth["key"]] = self._resolve_secret(auth["value"])
            if params:
                import urllib.parse
                url = url + ("&" if "?" in url else "?") + urllib.parse.urlencode(params)

            body_cfg = cfg.get("body")
            data: Optional[bytes] = None
            if body_cfg is not None:
                data = (json.dumps(body_cfg) if isinstance(body_cfg, dict)
                        else str(body_cfg)).encode()

            req = ureq.Request(url, data=data, headers=headers, method=method)
            with ureq.urlopen(req, timeout=timeout) as resp:
                status_code   = resp.status
                response_body = resp.read().decode(errors="replace")

            logger.info(f"API call {method} {url} → HTTP {status_code}")

            if retry_on_status and status_code in retry_on_status:
                raise Exception(f"HTTP {status_code} is in retry_on_status — Airflow will retry")

            if assert_status:
                if status_code not in assert_status:
                    raise Exception(f"HTTP {status_code} not in assert_status {assert_status}")
            elif status_code >= 400:
                raise Exception(f"HTTP {status_code}: {response_body[:500]}")

            if cfg.get("response_xcom_key"):
                try:
                    context["ti"].xcom_push(key=cfg["response_xcom_key"],
                                            value=json.loads(response_body))
                except json.JSONDecodeError:
                    context["ti"].xcom_push(key=cfg["response_xcom_key"], value=response_body)

            context["ti"].xcom_push(key="status_code", value=status_code)

            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(run_id, "success", start_time=start_ts,
                                 end_time=datetime.now(timezone.utc),
                                 duration_seconds=duration,
                                 metrics={"status_code": status_code, "url": url, "method": method})
            return {"status_code": status_code}

        except Exception as exc:
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(run_id, "failed", start_time=start_ts,
                                 end_time=datetime.now(timezone.utc),
                                 duration_seconds=duration,
                                 error_message=str(exc),
                                 error_traceback=traceback.format_exc())
            raise

    # ── Auth helpers ───────────────────────────────────────────────────────────

    def _build_auth_header(self, auth: dict) -> dict:
        import base64
        auth_type = (auth or {}).get("type", "none")
        if auth_type == "basic":
            user  = self._resolve_secret(auth["username"])
            pwd   = self._resolve_secret(auth["password"])
            token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            return {"Authorization": f"Basic {token}"}
        if auth_type == "bearer":
            return {"Authorization": f"Bearer {self._resolve_secret(auth['token'])}"}
        if auth_type == "api_key" and auth.get("in", "header") == "header":
            return {auth["key"]: self._resolve_secret(auth["value"])}
        return {}

    def _oauth2_token(self, auth: dict) -> str:
        import urllib.request as ureq, urllib.parse
        data = urllib.parse.urlencode({
            "grant_type":    "client_credentials",
            "client_id":     self._resolve_secret(auth["client_id"]),
            "client_secret": self._resolve_secret(auth["client_secret"]),
            "scope":         auth.get("scope", ""),
        }).encode()
        req = ureq.Request(auth["token_url"], data=data,
                           headers={"Content-Type": "application/x-www-form-urlencoded"})
        with ureq.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())["access_token"]

    def _render_url(self, url: str, context: dict) -> str:
        return (url
                .replace("{{ run_id }}", context.get("run_id", ""))
                .replace("{{ ds }}", str(context.get("ds", "")))
                .replace("{{ pipeline_id }}", self.pipeline_id))
