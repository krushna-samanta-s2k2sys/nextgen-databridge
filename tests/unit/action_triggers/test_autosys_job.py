"""Unit tests for AutosysJobOperator."""
import json
import pytest
import time
from unittest.mock import MagicMock, patch, call

from tests.unit.action_triggers.conftest import make_op, make_ctx


def _op(cfg_extra):
    from action_triggers.autosys_job import AutosysJobOperator
    return make_op(AutosysJobOperator, {
        "type": "autosys_job",
        "autosys_url": "https://autosys.corp:9443",
        "job_name": "MY_JOB",
        **cfg_extra,
    })


def _urlopen_resp(body: dict):
    resp = MagicMock()
    resp.read.return_value = json.dumps(body).encode()
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__  = MagicMock(return_value=False)
    return resp


# ── _make_headers ──────────────────────────────────────────────────────────────

def test_make_headers_no_auth():
    from action_triggers.autosys_job import AutosysJobOperator
    op = _op({})
    headers = op._make_headers("http://a", {})
    assert "Content-Type" in headers
    assert "Authorization" not in headers


def test_make_headers_bearer():
    from action_triggers.autosys_job import AutosysJobOperator
    op = _op({})
    with patch.object(op, "_resolve_secret", return_value="tok"):
        headers = op._make_headers("http://a", {"type": "bearer", "token": "secret:tok"})
    assert headers["Authorization"] == "Bearer tok"


def test_make_headers_basic():
    import base64
    from action_triggers.autosys_job import AutosysJobOperator
    op = _op({})
    with patch.object(op, "_resolve_secret", side_effect=lambda v: v):
        headers = op._make_headers("http://a", {"type": "basic", "username": "u", "password": "p"})
    expected = base64.b64encode(b"u:p").decode()
    assert headers["Authorization"] == f"Basic {expected}"


# ── execute — trigger, no wait ─────────────────────────────────────────────────

def test_execute_trigger_no_wait(noop_audit):
    op = _op({"wait_for_completion": False})

    with patch("urllib.request.urlopen") as mock_url:
        mock_url.return_value = _urlopen_resp({"jobRunId": "run_42"})
        result = op.execute(make_ctx())

    assert result["job_run_id"] == "run_42"
    assert result["job_status"] == "ST"


def test_execute_xcom_push_job_run_id(noop_audit):
    op = _op({"wait_for_completion": False})

    with patch("urllib.request.urlopen") as mock_url:
        mock_url.return_value = _urlopen_resp({"jobRunId": "run_99"})
        ctx = make_ctx()
        op.execute(ctx)

    ctx["ti"].xcom_push.assert_any_call(key="job_run_id", value="run_99")


# ── execute — wait, success ────────────────────────────────────────────────────

def test_execute_wait_success(noop_audit):
    op = _op({"wait_for_completion": True, "poll_interval_seconds": 0, "timeout_seconds": 10})

    trigger_resp = _urlopen_resp({"jobRunId": "run_1"})
    poll_resp1   = _urlopen_resp({"status": "AC"})
    poll_resp2   = _urlopen_resp({"status": "SU"})

    with patch("urllib.request.urlopen", side_effect=[trigger_resp, poll_resp1, poll_resp2]):
        with patch("time.sleep"):
            result = op.execute(make_ctx())

    assert result["job_status"] == "SU"


# ── execute — wait, failure ────────────────────────────────────────────────────

def test_execute_wait_failure(noop_audit):
    op = _op({"wait_for_completion": True, "poll_interval_seconds": 0, "timeout_seconds": 10})

    trigger_resp = _urlopen_resp({"jobRunId": "run_2"})
    poll_resp    = _urlopen_resp({"status": "FA"})

    with patch("urllib.request.urlopen", side_effect=[trigger_resp, poll_resp]):
        with patch("time.sleep"):
            with pytest.raises(Exception, match="FA"):
                op.execute(make_ctx())


# ── execute — timeout ──────────────────────────────────────────────────────────

def test_execute_timeout(noop_audit):
    op = _op({"wait_for_completion": True, "poll_interval_seconds": 0, "timeout_seconds": 1})

    trigger_resp = _urlopen_resp({"jobRunId": "run_3"})
    poll_resp    = _urlopen_resp({"status": "AC"})

    call_count = 0

    def always_pending(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return trigger_resp
        return poll_resp

    with patch("urllib.request.urlopen", side_effect=always_pending):
        with patch("time.time", side_effect=[0, 0, 5, 5, 5]):
            with patch("time.sleep"):
                with pytest.raises(Exception, match="did not reach terminal status"):
                    op.execute(make_ctx())


# ── execute — network error re-raises ─────────────────────────────────────────

def test_execute_network_error_reraises(noop_audit):
    op = _op({"wait_for_completion": False})

    with patch("urllib.request.urlopen", side_effect=OSError("connection refused")):
        with pytest.raises(OSError, match="connection refused"):
            op.execute(make_ctx())


# ── _trigger_job fallback fields ──────────────────────────────────────────────

def test_trigger_job_uses_run_id_field(noop_audit):
    op = _op({"wait_for_completion": False})

    with patch("urllib.request.urlopen") as mock_url:
        mock_url.return_value = _urlopen_resp({"runId": "r-fallback"})
        result = op.execute(make_ctx())

    assert result["job_run_id"] == "r-fallback"
