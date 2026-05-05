"""Unit tests for APICallOperator."""
import base64
import json
import pytest
from unittest.mock import MagicMock, patch

from tests.unit.action_triggers.conftest import make_op, make_ctx


def _op(cfg_extra):
    from action_triggers.api_call import APICallOperator
    return make_op(APICallOperator, {"type": "api_call", "url": "http://example.com/api", **cfg_extra})


def _mock_urlopen(status=200, body=b'{"ok": true}'):
    resp = MagicMock()
    resp.status = status
    resp.read.return_value = body
    resp.__enter__ = MagicMock(return_value=resp)
    resp.__exit__  = MagicMock(return_value=False)
    return resp


# ── _render_url ────────────────────────────────────────────────────────────────

def test_render_url_substitutes_run_id():
    from action_triggers.api_call import APICallOperator
    op = _op({})
    rendered = op._render_url("http://api/{{ run_id }}/data", {"run_id": "r1", "ds": "2026"})
    assert rendered == "http://api/r1/data"


def test_render_url_substitutes_ds_and_pipeline_id():
    from action_triggers.api_call import APICallOperator
    op = _op({})
    rendered = op._render_url("http://api/{{ ds }}/{{ pipeline_id }}", {"run_id": "", "ds": "2026-05"})
    assert "2026-05" in rendered
    assert "test_pipeline" in rendered


# ── _build_auth_header ─────────────────────────────────────────────────────────

def test_auth_none_returns_empty():
    from action_triggers.api_call import APICallOperator
    op = _op({})
    assert op._build_auth_header({"type": "none"}) == {}


def test_auth_basic():
    from action_triggers.api_call import APICallOperator
    op = _op({})
    with patch.object(op, "_resolve_secret", side_effect=lambda v: v):
        header = op._build_auth_header({"type": "basic", "username": "u", "password": "p"})
    expected = base64.b64encode(b"u:p").decode()
    assert header == {"Authorization": f"Basic {expected}"}


def test_auth_bearer():
    from action_triggers.api_call import APICallOperator
    op = _op({})
    with patch.object(op, "_resolve_secret", return_value="my-token"):
        header = op._build_auth_header({"type": "bearer", "token": "secret:tok"})
    assert header == {"Authorization": "Bearer my-token"}


def test_auth_api_key_in_header():
    from action_triggers.api_call import APICallOperator
    op = _op({})
    with patch.object(op, "_resolve_secret", return_value="key-value"):
        header = op._build_auth_header({"type": "api_key", "key": "X-Api-Key", "value": "v", "in": "header"})
    assert header == {"X-Api-Key": "key-value"}


# ── execute — GET no auth ──────────────────────────────────────────────────────

def test_execute_get_success(noop_audit):
    op = _op({"method": "GET"})
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(200)) as m:
        result = op.execute(make_ctx())
    assert result["status_code"] == 200


def test_execute_posts_json_body(noop_audit):
    op = _op({"method": "POST", "body": {"key": "value"}})
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(201)) as mock_url:
        op.execute(make_ctx())
    req = mock_url.call_args[0][0]
    assert req.data == b'{"key": "value"}'


# ── assert_status ──────────────────────────────────────────────────────────────

def test_assert_status_pass(noop_audit):
    op = _op({"assert_status": [200, 201]})
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(200)):
        result = op.execute(make_ctx())
    assert result["status_code"] == 200


def test_assert_status_fail_raises(noop_audit):
    op = _op({"assert_status": [200]})
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(404)):
        with pytest.raises(Exception, match="404"):
            op.execute(make_ctx())


def test_default_status_check_4xx_raises(noop_audit):
    op = _op({})
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(500, b"error")):
        with pytest.raises(Exception, match="500"):
            op.execute(make_ctx())


# ── retry_on_status ────────────────────────────────────────────────────────────

def test_retry_on_status_raises(noop_audit):
    op = _op({"retry_on_status": [429]})
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(429)):
        with pytest.raises(Exception, match="429"):
            op.execute(make_ctx())


# ── response_xcom_key ──────────────────────────────────────────────────────────

def test_response_xcom_key_json(noop_audit):
    op = _op({"response_xcom_key": "api_result"})
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(200, b'{"result": 42}')):
        ctx = make_ctx()
        op.execute(ctx)
    ctx["ti"].xcom_push.assert_any_call(key="api_result", value={"result": 42})


def test_response_xcom_key_non_json(noop_audit):
    op = _op({"response_xcom_key": "raw_resp"})
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(200, b"plain text")):
        ctx = make_ctx()
        op.execute(ctx)
    ctx["ti"].xcom_push.assert_any_call(key="raw_resp", value="plain text")


# ── api_key in query ───────────────────────────────────────────────────────────

def test_api_key_in_query_appended_to_url(noop_audit):
    op = _op({"auth": {"type": "api_key", "key": "token", "value": "abc", "in": "query"}})
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(200)) as mock_url:
        with patch.object(op, "_resolve_secret", return_value="abc"):
            op.execute(make_ctx())
    req = mock_url.call_args[0][0]
    assert "token=abc" in req.full_url


# ── query params ──────────────────────────────────────────────────────────────

def test_query_params_appended(noop_audit):
    op = _op({"params": {"page": 1, "limit": 50}})
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(200)) as mock_url:
        op.execute(make_ctx())
    req = mock_url.call_args[0][0]
    assert "page=1" in req.full_url


# ── execute failure ────────────────────────────────────────────────────────────

def test_execute_network_error_reraises(noop_audit):
    op = _op({})
    with patch("urllib.request.urlopen", side_effect=OSError("network error")):
        with pytest.raises(OSError, match="network error"):
            op.execute(make_ctx())


# ── status_code xcom push ─────────────────────────────────────────────────────

def test_status_code_xcom_push(noop_audit):
    op = _op({})
    with patch("urllib.request.urlopen", return_value=_mock_urlopen(200)):
        ctx = make_ctx()
        op.execute(ctx)
    ctx["ti"].xcom_push.assert_any_call(key="status_code", value=200)
