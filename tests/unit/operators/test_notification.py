"""Unit tests for NotificationOperator."""
import pytest
from unittest.mock import MagicMock, patch

from tests.unit.operators.conftest import make_op, make_ctx


def _make_op(channels, monkeypatch, noop_audit, noop_s3, message="Hello", subject="Subj"):
    from operators.notification import NotificationOperator
    op = make_op(NotificationOperator, {
        "type": "notification",
        "channels": channels,
        "message": message,
        "subject": subject,
    })
    return op


# ── Slack ──────────────────────────────────────────────────────────────────────

def test_slack_notification_sent(monkeypatch, noop_audit, noop_s3):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "http://hooks.slack.com/fake")
    op = _make_op(["slack:#general"], monkeypatch, noop_audit, noop_s3, message="Test message")

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_urlopen.return_value.__exit__  = MagicMock(return_value=False)
        result = op.execute(make_ctx())

    mock_urlopen.assert_called_once()
    assert "slack:#general" in result


def test_slack_notification_skipped_when_no_webhook(monkeypatch, noop_audit, noop_s3):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    op = _make_op(["slack:#general"], monkeypatch, noop_audit, noop_s3)

    with patch("urllib.request.urlopen") as mock_urlopen:
        result = op.execute(make_ctx())

    mock_urlopen.assert_not_called()


# ── Email ──────────────────────────────────────────────────────────────────────

def test_email_notification_sent(monkeypatch, noop_audit, noop_s3):
    monkeypatch.setenv("SMTP_HOST", "localhost")
    monkeypatch.setenv("SMTP_PORT", "25")
    op = _make_op(["email:user@example.com"], monkeypatch, noop_audit, noop_s3,
                  subject="Test Subject", message="Body text")

    mock_smtp = MagicMock()
    mock_smtp.__enter__ = MagicMock(return_value=mock_smtp)
    mock_smtp.__exit__  = MagicMock(return_value=False)

    with patch("smtplib.SMTP", return_value=mock_smtp) as mock_smtp_cls:
        result = op.execute(make_ctx())

    mock_smtp.sendmail.assert_called_once()
    assert "email:user@example.com" in result


# ── Multiple channels ──────────────────────────────────────────────────────────

def test_multiple_channels(monkeypatch, noop_audit, noop_s3):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "http://hooks.slack.com/fake")
    op = _make_op(["slack:#general", "email:user@example.com"], monkeypatch, noop_audit, noop_s3)

    with patch("urllib.request.urlopen") as mock_url:
        mock_url.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_url.return_value.__exit__  = MagicMock(return_value=False)
        with patch("smtplib.SMTP") as mock_smtp:
            mock_smtp.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_smtp.return_value.__exit__  = MagicMock(return_value=False)
            result = op.execute(make_ctx())

    assert "slack:#general" in result
    assert "email:user@example.com" in result


# ── Channel failure is non-fatal ───────────────────────────────────────────────

def test_channel_failure_is_non_fatal(monkeypatch, noop_audit, noop_s3):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "http://hooks.slack.com/fake")
    op = _make_op(["slack:#general"], monkeypatch, noop_audit, noop_s3)

    with patch("urllib.request.urlopen", side_effect=OSError("network error")):
        result = op.execute(make_ctx())  # must not raise

    assert "Notified:" in result  # returns but didn't add to sent list


# ── XCom and task run ──────────────────────────────────────────────────────────

def test_execute_returns_notified_string(monkeypatch, noop_audit, noop_s3):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "http://hooks.slack.com/fake")
    op = _make_op(["slack:#ops"], monkeypatch, noop_audit, noop_s3)

    with patch("urllib.request.urlopen") as mock_url:
        mock_url.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_url.return_value.__exit__  = MagicMock(return_value=False)
        result = op.execute(make_ctx())

    assert result.startswith("Notified:")


def test_execute_empty_channels(monkeypatch, noop_audit, noop_s3):
    op = _make_op([], monkeypatch, noop_audit, noop_s3)
    result = op.execute(make_ctx())
    assert result == "Notified: []"
