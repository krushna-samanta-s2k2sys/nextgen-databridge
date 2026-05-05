"""Unit tests for KafkaOperator and PubSubOperator."""
import duckdb
import json
import os
import sys
import pytest
from unittest.mock import MagicMock

from tests.unit.operators.conftest import make_op, make_ctx


def _set_kafka_consumer(monkeypatch, consumer):
    """Replace sys.modules['kafka'].KafkaConsumer for the duration of a test."""
    mock_kafka = MagicMock()
    mock_kafka.KafkaConsumer.return_value = consumer
    monkeypatch.setitem(sys.modules, "kafka", mock_kafka)


def _set_pubsub_subscriber(monkeypatch, subscriber, *, side_effect=None):
    """Replace google.cloud.pubsub_v1.SubscriberClient for the duration of a test."""
    mock_pubsub = MagicMock()
    if side_effect is not None:
        mock_pubsub.SubscriberClient.side_effect = side_effect
    else:
        mock_pubsub.SubscriberClient.return_value = subscriber
    monkeypatch.setitem(sys.modules, "google.cloud.pubsub_v1", mock_pubsub)
    # Make `from google.cloud import pubsub_v1` work too
    mock_gc = MagicMock()
    mock_gc.pubsub_v1 = mock_pubsub
    monkeypatch.setitem(sys.modules, "google.cloud", mock_gc)


# ── KafkaOperator ──────────────────────────────────────────────────────────────

class TestKafkaOperator:
    def _make_op(self, monkeypatch, noop_audit, noop_s3, out_path):
        from operators.messaging import KafkaOperator
        op = make_op(KafkaOperator, {
            "type": "kafka_consume",
            "topic": "my-topic",
            "group_id": "test-group",
            "max_messages": 100,
            "output": {"table": "kafka_messages"},
        })
        monkeypatch.setattr(op, "local_duckdb_path", lambda *_: out_path)
        monkeypatch.setattr(op, "duckdb_s3_path",    lambda *_: "s3://bkt/kafka.duckdb")
        return op

    def _mock_consumer(self, records):
        mock_msgs = [MagicMock(value=rec) for rec in records]
        consumer = MagicMock()
        consumer.__iter__ = MagicMock(return_value=iter(mock_msgs))
        return consumer

    def test_consume_messages(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        out_path = str(tmp_path / "out.duckdb")
        op = self._make_op(monkeypatch, noop_audit, noop_s3, out_path)
        consumer = self._mock_consumer([{"id": 1, "event": "click"}, {"id": 2, "event": "view"}])
        _set_kafka_consumer(monkeypatch, consumer)

        result = op.execute(make_ctx())

        assert result == "s3://bkt/kafka.duckdb"
        conn = duckdb.connect(out_path, read_only=True)
        count = conn.execute("SELECT COUNT(*) FROM kafka_messages").fetchone()[0]
        conn.close()
        assert count == 2

    def test_xcom_push_duckdb_path(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        out_path = str(tmp_path / "out.duckdb")
        op = self._make_op(monkeypatch, noop_audit, noop_s3, out_path)
        consumer = self._mock_consumer([{"id": 1}])
        _set_kafka_consumer(monkeypatch, consumer)

        ctx = make_ctx()
        op.execute(ctx)
        ctx["ti"].xcom_push.assert_any_call(key="duckdb_path", value="s3://bkt/kafka.duckdb")

    def test_consume_error_reraises(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        out_path = str(tmp_path / "out.duckdb")
        op = self._make_op(monkeypatch, noop_audit, noop_s3, out_path)

        mock_kafka = MagicMock()
        mock_kafka.KafkaConsumer.side_effect = Exception("Kafka unavailable")
        monkeypatch.setitem(sys.modules, "kafka", mock_kafka)

        with pytest.raises(Exception, match="Kafka unavailable"):
            op.execute(make_ctx())

    def test_empty_messages_creates_empty_table(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        out_path = str(tmp_path / "out.duckdb")
        op = self._make_op(monkeypatch, noop_audit, noop_s3, out_path)
        consumer = self._mock_consumer([])
        _set_kafka_consumer(monkeypatch, consumer)

        result = op.execute(make_ctx())
        assert result == "s3://bkt/kafka.duckdb"
        conn = duckdb.connect(out_path, read_only=True)
        conn.execute("SELECT * FROM kafka_messages")
        conn.close()


# ── PubSubOperator ─────────────────────────────────────────────────────────────

class TestPubSubOperator:
    def _make_op(self, monkeypatch, noop_audit, noop_s3, out_path):
        from operators.messaging import PubSubOperator
        op = make_op(PubSubOperator, {
            "type": "pubsub_consume",
            "project_id": "my-project",
            "subscription": "my-sub",
            "topic": "my-topic",
            "max_messages": 100,
            "poll_timeout_seconds": 1,
            "output": {"table": "pubsub_messages"},
        })
        monkeypatch.setattr(op, "local_duckdb_path", lambda *_: out_path)
        monkeypatch.setattr(op, "duckdb_s3_path",    lambda *_: "s3://bkt/pubsub.duckdb")
        return op

    def _make_subscriber(self, records):
        import concurrent.futures

        def fake_subscribe(sub_path, callback):
            for rec in records:
                msg = MagicMock()
                msg.data = json.dumps(rec).encode()
                callback(msg)
            future = MagicMock()
            future.result.side_effect = concurrent.futures.TimeoutError()
            return future

        subscriber = MagicMock()
        subscriber.subscription_path.return_value = "projects/proj/subscriptions/sub"
        subscriber.subscribe.side_effect = fake_subscribe
        return subscriber

    def test_consume_messages(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        out_path = str(tmp_path / "out.duckdb")
        op = self._make_op(monkeypatch, noop_audit, noop_s3, out_path)
        subscriber = self._make_subscriber([{"id": 1}, {"id": 2}])
        _set_pubsub_subscriber(monkeypatch, subscriber)

        result = op.execute(make_ctx())
        assert result == "s3://bkt/pubsub.duckdb"

    def test_consume_error_reraises(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        out_path = str(tmp_path / "out.duckdb")
        op = self._make_op(monkeypatch, noop_audit, noop_s3, out_path)
        _set_pubsub_subscriber(monkeypatch, None, side_effect=Exception("PubSub down"))

        with pytest.raises(Exception, match="PubSub down"):
            op.execute(make_ctx())

    def test_xcom_push_duckdb_path(self, tmp_path, monkeypatch, noop_audit, noop_s3):
        out_path = str(tmp_path / "out.duckdb")
        op = self._make_op(monkeypatch, noop_audit, noop_s3, out_path)
        subscriber = self._make_subscriber([{"id": 1}])
        _set_pubsub_subscriber(monkeypatch, subscriber)

        ctx = make_ctx()
        op.execute(ctx)
        ctx["ti"].xcom_push.assert_any_call(key="duckdb_path", value="s3://bkt/pubsub.duckdb")
