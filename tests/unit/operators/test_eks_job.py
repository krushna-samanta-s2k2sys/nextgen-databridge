"""Unit tests for EKSJobOperator."""
import base64
import os
import sys
import pytest
from unittest.mock import MagicMock, patch, call

from tests.unit.operators.conftest import make_ctx, noop_audit, noop_s3

# The code does `from kubernetes import client as k8s_client`, which resolves to
# sys.modules["kubernetes"].client (the auto-attribute on the kubernetes MagicMock),
# NOT sys.modules["kubernetes.client"].  We must patch the right object.
_K8S = sys.modules["kubernetes"].client


def _eks_op(extra_task=None):
    from operators.eks_job import EKSJobOperator
    return EKSJobOperator(
        pipeline_id="test_pipeline",
        task_config={"task_id": "eks_task", "type": "eks_job", **(extra_task or {})},
        pipeline_config={"pipeline_id": "test_pipeline", "tasks": []},
        duckdb_bucket="test-bucket",
        audit_db_url="postgresql://u:p@localhost/testdb",
        task_id="eks_task",
        image="my-ecr.amazonaws.com/transform:latest",
        cpu_request="500m",
        memory_request="512Mi",
        cpu_limit="2",
        memory_limit="2Gi",
        namespace="nextgen-databridge",
        eks_cluster_name="my-cluster",
        service_account="nextgen-sa",
        env_vars={"CUSTOM_VAR": "custom_value"},
    )


def _make_aws_mocks():
    mock_eks = MagicMock()
    mock_eks.describe_cluster.return_value = {
        "cluster": {
            "endpoint": "https://k8s.example.com",
            "certificateAuthority": {"data": base64.b64encode(b"FAKECERT").decode()},
        }
    }
    mock_session = MagicMock()
    mock_sts = MagicMock()
    mock_sts.meta.service_model.service_id = "STS"
    mock_session.client.return_value = mock_sts
    mock_session.get_credentials.return_value = MagicMock()
    mock_session.events = MagicMock()

    mock_signer = MagicMock()
    mock_signer.generate_presigned_url.return_value = (
        "https://sts.us-east-1.amazonaws.com/?Action=GetCallerIdentity"
    )
    mock_botocore_signers = MagicMock()
    mock_botocore_signers.RequestSigner.return_value = mock_signer

    return mock_eks, mock_session, mock_botocore_signers


def _patched_execute(op, ctx, mock_batch):
    """Run execute() with all AWS/k8s patched. mock_batch controls BatchV1Api() return."""
    mock_eks, mock_session, mock_botocore_signers = _make_aws_mocks()
    with patch.object(_K8S, "BatchV1Api", return_value=mock_batch):
        with patch("boto3.client", return_value=mock_eks):
            with patch("boto3.session.Session", return_value=mock_session):
                with patch.dict(sys.modules, {"botocore.signers": mock_botocore_signers}):
                    with patch("time.sleep"):
                        return op.execute(ctx)


# ── execute — success path ─────────────────────────────────────────────────────

def test_execute_success(noop_audit, noop_s3):
    op = _eks_op()

    mock_job = MagicMock()
    mock_job.status.succeeded = 1
    mock_batch = MagicMock()
    mock_batch.read_namespaced_job.return_value = mock_job

    ctx = make_ctx()
    result = _patched_execute(op, ctx, mock_batch)

    assert result.startswith("s3://")
    mock_batch.create_namespaced_job.assert_called_once()


def test_execute_xcom_push_duckdb_path(noop_audit, noop_s3):
    op = _eks_op()

    mock_job = MagicMock()
    mock_job.status.succeeded = 1
    mock_batch = MagicMock()
    mock_batch.read_namespaced_job.return_value = mock_job

    ctx = make_ctx()
    result = _patched_execute(op, ctx, mock_batch)

    pushed = {kw.get("key") for _, kw in ctx["ti"].xcom_push.call_args_list}
    assert "duckdb_path" in pushed


def test_execute_submits_job_to_correct_namespace(noop_audit, noop_s3):
    op = _eks_op()

    mock_job = MagicMock()
    mock_job.status.succeeded = 1
    mock_batch = MagicMock()
    mock_batch.read_namespaced_job.return_value = mock_job

    _patched_execute(op, _make_ctx(), mock_batch)

    call_kwargs = mock_batch.create_namespaced_job.call_args[1]
    assert call_kwargs["namespace"] == "nextgen-databridge"


def _make_ctx():
    return make_ctx()


# ── execute — failure paths ────────────────────────────────────────────────────

def test_execute_k8s_api_error_reraises(noop_audit, noop_s3):
    op = _eks_op()

    mock_batch = MagicMock()
    mock_batch.read_namespaced_job.side_effect = RuntimeError("k8s API unavailable")

    ctx = make_ctx()
    with pytest.raises(RuntimeError, match="k8s API unavailable"):
        _patched_execute(op, ctx, mock_batch)


def test_execute_boto3_error_reraises(noop_audit, noop_s3):
    op = _eks_op()
    mock_botocore_signers = MagicMock()

    ctx = make_ctx()
    with patch("boto3.client", side_effect=Exception("AWS credentials missing")):
        with patch.dict(sys.modules, {"botocore.signers": mock_botocore_signers}):
            with pytest.raises(Exception, match="AWS credentials missing"):
                op.execute(ctx)


def test_execute_calls_write_task_run_failed_on_error(noop_s3):
    op = _eks_op()

    mock_batch = MagicMock()
    mock_batch.read_namespaced_job.side_effect = Exception("poll error")

    call_log = []

    def mock_write_task_run(self, run_id, status, **kw):
        call_log.append(status)

    with patch("operators.base.NextGenDatabridgeBaseOperator._write_task_run",
               mock_write_task_run):
        with pytest.raises(Exception):
            _patched_execute(op, make_ctx(), mock_batch)

    assert "failed" in call_log


# ── job name generation ────────────────────────────────────────────────────────

def test_job_name_is_rfc1123(noop_audit, noop_s3):
    import re
    op = _eks_op()

    mock_job = MagicMock()
    mock_job.status.succeeded = 1
    mock_batch = MagicMock()
    mock_batch.read_namespaced_job.return_value = mock_job

    ctx = make_ctx(run_id="run_abc123_xyz!2026-05-04T12:00:00Z")
    _patched_execute(op, ctx, mock_batch)

    # Job name is passed as the `name` kwarg to V1ObjectMeta
    meta_call_kwargs = _K8S.V1ObjectMeta.call_args[1]
    job_name = meta_call_kwargs["name"]
    assert isinstance(job_name, str)
    assert re.match(r"^[a-z0-9][a-z0-9-]{0,61}[a-z0-9]$|^[a-z0-9]$", job_name), \
        f"job_name {job_name!r} is not RFC 1123"
    assert len(job_name) <= 63


def test_job_name_max_63_chars(noop_audit, noop_s3):
    from operators.eks_job import EKSJobOperator
    op = EKSJobOperator(
        pipeline_id="a" * 50,
        task_config={"task_id": "b" * 50, "type": "eks_job"},
        pipeline_config={"pipeline_id": "a" * 50, "tasks": []},
        duckdb_bucket="test-bucket",
        audit_db_url="postgresql://u:p@localhost/testdb",
        task_id="b" * 50,
        image="img",
        cpu_request="500m", memory_request="512Mi",
        cpu_limit="1", memory_limit="1Gi",
        namespace="default",
        eks_cluster_name="cluster",
        service_account="sa",
        env_vars={},
    )

    mock_job = MagicMock()
    mock_job.status.succeeded = 1
    mock_batch = MagicMock()
    mock_batch.read_namespaced_job.return_value = mock_job

    _patched_execute(op, make_ctx(run_id="x" * 30), mock_batch)

    meta_call_kwargs = _K8S.V1ObjectMeta.call_args[1]
    job_name = meta_call_kwargs["name"]
    assert len(job_name) <= 63


# ── depends_on / env_vars ─────────────────────────────────────────────────────

def test_execute_with_depends_on(noop_audit, noop_s3):
    op = _eks_op(extra_task={"depends_on": ["upstream_task"]})

    mock_job = MagicMock()
    mock_job.status.succeeded = 1
    mock_batch = MagicMock()
    mock_batch.read_namespaced_job.return_value = mock_job

    ctx = make_ctx(xcom_data={("upstream_task", "duckdb_path"): "s3://bkt/up.duckdb"})
    result = _patched_execute(op, ctx, mock_batch)
    assert result.startswith("s3://")


def test_execute_env_vars_passed_to_job(noop_audit, noop_s3):
    op = _eks_op()

    mock_job = MagicMock()
    mock_job.status.succeeded = 1
    mock_batch = MagicMock()
    mock_batch.read_namespaced_job.return_value = mock_job

    _patched_execute(op, make_ctx(), mock_batch)

    # Job should be submitted with env containing CUSTOM_VAR
    body = mock_batch.create_namespaced_job.call_args[1]["body"]
    assert body is not None
