"""
EKS Job Operator
Submits a Kubernetes Job to the nextgen-databridge EKS cluster for heavy compute
tasks that need more resources than a standard MWAA worker can provide.
Authenticates to EKS using a presigned STS GetCallerIdentity token (AWS IAM auth).
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timezone

from airflow.utils.context import Context

from operators.base import NextGenDatabridgeBaseOperator

logger = logging.getLogger("nextgen_databridge.operators.eks_job")


class EKSJobOperator(NextGenDatabridgeBaseOperator):

    def __init__(
        self,
        image: str,
        cpu_request: str,
        memory_request: str,
        cpu_limit: str,
        memory_limit: str,
        namespace: str,
        eks_cluster_name: str,
        service_account: str,
        env_vars: dict,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.image            = image
        self.cpu_request      = cpu_request
        self.memory_request   = memory_request
        self.cpu_limit        = cpu_limit
        self.memory_limit     = memory_limit
        self.namespace        = namespace
        self.eks_cluster_name = eks_cluster_name
        self.service_account  = service_account
        self.env_vars         = env_vars or {}

    def execute(self, context: Context) -> str:
        run_id  = context["run_id"]
        task_id = self.task_config["task_id"]
        ti      = context["ti"]

        # Job name: RFC 1123, max 63 chars, unique per retry attempt
        attempt  = getattr(ti, "try_number", 1)
        _raw     = f"df-{self.pipeline_id[:18]}-{task_id[:18]}-{run_id[-12:]}-a{attempt}".lower()
        job_name = re.sub(r"-+", "-", re.sub(r"[^a-z0-9-]", "-", _raw)).strip("-")[:63].rstrip("-")
        out_s3   = self.duckdb_s3_path(run_id, task_id)

        start_ts    = datetime.now(timezone.utc)
        input_paths = {}
        for dep in self.task_config.get("depends_on", []):
            producer_id = self._find_duckdb_producer_task(ti, dep)
            if producer_id and producer_id not in input_paths:
                input_paths[producer_id] = self.duckdb_s3_path(run_id, producer_id)

        self._write_task_run(run_id, "running", start_time=start_ts, input_sources=[],
                             metrics={"cpu_request": self.cpu_request,
                                      "memory_request": self.memory_request})

        try:
            import base64
            import tempfile
            import boto3
            from botocore.signers import RequestSigner
            from kubernetes import client as k8s_client

            region       = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
            cluster_name = os.getenv("EKS_CLUSTER_NAME", "nextgen-databridge")

            eks_boto     = boto3.client("eks", region_name=region)
            cluster_info = eks_boto.describe_cluster(name=cluster_name)["cluster"]

            boto_session = boto3.session.Session()
            sts_client   = boto_session.client("sts", region_name=region)
            signer       = RequestSigner(
                sts_client.meta.service_model.service_id, region, "sts", "v4",
                boto_session.get_credentials(), boto_session.events,
            )
            presigned = signer.generate_presigned_url(
                {
                    "method": "GET",
                    "url": f"https://sts.{region}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15",
                    "body": {}, "headers": {"x-k8s-aws-id": cluster_name}, "context": {},
                },
                region_name=region, expires_in=60, operation_name="GetCallerIdentity",
            )
            k8s_token = "k8s-aws-v1." + base64.urlsafe_b64encode(
                presigned.encode("utf-8")
            ).decode("utf-8").rstrip("=")

            ca_bytes = base64.b64decode(cluster_info["certificateAuthority"]["data"])
            ca_file  = tempfile.NamedTemporaryFile(delete=False, suffix=".crt")
            ca_file.write(ca_bytes)
            ca_file.flush()
            ca_file.close()

            cfg = k8s_client.Configuration()
            cfg.host         = cluster_info["endpoint"]
            cfg.ssl_ca_cert  = ca_file.name
            cfg.api_key      = {"authorization": f"Bearer {k8s_token}"}
            k8s_client.Configuration.set_default(cfg)

            batch_v1 = k8s_client.BatchV1Api()

            env = [
                k8s_client.V1EnvVar(name="PIPELINE_ID",        value=self.pipeline_id),
                k8s_client.V1EnvVar(name="RUN_ID",             value=run_id),
                k8s_client.V1EnvVar(name="TASK_ID",            value=task_id),
                k8s_client.V1EnvVar(name="DS",                 value=context.get("ds", "")),
                k8s_client.V1EnvVar(name="OUTPUT_DUCKDB_PATH", value=out_s3),
                k8s_client.V1EnvVar(name="INPUT_PATHS",        value=json.dumps(input_paths)),
                k8s_client.V1EnvVar(name="TASK_CONFIG",        value=json.dumps(self.task_config)),
                k8s_client.V1EnvVar(name="AUDIT_DB_URL",       value=self.audit_db_url or ""),
                k8s_client.V1EnvVar(name="DUCKDB_BUCKET",      value=self.duckdb_bucket),
                k8s_client.V1EnvVar(name="AWS_ENDPOINT_URL",   value=os.getenv("AWS_ENDPOINT_URL", "")),
            ]
            for k, v in self.env_vars.items():
                env.append(k8s_client.V1EnvVar(name=k, value=str(v)))

            manifest = k8s_client.V1Job(
                api_version="batch/v1", kind="Job",
                metadata=k8s_client.V1ObjectMeta(
                    name=job_name, namespace=self.namespace,
                    labels={"app": "nextgen-databridge", "pipeline": self.pipeline_id, "task": task_id},
                ),
                spec=k8s_client.V1JobSpec(
                    backoff_limit=self.task_config.get("retries", 3),
                    ttl_seconds_after_finished=3600,
                    template=k8s_client.V1PodTemplateSpec(
                        spec=k8s_client.V1PodSpec(
                            restart_policy="Never",
                            service_account_name=self.service_account,
                            tolerations=[k8s_client.V1Toleration(
                                key="nextgen-databridge-job", operator="Equal",
                                value="true", effect="NoSchedule",
                            )],
                            containers=[k8s_client.V1Container(
                                name="transform", image=self.image,
                                image_pull_policy="Always", env=env,
                                resources=k8s_client.V1ResourceRequirements(
                                    requests={"cpu": self.cpu_request, "memory": self.memory_request},
                                    limits={"cpu": self.cpu_limit, "memory": self.memory_limit},
                                ),
                            )],
                        )
                    ),
                ),
            )

            batch_v1.create_namespaced_job(namespace=self.namespace, body=manifest)
            logger.info(f"EKS Job submitted: {job_name}")

            timeout_secs = self.task_config.get("timeout_minutes", 120) * 60
            elapsed = 0
            while elapsed < timeout_secs:
                time.sleep(15)
                elapsed += 15
                job = batch_v1.read_namespaced_job(name=job_name, namespace=self.namespace)
                if job.status.succeeded:
                    break
                if job.status.failed and job.status.failed >= manifest.spec.backoff_limit:
                    raise RuntimeError(f"EKS Job {job_name} failed after {job.status.failed} attempts")

            duration = (datetime.now(timezone.utc) - start_ts).total_seconds()
            self._write_task_run(
                run_id, "success",
                start_time=start_ts, end_time=datetime.now(timezone.utc),
                duration_seconds=duration, output_duckdb_path=out_s3,
                metrics={"eks_job_name": job_name},
            )
            context["ti"].xcom_push(key="duckdb_path", value=out_s3)
            return out_s3

        except Exception as e:
            self._write_task_run(run_id, "failed", start_time=start_ts,
                                 error_message=str(e)[:2000],
                                 metrics={"eks_job_name": job_name})
            raise
