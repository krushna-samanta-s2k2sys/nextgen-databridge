#!/usr/bin/env bash
_SM=$(aws secretsmanager get-secret-value \
  --region "${aws_region}" \
  --secret-id "nextgen-databridge/connections/audit_db" \
  --query SecretString --output text 2>/dev/null || echo '{}')
export _SM
export NEXTGEN_DATABRIDGE_AUDIT_DB_URL=$(python3 -c \
  "import json,os; d=json.loads(os.environ['_SM']); print(d.get('url',''))")
unset _SM
export NEXTGEN_DATABRIDGE_DUCKDB_BUCKET="${duckdb_store_bucket}"
export NEXTGEN_DATABRIDGE_PIPELINE_CONFIGS_BUCKET="${pipeline_configs_bucket}"
export NEXTGEN_DATABRIDGE_ENV="${environment}"
export NEXTGEN_DATABRIDGE_ECR_REGISTRY="${ecr_registry}"
export AWS_DEFAULT_REGION="${aws_region}"
export EKS_CLUSTER_NAME="nextgen-databridge"
