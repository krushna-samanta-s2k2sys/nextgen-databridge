#!/usr/bin/env bash
# Sync DAGs to the MWAA S3 bucket.
#
# plugins.zip, requirements.txt, and startup.sh are managed by Terraform.
# Re-uploading them here would create new S3 object versions, causing Terraform
# to detect version drift on the next apply and trigger a 20-minute MWAA
# environment update even when no infrastructure changed.
#
# Usage: MWAA_BUCKET=nextgen-databridge-mwaa-dev ./deploy_to_mwaa.sh

set -euo pipefail

BUCKET="${MWAA_BUCKET:?Set MWAA_BUCKET env var}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Syncing DAGs to s3://$BUCKET/dags/ ..."
aws s3 sync "$SCRIPT_DIR/dags/" "s3://$BUCKET/dags/" --delete

echo "Done. MWAA scheduler picks up DAG changes within 1-3 minutes."
