#!/usr/bin/env bash
# Deploy DAGs, plugins, and requirements to the MWAA S3 bucket.
# Usage: MWAA_BUCKET=nextgen-databridge-mwaa-dev-123456789 ./deploy_to_mwaa.sh

set -euo pipefail

BUCKET="${MWAA_BUCKET:?Set MWAA_BUCKET env var}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Uploading DAGs..."
aws s3 sync "$SCRIPT_DIR/dags/" "s3://$BUCKET/dags/" --delete

echo "Zipping plugins..."
(cd "$SCRIPT_DIR/plugins" && zip -r /tmp/plugins.zip . -x "*.pyc" -x "*/__pycache__/*")
aws s3 cp /tmp/plugins.zip "s3://$BUCKET/plugins.zip"
rm /tmp/plugins.zip

echo "Uploading requirements.txt..."
aws s3 cp "$SCRIPT_DIR/requirements.txt" "s3://$BUCKET/requirements.txt"

echo "Done. MWAA will pick up changes within ~1 minute."
