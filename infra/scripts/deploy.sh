#!/usr/bin/env bash
# deploy.sh — Post-terraform K8s deployment script.
#
# Reads all dynamic values from Terraform outputs, then:
#   1. Updates local kubeconfig for the EKS cluster.
#   2. Builds the K8s manifest with real values substituted for __PLACEHOLDERS__.
#   3. Applies the manifest.
#   4. Prints MWAA DAG deployment instructions.
#
# Prerequisites: aws CLI, kubectl, terraform (run `terraform apply` first).
#
# Usage:
#   cd infra/terraform
#   terraform apply -var="db_password=$DB_PASSWORD" -var="mssql_password=$MSSQL_PASSWORD"
#   cd ../scripts && ./deploy.sh
#
# Optional env vars:
#   TF_VAR_db_password   — PostgreSQL password (same value used during terraform apply)
#   TF_VAR_environment   — dev / staging / production  (default: dev)

set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[deploy]${NC} $*"; }
warn()  { echo -e "${YELLOW}[warn]${NC}  $*"; }
error() { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="${SCRIPT_DIR}/../terraform"
MANIFEST_TEMPLATE="${SCRIPT_DIR}/../../eks/manifests/platform.yaml"
AWS_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
ENVIRONMENT="${TF_VAR_environment:-dev}"

# ── Require db_password ────────────────────────────────────────────────────────
DB_PASSWORD="${TF_VAR_db_password:-}"
if [[ -z "$DB_PASSWORD" ]]; then
  error "Set TF_VAR_db_password to the PostgreSQL password used during terraform apply."
fi

# ── Read Terraform outputs ─────────────────────────────────────────────────────
info "Reading Terraform outputs from ${TF_DIR} ..."
tf_out() { terraform -chdir="$TF_DIR" output -raw "$1" 2>/dev/null; }

CLUSTER_NAME=$(tf_out cluster_name)
DB_ENDPOINT=$(tf_out audit_db_endpoint)
MWAA_URL=$(tf_out mwaa_webserver_url)
DUCKDB_BUCKET=$(tf_out duckdb_bucket)
PIPELINE_CONFIGS_BUCKET=$(tf_out pipeline_configs_bucket)
ARTIFACTS_BUCKET=$(tf_out artifacts_bucket)
MWAA_BUCKET=$(tf_out mwaa_bucket)

info "Cluster:          $CLUSTER_NAME"
info "DB endpoint:      $DB_ENDPOINT"
info "MWAA URL:         $MWAA_URL"
info "DuckDB bucket:    $DUCKDB_BUCKET"
info "Configs bucket:   $PIPELINE_CONFIGS_BUCKET"
info "Artifacts bucket: $ARTIFACTS_BUCKET"
info "MWAA bucket:      $MWAA_BUCKET"

# ── Update kubeconfig ──────────────────────────────────────────────────────────
info "Updating kubeconfig for cluster '$CLUSTER_NAME' ..."
aws eks update-kubeconfig \
  --region "$AWS_REGION" \
  --name   "$CLUSTER_NAME"

# ── Build substituted manifest ─────────────────────────────────────────────────
DATABASE_URL="postgresql+asyncpg://airflow:${DB_PASSWORD}@${DB_ENDPOINT}:5432/airflow"

MANIFEST_TMP="$(mktemp /tmp/platform-XXXXXX.yaml)"
trap "rm -f $MANIFEST_TMP" EXIT

sed \
  -e "s|__DATABASE_URL__|${DATABASE_URL}|g" \
  -e "s|__AIRFLOW_URL__|https://${MWAA_URL}|g" \
  "$MANIFEST_TEMPLATE" > "$MANIFEST_TMP"

# ── Apply manifest ─────────────────────────────────────────────────────────────
info "Applying K8s manifest ..."
kubectl apply -f "$MANIFEST_TMP"

# ── Wait for rollout ───────────────────────────────────────────────────────────
info "Waiting for deployments to roll out ..."
kubectl rollout status deployment/nextgen-databridge-api -n nextgen-databridge --timeout=180s
kubectl rollout status deployment/nextgen-databridge-ui  -n nextgen-databridge --timeout=120s

# ── Print service endpoints ────────────────────────────────────────────────────
info "Deployment complete. Service endpoints:"
echo ""
kubectl get svc -n nextgen-databridge -o custom-columns=\
'NAME:.metadata.name,TYPE:.spec.type,EXTERNAL-IP:.status.loadBalancer.ingress[0].hostname,PORT:.spec.ports[0].port' \
  2>/dev/null || true

echo ""
info "Next steps:"
echo "  1. DAG deployment:"
echo "     MWAA_BUCKET=${MWAA_BUCKET} ${SCRIPT_DIR}/../../airflow/deploy_to_mwaa.sh"
echo ""
echo "  2. WideWorldImporters restore (first time only):"
echo "     # Upload backup first:"
echo "     aws s3 cp WideWorldImporters-Full.bak s3://${ARTIFACTS_BUCKET}/"
echo "     # Then restore:"
echo "     RDS=\$(terraform -chdir=${TF_DIR} output -raw sqlserver_endpoint)"
echo "     ${SCRIPT_DIR}/restore_wwi.sh \"\$RDS\" \"\$MSSQL_PASSWORD\" ${ARTIFACTS_BUCKET}"
echo ""
echo "  3. UI is accessible at the nextgen-databridge-ui LoadBalancer hostname above on port 80."
