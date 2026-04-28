#!/usr/bin/env bash
# Provisions the WideWorldImporters SQL Server RDS instance, creates the
# Secrets Manager entries, and restores the WideWorldImporters database if
# WideWorldImporters-Full.bak is present in the project root.
#
# Prerequisites: up-core.sh must have run first (VPC and artifacts S3 bucket).
#
# Usage: ./up-wwi.sh [dev|staging|production]

set -euo pipefail

ENVIRONMENT="${1:-dev}"
MSSQL_PASSWORD="ActionDag!1"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$SCRIPT_DIR/infra/terraform/wwi"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
step() { echo -e "\n${CYAN}━━━ $* ${NC}"; }
info() { echo -e "${GREEN}[ok]${NC}  $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }

# ── Terraform apply ───────────────────────────────────────────────────────────
step "Provisioning WideWorldImporters SQL Server (environment: $ENVIRONMENT)..."
cd "$TF_DIR"
terraform init -upgrade
terraform apply -auto-approve \
  -var="environment=${ENVIRONMENT}" \
  -var="mssql_password=${MSSQL_PASSWORD}"

RDS_ENDPOINT=$(terraform output -raw sqlserver_endpoint)
ARTIFACTS_BUCKET=$(terraform output -raw artifacts_bucket)
info "SQL Server endpoint: $RDS_ENDPOINT"

# ── Restore WideWorldImporters (first-time only) ───────────────────────────────
BAK="$SCRIPT_DIR/WideWorldImporters-Full.bak"
if [[ -f "$BAK" ]]; then
  step "Uploading WideWorldImporters-Full.bak to s3://$ARTIFACTS_BUCKET ..."
  aws s3 cp "$BAK" "s3://${ARTIFACTS_BUCKET}/WideWorldImporters-Full.bak"
  info "Backup uploaded"

  step "Restoring WideWorldImporters database..."
  bash "$SCRIPT_DIR/infra/scripts/restore_wwi.sh" \
    "$RDS_ENDPOINT" "$MSSQL_PASSWORD" "$ARTIFACTS_BUCKET"
else
  warn "WideWorldImporters-Full.bak not found in project root — skipping restore."
  warn "To restore later: place the .bak file here and re-run ./up-wwi.sh $ENVIRONMENT"
fi

echo ""
info "WideWorldImporters ready. Environment: $ENVIRONMENT"
