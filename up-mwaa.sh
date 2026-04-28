#!/usr/bin/env bash
# Provisions the MWAA (Managed Workflows for Apache Airflow) environment,
# then uploads DAGs to the MWAA S3 bucket.
#
# Prerequisites: up-core.sh must have run successfully first.
#
# Usage: ./up-mwaa.sh [dev|staging|production]

set -euo pipefail

ENVIRONMENT="${1:-dev}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$SCRIPT_DIR/infra/terraform/mwaa"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; NC='\033[0m'
step() { echo -e "\n${CYAN}━━━ $* ${NC}"; }
info() { echo -e "${GREEN}[ok]${NC}  $*"; }

# ── Terraform apply ───────────────────────────────────────────────────────────
step "Provisioning MWAA environment (environment: $ENVIRONMENT)..."
cd "$TF_DIR"
terraform init -upgrade
terraform apply -auto-approve \
  -var="environment=${ENVIRONMENT}"

MWAA_BUCKET=$(terraform output -raw mwaa_bucket)
info "MWAA bucket: $MWAA_BUCKET"

# ── Upload DAGs ───────────────────────────────────────────────────────────────
step "Uploading DAGs to MWAA bucket..."
MWAA_BUCKET="$MWAA_BUCKET" bash "$SCRIPT_DIR/airflow/deploy_to_mwaa.sh"

echo ""
info "MWAA environment ready. Environment: $ENVIRONMENT"
info "Webserver URL: $(terraform -chdir="$TF_DIR" output -raw mwaa_webserver_url 2>/dev/null || echo '(available after ~10 min)')"
