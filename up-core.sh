#!/usr/bin/env bash
# Provisions core infrastructure: VPC, EKS, S3 buckets, RDS PostgreSQL,
# ElastiCache Redis, ECR repositories, IAM roles, and Secrets Manager entries.
# Then uploads the environment config and pipeline configs to S3.
#
# Usage: ./up-core.sh [dev|staging|production]

set -euo pipefail

ENVIRONMENT="${1:-dev}"
DB_PASSWORD="ActionDag!1"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$SCRIPT_DIR/infra/terraform/core"
STATE_BUCKET="nextgen-databridge-terraform-state"
REGION="us-east-1"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; NC='\033[0m'
step() { echo -e "\n${CYAN}━━━ $* ${NC}"; }
info() { echo -e "${GREEN}[ok]${NC}  $*"; }

# ── Ensure Terraform state bucket exists ──────────────────────────────────────
step "Ensuring Terraform state bucket..."
if ! aws s3api head-bucket --bucket "$STATE_BUCKET" 2>/dev/null; then
  aws s3api create-bucket --bucket "$STATE_BUCKET" --region "$REGION"
  aws s3api put-bucket-versioning \
    --bucket "$STATE_BUCKET" \
    --versioning-configuration Status=Enabled
  info "Created state bucket: $STATE_BUCKET"
else
  info "State bucket exists: $STATE_BUCKET"
fi

# ── Terraform apply ───────────────────────────────────────────────────────────
step "Provisioning core infrastructure (environment: $ENVIRONMENT)..."
cd "$TF_DIR"
terraform init -upgrade
terraform apply -auto-approve \
  -var="environment=${ENVIRONMENT}" \
  -var="db_password=${DB_PASSWORD}"

# ── Upload environment config from code ───────────────────────────────────────
step "Uploading environment config..."
CONFIGS_BUCKET="nextgen-databridge-pipeline-configs-${ENVIRONMENT}"
aws s3 cp "$SCRIPT_DIR/configs/environments/${ENVIRONMENT}.json" \
  "s3://${CONFIGS_BUCKET}/environments/${ENVIRONMENT}.json"
info "Uploaded configs/environments/${ENVIRONMENT}.json"

# ── Upload pipeline configs from code ─────────────────────────────────────────
step "Uploading pipeline configs..."
for f in "$SCRIPT_DIR/configs/pipelines/"*.json; do
  aws s3 cp "$f" "s3://${CONFIGS_BUCKET}/active/$(basename "$f")"
  info "Uploaded $(basename "$f")"
done

echo ""
info "Core infrastructure ready. Environment: $ENVIRONMENT"
