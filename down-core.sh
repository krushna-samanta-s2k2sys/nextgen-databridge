#!/usr/bin/env bash
# Destroys core infrastructure: VPC, EKS, S3 buckets, RDS PostgreSQL,
# ElastiCache Redis, ECR repositories, IAM roles.
#
# Prerequisites: run down-apps.sh, down-mwaa.sh, down-wwi.sh first.
#
# Usage: ./down-core.sh [dev|staging|production]

set -euo pipefail

ENVIRONMENT="${1:-dev}"
DB_PASSWORD="ActionDag!1"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$SCRIPT_DIR/infra/terraform/core"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
step() { echo -e "\n${CYAN}━━━ $* ${NC}"; }
info() { echo -e "${GREEN}[ok]${NC}  $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }

# ── Empty a versioned S3 bucket ───────────────────────────────────────────────
empty_bucket() {
  local bucket="$1"
  echo "  Emptying s3://$bucket ..."
  aws s3 rm "s3://$bucket" --recursive 2>/dev/null || true

  local tmp versions markers
  tmp="$(mktemp)"

  versions=$(aws s3api list-object-versions --bucket "$bucket" \
    --query 'Versions[].{Key:Key,VersionId:VersionId}' \
    --output json 2>/dev/null || echo 'null')
  if [[ "$versions" != "null" && "$versions" != "[]" && -n "$versions" ]]; then
    echo "{\"Objects\": $versions}" > "$tmp"
    aws s3api delete-objects --bucket "$bucket" --delete "file://$tmp" > /dev/null
  fi

  markers=$(aws s3api list-object-versions --bucket "$bucket" \
    --query 'DeleteMarkers[].{Key:Key,VersionId:VersionId}' \
    --output json 2>/dev/null || echo 'null')
  if [[ "$markers" != "null" && "$markers" != "[]" && -n "$markers" ]]; then
    echo "{\"Objects\": $markers}" > "$tmp"
    aws s3api delete-objects --bucket "$bucket" --delete "file://$tmp" > /dev/null
  fi

  rm -f "$tmp"
  info "Emptied $bucket"
}

# ── Purge ECR repository ──────────────────────────────────────────────────────
purge_ecr() {
  local repo="$1"
  local tmp ids
  tmp="$(mktemp)"
  ids=$(aws ecr list-images --repository-name "$repo" \
    --query 'imageIds[*]' --output json 2>/dev/null || echo '[]')
  if [[ "$ids" != "[]" && -n "$ids" ]]; then
    echo "$ids" > "$tmp"
    aws ecr batch-delete-image --repository-name "$repo" \
      --image-ids "file://$tmp" > /dev/null 2>&1 || true
    info "Purged ECR: $repo"
  fi
  rm -f "$tmp"
}

step "Emptying S3 buckets (environment: $ENVIRONMENT)..."
for bucket in \
  "nextgen-databridge-duckdb-store-${ENVIRONMENT}" \
  "nextgen-databridge-pipeline-configs-${ENVIRONMENT}" \
  "nextgen-databridge-artifacts-${ENVIRONMENT}"; do
  empty_bucket "$bucket"
done

step "Purging ECR repositories..."
for repo in \
  "nextgen-databridge/api" \
  "nextgen-databridge/ui" \
  "nextgen-databridge/transform"; do
  purge_ecr "$repo" || warn "ECR repo $repo not found or already empty"
done

step "Destroying core infrastructure (environment: $ENVIRONMENT)..."
cd "$TF_DIR"
terraform init -upgrade
terraform destroy -auto-approve \
  -var="environment=${ENVIRONMENT}" \
  -var="db_password=${DB_PASSWORD}"

echo ""
info "Core infrastructure destroyed. Environment: $ENVIRONMENT"
