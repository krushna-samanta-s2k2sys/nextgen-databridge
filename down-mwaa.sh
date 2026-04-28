#!/usr/bin/env bash
# Destroys the MWAA environment and its S3 bucket.
#
# Usage: ./down-mwaa.sh [dev|staging|production]

set -euo pipefail

ENVIRONMENT="${1:-dev}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$SCRIPT_DIR/infra/terraform/mwaa"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; NC='\033[0m'
step() { echo -e "\n${CYAN}━━━ $* ${NC}"; }
info() { echo -e "${GREEN}[ok]${NC}  $*"; }

# ── Empty MWAA S3 bucket ──────────────────────────────────────────────────────
step "Emptying MWAA S3 bucket..."
MWAA_BUCKET="nextgen-databridge-mwaa-${ENVIRONMENT}"
aws s3 rm "s3://$MWAA_BUCKET" --recursive 2>/dev/null || true

tmp="$(mktemp)"
versions=$(aws s3api list-object-versions --bucket "$MWAA_BUCKET" \
  --query 'Versions[].{Key:Key,VersionId:VersionId}' \
  --output json 2>/dev/null || echo 'null')
if [[ "$versions" != "null" && "$versions" != "[]" && -n "$versions" ]]; then
  echo "{\"Objects\": $versions}" > "$tmp"
  aws s3api delete-objects --bucket "$MWAA_BUCKET" --delete "file://$tmp" > /dev/null
fi

markers=$(aws s3api list-object-versions --bucket "$MWAA_BUCKET" \
  --query 'DeleteMarkers[].{Key:Key,VersionId:VersionId}' \
  --output json 2>/dev/null || echo 'null')
if [[ "$markers" != "null" && "$markers" != "[]" && -n "$markers" ]]; then
  echo "{\"Objects\": $markers}" > "$tmp"
  aws s3api delete-objects --bucket "$MWAA_BUCKET" --delete "file://$tmp" > /dev/null
fi
rm -f "$tmp"
info "Emptied $MWAA_BUCKET"

# ── Terraform destroy ─────────────────────────────────────────────────────────
step "Destroying MWAA environment (environment: $ENVIRONMENT)..."
cd "$TF_DIR"
terraform init -upgrade
terraform destroy -auto-approve \
  -var="environment=${ENVIRONMENT}"

echo ""
info "MWAA environment destroyed. Environment: $ENVIRONMENT"
