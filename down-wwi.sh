#!/usr/bin/env bash
# Destroys the WideWorldImporters SQL Server RDS instance and its
# associated Secrets Manager entries, IAM role, and option group.
#
# Usage: ./down-wwi.sh [dev|staging|production]

set -euo pipefail

ENVIRONMENT="${1:-dev}"
MSSQL_PASSWORD="ActionDag!1"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$SCRIPT_DIR/infra/terraform/wwi"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; NC='\033[0m'
step() { echo -e "\n${CYAN}━━━ $* ${NC}"; }
info() { echo -e "${GREEN}[ok]${NC}  $*"; }

step "Destroying WideWorldImporters SQL Server (environment: $ENVIRONMENT)..."
cd "$TF_DIR"
terraform init -upgrade
terraform destroy -auto-approve \
  -var="environment=${ENVIRONMENT}" \
  -var="mssql_password=${MSSQL_PASSWORD}"

echo ""
info "WideWorldImporters SQL Server destroyed. Environment: $ENVIRONMENT"
