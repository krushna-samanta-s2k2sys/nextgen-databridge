#!/usr/bin/env bash
# restore-wwi.sh — One-time WideWorldImporters database restore.
#
# Run this ONCE after `up.sh` has finished provisioning the infrastructure.
# Requires WideWorldImporters-Full.bak in the project root directory.
#
# Usage:
#   MSSQL_PASSWORD=... ./restore-wwi.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$SCRIPT_DIR/infra/terraform"

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[ok]${NC}  $*"; }
error() { echo -e "${RED}[error]${NC} $*" >&2; exit 1; }

MSSQL_PASSWORD="${MSSQL_PASSWORD:-${TF_VAR_mssql_password:-}}"
if [[ -z "$MSSQL_PASSWORD" ]]; then
  read -rsp "SQL Server password: " MSSQL_PASSWORD; echo
fi

BAK="$SCRIPT_DIR/WideWorldImporters-Full.bak"
[[ -f "$BAK" ]] || error "WideWorldImporters-Full.bak not found in project root."

RDS=$(terraform -chdir="$TF_DIR" output -raw sqlserver_endpoint)
ARTIFACTS=$(terraform -chdir="$TF_DIR" output -raw artifacts_bucket)

info "Uploading backup to s3://$ARTIFACTS ..."
aws s3 cp "$BAK" "s3://${ARTIFACTS}/WideWorldImporters-Full.bak"

bash "$SCRIPT_DIR/infra/scripts/restore_wwi.sh" "$RDS" "$MSSQL_PASSWORD" "$ARTIFACTS"
