#!/usr/bin/env bash
# Full NextGenDatabridge provisioning and deployment.
# Runs all components in order: core → WWI → MWAA → apps.
#
# Usage: ./up.sh [dev|staging|production]
#
# Run individual scripts for selective deploys:
#   ./up-core.sh   — VPC, EKS, S3, RDS PostgreSQL, Redis, ECR
#   ./up-wwi.sh    — WideWorldImporters SQL Server + database restore
#   ./up-mwaa.sh   — MWAA Airflow environment + DAG upload
#   ./up-apps.sh   — Docker build/push + EKS deployment

set -euo pipefail

ENVIRONMENT="${1:-dev}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; NC='\033[0m'
step() { echo -e "\n${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; echo -e "${CYAN}  $* ${NC}"; echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }
info() { echo -e "${GREEN}[ok]${NC}  $*"; }

step "STEP 1/4 — Core infrastructure"
bash "$SCRIPT_DIR/up-core.sh" "$ENVIRONMENT"

step "STEP 2/4 — WideWorldImporters SQL Server"
bash "$SCRIPT_DIR/up-wwi.sh" "$ENVIRONMENT"

step "STEP 3/4 — MWAA Airflow"
bash "$SCRIPT_DIR/up-mwaa.sh" "$ENVIRONMENT"

step "STEP 4/4 — EKS Applications"
bash "$SCRIPT_DIR/up-apps.sh" "$ENVIRONMENT"

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  NextGenDatabridge is live. Environment: $ENVIRONMENT${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
