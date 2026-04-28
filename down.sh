#!/usr/bin/env bash
# Tears down the entire NextGenDatabridge environment.
# Destroys in reverse dependency order: apps → MWAA → WWI → core.
#
# Usage: ./down.sh [dev|staging|production]
#
# Run individual scripts for selective teardown:
#   ./down-apps.sh   — Remove EKS namespaces / release NLBs
#   ./down-mwaa.sh   — Destroy MWAA environment
#   ./down-wwi.sh    — Destroy WideWorldImporters SQL Server
#   ./down-core.sh   — Destroy VPC, EKS, S3, RDS PostgreSQL, Redis, ECR

set -euo pipefail

ENVIRONMENT="${1:-dev}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

RED='\033[0;31m'; CYAN='\033[0;36m'; GREEN='\033[0;32m'; NC='\033[0m'
step() { echo -e "\n${CYAN}━━━ $* ${NC}"; }
info() { echo -e "${GREEN}[ok]${NC}  $*"; }

echo -e "${RED}"
echo "  ██████╗  ██████╗ ██╗    ██╗███╗   ██╗"
echo "  ██╔══██╗██╔═══██╗██║    ██║████╗  ██║"
echo "  ██║  ██║██║   ██║██║ █╗ ██║██╔██╗ ██║"
echo "  ██║  ██║██║   ██║██║███╗██║██║╚██╗██║"
echo "  ██████╔╝╚██████╔╝╚███╔███╔╝██║ ╚████║"
echo "  ╚═════╝  ╚═════╝  ╚══╝╚══╝ ╚═╝  ╚═══╝"
echo -e "${NC}"
echo -e "${RED}  This will permanently destroy ALL resources for environment: $ENVIRONMENT${NC}"
echo ""
read -r -p "  Type YES to continue: " CONFIRM
[[ "$CONFIRM" == "YES" ]] || { echo "Aborted."; exit 0; }

step "STEP 1/4 — Remove EKS applications"
bash "$SCRIPT_DIR/down-apps.sh" "$ENVIRONMENT"

step "STEP 2/4 — Destroy MWAA"
bash "$SCRIPT_DIR/down-mwaa.sh" "$ENVIRONMENT"

step "STEP 3/4 — Destroy WideWorldImporters SQL Server"
bash "$SCRIPT_DIR/down-wwi.sh" "$ENVIRONMENT"

step "STEP 4/4 — Destroy core infrastructure"
bash "$SCRIPT_DIR/down-core.sh" "$ENVIRONMENT"

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  All resources destroyed. Environment: $ENVIRONMENT${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
