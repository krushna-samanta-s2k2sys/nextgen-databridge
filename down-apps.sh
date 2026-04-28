#!/usr/bin/env bash
# Removes Kubernetes namespaces (and their LoadBalancers) from EKS.
# Run this before down-core.sh so NLBs are released before VPC deletion.
#
# Usage: ./down-apps.sh [dev|staging|production]

set -euo pipefail

ENVIRONMENT="${1:-dev}"

REGION="us-east-1"
CLUSTER_NAME="nextgen-databridge"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
step() { echo -e "\n${CYAN}━━━ $* ${NC}"; }
info() { echo -e "${GREEN}[ok]${NC}  $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }

step "Updating kubeconfig for cluster: $CLUSTER_NAME..."
aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER_NAME" 2>/dev/null \
  || warn "Could not update kubeconfig — cluster may already be gone"

step "Deleting Kubernetes namespaces..."
kubectl delete namespace nextgen-databridge nextgen-databridge-jobs \
  --ignore-not-found=true 2>/dev/null || true

step "Waiting 60 s for NLBs to be released..."
sleep 60

echo ""
info "EKS applications removed. Environment: $ENVIRONMENT"
