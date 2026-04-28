#!/usr/bin/env bash
# Builds and pushes Docker images (when no IMAGE_TAG is given), then deploys
# the Kubernetes manifests to EKS and uploads DAGs to MWAA.
#
# Prerequisites: up-core.sh and up-mwaa.sh must have run first.
#
# Usage:
#   ./up-apps.sh [dev|staging|production]             # builds fresh images
#   IMAGE_TAG=sha-abc123 ./up-apps.sh staging          # promotes existing image

set -euo pipefail

ENVIRONMENT="${1:-dev}"
DB_PASSWORD="ActionDag!1"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REGION="us-east-1"
CLUSTER_NAME="nextgen-databridge"
MWAA_BUCKET="nextgen-databridge-mwaa-${ENVIRONMENT}"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
step() { echo -e "\n${CYAN}━━━ $* ${NC}"; }
info() { echo -e "${GREEN}[ok]${NC}  $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }

# ── Resolve AWS account ───────────────────────────────────────────────────────
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGISTRY="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
info "AWS account: $ACCOUNT | Registry: $REGISTRY"

# ── Build & push images (skip if IMAGE_TAG already set) ───────────────────────
if [[ -z "${IMAGE_TAG:-}" ]]; then
  IMAGE_TAG="local-$(git rev-parse --short HEAD 2>/dev/null || date +%s)"

  step "Logging in to ECR..."
  aws ecr get-login-password --region "$REGION" \
    | docker login --username AWS --password-stdin "$REGISTRY" 2>/dev/null \
    || warn "docker login warning (buildx push should still work)"

  step "Building and pushing API image (tag: $IMAGE_TAG)..."
  docker buildx build --platform linux/amd64 --push \
    -t "${REGISTRY}/nextgen-databridge/api:${IMAGE_TAG}" \
    -t "${REGISTRY}/nextgen-databridge/api:${ENVIRONMENT}" \
    "$SCRIPT_DIR/backend"

  step "Building and pushing UI image..."
  docker buildx build --platform linux/amd64 --push \
    -t "${REGISTRY}/nextgen-databridge/ui:${IMAGE_TAG}" \
    -t "${REGISTRY}/nextgen-databridge/ui:${ENVIRONMENT}" \
    "$SCRIPT_DIR/frontend"

  step "Building and pushing Transform image..."
  docker buildx build --platform linux/amd64 --push \
    -t "${REGISTRY}/nextgen-databridge/transform:${IMAGE_TAG}" \
    -t "${REGISTRY}/nextgen-databridge/transform:${ENVIRONMENT}" \
    "$SCRIPT_DIR/eks/jobs"

  info "Images pushed with tag: $IMAGE_TAG"
else
  info "Using existing image tag: $IMAGE_TAG (no build)"
fi

# ── Update kubeconfig ─────────────────────────────────────────────────────────
step "Updating kubeconfig for cluster: $CLUSTER_NAME..."
aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER_NAME"

# ── Resolve dynamic values from Secrets Manager ───────────────────────────────
step "Resolving connection values from Secrets Manager..."
DB_SECRET=$(aws secretsmanager get-secret-value \
  --secret-id "nextgen-databridge/connections/audit_db" \
  --query SecretString --output text)
DATABASE_URL=$(python3 -c "import json,sys; d=json.loads(sys.argv[1]); print(d['url'])" "$DB_SECRET")

MWAA_WEBSERVER_URL=$(aws mwaa get-environment \
  --name "nextgen-databridge" \
  --query "Environment.WebserverUrl" \
  --output text 2>/dev/null || echo "pending")
AIRFLOW_URL="https://${MWAA_WEBSERVER_URL}"

API_SECRET_KEY="nextgen-databridge-${ENVIRONMENT}-$(openssl rand -hex 16 2>/dev/null || echo 'change-me')"

# ── Apply K8s manifest ────────────────────────────────────────────────────────
step "Applying Kubernetes manifests (image: $IMAGE_TAG)..."
MANIFEST_TMP="$(mktemp)"
trap "rm -f $MANIFEST_TMP" EXIT

sed \
  -e "s|__ACCOUNT_ID__|${ACCOUNT}|g" \
  -e "s|__REGISTRY__|${REGISTRY}|g" \
  -e "s|__IMAGE_TAG__|${IMAGE_TAG}|g" \
  -e "s|__ENVIRONMENT__|${ENVIRONMENT}|g" \
  -e "s|__DUCKDB_BUCKET__|nextgen-databridge-duckdb-store-${ENVIRONMENT}|g" \
  -e "s|__PIPELINE_CONFIGS_BUCKET__|nextgen-databridge-pipeline-configs-${ENVIRONMENT}|g" \
  -e "s|__DATABASE_URL__|${DATABASE_URL}|g" \
  -e "s|__AIRFLOW_URL__|${AIRFLOW_URL}|g" \
  -e "s|__API_SECRET_KEY__|${API_SECRET_KEY}|g" \
  "$SCRIPT_DIR/eks/manifests/platform.yaml" > "$MANIFEST_TMP"

kubectl apply -f "$MANIFEST_TMP"

# ── Wait for rollout ──────────────────────────────────────────────────────────
step "Waiting for deployments to roll out..."
kubectl rollout status deployment/nextgen-databridge-api \
  -n nextgen-databridge --timeout=180s
kubectl rollout status deployment/nextgen-databridge-ui  \
  -n nextgen-databridge --timeout=120s

# ── Upload DAGs ───────────────────────────────────────────────────────────────
step "Uploading DAGs to MWAA..."
MWAA_BUCKET="$MWAA_BUCKET" bash "$SCRIPT_DIR/airflow/deploy_to_mwaa.sh"

# ── Print endpoints ───────────────────────────────────────────────────────────
echo ""
info "Deployment complete. Image: $IMAGE_TAG | Environment: $ENVIRONMENT"
kubectl get svc -n nextgen-databridge \
  -o custom-columns='NAME:.metadata.name,EXTERNAL-IP:.status.loadBalancer.ingress[0].hostname,PORT:.spec.ports[0].port' \
  2>/dev/null || true
echo ""
info "MWAA: $AIRFLOW_URL"
