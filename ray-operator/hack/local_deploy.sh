#!/bin/bash

set -euxo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/../.." # hack/ -> ray-operator/ -> kuberay/
HELM_CHART_PATH="${PROJECT_ROOT}/helm-chart/kuberay-operator"
RAY_OPERATOR_PATH="${PROJECT_ROOT}/ray-operator"

# --- Configuration Variables ---
IMAGE_TAG="${IMAGE_TAG:=kuberay-dev}"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:=kuberay-dev}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:=kindest/node:v1.29.0}"

delete_kind_cluster() {
  echo "--- Deleting Kind Cluster ---"
  if kind get clusters | grep -q "${KIND_CLUSTER_NAME}"; then
    echo "Kind cluster '${KIND_CLUSTER_NAME}' found. Deleting it..."
    kind delete cluster --name "${KIND_CLUSTER_NAME}"
  else
    echo "Kind cluster '${KIND_CLUSTER_NAME}' not found. Skipping deletion."
  fi
}

create_kind_cluster() {
  echo "--- Creating Kind Cluster ---"
  echo "Creating Kind cluster '${KIND_CLUSTER_NAME}' with image '${KIND_NODE_IMAGE}'..."
  kind create cluster --name "${KIND_CLUSTER_NAME}" --image="${KIND_NODE_IMAGE}"
  echo "Kind cluster nodes are ready."
}

# Parsing for arguements
SHOW_LOGS=false
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -l|--logs)
      SHOW_LOGS=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

echo "--- Ensuring Clean Kind Cluster ---"
delete_kind_cluster
create_kind_cluster

echo "--- Building Docker Image ---"
FULL_IMAGE_NAME="kuberay-operator:${IMAGE_TAG}"
echo "Building image: ${FULL_IMAGE_NAME}"

echo "Running make docker-build from ray-operator path: ${RAY_OPERATOR_PATH}"
make -C "${RAY_OPERATOR_PATH}" docker-build IMG="${FULL_IMAGE_NAME}"

echo "--- Loading Image into Kind Cluster ---"
kind load docker-image "${FULL_IMAGE_NAME}" --name "${KIND_CLUSTER_NAME}"

echo "Running make sync from ray-operator path: ${RAY_OPERATOR_PATH}"
make -C "${RAY_OPERATOR_PATH}" sync

echo "--- Installing KubeRay Operator via Helm Chart ---"
echo "Installing new Helm release: kuberay-operator"
echo "Helm chart path: ${HELM_CHART_PATH}"
helm install "kuberay-operator" "${HELM_CHART_PATH}" \
  --namespace "default" \
  --set "image.repository=kuberay-operator" \
  --set "image.tag=${IMAGE_TAG}"

echo "--- Waiting for Deployment Rollout ---"
kubectl -n default rollout status deployment kuberay-operator --timeout=5m

# Check the logs
if [ "$SHOW_LOGS" = true ]; then
  echo "--- Streaming Controller Logs (Ctrl+C to stop) ---"
  kubectl -n default logs -f deployment/kuberay-operator
fi
