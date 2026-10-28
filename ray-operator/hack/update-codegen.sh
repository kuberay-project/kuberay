#!/bin/bash

# This shell is used to auto generate some useful tools for k8s, such as clientset, lister, informer and so on.
# We don't use this tool to generate deepcopy because kubebuilder (controller-tools) has coverred that part.

set -o errexit
set -o nounset
set -o pipefail
GOPATH=$(go env GOPATH)
export GOPATH

SCRIPT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
ROOT_PKG="github.com/ray-project/kuberay/ray-operator"

cd "${SCRIPT_ROOT}"

# Fallback to vendored location if go list cannot resolve code-generator
CODEGEN_PKG="$(go list -m -f '{{.Dir}}' k8s.io/code-generator 2>/dev/null || true)"
CODEGEN_PKG=${CODEGEN_PKG:-$(
  if [[ -d "${SCRIPT_ROOT}/vendor/k8s.io/code-generator" ]]; then
    echo "${SCRIPT_ROOT}/vendor/k8s.io/code-generator"
  fi
)}

if [[ ! -d "${CODEGEN_PKG}" ]]; then
  echo "${CODEGEN_PKG} is missing. Running 'go mod download'..."
  go mod download
  CODEGEN_PKG=$(go list -m -f "{{.Dir}}" k8s.io/code-generator)
fi

echo ">> Using ${CODEGEN_PKG}"

# shellcheck source=/dev/null
source "${CODEGEN_PKG}/kube_codegen.sh"

# Generating conversion and defaults functions
kube::codegen::gen_helpers \
  --boilerplate "${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
  "${SCRIPT_ROOT}/apis"

kube::codegen::gen_client \
  --boilerplate "${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
  --output-dir "${SCRIPT_ROOT}/pkg/client" \
  --output-pkg "${ROOT_PKG}/pkg/client" \
  --with-watch \
  --with-applyconfig \
  --applyconfig-externals k8s.io/api/core/v1.PodTemplateSpec:k8s.io/client-go/applyconfigurations/core/v1 \
  --one-input-api ray/v1 \
  "${SCRIPT_ROOT}/apis"
