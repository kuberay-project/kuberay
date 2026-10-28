#!/bin/bash

# Best-effort diagnostics for the IPv6-only E2E step. This script is
# called only after a test failure, so every collection command is allowed to
# fail without hiding the original go test exit status.
set +e

artifact_name="${1:-e2e-ipv6-log.tar}"
output_dir="${KUBERAY_TEST_OUTPUT_DIR:-$(pwd)/tmp}"
mkdir -p "${output_dir}"

collect_pod_logs() {
  local selector="$1"
  local destination="$2"

  # `kubectl logs` does not support `-A`. Resolve namespace/name pairs first,
  # then collect every regular and init container from each matching Pod.
  while read -r namespace pod_name; do
    if [[ -z "${namespace}" || -z "${pod_name}" ]]; then
      continue
    fi
    kubectl logs -n "${namespace}" "${pod_name}" \
      --all-containers=true --prefix=true --ignore-errors=true
  done < <(kubectl get pods -A -l "${selector}" \
    -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name' \
    --no-headers) >"${destination}" 2>&1
}

collect_pod_logs "app.kubernetes.io/name=kuberay" "${output_dir}/kuberay-operator.log"
collect_pod_logs "ray.io/cluster" "${output_dir}/ray-head-worker-pods.log"
kubectl get pods -A -o wide >"${output_dir}/pods.txt" 2>&1
kubectl get events -A --sort-by=.lastTimestamp >"${output_dir}/events.txt" 2>&1
kind export logs "${output_dir}/kind-logs" >/dev/null 2>&1

tar -cf "/artifact-mount/${artifact_name}" -C "${output_dir}" .
exit 0
