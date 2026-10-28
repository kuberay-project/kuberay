#!/usr/bin/env bash

set -euo pipefail

raycluster_crd_schema=./schema/raycluster_v1.json

if [ ! -f "$raycluster_crd_schema" ]; then
  echo "CRD schema not found: $raycluster_crd_schema"
  echo 'Please run "pre-commit genearete-crd-schema --all-files" first'
  exit 1
fi

charts=("kuberay-apiserver" "kuberay-operator" "ray-cluster")

for chart in "${charts[@]}"; do
  helm template ./helm-chart/"$chart" | kubeconform --summary -schema-location default -schema-location "$raycluster_crd_schema"
done
