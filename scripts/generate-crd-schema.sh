#!/usr/bin/env bash

set -euo pipefail

if [ ! -d "schema" ]; then
  mkdir schema
fi

convert_script=$(realpath scripts/openapi2jsonschema.py)
crd_files=$(find ray-operator/config/crd/bases -name "*.yaml" -exec realpath {} \;)

cd schema

for crd_file in $crd_files; do
  $convert_script "$crd_file"
done
