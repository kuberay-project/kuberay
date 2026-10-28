#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

kubectl apply -f "${SCRIPT_DIR}"/image-preload-daemonset.yaml

kubectl rollout status daemonset ray-image-preloader --timeout 25m
