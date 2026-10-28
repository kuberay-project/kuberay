#!/bin/bash

expect_succeeded=$1
echo "waiting for $expect_succeeded PyTorch RayJobs to be completed successfully"

while true; do
  num_succeeded=$(kubectl get rayjob -A -l perf-test=rayjob-pytorch-mnist  -o jsonpath='{range .items[*]}{.metadata.name} {.status.jobStatus}{"\n"}' | grep -c SUCCEEDED)
  echo "$num_succeeded RayJobs completed..."

  if [[ "$num_succeeded" == "$expect_succeeded" ]]; then
     break;
  fi

  echo "printing RayJobs with Failed deployment status"
  kubectl get rayjob -A -l perf-test=rayjob-pytorch-mnist -o jsonpath='{range .items[*]}{.metadata.name} {.status.jobDeploymentStatus}{"\n"}' | grep Failed

  echo "printing RayJobs with FAILED job status"
  kubectl get rayjob -A -l perf-test=rayjob-pytorch-mnist -o jsonpath='{range .items[*]}{.metadata.name} {.status.jobStatus}{"\n"}' | grep FAILED

  sleep 30
done

echo "waiting for $expect_succeeded Ray Data RayJobs to be completed successfully"

while true; do
  num_succeeded=$(kubectl get rayjob -A -l perf-test=ray-data-image-resize -o jsonpath='{range .items[*]}{.metadata.name} {.status.jobStatus}{"\n"}' | grep -c SUCCEEDED)
  echo "$num_succeeded RayJobs completed..."

  if [[ "$num_succeeded" == "$expect_succeeded" ]]; then
     break;
  fi

  echo "printing RayJobs with Failed deployment status"
  kubectl get rayjob -A -l perf-test=ray-data-image-resize -o jsonpath='{range .items[*]}{.metadata.name} {.status.jobDeploymentStatus}{"\n"}' | grep Failed

  echo "printing RayJobs with FAILED job status"
  kubectl get rayjob -A -l perf-test=ray-data-image-resize -o jsonpath='{range .items[*]}{.metadata.name} {.status.jobStatus}{"\n"}' | grep FAILED

  sleep 30
done

echo "$num_succeeded RayJobs completed!"
