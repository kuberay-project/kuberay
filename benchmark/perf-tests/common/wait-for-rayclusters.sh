#!/bin/bash

expect_succeeded=$1
echo "waiting for $expect_succeeded RayClusters to be completed successfully"

while true; do
  num_succeeded=$(kubectl get raycluster -l perf-test=ray-cluster -A -o jsonpath='{range .items[*]}{.metadata.name} {.status.state}{"\n"}' | grep -c ready)
  echo "$num_succeeded RayClusters ready..."

  if [[ "$num_succeeded" == "$expect_succeeded" ]]; then
     break;
  fi

  sleep 5
done

echo "$num_succeeded RayClusters ready!"
