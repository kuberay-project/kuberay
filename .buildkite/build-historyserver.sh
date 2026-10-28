#!/bin/bash

# This script is used to build container images of history server components in
# the buildkite test-historyserver-e2e step.

# TODO(jwj): Skip building if starting from ray ci release automation.
make localimage-build &&
kind load docker-image collector:v0.1.0 &&
kind load docker-image historyserver:v0.1.0
