#!/bin/bash
set -eo pipefail

# This script is used to start the operator in the buildkite test-e2e steps.

# When starting from the ray ci release automation, we want to install the latest
# released version from helm as actual users might. Ray ci is also always expected
# to kick off from the release branch so tests should match up accordingly.
#
# NOTE: Do not enable `set -u` (nounset) here. This script is sourced by the
# Buildkite steps, so shell options propagate to the caller; downstream steps
# reference $KUBERAY_TEST_RAY_IMAGE without a default and would fail under -u
# when the variable is not set (e.g. in the non-release path).

if [ "$IS_FROM_RAY_RELEASE_AUTOMATION" = 1 ]; then
    helm repo update
    echo "Installing helm chart with test override values (feature gates enabled as needed)"
    # NOTE: The override file is CI/test-only. It is NOT part of the released chart defaults.
    helm install kuberay-operator kuberay/kuberay-operator -f ../.buildkite/values-kuberay-operator-override.yaml
    KUBERAY_TEST_RAY_IMAGE="rayproject/ray:nightly-extra-py310-cpu"
    export KUBERAY_TEST_RAY_IMAGE
else
    IMG=kuberay/operator:nightly make docker-image
    kind load docker-image kuberay/operator:nightly
    echo "Deploying operator with test overrides (feature gates via test-overrides overlay)"
    IMG=kuberay/operator:nightly make deploy-with-override
fi
