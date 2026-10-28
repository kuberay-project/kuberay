#!/bin/bash
# Run go tests with gotestsum so a JUnit XML is produced for Buildkite Test
# Engine (uploaded by .buildkite/hooks/pre-exit). Falls back to plain
# `go test -v` when gotestsum cannot be installed (e.g. Go module proxy is
# unreachable) so tests still run — that run just isn't uploaded.
#
# Usage: run-gotest.sh <timeout> <go test args...>
#   e.g. run-gotest.sh 30m ./test/e2e
#        run-gotest.sh 30m -run TLS ./test/e2e
# Result files go to KUBERAY_TEST_OUTPUT_DIR if the step exported it,
# otherwise to ./tmp. Deliberately NOT exported as a default: some test
# suites change behavior when KUBERAY_TEST_OUTPUT_DIR is set, so the
# test-visible environment stays exactly as the step defined it.
set -o pipefail

TIMEOUT="$1"; shift
AWK_SCRIPT="$(dirname "$0")/format.awk"
OUT_DIR="${KUBERAY_TEST_OUTPUT_DIR:-$(pwd)/tmp}"
mkdir -p "$OUT_DIR"
GOPATH_BIN="$(go env GOPATH)/bin"
export PATH="$PATH:$GOPATH_BIN"

if go install gotest.tools/gotestsum@v1.13.0 && command -v gotestsum >/dev/null; then
  gotestsum --junitfile "$OUT_DIR/junit.xml" --format standard-verbose \
    -- -timeout "$TIMEOUT" "$@" 2>&1 | awk -f "$AWK_SCRIPT" | tee "$OUT_DIR/gotest.log"
else
  echo "WARNING: gotestsum unavailable; falling back to go test -v (no Test Engine upload for this run)"
  go test -timeout "$TIMEOUT" -v "$@" 2>&1 | awk -f "$AWK_SCRIPT" | tee "$OUT_DIR/gotest.log"
fi
