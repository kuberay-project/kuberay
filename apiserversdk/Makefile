## Local bin directory
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

REPO_ROOT := $(shell dirname ${PWD})
REPO_ROOT_BIN := $(REPO_ROOT)/bin
GOLANGCI_LINT ?= $(REPO_ROOT_BIN)/golangci-lint
GOLANGCI_LINT_VERSION ?= v2.11.4

# Setting SHELL to bash allows bash commands to be executed by recipes.
# This is a requirement for 'setup-envtest.sh' in the test target.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

fmt: ## Run go fmt against code.
	go fmt ./...

vet: ## Run go vet against code.
	go vet ./...

.PHONY: golangci-lint
golangci-lint: ## Download golangci_lint locally if necessary.
	test -s $(GOLANGCI_LINT) || (curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $(REPO_ROOT_BIN)/ $(GOLANGCI_LINT_VERSION))

.PHONY: lint
lint: golangci-lint fmt vet ## Run the linter.
	test -s $(GOLANGCI_LINT) || ($(GOLANGCI_LINT) run --timeout=3m --allow-parallel-runners)

# TODO: Remove this once a fix for https://github.com/golang/go/issues/75031 is released.
# This is a workaround for an issue where `go test -cover` reports errors for packages with no test files.
GOVERSION := $(shell go env GOVERSION)
export GOTOOLCHAIN := $(GOVERSION)+auto

test: WHAT ?= $(shell go list ./... | grep -v /test/)
test: ENVTEST_K8S_VERSION ?= 1.37.0
test: envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test -v $(WHAT) -coverprofile cover.out

##@ Binaries

ENVTEST = $(LOCALBIN)/setup-envtest
$(ENVTEST): $(LOCALBIN)
envtest: $(ENVTEST) ## Download envtest locally if necessary.
	test -s $(ENVTEST) || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.25

.PHONY: clean
clean:
	rm -rf $(LOCALBIN)
