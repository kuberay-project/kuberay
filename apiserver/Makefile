BUILD_TIME      := $(shell date "+%F %T")
COMMIT_SHA1     := $(shell git rev-parse HEAD)
REPO_ROOT	    := $(shell dirname ${PWD})
REPO_ROOT_BIN	:= $(REPO_ROOT)/bin

# Image URL to use all building/pushing image targets
IMG_REPO ?= quay.io/kuberay/apiserver
IMG_TAG ?= latest
IMG ?= $(IMG_REPO):$(IMG_TAG)

# Allow for additional test flags (-v, etc)
GO_TEST_FLAGS ?=
# Ray docker images to use for end to end tests based upon the architecture
# for arm64 environments (Apple silicon included) pull the architecture specific image
ifeq (arm64, $(shell go env GOARCH))
	E2E_API_SERVER_RAY_IMAGE ?= rayproject/ray:2.46.0-py310-aarch64
else
	E2E_API_SERVER_RAY_IMAGE ?= rayproject/ray:2.46.0-py310
endif
# Kuberay API Server base URL to use in end to end tests
E2E_API_SERVER_URL ?= http://localhost:31888
# Export so `go test` subprocesses pick them up via os.Getenv. Without this,
# the e2e helpers fall back to their hard-coded defaults and disagree with the
# image preloaded into kind by `load-ray-test-image`.
export E2E_API_SERVER_RAY_IMAGE
export E2E_API_SERVER_URL

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (, $(shell go env GOBIN))
	GOBIN = $(shell go env GOPATH)/bin
else
	GOBIN = $(shell go env GOBIN)
endif

# Setting SHELL to bash allows bash commands to be executed by recipes.
# This is a requirement for 'setup-envtest.sh' in the test target.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# Container Engine to be used for building images
ENGINE ?= docker

all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Deployment

.PHONY: start-local-apiserver
start-local-apiserver: operator-image cluster load-operator-image deploy-operator install ## Build and start apiserver from scratch.

# Build and start apiserver and curl from scratch.
.PHONY: start-local-apiserver-e2e
start-local-apiserver-e2e: operator-image cluster load-operator-image deploy-operator install-apiserver-e2e

.PHONY: clear-local-apiserver
clear-local-apiserver: clean-cluster ## Clear local apiserver.

##@ Development

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: fumpt
fumpt: gofumpt ## Run gofmtumpt against code.
	$(GOFUMPT) -l -w .

.PHONY: imports
imports: goimports ## Run goimports against code.
	$(GOIMPORTS) -l -w .

.PHONY: lint
lint: golangci-lint fmt vet fumpt imports ## Run the linter.
	test -s $(GOLANGCI_LINT) || ($(GOLANGCI_LINT) run --timeout=3m --allow-parallel-runners)

build: fmt vet fumpt imports ## Build api server binary.
	go build -o ${REPO_ROOT_BIN}/kuberay-apiserver cmd/main.go

run: fmt vet fumpt imports lint ## Run the api server from your host.
	go run -race cmd/main.go -localSwaggerPath ${REPO_ROOT}/proto/swagger

.PHONY: build-swagger
build-swagger: go-bindata
	cd $(REPO_ROOT) && $(GOBINDATA) --nocompress --pkg swagger -o apiserver/pkg/swagger/datafile.go third_party/swagger-ui/...

.PHONY: generate
generate: mockgen # Generate code using command after //go:generate in each file
	# temporarily prepend local bin/ directory to the system $PATH to run mockgen
	PATH=$(REPO_ROOT_BIN):$$PATH go generate ./...

##@ Testing

# TODO: Remove this once a fix for https://github.com/golang/go/issues/75031 is released.
# This is a workaround for an issue where `go test -cover` reports errors for packages with no test files.
GOVERSION := $(shell go env GOVERSION)
export GOTOOLCHAIN := $(GOVERSION)+auto

.PHONY: test
test: fmt vet fumpt imports generate ## Run all unit tests.
	go test ./pkg/... $(GO_TEST_FLAGS) -race -coverprofile ray-kube-api-server-coverage.out -parallel 4

.PHONY: e2e-test
e2e-test: ## Run end to end tests using a pre-existing cluster.
	go test ./test/e2e/... $(GO_TEST_FLAGS) -timeout 60m -race -coverprofile ray-kube-api-server-e2e-coverage.out -count=1 -parallel 4

# 1. Clean up the cluster if it exists.
# 2. Create a fresh kind cluster with api server and operator installed.
# 3. Load the ray test image into the cluster.
# 4. Run the end to end tests.
# 5. Clean up the cluster.
.PHONY: local-e2e-test
local-e2e-test: clean-cluster start-local-apiserver-e2e load-ray-test-image e2e-test clean-cluster

##@ Testing Setup
KIND_CONFIG ?= hack/kind-cluster-config.yaml
KIND_CLUSTER_NAME ?= ray-api-server-cluster
OPERATOR_IMAGE_TAG ?= latest
SECURITY_IMAGE_TAG ?= latest
.PHONY: cluster
cluster: kind ## Start kind development cluster.
	@if kind get clusters | grep -q "^$(KIND_CLUSTER_NAME)$$"; then \
		echo "Kind cluster $(KIND_CLUSTER_NAME) already exists, please run 'make clean-cluster' to delete the cluster."; \
	else \
		$(KIND) create cluster -n $(KIND_CLUSTER_NAME) --config $(KIND_CONFIG); \
	fi

.PHONY: clean-cluster
clean-cluster: kind ## Delete kind development cluster.
	$(KIND) delete cluster -n $(KIND_CLUSTER_NAME)

.PHONY: load-image
load-image: ## Load the api server image to the kind cluster created with create-kind-cluster.
	$(KIND) load docker-image $(IMG) -n $(KIND_CLUSTER_NAME)

.PHONY: operator-image
operator-image: ## Build the operator image to be loaded in your kind cluster.
	cd ../ray-operator && $(MAKE) docker-image -e IMG=quay.io/kuberay/operator:$(OPERATOR_IMAGE_TAG)


.PHONY: deploy-operator
deploy-operator: ## Deploy operator via helm into the K8s cluster specified in ~/.kube/config.
# Note that you should make your operator image available by either pushing it to an image registry, such as DockerHub or Quay, or by loading the image into the Kubernetes cluster.
# If you are using a Kind cluster for development, you can run `make load-operator-image` to load the newly built image into the Kind cluster.
	helm upgrade --install raycluster ../helm-chart/kuberay-operator --wait \
	--set image.tag=${OPERATOR_IMAGE_TAG} --set image.pullPolicy=IfNotPresent

.PHONY: undeploy-operator
undeploy-operator: ## Undeploy operator via helm from the K8s cluster specified in ~/.kube/config.
	helm uninstall raycluster --wait

.PHONY: load-operator-image
load-operator-image: ## Load the operator image to the kind cluster created with make cluster.
ifneq ($(OPERATOR_IMAGE_TAG), latest)
	$(ENGINE) pull quay.io/kuberay/operator:$(OPERATOR_IMAGE_TAG)
endif
	$(KIND) load docker-image quay.io/kuberay/operator:$(OPERATOR_IMAGE_TAG) -n $(KIND_CLUSTER_NAME)

.PHONY: load-security-proxy-image
load-security-proxy-image: ## Load the security proxy image to the kind cluster created with make cluster.
ifneq ($(SECURITY_IMAGE_TAG), latest)
	$(ENGINE) pull quay.io/kuberay/security-proxy:$(SECURITY_IMAGE_TAG)
endif
	$(KIND) load docker-image quay.io/kuberay/security-proxy:$(SECURITY_IMAGE_TAG) -n $(KIND_CLUSTER_NAME)

.PHONY: load-ray-test-image
load-ray-test-image: ## Load the ray test images.
	$(ENGINE) pull $(E2E_API_SERVER_RAY_IMAGE)
	$(KIND) load docker-image $(E2E_API_SERVER_RAY_IMAGE) -n $(KIND_CLUSTER_NAME)

##@ Docker Build
docker-image: test ## Build image for the api server.
	$(ENGINE) build -t ${IMG} -f Dockerfile ..

docker-push: ## Push image for the api server.
	$(ENGINE) push ${IMG}

docker-multi-arch-build: test ## Build multi arch image, amd64 and arm64 currently.
	$(ENGINE) buildx build --platform linux/amd64,linux/arm64 -t ${IMG} -f Dockerfile.buildx ..

docker-multi-arch-push: test ## Build and Push multi arch image, amd64 and arm64 currently.
	$(ENGINE) buildx build --push --platform linux/amd64,linux/arm64 -t ${IMG} -f Dockerfile.buildx ..

##@ Deployment
.PHONY: install
install: kustomize docker-image load-image  ## Install the kuberay api server without security to the K8s cluster specified in ~/.kube/config.
	cd deploy/local/insecure && $(KUSTOMIZE) edit set image kuberay/apiserver=$(IMG)
	$(KUSTOMIZE) build deploy/local/insecure | kubectl create -f -

# Install apiserver and curl for e2e test
.PHONY: install-apiserver-e2e
install-apiserver-e2e: kustomize docker-image load-image  ## Install the kuberay api server without security to the K8s cluster specified in ~/.kube/config.
	cd deploy/local/e2e && $(KUSTOMIZE) edit set image kuberay/apiserver=$(IMG)
	$(KUSTOMIZE) build deploy/local/e2e | kubectl create -f -

.PHONY: uninstall
uninstall: ## Remove the kuberay api server without security server from the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build deploy/local/insecure | kubectl delete -f -

.PHONY: uninstall-apiserver-e2e
uninstall-apiserver-e2e: ## Remove the kuberay api server for e2e from the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build deploy/local/e2e | kubectl delete -f -

.PHONY: install-secure
install-secure: kustomize docker-image security-proxy-image load-image load-security-proxy-image  ## Install the kuberay api server with security to the K8s cluster specified in ~/.kube/config.
	cd deploy/local/secure && $(KUSTOMIZE) edit set image kuberay/apiserver=$(IMG) && $(KUSTOMIZE) edit set image kuberay/security-proxy=kuberay/security-proxy:latest
	$(KUSTOMIZE) build deploy/local/secure | kubectl create -f -

.PHONY: uninstall-secure
uninstall-secure: ## Remove the kuberay api server with security server from the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build deploy/local/secure | kubectl delete -f -

.PHONY: deploy
deploy: ## Deploy via helm the kuberay api server to the K8s cluster specified in ~/.kube/config.
# Note that you should make your KubeRay APIServer image available by either pushing it to an image registry, such as DockerHub or Quay, or by loading the image into the Kubernetes cluster.
# If you are using a Kind cluster for development, you can run `make load-image` to load the newly built image into the Kind cluster.
	helm upgrade --install kuberay-apiserver ../helm-chart/kuberay-apiserver --wait \
	--set image.repository=${IMG_REPO},image.tag=${IMG_TAG} --set image.pullPolicy=IfNotPresent

.PHONY: undeploy
undeploy: ## Undeploy via helm the kuberay api server to the K8s cluster specified in ~/.kube/config.
	helm uninstall kuberay-apiserver --wait

##@ Development Tools Setup

## Location to install dependencies to
$(REPO_ROOT_BIN):
	mkdir -p $(REPO_ROOT_BIN)

## Tool Binaries
KUSTOMIZE ?= $(REPO_ROOT_BIN)/kustomize
GOIMPORTS ?= $(REPO_ROOT_BIN)/goimports
GOFUMPT ?= $(REPO_ROOT_BIN)/gofumpt
GOLANGCI_LINT ?= $(REPO_ROOT_BIN)/golangci-lint
KIND ?= $(REPO_ROOT_BIN)/kind
GOBINDATA ?= $(REPO_ROOT_BIN)/go-bindata
MOCKGEN = $(REPO_ROOT_BIN)/mockgen


## Tool Versions
KUSTOMIZE_VERSION ?= v5.4.3
GOFUMPT_VERSION ?= v0.3.1
GOIMPORTS_VERSION ?= v0.14.0
GOLANGCI_LINT_VERSION ?= v2.11.4
KIND_VERSION ?= v0.19.0
GOBINDATA_VERSION ?= v4.0.2
MOCKGEN_VERSION ?= v1.6.0

KUSTOMIZE_INSTALL_SCRIPT ?= "https://raw.githubusercontent.com/kubernetes-sigs/kustomize/master/hack/install_kustomize.sh"
.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(REPO_ROOT_BIN)
	test -s $(KUSTOMIZE) || (curl -Ss $(KUSTOMIZE_INSTALL_SCRIPT) | bash -s -- $(subst v,,$(KUSTOMIZE_VERSION)) $(REPO_ROOT_BIN);)

.PHONY: goimports
goimports: $(GOIMPORTS) ## Download goimports locally if necessary
$(GOIMPORTS): $(REPO_ROOT_BIN)
	test -s $(GOIMPORTS) || GOBIN=$(REPO_ROOT_BIN) go install golang.org/x/tools/cmd/goimports@$(GOIMPORTS_VERSION)

.PHONY: gofumpt
gofumpt: $(GOFUMPT) ## Download gofumpt locally if necessary.
$(GOFUMPT): $(REPO_ROOT_BIN)
	test -s $(GOFUMPT) || GOBIN=$(REPO_ROOT_BIN) go install mvdan.cc/gofumpt@$(GOFUMPT_VERSION)

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci_lint locally if necessary.
$(GOLANGCI_LINT): $(REPO_ROOT_BIN)
	test -s $(GOLANGCI_LINT) || (curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $(REPO_ROOT_BIN)/ $(GOLANGCI_LINT_VERSION))

.PHONY: kind
kind: $(KIND) ## Download kind locally if necessary.
$(KIND): $(REPO_ROOT_BIN)
	test -s $(KIND) || GOBIN=$(REPO_ROOT_BIN) go install sigs.k8s.io/kind@$(KIND_VERSION)

.PHONY: go-bindata
go-bindata: $(GOBINDATA) ## Download the go-bindata executable if necessary.
$(GOBINDATA): $(REPO_ROOT_BIN)
	test -s $(GOBINDATA) || GOBIN=$(REPO_ROOT_BIN) go install github.com/kevinburke/go-bindata/v4/...@$(GOBINDATA_VERSION)

.PHONY: mockgen
mockgen: $(MOCKGEN) ## Download mockgen locally if necessary.
$(MOCKGEN): $(REPO_ROOT_BIN)
	test -s $(MOCKGEN) || GOBIN=$(REPO_ROOT_BIN) go install github.com/golang/mock/mockgen@$(MOCKGEN_VERSION)

.PHONY: dev-tools
dev-tools: kind golangci-lint gofumpt kustomize goimports go-bindata mockgen ## Install all development tools.

.PHONY: clean-dev-tools
clean-dev-tools: ## Remove all development tools.
	rm -f $(GOLANGCI_LINT)
	rm -f $(GOFUMPT)
	rm -f $(KUSTOMIZE)
	rm -f $(GOIMPORTS)
	rm -f $(KIND)
	rm -f $(GOBINDATA)
	rm -f $(MOCKGEN)
