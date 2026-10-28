# Image URL to use all building/pushing image targets
GOLANGCI_LINT_VERSION ?= v2.11.4
REPO_ROOT := $(shell dirname ${PWD})
REPO_ROOT_BIN := $(REPO_ROOT)/bin
GOLANGCI_LINT ?= $(REPO_ROOT_BIN)/golangci-lint

DOCKERBUILDER_INSTANCE=historyserver
OUT_DIR=output
BIN_DIR=$(OUT_DIR)/bin
BINARY_NAME=historyserver
BINARY_NAME_COLLECTOR=collector
COLLECTOR_IMG ?= collector:v0.1.0
HISTORYSERVER_IMG ?= historyserver:v0.1.0
# Set GOPROXY to address of your preferred Go module proxy server.
# For example: GOPROXY=https://goproxy.cn,direct make localimage-build
GOPROXY ?= https://proxy.golang.org,direct

# Container Engine for building images.
ENGINE ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
BUILD_TIMESTAMP = $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
COMMIT_SHORT ?= $(shell git rev-parse --short HEAD)
BRANCH ?= $(shell git branch --show-current)
VERSION ?= $(shell git describe --tags --long|awk -F '-' '{print $$1"."$$2"-"$$3""}')

PACKAGE = github.com/ray-project/kuberay/historyserver

GO_LDFLAGS := -extldflags "-static"
# GO_LDFLAGS += -w -s # Drop debugging symbols.
GO_LDFLAGS += -X $(PACKAGE)/pkg.Version=$(VERSION) \
	-X $(PACKAGE)/pkg.CommitID=$(COMMIT_SHORT) \
	-X $(PACKAGE)/pkg.BuildDate=$(BUILD_TIMESTAMP) \
	-X $(PACKAGE)/pkg.Branch=$(BRANCH)
GO_BUILD_FLAGS := -ldflags '$(GO_LDFLAGS)'

GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

.PHONY: all

all: build

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: clean
clean:
	rm -rf $(OUT_DIR)

.PHONY: build
build: fmt vet buildcollector buildhistoryserver

.PHONY: buildcollector
buildcollector:
	@echo ""
	@echo "go build ..."
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -v $(GO_BUILD_FLAGS) -o $(BIN_DIR)/$(BINARY_NAME_COLLECTOR) ./cmd/collector/main.go

.PHONY: buildcollector-race
buildcollector-race:
	@echo ""
	@echo "go build with race detector..."
	CGO_ENABLED=1 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -race -v $(GO_BUILD_FLAGS) -o $(BIN_DIR)/$(BINARY_NAME_COLLECTOR) ./cmd/collector/main.go

.PHONY: buildhistoryserver
buildhistoryserver:
	@echo ""
	@echo "go build ..."
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -v $(GO_BUILD_FLAGS) -o $(BIN_DIR)/$(BINARY_NAME) ./cmd/historyserver/main.go

.PHONY: simplebuild
simplebuild:
	@echo ""
	@echo "go build ..."
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -v $(GO_BUILD_FLAGS) -o $(BIN_DIR)/$(BINARY_NAME)

.PHONY: localimage-build
localimage-build: localimage-collector localimage-historyserver

# Platform for cross-compilation (e.g., linux/amd64, linux/arm64)
PLATFORM ?=

.PHONY: localimage-collector
localimage-collector:
ifdef PLATFORM
	${ENGINE} build --platform $(PLATFORM) -t $(COLLECTOR_IMG) --build-arg GOPROXY=$(GOPROXY) -f Dockerfile.collector .
else
	${ENGINE} build -t $(COLLECTOR_IMG) --build-arg GOPROXY=$(GOPROXY) -f Dockerfile.collector .
endif

.PHONY: localimage-collector-race
localimage-collector-race:
	${ENGINE} build --platform linux/amd64 -t $(COLLECTOR_IMG)-race --build-arg GOPROXY=$(GOPROXY) --build-arg ENABLE_RACE=true -f Dockerfile.collector .

.PHONY: localimage-historyserver
localimage-historyserver:
ifdef PLATFORM
	${ENGINE} build --platform $(PLATFORM) -t $(HISTORYSERVER_IMG) --build-arg GOPROXY=$(GOPROXY) -f Dockerfile.historyserver .
else
	${ENGINE} build -t $(HISTORYSERVER_IMG) --build-arg GOPROXY=$(GOPROXY) -f Dockerfile.historyserver .
endif

.PHONY: buildhistoryserver-multi-arch
buildhistoryserver-multi-arch:
	@echo ""
	@echo "go build multi-arch $(BINARY_NAME) ..."
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v $(GO_BUILD_FLAGS) -o $(BIN_DIR)/$(BINARY_NAME)-amd64 ./cmd/historyserver/main.go
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -v $(GO_BUILD_FLAGS) -o $(BIN_DIR)/$(BINARY_NAME)-arm64 ./cmd/historyserver/main.go

.PHONY: buildcollector-multi-arch
buildcollector-multi-arch:
	@echo ""
	@echo "go build multi-arch $(BINARY_NAME_COLLECTOR) ..."
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v $(GO_BUILD_FLAGS) -o $(BIN_DIR)/$(BINARY_NAME_COLLECTOR)-amd64 ./cmd/collector/main.go
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -v $(GO_BUILD_FLAGS) -o $(BIN_DIR)/$(BINARY_NAME_COLLECTOR)-arm64 ./cmd/collector/main.go

.PHONY: build-multi-arch
build-multi-arch: buildhistoryserver-multi-arch buildcollector-multi-arch ## Build multi-arch binaries for historyserver and collector.

.PHONY: dockerbuilder_instance
dockerbuilder_instance:
ifeq ($(ENGINE),docker)
	${ENGINE} buildx use $(DOCKERBUILDER_INSTANCE) || ${ENGINE} buildx create --name $(DOCKERBUILDER_INSTANCE)
	${ENGINE} buildx use $(DOCKERBUILDER_INSTANCE)
endif

# TODO: Remove this once a fix for https://github.com/golang/go/issues/75031 is released.
# This is a workaround for an issue where `go test -cover` reports errors for packages with no test files.
GOVERSION := $(shell go env GOVERSION)
export GOTOOLCHAIN := $(GOVERSION)+auto

# Run tests
.PHONY: test
test:
	go test -race -v ./pkg/... ./cmd/... -parallel 4

.PHONY: golangci-lint
golangci-lint: ## Download golangci-lint locally if necessary.
	test -s $(GOLANGCI_LINT) || (curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $(REPO_ROOT_BIN)/ $(GOLANGCI_LINT_VERSION))

.PHONY: lint
lint: golangci-lint ## Run golangci-lint against code, using the repo-root .golangci.yml.
	$(GOLANGCI_LINT) run --timeout 10m0s
