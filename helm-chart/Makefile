.SILENT:

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

# Absolute path to Makefile (trim trailing slash).
WORKDIR ?= $(patsubst %/,%,$(dir $(abspath $(lastword $(MAKEFILE_LIST)))))

# Absolute paths to Helm charts.
KUBERAY_OPERATOR_CHART_PATH ?= $(WORKDIR)/kuberay-operator
KUBERAY_APISERVER_CHART_PATH ?= $(WORKDIR)/kuberay-apiserver
RAY_CLUSTER_CHART_PATH ?= $(WORKDIR)/ray-cluster

# Tool versions.
HELM_VERSION ?= v3.17.3
HELM_UNITTEST_VERSION ?= 0.8.1
HELM_DOCS_VERSION ?= v1.14.2

# Tool binaries.
LOCALBIN ?= $(WORKDIR)/bin
HELM ?= $(LOCALBIN)/helm-$(HELM_VERSION)
HELM_DOCS ?= $(LOCALBIN)/helm-docs-$(HELM_DOCS_VERSION)

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-30s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Helm

.PHONY: helm-unittest
helm-unittest: helm-unittest-plugin ## Run Helm chart unittests.
	$(HELM) unittest $(KUBERAY_OPERATOR_CHART_PATH) --file "tests/**/*_test.yaml" --strict --debug
	$(HELM) unittest $(KUBERAY_APISERVER_CHART_PATH) --file "tests/**/*_test.yaml" --strict --debug
	$(HELM) unittest $(RAY_CLUSTER_CHART_PATH) --file "tests/**/*_test.yaml" --strict --debug

.PHONY: helm-lint
helm-lint: ## Run Helm chart lint test.
	docker run --rm --workdir /workspace --volume $(WORKDIR):/workspace quay.io/helmpack/chart-testing:v3.12.0 \
	ct lint --chart-dirs=. --charts=kuberay-operator,kuberay-apiserver,ray-cluster --target-branch=master --validate-maintainers=false

.PHONY: helm-docs
helm-docs: helm-docs-plugin ## Generates markdown documentation for Helm charts from requirements and values files.
	$(HELM_DOCS) --chart-search-root=$(WORKDIR) --chart-to-generate=$(KUBERAY_OPERATOR_CHART_PATH) --sort-values-order=file
	$(HELM_DOCS) --chart-search-root=$(WORKDIR) --chart-to-generate=$(RAY_CLUSTER_CHART_PATH) --sort-values-order=file
	$(HELM_DOCS) --chart-search-root=$(WORKDIR) --chart-to-generate=$(KUBERAY_APISERVER_CHART_PATH) --sort-values-order=file

##@ Dependencies

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

.PHONY: helm
helm: $(HELM) ## Download helm locally if necessary.
$(HELM): $(LOCALBIN)
	$(call go-install-tool,$(HELM),helm.sh/helm/v3/cmd/helm,$(HELM_VERSION))

.PHONY: helm-unittest-plugin
helm-unittest-plugin: helm ## Download helm unittest plugin locally if necessary.
	if [ -z "$(shell $(HELM) plugin list | grep unittest)" ]; then \
		echo "Installing helm unittest plugin"; \
		$(HELM) plugin install https://github.com/helm-unittest/helm-unittest.git --version $(HELM_UNITTEST_VERSION); \
	fi

.PHONY: helm-docs-plugin
helm-docs-plugin: $(HELM_DOCS) ## Download helm-docs plugin locally if necessary.
$(HELM_DOCS): $(LOCALBIN)
	$(call go-install-tool,$(HELM_DOCS),github.com/norwoodj/helm-docs/cmd/helm-docs,$(HELM_DOCS_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary (ideally with version)
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f $(1) ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv "$$(echo "$(1)" | sed "s/-$(3)$$//")" $(1) ;\
}
endef
