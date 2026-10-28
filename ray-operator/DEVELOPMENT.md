<!-- markdownlint-disable MD013 -->
# Development

This section walks through how to build and test the operator in a running Kubernetes cluster.

## Requirements

| software | version  |                                                                link |
|:---------|:--------:|--------------------------------------------------------------------:|
| kubectl  | v1.23.0+ | [download](https://kubernetes.io/docs/tasks/tools/install-kubectl/) |
| go       |  v1.26   |                                  [download](https://golang.org/dl/) |
| docker   |  19.03+  |                        [download](https://docs.docker.com/install/) |

Alternatively, you can use podman (version 4.5+) instead of docker. See [podman.io](https://podman.io/getting-started/installation) for installation instructions. The Makefile allows you to specify the container engine to use via the `ENGINE` variable. For example, to use podman, you can run `ENGINE=podman make docker-build`.

The instructions assume you have access to a running Kubernetes cluster via `kubectl`. If you want to test locally, consider using [Kind](https://kind.sigs.k8s.io/) or [Minikube](https://kubernetes.io/docs/tasks/tools/install-minikube/).

### Setup on Kind

For local development, we recommend using [Kind](https://kind.sigs.k8s.io/) to create a Kubernetes cluster.

### Use go v1.26

Currently, KubeRay uses go v1.26 for development.

```bash
go install golang.org/dl/go1.26.0@latest
go1.26.0 download
export GOROOT=$(go1.26.0 env GOROOT)
export PATH="$GOROOT/bin:$PATH"
```

## IDE Setup (VS Code)

* Step 1: Install the [VS Code Go extension](https://marketplace.visualstudio.com/items?itemName=golang.go).
* Step 2: Import the KubeRay workspace configuration by using the file `kuberay.code-workspace` in the root of the KubeRay git repo:
  * "File" -> "Open Workspace from File" -> "kuberay.code-workspace"

Setting up workspace configuration is required because KubeRay contains multiple Go modules. See the [VS Code Go documentation](https://github.com/golang/vscode-go/blob/master/README.md#setting-up-your-workspace) for details.

### IMPORTANT: Change your working directory to `ray-operator`

All the following guidance require you to switch your working directory to the `ray-operator`.

```bash
cd ray-operator
```

## Cleanup local binaries, such as controller-gen and kustomize

To keep consistent results of code generation and testing, you need to remove outdated binaries installed by the Makefile.

```bash
rm -rf bin
# or
make clean
```

## End-to-end local development process on Kind

### Run the operator inside the cluster

```bash
# Step 1: Create a Kind cluster
kind create cluster --image=kindest/node:v1.29.0

# Step 2: Modify KubeRay source code
# For example, add a log by adding setupLog.Info("Hello KubeRay") in the function `main` in `main.go`.


# Step 3: Build an image
#         This command will copy the source code directory into the image, and build it.
# Command: IMG={IMG_REPO}:{IMG_TAG} make docker-build
IMG=kuberay/operator:nightly make docker-build

# To skip Go project compilation, run the following command instead:
# IMG=kuberay/operator:nightly make docker-image

# Step 4: Load the custom KubeRay image into the Kind cluster.
# Command: kind load docker-image {IMG_REPO}:{IMG_TAG}
kind load docker-image kuberay/operator:nightly

# Step 5: Keep consistency
# If you update RBAC or CRD, you need to synchronize them.
# See the section "Consistency check" for more information.

# Step 6: Install KubeRay operator with the custom image via local Helm chart
# (Path: helm-chart/kuberay-operator)
# Command: helm install kuberay-operator --set image.repository={IMG_REPO} --set image.tag={IMG_TAG} ../helm-chart/kuberay-operator
helm install kuberay-operator --set image.repository=kuberay/operator --set image.tag=nightly ../helm-chart/kuberay-operator

# Step 7: Check the logs
kubectl logs deployments/kuberay-operator
```

* Replace `{IMG_REPO}` and `{IMG_TAG}` with your own repository and tag.
* The command `make docker-build` (Step 3) will also run `make build` (Go project compilation).
* Step 6 also installs the custom resource definitions (CRDs) used by the KubeRay operator.

#### Using Local Deployment Script

You can also run the `local_deploy.sh` bash script (located in `ray-operator/hack`) which runs the steps shown above, but deletes and recreates the kind cluster each run for consistency during repeated development.

There are configuable variables in the script, the defaults are shown below:

```bash
IMAGE_TAG="kuberay-dev"
KIND_CLUSTER_NAME="kuberay-dev"
KIND_NODE_IMAGE="kindest/node:v1.29.0"
```

Additionally, you can run the script with a `-l` or `--logs` to stream the logs of the ray operator to the terminal after installation.

### Run the operator outside the cluster

This step requires you to switch your working directory to the kuberay project root. If
you are in `ray-operator`, do:

```bash
cd ..
```

> Note: Running the operator outside the cluster allows you to debug the operator using your IDE. For example, you can set breakpoints in the code and inspect the state of the operator.

```bash
# Step 1: Create a Kind cluster
kind create cluster --image=kindest/node:v1.29.0

# Step 2: Install CRDs
make -C ray-operator install

# Step 3: Compile the source code
make -C ray-operator build

# Step 4: Run the KubeRay operator
./ray-operator/bin/manager -leader-election-namespace default -use-kubernetes-proxy
```

## Tests

### Kind of tests

* Unit tests: These are run locally and do not require a Kubernetes cluster. The filenames follow the pattern `*_controller_unit_test.go` and use the standard Go testing framework.
* Env tests: These use the `envtest` package to start a local Kubernetes API server and etcd, without kubelet, controller-manager, or other components. You don't need to start a Kubernetes cluster beforehand. They are written using the Ginkgo framework. Since only the KubeRay operator is present, resource states like pod status must be manually updated. Filenames follow the pattern `*_controller_test.go`. See [Kubebuilder Envtest](https://book.kubebuilder.io/reference/envtest.html) for more details.
* E2E tests: These run on a real Kubernetes cluster to test the KubeRay operator with actual resources. A running Kubernetes cluster and an installed KubeRay operator are required. These tests are located in the `test/` directory and use the standard Go testing framework.

### Running the tests

The unit tests and env tests can be run by executing the following command:

```bash
make test
```

Example output:

```console
✗ make test
...
go fmt ./...
go vet ./...
...
setting up env vars
?    github.com/ray-project/kuberay/ray-operator [no test files]
ok   github.com/ray-project/kuberay/ray-operator/api/v1alpha1 0.023s coverage: 0.9% of statements
ok   github.com/ray-project/kuberay/ray-operator/controllers 9.587s coverage: 66.8% of statements
ok   github.com/ray-project/kuberay/ray-operator/controllers/common 0.016s coverage: 75.6% of statements
ok   github.com/ray-project/kuberay/ray-operator/controllers/utils 0.015s coverage: 31.4% of statements
```

The e2e tests can be run by executing the following command:

```bash
# Reinstall the kuberay-operator to make sure it uses the latest nightly image you just built.
helm uninstall kuberay-operator
helm install kuberay-operator --set image.repository=kuberay/operator --set image.tag=nightly ../helm-chart/kuberay-operator
make test-e2e
```

Example output:

```asciidoc
go test -timeout 30m -v ./test/e2e
=== RUN   TestRayJobWithClusterSelector
    rayjob_cluster_selector_test.go:41: Created ConfigMap test-ns-jtlbd/jobs successfully
    rayjob_cluster_selector_test.go:159: Created RayCluster test-ns-jtlbd/raycluster successfully
    rayjob_cluster_selector_test.go:161: Waiting for RayCluster test-ns-jtlbd/raycluster to become ready
=== RUN   TestRayJobWithClusterSelector/Successful_RayJob
=== PAUSE TestRayJobWithClusterSelector/Successful_RayJob
=== RUN   TestRayJobWithClusterSelector/Failing_RayJob
=== PAUSE TestRayJobWithClusterSelector/Failing_RayJob
=== CONT  TestRayJobWithClusterSelector/Successful_RayJob
=== CONT  TestRayJobWithClusterSelector/Failing_RayJob
=== NAME  TestRayJobWithClusterSelector
    rayjob_cluster_selector_test.go:213: Created RayJob test-ns-jtlbd/counter successfully
    rayjob_cluster_selector_test.go:215: Waiting for RayJob test-ns-jtlbd/counter to complete
    rayjob_cluster_selector_test.go:268: Created RayJob test-ns-jtlbd/fail successfully
    rayjob_cluster_selector_test.go:270: Waiting for RayJob test-ns-jtlbd/fail to complete
    test.go:118: Retrieving Pod Container test-ns-jtlbd/counter-zs9s8/ray-job-submitter logs
    test.go:106: Creating ephemeral output directory as KUBERAY_TEST_OUTPUT_DIR env variable is unset
    test.go:109: Output directory has been created at: /var/folders/mx/kpgdgdqd5j56ynylglgn0nvh0000gn/T/TestRayJobWithClusterSelector2055000419/001
    test.go:118: Retrieving Pod Container test-ns-jtlbd/fail-gdws6/ray-job-submitter logs
    test.go:118: Retrieving Pod Container test-ns-jtlbd/raycluster-head-gnhlw/ray-head logs
    test.go:118: Retrieving Pod Container test-ns-jtlbd/raycluster-worker-small-group-9dffx/ray-worker logs
--- PASS: TestRayJobWithClusterSelector (12.19s)
    --- PASS: TestRayJobWithClusterSelector/Failing_RayJob (16.11s)
    --- PASS: TestRayJobWithClusterSelector/Successful_RayJob (19.14s)
PASS
ok      github.com/ray-project/kuberay/ray-operator/test/e2e    32.066s
```

Note you can set the `KUBERAY_TEST_OUTPUT_DIR` environment to specify the test output directory.
If not set, it defaults to a temporary directory that's removed once the tests execution completes.

Alternatively, You can run the e2e test(s) from your preferred IDE / debugger.

### Tips

* For Ginkgo tests, you can use [focused specs](https://onsi.github.io/ginkgo/#focused-specs) to run a specific test for debugging purpose.

## Manually test new image in running cluster

Build and apply the CRD:

```bash
make install
```

Deploy the manifests and controller

```bash
helm uninstall kuberay-operator
helm install kuberay-operator --set image.repository=kuberay/operator --set image.tag=nightly ../helm-chart/kuberay-operator
```

> Note: remember to replace with your own image

## pre-commit hooks

See [main development documentation][main-dev-doc].

## CI/CD

### Helm chart linter

We have [chart lint tests](https://github.com/ray-project/kuberay/blob/master/.github/workflows/helm.yaml) with Helm v3.4.1 and Helm v3.9.4 on GitHub Actions. We also provide a script to execute the lint tests on your laptop. If you cannot reproduce the errors on GitHub Actions, the possible reason is the different version of Helm. Issue [#537](https://github.com/ray-project/kuberay/issues/537) is an example that some errors only happen in old helm versions.

Run tests with docker

```bash
./helm-chart/script/chart-test.sh
```

Run tests on your local environment

* Step1: Install `ct` (chart-testing) and related dependencies. See <https://github.com/helm/chart-testing> for more details.
* Step2: `./helm-chart/script/chart-test.sh local`

### Generating API Reference

We use [elastic/crd-ref-docs](https://github.com/elastic/crd-ref-docs) to generate API reference for CRDs of KubeRay. The configuration file of `crd-ref-docs` is located at `hack/config.yaml`. Please refer to the documentation for more details.

Generate API refernece:

```bash
make api-docs
```

The file will be generated at `docs/reference/api.md` as configured.

### Consistency check

We have several [consistency checks](https://github.com/ray-project/kuberay/blob/master/.github/workflows/consistency-check.yaml) on GitHub Actions. There are several files which need synchronization.

1. `ray-operator/apis/ray/v1alpha1/*_types.go` should be synchronized with the CRD YAML files (`ray-operator/config/crd/bases/`)
2. `ray-operator/apis/ray/v1alpha1/*_types.go` should be synchronized with generated API (`ray-operator/pkg/client`)
3. `ray-operator/apis/ray/v1alpha1/*_types.go` should be synchronized with generated API reference (`docs/reference/api.md`)
4. CRD YAML files in `ray-operator/config/crd/bases/` and `helm-chart/kuberay-operator/crds/` should be the same.
5. Kubebuilder markers in `ray-operator/controllers/ray/*_controller.go` should be synchronized with RBAC YAML files in `ray-operator/config/rbac`.
6. RBAC YAML files in `helm-chart/kuberay-operator/templates` and `ray-operator/config/rbac` should be synchronized. **Currently, we need to synchronize this manually.** See [#631](https://github.com/ray-project/kuberay/pull/631) as an example.
7. `multiple_namespaces_role.yaml` and `multiple_namespaces_rolebinding.yaml` should be synchronized with `role.yaml` and `rolebinding.yaml` in the `helm-chart/kuberay-operator/templates` directory. The only difference is that the former creates namespaced RBAC resources, while the latter creates cluster-scoped RBAC resources.

```bash
# Synchronize consistency 1 and 5:
make manifests

# Synchronize consistency 2:
./hack/update-codegen.sh

# Synchronize consistency 3:
make api-docs

# Synchronize consistency 4:
make helm

# Synchronize 1, 2, 3, 4 and 5 in one command
# [Note]: Currently, we need to synchronize consistency 5 and 6 manually.
make sync

# Reproduce CI error for job "helm-chart-verify-rbac" (consistency 5)
python3 ../scripts/rbac-check.py
```

### Building Multi architecture images locally

Most image repositories support multiple architectures container images. When running an image from a device, the docker client automatically pulls the correct image with a matching architecture. The easiest way to build multi-arch images is to utilize Docker `Buildx` plug-in which allows easily building multi-arch images using Qemu emulation from a single machine. Buildx plugin is readily available when you install the [Docker Desktop](https://docs.docker.com/desktop/) on your machine.
Verify Buildx installation and make sure it does not return error

```console
docker buildx version
```

Verify the builder instance has a default(with *) DRIVER/ENDPOINT starting with `docker-container` by running:

```console
docker buildx ls
```

You may see something:

```console
NAME/NODE    DRIVER/ENDPOINT             STATUS  BUILDKIT             PLATFORMS
sad_brown *  docker-container
  sad_brown0 unix:///var/run/docker.sock running v0.12.4              linux/amd64, linux/amd64/v2, linux/amd64/v3, linux/amd64/v4, linux/arm64, linux/riscv64, linux/ppc64le, linux/s390x, linux/386, linux/mips64le, linux/mips64, linux/arm/v7, linux/arm/v6
default      docker
  default    default                     running v0.11.7+d3e6c1360f6e linux/amd64, linux/amd64/v2, linux/amd64/v3, linux/amd64/v4, linux/386, linux/arm64, linux/riscv64, linux/ppc64le, linux/s390x, linux/mips64le, linux/mips64, linux/arm/v7, linux/arm/v6
```

If not, create the instance by running:

```console
docker buildx create --use --bootstrap
```

Run the following `docker buildx build` command to build and push linux/arm64 and linux/amd64 images(manifests) in a single command:

```console
cd ray-operator
docker buildx build --tag quay.io/<my org>/operator:latest --tag docker.io/<my org>/operator:latest --platform linux/amd64,linux/arm64 --push --provenance=false .
```

* --platform is a comma separated list of targeted platforms to build.
* --tag is a remote repo_name:tag to push.
* --push/--load optionally Push to remote registry or Load into local docker.
* Some registry such as Quay.io dashboard displays attestation manifests as unknown platforms. Setting --provenance=false to avoid this issue.

[main-dev-doc]: ../docs/development/development.md#pre-commit-hooks

## Kubernetes Workload-Aware Scheduling v1alpha3

The Kubernetes Workload-Aware Scheduling (WAS) batch scheduler enables gang scheduling of RayClusters through the `scheduling.k8s.io/v1alpha3` Workload and PodGroup APIs, using the Kubernetes default scheduler. It is enabled with the `KubernetesWAS` feature gate (`--feature-gates=KubernetesWAS=true`); do not also set `--batch-scheduler` (the two are mutually exclusive). Each RayCluster opts in with the `ray.io/gang-scheduling-enabled: "true"` label.

For user-facing documentation, see the [Kubernetes WAS guide](../docs/guidance/kubernetes-was.md).

### Testing locally with Kind

> **NOTE:** Change your working directory to `ray-operator` before running the commands below.
>
> ```bash
> cd ray-operator
> ```

Kubernetes WAS v1alpha3 requires Kubernetes 1.37+ with `GenericWorkload` enabled on the API server, controller manager, and scheduler, and both `scheduling.k8s.io/v1beta1` and `scheduling.k8s.io/v1alpha3` served by the API server. KubeRay uses v1alpha3, while kube-scheduler's GenericWorkload integration requires v1beta1. The kind config uses the published `kindest/node:v1.37.0` image:

Kind v0.32.0 or newer is required.

```bash
# Create the cluster with the required feature gates and alpha API.
kind create cluster --name kubernetes-was-v1alpha3 \
  --config hack/kind-config-kubernetes-was-v1alpha3.yml

# Build and load the operator image.
make docker-image IMG=kuberay/operator:latest
kind load docker-image kuberay/operator:latest --name kubernetes-was-v1alpha3

# Deploy with the Kubernetes WAS batch scheduler enabled (via the KubernetesWAS feature gate).
make deploy-kubernetes-was-v1alpha3 IMG=kuberay/operator:latest
```

### Running tests

```bash
# Unit tests.
make test WHAT='./apis/config/v1alpha1 ./controllers/ray/batchscheduler/... ./controllers/ray'

# E2E tests. Requires the kind cluster above with the operator deployed.
make test-e2e-kubernetes-was-v1alpha3
```
