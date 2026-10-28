<!-- markdownlint-disable MD013 -->
# Kuberay API Server User Guide

This guide covers the purpose, requirements, and deployment of the Kuberay API Server.

## Requirements

| Software | Version  |                                                                Link |
|:---------|:--------:|--------------------------------------------------------------------:|
| kubectl  | v1.18.3+ | [Download](https://kubernetes.io/docs/tasks/tools/install-kubectl/) |
| Go       |  v1.26+  |                                  [Download](https://golang.org/dl/) |
| Docker   |  19.03+  |                        [Download](https://docs.docker.com/install/) |
| GNU Make |  3.81+   |                                                                     |
| curl     |  7.88+   |                                                                     |
| helm     | v3.12.1  |                      [Install](https://helm.sh/docs/intro/install/) |

### Optional Development Tools

These tools are downloaded and installed when they are needed. The directory of the download is `../bin`.
Typing `make dev-tools` will download and install all of them. The `make clean-dev-tools` command can be used to remove all the tools from the filesystem.

| Software      | Version  |                                                                    Link |
| :-------      | :------: | -----------------------------------------------------------------------:|
| kind          | v0.19.0  | [Install](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) |
| golangci-lint | v2.11.4  | [Install](https://golangci-lint.run/docs/welcome/install/local/)        |
| kustomize     | v3.8.7   | [install](https://kubectl.docs.kubernetes.io/installation/kustomize/)   |
| gofumpt       | v0.3.1   | To install `go install mvdan.cc/gofumpt@v0.3.1`                         |
| goimports     | latest   | To install `go install golang.org/x/tools/cmd/goimports@latest`         |
| go-bindata    | v4.0.2   | To install `github.com/kevinburke/go-bindata/v4/...@v4.0.2`             |

## Purpose

The Kuberay API Server is designed to simplify the lifecycle management of Ray clusters for users who may not be well-versed in Kubernetes. It provides a RESTful web service to manage Ray cluster Kubernetes resources.

## Build and Deployment

The backend service can be deployed locally or within a Kubernetes cluster. The HTTP service listens on port 8888, the RPC port on 8887.

### Pre-requisites

Ensure that the admin Kubernetes configuration file is located at `~/.kube/config`. As a convenience, there are two makefile targets provided to help you manage a local kind cluster:

* `make cluster` -- creates a 3 node cluster (1 control plane 2 worker) named ray-api-server-cluster
* `make clean-cluster` -- deletes the cluster created with the `cluster` target

### Local Development

#### Build

```bash
#To build the executable
make build

#To start the executable build above
../bin/kuberay-apiserver
```

#### Test

```bash
#To run the unit tests
make test
```

To generate mock files for interfaces, add a `//go:generate` comment in the target Go file:

```go
//go:generate mockgen -source=your_file.go -destination=your_file_mock.go -package=your_package
```

Then run:

```sh
make generate
```

This will create or update mock files.

#### End to End Testing

There are two `make` targets provided to execute the end to end test (integration between Kuberay API server and Kuberay Operator):

* `make e2e-test` executes all the tests defined in the [test/e2e package](./test/e2e/). It uses the cluster defined in `~/.kube/config` to submit the workloads. Please make sure you have done the following before running `make e2e-test`:
    1. Install the KubeRay Operator into the cluster by running `make operator-image load-operator-image deploy-operator`
    2. Install the KubeRay API server into the cluster by running `make install`
    3. Verify the setup with a smoke test `curl -I localhost:31888/healthz`, and you should see a response like:

```bash
> curl -I localhost:31888/healthz
HTTP/1.1 200 OK
Date: Tue, 29 Apr 2025 12:36:05 GMT
```

> [!NOTE]
> Please rerun `make uninstall && make install` whenever you make changes to the `apiserver/` codebase to ensure the KubeRay API server is rebuilt and redeployed properly. Alternatively, you can simply run `make start-local-apiserver` to spin up the API server within the kind cluster in one single command.

* `make local-e2e-test` creates a local kind cluster, builds the Kuberay operator and API server images from the current branch and deploys the operator and API server into the kind cluster. It shuts down the kind cluster upon successful execution of the end to end test. If the tests fail the cluster will be left running and will have to manually be shutdown by executing the `make clean-cluster`

The `e2e` test targets use two variables to control what version of Ray images to use in the end to end tests:

* `E2E_API_SERVER_RAY_IMAGE` -- for the ray docker image. Currently set to `rayproject/ray:2.46.0-py310`. On Apple silicon or arm64 development machines the `-aarch64` suffix is added to the image.
* `E2E_API_SERVER_URL` -- for the base URL of the deployed KubeRay API server. The default value is: `http://localhost:31888`

The end to end test targets share the usage of the `GO_TEST_FLAGS`. Overriding the make file variable with a `-v` option allows for both unit and end to end tests to print any output / debug messages. By default, only if there's a test failure those messages are shown.

The default values of the variables can be overridden using the `-e` make command line arguments.

Examples:

```bash
# To run end to end test using default cluster
make e2e-test

# To run end to end test in fresh cluster.
# Please note that:
# * the cluster created for this test is the same as the cluster created by make cluster.
# * if the end to end tests fail the cluster will still be up and will have to be explicitly shutdown by executing make clean-cluster
make local-e2e-test
```

#### Swagger UI updates

To update the swagger ui files deployed with the Kuberay API server, you'll need to:

* Manually run the [hack/update-swagger-ui.bash](hack/update-swagger-ui.bash) script. The script downloads the swagger ui release and copies the downloaded files
to the [../third_party/swagger-ui](../third_party/swagger-ui/) directory. It copies the [swagger-initializer.js](../third_party/swagger-ui/swagger-initializer.js)
to [swagger-initializer.js.backup](../third_party/swagger-ui/swagger-initializer.js.backup).
* Update the contents of the [swagger-initializer.js](../third_party/swagger-ui/swagger-initializer.js) to set the URLs for for the individual swagger docs.
* Execute `make build-swagger` target to update the contents of the [datafile.go](pkg/swagger/datafile.go) file. This will package the content of the [swagger-ui](../third_party/swagger-ui/) directory for serving by the api server (see [func serveSwaggerUI(mux *http.ServeMux)](https://github.com/ray-project/kuberay/blob/f1067378bc99987f3eba1e5b12b4cc797465336d/apiserver/cmd/main.go#L149) in [main.go](cmd/main.go))

The swagger ui is available at the following URLs:

* [local service -- http://localhost:8888/swagger-ui/](http://localhost:8888/swagger-ui/)
* [kind cluster -- http://localhost:31888/swagger-ui/](http://localhost:31888/swagger-ui/)

Please note that for the local service the directory containing the `*.swagger.files` is specified using the `-localSwaggerPath` command line argument. The `make run` command sets the value correctly.

#### Start Local Service

This will start the api server on your development machine. The golang race detector is turned on when starting the api server this way. It will use Kubernetes configuration file located at `~/.kube/config`. The service will not start if you do not have a connection to a Kubernetes cluster.

```bash
make run
```

#### Access

Access the service at `localhost:8888` for http, and `localhost:8887` for the RPC port.

### Kubernetes Deployment

#### Build Image

```bash
#creates an image with the tag kuberay/apiserver:latest
make docker-image
```

#### Build Multi-Architecture Image

The API server supports building multi-architecture images for `linux/amd64` and `linux/arm64` platforms. This is useful for deploying on ARM-based systems like Apple Silicon.

**Prerequisites:**

* Docker Buildx installed and configured

**Build Process:**

The multi-arch build follows a similar pattern to the ray-operator:

**Build binaries for each architecture:**

```bash
# Build for amd64
cd apiserver
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o kuberay-apiserver-amd64 cmd/main.go

# Build for arm64
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -a -o kuberay-apiserver-arm64 cmd/main.go
```

**Build and push multi-arch image using Dockerfile.buildx:**

```bash
# From the project root directory
docker buildx build --platform linux/amd64,linux/arm64 \
  -t your-registry/apiserver:your-tag \
  -f apiserver/Dockerfile.buildx \
  --push .
```

**Note:**

* The multi-arch image uses `apiserver/Dockerfile.buildx` which is optimized for copying pre-built binaries.
* Since the API server is built with `CGO_ENABLED=0`, no cross-compilation tools (like gcc-aarch64-linux-gnu) are needed, making the build process simpler than the operator.
* The build context must be the project root directory (`.`) because the `proto/` directory needs to be copied for serving swagger files at runtime.
* Multi-arch images are automatically built and pushed to `quay.io/kuberay/apiserver:nightly` on merges to the `master` branch via the GitHub Actions workflow.

#### Start Kubernetes Deployment

Note that you should make your KubeRay API server image available by either pushing it to an image registry, such as DockerHub or Quay, or by loading the image into the Kubernetes cluster. If you are using a Kind cluster for development, you can run `make load-image` to load the newly built API server image into the Kind cluster.  The operator image will also be needed to be loaded on your cluster. If you want run secure API server, you can build security proxy using `make security-proxy-image` and load it to the cluster using `make load-security-proxy-image`

You can use `make operator-image` to build a fresh image from sources, and, if you are using a Kind cluster for development, you can run `make load-operator-image`.

```bash
#Optionally, to load the api server image into the local kind cluster created with make cluster
make load-image

#To use the  helm charts
make deploy

#To use the configuration from deploy/base for insecure API server
make install

#To use the configuration from deploy/base for secure API server
make install-secure
```

#### Stop Kubernetes Deployment

```bash
#To use the helm charts
make undeploy

#To use the configuration insecure
make uninstall

#To use the configuration secure
make uninstall-secure
```

#### Local Kind Cluster Deployment

As a convenience for local development the following `make` targets are provided:

* `make cluster` -- creates a local kind cluster, using the configuration from `hack/kind-cluster-config.yaml`. It creates a port mapping allowing for the service running in the kind cluster to be accessed on  `localhost:31888` for HTTP and `localhost:31887` for RPC.
* `make clean-cluster` -- deletes the local kind cluster created with `make cluster`
* `load-image` -- loads the docker image defined by the `IMG` make variable into the kind cluster. The default value for variable is: `kuberay/apiserver:latest`. The name of the image can be changed by using `make load-image -e IMG=<your image name and tag>`
* `operator-image` -- Build the operator image to be loaded in your kind cluster. The operator image build is `kuberay/operator:latest`. The image tag can be overridden from the command line: ( example: `make operator-image -e OPERATOR_IMAGE_TAG=foo`)
* `load-operator-image` -- Load the operator image to the kind cluster created with `make cluster`.  It should be used in conjunction with the `deploy-operator target`
* `security-proxy-image` -- Build the security proxy image to be loaded in your kind cluster. The security proxy image build is `kuberay/security-proxy:latest`. The image tag can be overridden from the command line: ( example: `make security-proxy-image -e SECURITY_IMAGE_TAG=foo`)
* `load-security-proxy-image` -- Load the security proxy image to the kind cluster created with `make cluster`.  It should be used in conjunction with the `install-secure`
* `deploy-operator` -- Deploy operator into your cluster.  The tag for the operator image is `kuberay/operator:latest`.
* `undeploy-operator` -- Undeploy operator from your cluster
* `load-ray-test-image` -- Load the ray test images into the cluster.

When developing and testing with kind you might want to execute these targets together:

```bash
#To create a new API server image and to deploy it on a new cluster
make docker-image cluster load-image deploy

#To create a new API server image, operator image and deploy them on a new cluster
make docker-image operator-image cluster load-image load-operator-image deploy deploy-operator

#To execute end 2 end tests with a local build operator and verbose output
make local-e2e-test -e GO_TEST_FLAGS="-v"

#To execute end 2 end test with the nightly build operator
make local-e2e-test -e OPERATOR_IMAGE_TAG=nightly
```

#### Access API Server in the Cluster

Access the service at `localhost:31888` for http and `localhost:31887` for the RPC port.
