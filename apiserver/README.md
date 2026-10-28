<!-- markdownlint-disable MD013 -->
# KubeRay APIServer V1 (deprecated)

> [!WARNING]
> KubeRay APIServer V1 is deprecated and will be removed in the future.
> Please use [KubeRay APIServer V2](../apiserversdk/README.md) instead.

The KubeRay APIServer offers gRPC and HTTP APIs to manage KubeRay resources.

## Introduction

The KubeRay APIServer is an optional component that provides a layer of simplified configuration for KubeRay resources. Some organizations use the KubeRay APIServer internally to support user interfaces for managing KubeRay resources.

## Installation

### Install with Helm

Ensure that the version of Helm is v3+. Currently, [existing CI tests](https://github.com/ray-project/kuberay/blob/master/.github/workflows/helm.yaml) are based on Helm v3.4.1 and v3.9.4.

```sh
helm version
```

#### Create a Kubernetes cluster

Create a local Kubernetes cluster using [Kind](https://kind.sigs.k8s.io/). If you already
have a Kubernetes cluster, you can skip this step.

```sh
kind create cluster --image=kindest/node:v1.29.0
```

#### Install KubeRay Operator

Refer to [this document](https://docs.ray.io/en/master/cluster/kubernetes/getting-started/kuberay-operator-installation.html#kuberay-operator-deploy) to install the latest stable KubeRay operator.

#### Install KubeRay APIServer

Refer to [this document](../helm-chart/kuberay-apiserver/README.md#without-security-proxy) to install the latest
stable KubeRay operator and APIServer (without the security proxy) from the Helm
repository.

> [!IMPORTANT]
> If you install APIServer with security proxy, you may receive an "Unauthorized" error
> when making a request. Please add an authorization header to the request: `-H 'Authorization: 12345'`
> or install the APIServer without a security proxy.

#### Port-forwarding the APIServer service

Use the following command for port-forwarding to access the APIServer through port 31888:

```sh
kubectl port-forward service/kuberay-apiserver-service 31888:8888
```

### For Development: Start a Local APIServer

You can build and start the APIServer from scratch in your local environment with a single command. This will deploy all the necessary components to a local kind cluster.

```sh
make start-local-apiserver
```

### Verify the installation

The APIServer supports HTTP requests, so you can easily verify its successful startup by issuing two simple curl commands.

```sh
# Create complete template.
curl --silent -X 'POST' \
    'http://localhost:31888/apis/v1/namespaces/ray-system/compute_templates' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
        "name": "default-template",
        "namespace": "ray-system",
        "cpu": 2,
        "memory": 4
      }'

# Check whether compute template is created successfully.
curl --silent -X 'GET' \
    'http://localhost:31888/apis/v1/namespaces/ray-system/compute_templates' \
    -H 'accept: application/json'
```

## Usage

After deployment, you can use the `{{baseUrl}}` to access the service. Refer to the [Swagger support section](https://ray-project.github.io/kuberay/components/apiserver/#swagger-support) for complete API definitions.

- (default) for NodePort access, use port `31888` for connection

- for ingress access, you will need to create your own ingress

Details of the request parameters can be found in [KubeRay Swagger](https://github.com/ray-project/kuberay/tree/master/proto/swagger). This README provides only basic examples.

### Setup a smoke test

The following steps allow you to validate the integration of the KubeRay APIServer components and the KubeRay Operator in your environment.

1. (Optional) You may use your local kind cluster or minikube

    ```bash
    cat <<EOF | kind create cluster --name ray-test --config -
    kind: Cluster
    apiVersion: kind.x-k8s.io/v1alpha4
    nodes:
    - role: control-plane
      image: kindest/node:v1.29.0
      kubeadmConfigPatches:
        - |
          kind: InitConfiguration
          nodeRegistration:
            kubeletExtraArgs:
              node-labels: "ingress-ready=true"
      extraPortMappings:
      - containerPort: 30265
        hostPort: 8265
        listenAddress: "0.0.0.0"
        protocol: tcp
      - containerPort: 30001
        hostPort: 10001
        listenAddress: "0.0.0.0"
        protocol: tcp
      - containerPort: 8000
        hostPort: 8000
        listenAddress: "0.0.0.0"
      - containerPort: 31888
        hostPort: 31888
        listenAddress: "0.0.0.0"
      - containerPort: 31887
        hostPort: 31887
        listenAddress: "0.0.0.0"
    - role: worker
      image: kindest/node:v1.29.0
    - role: worker
      image: kindest/node:v1.29.0
    EOF
    ```

2. Deploy the KubeRay APIServer within the same cluster of KubeRay operator

    ```sh
    helm repo add kuberay https://ray-project.github.io/kuberay-helm/
    helm -n ray-system install kuberay-apiserver kuberay/kuberay-apiserver -n ray-system --create-namespace
    ```

3. The APIServer exposes its service using `NodePort` by default. You can test access via your host and port; the default port is set to `31888`. The examples below assume a kind (localhost) deployment. If the KubeRay APIServer is deployed on another type of cluster, you'll need to adjust the hostname to match your environment.

    ```sh
    curl localhost:31888
    ...
    {
      "code": 5,
      "message": "Not Found"
    }
    ```

4. You can create `RayCluster`, `RayJobs`, or `RayService` by accessing the endpoints.

## Swagger Support

The KubeRay APIServer supports Swagger UI. The Swagger page can be accessed at:

- [localhost:31888/swagger-ui](localhost:31888/swagger-ui) for local kind deployments
- [localhost:8888/swagger-ui](localhost:8888/swagger-ui) for instances started with `make run` (development machine builds)
- `<host name>:31888/swagger-ui` for NodePort deployments

## HTTP definition endpoints

The APIServer supports HTTP requests. For detailed specifications, check out the [full spec document](HttpRequestSpec.md).

## Advanced Usage

- [Creating Autoscaling clusters using API server](./Autoscaling.md)
- [Setting Up a Highly Available Cluster via the API Server](./HACluster.md)
- [RayJob Submission using API server](./JobSubmission.md)
- [Monitoring API server and created RayClusters](./Monitoring.md)
- [Setup API Server with Security](./SecuringImplementation.md)
- [Different Volumes support for RayCluster by the API server](./Volumes.md)
