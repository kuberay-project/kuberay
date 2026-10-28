# KubeRay APIServer V2 (alpha)

KubeRay APIServer V2 is the successor to the original [KubeRay APIServer V1](../apiserver/README.md).
There are two ways to use KubeRay APIServer V2 as an HTTP proxy server to manage Ray resources:

1. Use it as a Go module to build your own HTTP proxies with custom middleware functions.
2. Use the container image provided by the KubeRay community to run a proxy server.

KubeRay APIServer V2 provides an HTTP proxy to the Kubernetes APIServer with the same
interface defined in the Kubernetes OpenAPI Spec and KubeRay CRD.
Therefore, it is compatible with existing Kubernetes clients and API interfaces.

## When to use KubeRay APIServer V2

Consider using KubeRay APIServer V2 if:

- You want to manage Ray clusters in Kubernetes via HTTP/REST (e.g., from a UI, SDK, or CLI).
- You want to create templates or default values to simplify configuration setup.

For brevity, "KubeRay APIServer V2" will be referred to as "KubeRay APIServer" throughout this documentation.

## Installation

Please follow the [installation guide](docs/installation.md) to install the APIServer, and
port-forward the HTTP endpoint to local port 31888.

## Quick Start

- [RayCluster Quickstart](./docs/raycluster-quickstart.md)
- [RayJob Quickstart](./docs/rayjob-quickstart.md)
- [RayService Quickstart](./docs/rayservice-quickstart.md)

## Usage

The KubeRay APIServer exposes a RESTful API that mirrors the Kubernetes APIServer. You
can interact with it using Kubernetes-style endpoints and request patterns to create,
retrieve, update, and delete custom resources such as `RayCluster` and `RayJob`.

### API Structure

The KubeRay API follows standard Kubernetes conventions. The structure of the endpoint
path depends on whether you are interacting with custom resources (e.g., `RayCluster`) or
core Kubernetes resources.

#### Custom Resources

For custom resources defined by CRDs (e.g., `RayCluster`, `RayJob`, etc.), the endpoint format is:

```sh
<baseURL>/apis/ray.io/v1/namespaces/<namespace>/<resourceType>/<resourceName>
```

- `namespace`: Your target Kubernetes namespace (e.g., `default`)
- `resourceType`: Custom resource type (e.g., `rayclusters`, `rayjobs`, `rayservices`)
- `resourceName`: Name of the resource.

### Label and Field Selectors

When listing resources (using `GET` on a collection endpoint), you can filter results using selectors:

- Label selector: filters resources by their labels.

```sh
# Get all RayClusters with the label key=value
GET /apis/ray.io/v1/namespaces/default/rayclusters?labelSelector=key%3Dvalue
```

- Field selector: Filters resources based on the value of their resource
fields (e.g., `metadata.name`, `status.phase`).

```sh
# Retrieve the RayCluster where the name is raycluster-prod
GET /apis/ray.io/v1/namespaces/default/rayclusters?fieldSelector=metadata.name%3Draycluster-prod
```

- Combined selectors: You can combine label and field selectors using `&`

```sh
# Get the RayCluster named raycluster-prod that also has the label env=prod.
GET /apis/ray.io/v1/namespaces/default/rayclusters?labelSelector=env%3Dprod&fieldSelector=metadata.name%3Draycluster-prod
```
