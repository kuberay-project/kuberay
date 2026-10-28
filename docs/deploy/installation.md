# Installation

Make sure your Kubernetes cluster and Kubectl are both at version at least 1.23.

## Nightly version

```sh
export KUBERAY_VERSION=master

# Install CRD and KubeRay operator
kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default?ref=${KUBERAY_VERSION}&timeout=90s"
```

## Stable version

### Method 1: Install charts from Helm repository (Recommended)

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/

# Install both CRDs and KubeRay operator
helm install kuberay-operator kuberay/kuberay-operator
```

### Method 2: Kustomize

```sh
# Install CRD and KubeRay operator
kubectl create -k "github.com/ray-project/kuberay/ray-operator/config/default?ref=v1.1.0&timeout=90s"
```
