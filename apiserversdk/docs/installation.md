# KubeRay APIServer Installation

## Step 1: Create a Kubernetes cluster

This step creates a local Kubernetes cluster using [Kind](https://kind.sigs.k8s.io/). If you already have a Kubernetes
cluster, you can skip this step.

```sh
kind create cluster --image=kindest/node:v1.35.0
```

## Step 2: Deploy a KubeRay operator

Follow [this
document](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/kuberay-operator-installation.html#kuberay-operator-deploy)
to install the latest stable KubeRay operator from the Helm repository.

## Step 3: Install APIServer with Helm

* For v1.4.0, the following Helm chart will launch both KubeRay APIServer V1 and V2 simultaneously in the APIServer Pod.
For v1.5.0, only KubeRay APIServer V2 will be launched, and V1 will be removed.

* The security proxy is an optional feature that is still experimental.
Therefore, the documentation is written without the security proxy.

### Without security proxy

* Install a stable version via Helm repository

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
# Install KubeRay APIServer without security proxy
helm install kuberay-apiserver kuberay/kuberay-apiserver --version 1.4.0 --set security=null
```

* Install the nightly version

```sh
# Step1: Clone KubeRay repository

# Step2: Navigate to `helm-chart/kuberay-apiserver`
cd helm-chart/kuberay-apiserver

# Step3: Install the KubeRay apiserver
helm install kuberay-apiserver . --set security=null
```

### With security proxy

* Install a stable version via Helm repository

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
# Install KubeRay APIServer
helm install kuberay-apiserver kuberay/kuberay-apiserver --version 1.4.0
```

* Install the nightly version

```sh
# Step1: Clone KubeRay repository

# Step2: Navigate to `helm-chart/kuberay-apiserver`
cd helm-chart/kuberay-apiserver

# Step3: Install the KubeRay apiserver
helm install kuberay-apiserver .
```

> [!IMPORTANT]
> You may receive an "Unauthorized" error when making a request if you install the
> APIServer with security proxy. Please try the APIServer without a security proxy.

## Step 4: Port-forwarding the APIServer service

Use the following command for port-forwarding to access the APIServer through port 31888:

```sh
kubectl port-forward service/kuberay-apiserver-service 31888:8888
```

## Step 5: Validate installation

Check that the KubeRay APIServer is running in the "default" namespace.

```sh
kubectl get pods
# NAME                            READY   STATUS    RESTARTS   AGE
# kuberay-apiserver-xxxxxx    1/1     Running   0          17s
```
