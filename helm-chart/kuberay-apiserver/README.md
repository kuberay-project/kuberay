# KubeRay APIServer

![Version: 1.1.0](https://img.shields.io/badge/Version-1.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square)

A Helm chart for kuberay-apiserver

## Introduction

This document provides instructions to install the KubeRay APIServer with a Helm chart.
KubeRay APIServer V2 is enabled by default, for more information,
please refer to the [KubeRay APIServer V2 documentation].

## Helm

Make sure the version of Helm is v3+. Currently, [existing CI tests] are based on Helm v3.4.1 and
v3.9.4.

```sh
helm version
```

## Install KubeRay APIServer

### Without security proxy

- Install a stable version via Helm repository

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
# Install KubeRay APIServer without security proxy
helm install kuberay-apiserver kuberay/kuberay-apiserver --version 1.4.2 --set security=null
```

- Install the nightly version

```sh
# Step1: Clone KubeRay repository

# Step2: Navigate to `helm-chart/kuberay-apiserver`
cd helm-chart/kuberay-apiserver

# Step3: Install the KubeRay APIServer
helm install kuberay-apiserver . --set security=null
```

### With security proxy

- Install a stable version via Helm repository

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
# Install KubeRay APIServer
helm install kuberay-apiserver kuberay/kuberay-apiserver --version 1.4.2
```

- Install the nightly version

```sh
# Step1: Clone KubeRay repository

# Step2: Navigate to `helm-chart/kuberay-apiserver`
cd helm-chart/kuberay-apiserver

# Step3: Install the KubeRay APIServer
helm install kuberay-apiserver .
```

> [!IMPORTANT]
> If you receive an "Unauthorized" error when making a request, please add an
> authorization header to the request: `-H 'Authorization: 12345'` or install the
> APIServer without a security proxy.

## List the chart

To list the `kuberay-apiserver` release:

```sh
helm ls
# NAME                    NAMESPACE       REVISION        UPDATED                                 STATUS          CHART
# kuberay-apiserver       default         1               2025-08-08 17:07:51.472353906 +0000 UTC deployed        kuberay-apiserver-1.4.2
```

## Uninstall the Chart

```sh
# Uninstall the `kuberay-apiserver` release
helm uninstall kuberay-apiserver

# The APIServer Pod should be removed.
kubectl get pods
# No resources found in default namespace.
```

[existing CI tests]: https://github.com/ray-project/kuberay/blob/master/.github/workflows/helm.yaml
[KubeRay APIServer V2 documentation]: https://github.com/ray-project/kuberay/blob/master/apiserversdk/README.md

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| replicaCount | int | `1` |  |
| name | string | `"kuberay-apiserver"` |  |
| image.repository | string | `"quay.io/kuberay/apiserver"` | Image repository. |
| image.tag | string | `"nightly"` | Image tag. |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy. |
| cors | string | `nil` |  |
| labels | object | `{}` | Extra labels. |
| annotations | object | `{}` | Extra annotations. |
| serviceAccount.create | bool | `true` | Specifies whether a service account should be created. |
| serviceAccount.name | string | `"kuberay-apiserver"` | The name of the service account to use. If not set and create is true, a name is generated using the fullname template |
| containerPort[0].name | string | `"http"` |  |
| containerPort[0].containerPort | int | `8888` |  |
| containerPort[0].protocol | string | `"TCP"` |  |
| containerPort[1].name | string | `"grpc"` |  |
| containerPort[1].containerPort | int | `8887` |  |
| containerPort[1].protocol | string | `"TCP"` |  |
| resources | object | `{"limits":{"cpu":"500m","memory":"500Mi"},"requests":{"cpu":"300m","memory":"300Mi"}}` | Resource requests and limits for containers. |
| sidecarContainers | list | `[]` | Sidecar containers to run along with the main container. |
| nodeSelector | object | `{}` | Node selector for pods. |
| affinity | object | `{}` | Affinity for pods. |
| tolerations | list | `[]` | Tolerations for pods. |
| service.type | string | `"ClusterIP"` | Service type. |
| service.ports | list | `[{"name":"http","port":8888,"protocol":"TCP","targetPort":8888},{"name":"rpc","port":8887,"protocol":"TCP","targetPort":8887}]` | Service port. |
| ingress.enabled | bool | `false` |  |
| ingress.annotations | object | `{}` |  |
| ingress.className | string | `""` |  |
| ingress.tls | list | `[]` |  |
| route.enabled | bool | `false` |  |
| route.annotations | object | `{}` |  |
| rbacEnable | bool | `true` | Install Default RBAC roles and bindings |
| singleNamespaceInstall | bool | `false` | The chart can be installed by users with permissions to a single namespace only |
| enableAPIServerV2 | bool | `true` | If set to true, APIServer v2 would be served on the same port as the APIServer v1. |
| security.proxy | object | `{"pullPolicy":"IfNotPresent","repository":"quay.io/kuberay/security-proxy","tag":"nightly"}` | security proxy image. |
| security.env | object | `{"ENABLE_GRPC":"true","GRPC_LOCAL_PORT":8987,"HTTP_LOCAL_PORT":8988,"SECURITY_PREFIX":"/"}` | security proxy environment variables. |
