# RayCluster

![Version: 1.2.0](https://img.shields.io/badge/Version-1.2.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square)

A Helm chart for deploying the RayCluster with the kuberay operator.

**Homepage:** <https://github.com/ray-project/kuberay>

## Introduction

RayCluster is a custom resource definition (CRD).
KubeRay operator will listen to the resource events about RayCluster and create related Kubernetes resources
(e.g. Pod & Service).
Hence, KubeRay operator installation and CRD registration are required for this guide.

## Prerequisites

See [kuberay-operator/README.md] for more details.

* Helm
* Install custom resource definition and KubeRay operator (covered by the following end-to-end
  example.)

## End-to-end example

```sh
# Step 1: Create a KinD cluster
kind create cluster

# Step 2: Register a Helm chart repo
helm repo add kuberay https://ray-project.github.io/kuberay-helm/

# Step 3: Install both CRDs and KubeRay operator v1.1.0.
helm install kuberay-operator kuberay/kuberay-operator --version 1.1.0

# Step 4: Install a RayCluster custom resource
# (For x86_64 users)
helm install raycluster kuberay/ray-cluster --version 1.1.0
# (For arm64 users, e.g. Mac M1)
# See here for all available arm64 images: https://hub.docker.com/r/rayproject/ray/tags?page=1&name=aarch64
helm install raycluster kuberay/ray-cluster --version 1.1.0 --set image.tag=nightly-aarch64

# Step 5: Verify the installation of KubeRay operator and RayCluster
kubectl get pods
# NAME                                          READY   STATUS    RESTARTS   AGE
# kuberay-operator-6fcbb94f64-gkpc9             1/1     Running   0          89s
# raycluster-kuberay-head-qp9f4                 1/1     Running   0          66s
# raycluster-kuberay-worker-workergroup-2jckt   1/1     Running   0          66s

# Step 6: Forward the port of Dashboard
kubectl port-forward svc/raycluster-kuberay-head-svc 8265:8265

# Step 7: Check 127.0.0.1:8265 for the Dashboard

# Step 8: Log in to Ray head Pod and execute a job.
kubectl exec -it ${RAYCLUSTER_HEAD_POD} -- bash
python -c "import ray; ray.init(); print(ray.cluster_resources())" # (in Ray head Pod)

# Step 9: Check 127.0.0.1:8265/#/job. The status of the job should be "SUCCEEDED".

# Step 10: Uninstall RayCluster
helm uninstall raycluster

# Step 11: Verify that RayCluster has been removed successfully
# NAME                                READY   STATUS    RESTARTS   AGE
# kuberay-operator-6fcbb94f64-gkpc9   1/1     Running   0          9m57s
```

[kuberay-operator/README.md]: https://github.com/ray-project/kuberay/blob/master/helm-chart/kuberay-operator/README.md

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| image.repository | string | `"rayproject/ray"` | Image repository. |
| image.tag | string | `"2.52.0"` | Image tag. |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy. |
| nameOverride | string | `"kuberay"` | String to partially override release name. |
| fullnameOverride | string | `""` | String to fully override release name. |
| imagePullSecrets | list | `[]` | Secrets with credentials to pull images from a private registry |
| upgradeStrategy | object | `{}` | Recreate all Pods on RayCluster spec change. Intended for GitOps; running workloads are lost. |
| authOptions | object | `{}` | Token authentication for the RayCluster. |
| gcsFaultTolerance.enabled | bool | `false` |  |
| common.containerEnv | list | `[]` | containerEnv specifies environment variables for the Ray head and worker containers. Follows standard K8s container env schema. |
| common.envFrom | list | `[]` | envFrom specifies sources used to populate environment variables in the main Ray head and worker containers. Common sources are rendered before group-specific sources. Referenced ConfigMaps and Secrets must already exist. |
| head.initContainers | list | `[]` | Init containers to add to the head pod |
| head.labels | object | `{}` | Labels for the head pod |
| head.serviceAccountName | string | `""` |  |
| head.restartPolicy | string | `""` |  |
| head.runtimeClassName | string | `""` | runtimeClassName is the name of the RuntimeClass to use to run the head Pod. |
| head.containerEnv | list | `[]` |  |
| head.envFrom | list | `[]` | envFrom to pass to head pod |
| head.resources.limits.cpu | string | `"1"` |  |
| head.resources.limits.memory | string | `"5Gi"` |  |
| head.resources.requests.cpu | string | `"1"` |  |
| head.resources.requests.memory | string | `"5Gi"` |  |
| head.resourceClaims | list | `[]` | ResourceClaims to allocate with the head pod |
| head.annotations | object | `{}` | Extra annotations for head pod |
| head.nodeSelector | object | `{}` | Node labels for head pod assignment |
| head.tolerations | list | `[]` | Node tolerations for head pod scheduling to nodes with taints |
| head.affinity | object | `{}` | Head pod affinity |
| head.podSecurityContext | object | `{}` | Head pod security context. |
| head.securityContext | object | `{}` | Ray container security context. |
| head.volumes[0].name | string | `"log-volume"` |  |
| head.volumes[0].emptyDir | object | `{}` |  |
| head.volumeMounts[0].mountPath | string | `"/tmp/ray"` |  |
| head.volumeMounts[0].name | string | `"log-volume"` |  |
| head.sidecarContainers | list | `[]` |  |
| head.command | list | `[]` |  |
| head.args | list | `[]` |  |
| head.headService | object | `{}` |  |
| head.topologySpreadConstraints | list | `[]` |  |
| head.rayStartParams | object | `{}` |  |
| worker.groupName | string | `"workergroup"` | The name of the workergroup |
| worker.replicas | int | `1` | The number of replicas for the worker pod |
| worker.minReplicas | int | `1` | The minimum number of replicas for the worker pod |
| worker.maxReplicas | int | `5` | The maximum number of replicas for the worker pod |
| worker.labels | object | `{}` | Labels for the worker pod |
| worker.serviceAccountName | string | `""` |  |
| worker.restartPolicy | string | `""` |  |
| worker.runtimeClassName | string | `""` | runtimeClassName is the name of the RuntimeClass to use to run the worker Pods. |
| worker.initContainers | list | `[]` | Init containers to add to the worker pod |
| worker.containerEnv | list | `[]` |  |
| worker.envFrom | list | `[]` | envFrom to pass to worker pod |
| worker.resources.limits.cpu | string | `"1"` |  |
| worker.resources.limits.memory | string | `"1Gi"` |  |
| worker.resources.requests.cpu | string | `"1"` |  |
| worker.resources.requests.memory | string | `"1Gi"` |  |
| worker.resourceClaims | list | `[]` | ResourceClaims to allocate with the worker pod |
| worker.annotations | object | `{}` | Extra annotations for worker pod |
| worker.nodeSelector | object | `{}` | Node labels for worker pod assignment |
| worker.tolerations | list | `[]` | Node tolerations for worker pod scheduling to nodes with taints |
| worker.affinity | object | `{}` | Worker pod affinity |
| worker.podSecurityContext | object | `{}` | Worker pod security context. |
| worker.securityContext | object | `{}` | Ray container security context. |
| worker.volumes[0].name | string | `"log-volume"` |  |
| worker.volumes[0].emptyDir | object | `{}` |  |
| worker.volumeMounts[0].mountPath | string | `"/tmp/ray"` |  |
| worker.volumeMounts[0].name | string | `"log-volume"` |  |
| worker.sidecarContainers | list | `[]` |  |
| worker.command | list | `[]` |  |
| worker.args | list | `[]` |  |
| worker.topologySpreadConstraints | list | `[]` |  |
| worker.rayStartParams | object | `{}` |  |
| additionalWorkerGroups.smallGroup.disabled | bool | `true` |  |
| additionalWorkerGroups.smallGroup.replicas | int | `0` | The number of replicas for the additional worker pod |
| additionalWorkerGroups.smallGroup.minReplicas | int | `0` | The minimum number of replicas for the additional worker pod |
| additionalWorkerGroups.smallGroup.maxReplicas | int | `5` | The maximum number of replicas for the additional worker pod |
| additionalWorkerGroups.smallGroup.labels | object | `{}` | Labels for the additional worker pod |
| additionalWorkerGroups.smallGroup.serviceAccountName | string | `""` |  |
| additionalWorkerGroups.smallGroup.restartPolicy | string | `""` |  |
| additionalWorkerGroups.smallGroup.runtimeClassName | string | `""` | runtimeClassName for this additional worker group. Empty string means default runtime. |
| additionalWorkerGroups.smallGroup.containerEnv | list | `[]` |  |
| additionalWorkerGroups.smallGroup.envFrom | list | `[]` | envFrom to pass to additional worker pod |
| additionalWorkerGroups.smallGroup.resources.limits.cpu | int | `1` |  |
| additionalWorkerGroups.smallGroup.resources.limits.memory | string | `"1Gi"` |  |
| additionalWorkerGroups.smallGroup.resources.requests.cpu | int | `1` |  |
| additionalWorkerGroups.smallGroup.resources.requests.memory | string | `"1Gi"` |  |
| additionalWorkerGroups.smallGroup.resourceClaims | list | `[]` | ResourceClaims to allocate with the additional worker pod |
| additionalWorkerGroups.smallGroup.annotations | object | `{}` | Extra annotations for additional worker pod |
| additionalWorkerGroups.smallGroup.nodeSelector | object | `{}` | Node labels for additional worker pod assignment |
| additionalWorkerGroups.smallGroup.tolerations | list | `[]` | Node tolerations for additional worker pod scheduling to nodes with taints |
| additionalWorkerGroups.smallGroup.affinity | object | `{}` | Additional worker pod affinity |
| additionalWorkerGroups.smallGroup.podSecurityContext | object | `{}` | Additional worker pod security context. |
| additionalWorkerGroups.smallGroup.securityContext | object | `{}` | Ray container security context. |
| additionalWorkerGroups.smallGroup.volumes[0].name | string | `"log-volume"` |  |
| additionalWorkerGroups.smallGroup.volumes[0].emptyDir | object | `{}` |  |
| additionalWorkerGroups.smallGroup.volumeMounts[0].mountPath | string | `"/tmp/ray"` |  |
| additionalWorkerGroups.smallGroup.volumeMounts[0].name | string | `"log-volume"` |  |
| additionalWorkerGroups.smallGroup.sidecarContainers | list | `[]` |  |
| additionalWorkerGroups.smallGroup.command | list | `[]` |  |
| additionalWorkerGroups.smallGroup.args | list | `[]` |  |
| additionalWorkerGroups.smallGroup.topologySpreadConstraints | list | `[]` |  |
| additionalWorkerGroups.smallGroup.rayStartParams | object | `{}` |  |
| service.type | string | `"ClusterIP"` |  |
| podDisruptionBudget.enabled | bool | `false` | Whether to create a PodDisruptionBudget for the RayCluster. |
| podDisruptionBudget.minAvailable | int | `1` | Minimum number of pods that must remain available during a voluntary disruption. An absolute number or a percentage (e.g. "50%"). Mutually exclusive with maxUnavailable; when both are set, minAvailable takes precedence. |
| podDisruptionBudget.maxUnavailable | string | `""` | Maximum number of pods that can be unavailable during a voluntary disruption. An absolute number or a percentage (e.g. "50%"). Set minAvailable to null to use this instead. |
| podDisruptionBudget.selectorLabels | object | `{}` | Extra labels to merge into the PDB selector. By default the PDB selects every pod of this RayCluster through the operator-managed `ray.io/cluster` label. |
