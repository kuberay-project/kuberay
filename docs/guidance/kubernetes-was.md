# Kubernetes Workload-Aware Scheduling

This guide explains how to use KubeRay's Kubernetes Workload-Aware Scheduling (WAS). It gang schedules an entire
RayCluster — the head and all worker groups — as a single atomic unit by integrating RayCluster pods with the in-tree
Kubernetes `scheduling.k8s.io` Workload and PodGroup APIs and the default Kubernetes scheduler.

> **Scope of this guide.** This release targets the `scheduling.k8s.io/v1alpha3` API served by Kubernetes 1.37.

## Overview

Distributed AI/ML workloads on Kubernetes can suffer from partial scheduling. Some pods in a group get scheduled and
hold expensive nodes idle while waiting for the remaining pods, or partially scheduled groups block other workloads
indefinitely. Gang scheduling solves this by treating a group of pods as an atomic unit.

Kubernetes WAS uses the Workload and PodGroup APIs introduced by [KEP-4671][kep-4671] and [KEP-5832][kep-5832].
Unlike Volcano, YuniKorn, or other external schedulers, it keeps pods on the Kubernetes default scheduler and sets
`spec.schedulingGroup` on each pod to connect it to its PodGroup.

## Prerequisites

- Kubernetes 1.37, which serves `scheduling.k8s.io/v1alpha3`.
- `scheduling.k8s.io/v1beta1=true,scheduling.k8s.io/v1alpha3=true` in the kube-apiserver runtime config. KubeRay uses
  v1alpha3, while kube-scheduler's GenericWorkload integration requires v1beta1 to be served as well.
- `GenericWorkload=true` on the kube-apiserver, kube-controller-manager, and kube-scheduler.
- Fixed-size RayCluster worker groups. Autoscaling RayClusters are not supported.

## Enable Kubernetes WAS

Kubernetes WAS is gated by the KubeRay `KubernetesWAS` feature gate (alpha, disabled by default). **Enabling the
feature gate is all that is required to turn it on** — no other operator configuration is needed.

With Helm, add the feature gate:

```yaml
# values.yaml
featureGates:
  - name: KubernetesWAS
    enabled: true
```

```bash
helm install kuberay-operator helm-chart/kuberay-operator \
  --set 'featureGates[0].name=KubernetesWAS' \
  --set 'featureGates[0].enabled=true'
```

With operator flags, pass:

```bash
--feature-gates=KubernetesWAS=true
```

Kubernetes WAS is mutually exclusive with KubeRay's external batch scheduler integrations (Volcano, YuniKorn, KAI,
scheduler-plugins); enable only one at a time.

### Opt in per RayCluster

While the feature gate is enabled operator-wide, gang scheduling is applied to a RayCluster only when it carries the
opt-in label:

```yaml
metadata:
  labels:
    ray.io/gang-scheduling-enabled: "true"
```

This is the same label used by KubeRay's other gang-scheduling integrations. RayClusters without the label are
scheduled normally, pod by pod.

## Behavior

Once enabled and opted in, a RayCluster is scheduled as a single gang:

- **All-or-nothing.** The head pod and every worker-group pod are scheduled together. If the cluster cannot fit in
  full, none of its pods start — they stay `Pending` until there is room for the entire cluster. This avoids partial
  startups that hold expensive nodes idle.
- **What counts toward the gang.** One head pod plus the desired replicas of every worker group. A multi-host group
  contributes `replicas × numOfHosts` pods. Suspended worker groups contribute nothing.
- **Default scheduler.** Pods are placed by the standard Kubernetes scheduler; there is no separate scheduler to
  install or run.
- **Editing and scaling.** Changing worker groups or replica counts is picked up automatically — the gang requirement
  is updated to match the new cluster shape.
- **Suspend and resume.** Suspending a RayCluster deletes its pods but keeps the Workload and PodGroup in place;
  resuming reuses them so the recreated pods rejoin the same gang.
- **Cleanup.** The scheduling resources are garbage collected automatically when the RayCluster is deleted.

You can confirm a cluster is being gang scheduled by checking that its pods carry a scheduling group:

```bash
kubectl get pods -n <namespace> -l ray.io/cluster=<raycluster-name> \
  -o custom-columns=NAME:.metadata.name,GROUP:.spec.schedulingGroup.podGroupName
```

## Limitations

- This release is tied to the Kubernetes `scheduling.k8s.io/v1alpha3` alpha API served by Kubernetes 1.37.
- Autoscaling RayClusters are skipped and any existing Workload/PodGroup resources for that RayCluster are cleaned up.
  Fixed-size worker groups only.
- The entire RayCluster is scheduled as one gang. Partial scheduling of a subset of worker groups is not supported; if
  the cluster cannot be scheduled in full, none of its pods are scheduled.
- `spec.schedulingGroup` on pods is immutable. If you add the opt-in label to an already-running RayCluster, existing
  pods will not get a scheduling group until they are recreated.

## Troubleshooting

### Pods stay Pending

If a cluster's pods never leave `Pending`, the gang cannot be placed in full. Check that the cluster fits (enough nodes
and resources for the head plus all workers at once) and inspect pod events:

```bash
kubectl describe pod <pod-name> -n <namespace>
```

If pods are gated even though there appears to be capacity, confirm the cluster meets the [prerequisites](#prerequisites)
(the alpha API is served and `GenericWorkload` is enabled on the control plane).

### A running RayCluster did not start gang scheduling

The pod scheduling group is set at pod creation and is immutable. If you add the `ray.io/gang-scheduling-enabled` label
to an already-running RayCluster, existing pods are not affected — they pick up gang scheduling only when recreated.

### Autoscaling clusters are not gang scheduled

Autoscaling is intentionally unsupported. A RayCluster with `enableInTreeAutoscaling: true` is scheduled normally, pod
by pod, and no scheduling group is set on its pods.

[kep-4671]: https://github.com/kubernetes/enhancements/tree/master/keps/sig-scheduling/4671-gang-scheduling
[kep-5832]: https://github.com/kubernetes/enhancements/tree/master/keps/sig-scheduling/5832-decouple-podgroup-api
