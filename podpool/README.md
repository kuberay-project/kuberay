# KubeRay Pod Pool Virtual Kubelet

KubeRay Pod Pool Virtual Kubelet is an optional and standalone component of the KubeRay Ecosystem.
It provides warmed-up pod pools by registering itself as a virtual kubelet to a Kubernetes cluster.
When KubeRay requests pods from the virtual kubelet,
the actual pods are taken from one of the pod pools specified by the users and effectively skip:

* resource scheduling time
* image pulling time
* volume preparation time
  because those pods are already active and waiting in pools.

<!-- TODO: Add docs for local development -->

## controller

pod pool controller

## manager

watch pod which scheduled by kubernetes, pick and sync pod status from pod pool to kubernetes
