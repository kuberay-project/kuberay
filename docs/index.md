<!-- markdownlint-disable MD013 MD033 -->
# Welcome

<p align="center">
    <a href="https://github.com/ray-project/kuberay/issues">
        <img src="https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat" alt="contributions welcome"/>
    </a>
    <a href="https://github.com/ray-project/kuberay/issues">
        <img src="https://img.shields.io/github/issues-raw/ray-project/kuberay?style=flat" alt="github issues"/>
    </a>
    <img src="https://img.shields.io/badge/status-alpha-brightgreen?style=flat" alt="status is alpha"/>
    <img src="https://img.shields.io/github/license/ray-project/kuberay?style=flat" alt="apache license"/>
</p>
<p align="center">
    <a href="https://goreportcard.com/report/github.com/ray-project/kuberay">
        <img src="https://goreportcard.com/badge/github.com/ray-project/kuberay" alt="go report card"/>
    </a>
    <img src="https://img.shields.io/github/watchers/ray-project/kuberay?style=social" alt="github watchers"/>
    <img src="https://img.shields.io/github/stars/ray-project/kuberay?style=social" alt="github stars"/>
    <img src="https://img.shields.io/github/forks/ray-project/kuberay?style=social" alt="github forks"/>
</p>

## KubeRay

> We have moved all documentation to the [ray-project/ray](https://github.com/ray-project/ray) repository.
Please refer to the [Ray docs](https://docs.ray.io/en/latest/cluster/kubernetes/index.html) for the latest information.
The [ray-project/kuberay](https://github.com/ray-project/kuberay) repository hosts the KubeRay source code and community information.

KubeRay is a powerful, open-source Kubernetes operator that simplifies the deployment and management of [Ray](https://github.com/ray-project/ray) applications on Kubernetes. It offers several key components:

**KubeRay core**: This is the official, fully-maintained component of KubeRay that provides three custom resource definitions, RayCluster, RayJob, and RayService. These resources are designed to help you run a wide range of workloads with ease.

* **RayCluster**: KubeRay fully manages the lifecycle of RayCluster, including cluster creation/deletion, autoscaling, and ensuring fault tolerance.

* **RayJob**: With RayJob, KubeRay automatically creates a RayCluster and submits a job when the cluster is ready. You can also configure RayJob to automatically delete the RayCluster once the job finishes.

* **RayService**: RayService is made up of two parts: a RayCluster and a Ray Serve deployment graph. RayService offers zero-downtime upgrades for RayCluster and high availability.

**KubeRay ecosystem**: Some optional components.

* **Kubectl Plugin** (Beta): Starting from KubeRay v1.3.0, you can use the `kubectl ray` plugin to simplify
common workflows when deploying Ray on Kubernetes. If you aren’t familiar with Kubernetes, this
plugin simplifies running Ray on Kubernetes. See [kubectl-plugin](https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/kubectl-plugin.html#kubectl-plugin) for more details.

* **KubeRay APIServer** (Alpha): It provides a layer of simplified configuration for KubeRay resources. The KubeRay API server is used internally
by some organizations to back user interfaces for KubeRay resource management.

* **KubeRay Dashboard** (Experimental): Starting from KubeRay v1.4.0, we have introduced a new dashboard that enables users to view and manage KubeRay resources.
While it is not yet production-ready, we welcome your feedback.

---

We are a [Cloud Native Computing Foundation](https://www.cncf.io/) sandbox project.

<img src="https://raw.githubusercontent.com/cncf/artwork/main/other/cncf/horizontal/color/cncf-color.svg" alt="CNCF logo" width="300">
