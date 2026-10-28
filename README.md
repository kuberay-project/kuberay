<!-- markdownlint-disable MD013 -->
# KubeRay

[![Build Status](https://github.com/ray-project/kuberay/workflows/Go-build-and-test/badge.svg)](https://github.com/ray-project/kuberay/actions)
[![Release](https://img.shields.io/github/v/release/ray-project/kuberay)](https://github.com/ray-project/kuberay/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/ray-project/kuberay)](https://goreportcard.com/report/github.com/ray-project/kuberay)

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
by some organizations to back user interfaces for KubeRay resource management. See [KubeRay APIServer V2](https://github.com/ray-project/kuberay/blob/master/apiserversdk/README.md) for more details.

* **KubeRay Dashboard** (Experimental): Starting from KubeRay v1.4.0, we have introduced a new dashboard that enables users to view and manage KubeRay resources.
While it is not yet production-ready, we welcome your feedback. See [KubeRay Dashboard](https://docs.ray.io/en/master/cluster/kubernetes/user-guides/kuberay-dashboard.html) for more details.

## Documentation

From September 2023, all user-facing KubeRay documentation will be hosted on the [Ray documentation](https://docs.ray.io/en/latest/cluster/kubernetes/index.html).
The KubeRay repository only contains documentation related to the development and maintenance of KubeRay.

## Quick Start

* [RayCluster Quickstart](https://docs.ray.io/en/master/cluster/kubernetes/getting-started/raycluster-quick-start.html)
* [RayJob Quickstart](https://docs.ray.io/en/master/cluster/kubernetes/getting-started/rayjob-quick-start.html)
* [RayService Quickstart](https://docs.ray.io/en/master/cluster/kubernetes/getting-started/rayservice-quick-start.html)

## Examples

KubeRay examples are hosted on the [Ray documentation](https://docs.ray.io/en/latest/cluster/kubernetes/examples.html).
Examples span a wide range of use cases, including training, LLM online inference, batch inference, and more.

## Kubernetes Ecosystem

KubeRay integrates with the Kubernetes ecosystem, including observability tools (e.g., Prometheus, Grafana, py-spy), queuing systems (e.g., Volcano, Apache YuniKorn, Kueue), ingress controllers (e.g., Nginx), and more.
See [KubeRay Ecosystem](https://docs.ray.io/en/latest/cluster/kubernetes/k8s-ecosystem.html) for more details.

## Blog Posts

* [Scaling Ray to 10K Models and Beyond](https://medium.com/workday-engineering/scaling-ray-to-10k-models-and-beyond-92799b4c9fc3) Workday
* [How Klaviyo built a robust model serving platform with Ray Serve](https://klaviyo.tech/how-klaviyo-built-a-robust-model-serving-platform-with-ray-serve-c02ec65788b3) Klaviyo
* [Evolving Niantic AR Mapping Infrastructures with Ray](https://nianticlabs.com/news/ray) Niantic
* [Building a Modern Machine Learning Platform with Ray at Samsara](https://www.samsara.com/blog/building-a-modern-machine-learning-platform-with-ray) Samsara
* [Using Ray on Kubernetes with KubeRay at Google Cloud](https://cloud.google.com/blog/products/containers-kubernetes/use-ray-on-kubernetes-with-kuberay) Google
* [How DoorDash Built an Ensemble Learning Model for Time Series Forecasting with KubeRay](https://doordash.engineering/2023/06/20/how-doordash-built-an-ensemble-learning-model-for-time-series-forecasting/) Doordash
* [AI/ML Models Batch Training at Scale with Open Data Hub](https://cloud.redhat.com/blog/ai/ml-models-batch-training-at-scale-with-open-data-hub) Red Hat
* [Distributed Machine Learning at Instacart](https://tech.instacart.com/distributed-machine-learning-at-instacart-4b11d7569423) Instacart
* [Unleashing ML Innovation at Spotify with Ray](https://engineering.atspotify.com/2023/02/unleashing-ml-innovation-at-spotify-with-ray/) Spotify
* [Best Practices For Ray Cluster On ACK](https://www.alibabacloud.com/blog/best-practices-for-ray-clusters---ray-on-ack_600925) Alibaba Cloud

## Talks

* [Advanced Model Serving Techniques with Ray on Kubernetes | KubeCon 2024 NA](https://youtu.be/mASxYpfWUNU?si=iCuXakrP7ORAg37z) Anyscale + Google
* [Building Scalable AI Infrastructure with Kuberay and Kubernetes | Ray Summit 2024](https://youtu.be/bbKpBTGf_AU?si=BkdCL7FGOde71t_P) Anyscale + Google
* [Ray at Scale: Apple's Approach to Elastic GPU Management | Ray Summit 2024](https://youtu.be/ZCRZQVt-r3g?si=1Gxkpy8CNVVDDBP0) Apple
* [Scaling Ray Train to 10K Kubernetes Nodes on GKE | Ray Summit 2024](https://youtu.be/9S5WznGnIpE?si=O6Rqpor9QmAvdv6u) Google
* [KubeSecRay: Fortifying Multi-Tenant Ray Clusters on Kubernetes | Ray Summit 2024](https://youtu.be/Y-kLmZ3nklQ?si=N9FIc5Nk_rWwKBRp) Microsoft
* [Scaling LLM Inference: AWS Inferentia Meets Ray Serve on EKS | Ray Summit 2024](https://youtu.be/6rNfYlm6s1k?si=WZeXZXrMDtRbbVKO) AWS
* [How Roblox Scaled Machine Learning by Leveraging Ray for Efficient Batch Inference | Ray Summit 2024](https://youtu.be/BN1CVDZjQRE?si=9pN9A3bReSL26Pc-) Roblox
* [Airbnb's LLM Evolution: Fine-Tuning with Ray | Ray Summit 2024](https://youtu.be/jYQ9ry8uXY0?si=3P56QNo8Qwovv4Vf) Airbnb
* [Ray @ eBay: Pioneering a Next-Gen AI Platform | Ray Summit 2024](https://youtu.be/5KuTdRq9Zto?si=8m485B1411ixfdlx) eBay
* [Spotify Harnesses Ray for Next-Gen AI Infrastructure | Ray Summit 2024](https://youtu.be/4kw3EYBz1Gs?si=PswsNR88xe6Mxuas) Spotify
* [Spotify's Approach to Distributed LLM Training with Ray on GKE | Ray Summit 2024](https://youtu.be/2l1lVBdmNIQ?si=PwCeZD1-XajPNLam) Spotify
* [Reddit's ML Evolution: Scaling with Ray and KubeRay | Ray Summit 2024](https://youtu.be/XwrGk0SM6ls?si=xNMQo548lOonKLiK) Reddit
* [IBM's Approach to Building a Cloud-Native AI Platform | Ray Summit 2024](https://youtu.be/Q27JFtLE6b4?si=QQhVMZyBRelkLC13) IBM
* [Exploring Hinge's ML Platform Evolution with Ray | Ray Summit 2024](https://youtu.be/_nsTcYtfnXU?si=dKNasWOxiTRJgyvj) Hinge
* [How Rubrik Unlocked AI at Scale with Ray Serve | Ray Summit 2024](https://youtu.be/Md5vww4ardo?si=leiuvNkDy2fKeK8r) Rubrik
* [Supercharge Your AI Platform with KubeRay | KubeCon 2023 NA](https://youtu.be/DgfJR6wR4BQ?si=QuK3j7VEkteSwglA) Anyscale + Google
* [Sailing Ray Workloads with KubeRay and Kueue in Kubernetes](https://www.youtube.com/watch?v=Q-sQLDMeJ8M) Volcano + DaoCloud
* [Serving Large Language Models with KubeRay on TPUs](https://www.youtube.com/watch?v=RK_u6cfPnnw) Google

## Development

Please read our [CONTRIBUTING](CONTRIBUTING.md) guide before making a pull request. Refer to our [DEVELOPMENT](./ray-operator/DEVELOPMENT.md) to build and run tests locally.

## Getting Involved

Join [Ray's Slack workspace](https://docs.google.com/forms/d/e/1FAIpQLSfAcoiLCHOguOm8e7Jnn-JJdZaCxPGjgVCvFijHB5PLaQLeig/viewform), and search the following public channels:

* `#kuberay`: This channel aims to help KubeRay users with their questions. The messages will be closely monitored by the Ray and KubeRay maintainers.

KubeRay contributors are welcome to join the bi-weekly KubeRay community meetings.

* See the KubeRay community meeting schedule on [Ray/KubeRay LFX calendar](https://zoom-lfx.platform.linuxfoundation.org/meetings/ray?view=month).
* Join the KubeRay community meeting on [Zoom](https://zoom-lfx.platform.linuxfoundation.org/meeting/99190775938?password=96660107-f00b-4d1d-8015-d6348e911a0a).

See [GOVERNANCE.md](GOVERNANCE.md) for the roles in the KubeRay community and how they are granted, and
[PEOPLE.md](docs/community/PEOPLE.md) for the current role holders.

## Security

If you discover a potential security issue in this project, or think you may
have discovered a security issue, please report it according to our
[security policy](SECURITY.md).
Please do **not** create a public GitHub issue.

## License

This project is licensed under the [Apache-2.0 License](LICENSE).

---

We are a [Cloud Native Computing Foundation](https://www.cncf.io/) sandbox project.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cncf/artwork/main/other/cncf/horizontal/white/cncf-white.svg">
  <img src="https://raw.githubusercontent.com/cncf/artwork/main/other/cncf/horizontal/color/cncf-color.svg" alt="CNCF logo" width="300">
</picture>

Copyright Contributors to KubeRay, established as KubeRay a Series of LF Projects, LLC.
For website terms of use, trademark policy and other project policies please see [lfprojects.org/policies](https://lfprojects.org/policies/).
