# KubeRay History Server documentation

> [!NOTE]
> The KubeRay History Server is under active development. This directory contains
> guides to set up and run the History Server components locally for development
> and testing.

## Overview

The KubeRay History Server collects, stores, and displays historical logs and
metadata from Ray clusters. It has two parts:

1. **Collector**: A sidecar container that runs alongside Ray nodes to collect
   logs and metadata, then uploads them to blob storage.
2. **History Server**: A central service that reads data from blob storage and
   provides a web interface to explore the history of Ray jobs, tasks, actors,
   and other cluster activities.

## Guides

| Guide | Description |
|-------|-------------|
| [Collector setup](set_up_collector.md) | How to set up the Collector on a Kind cluster with RustFS or Azure storage |
| [History Server setup](set_up_historyserver.md) | Quick start guide for deploying and using the History Server with API examples |

## Collector image

KubeRay injects the collector sidecar into Ray Pods of every RayCluster that sets
`spec.historyServerOptions.collectorOptions`. The image is resolved in this order:

1. `spec.historyServerOptions.collectorOptions.image` on the RayCluster, if set.
2. The collector image configured on the operator, either with the `--collector-image` CLI flag,
   the `collectorImage` field of the operator configuration file, or the `collectorImage` Helm
   value of the `kuberay-operator` chart.
3. `quay.io/kuberay/collector` with the tag matching the KubeRay operator version.

Configuring the image on the operator keeps every RayCluster manifest free of a collector image
tag that must be bumped on each KubeRay upgrade, and lets clusters that cannot reach `quay.io`
or Docker Hub pull the collector from a mirror registry:

```sh
helm install kuberay-operator kuberay/kuberay-operator \
  --set collectorImage=my-registry.io/kuberay/collector:v1.7.0
```

The collector is only injected when the `RayClusterHistoryServer` feature gate is enabled on the
operator. See the `featureGates` value of the `kuberay-operator` chart.

> [!WARNING]
> The injected container is named `ray-history-collector`, and KubeRay only recognizes a collector
> by that name. If your Ray Pods already run a manually added collector sidecar, remove it in the
> same patch that enables `collectorOptions`, otherwise every Ray Pod gets a second collector.
> A manual container that reuses the name `ray-history-collector` is instead rejected by validation.

## Supported storage backends

The History Server supports multiple storage backends:

| Backend | Description | Configuration |
|---------|-------------|---------------|
| S3 | AWS S3 or S3-compatible storage (e.g. RustFS) | Use `--storage-backend=s3` |
| Azure Blob Storage | Microsoft Azure Blob Storage | Use `--storage-backend=azureblob` |
| Aliyun OSS | Alibaba Cloud Object Storage Service | Use `--storage-backend=aliyunoss` |
| Local test | For local testing and development | Use `--storage-backend=localtest` |

## Running locally

### Prerequisites

- Go v1.24+
- Docker
- Kind
- kubectl
- GNU Make

### Quick start

1. **Set up the environment**: Follow the [Collector setup guide](set_up_collector.md)
   to spin up a Kind cluster, deploy the KubeRay operator, and configure storage.

2. **Build the components**:

   ```bash
   # Build both collector and history server images
   make -C historyserver localimage-build

   # Or build individually
   make -C historyserver localimage-collector
   make -C historyserver localimage-historyserver
   ```

3. **Load images into Kind**:

   ```bash
   kind load docker-image collector:v0.1.0
   kind load docker-image historyserver:v0.1.0
   ```

4. **Deploy and test**: Follow the [History Server setup guide](set_up_historyserver.md)
   to deploy the Ray cluster, submit jobs, and access the History Server API.

## Configuration files

Sample configs are in the `config/` directory:

| File | Description |
|------|-------------|
| `rustfs.yaml` | RustFS deployment for S3-compatible storage |
| `azurite.yaml` | Azurite deployment for Azure Blob Storage emulation |
| `rayjob.yaml` | Sample RayJob with collector sidecar; cluster shuts down after the job finishes (S3/RustFS) |
| `rayjob-azureblob.yaml` | Sample RayJob with collector sidecar (Azure Blob) |
| `rayjob-gcs.yaml` | Sample RayJob with collector sidecar (GCS) |
| `rayjob-aliyunoss.yaml` | Sample RayJob with collector sidecar (Alibaba Cloud OSS via RRSA) |
| `rayjob-kubernetes-auth.yaml` | Sample RayJob with collector sidecar using Kubernetes token authentication (S3/RustFS) |
| `ray-data.yaml` | Sample Ray Data RayJob with collector sidecar (S3/RustFS) |
| `rayservice.yaml` | Sample RayService with collector sidecar (S3/RustFS) |
| `historyserver.yaml` | History Server deployment (S3/RustFS) |
| `historyserver-azureblob.yaml` | History Server deployment (Azure Blob) |
| `service_account.yaml` | Service account for History Server |

## Additional resources

- [REP: Ray History Server #62](https://github.com/ray-project/enhancements/pull/62)
- [Design doc](https://docs.google.com/document/d/15Y2bW4uzeUJe84FxRNUnHozoQPqYdLB2yLmgrdF2ZmI/edit?pli=1&tab=t.0#heading=h.xrvvvqarib6g)
- [Slack channel: #ray-history-server](https://app.slack.com/client/TN4768NRM/C09QLLU8HTL)
