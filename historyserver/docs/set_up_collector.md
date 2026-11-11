# KubeRay History Server - Collector

> [!NOTE]
> The KubeRay History Server is currently under active development. This document aims to provide the step-by-step
guideline to set up the Collector component for local development and testing.

## Table of contents

- [Materials for learning KubeRay History Server](#materials-for-learning-kuberay-history-server)
- [Test the Collector on the Kind cluster](#test-the-collector-on-the-kind-cluster)
  - [Prerequisites](#prerequisites)
  - [Spin up a Kind cluster and run KubeRay Operator](#spin-up-a-kind-cluster-and-run-kuberay-operator)
  - [Build the Collector container image](#build-the-collector-container-image)
  - [Load the Collector image into the Kind cluster](#load-the-collector-image-into-the-kind-cluster)
  - [Deploy storage backend](#deploy-storage-backend)
  - [Deploy a Ray cluster for checks](#deploy-a-ray-cluster-for-checks)
- [Troubleshooting](#troubleshooting)

## Materials for Learning KubeRay History Server

- [REP: Ray History Server #62](https://github.com/ray-project/enhancements/pull/62)
- [Design doc](https://docs.google.com/document/d/15Y2bW4uzeUJe84FxRNUnHozoQPqYdLB2yLmgrdF2ZmI/edit?pli=1&tab=t.0#heading=h.xrvvvqarib6g)
- Related issues
  - [\[Epic\]\[Feature\] history server collector #4274](https://github.com/ray-project/kuberay/issues/4274)
  - [\[Epic\]\[Feature\] Support History Server #3966](https://github.com/ray-project/kuberay/issues/3966)
  - [\[Feature\] Ray History Server #3884](https://github.com/ray-project/kuberay/issues/3884)
- Related PRs
  - [Historyserver beta version #4187](https://github.com/ray-project/kuberay/pull/4187)
  - [add the implementation of historyserver collector #4241](https://github.com/ray-project/kuberay/pull/4241)
  - [add the implementation of historyserver #4242](https://github.com/ray-project/kuberay/pull/4242)

## Test the Collector on the Kind Cluster

### Prerequisites

Please ensure your environment matches the version requirements specified in the [ray-operator development guide](https://github.com/ray-project/kuberay/blob/2959d7d8a4174eedbf7b4a71a79219547f62cc82/ray-operator/DEVELOPMENT.md):

- Go v1.24+
- Docker: Engine for building the container image
- GNU Make
- K9s (optional)

### Spin Up a Kind Cluster and Run KubeRay Operator

The following environment setup is based on the [ray-operator development guide](https://github.com/JiangJiaWei1103/kuberay/blob/e4d8ad6e34adbe13b4c77c35313af2c9bc16da82/ray-operator/DEVELOPMENT.md#run-the-operator-inside-the-cluster).

```bash
# Clone the KubeRay repo and cd into the working dir.
git clone https://github.com/ray-project/kuberay.git
cd kuberay

# Spin up a kind cluster.
kind create cluster --image=kindest/node:v1.29.0

# Build the KubeRay operator image.
IMG=kuberay/operator:latest make -C ray-operator docker-build

# Load the custom KubeRay image into the kind cluster.
kind load docker-image kuberay/operator:latest

# Install the KubeRay operator with the custom image via local Helm chart.
helm install kuberay-operator \
  --set image.repository=kuberay/operator \
  --set image.tag=latest \
  ./helm-chart/kuberay-operator

# Check the logs via kubectl or k9s.
kubectl logs deployments/kuberay-operator
# or
k9s
```

### Build the Collector Container Image

```bash
# Build the collector image.
make -C historyserver localimage-collector

# Or to build the collector image with race detector
# make -C historyserver localimage-collector-race

# Check the built image.
docker images | grep collector
```

You're supposed to see a `collector:v0.1.0` image. If you'd like to change the image reference, please feel free to tag
it.

### Load the Collector Image into the Kind Cluster

```bash
# Load the image into the kind cluster.
kind load docker-image collector:v0.1.0

# Or to load the collector image with race detector
# kind load docker-image collector:v0.1.0-race

# Check the loaded image.
docker exec -it kind-control-plane crictl images | grep collector
```

### Deploy storage backend

Choose one of the following storage backends for log and event storage:

- [Option A: RustFS (S3-compatible)](#option-a-rustfs-s3-compatible) - recommended for most users
- [Option B: Azure Blob Storage (Azurite)](#option-b-azure-blob-storage-azurite)

#### Option A: RustFS (S3-compatible)

Deploy RustFS as an S3-compatible storage backend:

```bash
# Apply the RustFS manifest.
kubectl apply -f historyserver/config/rustfs.yaml

# Port-forward the RustFS console for sanity check.
kubectl -n rustfs-dev port-forward svc/rustfs-service 9001:9001

# Open the RustFS console.
open http://localhost:9001/rustfs/console/
```

Login with:

```text
Account: rustfsadmin
Key: rustfsadmin
```

#### Option B: Azure Blob Storage (Azurite)

Deploy Azurite as an Azure Blob Storage emulator for local development:

```bash
# Apply the azurite manifest.
kubectl apply -f historyserver/config/azurite.yaml

# Verify the azurite pod is running.
kubectl get pods -n azurite-dev
```

Azurite uses the well-known development storage account credentials:

```text
Account name: devstoreaccount1
Account key:  Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
```

The collector connects to Azurite using a connection string configured in the Ray cluster
manifest. See `config/rayjob-azureblob.yaml` for the full configuration.

### Deploy a Ray cluster for checks

You can check if the collector works as expected by deploying a Ray cluster with the collector
enabled and interacting with the storage backend.

> [!IMPORTANT]
> The collector sidecar must share the `/tmp/ray` directory with the Ray container using an
> `emptyDir` volume. This allows the collector to read session logs and events from the Ray
> process. Without this shared volume, the collector will fail with errors like
> `read session_latest file error`. See the example manifest for the required volume
> configuration:

```yaml
spec:
  containers:
  - name: ray-head
    volumeMounts:
    - name: ray-logs
      mountPath: /tmp/ray
  - name: collector
    volumeMounts:
    - name: ray-logs
      mountPath: /tmp/ray
  volumes:
  - name: ray-logs
    emptyDir: {}
```

> [!IMPORTANT]
> The Ray container must also include a `postStart` lifecycle hook to write the raylet node ID
> to a file. The collector reads this file to identify the node. Without this hook, the
> collector will fail with errors like `read nodeid file error`. Add the following lifecycle
> configuration to the Ray container:

```yaml
lifecycle:
  postStart:
    exec:
      command:
      - /bin/sh
      - -c
      - |
        while true; do
          nodeid=$(ps -ef | grep 'raylet.*--node_id' | grep -v grep | sed -n 's/.*--node_id=\([^ ]*\).*/\1/p' | head -1)
          if [ -n "$nodeid" ]; then
            echo "$nodeid" > /tmp/ray/raylet_node_id
            break
          fi
          sleep 1
        done
```

> Note: The example manifest at `config/rayjob.yaml` uses `grep -oP` (Perl regex) which may
> not be available in all containers. The `sed` command above is more portable.

Submit the RayJob that matches your storage backend. It creates a Ray cluster with the
collector sidecar, runs a sample workload, and shuts the cluster down after the job
finishes (`shutdownAfterJobFinishes` with a 30s TTL):

```bash
# For RustFS (S3-compatible):
kubectl apply -f historyserver/config/rayjob.yaml

# For Azure Blob Storage (Azurite):
kubectl apply -f historyserver/config/rayjob-azureblob.yaml
```

> [!IMPORTANT]
> After deploying the Ray cluster, the collector automatically creates a container/bucket named
> `ray-historyserver` in your storage backend. For RustFS, you can verify this in the RustFS console.

![create_bucket](assets/create_bucket.png)

Wait for the job to succeed:

```bash
kubectl wait rayjob/rayjob-historyserver --for=jsonpath='{.status.jobStatus}=SUCCEEDED' --timeout=5m
# For Azure Blob Storage, the RayJob name is rayjob-historyserver-azureblob.
```

Session logs are processed and events are flushed when the Ray cluster is deleted, which
happens automatically 30s after the job finishes. To skip the TTL wait, delete the RayJob:

```bash
# For RustFS:
kubectl delete -f historyserver/config/rayjob.yaml

# For Azure Blob Storage:
kubectl delete -f historyserver/config/rayjob-azureblob.yaml
```

You should see the uploaded logs and events in the storage backend UI. For RustFS:

![write_logs_and_events](assets/write_logs_and_events.png)

## Troubleshooting

### "too many open files" error

If you encounter `level=fatal msg="Create fsnotify NewWatcher error too many open files"` in the collector logs,
it is likely due to the inotify limits on the Kubernetes nodes.

To fix this, increase the limits on the **host nodes** (not inside the container):

```bash
# Apply changes immediately
sudo sysctl -w fs.inotify.max_user_instances=8192
sudo sysctl -w fs.inotify.max_user_watches=524288
```

To make these changes persistent across reboots, use the following lines:

```text
echo "fs.inotify.max_user_instances=8192" | sudo tee -a /etc/sysctl.conf
echo "fs.inotify.max_user_watches=524288" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```
