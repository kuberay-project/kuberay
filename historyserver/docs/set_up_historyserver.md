# History Server Quick Start Guide

## Prerequisites

- Kind
- Docker
- kubectl
- Go 1.24+

## Setup Steps

### 1. Create Kind Cluster

```bash
kind create cluster --image=kindest/node:v1.29.0
```

### 2. Build and Run Ray Operator

Build and deploy the KubeRay operator (binary or deployment). For details, please refer to the
[ray-operator development guide](https://github.com/ray-project/kuberay/blob/master/ray-operator/DEVELOPMENT.md#run-the-operator-inside-the-cluster).

### 3. Deploy & Access RustFS

```bash
kubectl apply -f historyserver/config/rustfs.yaml
```

Use the following command to port-forward the console and API ports. The API port is required only when running the
history server outside the kind cluster.

```bash
kubectl --namespace rustfs-dev port-forward svc/rustfs-service 9001:9001 9000:9000
```

> [!NOTE]
> Get the correct session directory from the RustFS console (`http://localhost:9001/rustfs/console/`).
> Login: `rustfsadmin` / `rustfsadmin`
> See: [RustFS Setup Guide](./set_up_collector.md#option-a-rustfs-s3-compatible)

### 4. Build and Load Collector & History Server Images

If you'd like to run the history server outside the Kind cluster, you don't need to build the history server image.

```bash
make -C historyserver localimage-build
kind load docker-image historyserver:v0.1.0
kind load docker-image collector:v0.1.0
```

### 5. Submit Ray Job

The RayJob creates its own Ray cluster with the collector sidecar and shuts it down
after the job finishes (`shutdownAfterJobFinishes` with a 30s TTL).

```bash
kubectl apply -f historyserver/config/rayjob.yaml
kubectl wait rayjob/rayjob-historyserver --for=jsonpath='{.status.jobStatus}=SUCCEEDED' --timeout=5m
```

### 6. Wait for Cluster Shutdown (Trigger Log Upload)

The cluster shuts down automatically 30s after the job finishes, which flushes logs and
events to storage. To skip the TTL wait, delete the RayJob:

```bash
kubectl delete -f historyserver/config/rayjob.yaml
```

### 8. Create Service Account

```bash
kubectl apply -f historyserver/config/service_account.yaml
```

### 9. Run and Access History Server

#### Deploy In-Cluster History Server

```bash
kubectl apply -f historyserver/config/historyserver.yaml

# Port-forward to access the history server.
kubectl port-forward svc/historyserver 8080:30080
```

> [!IMPORTANT]
> Access to live RayClusters is disabled by default. When disabled, `/clusters` only lists sessions
> already flushed to storage, and `/enter_cluster/.../live` returns 404. To enable access to live
> RayCluster, set `--enable-live-clusters=true`.

#### Run History Server Outside the Kind Cluster

You can also run the history server outside the Kind cluster to accelerate the development iteration and enable
debugging in your own IDE. For example, you can set up `.vscode/launch.json` as follows:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug (historyserver)",
            "type": "go",
            "request": "launch",
            "program": "${workspaceFolder}/historyserver/cmd/historyserver/main.go",
            "cwd": "${workspaceFolder}",
            "args": [
                "--storage-backend=s3",
                "--enable-live-clusters=true",
            ],
            "env": {
                "S3_REGION": "test",
                "S3_ENDPOINT": "localhost:9000",
                "S3_BUCKET": "ray-historyserver",
                "AWS_ACCESS_KEY_ID": "rustfsadmin",
                "AWS_SECRET_ACCESS_KEY": "rustfsadmin",
                "AWS_SESSION_TOKEN": "",
                "S3FORCE_PATH_STYLE": "true",
                "S3DISABLE_SSL": "true"
            }
        }
    ]
}
```

For setting up the `args` and `env` fields, please refer to `spec.template.spec.containers.command` and
`spec.template.spec.containers.env` in `historyserver/config/historyserver.yaml`.

You can also build and run the history server binary directly from the command line:

```bash
# Build the history server binary.
cd historyserver
make buildhistoryserver

# Configure S3 connection via environment variables.
export S3_REGION=test
export S3_ENDPOINT=localhost:9000
export S3_BUCKET=ray-historyserver
export AWS_ACCESS_KEY_ID=rustfsadmin
export AWS_SECRET_ACCESS_KEY=rustfsadmin
export AWS_SESSION_TOKEN=
export S3FORCE_PATH_STYLE=true
export S3DISABLE_SSL=true

# Run the history server.
./output/bin/historyserver \
  --storage-backend=s3 \
  --use-kubernetes-proxy=true \
  --enable-live-clusters=true
```

## API Endpoints

### Health Check

```bash
curl "http://localhost:8080/readz"
curl "http://localhost:8080/livez"
```

### List Clusters

```bash
curl "http://localhost:8080/clusters"
```

### Enter a Session (Dead Cluster)

```bash
SESSION="session_2026-01-11_19-38-40_146706_1"  # Replace with actual session
curl -c ~/cookies.txt "http://localhost:8080/enter_cluster/default/rayjob/rayjob-historyserver/$SESSION"
```

### Dead Cluster Endpoints

```bash
# All Tasks
curl -b ~/cookies.txt "http://localhost:8080/api/v0/tasks"

# Tasks by job_id
curl -b ~/cookies.txt "http://localhost:8080/api/v0/tasks?filter_keys=job_id&filter_predicates==&filter_values=AgAAAA=="

# Task by task_id
curl -b ~/cookies.txt "http://localhost:8080/api/v0/tasks?filter_keys=task_id&filter_predicates==&filter_values=Z6Loz6WgbbP///////////////8CAAAA"

# All Actors
curl -b ~/cookies.txt "http://localhost:8080/logical/actors"

# Single Actor
curl -b ~/cookies.txt "http://localhost:8080/logical/actors/<ACTOR_ID>"

# Nodes
curl -b ~/cookies.txt "http://localhost:8080/nodes?view=summary"
```

### Enter a Session (Live Cluster)

```bash
SESSION="live"
curl -c ~/cookies.txt "http://localhost:8080/enter_cluster/default/rayjob/rayjob-historyserver/$SESSION"
```

If the command returns a "RayCluster not found" error, you need a live cluster before
connecting. Re-submit the sample RayJob — its cluster stays alive for the 30s TTL after
the job finishes:

```bash
kubectl apply -f historyserver/config/rayjob.yaml

# If the RayJob already exists, please delete it first and re-apply
# kubectl delete -f historyserver/config/rayjob.yaml
```

### Live Cluster Endpoints

Switch to live session first, then:

```bash
# All Tasks
curl -b ~/cookies.txt "http://localhost:8080/api/v0/tasks"

# Tasks by job_id
curl -b ~/cookies.txt "http://localhost:8080/api/v0/tasks?filter_keys=job_id&filter_predicates==&filter_values=04000000"

# Task Summarize
curl -b ~/cookies.txt "http://localhost:8080/api/v0/tasks/summarize"

# All Actors
curl -b ~/cookies.txt "http://localhost:8080/logical/actors"

# Single Actor
curl -b ~/cookies.txt "http://localhost:8080/logical/actors/<ACTOR_ID>"

# Nodes Summary
curl -b ~/cookies.txt "http://localhost:8080/nodes?view=summary"

# Jobs
curl -b ~/cookies.txt "http://localhost:8080/api/jobs/"

# Cluster Status
curl -b ~/cookies.txt "http://localhost:8080/api/cluster_status"
```

### Live Cluster with prometheus and grafana

```bash
# Install prometheus and grafana. ref: https://docs.ray.io/en/latest/cluster/kubernetes/k8s-ecosystem/prometheus-grafana.html#step-2-install-kubernetes-prometheus-stack-via-helm-chart
./install/prometheus/install.sh --auto-load-dashboard true

# Apply RayCluster with Grafana setting
kubectl apply -f ray-operator/config/samples/ray-cluster.embed-grafana.yaml

# Get live session cookie. (Port-forward is required)
curl -c ~/cookies.txt "http://localhost:8080/enter_cluster/default/raycluster/raycluster-embed-grafana/live"

# Request to prometheus health endpoint
curl -b ~/cookies.txt http://localhost:8080/api/prometheus_health

# Request to grafana health endpoint
curl -b ~/cookies.txt http://localhost:8080/api/grafana_health
```

After completing the Prometheus and Grafana testing, you can clean up the associated resources using the following commands:

```bash
# 1. Delete the RayCluster
kubectl delete -f ray-operator/config/samples/ray-cluster.embed-grafana.yaml

# 2. Uninstall the kube-prometheus-stack helm chart
helm --namespace prometheus-system uninstall prometheus

# 3. Delete the prometheus-system namespace
kubectl delete namespace prometheus-system
```
