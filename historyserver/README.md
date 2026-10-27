# KubeRay History Server

This project is under active development.
See [#ray-history-server](https://app.slack.com/client/TN4768NRM/C09QLLU8HTL) channel to provide feedback.

Ray History Server is a service for collecting, storing, and viewing historical logs and metadata from Ray clusters.
It provides a web interface to explore the history of Ray jobs, tasks, actors, and other cluster activities.

## Components

The History Server consists of two main components:

1. **Collector**: Runs as a sidecar container in Ray clusters to collect logs and metadata
2. **History Server**: Serves a Ray Dashboard compatible HTTP API and ingests cluster sessions' events on demand

## Building

### Prerequisites

- Go 1.19 or higher
- Docker (for building container images)
- Make

### Building Binaries

To build the binaries locally:

```bash
make build
```

This will generate two binaries in the `output/bin/` directory:

- `collector`: The collector service that runs alongside Ray nodes
- `historyserver`: The main history server service

You can also build individual components:

```bash
make buildcollector      # Build only the collector
make buildhistoryserver  # Build only the history server
```

### Building Docker Images

To build a Docker image:

```bash
make localimage
```

This creates a Docker image named `historyserver:laster` with both binaries and necessary assets.

For multi-platform builds, you can use:

```bash
docker buildx build -t <image-name>:<tag> --platform linux/amd64,linux/arm64 . --push
```

## Configuration

### History Server Configuration

The history server can be configured using command-line flags:

- `--storage-backend`: Storage backend type (e.g., "s3", "aliyunoss", "localtest")
- `--ray-root-dir`: Root directory for Ray logs
- `--kubeconfigs`: Path to kubeconfig file(s) for accessing Kubernetes clusters
- `--dashboard-dir`: Directory containing dashboard assets (default: "/dashboard")
- `--storage-backend-config-path`: Path to storage backend configuration file
- `--enable-live-clusters`: Serve RayClusters that are still running by reverse-proxying to their
  head dashboard (default: `false`)
- `--namespace`: Restrict live and archived session access to one namespace (default: all namespaces)
- `--namespace-prefix`: Restrict live and archived session access to namespaces beginning with this
  literal prefix; mutually exclusive with `--namespace` (default: no prefix restriction)

> [!WARNING]
> The history server does not authenticate its own callers, and the RayCluster it proxies to is
> chosen from a client-supplied cookie. With `--enable-live-clusters` enabled, anyone who can reach
> the history server can reach the Ray Dashboard API of every RayCluster the history server can
> access. Only turn it on where that access is already restricted by other means.

#### Namespace scope

Set `--namespace=team-a` to serve only that namespace. When live discovery is enabled, its
Kubernetes list request is namespaced, so it can use a Role/RoleBinding rather than a cluster-wide
reader. Existing label selectors still apply. Access to other namespaces returns HTTP 403.

Set `--namespace-prefix=team-` to serve namespaces such as `team-a` and `team-b`, including matching
namespaces created after startup. This is a literal prefix, not a glob: use `team-`, not `team-*`.
Prefix discovery lists RayClusters across all namespaces and filters the results in the server;
Kubernetes RBAC cannot express a namespace-name prefix, so this mode still requires cluster-wide
RayCluster list permission. It does not require listing Namespace objects.

Both options filter live clusters and stored sessions before the selector's archive limit is applied.
They also check direct `/enter_cluster` links and cookie-based dashboard requests, including cached
live routes. With neither option set, namespace access is unchanged.

The scope applies to every caller of this server instance. It does not authenticate users or implement
per-user authorization. It also does not restrict which object-storage metadata the reader scans:
use storage credentials and storage-root configuration to control the server's underlying access.
Collectors and readable archives are still required for historical replay.

### Collector Configuration

The collector can be configured using command-line flags:

- `--role`: Node role ("Head" or "Worker")
- `--storage-backend`: Storage backend type (e.g., "s3", "aliyunoss")
- `--ray-cluster-name`: Name of the Ray cluster
- `--ray-cluster-namespace`: Namespace of the Ray cluster
- `--ray-root-dir`: Root directory for Ray logs
- `--log-batching`: Number of log entries to batch before writing
- `--events-port`: Port for the events server
- `--push-interval`: Interval between pushes to storage
- `--storage-backend-config-path`: Path to storage backend configuration file

And using environment variables:

- `RAY_COLLECTOR_ROTATED_LOG_SCAN_INTERVAL`: How often the collector scans the active session
  log directory for completed Ray log rotation backups (default: `30s`)

#### Rotated log collection

Ray keeps only a limited number of local rotation backups per log stream (`raylet.out.1`,
`raylet.out.2`, ...) and overwrites the oldest as the ring advances. The collector periodically
scans the active session for those backups and uploads each one before Ray can overwrite it,
highest rotation index first because that is the generation Ray evicts next.

Collection is best effort: a backup Ray removes before the collector reaches it is lost, so set
`RAY_COLLECTOR_ROTATED_LOG_SCAN_INTERVAL` shorter than the time Ray takes to cycle through its
rotation backups. That time depends on `RAY_ROTATION_MAX_BYTES`, `RAY_ROTATION_BACKUP_COUNT` and
how fast the node writes logs; see the
[Ray log rotation docs](https://docs.ray.io/en/latest/ray-observability/user-guides/configure-logging.html#log-rotation).

Rotation indexes are reused as the ring advances, so an uploaded backup is named after the
generation it holds rather than its index: `raylet.out.2` is stored as
`raylet.rotated.<modification-time-ns>-<inode>.out`. Rotated objects are listed and can be
fetched by explicit filename; `task_id`, `actor_id` and `pid` lookups continue to resolve the
canonical active log, since one worker stream can span several generations.

## Supported Storage Backends

History Server supports multiple storage backends:

1. **S3**: For AWS S3 or S3-compatible storage (e.g. RustFS)
2. **Aliyun OSS**: For Alibaba Cloud Object Storage Service
3. **Local Test**: For local testing and development

Each backend requires specific configuration parameters passed through environment variables or configuration files.

## Running

### Running the History Server

```bash
./output/bin/historyserver \
  --storage-backend=s3 \
  --ray-root-dir=/path/to/logs
```

### Running the Collector

```bash
./output/bin/collector \
  --role=Head \
  --storage-backend=s3 \
  --ray-cluster-name=my-cluster \
  --ray-root-dir=/path/to/logs
```

## Development

### Code Structure

- `cmd/`: Main applications (collector and historyserver)
- `pkg/`: Core logic for storage backends and collection
- `pkg/collector/`: Collector-specific code
- `pkg/storage/`: Storage backend implementations
- `dashboard/`: Web UI files

### Testing

To run tests:

```bash
make test
```

### Linting

To run lint checks:

```bash
make lint
```

## Smoke Tests

### 1. Deploy History Server

Apply RustFS and the History Server manifests:

```bash
kubectl apply -f historyserver/config/rustfs.yaml
kubectl apply -f historyserver/config/service_account.yaml
kubectl apply -f historyserver/config/historyserver.yaml
```

Port-forward the HS service:

```bash
kubectl port-forward svc/historyserver 8080:30080
```

### 2. Generate a Dead Session

Submit the sample RayJob; it creates its own cluster with the collector sidecar, runs a
deterministic workload, and shuts the cluster down after the job finishes:

```bash
kubectl apply -f historyserver/config/rayjob.yaml
kubectl wait rayjob/rayjob-historyserver --for=jsonpath='{.status.jobStatus}=SUCCEEDED' --timeout=5m

# The cluster shuts down automatically 30s after the job finishes
# (shutdownAfterJobFinishes + ttlSecondsAfterFinished), producing a 'dead' session.
# Delete the RayJob only to skip the TTL wait:
kubectl delete -f historyserver/config/rayjob.yaml

# Discover the session name. /clusters lists dead sessions, plus live ones when
# --enable-live-clusters is set; dead sessions carry the `session_*` name you'll
# feed into /enter_cluster.
curl -sS http://localhost:8080/clusters
```

### 3. Cold Path (first visit)

Trigger the lazy load synchronously. Replace `<session>` with the session name from §2:

```bash
time curl -s -o /dev/null \
  http://localhost:8080/enter_cluster/default/rayjob/rayjob-historyserver/<session>
```

> [!NOTE]
> Cold path runs synchronously: K8s probe + event parse. Expect this to take seconds. Subsequent endpoint calls
> (`/api/v0/jobs`, `/api/v0/tasks/...`) read from in-memory state populated by this call.

### 4. Warm Path (subsequent visits on same replica)

Re-enter the same cluster:

```bash
time curl -s -o /dev/null \
  http://localhost:8080/enter_cluster/default/rayjob/rayjob-historyserver/<session>
```

> [!NOTE]
> Warm path returns immediately — SessionLoader's loaded-session set fast-paths the request without re-parsing. If the HS
> process restarts, the next visit returns to cold-path latency.

### 5. Tail Logs

```bash
kubectl logs -f -l app=historyserver --tail=50
```

You should see one `current eventFileList for cluster ...` line (and the per-file `Reading event file: ...` entries)
per first-time cold-path call. Warm-path calls produce no parse log lines.

## Deployment

History Server can be deployed in Kubernetes using the manifests in the `config/` directory.
Examples are provided for different storage backends including S3 (RustFS) and Aliyun OSS.
