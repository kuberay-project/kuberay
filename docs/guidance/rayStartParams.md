<!-- markdownlint-disable MD013 -->
# Default Ray Start Parameters for KubeRay

This document outlines the default settings for `rayStartParams` in KubeRay.

## Options Exclusive to the Head Pod

- `--dashboard-host`: Host for the dashboard server, either `localhost` (127.0.0.1) or `0.0.0.0`.
The latter setting exposes the Ray dashboard outside the Ray cluster, which is required when [ingress](https://github.com/ray-project/kuberay/blob/master/docs/guidance/ingress.md) is utilized for Ray cluster access.
The default value for both Ray and KubeRay 0.5.0 is `localhost`. Please note that this will change for versions of KubeRay later than 0.5.0, where the default setting will be `0.0.0.0`.

- `--no-monitor` (Modification is not recommended):
  - Ray autoscaler supports various node providers such as AWS, GCP, Azure, and Kubernetes. However, the default autoscaler is not compatible with Kubernetes. Therefore, when [KubeRay autoscaling](https://github.com/ray-project/kuberay/blob/master/docs/guidance/autoscaler.md) is enabled (i.e. `EnableInTreeAutoscaling` is true), KubeRay disables the monitor process via setting `--no-monitor` to true and injects a sidecar container for KubeRay autoscaler. See [PR #13505](https://github.com/ray-project/ray/pull/13505) for more details.
  - Please note that the monitor process serves not only for autoscaling but also for observability, such as Prometheus metrics. Considering this, it is reasonable to disable the Kubernetes-incompatible autoscaler regardless of the value of `EnableInTreeAutoscaling`. To achieve this, we can launch the monitor process without autoscaling functionality by setting the autoscaler to READONLY mode. If `autoscaling-option` is not set, the autoscaler will default to READONLY mode.

- `--port`: Port for the GCS server. The port is set to `6379` by default. Please ensure that this value matches the `gcs-server` container port in Ray head container.

- `--redis-password`: Redis password for an external Redis, necessary when [fault tolerance](https://github.com/ray-project/kuberay/blob/master/docs/guidance/gcs-ft.md) is enabled.
The default value is `""` after Ray 2.3.0. See [#929](https://github.com/ray-project/kuberay/pull/929) for more details.

## Options Exclusive to Worker Pods

- `--address`: Address of the GCS server. Worker pods utilize this address to establish a connection with the Ray cluster. By default, this address takes the form `<FQDN>:<GCS_PORT>`. The `GCS_PORT` corresponds to the value set in the `--port` option. For more insights on Fully Qualified Domain Name (FQDN), refer to [PR #938](https://github.com/ray-project/kuberay/pull/938) and [PR #951](https://github.com/ray-project/kuberay/pull/951).

## Options Applicable to Both Head and Worker Pods

- `--block`: This option blocks the ray start command indefinitely. It will be automatically set by KubeRay. See [PR #675](https://github.com/ray-project/kuberay/pull/675) for more details. Modification is not recommended.

- `--memory`: Amount of memory on this Ray node, in bytes. See [PR #170](https://github.com/ray-project/kuberay/pull/170).
  - When `rayStartParams` omits `memory` and the Ray container has a memory limit, KubeRay fills `--memory` from that limit. The value is the limit in bytes; a limit of `5G` becomes `5000000000`. KubeRay injects that number as an explicit `--memory` and does not reduce it for the object store. If the container has no memory limit, KubeRay leaves `--memory` unset and Ray auto-detects memory.
  - An explicit `--memory` does not cause Ray to subtract object-store memory. Ray subtracts the object store only when it auto-detects memory because `--memory` is absent. On that path Ray assigns about 70% of available memory to the `memory` resource and about 30% to the object store ([Resources](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html)). KubeRay's injected flag takes the explicit path, so Ray keeps the full limit as schedulable `memory` and still allocates object-store memory separately. The object store defaults to 30% of the memory available when the node starts (total memory minus memory already in use), capped by `/dev/shm` and by Ray's object-store maximum ([Memory Management](https://docs.ray.io/en/latest/ray-core/scheduling/memory-management.html)). Unless `plasma-directory` is set, KubeRay mounts an in-memory `emptyDir` at `/dev/shm` for that store and sizes the volume from the container memory limit, or from the memory request when no limit is set. Both the Ray `memory` resource and the `/dev/shm` object store count against the pod's memory cgroup, so tasks scheduled up to the advertised `memory` can push the pod past its limit and be OOMKilled.
  - A worker with `limits.memory: 5G` (5,000,000,000 bytes) and no `memory` in `rayStartParams` starts with `--memory=5000000000`. Ray reports `memory` of 5e9 plus roughly 1.46e9 bytes of object-store memory. The reported `object_store_memory` was 1,458,823,987, which is 30% of the memory still free at startup rather than 30% of the full 5e9 limit. Those two allocations sum to about 6.46e9 bytes on a 5e9 cgroup limit. Every other memory limit follows the same injection: `--memory` is the whole limit, and the object store is an additional allocation.
  - Set `memory` and `object-store-memory` in `rayStartParams` so their sum fits in the container memory limit, with room left for Ray system processes. Both values are integer byte strings passed to `ray start`, not Kubernetes quantities such as `5Gi`. For the 5G worker above, `memory: "3500000000"` and `object-store-memory: "1000000000"` sum to 4.5e9 and leave about 0.5e9 bytes of headroom. A group-level `resources.memory` value overrides `rayStartParams.memory` and is also passed through as an explicit `--memory`, so keep that quantity inside the same sum. If `memory` stays unset, raising the container memory limit causes KubeRay to inject `--memory` equal to the new limit, and the object store is still allocated on top of it.

    ```yaml
    # Fields on a workerGroupSpec or headGroupSpec. The container limit stays 5G.
    rayStartParams:
      memory: "3500000000"
      object-store-memory: "1000000000"
    template:
      spec:
        containers:
        - name: ray-worker
          resources:
            limits:
              memory: "5G"
    ```

- `--metrics-export-port`: Port for exposing Ray metrics through a Prometheus endpoint. The port is set to `8080` by default. Please ensure that this value matches the `metrics` container port if you need to customize it. See [PR #954](https://github.com/ray-project/kuberay/pull/954) and [prometheus-grafana doc](https://github.com/ray-project/kuberay/blob/master/docs/guidance/prometheus-grafana.md) for more details.

- `--num-cpus`: Number of logical CPUs on this Ray node. Default is determined by Ray container resource limits. Modify Ray container resource limits instead of this option. See [PR #170](https://github.com/ray-project/kuberay/pull/170). However, it is sometimes useful to override this autodetected value. For example, setting `num-cpus:"0"` for the Ray head pod will prevent Ray workloads with non-zero CPU requirements from being scheduled on the head.

- `--num-gpus`: Number of GPUs on this Ray node. Default is determined by Ray container resource limits. Modify Ray container resource limits instead of this option. See [PR #170](https://github.com/ray-project/kuberay/pull/170).
