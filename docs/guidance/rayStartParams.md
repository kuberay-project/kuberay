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

- `--memory`: Amount of memory on this Ray node. Default is determined by Ray container resource limits. Modify Ray container resource limits instead of this option. See [PR #170](https://github.com/ray-project/kuberay/pull/170).

- `--metrics-export-port`: Port for exposing Ray metrics through a Prometheus endpoint. The port is set to `8080` by default. Please ensure that this value matches the `metrics` container port if you need to customize it. See [PR #954](https://github.com/ray-project/kuberay/pull/954) and [prometheus-grafana doc](https://github.com/ray-project/kuberay/blob/master/docs/guidance/prometheus-grafana.md) for more details.

- `--num-cpus`: Number of logical CPUs on this Ray node. Default is determined by Ray container resource limits. Modify Ray container resource limits instead of this option. See [PR #170](https://github.com/ray-project/kuberay/pull/170). However, it is sometimes useful to override this autodetected value. For example, setting `num-cpus:"0"` for the Ray head pod will prevent Ray workloads with non-zero CPU requirements from being scheduled on the head.

- `--num-gpus`: Number of GPUs on this Ray node. Default is determined by Ray container resource limits. Modify Ray container resource limits instead of this option. See [PR #170](https://github.com/ray-project/kuberay/pull/170).
