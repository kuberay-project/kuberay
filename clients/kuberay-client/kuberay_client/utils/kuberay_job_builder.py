"""
KubeRay Job Builder - Clean API for constructing RayJob manifests.

Builders assemble plain dictionaries and validate them against the
CRD-generated Pydantic root models. The generated models are internal
implementation details; only the root RayJob class is imported for
validation.

Usage:
    from kuberay_client.utils.kuberay_job_builder import RayJobBuilder, create_ray_job

    # Create a RayJob targeting existing cluster
    job = RayJobBuilder("my-job") \
        .with_entrypoint("python train.py") \
        .with_cluster_selector("my-cluster") \
        .build()

    # Create a RayJob with embedded cluster
    job = RayJobBuilder("standalone-job") \
        .with_entrypoint("python train.py") \
        .with_cluster_spec(
            head_image="rayproject/ray:2.48.0",
            worker_replicas=2,
        ) \
        .with_shutdown_after_finish(True) \
        .build()
"""

from typing import Any, Optional
import json

from kuberay_client.models import RayJob


class RayJobBuilder:
    """Builder for RayJob CRD manifests."""

    def __init__(self, name: str, namespace: str = "default"):
        self.name = name
        self.namespace = namespace
        self.labels: dict[str, str] = {}
        self.annotations: dict[str, str] = {}

        # Spec fields
        self.entrypoint: Optional[str] = None
        self.cluster_selector: Optional[dict[str, str]] = None
        self._ray_cluster_spec: Optional[dict[str, Any]] = None
        self.shutdown_after_finish: bool = False
        self.ttl_seconds: Optional[int] = None
        self.runtime_env_yaml: Optional[str] = None
        self.entrypoint_num_cpus: Optional[float] = None
        self.entrypoint_num_gpus: Optional[float] = None
        self.active_deadline_seconds: Optional[int] = None

    def with_labels(self, labels: dict[str, str]) -> "RayJobBuilder":
        """Add labels to the RayJob metadata."""
        self.labels.update(labels)
        return self

    def with_entrypoint(self, entrypoint: str) -> "RayJobBuilder":
        """Set the job entrypoint command."""
        self.entrypoint = entrypoint
        return self

    def with_cluster_selector(self, cluster_name: str) -> "RayJobBuilder":
        """Target an existing RayCluster by name."""
        self.cluster_selector = {"ray.io/cluster": cluster_name}
        return self

    def with_cluster_spec(
        self,
        head_image: str = "rayproject/ray:2.48.0",
        worker_image: Optional[str] = None,
        worker_replicas: int = 1,
        worker_min_replicas: int = 0,
        worker_max_replicas: int = 4,
        ray_version: str = "2.48.0",
    ) -> "RayJobBuilder":
        """Create an embedded RayCluster for this job."""
        if worker_image is None:
            worker_image = head_image

        cluster_spec: dict[str, Any] = {
            "rayVersion": ray_version,
            "headGroupSpec": {
                "rayStartParams": {"dashboard-host": "0.0.0.0"},
                "template": {
                    "spec": {
                        "containers": [
                            {"name": "ray-head", "image": head_image}
                        ]
                    }
                },
            },
        }

        if worker_replicas > 0:
            cluster_spec["workerGroupSpecs"] = [
                {
                    "groupName": "default-worker",
                    "replicas": worker_replicas,
                    "minReplicas": worker_min_replicas,
                    "maxReplicas": worker_max_replicas,
                    "template": {
                        "spec": {
                            "containers": [
                                {"name": "ray-worker", "image": worker_image}
                            ]
                        }
                    },
                }
            ]

        self._ray_cluster_spec = cluster_spec
        return self

    def with_shutdown_after_finish(self, shutdown: bool = True) -> "RayJobBuilder":
        """Delete the cluster after job completes."""
        self.shutdown_after_finish = shutdown
        return self

    def with_ttl_seconds(self, seconds: int) -> "RayJobBuilder":
        """TTL for cleanup after job finishes."""
        self.ttl_seconds = seconds
        return self

    def with_runtime_env(self, runtime_env_yaml: str) -> "RayJobBuilder":
        """Set runtime environment YAML."""
        self.runtime_env_yaml = runtime_env_yaml
        return self

    def with_resources(
        self,
        num_cpus: Optional[float] = None,
        num_gpus: Optional[float] = None
    ) -> "RayJobBuilder":
        """Set entrypoint resource requirements."""
        if num_cpus is not None:
            self.entrypoint_num_cpus = num_cpus
        if num_gpus is not None:
            self.entrypoint_num_gpus = num_gpus
        return self

    def with_deadline(self, seconds: int) -> "RayJobBuilder":
        """Set active deadline in seconds."""
        self.active_deadline_seconds = seconds
        return self

    def build(self) -> dict:
        """Build and validate the RayJob manifest dict."""
        if not self.entrypoint:
            raise ValueError("entrypoint is required")

        if not self.cluster_selector and not self._ray_cluster_spec:
            raise ValueError("Either cluster_selector or cluster_spec is required")

        spec: dict[str, Any] = {
            "entrypoint": self.entrypoint,
            "submissionMode": "K8sJobMode",
        }

        if self.cluster_selector:
            spec["clusterSelector"] = self.cluster_selector
        if self._ray_cluster_spec:
            spec["rayClusterSpec"] = self._ray_cluster_spec
        if self.shutdown_after_finish:
            spec["shutdownAfterJobFinishes"] = True
        if self.ttl_seconds is not None:
            spec["ttlSecondsAfterFinished"] = self.ttl_seconds
        if self.runtime_env_yaml:
            spec["runtimeEnvYAML"] = self.runtime_env_yaml
        if self.entrypoint_num_cpus is not None:
            spec["entrypointNumCpus"] = self.entrypoint_num_cpus
        if self.entrypoint_num_gpus is not None:
            spec["entrypointNumGpus"] = self.entrypoint_num_gpus
        if self.active_deadline_seconds is not None:
            spec["activeDeadlineSeconds"] = self.active_deadline_seconds

        metadata: dict[str, Any] = {
            "name": self.name,
            "namespace": self.namespace,
        }
        if self.labels:
            metadata["labels"] = self.labels
        if self.annotations:
            metadata["annotations"] = self.annotations

        manifest: dict[str, Any] = {
            "apiVersion": "ray.io/v1",
            "kind": "RayJob",
            "metadata": metadata,
            "spec": spec,
        }

        # Validate against the CRD-generated Pydantic model
        RayJob.model_validate(manifest)

        return manifest

    def to_dict(self) -> dict:
        """Build and convert to dict for K8s API (alias for build)."""
        return self.build()

    def to_json(self, indent: int = 2) -> str:
        """Build and convert to JSON string."""
        return json.dumps(self.build(), indent=indent)


def create_ray_job(
    name: str,
    entrypoint: str,
    namespace: str = "default",
    cluster_name: Optional[str] = None,
    worker_replicas: int = 1,
    shutdown_after_finish: bool = True,
    head_image: str = "rayproject/ray:2.48.0",
    worker_image: Optional[str] = None,
    worker_min_replicas: int = 0,
    worker_max_replicas: int = 4,
    ray_version: str = "2.48.0",
    ttl_seconds: Optional[int] = None,
    runtime_env_yaml: Optional[str] = None,
    num_cpus: Optional[float] = None,
    num_gpus: Optional[float] = None,
    active_deadline_seconds: Optional[int] = None,
    labels: Optional[dict] = None,
) -> dict:
    """
    Create a RayJob manifest dict.

    Args:
        name: Job name
        entrypoint: Command to run
        namespace: K8s namespace
        cluster_name: Target existing cluster (if None, creates ephemeral cluster)
        worker_replicas: Number of workers (for ephemeral cluster)
        shutdown_after_finish: Delete cluster when job completes
        head_image: Ray image for the head node (ephemeral cluster only)
        worker_image: Ray image for worker nodes; defaults to head_image
        worker_min_replicas: Minimum worker replicas for autoscaling
        worker_max_replicas: Maximum worker replicas for autoscaling
        ray_version: Ray version string (ephemeral cluster only)
        ttl_seconds: Seconds to keep the job resource after it finishes
        runtime_env_yaml: Runtime environment YAML string
        num_cpus: Number of CPUs reserved for the entrypoint
        num_gpus: Number of GPUs reserved for the entrypoint
        active_deadline_seconds: Hard deadline for the job in seconds
        labels: Metadata labels to attach to the RayJob

    Returns:
        K8s manifest dict ready for API submission
    """
    builder = RayJobBuilder(name, namespace).with_entrypoint(entrypoint)

    if labels:
        builder.with_labels(labels)

    if cluster_name:
        builder.with_cluster_selector(cluster_name)
    else:
        builder.with_cluster_spec(
            head_image=head_image,
            worker_image=worker_image,
            worker_replicas=worker_replicas,
            worker_min_replicas=worker_min_replicas,
            worker_max_replicas=worker_max_replicas,
            ray_version=ray_version,
        )
        builder.with_shutdown_after_finish(shutdown_after_finish)

    if ttl_seconds is not None:
        builder.with_ttl_seconds(ttl_seconds)
    if runtime_env_yaml is not None:
        builder.with_runtime_env(runtime_env_yaml)
    if num_cpus is not None or num_gpus is not None:
        builder.with_resources(num_cpus=num_cpus, num_gpus=num_gpus)
    if active_deadline_seconds is not None:
        builder.with_deadline(active_deadline_seconds)

    return builder.build()
