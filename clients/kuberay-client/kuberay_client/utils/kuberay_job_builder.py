"""
KubeRay Job Builder - Clean API for constructing RayJob manifests.

Builders assemble plain dictionaries and validate them against the
CRD-generated Pydantic root models. The generated models are internal
implementation details; only the root RayJob class is imported for
validation.

Usage:
    from kuberay_client.utils.kuberay_job_builder import RayJobBuilder

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
        worker_max_replicas: Optional[int] = None,
        ray_version: str = "2.48.0",
    ) -> "RayJobBuilder":
        """Create an embedded RayCluster for this job.

        worker_max_replicas defaults to worker_replicas. Explicit bounds must
        satisfy 0 <= worker_min_replicas <= worker_replicas <= worker_max_replicas.
        A worker_replicas value of zero creates a head-only cluster.
        """
        if worker_max_replicas is None:
            worker_max_replicas = worker_replicas
        if not 0 <= worker_min_replicas <= worker_replicas <= worker_max_replicas:
            raise ValueError(
                "Worker replicas must satisfy "
                "0 <= worker_min_replicas <= worker_replicas <= worker_max_replicas"
            )

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
