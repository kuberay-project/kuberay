import json

import pytest
from pydantic import ValidationError

from kuberay_client.models import RayJob
from kuberay_client.utils.kuberay_job_builder import RayJobBuilder


def test_existing_cluster_manifest():
    job = (
        RayJobBuilder("test-job", namespace="jobs")
        .with_entrypoint("python train.py")
        .with_cluster_selector("my-cluster")
        .with_labels({"team": "ml"})
        .with_runtime_env("working_dir: https://example.com/code.zip")
        .with_resources(num_cpus=2, num_gpus=1)
        .with_deadline(600)
        .build()
    )

    assert job == {
        "apiVersion": "ray.io/v1",
        "kind": "RayJob",
        "metadata": {
            "name": "test-job",
            "namespace": "jobs",
            "labels": {"team": "ml"},
        },
        "spec": {
            "entrypoint": "python train.py",
            "submissionMode": "K8sJobMode",
            "clusterSelector": {"ray.io/cluster": "my-cluster"},
            "runtimeEnvYAML": "working_dir: https://example.com/code.zip",
            "entrypointNumCpus": 2,
            "entrypointNumGpus": 1,
            "activeDeadlineSeconds": 600,
        },
    }


@pytest.mark.parametrize("worker_replicas", [1, 8])
def test_worker_max_defaults_to_requested_replicas(worker_replicas):
    job = (
        RayJobBuilder("training-job")
        .with_entrypoint("python train.py")
        .with_cluster_spec(worker_replicas=worker_replicas)
        .with_shutdown_after_finish()
        .with_ttl_seconds(0)
        .build()
    )

    assert job["spec"]["shutdownAfterJobFinishes"] is True
    assert job["spec"]["ttlSecondsAfterFinished"] == 0
    cluster_spec = job["spec"]["rayClusterSpec"]
    worker = cluster_spec["workerGroupSpecs"][0]
    assert worker["replicas"] == worker_replicas
    assert worker["minReplicas"] == 0
    assert worker["maxReplicas"] == worker_replicas
    assert worker["template"]["spec"]["containers"][0]["image"] == (
        cluster_spec["headGroupSpec"]["template"]["spec"]["containers"][0]["image"]
    )


def test_explicit_worker_bounds_and_image():
    job = (
        RayJobBuilder("training-job")
        .with_entrypoint("python train.py")
        .with_cluster_spec(
            worker_replicas=8,
            worker_min_replicas=2,
            worker_max_replicas=12,
            worker_image="example.com/custom-worker:latest",
        )
        .build()
    )

    worker = job["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]
    assert (worker["minReplicas"], worker["replicas"], worker["maxReplicas"]) == (2, 8, 12)
    assert worker["template"]["spec"]["containers"][0]["image"] == (
        "example.com/custom-worker:latest"
    )


@pytest.mark.parametrize(
    "bounds",
    [
        {"worker_replicas": 8, "worker_max_replicas": 4},
        {"worker_replicas": 2, "worker_min_replicas": 3},
        {"worker_min_replicas": -1},
        {"worker_replicas": -1},
        {"worker_max_replicas": -1},
        {"worker_replicas": 0, "worker_min_replicas": 1},
    ],
)
def test_invalid_worker_bounds(bounds):
    with pytest.raises(ValueError, match="Worker replicas must satisfy"):
        RayJobBuilder("training-job").with_cluster_spec(**bounds)


def test_head_only_cluster():
    job = (
        RayJobBuilder("head-only-job")
        .with_entrypoint("python train.py")
        .with_cluster_spec(worker_replicas=0)
        .build()
    )

    assert "workerGroupSpecs" not in job["spec"]["rayClusterSpec"]


def test_missing_entrypoint():
    with pytest.raises(ValueError, match="entrypoint is required"):
        RayJobBuilder("test-job").with_cluster_selector("my-cluster").build()


def test_missing_cluster():
    with pytest.raises(ValueError, match="Either cluster_selector or cluster_spec is required"):
        RayJobBuilder("test-job").with_entrypoint("python train.py").build()


def test_builder_validates_against_generated_model():
    builder = (
        RayJobBuilder("test-job")
        .with_entrypoint("python train.py")
        .with_cluster_selector("my-cluster")
        .with_resources(num_cpus="invalid")
    )

    with pytest.raises(ValidationError, match="entrypointNumCpus"):
        builder.build()


def test_serialization():
    builder = (
        RayJobBuilder("test-job")
        .with_entrypoint("python train.py")
        .with_cluster_spec()
    )

    assert json.loads(builder.to_json()) == builder.to_dict() == builder.build()


@pytest.mark.parametrize("quantity", [1, "1", "500m", "1Gi"])
def test_generated_model_accepts_resource_quantities(quantity):
    job = (
        RayJobBuilder("test-job")
        .with_entrypoint("python train.py")
        .with_cluster_spec()
        .build()
    )
    container = job["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]["containers"][0]
    container["resources"] = {"limits": {"cpu": quantity}, "requests": {"cpu": quantity}}

    model = RayJob.model_validate(job)

    assert model.model_dump(mode="json", exclude_unset=True) == job


def test_generated_model_rejects_invalid_resource_quantity():
    job = (
        RayJobBuilder("test-job")
        .with_entrypoint("python train.py")
        .with_cluster_spec()
        .build()
    )
    container = job["spec"]["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]["containers"][0]
    container["resources"] = {"limits": {"memory": "invalid"}}

    with pytest.raises(ValidationError):
        RayJob.model_validate(job)
