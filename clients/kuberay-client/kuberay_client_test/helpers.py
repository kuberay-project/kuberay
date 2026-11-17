import time
from kuberay_client import constants


def create_job_with_cluster_selector(
    job_name,
    namespace,
    cluster_name,
    entrypoint="python -c \"import ray; ray.init(); @ray.remote\ndef hello(): return 'Hello from Ray!'; print(ray.get(hello.remote()))\"",
    labels=None,
):
    job_body = {
        "apiVersion": constants.GROUP + "/" + constants.JOB_VERSION,
        "kind": constants.JOB_KIND,
        "metadata": {
            "name": job_name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": job_name,
                "app.kubernetes.io/managed-by": "kuberay",
            },
        },
        "spec": {
            "clusterSelector": {
                "ray.io/cluster": cluster_name,
            },
            "entrypoint": entrypoint,
            "submissionMode": "K8sJobMode",
        },
    }

    # Add any additional labels if provided
    if labels:
        job_body["metadata"]["labels"].update(labels)

    return job_body


def create_job_with_ray_cluster_spec(
    job_name,
    namespace,
    entrypoint="python -c \"import ray; ray.init(); @ray.remote\ndef hello(): return 'Hello from Ray!'; print(ray.get(hello.remote()))\"",
    labels=None,
):
    job_body = {
        "apiVersion": constants.GROUP + "/" + constants.JOB_VERSION,
        "kind": constants.JOB_KIND,
        "metadata": {
            "name": job_name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": job_name,
                "app.kubernetes.io/managed-by": "kuberay",
            },
        },
        "spec": {
            "rayClusterSpec": {
                "headGroupSpec": {
                    "serviceType": "ClusterIP",
                    "replicas": 1,
                    "rayStartParams": {
                        "dashboard-host": "0.0.0.0",
                    },
                    "template": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "ray-head",
                                    "image": "rayproject/ray:2.48.0",
                                    "ports": [
                                        {"containerPort": 6379, "name": "gcs"},
                                        {
                                            "containerPort": 8265,
                                            "name": "dashboard",
                                        },
                                        {
                                            "containerPort": 10001,
                                            "name": "client",
                                        },
                                    ],
                                    "resources": {
                                        "limits": {
                                            "cpu": "1",
                                            "memory": "2Gi",
                                        },
                                        "requests": {
                                            "cpu": "500m",
                                            "memory": "1Gi",
                                        },
                                    },
                                }
                            ]
                        }
                    },
                },
                "workerGroupSpecs": [
                    {
                        "groupName": "small-worker",
                        "replicas": 1,
                        "rayStartParams": {
                            "num-cpus": "1",
                        },
                        "template": {
                            "spec": {
                                "containers": [
                                    {
                                        "name": "ray-worker",
                                        "image": "rayproject/ray:2.48.0",
                                        "resources": {
                                            "limits": {
                                                "cpu": "1",
                                                "memory": "1Gi",
                                            },
                                            "requests": {
                                                "cpu": "500m",
                                                "memory": "512Mi",
                                            },
                                        },
                                    }
                                ]
                            }
                        },
                    }
                ],
            },
            "entrypoint": entrypoint,
            "submissionMode": "K8sJobMode",
            "shutdownAfterJobFinishes": True,
        },
    }

    if labels:
        job_body["metadata"]["labels"].update(labels)

    return job_body
