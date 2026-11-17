import json
from os import path
import os
import sys


"""
in case you are working directly with the source, and don't wish to
install the module with pip install, you can directly import the packages by uncommenting the following code.
"""

"""
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sibling_dirs = [
    d for d in os.listdir(parent_dir) if os.path.isdir(os.path.join(parent_dir, d))
]
for sibling_dir in sibling_dirs:
    sys.path.append(os.path.join(parent_dir, sibling_dir))
"""
from kuberay_client import kuberay_cluster_api

cluster_body: dict = {
    "apiVersion": "ray.io/v1",
    "kind": "RayCluster",
    "metadata": {
        "labels": {"controller-tools.k8s.io": "1.0", "demo-cluster": "yes"},
        "name": "raycluster-mini-raw",
    },
    "spec": {
        "rayVersion": "2.46.0",
        "headGroupSpec": {
            "rayStartParams": {
                "dashboard-host": "0.0.0.0",
                "num-cpus": "1",
            },
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "ray-head",
                            "image": "rayproject/ray:2.46.0",
                            "resources": {
                                "limits": {"cpu": 1, "memory": "2Gi"},
                                "requests": {"cpu": "500m", "memory": "2Gi"},
                            },
                            "ports": [
                                {"containerPort": 6379, "name": "gcs-server"},
                                {"containerPort": 8265, "name": "dashboard"},
                                {"containerPort": 10001, "name": "client"},
                            ],
                        }
                    ]
                }
            },
        },
    },
}


cluster_body2: dict = {
    "apiVersion": "ray.io/v1",
    "kind": "RayCluster",
    "metadata": {
        "labels": {"controller-tools.k8s.io": "1.0", "demo-cluster": "yes"},
        "name": "raycluster-complete-raw",
    },
    "spec": {
        "rayVersion": "2.46.0",
        "headGroupSpec": {
            "rayStartParams": {"dashboard-host": "0.0.0.0"},
            "template": {
                "metadata": {"labels": {}},
                "spec": {
                    "containers": [
                        {
                            "name": "ray-head",
                            "image": "rayproject/ray:2.46.0",
                            "ports": [
                                {"containerPort": 6379, "name": "gcs"},
                                {"containerPort": 8265, "name": "dashboard"},
                                {"containerPort": 10001, "name": "client"},
                            ],
                            "lifecycle": {
                                "preStop": {
                                    "exec": {"command": ["/bin/sh", "-c", "ray stop"]}
                                }
                            },
                            "volumeMounts": [
                                {"mountPath": "/tmp/ray", "name": "ray-logs"}
                            ],
                            "resources": {
                                "limits": {"cpu": "1", "memory": "2G"},
                                "requests": {"cpu": "500m", "memory": "2G"},
                            },
                        }
                    ],
                    "volumes": [{"name": "ray-logs", "emptyDir": {}}],
                },
            },
        },
        "workerGroupSpecs": [
            {
                "replicas": 1,
                "minReplicas": 1,
                "maxReplicas": 10,
                "groupName": "small-group",
                "rayStartParams": {},
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "ray-worker",
                                "image": "rayproject/ray:2.46.0",
                                "lifecycle": {
                                    "preStop": {
                                        "exec": {
                                            "command": ["/bin/sh", "-c", "ray stop"]
                                        }
                                    }
                                },
                                "volumeMounts": [
                                    {"mountPath": "/tmp/ray", "name": "ray-logs"}
                                ],
                                "resources": {
                                    "limits": {"cpu": "2", "memory": "3G"},
                                    "requests": {"cpu": "1500m", "memory": "3G"},
                                },
                            }
                        ],
                        "volumes": [{"name": "ray-logs", "emptyDir": {}}],
                    }
                },
            }
        ],
    },
}


def main():

    print("starting cluster handler...")

    my_kube_ray_api = kuberay_cluster_api.RayClusterApi()

    my_kube_ray_api.create_ray_cluster(body=cluster_body)

    my_kube_ray_api.create_ray_cluster(body=cluster_body2)

    # the rest of the code is simply to fetch, print and cleanup the created cluster
    kube_ray_cluster = my_kube_ray_api.get_ray_cluster(
        name=cluster_body["metadata"]["name"], k8s_namespace="default"
    )

    if kube_ray_cluster:
        print("printing the raycluster json representation...")
        json_formatted_str = json.dumps(kube_ray_cluster, indent=2)
        print(json_formatted_str)
    else:
        print("Unable to fetch cluster {}".format(cluster_body["metadata"]["name"]))
        return

    print(
        "try: kubectl -n default get raycluster {} -o yaml".format(
            kube_ray_cluster["metadata"]["name"]
        )
    )
    # the rest of the code is simply to list and cleanup the created cluster
    kube_ray_list = my_kube_ray_api.list_ray_clusters(k8s_namespace="default", label_selector='demo-cluster=yes')
    if "items" in kube_ray_list:
        line = "-" * 72
        print(line)
        print("{:<63s}{:>2s}".format("Name", "Namespace"))
        print(line)
        for cluster in kube_ray_list["items"]:
            print(
                "{:<63s}{:>2s}".format(
                    cluster["metadata"]["name"],
                    cluster["metadata"]["namespace"],
                )
            )
    print(line)

    if "items" in kube_ray_list:
        for cluster in kube_ray_list["items"]:
            print("deleting raycluster = {}".format(cluster["metadata"]["name"]))
            my_kube_ray_api.delete_ray_cluster(
                name=cluster["metadata"]["name"],
                k8s_namespace=cluster["metadata"]["namespace"],
            )


if __name__ == "__main__":
    main()
