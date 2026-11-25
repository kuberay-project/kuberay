# Overview

This python client library provide APIs to handle `raycluster` and `rayjobs` from your python application.

## Prerequisites

It is assumed that your `k8s cluster in already setup`. Your kubectl configuration is expected to be
in  `~/.kube/config` if you are running the code directly from you terminal.

It is also expected that the `kuberay operator` is installed.
[Installation instructions are here][quick-start]

## Usage

There are multiple levels of using the API with increasing levels of complexity.

### director

This is the easiest form of using the API to create rayclusters with predefined cluster sizes

```python
my_kuberay_api = kuberay_cluster_api.RayClusterApi()

my_cluster_director = kuberay_cluster_builder.Director()

cluster0 = my_cluster_director.build_small_cluster(name="new-cluster0")

if cluster0:
    my_kuberay_api.create_ray_cluster(body=cluster0)
```

the director create the cluster definition, and the `cluster_api` acts as the HTTP client sending
the create (post) request to the k8s api-server

### cluster_builder

The builder allows you to build the cluster piece by piece. You can customize the cluster more.

```python
cluster1 = (
        my_cluster_builder.build_meta(name="new-cluster1")
        .build_head()
        .build_worker(group_name="workers", replicas=3)
        .get_cluster()
    )

if not my_cluster_builder.succeeded:
    return

my_kuberay_api.create_ray_cluster(body=cluster1)
```

### cluster_utils

`cluster_utils` gives you even more options to modify your cluster definition, add/remove worker
groups, change replicas in a worker group, duplicate a worker group, etc.

```python
my_Cluster_utils = kuberay_cluster_utils.ClusterUtils()

cluster_to_patch, succeeded = my_Cluster_utils.update_worker_group_replicas(
    cluster2, group_name="workers", max_replicas=4, min_replicas=1, replicas=2
)

if succeeded:
    my_kuberay_api.patch_ray_cluster(
        name=cluster_to_patch["metadata"]["name"], ray_patch=cluster_to_patch
    )
```

### cluster_api

The `cluster_api` is the one you always use to implement your cluster change in k8s. You can
use it with raw `JSON` if you wish. The `director/cluster_builder/cluster_utils` are just tools to
shield the user from using raw `JSON`.

### job_api

Finally, the `job_api` can be used to submit RayJobs to a pre-existing RayCluster.

#### Submitting to Existing Cluster

```python
from kuberay_client import kuberay_job_api, kuberay_cluster_api, constants

job_body = {
    "apiVersion": "ray.io/v1",
    "kind": "RayJob",
    "metadata": {...},
    "spec": {
        "clusterSelector": {
            "ray.io/cluster": "ray-cluster-name",
        },
        "entrypoint": 'python -c training_script.py',
        "submissionMode": "K8sJobMode",
    },
}

kuberay_job_api.submit_job(
    job=job_body,
    k8s_namespace=namespace,
)
```

## Code Organization

```text
clients/
└── kuberay-client
    ├── examples
    │   ├── complete-example.py
    │   ├── use-builder.py
    │   ├── use-director.py
    │   ├── use-raw-config_map_with-api.py
    │   ├── use-raw-with-api.py
    │   └── use-utils.py
    ├── LICENSE
    ├── poetry.lock
    ├── pyproject.toml
    ├── kuberay_client
    │   ├── __init__.py
    │   ├── constants.py
    │   ├── kuberay_cluster_api.py
    │   ├── kuberay_job_api.py
    │   └── utils
    │       ├── __init__.py
    │       ├── kuberay_cluster_builder.py
    │       └── kuberay_cluster_utils.py
    ├── kuberay_client_test
    │   ├── README.md
    │   ├── test_cluster_api.py
    │   ├── test_director.py
    │   ├── test_job_api.py
    │   └── test_utils.py
    └── README.md
```

## For developers

make sure you have installed setuptool

`pip install -U pip setuptools`

### run the pip command

from the directory `path/to/kuberay/clients/kuberay-client`

`pip install -e .`

### to uninstall the module run

`pip uninstall kuberay-client`

### For testing run

 `python -m unittest discover 'path/to/kuberay/clients/kuberay-client/kuberay_client_test/'`

[quick-start]: https://github.com/ray-project/kuberay#quick-start
