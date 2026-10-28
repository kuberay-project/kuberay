# Creating Autoscaling clusters using APIServer

One of Ray's key features is autoscaling. This [document] explains how to set up autoscaling
with the Ray operator. Here, we demonstrate how to configure it using the APIServer and
run an example.

## Setup

Refer to the [Install with Helm](README.md#install-with-helm) section in the README for
setting up the KubeRay operator and APIServer, and port-forward the HTTP endpoint to local
port 31888.

## Example

This example walks through how to trigger scale-up and scale-down for RayCluster.

Before proceeding with the example, remove any running RayClusters to ensure a successful
execution of the steps below.

```sh
kubectl delete raycluster --all
```

> [!IMPORTANT]
> All the following guidance requires you to switch your working directory to the KubeRay `apiserver`

### Install ConfigMap

Install this [ConfigMap], which contains the code for our example. Simply download
the file and run:

```sh
kubectl apply -f test/cluster/cluster/detachedactor.yaml
```

Check if the ConfigMap is successfully created. You should see `ray-example` in the list:

```sh
kubectl get configmaps
# NAME               DATA   AGE
# ray-example        2      8s
```

### Deploy RayCluster

Before running the example, deploy a RayCluster with the following command:

```sh
# Create compute template
curl -X POST 'localhost:31888/apis/v1/namespaces/default/compute_templates' \
--header 'Content-Type: application/json' \
--data  @docs/api-example/compute_template.json

# Create RayCluster
curl -X POST 'localhost:31888/apis/v1/namespaces/default/clusters' \
--header 'Content-Type: application/json' \
--data @docs/api-example/autoscaling_clusters.json
```

This command performs two main operations:

1. Creates a compute template `default-template` that specifies resources to use during
   scale-up (2 CPUs and 4 GiB memory).

2. Deploys a RayCluster (test-cluster) with:
    - A head pod that manages the cluster
    - A worker group configured to scale between 0 and 5 replicas

The worker group uses the following autoscalerOptions to control scaling behavior:

- **`upscalingMode: "Default"`**: Default scaling behavior. Ray will scale up only as
needed.
- **`idleTimeoutSeconds: 30`**: If a worker pod remains idle (i.e., not running any tasks)
for 30 seconds, it will be automatically removed.
- **`cpu: "500m"`, `memory: "512Mi"`**: Defines the **minimum resource unit** Ray uses to
assess scaling needs.  If no worker pod has at least this much free capacity, Ray will
trigger a scale-up and launch a new worker pod.

> **Note:** These values **do not determine the actual size** of the worker pod. The
> pod size comes from the `computeTemplate` (in this case, 2 CPUs and 4 GiB memory).

### Validate that RayCluster is deployed correctly

Run the following command to get a list of pods running. You should see something like below:

```sh
kubectl get pods
# NAME                                READY   STATUS    RESTARTS   AGE
# kuberay-operator-545586d46c-f9grr   1/1     Running   0          49m
# test-cluster-head                   2/2     Running   0          3m1s
```

Note that there is no worker for `test-cluster` as we set its initial replicas to 0. You
will only see head pod with 2 containers for `test-cluster`.

### Trigger RayCluster scale-up

Create a detached actor to trigger scale-up with the following command:

```sh
curl -X POST 'localhost:31888/apis/v1/namespaces/default/jobs' \
--header 'Content-Type: application/json' \
--data '{
  "name": "create-actor",
  "namespace": "default",
  "user": "kuberay",
  "entrypoint": "python /home/ray/samples/detached_actor.py actor1",
  "clusterSelector": {
    "ray.io/cluster": "test-cluster"
  }
}'
```

The `detached_actor.py` file is defined in the [ConfigMap] we installed earlier and
mounted to the head node, which requires `num_cpus=1`. Recall that initially there is no
worker pod exists, RayCluster needs to scale up a worker for running this actor.

Check if a worker is created. You should see a worker `test-cluster-small-wg-worker` spin
up.

```sh
kubectl get pods

# NAME                                 READY   STATUS      RESTARTS   AGE
# create-actor-tsvfc                   0/1     Completed   0          99s
# kuberay-operator-545586d46c-f9grr    1/1     Running     0          55m
# test-cluster-head                    2/2     Running     0          9m37s
# test-cluster-small-wg-worker-j54xf   1/1     Running     0          88s
```

### Trigger RayCluster scale-down

Run the following command to delete the actor we created earlier:

```sh
curl -X POST 'localhost:31888/apis/v1/namespaces/default/jobs' \
--header 'Content-Type: application/json' \
--data '{
  "name": "delete-actor",
  "namespace": "default",
  "user": "kuberay",
  "entrypoint": "python /home/ray/samples/terminate_detached_actor.py actor1",
  "clusterSelector": {
    "ray.io/cluster": "test-cluster"
  }
}'
```

Once the actor is deleted, the worker is no longer needed. The worker pod will be deleted
after `idleTimeoutSeconds` (default 60; we specified 30) seconds.

List all pods to verify that the worker pod is deleted:

```sh
kubectl get pods

# NAME                                READY   STATUS      RESTARTS   AGE
# create-actor-tsvfc                  0/1     Completed   0          6m37s
# delete-actor-89z8c                  0/1     Completed   0          83s
# kuberay-operator-545586d46c-f9grr   1/1     Running     0          60m
# test-cluster-head                   2/2     Running     0          14m

```

### Clean up

```sh
make clean-cluster
# Remove apiserver from helm
helm uninstall kuberay-apiserver
```

[document]: https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/configuring-autoscaling.html
[ConfigMap]: test/cluster/cluster/detachedactor.yaml
