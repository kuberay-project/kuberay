# RayCluster QuickStart

This document explains how to manage and interact with RayCluster using the KubeRay APIServer.
See [this guide](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/raycluster-quick-start.html) for more details.

## Step 1: Create a Kubernetes cluster

This step creates a local Kubernetes cluster using [Kind](https://kind.sigs.k8s.io/).
If you already have a Kubernetes cluster, you can skip this step.

```sh
kind create cluster --image=kindest/node:v1.29.0
```

## Step 2: Install KubeRay operator and APIServer

Follow the [Installation Guide](installation.md) to install the latest stable KubeRay
operator and APIServer (without the security proxy) from the Helm repository, and
port-forward the HTTP endpoint to local port 31888.

## Step 3: Deploy a RayCluster custom resource

Once the KubeRay operator is running, you are ready to deploy a RayCluster. While we are using APIServer, we can do this
with curl. The following command will create a RayCluster CR named `raycluster-kuberay` in your current cluster:

```sh
curl -s https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.sample.yaml | \
curl -X POST http://localhost:31888/apis/ray.io/v1/namespaces/default/rayclusters \
  -H "Content-Type: application/yaml" \
  --data-binary @-
```

Once the RayCluster CR has been created, you can view its details by executing the following command:

```sh
curl http://localhost:31888/apis/ray.io/v1/namespaces/default/rayclusters/raycluster-kuberay
```

## Step 4: Modify Created RayCluster

To modify the created RayCluster, we can use the `PATCH` method of the KubeRay APIServer.
The following command adds an `annotation` to the raycluster-kuberay resource:

```sh
curl -X PATCH 'http://localhost:31888/apis/ray.io/v1/namespaces/default/rayclusters/raycluster-kuberay' \
-H 'Content-Type: application/merge-patch+json' \
--data '{
  "metadata": {
    "annotations": {
      "example.com/purpose": "model-training"
    }
  }
}'
```

You can verify if the `annotation` is added with the following command. You should see the
annotations you added in the output:

```sh
curl -s http://localhost:31888/apis/ray.io/v1/namespaces/default/rayclusters/raycluster-kuberay \
  | jq '.metadata.annotations'

# [Expected Output]
# {
#   "example.com/purpose": "model-training"
# }
```

## Step 5: Delete the RayCluster

To delete the RayCluster with KubeRay APIServer, execute the following command. The `raycluster-kuberay` is the name of
the RayCluster that we created earlier:

```sh
curl -X DELETE 'localhost:31888/apis/ray.io/v1/namespaces/default/rayclusters/raycluster-kuberay'
```

You can then verify if the RayCluster is removed. The following command should return 404:

```sh
curl -s http://localhost:31888/apis/ray.io/v1/namespaces/default/rayclusters/raycluster-kuberay
```

## Clean up

```sh
kind delete cluster
```
