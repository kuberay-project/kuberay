# RayService QuickStart

This document explains how to manage and interact with RayService using KubeRay APIServer.
See [this guide](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayservice-quick-start.html) for more details.

## Step 1: Create a Kubernetes cluster

This step creates a local Kubernetes cluster using [Kind](https://kind.sigs.k8s.io/). If you already have a Kubernetes
cluster, you can skip this step.

```sh
kind create cluster --image=kindest/node:v1.35.0
```

## Step 2: Install KubeRay operator and APIServer

Follow the [Installation Guide](installation.md) to install the latest stable KubeRay
operator and APIServer (without the security proxy) from the Helm repository, and
port-forward the HTTP endpoint to local port 31888.

## Step 3: Install a RayService

The RayService can be created with follow curl command. This will create a RayService
named `rayservice-sample`:

```sh
curl -s https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.sample.yaml | \
curl -X POST http://localhost:31888/apis/ray.io/v1/namespaces/default/rayservices \
  -H "Content-Type: application/yaml" \
  --data-binary @-
```

Once the RayService CR has been created, you can view its detail by executing following command:

```sh
curl http://localhost:31888/apis/ray.io/v1/namespaces/default/rayservices/rayservice-sample
```

You can also see RayCluster being created:

```sh
curl http://localhost:31888/apis/ray.io/v1/namespaces/default/rayclusters
```

## Step 4: Check the status of RayService

To check if the RayService is ready, use following command and check if the `status` field
is `True`:

```sh
curl -s http://localhost:31888/apis/ray.io/v1/namespaces/default/rayservices/rayservice-sample | jq -r '.status.conditions[] | select(.type=="Ready") | to_entries[] | "\(.key): \(.value)"'

# lastTransitionTime: 2025-05-19T13:19:26Z
# message: Number of serve endpoints is greater than 0
# observedGeneration: 1
# reason: NonZeroServeEndpoints
# status: True
# type: Ready
```

## Step 5: Zero downtime upgrade for RayCluster

RayService enables a zero downtime upgrades for RayCluster. That is, if you modify
`spec.rayClusterConfig` in the RayService config, it triggers a zero downtime upgrade for
Ray clusters. RayService temporarily creates a new RayCluster and waits for it to be
ready, then switches traffic to the new RayCluster by updating the selector of the head
service managed by RayService `rayservice-sample-head-svc` and terminates the old one.

As an example, we will modify `rayVersion` in `rayClusterConfig` to `2.100.0`. Let's have
a look at the original `rayVersion`. Execute the following command:

```sh
curl -s http://localhost:31888/apis/ray.io/v1/namespaces/default/rayservices/rayservice-sample \
  | jq -r '.spec.rayClusterConfig.rayVersion'

# Expected output
# 2.46.0
```

Now, use the following for modifying the `rayVersion` for RayCluster managed by RayService.

```sh
curl -X PATCH http://localhost:31888/apis/ray.io/v1/namespaces/default/rayservices/rayservice-sample \
  -H "Content-Type: application/merge-patch+json" \
  --data '{
    "spec": {
      "rayClusterConfig": {
        "rayVersion": "2.100.0"
      }
    }
  }'
```

After the execution, you can see the `rayVersion` has been modifie:

```sh
curl -s http://localhost:31888/apis/ray.io/v1/namespaces/default/rayservices/rayservice-sample \
  | jq -r '.spec.rayClusterConfig.rayVersion'

# Expected output
# 2.100.0
```

## Step 6: Delete the RayService

To delete the RayService with KubeRay APIServer, execute the following command. The `rayservice-sample` is the name of
the RayService we created earlier.

```sh
curl -X DELETE 'localhost:31888/apis/ray.io/v1/namespaces/default/rayservices/rayservice-sample'
```

You can then verify if the RayService is removed. The following command should return 404:

```sh
curl http://localhost:31888/apis/ray.io/v1/namespaces/default/rayservices/rayservice-sample
```

## Clean up

```sh
kind delete cluster
```
