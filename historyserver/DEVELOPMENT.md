# History Server - Local Development with Ray Dashboard

This guide walks through how to develop the History Server locally with RustFS as the
S3-compatible object store, and browse the UI through the Ray Dashboard's middleware
(from [ray-project/ray#61295](https://github.com/ray-project/ray/pull/61295)).

## Table of Contents

- [Prerequisites](#prerequisites)
- [Step 1: Set Up Kind and KubeRay Operator](#step-1-set-up-kind-and-kuberay-operator)
- [Step 2: Build and Load Images](#step-2-build-and-load-images)
- [Step 3: Deploy RustFS](#step-3-deploy-rustfs)
- [Step 4: Generate a Dead Session](#step-4-generate-a-dead-session)
- [Step 5: Deploy History Server](#step-5-deploy-history-server)
- [Step 6: Access the Local Ray Dashboard](#step-6-access-the-local-ray-dashboard)
- [Step 7: Validate the Dead Path](#step-7-validate-the-dead-path)
- [Step 8: Validate the Live Path](#step-8-validate-the-live-path)
- [Cleanup](#cleanup)
- [Troubleshooting](#troubleshooting)

## Prerequisites

- Kind, kubectl
- Go v1.25+
- Docker
- GNU Make
- K9s (optional)

## Step 1: Set Up Kind and KubeRay Operator

Make sure you are under the `kuberay` directory.

```bash
# Spin up the kind cluster.
kind create cluster --image=kindest/node:v1.29.0

# Build and load the operator.
IMG=kuberay/operator:latest make -C ray-operator docker-build
kind load docker-image kuberay/operator:latest

# Install via Helm.
helm install kuberay-operator \
  --set image.repository=kuberay/operator \
  --set image.tag=latest \
  ./helm-chart/kuberay-operator
```

## Step 2: Build and Load Images

Build the collector and the history server images, then load both of them into the kind cluster.

```bash
# Build images.
make -C historyserver localimage-build

# Load into kind.
kind load docker-image collector:v0.1.0
kind load docker-image historyserver:v0.1.0

# Verify.
docker exec -it kind-control-plane crictl images | grep -E 'collector|historyserver'
```

## Step 3: Deploy RustFS

```bash
kubectl apply -f historyserver/config/rustfs.yaml
```

Port forward the console and API ports:

```bash
kubectl -n rustfs-dev port-forward svc/rustfs-service 9001:9001 9000:9000
```

## Step 4: Generate a Dead Session

> [!IMPORTANT]
> Finish this step before [Step 5](#step-5-deploy-history-server): the history server
> only scans object store on startup and hourly, so a late session won't show up until
> the next tick (see [Troubleshooting](#troubleshooting)).

```bash
# 1. Submit the RayJob (it creates its own cluster with the collector sidecar).
kubectl apply -f historyserver/config/rayjob.yaml
# RayJob uses status.jobStatus (not status.conditions); use a jsonpath wait until the user job succeeds.
kubectl wait rayjob/rayjob-historyserver --for=jsonpath='{.status.jobStatus}=SUCCEEDED' --timeout=5m

# 2. Delete to trigger a final event/log flush. (Optional: the RayJob sets
# shutdownAfterJobFinishes with a 30s TTL, so it also cleans itself up.)
kubectl delete -f historyserver/config/rayjob.yaml
```

> [!NOTE]
> Open `http://localhost:9001/rustfs/console/`, log in with `rustfsadmin` / `rustfsadmin`, and confirm the `ray-historyserver`
> bucket exists.

## Step 5: Deploy History Server

Run the History Server **in-cluster** so that the dashboard middleware can reach it via `http://historyserver:30080`.

```bash
kubectl apply -f historyserver/config/service_account.yaml
kubectl apply -f historyserver/config/historyserver.yaml
```

> [!NOTE]
> Access to live RayClusters is off by default, so `/clusters` lists only sessions already flushed to
> storage. If you want to access live RayClusters, uncomment the `--enable-live-clusters=true` line
> in the container `args` of `historyserver/config/historyserver.yaml` before applying it.

## Step 6: Access the Local Ray Dashboard

To access the local Ray Dashboard, you have to port forward the History Server service:

```bash
kubectl port-forward svc/historyserver 8080:30080
```

Install Ray locally. Make sure to use at least Ray `v2.58`.

```bash
pip uninstall -y ray
pip install -U "ray[default]==2.58.0"
```

Run the `ray start` command:

```bash
ray start --head --num-cpus=1 --proxy-server-url=http://localhost:8080
```

> [!NOTE]
> Use `--proxy-server-url` parameter to route requests to the port-forwarded History Server.

## Step 7: Validate the Dead Path

Open a browser and request the `/clusters` endpoint to view all clusters (including live and dead):

```text
http://localhost:8265/clusters
```

The result should look something like the following:

![clusters_endpoint](https://github.com/ray-project/kuberay/blob/40bf59590022c459086629e56b96444297c507d1/historyserver/docs/assets/clusters_endpoint.png)

Substitute your real `<SELECTED_SESSION_ID>` (the URL addresses the cluster by its owning
RayJob, so no generated cluster name is needed):

```text
http://localhost:8265/enter_cluster/default/rayjob/rayjob-historyserver/<SELECTED_SESSION_ID>
```

A successful request produces output like the following:

![enter_cluster_dead](https://github.com/ray-project/kuberay/blob/40bf59590022c459086629e56b96444297c507d1/historyserver/docs/assets/enter_cluster_dead.png)

Once set up, you can hit any endpoint via the Ray Dashboard. Take `http://localhost:8265/#/cluster` as an example:

![dead_raycluster](https://github.com/ray-project/kuberay/blob/93e81e60ddc486f60ae6432a3d178edbf952eff3/historyserver/docs/assets/dead_raycluster.png)

## Step 8: Validate the Live Path

Re-submit the RayJob. Its cluster stays alive for `ttlSecondsAfterFinished` (30s)
after the job succeeds, which leaves time to inspect the live path.

```bash
kubectl apply -f historyserver/config/rayjob.yaml
kubectl wait rayjob/rayjob-historyserver --for=jsonpath='{.status.jobStatus}=SUCCEEDED' --timeout=5m
```

In the browser, switch to the live session by visiting:

```text
http://localhost:8265/enter_cluster/default/rayjob/rayjob-historyserver/live
```

The result should look like the following:

![enter_cluster_live](https://github.com/ray-project/kuberay/blob/40bf59590022c459086629e56b96444297c507d1/historyserver/docs/assets/enter_cluster_live.png)

Now open `http://localhost:8265/#/cluster`. Unlike Step 7, the History Server reverse-proxies each API
call to the live RayCluster's head dashboard service, so you see real-time state instead of replay:

![live_raycluster](https://github.com/ray-project/kuberay/blob/93e81e60ddc486f60ae6432a3d178edbf952eff3/historyserver/docs/assets/live_raycluster.png)

## Cleanup

```bash
kubectl delete -f historyserver/config/rayjob.yaml --ignore-not-found
kubectl delete -f historyserver/config/historyserver.yaml
kubectl delete -f historyserver/config/service_account.yaml
kubectl delete -f historyserver/config/rustfs.yaml

# Or delete the whole cluster directly.
# kind delete cluster
```

## Troubleshooting

- **Pages are empty but `/clusters` and `/enter_cluster` work, history server logs
  `Task map not found for cluster session: ...`:** The EventHandler's in-memory maps were not populated for this
  session.

  To fix this problem, force a re-scan by restarting the history server:

  ```bash
  kubectl rollout restart deploy/historyserver-demo
  kubectl rollout status deploy/historyserver-demo

  # The previous port-forward tunnel dies with the old pod; re-run it.
  kubectl port-forward svc/historyserver 8080:30080
  ```

  Then, hard-refresh the browser, the UI will now render.
