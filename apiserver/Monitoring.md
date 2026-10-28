# Monitoring of the APIServer and created RayClusters

To ensure the proper functioning of the APIServer and created RayClusters, it is
typically necessary to monitor them. This document describes how to monitor both the APIServer
and created clusters with Prometheus and Grafana.

The current implementation of the APIServer provides a flag `collectMetricsFlag` that defines whether to collect and
expose Prometheus metrics, which is set to `true` by default. To disable metrics collection, this flag needs to
be set to `false`, and the APIServer image needs to be rebuilt.

If this flag is enabled, an `http` port at the `/metrics` endpoint provides APIServer metrics in Prometheus format.

## Monitoring of the APIServer

### Deploy KubeRay operator and APIServer

Refer to the [Install with Helm](README.md#install-with-helm) section in the README for
setting up the KubeRay operator and APIServer, and port-forward the HTTP endpoint to local
port 31888. This will set the flag `collectMetricsFlag` to `true`, which enables metrics
collection.

> [!IMPORTANT]
> All the following guidance requires you to switch your working directory to the KubeRay project root.

### Install Kubernetes Prometheus Stack via Helm Chart

Please navigate to the path `install/prometheus` under the KubeRay repository and execute the
install script there to install the Prometheus Stack on the kind cluster.

```sh
cd install/prometheus
bash install.sh
```

You can see multiple pods are created in the `prometheus-system` namespace:

```sh
kubectl get pods -n prometheus-system
# NAME                                                     READY   STATUS            RESTARTS   AGE
# alertmanager-prometheus-kube-prometheus-alertmanager-0   2/2     Running   0          2m7s
# prometheus-grafana-84ccb68cc-g9br2                       3/3     Running   0          2m22s
# prometheus-kube-prometheus-operator-895b579fc-f55f6      1/1     Running   0          2m22s
# prometheus-kube-state-metrics-77b6c5d54b-wd2ml           1/1     Running   0          2m22s
# prometheus-prometheus-kube-prometheus-prometheus-0       2/2     Running   0          2m7s
# prometheus-prometheus-node-exporter-fs642                1/1     Running   0          2m22s
```

The `ray-head-monitor` and `ray-workers-monitor` in the `prometheus-system` namespace
created by the script will not be used in this example. We can safely delete them with:

```shell
kubectl delete servicemonitor ray-head-monitor -n prometheus-system
kubectl delete podmonitor ray-workers-monitor -n prometheus-system
```

### Testing monitoring of the APIServer

Now we can install a [service monitor](deploy/prometheus/api_server_service_monitor.yaml) to scrape APIServer metrics into
Prometheus using the following command:

```sh
# Assume you are already in the KubeRay project root
kubectl apply -f apiserver/deploy/prometheus/api_server_service_monitor.yaml
```

Then, please open a new terminal and use port-forwarding to expose Prometheus with:

```shell
kubectl port-forward svc/prometheus-operated -n prometheus-system 9090
```

Now you can point your browser to `http://localhost:9090/` to get a PromQL panel. Start
typing `apiserver` in the search bar, and you will see all of the APIServer metrics in
Prometheus.

### Monitoring of the Ray Cluster created by the APIServer

Ray provides
[documentation](https://docs.ray.io/en/master/cluster/kubernetes/k8s-ecosystem/prometheus-grafana.html#kuberay-prometheus-grafana)
describing how to monitor Ray clusters created using the KubeRay operator. As the APIServer is
using the KubeRay operator to create the cluster, this documentation can be used directly.

Instead of creating a `service monitor` for scraping the head node and a `pod monitor` for
scraping worker nodes, we will utilize a simpler approach by creating a single [pod
monitor](deploy/prometheus/ray_cluster_pod_monitor.yaml) that can be installed using the
following command:

```sh
kubectl apply -f apiserver/deploy/prometheus/ray_cluster_pod_monitor.yaml
```

Now you can go back to the PromQL panel at `http://localhost:9090/`. Go to the Status > Targets
pane from the top bar, and you should be able to see `podMonitor/prometheus-system/ray-workers-monitor/0` in the list.

Also, take a look at the Ray [documentation](https://docs.ray.io/en/master/cluster/kubernetes/k8s-ecosystem/prometheus-grafana.html#kuberay-prometheus-grafana)
for additional monitoring features, including recording rules, alerts, and Grafana integration.

### Clean up

```sh
make clean-cluster
# Remove APIServer from Helm
helm uninstall kuberay-apiserver
# Remove Prometheus stack from Helm
helm uninstall kube-prometheus-stack -n prometheus-system
```
