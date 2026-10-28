# Google Cloud Storage (GCS)

This module is the writer and reader for GCS storage.

It is required for the GKE Cluster running Ray to have workload identity (WI), to setup WI, please follow:
[How-to: Workload Identity](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)

To use it with the History Server, set `--storage-backend=gcs`.

```yaml
apiVersion: apps/v1
kind: Deployment
...
spec:
  ...
  template:
    ...
    spec:
      serviceAccountName: historyserver
      containers:
      - name: historyserver
        env:
          - name: GCS_BUCKET
            value: "${GCS_BUCKET}"
          - name: STORAGE_BACKEND
            value: "gcs"
        image: ${HISTORYSERVER_IMAGE}
        imagePullPolicy: Always
        ports:
        - containerPort: 8080
        resources:
          limits:
            cpu: "500m"
```

RayCluster will also have the following under both the worker and head collector spec

```yaml
  - name: collector
    image: ray-collector:v0.1.0
    imagePullPolicy: Always
    env:
    - name: GCS_BUCKET
      value: "<GCS_BUCKET_NAME>"
    - name: STORAGE_BACKEND
      value: "gcs"
    - name: EVENTS_PORT
      value: "8084"
    - name: RAY_CLUSTER_NAME
      valueFrom:
        fieldRef:
          fieldPath: metadata.labels['ray.io/cluster']
    - name: RAY_ROLE
      value: "Head"
```
