<!-- markdownlint-disable MD013 -->
# Volumes support for Ray cluster by the API server

API server allows to specify multiple types of volumes mounted to the Ray pods (nodes). These include:
[hostPath](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath),
[PVC](https://kubernetes.io/docs/concepts/storage/persistent-volumes/),
[ephemeral](https://kubernetes.io/docs/concepts/storage/ephemeral-volumes/),
[config maps](https://kubernetes.io/docs/concepts/storage/volumes/#configmap),
[secrets](https://kubernetes.io/docs/concepts/storage/volumes/#secret),
and [empty dir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir).
Multiple volumes of different type can be mounted to both head and worker nodes, by defining a volume array for them

## HostPath volumes

A hostPath volume mounts a file or directory from the host node's filesystem into your Pod. This is not something that
most Pods will need, but it offers a powerful escape hatch for some applications.

For example, some uses for a hostPath are:

* running a container that needs access to Docker internals; use a hostPath of /var/lib/docker
* running cAdvisor in a container; use a hostPath of /sys
* allowing a Pod to specify whether a given hostPath should exist prior to the Pod running, whether it should be created, and what it should exist as

The code below gives an example of hostPath volume definition:

```json
{
    "name": "hostPath",             # unique name
    "source": "/tmp",               # data location on host
    "mountPath": "/tmp/hostPath",   # mounting path
    "volumeType": 1,                # volume type - host path
    "hostPathType": 0,              # host path type - directory
    "mountPropagationMode": 1       # mount propagation - host to container
}
```

## PVC volumes

A Persistent Volume Claim (PVC) is a request for storage by a user. It is similar to a Pod. Pods consume node resources and PVCs consume PV resources. Pods can request specific levels of resources (CPU and Memory). Claims can request
specific size and access modes (e.g., they can be mounted `ReadWriteOnce`, `ReadOnlyMany` or `ReadWriteMany`).

The caveat of using PVC volumes is that the same PVC is mounted to all nodes. As a result only PVCs with access mode `ReadOnlyMany` can be used in this case.

The code below gives an example of PVC volume definition:

```json
{
    "name": "pvc",              # unique name
    "mountPath": "/tmp/pvc",    # mounting path
    "source": "claim",          # claim name
    "volumeType": 0,            # volume type - PVC
    "mountPropagationMode": 2,  # mount propagation mode - bidirectional
    "readOnly": false           # read only
}
```

## Ephemeral volumes

Some application need additional storage but don't care whether that data is stored persistently across restarts. For example, caching services are often limited by memory size and can move infrequently used data into storage that is slower than memory with little impact on overall performance. Ephemeral volumes are designed for these use cases.

Because volumes follow the Pod's lifetime and get created and deleted along with the Pod, Pods can be stopped and restarted without being limited to where some persistent volume is available.

Although there are several option of ephemeral volumes, here we are using generic ephemeral volumes, which can be provided by all storage drivers that also support persistent volumes. Generic ephemeral volumes are similar to emptyDir volumes in the sense that they provide a per-pod directory for scratch data that is usually empty after provisioning. But they may also have additional features:

* Storage can be local or network-attached.
* Volumes can have a fixed size that Pods are not able to exceed.

The code below gives an example of ephemeral volume definition:

```json
{
    "name": "ephemeral",            # unique name
    "mountPath": "/tmp/ephemeral"   # mounting path,
    "mountPropagationMode": 0,      # mount propagation mode - None
    "volumeType": 2,                # volume type - ephemeral
    "storage": "5Gi",               # disk size
    "storageClass": "default",      # storage class - optional
    "accessMode": 0                 # access mode RWO - optional
}
```

## Config map volumes

A ConfigMap provides a way to inject configuration data into pods. The data stored in a ConfigMap can be referenced in a volume of type configMap and then consumed by containerized applications running in a pod.

When referencing a ConfigMap, you provide the name of the ConfigMap in the volume. You can customize the path to use for a specific entry in the ConfigMap.

The code below gives an example of config map volume definition:

```json
{
    "name":"code-sample",               # Unique name
    "mountPath":"/home/ray/samples",    # mounting path
    "volumeType":3,                     # volume type - config map
    "source":"ray-job-code-sample",     # config map name
    "items":{                           # key/path items (optional)
        "sample_code.py":"sample_code.py"
    }
}
```

## Secret volumes

A secret volume is used to pass sensitive information, such as passwords, to Pods. You can store secrets in the Kubernetes API and mount them as files for use by pods without coupling to Kubernetes directly. Secret volumes are backed by tmpfs (a RAM-backed filesystem) so they are never written to non-volatile storage.

The code below gives an example of secret volume definition:

```json
{
    "name":"important-secret",          # Unique name
    "mountPath":"/home/ray/sensitive",  # mounting path
    "volumeType":4,                     # volume type - secret
    "source":"very-important-secret",   # secret map name
    "items":{                           # subpath info (optional)
        "subPath": "password"
    }
}
```

## Emptydir volumes

An emptyDir volume is first created when a Pod is assigned to a node, and exists as long as that Pod is running on that node. As the name says, the emptyDir volume is initially empty. All containers in the Pod can read and write the same files in the emptyDir volume, though that volume can be mounted at the same or different paths in each container. When a Pod is removed from a node for any reason, the data in the emptyDir is deleted permanently.

The code below gives an example of empty directory volume definition:

```json
{
    "name": "emptyDir",            # unique name
    "mountPath": "/tmp/emptydir"   # mounting path,
    "volumeType": 5,                # vlume type - ephemeral
    "storage": "5Gi",               # max storage size - optional
}
````
