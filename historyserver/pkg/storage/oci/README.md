# OCI Object Storage

This module is the writer (collector) and reader (history server) for
[Oracle Cloud Infrastructure Object Storage](https://docs.oracle.com/en-us/iaas/Content/Object/home.htm).

Set `--storage-backend=oci` (or `STORAGE_BACKEND=oci`) to enable this module.

Configuration is read from environment variables, and individual keys can be
overridden by the JSON file passed with `--storage-backend-config-path`
(`/var/collector-config/data` in the sample manifests):

```json
{
  "ociBucket": "ray-historyserver",
  "ociNamespace": "",
  "ociRegion": "us-ashburn-1",
  "ociCompartmentId": "",
  "ociAuthType": "oke_workload_identity",
  "ociConfigFile": "",
  "ociConfigProfile": ""
}
```

## Environment variables

| Variable | JSON key | Description |
|----------|----------|-------------|
| `OCI_BUCKET` | `ociBucket` | Bucket name (default: `ray-historyserver`) |
| `OCI_NAMESPACE` | `ociNamespace` | Object Storage namespace. Resolved with `GetNamespace` when empty |
| `OCI_REGION` | `ociRegion` | Region identifier, e.g. `us-ashburn-1`. Defaults to the region of the credentials |
| `OCI_COMPARTMENT_ID` | `ociCompartmentId` | Compartment OCID. When set, the bucket is created there if it does not exist and the namespace lookup is scoped to that compartment's tenancy |
| `OCI_AUTH_TYPE` | `ociAuthType` | One of `api_key`, `session_token`, `instance_principal`, `oke_workload_identity`, `resource_principal`. Auto-detected when empty (see below) |
| `OCI_CONFIG_FILE` | `ociConfigFile` | OCI config file for `api_key` / `session_token` (default: `~/.oci/config`) |
| `OCI_CONFIG_PROFILE` | `ociConfigProfile` | Profile inside the config file (default: `DEFAULT`) |

The bucket must already exist unless `OCI_COMPARTMENT_ID` is set, in which case
the collector and the history server create it on startup (a `409` from a
concurrent creator is treated as success). Note that Object Storage answers
`404 BucketNotFound` both when the bucket is missing and when the principal is
not allowed to read it.

## Authentication

| `OCI_AUTH_TYPE` | Use it when | Credentials come from |
|-----------------|-------------|-----------------------|
| `oke_workload_identity` | Running on OKE (recommended) | The pod's Kubernetes ServiceAccount, exchanged for a resource principal session token by OKE |
| `instance_principal` | Running on OCI compute outside OKE, or OKE without Workload Identity | The node's instance principal |
| `resource_principal` | Running on OCI Functions or another resource principal host | `OCI_RESOURCE_PRINCIPAL_*` environment variables |
| `api_key` | Local development, or a mounted OCI config file with an API signing key | `OCI_CONFIG_FILE` / `OCI_CONFIG_PROFILE` |
| `session_token` | Local development with `oci session authenticate` | `OCI_CONFIG_FILE` / `OCI_CONFIG_PROFILE` (`security_token_file` in the profile) |

When `OCI_AUTH_TYPE` is empty the backend picks, in order:

1. `oke_workload_identity` if `OCI_RESOURCE_PRINCIPAL_VERSION` and `KUBERNETES_SERVICE_HOST` are set.
2. `resource_principal` if only `OCI_RESOURCE_PRINCIPAL_VERSION` is set.
3. `session_token` or `api_key` if an OCI config file exists (the profile decides).
4. `instance_principal` otherwise.

### OKE Workload Identity

1. Create the OKE cluster as an *enhanced* cluster (Workload Identity is not
   available on basic clusters).
2. Set the resource principal environment variables the OCI SDK expects on the
   collector and history server containers:

   ```yaml
   env:
   - name: STORAGE_BACKEND
     value: "oci"
   - name: OCI_AUTH_TYPE
     value: "oke_workload_identity"
   - name: OCI_RESOURCE_PRINCIPAL_VERSION
     value: "2.2"
   - name: OCI_RESOURCE_PRINCIPAL_REGION
     value: "us-ashburn-1"
   - name: OCI_BUCKET
     value: "ray-historyserver"
   ```

3. Grant the ServiceAccount access to the bucket with an IAM policy. Replace
   the placeholders with your compartment, namespace, ServiceAccount and
   cluster OCID:

   ```text
   Allow any-user to read objectstorage-namespaces in tenancy where all {
     request.principal.type = 'workload',
     request.principal.cluster_id = '<cluster-ocid>' }
   Allow any-user to read buckets in compartment <compartment-name> where all {
     request.principal.type = 'workload',
     request.principal.namespace = '<k8s-namespace>',
     request.principal.service_account = 'historyserver',
     request.principal.cluster_id = '<cluster-ocid>' }
   Allow any-user to manage objects in compartment <compartment-name> where all {
     request.principal.type = 'workload',
     request.principal.namespace = '<k8s-namespace>',
     request.principal.service_account = 'historyserver',
     request.principal.cluster_id = '<cluster-ocid>',
     target.bucket.name = 'ray-historyserver' }
   ```

   Add `manage buckets` (and set `OCI_COMPARTMENT_ID`) only if you want the
   collector to create the bucket itself.

See `config/rayjob-oci.yaml` and `config/historyserver-oci.yaml` for complete
manifests.

### Instance principal

Put the worker nodes in a dynamic group and grant that group the same
`read objectstorage-namespaces`, `read buckets` and `manage objects` verbs as
above (matching on `request.principal.type = 'instance'` is not needed; the
dynamic group already scopes the rule). Set `OCI_AUTH_TYPE=instance_principal`,
or leave it empty on a node without an OCI config file.

## Local development

Run the history server on your workstation against a real bucket with the
credentials from the OCI CLI:

```bash
# API key profile
export STORAGE_BACKEND=oci
export OCI_BUCKET=ray-historyserver
export OCI_REGION=us-ashburn-1
export OCI_CONFIG_PROFILE=DEFAULT
./output/bin/historyserver --storage-backend=oci

# Session token profile (oci session authenticate --profile-name dev)
export OCI_AUTH_TYPE=session_token
export OCI_CONFIG_PROFILE=dev
./output/bin/historyserver --storage-backend=oci
```

There is no local emulator for OCI Object Storage; use a throwaway bucket in a
sandbox compartment instead.

## Layout in the bucket

The object layout is the same as for the other backends:

```text
<STORAGE_ROOT_DIR>/cluster-metadata/<raycluster|rayjob|rayservice>/<namespace>_<name>/<session>
<STORAGE_ROOT_DIR>/<namespace>_<name>/<session>/logs/<node-id>/...
```

Directories are represented by zero-byte objects whose name ends in `/`, so
they show up as folders in the OCI Console; `ListFiles` filters those markers
out of its results.
