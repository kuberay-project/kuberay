# Azure Blob Storage Collector

This module is the collector for Azure Blob Storage.

Azure endpoint, container, and authentication are read from environment
variables or `/var/collector-config/data`.

Content in `/var/collector-config/data` should be in JSON format, like
`{"azureContainer": "", "azureConnectionString": "", "azureAccountURL": ""}`

Set `--storage-backend=azureblob` to enable this module.

## Authentication

This module supports two authentication methods:

1. **Connection String**: Set `AZURE_STORAGE_CONNECTION_STRING` environment
   variable. Suitable for development and testing.

2. **Workload Identity**: Set `AZURE_STORAGE_ACCOUNT_URL` environment variable
   (e.g., `https://<account>.blob.core.windows.net`). For AKS with workload
   identity enabled, the pod must have label `azure.workload.identity/use: "true"`
   and use a ServiceAccount annotated with
   `azure.workload.identity/client-id: "<client-id>"`.

If both are set, connection string takes precedence.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `AZURE_STORAGE_CONNECTION_STRING` | Azure Storage connection string |
| `AZURE_STORAGE_ACCOUNT_URL` | Storage account URL for workload identity |
| `AZURE_STORAGE_CONTAINER` | Container name (default: `ray-historyserver`) |
| `AZURE_STORAGE_AUTH_MODE` | Auth mode: `connection_string`, `workload_identity`, or `default` |

## Local Development

Use [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite)
for local testing:

```bash
docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite \
  azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck
```

Connection string for Azurite:

```text
DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;
```
