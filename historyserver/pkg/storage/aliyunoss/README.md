# Aliyun OSS Storage Collector

This module provides the storage collector for Aliyun OSS.

OSS endpoint and OSS bucket are read from `/var/collector-config/data`.

Content in `/var/collector-config/data` should be in JSON format, for example:

```json
{
  "ossRegion": "",
  "ossEndpoint": "",
  "ossBucket": ""
}
```

Set `--storage-backend=aliyunoss` to enable this module.

Currently, this module can only be used in an ACK environment. OIDC must be
enabled for the cluster, and write permission to OSS must be granted.
