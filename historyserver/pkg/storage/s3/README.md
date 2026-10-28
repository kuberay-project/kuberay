# S3 Storage Collector

This module is the collector for S3 compatible storage.

S3 endpoint, S3 region and S3 bucket are read from /var/collector-config/data.

Content in /var/collector-config/data should be in json format, like
`{"s3Bucket": "", "s3Endpoint": "", "s3Region": ""}`

Set `--storage-backend=s3` to enable this module.

This module can be used with any S3 compatible storage service.
