# Local Filesystem Storage Backend

This module reads and writes directly against a POSIX filesystem — typically
a JuiceFS FUSE mount — instead of going through an S3-compatible gateway.

Set `--runtime-class-name=localfs` to enable this module. The only
configuration is `--ray-root-dir`, which must point at an already-mounted
POSIX path shared by every collector and the history server (e.g. the same
JuiceFS volume mounted into every pod). There is no JSON config file support
(unlike s3/azureblob/gcs) — RootDir is the entire configuration surface.

## Requirements

- The mount must support atomic `rename(2)` within itself. `WriteFile`
  overwrites the same path in full on every call (e.g. an hourly event
  bucket), by writing to a temp file in the target's directory and renaming
  it into place — so a concurrent `GetContent` never sees a half-written
  file. This only holds if the temp file and target are on the same
  filesystem.
- Writer and reader must share the same mount; the history server rescans it
  independently of any running RayCluster, which is what lets it serve data
  for clusters that have already been deleted.

## Known limitation

A write that crashes between creating the temp file and the rename leaves a
`<file>.tmp.<pid>.<nanos>` file behind. `List`/`ListFiles`/`GetContent` filter
these out of results, but nothing reclaims the disk space — sweep stale
`.tmp.` files externally if that becomes a problem in practice.

## Local development

No emulator needed (contrast with S3's MinIO or Azure's Azurite) — any
writable local directory works:

```bash
mkdir -p /tmp/ray-historyserver
collector    --runtime-class-name=localfs --ray-root-dir=/tmp/ray-historyserver ...
historyserver --runtime-class-name=localfs --ray-root-dir=/tmp/ray-historyserver ...
```

