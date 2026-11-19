---
name: bump-helm-charts
description: Bump the KubeRay version references on a release branch before tagging. Use when asked to bump the KubeRay version for a release or release candidate, for example 1.8.0 or 1.8.0-rc.0.
argument-hint: [version]
---

# Bump the KubeRay version

The argument is the new version, for example `1.8.0`, `v1.8.0` or `1.8.0-rc.0`. Ask for it if it is missing. Below,
`X.Y.Z` is the version without a leading `v` and `vX.Y.Z` is the version with it. An `-rc.N` suffix is part of both.

Run the commands below from the repository root, on `release-X.Y` (`release-1.8` for `1.8.0-rc.0`) with no
uncommitted changes. If `git branch --show-current` prints anything else, ask the user to confirm that this
checkout is `release-X.Y` before editing: `master` must keep the placeholder chart version `1.1.0` and the
`nightly` image tags.

## 1. Update the version references

Set each spot to the new version, whatever value it holds now:

| File | Spot | Format |
| --- | --- | --- |
| `helm-chart/kuberay-operator/Chart.yaml` | `version` | `X.Y.Z` |
| `helm-chart/kuberay-apiserver/Chart.yaml` | `version` | `X.Y.Z` |
| `helm-chart/ray-cluster/Chart.yaml` | `version` | `X.Y.Z` |
| `helm-chart/kuberay-operator/values.yaml` | `image.tag` | `vX.Y.Z` |
| `helm-chart/kuberay-apiserver/values.yaml` | `image.tag` | `vX.Y.Z` |
| `ray-operator/config/default/kustomization.yaml` | `newTag` | `vX.Y.Z` |
| `ray-operator/config/default-with-webhooks/kustomization.yaml` | `newTag` | `vX.Y.Z` |
| `ray-operator/controllers/ray/utils/constant.go` | `KUBERAY_VERSION` | `vX.Y.Z` |
| `helm-chart/kuberay-apiserver/README.md.gotmpl` | both `--version`, and `kuberay-apiserver-<version>` in the `helm ls` output | `X.Y.Z` |
| `helm-chart/ray-cluster/README.md.gotmpl` | all three `--version` | `X.Y.Z` |
| `helm-chart/ray-cluster/README.md.gotmpl` | `KubeRay operator v<version>` in the `# Step 3: Install both CRDs ...` comment | `vX.Y.Z` |
| `helm-chart/kuberay-operator/README.md.gotmpl` | both `targetRevision` | `vX.Y.Z` |

The README templates are not bumped on `master`, so they can hold any older version (for example `1.4.2` or
`v1.0.0-rc.0`). Don't search for the previous release; run the `git grep` from step 3 first to list every spot.

Leave everything else unchanged, including:

- `security.proxy.tag` in `helm-chart/kuberay-apiserver/values.yaml`, which stays `nightly`. Quay only publishes a
  `nightly` security-proxy image.
- `{{ template "chart.version" . }}` in `helm-chart/kuberay-operator/README.md.gotmpl`, which already follows
  `Chart.yaml`.
- The Ray image tags: `image.tag` in `helm-chart/ray-cluster/values.yaml` and `nightly-aarch64` in the ray-cluster
  README template.
- The upgrade test versions in `.buildkite/test-e2e.yml` and `ray-operator/test/support/defaults.go`. They are
  updated on `master` after the release.
- The `README.md` files next to the templates. Step 2 regenerates them.

## 2. Regenerate the chart READMEs

```bash
make -C helm-chart helm-docs
```

## 3. Check

1. `git diff --stat` lists only the files in the table and the 3 `helm-chart/*/README.md`, and each changed spot
   uses the format from the table.
2. This prints the spots that don't hold the new version yet, so it should print only `security.proxy.tag`, still
   `nightly`. Put the new version, without the `v`, in place of `1\.8\.0-rc\.0`.

   ```bash
   git grep -nE -e '^version:|^ +tag:|newTag:|KUBERAY_VERSION =|--version [0-9]|targetRevision:|operator v[0-9]|kuberay-apiserver-[0-9]' -- \
     'helm-chart/*/Chart.yaml' helm-chart/kuberay-operator/values.yaml helm-chart/kuberay-apiserver/values.yaml \
     'helm-chart/*/README.md*' 'ray-operator/config/default*/kustomization.yaml' \
     ray-operator/controllers/ray/utils/constant.go | grep -vE '1\.8\.0-rc\.0([" .]|$)'
   ```

Show the user the check output and the diff. Don't commit, push or open a PR.
