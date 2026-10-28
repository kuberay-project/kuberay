# Release test

Validates a KubeRay release candidate on a local kind cluster against the docs on docs.ray.io, the samples in
`ray-operator/config/samples/`, the Helm charts and the Go modules. It replaces the release validation
spreadsheet.

The agent skills in `.agents/skills/` turn each doc page into shell steps once (`release-test-plan`), explain
a failure (`release-test-run`) and drive a whole run (`release-test`). The Go runner executes the steps and
judges them by exit code alone.

| File | Written by | Committed |
|---|---|---|
| `items.yaml` | humans: what to validate | yes |
| `manifest.yaml` | the `release-test-plan` skill, reviewed like code: how to validate each Item | yes |
| `results/<tag>/` | `go run ./release-test run` | no |

## Running a release

You need Go, kind, kubectl, helm, git, [yq v4](https://github.com/mikefarah/yq),
[uv](https://github.com/astral-sh/uv) and, to plan and triage, a coding agent that loads `.agents/skills/`.
Run the commands from the repository root; `--help` lists a command's flags.

| Command | Does |
|---|---|
| `go run ./release-test stale --kuberay-version <tag> [ID...]` | lists Items whose unit is missing or whose source changed since it was planned |
| `go run ./release-test lint` | checks `manifest.yaml` |
| `go run ./release-test run --kuberay-version <tag> [ID...]` | runs the manifest and writes the results |
| `go run ./release-test summary --kuberay-version <tag>` | rebuilds `results/<tag>/SUMMARY.md` |

1. Run `stale`, plan what it lists with `/release-test-plan <tag> <id>...`, review
   `git diff release-test/manifest.yaml` and commit it.
2. Run the manifest. It resumes where it stopped; pass IDs to re-run single Items. A unit whose source changed
   since it was planned is not run but reported as stale.

   ```sh
   kind create cluster --name release-test
   go run ./release-test run --kuberay-version v1.8.0-rc.0
   ```

3. Triage each failure with `/release-test-run results/<tag>/<id>/`: it appends a Triage section to the
   report and fills in its `triage` and `brief`. Then `summary` puts them in `results/<tag>/SUMMARY.md`.

`/release-test v1.8.0-rc.0` does all three.

## items.yaml

```yaml
- id: raycluster-quick-start           # stable slug; the unit id and results/<tag>/<id>/
  source: https://docs.ray.io/en/master/kuberay/getting-started/raycluster-quick-start.html
- id: ray-cluster-sample
  source: ray-operator/config/samples/ray-cluster.sample.yaml
- id: ray-cluster-tpu-v6e-singlehost
  source: ray-operator/config/samples/ray-cluster.tpu-v6e-singlehost.yaml
  requires: [tpu, gke]                 # not runnable on kind: listed under Manual, never planned or run
- id: go-get-ray-operator
  procedure: |                         # for Items that are neither a page nor a sample
    In an empty Go module, go get github.com/ray-project/kuberay/ray-operator@$KUBERAY_VERSION succeeds ...
```

## manifest.yaml

Each step is a `run` and a `check` command; it passes when both exit 0. Commands use `$KUBERAY_VERSION` and
`$CHART_VERSION`, never a fixed version. `source_sha` is the git blob sha of what the unit was planned from:
for a page, its code blocks without comments and with KubeRay versions normalized, so only a changed command
or expected output makes the unit stale; for a sample, the whole file at the tag. The format is in
`.agents/skills/release-test-plan/SKILL.md`.

## Results

```text
results/<tag>/
├── SUMMARY.md
└── <item-id>/
    ├── report.md        front matter (item, status, version, failed_step, triage, brief) + step table
    ├── steps/NN.log     the command, the check, the output, the exit code
    └── evidence/        on failure: pods, events, Ray resources, describe, Pod logs
```

`status` is `pass`, `fail` or `stale`. `triage` is one of `plan-bug` (the manifest is wrong), `doc-bug`,
`sample-bug`, `product-bug`, `env` (kind cannot do it) or `flaky`. Nothing files GitHub issues; a human
decides.
