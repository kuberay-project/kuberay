---
name: release-test-plan
description: >-
  Write release-test/manifest.yaml units for new or changed release test Items, turning doc commands
  into checked steps. Plan only, no cluster. Use when `go run ./release-test stale` lists Items or when
  asked to plan release test Items.
---

# Release test plan

Turn Items from `release-test/items.yaml` into units in `release-test/manifest.yaml`. The runner
(`go run ./release-test run`) judges each step only by exit codes, so the quality of every `check` is the
quality of the release test.

Never run kubectl, helm, kind or docker here.

The arguments are the tag under test, for example `v1.8.0-rc.0`, and optionally Item ids. Ask for the
tag if it is missing.

## Steps

1. Run `go run ./release-test stale --kuberay-version <tag>`, with the Item ids you were given if any. Plan only
   the Items it prints as `new` or `changed`. Do not touch any other unit: those carry fixes from
   earlier runs. Report `dead` and `orphan` lines to the user instead of planning them.
2. For each Item, read the file `stale` printed: the page's markdown source or the sample at the tag,
   both in `release-test/.cache/`, or the Item's `procedure` in items.yaml. Do not fetch docs.ray.io
   HTML. Fetch other files the page includes only when a step depends on their content.
3. Write the unit and put it in manifest.yaml in items.yaml order, replacing the unit with the same id.
4. Run `go run ./release-test stale --kuberay-version <tag> ID...` for the planned Items: it must print nothing. Run
   `go run ./release-test lint`: it must print ok.
5. Print a table for the reviewer: id, number of steps, notes. Stop there; a human reviews
   `git diff release-test/manifest.yaml` before anything runs.

## Unit format

```yaml
  - id: raycluster-quick-start   # the Item id
    source_sha: 6730a0521daafb2bf9533c90ac16ffbfbc7975d0   # exactly as stale printed it
    notes:                       # optional; what the reviewer and the report reader should know
      - "doc-bug: step 3 output shows 2 workers, the sample has 1"
    steps:
      - name: Install the KubeRay operator
        run: |
          helm repo add kuberay https://ray-project.github.io/kuberay-helm/
          helm repo update
          helm install kuberay-operator kuberay/kuberay-operator --version $CHART_VERSION
        check: kubectl wait deploy/kuberay-operator --for=condition=Available --timeout=300s
        timeout: 300             # seconds, for run and for check each; default 300
    cleanup:
      - helm uninstall kuberay-operator
```

## Writing steps

- Copy the page's commands verbatim and in order. Change only versions:
  - KubeRay: raw URLs and refs use `$KUBERAY_VERSION`, chart `--version` uses `$CHART_VERSION`, images
    use `quay.io/kuberay/operator:$KUBERAY_VERSION`. Never master, latest or a fixed KubeRay version;
    the lint rejects them.
  - Third-party installs (cert-manager, Prometheus, Kueue, Volcano): pin the exact version the page
    names, or the current release if it names none.
- Every step that the page makes a claim about gets a `check`. Write the claim, not the output:
  - Waiting: `kubectl wait --for=condition=...` or `--for=jsonpath='{.status.state}'=ready` with a
    `--timeout`. Never `sleep` and then look.
  - Counting: `test "$(kubectl get pods -l ray.io/node-type=worker -o name | wc -l)" -eq 2`.
  - Output: `grep -q 'pattern' "$OUT"`, where `$OUT` holds the step's `run` output.
  - Reading YAML or JSON: `yq` (mikefarah v4; `yq -p json -o yaml` for JSON).
  - Never match Pod names, IPs, timestamps or whole outputs.
- When the page claims a behaviour (failover, autoscaling, restart, upgrade), add the step that
  provokes it, then check the result.
- A step that is expected to fail checks that: `run: kubectl ray get token no-auth || true` with
  `check: grep -q 'not configured' "$OUT"`.
- Commands the page runs on a laptop with Ray installed (`ray job submit`, Python scripts) run in the
  head Pod instead: `kubectl exec "$(kubectl get pod -l ray.io/node-type=head -o name)" -- ...`.
- Background processes redirect their output and are left running; the runner kills them when the
  unit ends: `kubectl port-forward svc/raycluster-kuberay-head-svc 8265:8265 >/dev/null 2>&1 &`. The
  next step's check retries: `curl -sf --retry 10 --retry-delay 2 --retry-all-errors ...`.
- Steps run with `bash -e`, stdin closed, in a temp directory the unit's steps share. Nothing is
  interactive.
- `cleanup` deletes everything the unit created. The runner also deletes what is left, but a unit
  that relies on that shows up in its report.
- Sections the page runs on a cloud (GKE, EKS) are left out and named in `notes`.
- Samples (`source` is a repo path): install the operator, `kubectl apply -f
  https://raw.githubusercontent.com/ray-project/kuberay/$KUBERAY_VERSION/<path>`, wait until ready,
  then check the one behaviour the sample exists to show (an auth sample rejects an unauthenticated
  request; an autoscaler sample scales up under load).
- A kind node has about 8 CPU and 16 GiB. If the page's resources do not fit, lower the requests and
  say so in `notes`.
- Problems you find while planning (the page pins master, a link is dead, the page and the sample
  disagree) go in `notes` as `doc-bug:` or `sample-bug:` one-liners. Quote every note.
