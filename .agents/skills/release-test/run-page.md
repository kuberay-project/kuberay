# Run one docs page

You get a page (its path under `doc/source/kuberay/`), `TAG`, `NEW`, `RAY`, `RES` (see the
release-test skill) and a kind cluster name `CLUSTER`. Run the page like a reader would, with the
release candidate, and reply with a report of what you found. The main agent saves it as
`OUT/report.md`. `OUT` is `RES/` plus the page path without the extension, like
`RES/getting-started/raycluster-quick-start/`. Leave the docs as they are, and put the files your
commands create in `OUT/work/`.

## Read

Read `RAY/doc/source/kuberay/<page>` and the files it includes. Before you run anything, check for
two statuses:

- `manual`: the page needs a GPU or a TPU anywhere, or its main steps need a cloud cluster (GKE, EKS,
  AKS) or gVisor. A page that only names a cloud as one place to run, and whose commands work
  anywhere, runs on kind.
- `none`: the page has no commands to run.

For these two, the report is only the front matter and one line with the reason.

## Run the commands as written

Work in `OUT/work/` and use its absolute path. Shell state does not carry over between calls, so
start each call with `cd <work> && export KUBECONFIG=<work>/kubeconfig`. `kind create cluster` writes
that file, so every cluster you use is your own.

Run the page's commands in order, exactly as written. You may only change them in these ways, and the
report lists each change you made:

| The page has | You run |
|---|---|
| a KubeRay version in a command, like `--version 1.7.0`, or `v1.7.0` in a URL, a ref or an image tag | the candidate: `--version <TAG without v>` or `TAG`. Keep the version the upgrade guide upgrades from |
| a `kind` command (`create cluster`, `delete cluster`, `load`) | the same command with `--name CLUSTER` instead of the page's name |
| no `kind create cluster` before the first kubectl or helm command | `kind create cluster --name CLUSTER` first |
| a value the reader fills in (`<your-bucket>`, `$YOUR_TOKEN`) | a value you choose |
| a command that never ends (`port-forward`, `logs -f`, `--watch`, `watch`, a server) | the same command in the background; stop it when the page is done |
| a command that affects more than your run (`killall kubectl`, `pkill`) | the same command, only for the processes you started |
| `pip install` | the same command in a virtualenv under `OUT/work/` |
| a step that sends the reader to another page, like installing the operator | that page's commands for that step, on the cluster you already have |
| a choice of tabs, platforms or methods | the first one that works on Linux and kind; list the others under Skipped |

You may add read-only commands at any time: `kubectl get`, `describe`, `logs`, `wait`, or a `curl`
that checks something. Use them to wait for what the page waits for, to compare output, and to find
causes. You may also pull an image the page uses on the host and load it with
`kind load docker-image IMAGE --name CLUSTER`, so that each page does not pull the Ray image again. If
the load fails, the cluster pulls the image itself.

When the page shows output, compare what a reader depends on: states, counts, and values the text
mentions. Random name suffixes, ages, IPs, timestamps and order can always differ.

A **finding** is a command that fails, or output that does not match the page. Write one finding per
cause: for example, a wrong memory size in the text and the outputs it changes are one finding. Retry
a failed command once after a short wait. After a finding, continue with the steps that do not depend
on it. Give up on a wait after 15 minutes.

At the end, always run `kind delete cluster --name CLUSTER` and stop your background jobs, even if
something failed.

## Classify each finding

- `doc-bug`: the page is wrong. The proposed fix is the exact page text before and after.
- `product-bug`: KubeRay at the tag is wrong: the operator, a chart, a sample or the plugin. This
  blocks the release, so include the evidence.
- `env`: kind or this machine cannot do it: CPU, memory, image pull limits, kernel features.
- `flaky`: it passed on the retry.

## Report

Reply with the report only, in this format:

```markdown
---
page: getting-started/raycluster-quick-start.md
status: fail
---

## Findings

### 1. doc-bug: the page says 3G of memory, the chart requests 5Gi

- Step: "Deploy a RayCluster custom resource", `kubectl get pods`
- Output: the lines that show it
- Cause: the ray-cluster chart's head requests 5Gi since #4588
- Proposed fix: in `getting-started/raycluster-quick-start.md`, replace `3G` with `5Gi` in "... at least 3G ..."

## Rewrites

- `--version 1.7.0` → `--version 1.8.0-rc.0` (2 commands)
- `kind create cluster` → `kind create cluster --name rt-3`

## Skipped

- "Option 2: GKE" tab: needs GKE
```

`status` is `pass`, `fail` (at least one finding), `manual` or `none`.
