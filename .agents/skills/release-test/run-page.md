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

For these two, the report is only the front matter, with the reason as `summary`.

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

Each finding gets a type and a severity. The type says where the fix goes:

- `doc-bug`: the page is wrong.
- `product-bug`: KubeRay at the tag is wrong: the operator, a chart, a sample or the plugin.
- `env`: kind or this machine cannot do it: CPU, memory, image pull limits, kernel features.
- `flaky`: it passed on the retry.

The severity says what the finding does to a reader who follows the page:

- `blocker`: the reader cannot go on, or does not get the result the page is about. A command or
  manifest that later steps need fails, or a resource never reaches the state the page says.
- `minor`: the reader still gets the result. For example: an old Pod name, version or count in the
  page's output, a wrong name in the text, or a cleanup command that fails.

## Report

Reply with the report only, in this format:

````markdown
---
page: getting-started/raycluster-quick-start.md
status: pass
blocker: 0
minor: 1
summary: The RayCluster becomes ready and the job succeeds. The page says 3G of memory, but the chart requests 5Gi.
---

## Checks

| # | Step | The page says | Result |
|---|---|---|---|
| 1 | Step 2: `helm install kuberay-operator` | the operator Pod is `Running` | ✅ |
| 2 | Step 3: `helm install raycluster` | 1 head Pod and 1 worker Pod are `Running` | ✅ |
| 3 | Step 3: "at least 3G of memory" | the head Pod requests 3G | ❌ 1 |
| 4 | Step 4: `ray job submit` | the job succeeds | ✅ |

## Findings

| # | Severity | Type | Step | Problem | Proposed fix |
|---|---|---|---|---|---|
| 1 | minor | doc-bug | Step 3 | the page says 3G of memory, the ray-cluster chart requests 5Gi since #4588 | "at least 3G" → "at least 5Gi" |

Finding 1:

```
$ kubectl get pod raycluster-kuberay-head-x7k2p -o jsonpath='{.spec.containers[0].resources.requests.memory}'
5Gi
```

## Evidence

```
$ helm ls -n ray-system
kuberay-operator  ray-system  1  deployed  kuberay-operator-1.8.0-rc.0
$ kubectl get raycluster
NAME                 DESIRED WORKERS   AVAILABLE WORKERS   STATUS   AGE
raycluster-kuberay   1                 1                   ready    95s
```

## Rewrites

- `--version 1.7.0` → `--version 1.8.0-rc.0` (2 commands)
- `kind create cluster` → `kind create cluster --name rt-3`

## Skipped

- "Option 2: GKE" tab: needs GKE
````

Write only these sections, and leave out an empty one. A reader gets the result from the front matter
and the two tables:

- Front matter: `status` is `pass` (no `blocker` finding), `fail` (at least one), `manual` or `none`.
  `summary` is one or two sentences: what works with the candidate, then the findings. It becomes
  the page's row in the release report.
- Checks: one row for each thing the page says will happen. The result is `✅`, `❌` with the finding
  number, or `⏸️` when the step did not run because an earlier one failed. Commands that only prepare
  the next step share its row.
- Findings: one row per finding and one line per cell. Under the table, each finding gets at most 10
  lines: the command and the output lines that show the problem. A fix longer than one line goes
  there too, as the page text before and after.
- Evidence: at most 10 lines that show the page's main result with the candidate, like the installed
  chart version and the final state of the page's own resources.
