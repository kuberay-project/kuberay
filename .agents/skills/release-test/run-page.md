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

For these two, the report is only the front matter and the Verdict, which gives the reason.

## Run the commands as written

Work in `OUT/work/` and use its absolute path. Shell state does not carry over between calls, so
start each call with `cd <work> && export KUBECONFIG=<work>/kubeconfig`. `kind create cluster` writes
that file, so every cluster you use is your own.

Run the page's commands in order, exactly as written. You may only change them in these ways. The last
column says if the change goes under Deviations in the report:

| The page has | You run | Deviation |
|---|---|---|
| a KubeRay version in a command, like `--version 1.7.0`, or `v1.7.0` in a URL, a ref or an image tag | the candidate: `--version <TAG without v>` or `TAG`. Keep the version the upgrade guide upgrades from | yes, with a count |
| a `kind` command (`create cluster`, `delete cluster`, `load`) | the same command with `--name CLUSTER` instead of the page's name | no |
| no `kind create cluster` before the first kubectl or helm command | `kind create cluster --name CLUSTER` first | no |
| a value the reader fills in (`<your-bucket>`, `$YOUR_TOKEN`) | a value you choose | yes |
| a command that never ends (`port-forward`, `logs -f`, `--watch`, `watch`, a server) | the same command in the background; stop it when the page is done | no |
| a command that affects more than your run (`killall kubectl`, `pkill`) | the same command, only for the processes you started | no |
| `pip install` | the same command in a virtualenv under `OUT/work/` | no |
| a step that sends the reader to another page, like installing the operator | that page's commands for that step, on the cluster you already have | yes |
| a choice of tabs, platforms or methods | the first one that works on Linux and kind | yes: the ones you did not run |

You may add read-only commands at any time: `kubectl get`, `describe`, `logs`, `wait`, or a `curl`
that checks something. Use them to wait for what the page waits for, to compare output, and to find
causes. You may also pull an image the page uses on the host and load it with
`kind load docker-image IMAGE --name CLUSTER`, so that each page does not pull the Ray image again. If
the load fails, the cluster pulls the image itself.

When the page shows output, compare what a reader depends on: states, counts, and values the text
mentions. Random name suffixes, ages, IPs, timestamps and order can always differ.

A **finding** is a command that fails, or output that does not match the page. Write one finding per
cause: for example, a wrong memory size in the text and the outputs it changes are one finding. A
wrong memory size and a changed Pod name are two findings. Retry a failed command once after a short
wait. After a finding, continue with the steps that do not depend on it. Give up on a wait after 15
minutes.

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
---

# getting-started/raycluster-quick-start

## Verdict

Pass, 1 minor finding. The RayCluster becomes ready and the job succeeds. The page says 3G of memory,
but the chart requests 5Gi.

## Environment

- Page: https://docs.ray.io/en/master/kuberay/getting-started/raycluster-quick-start.html at ray master 45c4123857
- kind v0.32.0, node image `kindest/node:v1.26.0`
- Operator: `quay.io/kuberay/operator:v1.8.0-rc.0`, from the `kuberay-operator` chart 1.8.0-rc.0
- RayCluster: the `ray-cluster` chart 1.8.0-rc.0, with `rayproject/ray:2.52.0`

## What was verified

| # | Claim | Result |
|---|---|---|
| 1 | Step 2: the operator Pod is `Running` | ✅ |
| 2 | Step 3: 1 head Pod and 1 worker Pod are `Running` | ✅ `ready` after 95s |
| 3 | Step 3: the head Pod needs "at least 3G of memory" | ⚠️ finding 1 |
| 4 | Step 4: the `ray job submit` job succeeds | ✅ |

## Deviations

- `--version 1.7.0` → `--version 1.8.0-rc.0` (2 commands)
- Not run: "Option 2: GKE" tab, it needs GKE

## Findings

### 1. doc-bug (minor): the page says 3G of memory, the chart requests 5Gi

Step 3. The `ray-cluster` chart requests 5Gi for the head Pod since #4588.

```
$ kubectl get pod raycluster-kuberay-head-x7k2p -o jsonpath='{.spec.containers[0].resources.requests.memory}'
5Gi
```

Suggested fix: "at least 3G" → "at least 5Gi".

## Repro

```sh
kind create cluster
helm install kuberay-operator kuberay/kuberay-operator --version 1.8.0-rc.0
helm install raycluster kuberay/ray-cluster --version 1.8.0-rc.0
kubectl get pod -l ray.io/node-type=head -o jsonpath='{.items[0].spec.containers[0].resources.requests.memory}'
```
````

Write these sections in this order, and keep them short. A reader gets the result from the Verdict
and the table:

- Front matter: `status` is `pass` (no `blocker` finding), `fail` (at least one), `manual` or `none`.
  `blocker` and `minor` count the findings.
- Verdict: `Pass`, `Pass, N minor findings` or `Fail, N blockers`, then one or two sentences: what
  works with the candidate, then the findings. It becomes the page's row in the release report.
- Environment: one line each for the page URL with the ray commit
  (`git -C RAY rev-parse --short=10 HEAD`), the kind version and node image, the operator image and
  chart, and the charts, samples and Ray image the page uses.
- What was verified: one row for each thing the page says will happen, with its step. Commands that
  only prepare the next step share its row. The result is one of:
  - `✅`, and if it helps a few words of proof, like a state, a count or a time
  - `⚠️ finding N` for a `minor` finding, or `❌ finding N` for a `blocker` finding, and nothing else:
    the finding has the details
  - `⏭️` when the step did not run because an earlier one failed
- Deviations: the changes that the rewrite table marks `yes`, one line each, and nothing else. A
  difference that matters to the reader is a finding.
- Findings: one heading per finding, with the type, the severity and the problem in one line. Below
  it: the step and the cause in one or two sentences, at most 10 lines of the command and output that
  show it, and the suggested fix in one line. A fix longer than one line is the page text before and
  after. Write `None.` when there are no findings.
- Repro: the shortest commands that show the findings on a new kind cluster. Leave it out when there
  are no findings.
