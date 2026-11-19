# Run one docs page

You get a page (its path under `doc/source/kuberay/`), `TAG`, `OLD`, `RAY`, `RES` (see the
release-test skill) and a kind cluster name `CLUSTER`. Run the page like a reader would, with the
release candidate, and reply with a report of what you found. The main agent saves it as
`OUT/report.md`. `OUT` is `RES/` plus the page path without the extension, like
`RES/getting-started/raycluster-quick-start/`. Leave the docs as they are, and put the files your
commands create in `OUT/work/`.

## Read

Read `RAY/doc/source/kuberay/<page>` and the files it includes. Before you run anything, check if
the page is `manual` or `none`, as step 2 of `.agents/skills/release-test/SKILL.md` defines them for
pages. For these two, the report is only the front matter and the Verdict, which gives the reason.

## Run the commands as written

Work in `OUT/work/` and use its absolute path. Shell state does not carry over between calls, so
start each call with `cd <work> && export KUBECONFIG=<work>/kubeconfig`. `kind create cluster` writes
that file, so every cluster you use is your own.

Run the page's commands in order, exactly as written. You may only change them in these ways. The last
column says if the change goes under Deviations in the report:

| The page has | You run | Deviation |
|---|---|---|
| a KubeRay version in a command, like `--version 1.7.0`, or `v1.7.0` in a URL, a ref or an image tag | the candidate: `--version <TAG without v>` or `TAG` | yes, with a count |
| steps that upgrade a KubeRay the reader already runs, like the upgrade guide | first the Helm commands of getting-started/operator-installation with `--version OLD`, then the page | yes |
| a `kind` command (`create cluster`, `delete cluster`, `load`) | the same command with `--name CLUSTER` instead of the page's name | no |
| no `kind create cluster` before the first kubectl or helm command | `kind create cluster --name CLUSTER` first | no |
| a value the reader fills in (`<your-bucket>`, `$YOUR_TOKEN`) | a value you choose | yes |
| a command that never ends (`port-forward`, `logs -f`, `--watch`, `watch`, a server) | the same command in the background; stop it when the page is done | no |
| `kubectl exec -it` | the same command without `-t`, because there is no terminal | no |
| a command that affects more than your run (`killall kubectl`, `pkill`) | the same command, only for the processes you started | no |
| `pip install` | the same command in a virtualenv under `OUT/work/` | no |
| a step that sends the reader to another page, like installing the operator | that page's commands for that step, on the cluster you already have | yes |
| a download for one OS and CPU, like `kubectl-ray_v1.7.0_linux_amd64.tar.gz` | the file for this machine | yes |
| a command that installs into your home or the system, like `cp kubectl-ray ~/.local/bin` | the same into `OUT/work/bin`, and add that folder to `PATH` | no |
| a choice of tabs, platforms or methods | each one that works on this machine and kind, see below | yes: the ones you did not run, and why |

A reader may take any choice that the page gives, so each one that works on this machine and kind must
work. Follow the page to its end with the first one. Then run each other one on a new cluster: delete
the cluster, create it again as before, and run the page from its start up to the end of that choice
and the step that checks its result. A choice that fails is a finding, and its severity is for a
reader who takes that choice. When a step sends you to another page, take the first choice of that
page: the run of that page tests the others.

You may add read-only commands at any time: `kubectl get`, `describe`, `logs`, `wait`, or a `curl`
that checks something. Use them to wait for what the page waits for, to compare output, and to find
causes. You may also pull an image the page uses on the host and load it with
`kind load docker-image IMAGE --name CLUSTER`, so that each page does not pull the Ray image again. If
the load fails, the cluster pulls the image itself.

When the page shows output, compare what a reader depends on: states, counts, and values the text
mentions. The generated part of a name (a ReplicaSet hash and a random suffix), ages, IPs,
timestamps and order can always differ: only the part of a name that a reader types or searches for
counts, like `workergroup` in `raycluster-kuberay-workergroup-worker-x7k2p`.

A **finding** is a command that fails, or output that does not match the page. Write one finding per
cause: for example, a wrong memory size in the text and the outputs it changes are one finding. A
wrong memory size and a changed worker group name are two findings. Retry a failed command once after a short
wait. After a finding, continue with the steps that do not depend on it. Give up on a wait after 15
minutes.

At the end, always run `kind delete cluster --name CLUSTER` and stop your background jobs, even if
something failed.

## Classify each finding

Each finding gets a type and a severity. The type says where the fix goes:

- `doc-bug`: the page is wrong.
- `sample-bug`: a sample in `ray-operator/config/samples/` at the tag is wrong: the YAML or its
  comments.
- `product-bug`: KubeRay at the tag is wrong: the operator, a chart or the plugin.
- `env`: kind or this machine cannot do it: CPU, memory, image pull limits, kernel features.
- `flaky`: it passed on the retry.

The severity says what the finding does to a reader who follows the page:

- `blocker`: the reader cannot go on, or does not get the result the page is about. A command or
  manifest that later steps need fails, or a resource never reaches the state the page says.
- `minor`: the reader still gets the result. For example: an old resource name, version or count in
  the page's output, a wrong name in the text, or a cleanup command that fails.

## Report

Reply with the report only, in this format. The values in it are made up: they show the format, not
the result of a run.

````markdown
---
page: getting-started/raycluster-quick-start.md
status: pass
blocker: 0
minor: 1
---

# getting-started/raycluster-quick-start

## Verdict

Pass, 1 minor finding. The RayCluster becomes ready and the job succeeds. The page's outputs show 3G
of memory, but the chart now gives 6Gi.

## Environment

- Page: https://docs.ray.io/en/master/kuberay/getting-started/raycluster-quick-start.html at ray master 45c4123857
- kind v0.32.0, node image `kindest/node:v1.36.1`
- Operator: `quay.io/kuberay/operator:v1.8.0-rc.0`, from the `kuberay-operator` chart 1.8.0-rc.0
- RayCluster: the `ray-cluster` chart 1.8.0-rc.0, with `rayproject/ray:2.52.0`

## What was verified

| # | Claim | Result |
|---|---|---|
| 1 | Step 2: the operator Pod is `Running` | ✅ |
| 2 | Step 3: 1 head Pod and 1 worker Pod are `Running` | ✅ `ready` after 95s |
| 3 | Step 3: `kubectl get rayclusters` shows 2 CPUs and 3G of memory | ⚠️ finding 1 |
| 4 | Step 4, Method 1: the job in the head Pod succeeds | ✅ |
| 5 | Step 4, Method 2: the `ray job submit` job succeeds | ✅ |

## Deviations

- `--version 1.7.0` → `--version 1.8.0-rc.0` (2 commands)
- Step 2: the Helm commands of getting-started/operator-installation. Not run: its Kustomize method,
  the run of that page tests it

## Findings

### 1. doc-bug (minor): the page shows 3G of memory, the chart gives 6Gi

Steps 3 and 4. Since #4588, the `ray-cluster` chart gives the head Pod 5Gi, so the RayCluster has
6Gi.

```
$ kubectl get rayclusters
NAME                 DESIRED WORKERS   AVAILABLE WORKERS   CPUS   MEMORY   GPUS   STATUS   AGE
raycluster-kuberay   1                 1                   2      6Gi      0      ready    95s
```

Suggested fix: `3G` → `6Gi` in Step 3, and `'memory': 3000000000.0` → `6442450944.0` in Step 4.

## Repro

```sh
kind create cluster
helm install kuberay-operator kuberay/kuberay-operator --version 1.8.0-rc.0
helm install raycluster kuberay/ray-cluster --version 1.8.0-rc.0
kubectl get rayclusters
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
  chart, and the charts, samples and Ray image the page uses. The node image is the part before `@`
  in the output of `docker inspect -f '{{.Config.Image}}' CLUSTER-control-plane`.
- What was verified: one row for each thing the page says will happen, with its step. Commands that
  only prepare the next step share its row. A row for a step that uses a sample from
  `ray-operator/config/samples/` names the sample file: the release report takes the result of the
  sample from this row. The result is one of:
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
- Repro: the shortest commands that show the result on a new kind cluster: each finding, or the main
  result when there are no findings.
