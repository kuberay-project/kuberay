---
name: release-test
description: >-
  Test a KubeRay release candidate with the KubeRay docs on docs.ray.io: plan which pages to run on
  kind, run them with the candidate, and record the old KubeRay versions in the docs, the doc changes
  the release needs, and every doc problem and bug found. Use for `/release-test v1.8.0-rc.0` or
  KubeRay release validation.
---

# Release test

The KubeRay docs on docs.ray.io come from the ray repository, in `doc/source/kuberay/`. This skill
runs those pages like a reader would, with the release candidate, and writes down what it finds. It
only records. It does not change the docs, the ray repository or this repository. A human decides
what to fix.

Arguments: the tag to test, like `v1.8.0-rc.0`, and optionally some pages to run again (step 5). If
the tag is missing, ask for it. Run from the root of this repository.

| Name | Example | Meaning |
|---|---|---|
| `TAG` | `v1.8.0-rc.0` | the tag the pages run with |
| `NEW` | `1.8.0` | the release version: `TAG` without `v` and `-rc.N` |
| `OLD` | `1.7.0` | the KubeRay version the docs install now (step 1) |
| `RAY` | `release-test/.cache/ray` | the ray docs, checked out at ray master |
| `RES` | `release-test/results/v1.8.0-rc.0` | the results folder. Each page has its own folder `RES/<page>/`, where `<page>` is the page path under `doc/source/kuberay/` without the extension |

If a step's file already exists in `RES`, skip that step.

## 1. Set up

1. Check that the release exists. Stop if the chart is not listed:

   ```sh
   helm repo add kuberay https://ray-project.github.io/kuberay-helm/
   helm repo update kuberay
   helm search repo kuberay/kuberay-operator --devel --version <TAG without v>
   ```

2. Get the tags: `git fetch --tags https://github.com/kuberay-project/kuberay`
3. Check out the docs at ray master:

   ```sh
   test -d RAY || git clone --filter=blob:none --no-checkout https://github.com/ray-project/ray RAY
   git -C RAY fetch origin master
   git -C RAY sparse-checkout set --no-cone /doc/source/kuberay/ /python/ray/autoscaler/kuberay/init-config.sh /release/k8s_tests/run_gcs_ft_on_k8s.py
   git -C RAY switch --detach origin/master
   ```

4. Find `OLD`. It is the most common version in this list. Charts of other tools show up only once or
   twice:

   ```sh
   git -C RAY grep -h -o -E -- '--version [0-9]+\.[0-9]+\.[0-9]+' -- doc/source/kuberay | sort | uniq -c | sort -rn
   ```

## 2. Plan the pages

Every `.md`, `.rst` and `.ipynb` file under `RAY/doc/source/kuberay/`, except `index.md` files and
`references/`, gets one row in `RES/plan.md`. All pages together are about 1 MB, too big to read. So
decide each page from grep results, not by reading the whole page:

- `none`: the page has no code block with commands to run. A code block without a language that has
  `$ kubectl` lines counts as commands.
- `manual`: the page needs a GPU or a TPU anywhere: in a command, a manifest or a prerequisite. One
  GPU section is enough to make the whole page `manual`. A page is also `manual` when its main steps
  need a cloud cluster (GKE, EKS, AKS, ACK), gVisor, or real credentials and paid services, like a
  Hugging Face token or a cloud bucket. Find the candidates with
  `grep -l -i -E 'gpu|google\.com/tpu|gcloud |eksctl|az aks|aliyun|gvisor|hf_token|gsutil|aws s3'`,
  then decide from the page headings (`grep -n '^#'`) and the lines around each match
  (`grep -n -C3`). A page that only names a cloud as one place to run, or uses a cloud in an optional
  section, is `run`.
- `run`: all other pages.

```markdown
# Plan for v1.8.0-rc.0

ray master 45c4123857. 52 run, 14 manual, 11 none.

| Page | Plan | Why |
|---|---|---|
| getting-started/raycluster-quick-start.md | run | |
| user-guides/gpu.md | manual | needs a GPU |
| examples/rayserve-llm-example.md | manual | needs a Hugging Face token |
| user-guides/k8s-autoscaler.md | none | no commands |
```

Show the user the counts and the `manual` and `none` rows. Then stop and wait until they confirm the
plan or edit `RES/plan.md`. If `RES/plan.md` exists from an earlier run, ask if it is still good.
When pages are given as arguments, skip the plan.

After the plan is confirmed, steps 3, 4 and 5 do not depend on each other. Start the step 4 subagent
and the step 5 page runs at the same time.

## 3. Record old versions

In `RAY/doc/source/kuberay/`, `init-config.sh` and `run_gcs_ft_on_k8s.py`, find every KubeRay version
that is older than `NEW` and tells the reader what to install or use:

- `--version` of the `kuberay/kuberay-operator`, `kuberay/ray-cluster` and `kuberay/kuberay-apiserver`
  charts
- links and refs to samples: `kuberay/vX/` and `refs/tags/vX/` in raw.githubusercontent.com and
  `/blob/` URLs, `?ref=vX`, `targetRevision`, image tags like `kuberay/operator:vX`,
  `releases/download/vX/`
- text that names the version the page uses, like "This guide uses KubeRay vX"

Versions used in other ways are not old: minimum versions and history ("requires KubeRay 1.6.0 or
newer", "Starting with KubeRay 1.6.0", deprecation notes), the version the upgrade guide upgrades
from, `references/api.md` (a ray bot updates it from the release tag), and versions of Ray and other
tools.

Write `RES/versions.md`:

```markdown
# KubeRay versions in the docs

The docs install 1.7.0. The release is 1.8.0. 95 references in 31 files are older.

| Page | Line | Text | Version |
|---|---|---|---|
| user-guides/observability.md | 51 | `helm upgrade kuberay-operator kuberay/kuberay-operator --version 1.2.2 \` | 1.2.2 |

## Kept

| Page | Line | Version | Why |
|---|---|---|---|
| user-guides/mtls.md | 29 | 1.7.0 | minimum version for the guide |

## Pin candidates

`kuberay/master/` links whose file exists at the tag (`git cat-file -e TAG:<path>`):

- examples/rayjob-agent-sandbox.md:42 `ray-operator/config/samples/...`
```

Kept lists the lines that have `OLD` but use it in another way. These are the lines a version bump
could change by mistake. This step is done when every match of `git -C RAY grep -n -F OLD` in those
files is a row in one of the two tables.

## 4. Read the release's commits

Start a subagent that follows `.agents/skills/release-test/commits.md`, and give it `OLD`, `TAG` and
`RAY`. It replies with the changes readers can notice and what each one means for the docs. Save the
reply as `RES/commits.md`.

## 5. Run the pages

Run the `run` rows of `RES/plan.md` that do not have `RES/<page>/report.md` yet. Pages given as
arguments always run again, even when they have a report.

Each page runs in its own subagent, which follows `.agents/skills/release-test/run-page.md`. Give it
the page, `TAG`, `NEW`, `RAY`, `RES` and a kind cluster name `rt-<n>` that no running page uses. A
cheaper model is enough. Each page creates its own kind cluster, so run at most 3 pages at once, and
at most the CPU count from `docker info` divided by 4.

Each subagent replies with its page report. Save it as `RES/<page>/report.md`. The `status` in its
front matter is `pass` or `fail`. It is `manual` or `none` when the page turns out different from the
plan. This step is done when every planned page has `RES/<page>/report.md`.

## 6. Report

Write `RES/REPORT.md` from `plan.md`, `versions.md`, `commits.md` and the page reports, in this
format. Leave out empty sections:

```markdown
# KubeRay v1.8.0-rc.0 docs test

## Summary

ray master 45c4123857, 2026-11-02. The docs install 1.7.0 (the latest release is 1.7.1). The pages
ran with 1.8.0-rc.0.

| Result | Pages |
|---|---|
| ✅ Pass | 41 |
| ❌ Fail | 3 |
| 👤 For a human to run | 12 |
| ➖ No commands | 20 |
| ⏸️ Not run | 0 |

- Release blockers: 1, user-guides/rayservice: the RayService never becomes Ready.
- Doc problems: 5 in 3 pages.
- Old versions: 95 references in 31 files.
- Doc changes from the release: 2 need updates, 1 needs docs.

## Results

Each page that ran: ✅ if it has no findings, ❌ if it has at least one.

| Page | Result | Findings |
|---|---|---|
| [getting-started/rayjob-quick-start](getting-started/rayjob-quick-start/report.md) | ✅ | |
| [getting-started/raycluster-quick-start](getting-started/raycluster-quick-start/report.md) | ❌ | 1 doc-bug |
| [user-guides/rayservice](user-guides/rayservice/report.md) | ❌ | 1 product-bug, 1 doc-bug |

## Release blockers

`product-bug` findings: bugs in KubeRay at the tag.

| Page | Step | Finding |
|---|---|---|
| [user-guides/rayservice](user-guides/rayservice/report.md) | Deploy the RayService | the RayService never becomes Ready: ... |

## Doc problems

`doc-bug` findings.

| Page | Step | Problem | Proposed fix |
|---|---|---|---|
| [getting-started/raycluster-quick-start](getting-started/raycluster-quick-start/report.md) | Deploy a RayCluster | page says 3G, the chart requests 5Gi | 3G → 5Gi |

## Old versions

95 references in 31 files use a KubeRay version older than 1.8.0. [versions.md](versions.md) lists
them all. Older than 1.7.0: user-guides/observability.md:51 (1.2.2),
troubleshooting/troubleshooting.md:126 (1.6.0).

## Doc changes from the release

The `needs update` and `needs docs` rows of [commits.md](commits.md).

## For a human to run

GPU, TPU, cloud and credential pages do not run here. A human runs each one on what it needs and
records the result.

| Page | Needs |
|---|---|
| [user-guides/gcp-gke-gpu-cluster](https://docs.ray.io/en/master/kuberay/user-guides/gcp-gke-gpu-cluster.html) | GKE with a GPU node pool |
| [examples/rayserve-llm-example](https://docs.ray.io/en/master/kuberay/examples/rayserve-llm-example.html) | GPUs and a Hugging Face token |

The rows are the `manual` rows of plan.md and the pages whose run came back `manual`.

## Open findings

`env` and `flaky` findings: the page and the reason for each.

## Not run

The `run` rows of plan.md that have no report, for example pages left out of a partial run.
```

Then tell the user the release blockers, the counts and the path to `RES/REPORT.md`.
