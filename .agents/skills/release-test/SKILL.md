---
name: release-test
description: >-
  Test a KubeRay release candidate on kind with the KubeRay docs on docs.ray.io and the sample YAMLs
  that no page runs: plan what to run, run it with the candidate, and record the old KubeRay
  versions in the docs, the doc changes the release needs, and every doc problem and bug found. Use
  for `/release-test v1.8.0-rc.0` or KubeRay release validation.
---

# Release test

The KubeRay docs on docs.ray.io come from the ray repository, in `doc/source/kuberay/`. This skill
runs those pages like a reader would, with the release candidate, and writes down what it finds. It
also runs the samples in `ray-operator/config/samples/` that no page runs. It only records. It does
not change the docs, the ray repository or this repository. A human decides what to fix.

Arguments: the tag to test, like `v1.8.0-rc.0`, and optionally some pages or samples to run again
(step 5). A sample is written `samples/<sample>`. If the tag is missing, ask for it. Run from the
root of this repository.

| Name | Example | Meaning |
|---|---|---|
| `TAG` | `v1.8.0-rc.0` | the tag the pages run with |
| `NEW` | `1.8.0` | the release version: `TAG` without `v` and `-rc.N` |
| `OLD` | `1.7.0` | the KubeRay version the docs install now (step 1) |
| `RAY` | `release-test/.cache/ray` | the ray docs, checked out at ray master |
| `RES` | `release-test/results/v1.8.0-rc.0` | the results folder. Each page has its own folder `RES/<page>/`, where `<page>` is the page path under `doc/source/kuberay/` without the extension. Each sample has `RES/samples/<sample>/`, where `<sample>` is its path under `ray-operator/config/samples/` without the extension |

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

5. Delete the kind clusters that an interrupted run left in `RES`. Each run deletes its own cluster at
   the end, and a run that stops early does not. A kubeconfig in a `work/` folder still opens its
   cluster only when that cluster is a leftover of this `RES`, so this never deletes a cluster of
   another session:

   ```sh
   find RES -path '*/work/kubeconfig' | while read -r k; do
     for c in $(grep -o 'kind-rt-[0-9]*' "$k" | sort -u); do
       kubectl --kubeconfig "$k" --context "$c" --request-timeout=5s get nodes >/dev/null 2>&1 \
         && KUBECONFIG="$k" kind delete cluster --name "${c#kind-}"
     done
   done
   ```

## 2. Plan the pages and the samples

`RES/plan.md` has three tables: the pages, the samples that a page runs, and the samples that no page
runs.

Every `.md`, `.rst` and `.ipynb` file under `RAY/doc/source/kuberay/`, except `index.md` files and
`references/`, gets one row in the first table. All pages together are about 1 MB, too big to read. So
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

A sample is a `.yaml` file under `ray-operator/config/samples/` at the tag. A sample runs with a
page when the page uses it in a command, like `kubectl apply -f` or `curl -LO`. A page that only
links to a sample does not run it. This lists every sample and the pages that run it, or `-` when no
page runs it:

```sh
for f in $(git ls-tree -r --name-only TAG -- ray-operator/config/samples | grep -E '\.ya?ml$'); do
  name=$(basename "$f" | sed 's/\./\\./g')
  pages=$(git -C RAY grep -E -- "(^|[^A-Za-z0-9._-])$name" -- doc/source/kuberay | grep -E 'kubectl|curl|wget|helm' \
    | cut -d: -f1 | sort -u | sed 's|^doc/source/kuberay/||' | paste -sd, - | sed 's/,/, /g')
  echo "${f#ray-operator/config/samples/}: ${pages:--}"
done
```

The script matches file names. Another file can have the same name, like the `sandbox.yaml` of
another project, so check that each page it lists uses this sample, and drop the pages that do not.

Each sample that a page runs gets one row in the second table. It does not run on its own, so its plan
follows its pages: `run` when at least one of them is `run`, and `manual` otherwise. Why says what its
`manual` pages need. Each sample that no page runs gets one row in the third table:

- `none`: the file has no RayCluster, RayJob, RayService or RayCronJob. It only helps another sample.
- `manual`: the sample asks for a GPU or a TPU (`nvidia.com/gpu`, `google.com/tpu`), or it needs a
  cloud cluster, a cloud bucket, gVisor or real credentials. Find the candidates with
  `git grep -l -i -E 'nvidia\.com/gpu|google\.com/tpu|gke|gcsfuse|gs://|s3://|gvisor|hf_token' TAG -- ray-operator/config/samples`,
  then decide from the lines around each match. A Ray resource like `num-gpus: "2"` asks for no real
  GPU, and a comment that names a cloud as one example does not need that cloud.
- `run`: all other samples.

```markdown
# Plan for v1.8.0-rc.0

ray master 45c4123857. Pages: 52 run, 14 manual, 11 none. Samples that a page runs: 35 run, 19 manual.
Samples that no page runs: 27 run, 14 manual, 4 none.

| Page | Plan | Why |
|---|---|---|
| getting-started/raycluster-quick-start.md | run | |
| user-guides/gpu.md | manual | needs a GPU |
| examples/rayserve-llm-example.md | manual | needs a Hugging Face token |
| user-guides/k8s-autoscaler.md | none | no commands |

## Samples that a page runs

| Sample | Pages | Plan | Why |
|---|---|---|---|
| ray-job.sample.yaml | getting-started/rayjob-quick-start.md, user-guides/dashboard.md | run | |
| ray-service.llm-serve.yaml | examples/rayserve-llm-example.md | manual | the page needs a Hugging Face token |

## Samples that no page runs

| Sample | Plan | Why |
|---|---|---|
| ray-job.sidecar-mode.yaml | run | |
| ray-cluster.tpu-v4-singlehost.yaml | manual | needs a TPU |
| gateway-api/istio-class.yaml | none | no Ray resource |
```

Show the user the counts and the `manual` and `none` rows of the first and the third table. The second
table follows from the first. Then stop and wait until they confirm the plan or edit `RES/plan.md`. If
`RES/plan.md` exists from an earlier run, ask if it is still good. When pages or samples are given as
arguments, skip the plan.

After the plan is confirmed, steps 3, 4 and 5 do not depend on each other. Start the step 4 subagent
and the step 5 runs at the same time.

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

## 5. Run the pages and the samples

Run the `run` pages of `RES/plan.md` and the `run` samples that no page runs, when they do not have a
report yet. A sample that a page runs has no run of its own. The report of a page is
`RES/<page>/report.md`, and the report of a sample is `RES/samples/<sample>/report.md`. Pages and
samples given as arguments always run again, even when they have a report.

Each page and each sample runs in its own subagent. A page follows
`.agents/skills/release-test/run-page.md`, and a sample follows
`.agents/skills/release-test/run-sample.md`. Give the subagent the page or the sample, `TAG`, `NEW`,
`RAY`, `RES` and a kind cluster name `rt-<n>` that no running subagent uses and that
`kind get clusters` does not list. A cheaper model is enough. Each run creates its own kind cluster,
so run at most 3 at once, and at most the CPU count from `docker info` divided by 4.

Each subagent replies with its report. Save it as the `report.md` above. The `status` in its front
matter is `pass` or `fail`. It is `manual` or `none` when the page or the sample turns out different
from the plan.

A run can fail for a reason that does not repeat on a new cluster. So run each page and sample whose
report is `fail` one more time, in a new subagent. Give it `RES/rerun` as its `RES`, so that it
starts with an empty work folder, and save its reply as the rerun report: `RES/rerun/<page>/report.md`
or `RES/rerun/samples/<sample>/report.md`. The first report stays as it is. When a page or a sample
given as an argument runs again, delete its old rerun report first.

A subagent that stops early, on an error or a timeout, leaves its kind cluster behind. When no
subagent is running any more, run the cleanup of step 1 again.

This step is done when every page and sample that this step runs has its `report.md`, every `fail`
report has its rerun report, and the cleanup has run.

## 6. Report

Write `RES/REPORT.md` from `plan.md`, `versions.md`, `commits.md` and the page and sample reports, in
this format. Leave out empty sections:

```markdown
# KubeRay v1.8.0-rc.0 release test

## Summary

ray master 45c4123857, 2026-11-02. The docs install 1.7.0 (the latest release is 1.7.1). The pages
and the samples ran with 1.8.0-rc.0. The Samples column counts the 45 samples that no page runs. The
other 54 run with a page.

| Result | Pages | Samples |
|---|---|---|
| ✅ Pass | 28 | 24 |
| ⚠️ Pass, minor findings | 13 | 1 |
| ❌ Fail | 3 | 1 |
| 🔁 Unstable | 0 | 1 |
| 👤 For a human to run | 12 | 14 |
| ➖ Nothing to run | 20 | 4 |
| ⏸️ Not run | 0 | 0 |

- Release blockers: 1, user-guides/rayservice: the RayService never becomes Ready.
- Broken pages: 2.
- Broken samples: 1, samples/ray-cluster.embedded-gcs-ft: the head never starts.
- Unstable: 1, samples/ray-cronjob-timezone.sample: the head Pod was `OOMKilled` in the first run, and
  the rerun passed.
- Minor findings: 20 in 14 pages and 1 sample.
- Old versions: 95 references in 31 files.
- Doc changes from the release: 2 need updates.
- New features without a page: 3, 2 of them not tested.

## Results

Each page and sample that ran: ✅ no findings, ⚠️ only `minor` findings, ❌ at least one `blocker`
in both runs, 🔁 a `blocker` in the first run that the rerun does not have. The pages come first, then
the samples.

| Page or sample | Result | Blocker | Minor | Summary |
|---|---|---|---|---|
| [getting-started/rayjob-quick-start](getting-started/rayjob-quick-start/report.md) | ✅ | | | Both RayJobs succeed. |
| [getting-started/raycluster-quick-start](getting-started/raycluster-quick-start/report.md) | ⚠️ | | 1 | The RayCluster becomes ready and the job succeeds. The page says 3G of memory, but the chart requests 5Gi. |
| [user-guides/rayservice](user-guides/rayservice/report.md) | ❌ | 1 | 1 | The RayService never becomes Ready. |
| [samples/ray-job.sidecar-mode](samples/ray-job.sidecar-mode/report.md) | ✅ | | | The RayJob succeeds. |
| [samples/ray-cluster.embedded-gcs-ft](samples/ray-cluster.embedded-gcs-ft/report.md) | ❌ | 1 | | The head never starts: the sample's Ray image has no RocksDB backend. |
| [samples/ray-cronjob-timezone.sample](samples/ray-cronjob-timezone.sample/report.md) | 🔁 | | | First run: the head Pod was `OOMKilled` and the RayJob never started. Rerun: the RayJob succeeds. |

## Release blockers

`blocker` findings of type `product-bug`: the operator, a chart or the plugin at the tag is wrong.

| Page or sample | Step | Problem |
|---|---|---|
| [user-guides/rayservice](user-guides/rayservice/report.md) | Step 3 | the RayService never becomes Ready: ... |

## Broken pages

`blocker` findings of type `doc-bug`: the page does not work as written.

| Page | Step | Problem | Proposed fix |
|---|---|---|---|
| [ecosystem/kueue](ecosystem/kueue/report.md) | Step 2 | Kueue v0.13.4 denies the RayCluster | install Kueue v0.15.2 |

## Broken samples

`blocker` findings of type `sample-bug`: the sample does not work as it is at the tag.

| Page or sample | Step | Problem | Proposed fix |
|---|---|---|---|
| [samples/ray-cluster.embedded-gcs-ft](samples/ray-cluster.embedded-gcs-ft/report.md) | RayCluster | the head never starts: `rayproject/ray:2.52.0` has no RocksDB backend | `rayproject/ray:2.52.0` → `rayproject/ray:2.57.0` |

## Minor findings

`minor` findings of type `doc-bug`, `sample-bug` and `product-bug`.

| Page or sample | Type | Step | Problem | Proposed fix |
|---|---|---|---|---|
| [getting-started/raycluster-quick-start](getting-started/raycluster-quick-start/report.md) | doc-bug | Step 3 | the page says 3G of memory, the chart requests 5Gi | "at least 3G" → "at least 5Gi" |

## Old versions

95 references in 31 files use a KubeRay version older than 1.8.0. [versions.md](versions.md) lists
them all. Older than 1.7.0: user-guides/observability.md:51 (1.2.2),
troubleshooting/troubleshooting.md:126 (1.6.0).

## Doc changes from the release

The `needs update` rows of [commits.md](commits.md).

## New features without a page

The `needs docs` rows of [commits.md](commits.md). No page covers these changes, so only the sample
tests them. Tested is the Result of the sample, or of the page that runs it, with a link to the
report. It is `not tested` with the reason when the tag has no sample, or when the sample is for a
human to run, has nothing to run or did not run.

| PR | Change for readers | Sample | Tested |
|---|---|---|---|
| 9f8e7d6 [Feature] Terminate idle RayClusters (#5303) | new field `idleTerminationOptions` on RayCluster | ray-cluster.autoscaler-v2-terminate-idle-cluster.yaml | [✅](samples/ray-cluster.autoscaler-v2-terminate-idle-cluster/report.md) |
| 7a6b5c4 [RayService] Attach incremental upgrade to an existing Gateway (#5288) | new field `gatewayRef` on RayService | ray-service.incremental-upgrade-gateway-ref.yaml | not tested: for a human to run, needs a Gateway controller |
| def5678 [Feature] Suspend RayCronJob (#5202) | new field `spec.suspend` on RayCronJob | none | not tested: no sample |

## For a human to run

GPU, TPU, cloud and credential pages and samples do not run here. A human runs each one on what it
needs and records the result.

| Page or sample | Needs | Samples the page runs |
|---|---|---|
| [user-guides/gcp-gke-gpu-cluster](https://docs.ray.io/en/master/kuberay/user-guides/gcp-gke-gpu-cluster.html) | GKE with a GPU node pool | |
| [examples/rayserve-llm-example](https://docs.ray.io/en/master/kuberay/examples/rayserve-llm-example.html) | GPUs and a Hugging Face token | ray-service.llm-serve.yaml |
| [samples/ray-cluster.tpu-v4-singlehost](https://github.com/kuberay-project/kuberay/blob/v1.8.0-rc.0/ray-operator/config/samples/ray-cluster.tpu-v4-singlehost.yaml) | GKE with TPU nodes | |

The rows are the `manual` pages of plan.md, the `manual` samples that no page runs, and the pages and
samples whose run came back `manual`. The last column has the `manual` samples of plan.md that the page
runs: the human who runs the page records them too.

## Open findings

`env` and `flaky` findings: the page or sample, the severity and the reason for each. Then each 🔁
page and sample: the `blocker` finding of the first run, what the rerun saw, and a link to each of
the two reports.

## Not run

The `run` pages of plan.md and the `run` samples that no page runs, when they have no report: for
example pages left out of a partial run.
```

The Result, Blocker and Minor of each page and sample come from the front matter of its report, and
the Summary is its Verdict. Each row of the four findings tables is one finding of a report: the
Problem is its heading and the Proposed fix is its suggested fix.

A `fail` report has a rerun report (step 5). A `blocker` finding stands when the rerun report has it
too: the same step and the same cause. Then the result is ❌. When no `blocker` finding stands, the
result is 🔁: the rerun passed, or it failed for another cause. The `blocker` findings of both runs
then go to Open findings, and not to the three `blocker` tables. The Blocker column counts only the
findings that stand, and the Summary says what each run saw.

Also write `RES/results.csv`, for people who track the release in a spreadsheet. It has one row per
page and per sample of plan.md, except the `none` rows:

```csv
Name / Link,Type,Assignee,2nd Reviewer,Status,Priority,Screenshot Link,Comment,Follow-up
https://docs.ray.io/en/master/kuberay/getting-started/raycluster-quick-start.html,Doc,,,Has Issue,,getting-started/raycluster-quick-start/report.md,"Minor: The RayCluster becomes ready and the job succeeds. The page says 3G of memory, but the chart requests 5Gi.",
https://docs.ray.io/en/master/kuberay/user-guides/gpu.html,Doc,,,Not Tested,,,needs a GPU,
https://github.com/kuberay-project/kuberay/blob/v1.8.0-rc.0/ray-operator/config/samples/ray-job.sidecar-mode.yaml,Sample YAML,,,No Issue,,samples/ray-job.sidecar-mode/report.md,The RayJob succeeds.,
https://github.com/kuberay-project/kuberay/blob/v1.8.0-rc.0/ray-operator/config/samples/ray-cluster.tpu-v4-singlehost.yaml,Sample YAML,,,Not Tested,,,needs a TPU,
https://github.com/kuberay-project/kuberay/blob/v1.8.0-rc.0/ray-operator/config/samples/ray-job.sample.yaml,Sample YAML,,,No Issue,,getting-started/rayjob-quick-start/report.md,Runs with getting-started/rayjob-quick-start.,
https://github.com/kuberay-project/kuberay/blob/v1.8.0-rc.0/ray-operator/config/samples/ray-service.llm-serve.yaml,Sample YAML,,,Not Tested,,,needs GPUs and a Hugging Face token. Runs with examples/rayserve-llm-example.,
```

`Type` is `Doc` for a page and `Sample YAML` for a sample. `Status` is a value that the tracking sheet
accepts: `No Issue` (✅), `Has Issue` (⚠️, ❌ and 🔁) or `Not Tested` (👤 and ⏸️). `Screenshot Link` is
the path of the report under `RES`. `Comment` is the Summary of the Results row, or what a `manual` row
needs. The `Comment` of a ⚠️ row starts with `Minor:`. People fill in the other columns.

A sample that a page runs has no report of its own. Its row comes from the reports of the pages that
run it:

- `Has Issue`: a report has a `sample-bug` finding about the sample, or a `blocker` finding in a step
  that uses the sample. The `Comment` is the heading of that finding, and it starts with `Minor:` when
  the finding is `minor`.
- `No Issue`: a page ran the steps that use the sample, and no report has such a finding.
- `Not Tested`: no page reached those steps, or the sample is `manual` in plan.md. The `Comment` is
  the reason.

`Screenshot Link` is the report of that page, and the `Comment` ends with `Runs with <page>.`

Then tell the user the release blockers, the broken pages, the broken samples, the unstable pages
and samples, the counts and the paths to `RES/REPORT.md` and `RES/results.csv`.
