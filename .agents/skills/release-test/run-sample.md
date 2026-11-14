# Run one sample

You get a sample (its path under `ray-operator/config/samples/`), `TAG`, `RES` (see the release-test
skill) and a kind cluster name `CLUSTER`. You run in the KubeRay repository. No docs page runs this
sample, so its comments are the only instructions. Check that it works with the release candidate,
and reply with a report of what you found. The main agent saves it as `OUT/report.md`. `OUT` is
`RES/samples/` plus the sample path without `.yaml`, like `RES/samples/ray-cronjob-timezone.sample/`.
Put the files your commands create in `OUT/work/`.

## Read

Get the sample from the tag and read it:
`git show TAG:ray-operator/config/samples/<sample> > OUT/work/<file name>`. Before you run anything,
check if the sample is `manual` or `none`, as step 2 of `.agents/skills/release-test/SKILL.md`
defines them for samples. For these two, the report is only the front matter and the Verdict, which
gives the reason.

## Run

Work in `OUT/work/` and use its absolute path. Shell state does not carry over between calls, so
start each call with `cd <work> && export KUBECONFIG=<work>/kubeconfig`. `kind create cluster` writes
that file, so every cluster you use is your own.

1. Create the cluster: `kind create cluster --name CLUSTER`.
2. Install the candidate, and wait until the operator Deployment is `Available`:

   ```sh
   helm repo add kuberay https://ray-project.github.io/kuberay-helm/
   helm repo update kuberay
   helm install kuberay-operator kuberay/kuberay-operator --version <TAG without v>
   ```

   A sample can need a feature gate that is off by default: RayCronJob does, and so do some fields.
   `git show TAG:ray-operator/pkg/features/features.go` lists every gate, what it enables and its
   default. Turn the gate on in the same `helm install`:
   `--set "featureGates[0].name=<gate>" --set "featureGates[0].enabled=true"`. This replaces the
   chart's list of gates, so also set each gate that the chart's `values.yaml` turns on and that
   `features.go` turns off by default.
3. Run the commands that the sample's comments give, in order and as written.
4. Apply the sample: `kubectl apply -f <file name>`. A RayCronJob waits for its schedule, so apply it
   with `schedule: "* * * * *"`. It then creates a RayJob every minute. When the first RayJob exists,
   stop the others: `kubectl patch raycronjob <name> --type merge -p '{"spec":{"suspend":true}}'`.
5. Wait for every Ray resource in the file to work:

   | Kind | Works when |
   |---|---|
   | RayCluster | the `RayClusterProvisioned` condition is `True`: the head Pod and all worker Pods are running and ready |
   | RayJob | `.status.jobStatus` is `SUCCEEDED` and `.status.jobDeploymentStatus` is `Complete` |
   | RayService | the `Ready` condition is `True` |
   | RayCronJob | `.status.lastScheduleTime` is set, and the RayJob it created works |

6. Check each result that the comments say will happen, like a cluster that suspends when it is idle.
7. Check the `RESTARTS` column of `kubectl get pods`. A container of the sample that restarted is a
   finding, even when the resource works in the end. Get the reason, like `OOMKilled`, from
   `kubectl get pod <pod> -o jsonpath='{.status.containerStatuses[*].lastState.terminated.reason}'`.
8. Delete the sample: `kubectl delete -f <file name> --ignore-not-found --timeout=5m` returns without
   an error. A resource that the sample deletes by itself, like a RayJob with a deletion rule, is
   already gone, and that is not a finding.
9. Always run `kind delete cluster --name CLUSTER` at the end, even if something failed.

You may add read-only commands at any time: `kubectl get`, `describe`, `logs`, `wait`. Use them to
wait and to find causes. You may also pull an image the sample uses on the host and load it with
`kind load docker-image IMAGE --name CLUSTER`. If the load fails, the cluster pulls the image itself.

A **finding** is a resource that `kubectl apply` rejects, a resource that does not work, a result
from the comments that does not happen, a container that restarted, or a delete that fails. Write
one finding per cause. Retry a failed command once after a short wait. After a finding, continue with
the steps that do not depend on it. Give up on a wait after 15 minutes.

## Report

Classify each finding and write the report as "Classify each finding" and "Report" in
`.agents/skills/release-test/run-page.md` say, with these differences:

- A wrong sample or a wrong comment is `sample-bug`. A wrong operator or chart is `product-bug`. No
  page is involved, so no finding is `doc-bug`.
- The front matter has `sample:` where a page report has `page:`.
- Environment: the first line is the sample's URL at the tag.
- What was verified: one row for the apply, one for each Ray resource, one for each result from the
  comments, one for the restarts, and one for the delete. A result that a deviation makes impossible
  to check gets `⏭️` and the reason.
- Deviations: only a feature gate you turned on, a RayCronJob schedule you changed, and a value you
  chose. Write `None.` when there are none.

The values in this example are made up: they show the format, not the result of a run.

````markdown
---
sample: ray-job.light-weight-submitter.yaml
status: pass
blocker: 0
minor: 0
---

# samples/ray-job.light-weight-submitter

## Verdict

Pass. The RayJob succeeds with the light-weight submitter image.

## Environment

- Sample: https://github.com/kuberay-project/kuberay/blob/v1.8.0-rc.0/ray-operator/config/samples/ray-job.light-weight-submitter.yaml
- kind v0.32.0, node image `kindest/node:v1.36.1`
- Operator: `quay.io/kuberay/operator:v1.8.0-rc.0`, from the `kuberay-operator` chart 1.8.0-rc.0
- Images: `rayproject/ray:2.52.0`, `quay.io/kuberay/submitter:v1.5.1`

## What was verified

| # | Claim | Result |
|---|---|---|
| 1 | `kubectl apply` accepts the RayJob and the ConfigMap | ✅ |
| 2 | The RayJob is `SUCCEEDED` and `Complete` | ✅ after 75s |
| 3 | No container of the sample restarted | ✅ |
| 4 | `kubectl delete` removes the sample | ✅ |

## Deviations

None.

## Findings

None.

## Repro

```sh
kind create cluster
helm install kuberay-operator kuberay/kuberay-operator --version 1.8.0-rc.0
git show v1.8.0-rc.0:ray-operator/config/samples/ray-job.light-weight-submitter.yaml > sample.yaml
kubectl apply -f sample.yaml
kubectl wait --for=jsonpath='{.status.jobStatus}'=SUCCEEDED rayjob/rayjob-light-weight-submitter --timeout=15m
```
````
