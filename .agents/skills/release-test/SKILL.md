---
name: release-test
description: >-
  Validate a KubeRay release candidate on a local kind cluster: plan changed Items, run the manifest,
  report failures. Use for `/release-test v1.8.0-rc.0` or when asked to run KubeRay release validation.
---

# Release test

The argument is the tag under test, for example `v1.8.0-rc.0`. Ask for it if it is missing.

1. **Check the release exists.** `helm repo add kuberay https://ray-project.github.io/kuberay-helm/`,
   `helm repo update`, then `helm search repo kuberay/kuberay-operator --devel --version <tag without v>`
   must list it. If not, stop: there is nothing to test yet.
2. **Plan what changed.** Run `go run ./release-test stale --kuberay-version <tag>`. If it prints Items,
   plan them with the `release-test-plan` skill and the tag, then show
   `git diff --stat release-test/manifest.yaml` and stop until the user has reviewed the diff. Never
   run units nobody reviewed.
3. **Cluster.** The current kube context must be a kind cluster; create one with
   `kind create cluster --name release-test` if there is none.
4. **Run.** Start `go run ./release-test run --kuberay-version <tag>` in the background; it takes hours. It
   skips units whose page changed since they were planned, resumes where it stopped, and writes
   `release-test/results/<tag>/SUMMARY.md` at the end. Do not poll it; wait for it to exit.
5. **Triage.** For each Item in SUMMARY.md's Fail table whose Triage column is empty, run the
   `release-test-run` skill on `release-test/results/<tag>/<id>/` in a subagent of its own, so the logs
   stay out of this context. Then run `go run ./release-test summary --kuberay-version <tag>`.
6. **Report.** Read `release-test/results/<tag>/SUMMARY.md` and give the user the Fail table. For each
   `plan-bug`, offer to apply the fix from the report's Triage section to the manifest and re-run the
   Item with `go run ./release-test run --kuberay-version <tag> <id>`. Items under Stale go back to step
   2. Never open GitHub issues yourself.
