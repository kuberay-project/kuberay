# Read a release's commits for the docs

You get `OLD`, `TAG` and `RAY` (see the release-test skill). You run in the KubeRay repository. Read
the code and the pages in `RAY/doc/source/kuberay/`, then reply with `commits.md`. The main agent
saves it.

1. List the commits: `git log --no-merges --format='%h %s' vOLD..TAG`. From here on, work per PR, not
   per commit: a patch release puts several cherry-picked PRs into one commit, and the commit message
   lists them. `vOLD` is on a release branch, so the range can have PRs that `vOLD` already has as
   cherry-picks. Leave out each PR whose number is in `git log --format=%B TAG..vOLD`.
2. For each PR, decide if a docs reader can notice it. Read its diff (`git show --stat`, then the
   important parts) when it changes:
   - `ray-operator/apis/ray/`: CRD fields added, removed or renamed, defaults, validation
   - `helm-chart/`: values renamed or removed, new defaults, what a template creates
   - `ray-operator/pkg/features/`: feature gates added, promoted or removed
   - `ray-operator/main.go`, `ray-operator/apis/config/`,
     `ray-operator/controllers/ray/utils/constant.go`: operator flags, configuration fields and
     environment variables
   - `ray-operator/pkg/webhooks/`: what the operator's webhooks reject or change
   - `kubectl-plugin/`: commands and flags
   - `ray-operator/config/samples/`: samples the docs use that changed, moved or were removed
   - `ray-operator/config/`, other folders: the CRDs and the kustomize install
   - `ray-operator/controllers/`: behavior a page describes, like states, conditions, events, and what
     the operator creates or deletes
   - `historyserver/`, `apiserver/`, `dashboard/`, `config/`, `clients/`: only if `git -C RAY grep`
     finds a page about it

   Readers cannot notice tests, CI, refactoring, dependency bumps, the release's own version bump,
   this repository's own docs, or a bug fix that leaves every page right.
3. Write one row per change. A PR can make two changes, like a new field and a field that is no
   longer required. A change can take several PRs: list them all in its row. For each change readers
   can notice, find the pages about it with
   `git -C RAY grep -n -I -i '<field, value, flag or sample>' -- 'doc/source/kuberay/*.md'`, and give
   it one status:
   - `needs update`: the change makes a page wrong, for example a renamed Helm value, a removed field,
     a new default, a feature gate that is now on by default, or a moved sample. Write the proposed
     change: the current page text and the new text. Add a version note when readers on an older
     KubeRay still need the old way.
   - `already right`: the docs follow KubeRay master, so some changes are already in the docs.
   - `needs docs`: a new feature or field that no page covers. Name the sample that shows it:
     `git grep -l '<field or value>' TAG -- ray-operator/config/samples`. The release report shows how
     that sample did in the run. Write `none` when the tag has no sample for it: then nothing tests
     the change, and the release report lists it as not tested.

   Skip `references/api.md`: a ray bot updates it from the release tag.
4. Every sample the docs pin to a KubeRay version must exist at the tag. For each link or ref that
   pins a sample path to a version (`kuberay/vX/`, `blob/vX/`, `tree/vX/`, `refs/tags/vX/`, `ref=vX`,
   `targetRevision` with `path`), run `git cat-file -e TAG:<path>`. If the file is missing, add a `needs update` row. If the sample
   moved, get the new path from `git diff -M --name-status vOLD TAG -- ray-operator/config/samples`.
5. Reply with `commits.md` only, in this format. The rows are made up: they show the format, not the
   result of a run.

   ```markdown
   # Commits v1.7.0..v1.8.0-rc.0

   89 PRs. Readers can notice 5: 1 needs an update, 3 need docs, 1 already right.

   | PR | Change for readers | Pages | Status | Sample | Proposed change |
   |---|---|---|---|---|---|
   | f72fa098 Feat/remove restart policy never if autoscaler v2 (#4816) | with autoscaler V2 and `rayVersion` 2.56.0 or newer, KubeRay no longer sets `restartPolicy: Never` | user-guides/configuring-autoscaling.md:261 | needs update | | "KubeRay automatically sets `restartPolicy: Never` on worker pods if Autoscaler V2 is enabled." → "... if Autoscaler V2 is enabled and `rayVersion` is older than 2.56.0.", with a note for KubeRay v1.7 and earlier |
   | d4e4ca16 [ray-operator] Add spec.idleSuspend and spec.idleTerminationOptions for v1 RayCluster CRD (#5210) | new fields `idleSuspend` and `idleTerminationOptions` on RayCluster | | needs docs | `ray-cluster.autoscaler-v2-terminate-idle-cluster.yaml` | |
   | 38449c9c [Feature][History Server] Make the collector image configurable on the operator (#5207) | new Helm value `collectorImage` and operator flag `--collector-image` | | needs docs | `ray-cluster.historyserver.yaml` | |
   | 8586d3a2 feat: add ScaleGate field to ScaleStrategy for fast fallback on quota exceeded (#5094) | new field `scaleGate` in a worker group's `scaleStrategy` | | needs docs | none | |
   | eb636000 Forward Kubernetes Node infrastructure events to Ray custom resources (#4978) | new feature gate `RayNodeEventForwarder` and Helm values `nodeEventForwarder.*` | user-guides/k8s-events.md:229 | already right | | |
   ```

   Only `needs docs` rows fill in Sample.

This is done when every PR in the range is either a row or one of the PRs readers cannot notice.
