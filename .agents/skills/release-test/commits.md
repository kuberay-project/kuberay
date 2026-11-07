# Read a release's commits for the docs

You get `OLD`, `TAG` and `RAY` (see the release-test skill). You run in the KubeRay repository. Read
the code and the pages in `RAY/doc/source/kuberay/`, then reply with `commits.md`. The main agent
saves it.

1. List the commits: `git log --no-merges --format='%h %s' vOLD..TAG`. From here on, work per PR, not
   per commit: a patch release puts several cherry-picked PRs into one commit, and the commit message
   lists them.
2. For each PR, decide if a docs reader can notice it. Read its diff (`git show --stat`, then the
   important parts) when it changes:
   - `ray-operator/apis/`: CRD fields added, removed or renamed, defaults, validation
   - `helm-chart/`: values renamed or removed, new defaults, what a template creates
   - `ray-operator/pkg/features/`: feature gates added, promoted or removed
   - `ray-operator/main.go`, `ray-operator/controllers/ray/utils/constant.go`: operator flags and
     environment variables
   - `kubectl-plugin/`: commands and flags
   - `ray-operator/config/samples/`: samples the docs use that changed, moved or were removed
   - `ray-operator/controllers/`: behavior a page describes, like states, conditions, events, and what
     the operator creates or deletes
   - `historyserver/`, `apiserver/`, `dashboard/`: only if `git -C RAY grep` finds a page about it

   Readers cannot notice tests, CI, refactoring, the release's own version bump, or this repository's
   own docs.
3. For each change readers can notice, find the pages about it with
   `git -C RAY grep -n -I -i '<field, value, flag or sample>' -- 'doc/source/kuberay/*.md'`, and give
   it one status:
   - `needs update`: the change makes a page wrong, for example a renamed Helm value, a removed field,
     a new default, a feature gate that is now on by default, or a moved sample. Write the proposed
     change: the current page text and the new text. Add a version note when readers on an older
     KubeRay still need the old way.
   - `already right`: the docs follow KubeRay master, so some changes are already in the docs.
   - `needs docs`: a new feature or field that no page covers.

   Skip `references/api.md`: a ray bot updates it from the release tag.
4. Every sample the docs pin to a KubeRay version must exist at the tag. For each link or ref that
   pins a sample path to a version (`kuberay/vX/`, `blob/vX/`, `tree/vX/`, `refs/tags/vX/`, `ref=vX`),
   run `git cat-file -e TAG:<path>`. If the file is missing, add a `needs update` row. If the sample
   moved, get the new path from `git diff -M --name-status vOLD TAG -- ray-operator/config/samples`.
5. Reply with `commits.md` only, in this format:

   ```markdown
   # Commits v1.7.0..v1.8.0-rc.0

   81 PRs. Readers can notice 6: 3 need updates, 2 need docs, 1 already right.

   | PR | Change for readers | Pages | Status | Proposed change |
   |---|---|---|---|---|
   | abc1234 [Helm] Rename the ServiceMonitor selector value (#5101) | `metrics.serviceMonitor.selector` is now `additionalLabels` | ecosystem/prometheus-grafana.md:64 | needs update | `--set metrics.serviceMonitor.selector.release=prometheus` → `--set metrics.serviceMonitor.additionalLabels.release=prometheus`, with a note for v1.6.x and earlier |
   | def5678 [Feature] Suspend RayCronJob (#5202) | new field `spec.suspend` on RayCronJob | | needs docs | |
   | 0a1b2c3 [Sample] Pin the auth samples' Ray image (#5154) | the sample images change; no page shows them | user-guides/auth.md | already right | |
   ```

This is done when every PR in the range is either a row or one of the PRs readers cannot notice.
