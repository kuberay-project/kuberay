---
name: release-notes
description: Draft GitHub release notes for a KubeRay release (highlights, user-facing changes, breaking changes). Use when asked to write or draft release notes for a tag.
argument-hint: [prev_tag] [new_tag]
---

# Release notes

## Overview

Drafts the GitHub release notes for a KubeRay tag: Highlights, Other Notable Features, Notable Bug Fixes, Breaking
Changes, and Deprecations. The draft is built from the `release-note` and `breaking-change` blocks of every PR merged
between two tags, and written to `release-notes-<new_tag>.md` in the repo root.

Arguments: `$ARGUMENTS` (`<prev_tag> <new_tag>`, e.g. `v1.7.0 v1.8.0`). If empty, use the two newest tags from `git tag
--sort=-v:refname`.

Release type:

- `new_tag` ends in `.0` (e.g. `v1.8.0`): minor release, use `templates/minor.md`.
- otherwise (e.g. `v1.8.1`): patch release, use `templates/patch.md`.
- RC tag (e.g. `v1.8.0-rc.0`): do not draft. KubeRay publishes RCs as pre-releases with only the generated changelog,
  no Highlights or Breaking Changes. Tell the user to run this skill on the final tag instead.

## When to use

Use this skill when asked to write, draft, or update the release notes for a KubeRay release tag. It is not for
writing the `release-note` block of a single PR.

## Process

### 1. Collect

```bash
git fetch --tags
python3 .agents/skills/release-notes/scripts/collect.py <prev_tag> <new_tag> > "$SCRATCHPAD/prs.json"
git diff <prev_tag>..<new_tag> -- ray-operator/pkg/features/features.go
git diff <prev_tag>..<new_tag> --stat -- ray-operator/apis/ray/v1/ helm-chart/kuberay-operator/values.yaml ray-operator/config/samples/
```

`prs.json` is one object per PR: `number, title, labels, release-note, breaking-change, source`.

`source` tells where `release-note` came from:

- `release-note-block`: the PR has the `release-note` / `breaking-change` fenced blocks. Use as-is, light edits
  only. An empty block means the author marked it not applicable.
- `why-section`: the PR has no `release-note` block (it predates the template); `release-note` holds the first
  paragraph of "Why are these changes needed?".
  Written for reviewers, not users. Rewrite, never paste.

### 2. Filter

Drop a PR when `release-note` and `breaking-change` are both empty and the title starts with `[CI]`, `[Test]`,
`[Chore]`, `[Refactor]`, `[Docs]`, `[Flaky]`, or it bumps a dependency.

### 3. Classify (minor only)

Group PRs by title prefix (`[History Server]`, `[RayService]`, ...) and by feature gate name from the `features.go`
diff.

Primary source is the `release-note` and `breaking-change` blocks. The diffs from step 1 and the labels are a safety net
for PRs whose blocks are empty: a PR that adds an API field or Helm value with an empty block still needs a look.

**Include** a PR only if a user would change a manifest, Helm value, flag, or expectation because of it:

- new or changed API field, Helm value, CLI flag, or feature gate
- changed default behavior
- removed or renamed anything (→ Breaking Changes)
- deprecation notice (→ Deprecations)

**Exclude**, even if the PR touches user-visible code:

- bug fixes, unless users likely hit the bug: it closes a user-reported issue, the PR mentions a workaround, or the
  fix changes a default. Those go in Notable Bug Fixes as `Fixed a bug where ...`
- internal perf, refactor, metrics, informer, event API migration, chart metadata
- samples, docs, CI, tests, dependency bumps
- alpha-stage churn of a feature that has a Highlight: fold it into the Highlight paragraph

**Highlight**: a group with a feature gate added or promoted, a new CRD/API object, or ≥3 included PRs.

- One Highlight per feature and stage. If a group contains both a feature graduating (e.g. History Server to beta) and
  a new gate for it (e.g. `RayClusterHistoryServer` alpha), write two sections.
- Title is the user-facing feature name plus its stage from `features.go`, e.g. `Ray History Server (beta)`,
  `Automatic History Server Sidecar Configuration (alpha)`. The title must describe what the paragraph is mostly
  about.

**Other Notable Features**: the rest of the included PRs, one line each under a `####` component heading. Use only these
names, in this order, and only the ones that have entries: RayCluster, RayJob, RayService, RayCronJob, Operator, Helm,
kubectl plugin, History Server, APIServer, Dashboard. PRs already covered by a Highlight are not repeated here.

**Notable Bug Fixes**: the fixes that passed the test above, flat list, component in bold. Usually 3 to 5 lines.

**Breaking Changes**: `breaking-change` non-empty, or `breaking-change` label, or a renamed key in `values.yaml`. Always
complete. Merge related items into one bullet per area; an alpha→beta feature with several config changes is one bullet.

Patch: everything is a bug fix unless it is breaking or a backported feature (then add a Highlights section).

### 4. Write

Fill the template. Rules:

- Highlight citations: list at the end of the paragraph the PRs for what the paragraph describes. Follow-up fixes,
  finalizers, and tests in the same group are not cited unless the paragraph mentions them.
- Highlight: one paragraph of what it does and why; `Try it here:` doc link; YAML excerpt from the new sample file
  (≤15 lines); alpha features get the feature-gate warning block verbatim.
- Other Notable Features and Notable Bug Fixes lines are the PR's `release-note` text, lightly edited for tense and
  consistency. Breaking Changes lines are the `breaking-change` text. Rewrite from scratch only when `source` is
  `why-section`.
- Bullet lines end with `(#NNNN)`. Breaking changes state what changed and what the user must do.
- Aim for the length of v1.7.0: roughly 10 Other Notable Features lines and under 10 Breaking Changes. If the draft is
  much longer, the include test was too loose.
- Patch bug fixes use `Fixed a bug where ...`.
- Leave the Changelog section as the placeholder comment; it is generated by the "Generate release notes" button in
  the GitHub release UI.

Write to `release-notes-<new_tag>.md` in the repo root.

## Validate

Check the draft before replying:

- Every bullet ends with `(#NNNN)`, and every cited PR is in `prs.json`.
- Every Highlight has a `Try it here:` doc link and, for alpha features, the feature-gate warning block.
- Every Breaking Changes bullet says what changed and what the user must do. Do not contain redundant
information. Keep it short.
- API fields, Helm values, flags, and feature gates are spelled exactly as in the diffs from step 1.
- No PR with `source: why-section` was pasted as-is.
- No PR appears in both a Highlight and Other Notable Features.

End the reply with a **Needs review** list: PRs with `source: why-section` that were kept, and PRs whose group was a
guess.
