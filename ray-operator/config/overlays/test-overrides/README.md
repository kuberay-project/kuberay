# Test Overrides Overlay (CI / e2e ONLY)

This overlay enables test-only / alpha feature gates (currently `RayJobDeletionPolicy`) without modifying:

- The base manifests under `config/default`
- Generated CRDs (`make generate`)
- Helm chart defaults (`make helm`, users' `helm install` without -f override)

Use it only in CI or local end-to-end testing when you explicitly need gated behavior.

---

## Why It Exists

Some feature gates are intentionally disabled by default for stability.
E2E tests must exercise them to validate behavior prior to promotion.
This overlay provides a safe, isolated place to turn them on.

---

## Safety Guarantees

| Concern | Guarantee |
|---------|-----------|
| Default user deploy (`make deploy`) | Unchanged |
| Helm install (no -f override) | Unchanged |
| CRD generation / codegen | Unaffected |
| Feature gates scope | Only those explicitly listed here |

---

## Usage

Deploy with feature gates enabled:

```bash
make deploy-with-override IMG=kuberay/operator:nightly
```

Helm path (CI release automation):

```bash
helm install kuberay-operator kuberay/kuberay-operator -f .buildkite/values-kuberay-operator-override.yaml
```

---

## Adding Another Feature Gate

1. Edit `deployment-override.yaml` – append your gate inside the existing `--feature-gates=` list.
2. Update `.buildkite/values-kuberay-operator-override.yaml` likewise.
3. Add or adjust e2e tests as needed.

Keep gate ordering stable to minimize diff noise.

---

## Keeping In Sync

If the base operator Deployment args change in `config/manager/manager.yaml`:

1. Copy the updated arg list.
2. Re-apply the feature gates in `deployment-override.yaml`.
3. Re-render to confirm.

---

## Removal / Promotion Flow

When a gate graduates (enabled by default upstream):

1. Remove it from the override (if it's default-on, it no longer needs listing).
2. Remove corresponding logic from tests if they branch on gate state.
3. (Optional) Note the graduation in release notes.

---

## Troubleshooting

| Problem | Action |
|---------|--------|
| Patch no longer applies | Check if Deployment name or container name changed. |
| Gates not taking effect | Confirm args rendered (render target) and operator pod restarted. |
| Unexpected arg order | The strategic merge patch replaces the entire args list; adjust ordering there. |

---

## Do NOT

- Add unrelated production configuration (RBAC, CRDs, resources) here.
- Reference this overlay from user-facing docs.
- Rename directory without updating `Makefile` targets.
