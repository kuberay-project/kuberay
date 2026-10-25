# KubeRay Python Models

Auto-generated Pydantic models from KubeRay CRD OpenAPI schemas.

## Files

One module per custom resource, generated from the `v1` schema of the matching CRD:

| File | Kind | Source CRD |
|------|------|------------|
| `generated/raycluster.py` | `RayCluster` | `ray.io_rayclusters.yaml` |
| `generated/rayjob.py` | `RayJob` | `ray.io_rayjobs.yaml` |
| `generated/rayservice.py` | `RayService` | `ray.io_rayservices.yaml` |
| `generated/raycronjob.py` | `RayCronJob` | `ray.io_raycronjobs.yaml` |

`v1alpha1` is not generated - it is deprecated.

### Models are per-resource, not shared

CRD YAML inlines every schema instead of referencing a common definition, so each
module carries its own copy of the shared Kubernetes types (`Container`, `Template`,
`Resources`, ...). They are only interchangeable within one module - passing a
`raycluster.Template` into a `rayjob` model raises a pydantic validation error. Build
each custom resource from its own module:

```python
from python_client.models import RayCluster, RayJob, RayService, RayCronJob   # top-level models
from python_client.models.generated.raycluster import HeadGroupSpec, WorkerGroupSpec
from python_client.models.generated.rayjob import RayClusterSpec              # RayJob's embedded copy
```

Generating all four CRDs into a single module was measured and rejected: it numbers the
anonymous classes across all of them (`Resources57`, `Spec28`), so a RayService schema
change renames the classes the RayJob builders import.

## Usage

For builder APIs, use the utils module:

```python
from python_client.utils.kuberay_job_builder import RayJobBuilder, create_ray_job
from python_client.utils.kuberay_cluster_builder import ClusterBuilder, Director

# RayJob builder pattern
job = RayJobBuilder("my-job") \
    .with_entrypoint("python train.py") \
    .with_cluster_spec(worker_replicas=2) \
    .with_shutdown_after_finish(True) \
    .to_dict()

# RayJob convenience function
job = create_ray_job("my-job", "python train.py", worker_replicas=2)

# RayCluster builder pattern
cluster = ClusterBuilder() \
    .build_meta(name="my-cluster") \
    .build_head() \
    .build_worker(group_name="workers", replicas=2) \
    .get_cluster()
```

## Regenerating Models

When CRDs are updated, regenerate the Python models using the generation script.

### Prerequisites

```bash
pip install -r clients/python-client/scripts/requirements.txt
```

`datamodel-code-generator` is pinned to an exact version in that file. CRD YAML inlines
every schema (no `$ref`s), so the generator mints a class per occurrence and names the
anonymous ones positionally (`Spec2`, `Port2`, ...). How aggressively it collapses
identical occurrences changes between releases, so a different version produces
different class names and breaks the imports in `utils/`. The script refuses to run if
the installed version does not match the pin.

### Generate from CRD

```bash
# From repo root
python clients/python-client/scripts/generate_models.py
```

The script will, for each custom resource in `CUSTOM_RESOURCES`:

- Extract the `v1` OpenAPI schema from the CRD YAML
- Move Kubernetes int-or-string `pattern` constraints onto the string branch, so quantity
  fields accept both `"500m"` and `2` (the generator otherwise emits a regex constraint on
  an integer schema, which pydantic rejects at validation time)
- Generate Pydantic v2 models with deduplication
- Add a header with the source CRD path and the generator version (no timestamp, to avoid CI churn)

To cover another CRD, add an entry to `CUSTOM_RESOURCES` in the script.

### Bumping the generator version

1. Update the pin in `clients/python-client/scripts/requirements.txt`
2. Reinstall and regenerate as above
3. Fix up any renamed classes imported by `utils/kuberay_job_builder.py` and
   `utils/kuberay_cluster_utils.py`, and run `pytest python_client_test/`
4. Commit the pin, the regenerated models, and the import updates together

### Update Builders (if needed)

If the CRD schema changes significantly, update the builder files in `utils/`:

- `kuberay_job_builder.py` - RayJob builder
- `kuberay_cluster_builder.py` - RayCluster builder
