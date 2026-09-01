# KubeRay Python Models

Auto-generated Pydantic models from KubeRay CRD OpenAPI schemas.

## Files

| File | Description |
|------|-------------|
| `generated_models.py` | Auto-generated Pydantic models from the RayJob CRD schema (includes the embedded `RayClusterSpec`) |

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

`datamodel-code-generator` is pinned to an exact version in that file. The generator
names inlined schemas positionally (`Container1`, `Spec2`, `Resources6`, ...), so a
different version produces different class names and breaks the imports in `utils/`.
The script refuses to run if the installed version does not match the pin.

### Generate from CRD

```bash
# From repo root
python clients/python-client/scripts/generate_models.py
```

The script will:

- Extract the OpenAPI schema from the CRD YAML
- Generate Pydantic v2 models with proper deduplication
- Add a header with the source CRD path and the generator version (no timestamp, to avoid CI churn)

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
