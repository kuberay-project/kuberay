# KubeRay Python Models

Auto-generated Pydantic models from KubeRay CRD OpenAPI schemas.

## Files

| File | Description |
|------|-------------|
| `generated_rayjob_models.py` | Auto-generated Pydantic models from CRD schema |

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
pip install pyyaml datamodel-code-generator
```

### Generate from CRD

```bash
# From repo root
python clients/python-client/scripts/generate_models.py
```

The script will:
- Extract the OpenAPI schema from the CRD YAML
- Generate Pydantic v2 models with proper deduplication
- Add a header with source CRD path and timestamp

### Update Builders (if needed)

If the CRD schema changes significantly, update the builder files in `utils/`:
- `kuberay_job_builder.py` - RayJob builder
- `kuberay_cluster_builder.py` - RayCluster builder
