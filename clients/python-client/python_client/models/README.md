# KubeRay Python Models

Auto-generated Pydantic models from KubeRay CRD OpenAPI schemas.

## Files

| File | Description |
|------|-------------|
| `generated_rayjob_models.py` | Auto-generated Pydantic models from CRD schema |
| `kuberay_models.py` | Clean builder API for creating manifests |

## Usage

```python
from python_client.models import RayJobBuilder, create_ray_job

# Builder pattern
job = RayJobBuilder("my-job") \
    .with_entrypoint("python train.py") \
    .with_cluster_spec(worker_replicas=2) \
    .with_shutdown_after_finish(True) \
    .to_dict()

# Or convenience function
job = create_ray_job("my-job", "python train.py", worker_replicas=2)
```

## Regenerating Models

When CRDs are updated, regenerate the Python models:

### Prerequisites

```bash
pip install pyyaml datamodel-code-generator
```

### Generate from CRD

```bash
# Run from repo root
python3 -c "
import yaml, json, sys
with open('ray-operator/config/crd/bases/ray.io_rayjobs.yaml') as f:
    schema = yaml.safe_load(f)['spec']['versions'][0]['schema']['openAPIV3Schema']
json.dump(schema, sys.stdout)
" | datamodel-codegen \
    --input-file-type jsonschema \
    --output clients/python-client/python_client/models/generated_rayjob_models.py \
    --output-model-type pydantic_v2.BaseModel \
    --use-standard-collections \
    --use-union-operator \
    --field-constraints \
    --class-name RayJob
```

### Update Builder (if needed)

If the CRD schema changes significantly, update `kuberay_models.py` to expose new fields through the builder API.
