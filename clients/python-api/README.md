# kuberay-api

Generated Pydantic v2 models for KubeRay CRDs (RayCluster, RayJob, RayService, RayCronJob).

## Install

```bash
pip install kuberay-api
```

## Usage

```python
from kuberay_api.models.raycluster import RayCluster, RayClusterSpec, HeadGroupSpec

cluster = RayCluster(
    apiVersion="ray.io/v1",
    kind="RayCluster",
    spec=RayClusterSpec(
        headGroupSpec=HeadGroupSpec(
            rayStartParams={"dashboard-host": "0.0.0.0"},
        ),
    ),
)
```

## Regenerate

```bash
pip install -r scripts/requirements-python-models.txt
python scripts/generate_python_models.py
```
