"""Generated Pydantic models, one module per KubeRay custom resource.

The modules in this package are generated from the v1 CRD schemas by
clients/python-client/scripts/generate_models.py - do not edit them by hand.

Each module carries its own copy of the shared Kubernetes types (Container,
Template, Resources, ...) because CRD YAML inlines every schema instead of
referencing a common definition. Models are therefore only interchangeable
within a single module: passing a raycluster.Template into a rayjob model raises
a pydantic validation error. Build each custom resource from its own module.

    from python_client.models.generated.raycluster import RayCluster, HeadGroupSpec
    from python_client.models.generated.rayjob import RayJob, RayClusterSpec
"""

from .raycluster import RayCluster
from .raycronjob import RayCronJob
from .rayjob import RayJob
from .rayservice import RayService

__all__ = ["RayCluster", "RayCronJob", "RayJob", "RayService"]
