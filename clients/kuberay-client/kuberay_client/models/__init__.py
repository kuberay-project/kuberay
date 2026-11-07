# Re-export only the CRD root classes for internal validation use.
# These are the only stable names from the generated models.
from kuberay_client.models._generated import (  # noqa: F401
    RayCluster,
    RayCronJob,
    RayJob,
    RayService,
)
