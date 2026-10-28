import enum
from typing import Any

from python_apiserver_client.params import (
    BaseVolume,
    EnvironmentVariables,
    environment_variables_decoder,
    volume_decoder,
)

DEFAULT_HEAD_START_PARAMS = {"dashboard-host": "0.0.0.0", "metrics-export-port": "8080", "num-cpus": "0"}


class ServiceType(enum.Enum):
    """
    Enumeration of head node service types
    """

    ClusterIP = "ClusterIP"  # cluster IP
    NodePort = "NodePort"  # node port
    LoadBalancer = "LoadBalancer"  # load balancer


class HeadNodeSpec:
    """
    HeadNodeSpec is used to define Ray cluster head node configuration.
    It provides APIs to create, stringify and convert to dict.

    Methods:
    - Create head node specification: gets the following parameters:
        compute_template - required, the computeTemplate of head node group
        ray_start_params - required, Ray start parameters
        image - optional, image used for head node
        service_type - optional (ServiceType), service type foe headnode
        enable_ingress - optional, allow to enable ingress for dashboard
        volumes - optional, a list of volumes to attach to head node
        service_account - optional, a service account (has to exist) to run head node
        image_pull_secret - optional, secret to pull head node image from registry
        environment - optional, environment variables for head pod
        annotations - optional, annotations for head node
        labels - optional, labels for head node
        image_pull_policy - optional, head node pull image policy. Default IfNotPresent
    """

    def __init__(
            self,
            compute_template: str,
            image: str,
            ray_start_params: dict[str, str] = DEFAULT_HEAD_START_PARAMS,
            service_type: ServiceType = ServiceType.ClusterIP,
            enable_ingress: bool = False,
            volumes: list[BaseVolume] = None,
            service_account: str = None,
            image_pull_secret: str = None,
            environment: EnvironmentVariables = None,
            annotations: dict[str, str] = None,
            labels: dict[str, str] = None,
            image_pull_policy: str = None,
    ):
        """
        Initialization
        :param compute_template: compute template
        :param ray_start_params:  ray start parameters
        :param image: node image
        :param service_type: service type
        :param enable_ingress: enable ingress flag
        :param volumes:  volumes for head node
        :param service_account: service account
        :param image_pull_secret:  image pull secret
        :param environment: head node environment
        :param annotations: head node annotation
        :param labels: labels
        :param image_pull_policy: image pull policy
        """

        self.compute_template = compute_template
        self.ray_start_params = ray_start_params
        self.ray_start_params.update(DEFAULT_HEAD_START_PARAMS)
        self.image = image
        self.service_type = service_type
        self.enable_ingress = enable_ingress
        self.volumes = volumes
        self.service_account = service_account
        self.image_pull_secret = image_pull_secret
        self.environment = environment
        self.annotations = annotations
        self.labels = labels
        self.image_pull_policy = image_pull_policy

    def to_string(self) -> str:
        """
        Convert to string
        :return: string representation of the head node
        """
        val = f"compute template = {self.compute_template}, ray start params = {str(self.ray_start_params)}"
        if self.image is not None:
            val += f", image = {self.image}"
        if self.service_type is not None:
            val += f", service_type = {self.service_type.name}"
        if self.enable_ingress:
            val += ", enable_ingress = True"
        if self.service_account is not None:
            val += f", service_account = {self.service_account}"
        if self.image_pull_secret is not None:
            val += f", image_pull_secret = {self.image_pull_secret}"
        if self.image_pull_policy is not None:
            val += f", image_pull_policy = {self.image_pull_policy}"
        if self.volumes is not None:
            val = val + ",\n volumes = ["
            first = True
            for v in self.volumes:
                if first:
                    first = False
                else:
                    val += ", "
                val = val + "{" + v.to_string() + "}"
            val = val + "]"
        if self.environment is not None:
            val = val + f",\n environment = {self.environment.to_string()}"
        if self.annotations is not None:
            val = val + f",\n annotations = {str(self.annotations)}"
        if self.labels is not None:
            val = val + f",\n labels = {str(self.labels)}"
        return val

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary
        :return: dictionary representation of the head node
        """
        dct = {"computeTemplate": self.compute_template, "rayStartParams": self.ray_start_params}
        if self.image is not None:
            dct["image"] = self.image
        if self.service_type is not None:
            dct["serviceType"] = self.service_type.value
        if self.enable_ingress:
            dct["enableIngress"] = True
        if self.service_account is not None:
            dct["service_account"] = self.service_account
        if self.image_pull_secret is not None:
            dct["image_pull_secret"] = self.image_pull_secret
        if self.image_pull_policy is not None:
            dct["imagePullPolicy"] = self.image_pull_policy
        if self.volumes is not None:
            dct["volumes"] = [v.to_dict() for v in self.volumes]
        if self.environment is not None:
            dct["environment"] = self.environment.to_dict()
        if self.annotations is not None:
            dct["annotations"] = self.annotations
        if self.labels is not None:
            dct["labels"] = self.labels
        return dct


"""
    Creates new head node from dictionary, used for unmarshalling json. Python does not
    support multiple constructors, so do it this way
"""


def head_node_spec_decoder(dct: dict[str, Any]) -> HeadNodeSpec:
    """
    Create head node spec from dictionary
    :param dct: dictionary representation of head node spec
    :return: Head node spec
    """
    service_type = None
    if "serviceType" in dct:
        service_type = ServiceType(dct.get("serviceType", "ClusterIP"))
    volumes = None
    if "volumes" in dct:
        volumes = [volume_decoder(v) for v in dct["volumes"]]
    environments = None
    if "environment" in dct and len(dct.get("environment")) > 0:
        environments = environment_variables_decoder(dct.get("environment"))
    return HeadNodeSpec(
        compute_template=dct.get("computeTemplate"),
        ray_start_params=dct.get("rayStartParams"),
        image=dct.get("image"),
        service_type=service_type,
        enable_ingress=dct.get("enableIngress", False),
        volumes=volumes,
        service_account=dct.get("service_account", None),
        image_pull_secret=dct.get("imagePullSecret", None),
        image_pull_policy=dct.get("imagePullPolicy", None),
        environment=environments,
        annotations=dct.get("annotations", None),
        labels=dct.get("labels", None),
    )
