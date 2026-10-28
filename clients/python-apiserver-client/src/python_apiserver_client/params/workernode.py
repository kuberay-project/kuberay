from typing import Any

from python_apiserver_client.params import (
    BaseVolume,
    EnvironmentVariables,
    environment_variables_decoder,
    volume_decoder,
)


DEFAULT_WORKER_START_PARAMS = {"node-ip-address": "$MY_POD_IP"}


class WorkerNodeSpec:
    """
    WorkerNodeSpec is used to define Ray cluster worker node pool configuration.
    It provides APIs to create, stringify and convert to dict.

    Methods:
    - Create worker node pool specification: gets the following parameters:
        group_name - required, group name of the worker group
        compute_template - required, the computeTemplate of worker node group
        replicas - required, desired replicas of the worker group
        min_replicas - required Min replicas of the worker group, can't be greater than max_replicas
        max_replicas - required, max replicas of the worker group
        ray_start_params - required, Ray start parameters
        image - optional, image used for worker node
        volumes - optional, a list of volumes to attach to worker node
        service_account - optional, a service account (has to exist) to run worker node
        image_pull_secret - optional, secret to pull worker node image from registry
        environment - optional, environment variables for worker pod
        annotations - optional, annotations for worker node
        labels - optional, labels for worker node
        image_pull_policy - optional, worker node pull image policy. Default IfNotPresent
    """

    def __init__(
            self,
            group_name: str,
            compute_template: str,
            image: str,
            max_replicas: int,
            replicas: int = 1,
            min_replicas: int = 0,
            ray_start_params: dict[str, str] = DEFAULT_WORKER_START_PARAMS,
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
        :param group_name: name
        :param compute_template: compute template
        :param replicas: number of replicas
        :param min_replicas: min number of replicas
        :param max_replicas: max number of replicas
        :param ray_start_params: ray start parameters
        :param image: image name
        :param volumes: volumes
        :param service_account: service account
        :param image_pull_secret: image pull secret
        :param environment: environment
        :param annotations: annotations
        :param labels: labels
        :param image_pull_policy: image pull policy
        """
        # Validate replicas
        if min_replicas > replicas:
            raise RuntimeError(f"min_replicas {min_replicas} is can't be greater then replicas {replicas} ")
        if replicas > max_replicas:
            raise RuntimeError(f"replicas {replicas} is can't be greater then max_replicas {max_replicas} ")

        self.group_name = group_name
        self.compute_template = compute_template
        self.replicas = replicas
        self.min_replicas = min_replicas
        self.max_replicas = max_replicas
        self.ray_start_params = ray_start_params
        self.ray_start_params.update(DEFAULT_WORKER_START_PARAMS)
        self.image = image
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
        :return: string representation of worker node spec
        """
        val = (
            f"group_name = {self.group_name},  compute template = {self.compute_template}, "
            f"replicas = {self.replicas}, min_replicas = {self.min_replicas}, "
            f"max_replicas = {self.max_replicas}, ray start params = {str(self.ray_start_params)}"
        )
        if self.image is not None:
            val += f", image = {self.image}"
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
        :return: dictionary representation of worker node spec
        """
        dct = {
            "groupName": self.group_name,
            "computeTemplate": self.compute_template,
            "replicas": self.replicas,
            "minReplicas": self.min_replicas,
            "maxReplicas": self.max_replicas,
            "rayStartParams": self.ray_start_params,
        }
        if self.image is not None:
            dct["image"] = self.image
        if self.service_account is not None:
            dct["service_account"] = self.service_account
        if self.image_pull_secret is not None:
            dct["imagePullSecret"] = self.image_pull_secret
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
    Creates new worker node from dictionary, used for unmarshalling json. Python does not
    support multiple constructors, so do it this way
"""


def worker_node_spec_decoder(dct: dict[str, Any]) -> WorkerNodeSpec:
    """
    Create worker node spec from dictionary
    :param dct: dictionary definition of worker node spec
    :return: worker node spec
    """
    volumes = None
    if "volumes" in dct:
        volumes = [volume_decoder(v) for v in dct["volumes"]]
    environments = None
    if "environment" in dct and len(dct.get("environment")) > 0:
        environments = environment_variables_decoder(dct.get("environment"))
    return WorkerNodeSpec(
        group_name=dct.get("groupName"),
        compute_template=dct.get("computeTemplate"),
        replicas=dct.get("replicas", 0),
        min_replicas=dct.get("minReplicas", 0),
        max_replicas=dct.get("maxReplicas", 0),
        ray_start_params=dct.get("rayStartParams"),
        image=dct.get("image"),
        volumes=volumes,
        service_account=dct.get("service_account", None),
        image_pull_secret=dct.get("imagePullSecret", None),
        image_pull_policy=dct.get("imagePullPolicy", None),
        environment=environments,
        annotations=dct.get("annotations", None),
        labels=dct.get("labels", None),
    )
