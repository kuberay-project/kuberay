import enum
from typing import Any

from python_apiserver_client.params import (
    BaseVolume,
    EnvironmentVariables,
    HeadNodeSpec,
    WorkerNodeSpec,
    environment_variables_decoder,
    head_node_spec_decoder,
    volume_decoder,
    worker_node_spec_decoder,
)


class Environment(enum.Enum):
    """
    Environment definitions
    """

    DEV = 0  # development
    TESTING = 1  # testing
    STAGING = 2  # staging
    PRODUCTION = 3  # production


class UpscalingMode(enum.Enum):
    """
    Enumeration of autoscaling mode
    """

    Conservative = (
        "Conservative"  # Rate-limited; the number of pending worker pods is at most the size of the Ray cluster
    )
    Default = "Default"  # no rate limitations
    Aggressive = "Aggressive"  # same as default


class AutoscalerOptions:
    """
    AutoscalerOptions is used to define Ray cluster autoscaling.
    It provides APIs to create, stringify and convert to dict.

    Methods:
    - Create autoscaling options specification: gets the following parameters:
        idle_timeout - optional, number of seconds to wait before scaling down a worker pod which is not using Ray
                       resources. Default 60sec (one minute).
        upscaling_mode - required autoscaler upscaling mode
        image - optional, allows to override the autoscaler's container image
        image_pull_policy - optional, allows to override the autoscaler's container image pull policy
        cpus - optional, CPUs requirements for autoscaler - default "500m"
        memory - optional, memory requirements for autoscaler - default "512Mi"
        environment - optional, environment variables for autoscaler container
        volumes - optional, a list of volumes to attach to autoscaler container.
                  This is needed for enabling TLS for the autoscaler container.
    """

    def __init__(
            self,
            upscaling_mode: UpscalingMode = UpscalingMode.Default,
            idle_tmout: int = None,
            image: str = None,
            image_pull_policy: str = None,
            cpus: str = None,
            memory: str = None,
            environment: EnvironmentVariables = None,
            volumes: list[BaseVolume] = None,
    ):
        """
        Initialization
        :param upscaling_mode: upscale mode
        :param idle_tmout: idle timeout
        :param image: image
        :param image_pull_policy: image pull policy
        :param cpus: cpu requirement for autoscaling
        :param memory: memory requirement for autoscaling
        :param environment: autoscaler environment
        :param volumes:  volumes for autoscaler
        """
        self.upscaling_mode = upscaling_mode
        self.idle_tmout = idle_tmout
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.cpus = cpus
        self.memory = memory
        self.environment = environment
        self.volumes = volumes

    def to_string(self) -> str:
        """
        Convert to string
        :return: string representation of the head node
        """
        val = f"upscaling_mode = {self.upscaling_mode}"
        if self.idle_tmout is not None:
            val += f", idle_timeout = {self.idle_tmout}"
        if self.image is not None:
            val += f", image = {self.image}"
        if self.image_pull_policy is not None:
            val += f", image_pull_policy = {self.image_pull_policy}"
        if self.cpus is not None:
            val += f", cpus = {self.cpus}"
        if self.memory is not None:
            val += f", memory = {self.memory}"
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
        return val

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary
        :return: dictionary representation of the head node
        """
        dct = {"upscalingMode": self.upscaling_mode.value}
        if self.idle_tmout is not None:
            dct["idleTimeoutSeconds"] = self.idle_tmout
        if self.image is not None:
            dct["image"] = self.image
        if self.image_pull_policy is not None:
            dct["imagePullPolicy"] = self.image_pull_policy
        if self.cpus is not None:
            dct["cpu"] = self.cpus
        if self.memory is not None:
            dct["memory"] = self.memory
        if self.volumes is not None:
            dct["volumes"] = [v.to_dict() for v in self.volumes]
        if self.environment is not None:
            dct["envs"] = self.environment.to_dict()
        return dct


class ClusterSpec:
    """
    ClusterSpec is used to define Ray cluster.
    It provides APIs to create, stringify, convert to dict and json.

    Methods:
    - Create cluster spec from: gets the following parameters:
        head_group_spec - required, specification of the head node
        worker_group_spec - optional, list of worker group specs
        autoscaler_options - optional, autoscaling options
    - to_string() -> str: convert toleration to string for printing
    - to_dict() -> dict[str, Any] convert to dict
    """

    def __init__(
            self,
            head_node: HeadNodeSpec,
            worker_groups: list[WorkerNodeSpec] = None,
            autoscaling_options: AutoscalerOptions = None,
    ):
        """
        Initialization
        :param head_node - head node definition
        :param worker_groups - worker group definition
        :param autoscaling_options - autoscaler options
        """
        self.head_node = head_node
        self.worker_groups = worker_groups
        self.autoscaling_options = autoscaling_options

    def to_string(self) -> str:
        """
        Convert to string
        :return: string representation of cluster spec
        """
        val = f"head_group_spec: {self.head_node.to_string()}"
        if self.worker_groups is not None:
            val += "\nworker groups: "
            for w in self.worker_groups:
                val += f"\nworker_group_spec = {w.to_string()}]"
        if self.autoscaling_options is not None:
            val += f"\nautoscaling options = {self.autoscaling_options.to_string()}"
        return val

    def to_dict(self) -> dict[str, Any]:
        """
        Convert to dictionary
        :return: Dictionary representation of cluster spec
        """
        dst = {"headGroupSpec": self.head_node.to_dict()}
        if self.worker_groups is not None:
            dst["workerGroupSpec"] = [w.to_dict() for w in self.worker_groups]
        if self.autoscaling_options is not None:
            dst["enableInTreeAutoscaling"] = True
            dst["autoscalerOptions"] = self.autoscaling_options.to_dict()
        return dst


class ClusterEvent:
    """
    Cluster event is used to define events emitted during cluster creation.
    It provides APIs to create and stringify. Its output only data, so we do not need to implement to_dict

    Methods:
    - Create event: gets the dictionary with the following parameters:
        id - unique Event Id
        name - human readable event name
        created_at - event creation time
        first_timestamp - first time the event occur
        last_timestamp - last time the event occur
        reason - reason for the transition into the object's current status
        message - human-readable description of the status of this operation
        type - type of this event (Normal, Warning), new types could be added in the future
        count - number of times this event has occurred
    """

    def __init__(self, dst: dict[str, Any]):
        """
        Initialization from dictionary
        :param dst: dictionary representation of cluster event
        """
        self.id = dst.get("id", "")
        self.name = dst.get("name", "")
        self.created_at = dst.get("created_at", "")
        self.first_timestamp = dst.get("first_timestamp", "")
        self.last_timestamp = dst.get("last_timestamp", "")
        self.reason = dst.get("reason", "")
        self.message = dst.get("message", "")
        self.type = dst.get("type", "")
        self.count = dst.get("count", "0")

    def to_string(self) -> str:
        """
        Convert to string
        :return: string representation of cluster event
        """
        return (
            f"id = {self.id}, name = {self.name}, created_at = {self.created_at}, "
            f"first_timestamp = {self.first_timestamp}, last_timestamp = {self.last_timestamp},"
            f"reason = {self.reason}, message = {self.message}, type = {self.type}, count = {self.count}"
        )


class Cluster:
    """
    Cluster is used to define Ray cluster.
    It provides APIs to create, stringify, convert to dict and json.

    Methods:
    - Create env variable from: gets the following parameters:
        name - required, unique (per namespace) cluster name
        namespace - required, cluster's namespace (should exist)
        user - required, user who owns the cluster
        version - required, Ray cluster version - typically Ray version
        deployment_environment - optional (see Environment)
        cluster_spec - required, ray cluster configuration
        annotations - optional, annotations, for example, "kubernetes.io/ingress.class" to define Ingress class
        cluster_environment - optional, cluster environment variables
        created_at - output, cluster creation ts
        deleted_at - output, cluster deletion ts
        cluster_status - output, cluster status
        events - output, cluster events
        service_endpoint - output, cluster service endpoints
    - to_string() -> str: convert toleration to string for printing
    - to_dict() -> dict[str, Any] convert to dict
    """

    def __init__(
            self,
            name: str,
            namespace: str,
            user: str,
            version: str,
            cluster_spec: ClusterSpec,
            deployment_environment: Environment = None,
            annotations: dict[str, str] = None,
            cluster_environment: EnvironmentVariables = None,
            created_at: str = None,
            deleted_at: str = None,
            cluster_status: str = None,
            events: list[ClusterEvent] = None,
            service_endpoint: dict[str, str] = None,
    ):
        """
        Initialization
        :param name: cluster name
        :param namespace: cluster namespace
        :param user: user name
        :param version: version
        :param cluster_spec: cluster spec
        :param deployment_environment: cluster deployment environment
        :param annotations: cluster annotations
        :param cluster_environment: cluster environment
        :param created_at: created at
        :param deleted_at: deleted at
        :param cluster_status: status
        :param events: cluster events
        :param service_endpoint: service endpoint
        """
        self.name = name
        self.namespace = namespace
        self.user = user
        self.version = version
        self.cluster_spec = cluster_spec
        self.environment = deployment_environment
        self.annotations = annotations
        self.envs = cluster_environment
        self.created_at = created_at
        self.deleted_at = deleted_at
        self.cluster_status = cluster_status
        self.events = events
        self.service_endpoint = service_endpoint

    def to_string(self) -> str:
        """
        convert to string representation
        :return: string representation of cluster
        """
        val = (
            f"name: {self.name}, namespace = {self.namespace}, user = {self.user}, version = {self.version} "
            f"cluster_spec = {self.cluster_spec.to_string()}"
        )
        if self.environment is not None:
            val += f"deployment environment = {self.environment.name}"
        if self.annotations is not None:
            val += f" ,annotations = {str(self.annotations)}"
        if self.envs is not None:
            val = val + f",cluster environment = {self.envs.to_string()}"
        val += "\ncluster output\n"
        if self.created_at is not None:
            val += f" ,created_at = {self.created_at}"
        if self.deleted_at is not None:
            val += f" ,deleted_at = {self.deleted_at}"
        if self.cluster_status is not None:
            val += f" ,cluster status = {self.cluster_status}"
        if self.events is not None:
            val = val + ",\n cluster events = ["
            first = True
            for e in self.events:
                if first:
                    first = False
                else:
                    val += ", "
                val = val + "{" + e.to_string() + "}"
            val = val + "]"
        if self.service_endpoint is not None:
            val += f" ,service endpoints = {str(self.service_endpoint)}"
        return val

    def to_dict(self) -> dict[str, Any]:
        """
        convert to dictionary
        :return: dictionary representation of cluster
        """
        # only convert input variables
        dst = {
            "name": self.name,
            "namespace": self.namespace,
            "user": self.user,
            "version": self.version,
            "clusterSpec": self.cluster_spec.to_dict(),
        }
        if self.environment is not None:
            dst["environment"] = self.environment.value
        if self.annotations is not None:
            dst["annotations"] = self.annotations
        if self.envs is not None:
            dst["envs"] = self.envs.to_dict()
        return dst


"""
    Creates new cluster from dictionary, used for unmarshalling json. Python does not
    support multiple constructors, so do it this way
"""


def autoscaling_decoder(dct: dict[str, Any]) -> AutoscalerOptions:
    """
    Create autoscaling options from its dictionary representation
    :param dct: dictionary representation of cluster spec
    :return: autoscaling options
    """
    upscaling_mode = UpscalingMode.Default
    if "upscalingMode" in dct:
        upscaling_mode = UpscalingMode(dct.get("upscalingMode"))
    volumes = None
    if "volumes" in dct:
        volumes = [volume_decoder(v) for v in dct["volumes"]]
    environments = None
    if "environment" in dct and len(dct.get("envs")) > 0:
        environments = environment_variables_decoder(dct.get("envs"))
    return AutoscalerOptions(
        upscaling_mode=upscaling_mode,
        idle_tmout=dct.get("idleTimeoutSeconds", None),
        image=dct.get("image", None),
        image_pull_policy=dct.get("imagePullPolicy", None),
        cpus=dct.get("cpu", None),
        memory=dct.get("memory", None),
        environment=environments,
        volumes=volumes,
    )


def cluster_spec_decoder(dct: dict[str, Any]) -> ClusterSpec:
    """
    Create cluster spec from its dictionary representation
    :param dct: dictionary representation of cluster spec
    :return: cluster spec
    """
    workers = None
    autoscaling_options = None
    if "workerGroupSpec" in dct:
        workers = [worker_node_spec_decoder(w) for w in dct["workerGroupSpec"]]
    if "enableInTreeAutoscaling" in dct and dct.get("enableInTreeAutoscaling"):
        autoscaling_options = autoscaling_decoder(dct.get("autoscalerOptions", {}))
    return ClusterSpec(
        head_node=head_node_spec_decoder(dct.get("headGroupSpec")),
        worker_groups=workers,
        autoscaling_options=autoscaling_options,
    )


def cluster_decoder(dct: dict[str, Any]) -> Cluster:
    """
    Create cluster from its dictionary representation
    :param dct: dictionary representation of cluster
    :return: cluster
    """
    environment = None
    if "environment" in dct:
        environment = Environment(int(dct.get("environment", "0")))
    events = None
    if "events" in dct:
        events = [ClusterEvent(c) for c in dct["events"]]
    envs = None
    if "envs" in dct:
        envs = environment_variables_decoder(dct.get("envs"))
    return Cluster(
        name=dct.get("name", ""),
        namespace=dct.get("namespace", ""),
        user=dct.get("user", ""),
        version=dct.get("version", ""),
        cluster_spec=cluster_spec_decoder(dct.get("clusterSpec")),
        deployment_environment=environment,
        annotations=dct.get("annotations"),
        cluster_environment=envs,
        created_at=dct.get("createdAt"),
        deleted_at=dct.get("deletedAt"),
        cluster_status=dct.get("clusterState"),
        events=events,
        service_endpoint=dct.get("serviceEndpoint"),
    )


def clusters_decoder(dct: dict[str, any]) -> list[Cluster]:
    """
    Create list of clusters from its dictionary representation
    :param dct: dictionary representation of a list of clusters
    :return: list of clusters
    """
    return [cluster_decoder(cluster) for cluster in dct["clusters"]]
