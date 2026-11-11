"""
Set of helper methods to manage rayclusters. Requires Python 3.9 and higher
"""

import copy
import logging
import math
from typing import Any
from abc import ABCMeta, abstractmethod
from kuberay_client.utils import kuberay_cluster_utils
from kuberay_client import constants


log = logging.getLogger(__name__)
if logging.getLevelName(log.level) == "NOTSET":
    logging.basicConfig(format="%(asctime)s %(message)s", level=constants.LOGLEVEL)


class IClusterBuilder(metaclass=ABCMeta):
    """
    IClusterBuilder is an interface for building a cluster.

    The class defines abstract methods for building the metadata, head pod, worker groups, and retrieving the built cluster.
    """

    @staticmethod
    @abstractmethod
    def build_meta():
        "builds the cluster metadata"

    @staticmethod
    @abstractmethod
    def build_head():
        "builds the head pod"

    @staticmethod
    @abstractmethod
    def build_worker():
        "builds a worker group"

    @staticmethod
    @abstractmethod
    def get_cluster():
        "Returns the built cluster"


# Concrete implementation of the builder interface
class ClusterBuilder(IClusterBuilder):
    """
    ClusterBuilder implements the abstract methods of IClusterBuilder to build a cluster.
    """

    def __init__(self):
        self.cluster: dict[str, Any] = {}
        self.succeeded: bool = False
        self.cluster_utils = kuberay_cluster_utils.ClusterUtils()

    def build_meta(
        self,
        name: str,
        k8s_namespace: str = "default",
        labels: dict = None,
        ray_version: str = "2.46.0",
    ):
        """Builds the metadata and ray version of the cluster.

        Parameters:
        - name (str): The name of the cluster.
        - k8s_namespace (str, optional): The namespace in which the Ray cluster exists. Defaults to "default".
        - labels (dict, optional): A dictionary of key-value pairs to add as labels to the cluster. Defaults to None.
        - ray_version (str, optional): The version of Ray to use for the cluster. Defaults to "2.46.0".
        """
        self.cluster = self.cluster_utils.populate_meta(
            cluster=self.cluster,
            name=name,
            k8s_namespace=k8s_namespace,
            labels=labels,
            ray_version=ray_version,
        )
        return self

    def build_head(
        self,
        ray_image: str = "rayproject/ray:2.46.0",
        service_type: str = "ClusterIP",
        cpu_requests: str = "2",
        memory_requests: str = "3G",
        cpu_limits: str = "2",
        memory_limits: str = "3G",
        ray_start_params: dict = {
            "dashboard-host": "0.0.0.0",
        },
    ):
        """Build head node of the ray cluster.

        Parameters:
        - ray_image (str): Docker image for the head node. Default value is "rayproject/ray:2.46.0".
        - service_type (str): Service type of the head node. Default value is "ClusterIP", which creates a headless ClusterIP service.
        - cpu_requests (str): CPU requests for the head node. Default value is "2".
        - memory_requests (str): Memory requests for the head node. Default value is "3G".
        - cpu_limits (str): CPU limits for the head node. Default value is "2".
        - memory_limits (str): Memory limits for the head node. Default value is "3G".
        - ray_start_params (dict): Dictionary of start parameters for the head node.
        Default values is "dashboard-host": "0.0.0.0".
        """
        self.cluster, self.succeeded = self.cluster_utils.populate_ray_head(
            self.cluster,
            ray_image=ray_image,
            service_type=service_type,
            cpu_requests=cpu_requests,
            memory_requests=memory_requests,
            cpu_limits=cpu_limits,
            memory_limits=memory_limits,
            ray_start_params=ray_start_params,
        )
        return self

    def build_worker(
        self,
        group_name: str,
        ray_image: str = "rayproject/ray:2.46.0",
        ray_command: Any = ["/bin/bash", "-lc"],
        init_image: str = "busybox:1.28",
        cpu_requests: str = "1",
        memory_requests: str = "1G",
        cpu_limits: str = "2",
        memory_limits: str = "2G",
        replicas: int = 1,
        min_replicas: int = -1,
        max_replicas: int = -1,
        ray_start_params: dict = {},
    ):
        """Build worker specifications of the cluster.

        This function sets the worker configuration of the cluster, including the Docker image, CPU and memory requirements, number of replicas, and other parameters.

        Parameters:
        - group_name (str): name of the worker group.
        - ray_image (str, optional): Docker image for the Ray process. Default is "rayproject/ray:2.46.0".
        - ray_command (Any, optional): Command to run in the Docker container. Default is ["/bin/bash", "-lc"].
        - init_image (str, optional): Docker image for the init container. Default is "busybox:1.28".
        - cpu_requests (str, optional): CPU requests for the worker pods. Default is "1".
        - memory_requests (str, optional): Memory requests for the worker pods. Default is "1G".
        - cpu_limits (str, optional): CPU limits for the worker pods. Default is "2".
        - memory_limits (str, optional): Memory limits for the worker pods. Default is "2G".
        - replicas (int, optional): Number of worker pods to run. Default is 1.
        - min_replicas (int, optional): Minimum number of worker pods to run. Default is -1.
        - max_replicas (int, optional): Maximum number of worker pods to run. Default is -1.
        - ray_start_params (dict, optional): Additional parameters to pass to the ray start command. Default is {}.
        """
        if min_replicas < 0:
            min_replicas = int(math.ceil(replicas / 2))
        if max_replicas < 0:
            max_replicas = int(replicas * 3)

        if "spec" in self.cluster.keys():
            if "workerGroupSpecs" not in self.cluster.keys():
                log.info(
                    "setting the workerGroupSpecs for group_name {}".format(group_name)
                )
                self.cluster["spec"]["workerGroupSpecs"] = []
        else:
            log.error(
                "error creating custom resource: {meta}, the spec section is missing, did you run build_head()?".format(
                    self.cluster["metadata"]
                )
            )
            self.succeeded = False
            return self

        worker_group, self.succeeded = self.cluster_utils.populate_worker_group(
            group_name,
            ray_image,
            ray_command,
            init_image,
            cpu_requests,
            memory_requests,
            cpu_limits,
            memory_limits,
            replicas,
            min_replicas,
            max_replicas,
            ray_start_params,
        )

        if self.succeeded:
            self.cluster["spec"]["workerGroupSpecs"].append(worker_group)
        return self

    def get_cluster(self):
        cluster = copy.deepcopy(self.cluster)
        return cluster


class Director:
    def __init__(self):
        self.cluster_builder = ClusterBuilder()

    def build_basic_cluster(self, name: str, k8s_namespace: str = "default", labels: dict = None) -> dict:
        """Builds a basic cluster with the given name and k8s_namespace parameters.

        Parameters:
        - name (str): The name of the cluster.
        - k8s_namespace (str, optional): The kubernetes namespace for the cluster, with a default value of "default".

        Returns:
        dict: The basic cluster as a dictionary.
        """
        cluster: dict = (
            self.cluster_builder.build_meta(name=name, k8s_namespace=k8s_namespace, labels=labels)
            .build_head()
            .get_cluster()
        )

        if self.cluster_builder.succeeded:
            return cluster
        return None

    def build_small_cluster(self, name: str, k8s_namespace: str = "default", labels: dict = None) -> dict:
        """Builds a small cluster with the given name and k8s_namespace parameters with 1 workergroup,
        the workgroup has 1 replica with 2 cpu and 2G memory limits

        Parameters:
        - name (str): The name of the cluster.
        - k8s_namespace (str, optional): The kubernetes namespace for the cluster, with a default value of "default".

        Returns:
        dict: The small cluster as a dictionary.
        """
        cluster: dict = (
            self.cluster_builder.build_meta(name=name, k8s_namespace=k8s_namespace, labels=labels)
            .build_head()
            .build_worker(
                group_name="{}-workers".format(name),
                replicas=1,
                min_replicas=0,
                max_replicas=2,
                cpu_requests="1",
                memory_requests="1G",
                cpu_limits="2",
                memory_limits="2G",
            )
            .get_cluster()
        )

        if self.cluster_builder.succeeded:
            return cluster
        return None

    def build_medium_cluster(self, name: str, k8s_namespace: str = "default", labels: str = None) -> dict:
        """Builds a medium cluster with the given name and k8s_namespace parameters with 1 workergroup,
        the workgroup has 3 replicas with 4 cpu and 4G memory limits

            Parameters:
            - name (str): The name of the cluster.
            - k8s_namespace (str, optional): The kubernetes namespace for the cluster, with a default value of "default".

            Returns:
            dict: The small cluster as a dictionary.
        """
        cluster: dict = (
            self.cluster_builder.build_meta(name=name, k8s_namespace=k8s_namespace, labels=labels)
            .build_head()
            .build_worker(
                group_name="{}-workers".format(name),
                replicas=3,
                min_replicas=0,
                max_replicas=6,
                cpu_requests="2",
                memory_requests="2G",
                cpu_limits="4",
                memory_limits="4G",
            )
            .get_cluster()
        )

        if self.cluster_builder.succeeded:
            return cluster
        return None

    def build_large_cluster(self, name: str, k8s_namespace: str = "default", labels: dict = None) -> dict:
        """Builds a medium cluster with the given name and k8s_namespace parameters. with 1 workergroup,
        the workgroup has 6 replicas with 6 cpu and 6G memory limits

            Parameters:
            - name (str): The name of the cluster.
            - k8s_namespace (str, optional): The kubernetes namespace for the cluster, with a default value of "default".

            Returns:
            dict: The small cluster as a dictionary.
        """
        cluster: dict = (
            self.cluster_builder.build_meta(name=name, k8s_namespace=k8s_namespace, labels=labels)
            .build_head()
            .build_worker(
                group_name="{}-workers".format(name),
                replicas=6,
                min_replicas=0,
                max_replicas=12,
                cpu_requests="3",
                memory_requests="4G",
                cpu_limits="6",
                memory_limits="8G",
            )
            .get_cluster()
        )

        if self.cluster_builder.succeeded:
            return cluster
        return None
