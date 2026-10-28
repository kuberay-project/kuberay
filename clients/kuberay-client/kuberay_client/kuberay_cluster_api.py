"""
Set of APIs to manage rayclusters.
"""

__copyright__ = "Copyright 2021, Microsoft Corp."

import logging
import time
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from typing import Any, Optional
from kuberay_client import constants


log = logging.getLogger(__name__)
if logging.getLevelName(log.level) == "NOTSET":
    logging.basicConfig(format="%(asctime)s %(message)s", level=constants.LOGLEVEL)


class RayClusterApi:
    """
    RayClusterApi provides APIs to list, get, create, build, update, delete rayclusters.

    Methods:
    - list_ray_clusters(k8s_namespace: str = "default", async_req: bool = False) -> Any:
    - get_ray_cluster(name: str, k8s_namespace: str = "default") -> Any:
    - create_ray_cluster(body: Any, k8s_namespace: str = "default") -> Any:
    - delete_ray_cluster(name: str, k8s_namespace: str = "default") -> bool:
    - patch_ray_cluster(name: str, ray_patch: Any, k8s_namespace: str = "default") -> Any:
    """

    # initial config to setup the kube client
    def __init__(self):
        # loading the config
        try:
            self.kube_config: Optional[Any] = config.load_kube_config()
        except config.ConfigException:
            # No kubeconfig found, try in-cluster config
            try:
                self.kube_config: Optional[Any] = config.load_incluster_config()
            except config.ConfigException:
                log.error("Failed to load both kubeconfig and in-cluster config")
                raise

        self.api = client.CustomObjectsApi()
        self.core_v1_api = client.CoreV1Api()

    def __del__(self):
        self.api = None
        self.kube_config = None

    def list_ray_clusters(
        self,
        k8s_namespace: str = "default",
        label_selector: str = "",
        async_req: bool = False,
    ) -> Any:
        """List Ray clusters in a given namespace.

        Parameters:
        - k8s_namespace (str, optional): The namespace in which to list the Ray clusters. Defaults to "default".
        - async_req (bool, optional): Whether to make the request asynchronously. Defaults to False.

        Returns:
            Any: The custom resource for Ray clusters in the specified namespace, or None if not found.

        Raises:
            ApiException: If there was an error fetching the custom resource.
        """
        try:
            resource: Any = self.api.list_namespaced_custom_object(
                group=constants.GROUP,
                version=constants.CLUSTER_VERSION,
                plural=constants.CLUSTER_PLURAL,
                namespace=k8s_namespace,
                label_selector=label_selector,
                async_req=async_req,
            )
            if "items" in resource:
                return resource
            return None
        except ApiException as e:
            if e.status == 404:
                log.error("raycluster resource is not found. error = {}".format(e))
                return None
            else:
                log.error("error fetching custom resource: {}".format(e))
                return None

    def get_ray_cluster(self, name: str, k8s_namespace: str = "default") -> Any:
        """Get a specific Ray cluster in a given namespace.

        Parameters:
        - name (str): The name of the Ray cluster custom resource. Defaults to "".
        - k8s_namespace (str, optional): The namespace in which to retrieve the Ray cluster. Defaults to "default".

        Returns:
            Any: The custom resource for the specified Ray cluster, or None if not found.

        Raises:
            ApiException: If there was an error fetching the custom resource.
        """
        try:
            resource: Any = self.api.get_namespaced_custom_object(
                group=constants.GROUP,
                version=constants.CLUSTER_VERSION,
                plural=constants.CLUSTER_PLURAL,
                name=name,
                namespace=k8s_namespace,
            )
            return resource
        except ApiException as e:
            if e.status == 404:
                log.error("raycluster resource is not found. error = {}".format(e))
                return None
            else:
                log.error("error fetching custom resource: {}".format(e))
                return None

    def get_ray_cluster_status(
        self,
        name: str,
        k8s_namespace: str = "default",
        timeout: int = 60,
        delay_between_attempts: int = 5,
    ) -> Any:
        """Get a specific Ray cluster status in a given namespace.

        This method waits until the cluster has a status field populated by the operator.

        Parameters:
        - name (str): The name of the Ray cluster custom resource.
        - k8s_namespace (str, optional): The namespace in which to retrieve the Ray cluster. Defaults to "default".
        - timeout (int, optional): The duration in seconds after which we stop trying to get status. Defaults to 60 seconds.
        - delay_between_attempts (int, optional): The duration in seconds to wait between attempts. Defaults to 5 seconds.

        Returns:
            Any: The custom resource status for the specified Ray cluster, or None if not found or timeout.
        """
        while timeout > 0:
            try:
                resource: Any = self.api.get_namespaced_custom_object_status(
                    group=constants.GROUP,
                    version=constants.CLUSTER_VERSION,
                    plural=constants.CLUSTER_PLURAL,
                    name=name,
                    namespace=k8s_namespace,
                )
            except ApiException as e:
                if e.status == 404:
                    log.error("raycluster resource is not found. error = {}".format(e))
                    return None
                else:
                    log.error("error fetching custom resource: {}".format(e))
                    return None

            if resource and "status" in resource and resource["status"]:
                return resource["status"]
            else:
                log.info("raycluster {} status not set yet, waiting...".format(name))
                time.sleep(delay_between_attempts)
                timeout -= delay_between_attempts

        log.info("timed out waiting for raycluster {} status".format(name))
        return None

    def wait_until_ray_cluster_running(
        self,
        name: str,
        k8s_namespace: str = "default",
        timeout: int = 60,
        delay_between_attempts: int = 5,
    ) -> bool:
        """Wait until a Ray cluster is in ready state.

        This method waits for the cluster to have a state field with value 'ready'.

        Parameters:
        - name (str): The name of the Ray cluster custom resource.
        - k8s_namespace (str, optional): The namespace in which to retrieve the Ray cluster. Defaults to "default".
        - timeout (int, optional): The duration in seconds after which we stop trying. Defaults to 60 seconds.
        - delay_between_attempts (int, optional): The duration in seconds to wait between attempts. Defaults to 5 seconds.

        Returns:
            bool: True if the raycluster status is 'ready', False otherwise.
        """
        while timeout > 0:
            status = self.get_ray_cluster_status(
                name, k8s_namespace, timeout, delay_between_attempts
            )

            if status and "state" in status:
                current_state = status["state"]
                if current_state == "ready":
                    log.info(
                        "raycluster {} is ready with state: {}".format(
                            name, current_state
                        )
                    )
                    return True
                else:
                    log.info(
                        "raycluster {} is in state: {} (waiting for ready)".format(
                            name, current_state
                        )
                    )
            else:
                log.info(
                    "raycluster {} state field not available yet, waiting...".format(
                        name
                    )
                )

            time.sleep(delay_between_attempts)
            timeout -= delay_between_attempts

        log.info("raycluster {} has not become ready before timeout".format(name))
        return False

    def create_ray_cluster(self, body: Any, k8s_namespace: str = "default") -> Any:
        """Create a new Ray cluster custom resource.

        Parameters:
        - body (Any): The data of the custom resource to create.
        - k8s_namespace (str, optional): The namespace in which to create the custom resource. Defaults to "default".

        Returns:
            Any: The created custom resource, or None if it already exists or there was an error.
        """
        try:
            resource: Any = self.api.create_namespaced_custom_object(
                group=constants.GROUP,
                version=constants.CLUSTER_VERSION,
                plural=constants.CLUSTER_PLURAL,
                body=body,
                namespace=k8s_namespace,
            )
            return resource
        except ApiException as e:
            if e.status == 409:
                log.error(
                    "raycluster resource already exists. error = {}".format(e.reason)
                )
                return None
            else:
                log.error("error creating custom resource: {}".format(e))
                return None

    def delete_ray_cluster(self, name: str, k8s_namespace: str = "default") -> bool:
        """Delete a Ray cluster custom resource.

        Parameters:
        - name (str): The name of the Ray cluster custom resource to delete.
        - k8s_namespace (str, optional): The namespace in which the Ray cluster exists. Defaults to "default".

        Returns:
            Any: The deleted custom resource, or None if already deleted or there was an error.
        """
        try:
            resource: Any = self.api.delete_namespaced_custom_object(
                group=constants.GROUP,
                version=constants.CLUSTER_VERSION,
                plural=constants.CLUSTER_PLURAL,
                name=name,
                namespace=k8s_namespace,
            )
            return resource
        except ApiException as e:
            if e.status == 404:
                log.error(
                    "raycluster custom resource already deleted. error = {}".format(
                        e.reason
                    )
                )
                return None
            else:
                log.error(
                    "error deleting the raycluster custom resource: {}".format(e.reason)
                )
                return None

    def patch_ray_cluster(
        self, name: str, ray_patch: Any, k8s_namespace: str = "default"
    ) -> Any:
        """Patch an existing Ray cluster custom resource.

        Parameters:
        - name (str): The name of the Ray cluster custom resource to be patched.
        - ray_patch (Any): The patch data for the Ray cluster.
        - k8s_namespace (str, optional): The namespace in which the Ray cluster exists. Defaults to "default".

        Returns:
            bool: True if the patch was successful, False otherwise.
        """
        try:
            # we patch the existing raycluster with the new config
            self.api.patch_namespaced_custom_object(
                group=constants.GROUP,
                version=constants.CLUSTER_VERSION,
                plural=constants.CLUSTER_PLURAL,
                name=name,
                body=ray_patch,
                namespace=k8s_namespace,
            )
        except ApiException as e:
            log.error("raycluster `{}` failed to patch, with error: {}".format(name, e))
            return False
        else:
            log.info("raycluster `%s` is patched successfully", name)

        return True
