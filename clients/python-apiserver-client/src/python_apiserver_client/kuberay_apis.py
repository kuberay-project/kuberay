import time

import requests
from python_apiserver_client.params import (
    Cluster,
    RayJobInfo,
    RayJobRequest,
    Template,
    cluster_decoder,
    clusters_decoder,
    template_decoder,
    templates_decoder,
)

_headers = {"Content-Type": "application/json", "accept": "application/json"}

CONNECT_TIMEOUT = 50
READ_TIMEOUT = 50
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)


class KubeRayAPIs:
    """
    This class implements KubeRay APIs based on the API server.
    To create a class, the following parameters are required:
        base - the URL of the API server (default is set to the standalone API server)
        wait interval - the amount of sec to wait between checking for cluster ready
    """
    def __init__(
            self,
            server_url: str = "http://localhost:31888",
            token: str = None,
            wait_interval: int = 2
    ) -> None:
        """
        Initialization
        :param server_url: API server url
        :param token: token, only used for API server with security enabled
        :param wait_interval: wait interval
        """
        self.base = server_url
        if token is not None:
            _headers["Authorization"] = token
        self.wait_interval = wait_interval
        self.api_base = "/apis/v1/"

    def list_compute_templates(self) -> tuple[int, str, list[Template]]:
        """
        List compute templates across all namespaces of the k8 cluster
        :return: tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            list of compute templates
        """
        # Execute HTTP request
        url = self.base + self.api_base + "compute_templates"
        response = requests.get(url, headers=_headers, timeout=TIMEOUT)
        # Check execution status
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"], None
        return response.status_code, None, templates_decoder(response.json())

    def list_compute_templates_namespace(self, ns: str) -> tuple[int, str, list[Template]]:
        """
        List compute templates across for a given namespaces of the k8 cluster
        :param ns: namespace to query
        :return: return tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            list of compute templates
        """
        # Execute HTTP request
        url = self.base + self.api_base + f"namespaces/{ns}/compute_templates"
        response = requests.get(url, headers=_headers, timeout=TIMEOUT)
        # Check execution status
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"], None
        return response.status_code, None, templates_decoder(response.json())

    def get_compute_template(self, ns: str, name: str) -> tuple[int, str, Template]:
        """
        get a compute template
        :param ns: namespace
        :param name: template name
        :return: tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            compute templates
        """
        # Execute HTTP request
        url = self.base + self.api_base + f"namespaces/{ns}/compute_templates/{name}"
        response = requests.get(url, headers=_headers, timeout=TIMEOUT)
        # Check execution status
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"], None
        return response.status_code, None, template_decoder(response.json())

    def create_compute_template(self, template: Template) -> tuple[int, str]:
        """
        Create a compute template
        :param template - definition of a template
        :return: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
        """
        # Execute HTTP request
        url = self.base + self.api_base + f"namespaces/{template.namespace}/compute_templates"
        response = requests.post(url, json=template.to_dict(), headers=_headers, timeout=TIMEOUT)
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"]
        return response.status_code, None

    def delete_compute_template(self, ns: str, name: str) -> tuple[int, str]:
        """
        delete a compute template
        :param ns: namespace
        :param name: template name
        :returns: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
        """
        # Execute HTTP request
        url = self.base + self.api_base + f"namespaces/{ns}/compute_templates/{name}"
        response = requests.delete(url, headers=_headers, timeout=TIMEOUT)
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"]
        return response.status_code, None

    def list_clusters(self) -> tuple[int, str, list[Cluster]]:
        """
        List clusters across all namespaces of the k8 cluster
        :returns: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            list of clusters
        """
        # Execute HTTP request
        url = self.base + self.api_base + "clusters"
        response = requests.get(url, headers=_headers, timeout=TIMEOUT)
        # Check execution status
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"], None
        return response.status_code, None, clusters_decoder(response.json())

    def list_clusters_namespace(self, ns: str) -> tuple[int, str, list[Cluster]]:
        """
        List clusters across for a given namespaces of the k8 cluster
        :param ns: namespace to query
        :return: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            list of clusters
        """
        # Execute HTTP request
        url = self.base + self.api_base + f"namespaces/{ns}/clusters"
        response = requests.get(url, headers=_headers, timeout=TIMEOUT)
        # Check execution status
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"], None
        return response.status_code, None, clusters_decoder(response.json())

    def get_cluster(self, ns: str, name: str) -> tuple[int, str, Cluster]:
        """
        get cluster
        :param ns: namespace
        :param name: name of the cluster
        :return: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            clusters definition
        """
        # Execute HTTP request
        url = self.base + self.api_base + f"namespaces/{ns}/clusters/{name}"
        response = requests.get(url, headers=_headers, timeout=TIMEOUT)
        # Check execution status
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"], None
        return response.status_code, None, cluster_decoder(response.json())

    def create_cluster(self, cluster: Cluster) -> tuple[int, str]:
        """
        create cluster
        :param cluster: cluster definition
        :return: tuple containing
            http return code
            message - only returned if http return code is not equal to 200
        """
        # Execute HTTP request
        url = self.base + self.api_base + f"namespaces/{cluster.namespace}/clusters"
        response = requests.post(url, json=cluster.to_dict(), headers=_headers, timeout=TIMEOUT)
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"]
        return response.status_code, None

    def get_cluster_status(self, ns: str, name: str) -> tuple[int, str, str]:
        """
        get cluster status
        :param ns: namespace of the cluster
        :param name: name of the cluster
        :return: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            cluster status
        """
        # Execute HTTP request
        status, error, cluster = self.get_cluster(ns=ns, name=name)
        # Check execution status
        if status // 100 != 2:
            return status, error, None
        cluster_status = "creating"
        if cluster.cluster_status is not None:
            cluster_status = cluster.cluster_status
        return status, None, cluster_status

    def wait_cluster_ready(self, ns: str, name: str, wait: int = -1) -> tuple[int, str]:
        """
        wait for cluster to be ready
        :param ns: namespace of the cluster
        :param name: name of the cluster
        :param wait: wait time (-1 waits forever)
        :returns: A tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            cluster status
        """
        current_wait = 0
        while True:
            status, error, c_status = self.get_cluster_status(ns=ns, name=name)
            # Check execution status
            if status // 100 != 2:
                return status, error
            if c_status == "ready":
                return status, None
            if current_wait > wait > 0:
                return 408, f"Timed out waiting for cluster ready in {current_wait} sec"
            time.sleep(self.wait_interval)
            current_wait += self.wait_interval

    def get_cluster_endpoints(self, ns: str, name: str, wait: int = -1) -> tuple[int, str, str]:
        """
        get cluster endpoint
        :param ns: namespace of the cluster
        :param name: name of the cluster
        :param wait: wait time (-1 waits forever) for cluster to be ready
        :returns: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            endpoint (service for dashboard endpoint)
        """
        # Ensure that the cluster is ready
        status, error = self.wait_cluster_ready(ns=ns, name=name, wait=wait)
        if status // 100 != 2:
            return status, error, None
        # Get cluster
        status, error, cluster = self.get_cluster(ns=ns, name=name)
        if status // 100 != 2:
            return status, error, None
        return status, None, f"{name}-head-svc.{ns}.svc.cluster.local:{cluster.service_endpoint['dashboard']}"

    def delete_cluster(self, ns: str, name: str) -> tuple[int, str]:
        """
        delete cluster
        :param ns: namespace of the cluster
        :param name: name of the cluster
        :return: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
        """
        # Execute HTTP request
        url = self.base + self.api_base + f"namespaces/{ns}/clusters/{name}"
        response = requests.delete(url, headers=_headers, timeout=TIMEOUT)
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"]
        return response.status_code, None

    def submit_job(self, ns: str, name: str, job_request: RayJobRequest) -> tuple[int, str, str]:
        """
        submit Ray job
        :param ns: namespace of the cluster
        :param name: name of the cluster
        :param job_request: job submission
        :return: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            submission id
        """
        url = self.base + self.api_base + f"namespaces/{ns}/jobsubmissions/{name}"
        response = requests.post(url, json=job_request.to_dict(), headers=_headers, timeout=TIMEOUT)
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"], None
        return response.status_code, None, response.json()["submissionId"]

    def get_job_info(self, ns: str, name: str, sid: str) -> tuple[int, str, RayJobInfo]:
        """
        get Ray job details
        :param ns: namespace of the cluster
        :param name: name of the cluster
        :param sid: job submission id
        return: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            RayJobInfo object
        """
        url = self.base + self.api_base + f"namespaces/{ns}/jobsubmissions/{name}/{sid}"
        response = requests.get(url, headers=_headers, timeout=TIMEOUT)
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"], None
        return response.status_code, None, RayJobInfo(response.json())

    def list_job_info(self, ns: str, name: str) -> tuple[int, str, list[RayJobInfo]]:
        """
        list Ray job details
        :param ns: namespace of the cluster
        :param name: name of the cluster
        :return: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            list of RayJobInfo object
        """
        url = self.base + self.api_base + f"namespaces/{ns}/jobsubmissions/{name}"
        response = requests.get(url, headers=_headers, timeout=TIMEOUT)
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"], None
        infos = response.json().get("submissions", None)
        if infos is None:
            return response.status_code, None, []
        return response.status_code, None, [RayJobInfo(i) for i in infos]

    def get_job_log(self, ns: str, name: str, sid: str) -> tuple[int, str, str]:
        """
        get Ray job log
        :param ns: namespace of the cluster
        :param name: name of the cluster
        :param sid: job submission id
        return: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
            log
        """
        url = self.base + self.api_base + f"namespaces/{ns}/jobsubmissions/{name}/log/{sid}"
        response = requests.get(url, headers=_headers, timeout=TIMEOUT)
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"], None
        return response.status_code, None, response.json().get("log", "")

    def stop_ray_job(self, ns: str, name: str, sid: str) -> tuple[int, str]:
        """
        stop Ray job
        :param ns: namespace of the cluster
        :param name: name of the cluster
        :param sid: job submission id
        return: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
        """
        url = self.base + self.api_base + f"namespaces/{ns}/jobsubmissions/{name}/{sid}"
        response = requests.post(url, headers=_headers, timeout=TIMEOUT)
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"]
        return response.status_code, None

    def delete_ray_job(self, ns: str, name: str, sid: str) -> tuple[int, str]:
        """
        delete Ray job
        :param ns: namespace of the cluster
        :param name: name of the cluster
        :param sid: job submission id
        return: a tuple containing
            http return code
            message - only returned if http return code is not equal to 200
        """
        url = self.base + self.api_base + f"namespaces/{ns}/jobsubmissions/{name}/{sid}"
        response = requests.delete(url, headers=_headers, timeout=TIMEOUT)
        if response.status_code // 100 != 2:
            return response.status_code, response.json()["message"]
        return response.status_code, None
