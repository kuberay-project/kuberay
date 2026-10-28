import sys
import os
from os import path
import json
import time

"""
in case you are working directly with the source, and don't wish to
install the module with pip install, you can directly import the packages by uncommenting the following code.
"""

"""
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sibling_dirs = [
    d for d in os.listdir(parent_dir) if os.path.isdir(os.path.join(parent_dir, d))
]
for sibling_dir in sibling_dirs:
    sys.path.append(os.path.join(parent_dir, sibling_dir))

"""
from kuberay_client import kuberay_cluster_api

from kuberay_client.utils import kuberay_cluster_builder


def wait(duration: int = 5, step_name: str = "next"):
    print("waiting for {} seconds before {} step".format(duration, step_name))
    for i in range(duration, 0, -1):
        sys.stdout.write(str(i) + " ")
        sys.stdout.flush()
        time.sleep(1)
    print()


def main():

    print("starting cluster handler...")

    my_kube_ray_api = kuberay_cluster_api.RayClusterApi()

    my_cluster_director = kuberay_cluster_builder.Director()

    # building the raycluster representation
    cluster_body = my_cluster_director.build_small_cluster(
        name="new-small-cluster", k8s_namespace="default"
    )

    # creating the raycluster in k8s
    if cluster_body:
        print("creating the cluster...")
        my_kube_ray_api.create_ray_cluster(body=cluster_body)

    # now the cluster should be created.
    # the rest of the code is simply to fetch, print and cleanup the created cluster

    print("fetching the cluster...")
    # fetching the raycluster from k8s api-server
    kube_ray_cluster = my_kube_ray_api.get_ray_cluster(
        name=cluster_body["metadata"]["name"], k8s_namespace="default"
    )

    if kube_ray_cluster:
        print(
            "try: kubectl -n {} get raycluster {} -o yaml".format(
                kube_ray_cluster["metadata"]["namespace"],
                kube_ray_cluster["metadata"]["name"],
            )
        )
        wait(step_name="print created cluster in JSON")
        print("printing the raycluster JSON representation...")
        json_formatted_str = json.dumps(kube_ray_cluster, indent=2)
        print(json_formatted_str)

    # waiting until the cluster is running, and has its status updated
    is_running = my_kube_ray_api.wait_until_ray_cluster_running(
        name=kube_ray_cluster["metadata"]["name"],
        k8s_namespace=kube_ray_cluster["metadata"]["namespace"],
    )

    print("raycluster {} status is {}".format(kube_ray_cluster["metadata"]["name"],"Running" if is_running else "unknown"))

    wait(step_name="cleaning up")
    print("deleting raycluster {}.".format(kube_ray_cluster["metadata"]["name"]))

    my_kube_ray_api.delete_ray_cluster(
        name=kube_ray_cluster["metadata"]["name"],
        k8s_namespace=kube_ray_cluster["metadata"]["namespace"],
    )


if __name__ == "__main__":
    main()
