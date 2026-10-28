import sys
import os
from os import path
import json


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


def main():

    print("starting cluster handler...")
    my_kuberay_api = kuberay_cluster_api.RayClusterApi()

    my_cluster_builder = kuberay_cluster_builder.ClusterBuilder()

    cluster1 = (
        my_cluster_builder.build_meta(name="new-cluster1", labels={'demo-cluster':'yes'})
        .build_head()
        .build_worker(group_name="workers")
        .get_cluster()
    )

    if not my_cluster_builder.succeeded:
        print("error building the cluster, aborting...")
        return

    print("creating raycluster = {}".format(cluster1["metadata"]["name"]))
    my_kuberay_api.create_ray_cluster(body=cluster1)

    # the rest of the code is simply to list and cleanup the created cluster
    kube_ray_list = my_kuberay_api.list_ray_clusters(k8s_namespace="default", label_selector='demo-cluster=yes')
    if "items" in kube_ray_list:
        line = "-" * 72
        print(line)
        print("{:<63s}{:>2s}".format("Name", "Namespace"))
        print(line)
        for cluster in kube_ray_list["items"]:
            print(
                "{:<63s}{:>2s}".format(
                    cluster["metadata"]["name"],
                    cluster["metadata"]["namespace"],
                )
            )
    print(line)

    if "items" in kube_ray_list:
        for cluster in kube_ray_list["items"]:
            print("deleting raycluster = {}".format(cluster["metadata"]["name"]))
            my_kuberay_api.delete_ray_cluster(
                name=cluster["metadata"]["name"],
                k8s_namespace=cluster["metadata"]["namespace"],
            )


if __name__ == "__main__":
    main()
