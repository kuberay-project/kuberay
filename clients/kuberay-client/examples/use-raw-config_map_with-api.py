import json
from os import path
import os
import sys
import time
from kubernetes.client.rest import ApiException
from kubernetes import client
from kubernetes.stream import stream


"""
in case you are working directly with the source, and don't wish to
install the module with pip install, you can directly import the packages by uncommenting the following code.
"""

"""
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
sibling_dirs = [
    d for d in os.listdir(parent_dir) if os.path.isdir(os.path.join(parent_dir, d))
]
for sibling_dir in sibling_dirs:
    sys.path.append(os.path.join(parent_dir, sibling_dir))
"""
from kuberay_client import kuberay_cluster_api


configmap_body: dict = {
  "apiVersion": "v1",
  "kind": "ConfigMap",
  "metadata": {
    "name": "ray-code-single"
  },
  "data": {
    "sample_code.py": "import ray\nfrom os import environ\nredis_pass = environ.get(\"REDIS_PASSWORD\") \nprint(\"trying to connect to Ray!\")\nray.init(address=\"auto\", _redis_password=redis_pass)\nprint(\"now executing some code with Ray!\")\nimport time\nstart = time.time()\n@ray.remote\ndef f():\n  time.sleep(0.01)\n  return ray._private.services.get_node_ip_address()\nvalues=set(ray.get([f.remote() for _ in range(1000)]))\nprint(\"Ray Nodes: \",str(values))\nfile = open(\"/tmp/ray_nodes.txt\",\"a\")\nfile.write(\"available nodes: %s\\n\" % str(values))\nfile.close()\nend = time.time()\nprint(\"Execution time = \",end - start)\n"
  }
}

cluster_body: dict = {
  "apiVersion": "ray.io/v1",
  "kind": "RayCluster",
  "metadata": {
    "labels": {
      "controller-tools.k8s.io": "1.0",
      "demo-cluster": "yes"
    },
    "name": "raycluster-getting-started"
  },
  "spec": {
    "rayVersion": "2.46.0",
    "headGroupSpec": {
      "rayStartParams": {
        "dashboard-host": "0.0.0.0",
        "num-cpus": "2",
      },
      "template": {
        "spec": {
          "containers": [
            {
              "name": "ray-head",
              "image": "rayproject/ray:2.46.0",
              "volumeMounts": [
                {
                  "mountPath": "/opt",
                  "name": "config"
                }
              ]
            }
          ],
          "resources": {
              "limits": {"cpu": "2", "memory": "3G"},
              "requests": {"cpu": "1500m", "memory": "3G"},
          },
          "volumes": [
            {
              "name": "config",
              "configMap": {
                "name": configmap_body["metadata"]["name"],
                "items": [
                  {
                    "key": "sample_code.py",
                    "path": "sample_code.py"
                  }
                ]
              }
            }
          ]
        }
      }
    }
  }
}

"""
the following code is simply to create a configmap and a raycluster using the kuberay_cluster_api

after the cluster is created, the code will execute a python command in the head pod of the cluster

then it will print the logs of the head pod

then it will list the clusters and delete the cluster and the configmap
"""

def main():

    print("starting cluster handler...")

    my_kube_ray_api = kuberay_cluster_api.RayClusterApi() # creating the api object

    try:
        my_kube_ray_api.core_v1_api.create_namespaced_config_map("default", configmap_body)

    except ApiException as e:
        if e.status == 409:
            print("configmap {} already exists = {} moving on...".format(configmap_body["metadata"]["name"] ,e))
        else:
            print("error creating configmap: {}".format(e))




    # waiting for the configmap tp be created
    time.sleep(3)

    my_kube_ray_api.create_ray_cluster(body=cluster_body) # creating the cluster

    # the rest of the code is simply to fetch, print and cleanup the created cluster
    kube_ray_cluster = my_kube_ray_api.get_ray_cluster(
        name=cluster_body["metadata"]["name"], k8s_namespace="default"
    )

    if kube_ray_cluster:
        print("printing the raycluster json representation...")
        json_formatted_str = json.dumps(kube_ray_cluster, indent=2)
        print(json_formatted_str)
    else:
        print("Unable to fetch cluster {}".format(cluster_body["metadata"]["name"]))
        return

    print(
        "try: kubectl -n default get raycluster {} -o yaml".format(
            kube_ray_cluster["metadata"]["name"]
        )
    )
    # the rest of the code is simply to list and cleanup the created cluster

    time.sleep(3)
    try:
        pod_list: client.V1PodList = my_kube_ray_api.core_v1_api.list_namespaced_pod(namespace="default", label_selector='ray.io/cluster={}'.format(cluster_body["metadata"]["name"])) # getting the pods of the cluster
        if pod_list != None:
            for pod in pod_list.items:
                try:
                    # Calling exec and waiting for response
                    exec_command = [
                        'python',
                        '/opt/sample_code.py'
                        ]

                    print("executing a Python command in the raycluster: {}".format(exec_command))
                    # executing a ray command in the head pod
                    resp = stream( my_kube_ray_api.core_v1_api.connect_get_namespaced_pod_exec,
                                pod.metadata.name,
                                'default',
                                command=exec_command,
                                stderr=True, stdin=False,
                                stdout=True, tty=False)
                    print("Response: " + resp)

                    # getting the logs from the pod
                    time.sleep(3)
                    print("getting the logs from the raycluster pod: {}".format(pod.metadata.name))
                    api_response = my_kube_ray_api.core_v1_api.read_namespaced_pod_log(name=pod.metadata.name, namespace='default')
                    print(api_response)

                except ApiException as e:
                    print('An exception has ocurred in reading the logs {}'.format(e))
    except ApiException as e:
                    print('An exception has ocurred in listing pods the logs'.format(e))

    kube_ray_list = my_kube_ray_api.list_ray_clusters(k8s_namespace="default", label_selector='demo-cluster=yes')

    if "items" in kube_ray_list:
        for cluster in kube_ray_list["items"]:
            print("deleting raycluster = {}".format(cluster["metadata"]["name"]))
            my_kube_ray_api.delete_ray_cluster(
                name=cluster["metadata"]["name"],
                k8s_namespace=cluster["metadata"]["namespace"],
            ) # deleting the cluster

    try:
       my_kube_ray_api.core_v1_api.delete_namespaced_config_map(configmap_body["metadata"]["name"], "default") # deleting the configmap
       print("deleting configmap: {}".format(configmap_body["metadata"]["name"]))
    except ApiException as e:
        if e.status == 404:
            print("configmap  = {}, does not exist moving on...".format(configmap_body["metadata"]["name"], e))
        else:
            print("error deleting configmap: {}".format(e))

if __name__ == "__main__":
    main()
