"""Create RayCluster CR periodically"""
from string import Template
from datetime import datetime

import subprocess
import tempfile
import time

RAYCLUSTER_TEMPLATE = "ray-cluster.benchmark.yaml.template"


def create_ray_cluster(template, cr_name, num_pods):
    """Replace the template with the name of the RayCluster CR and create the CR"""
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print(
        f"Current Time = {current_time}, RayCluster CR: {cr_name} ({num_pods} Pods) is created"
    )

    with open(template, encoding="utf-8") as ray_cluster_template:
        template = Template(ray_cluster_template.read())
        yamlfile = template.substitute(
            {
                "raycluster_name": cr_name,
                "num_worker_pods": num_pods - 1,
            }
        )
    with tempfile.NamedTemporaryFile(
        "w", suffix="_ray_cluster_yaml"
    ) as ray_cluster_yaml:
        ray_cluster_yaml.write(yamlfile)
        ray_cluster_yaml.flush()
        # Execute a command "kubectl apply -f $ray_cluster_yaml.name"
        command = f"kubectl apply -f {ray_cluster_yaml.name}"
        subprocess.run(command, shell=True, check=False)


def period_create_cr(num_cr, period, num_pods):
    """Create RayCluster CR periodically"""
    for i in range(num_cr):
        create_ray_cluster(RAYCLUSTER_TEMPLATE, f"raycluster-{i}", num_pods)
        subprocess.run("kubectl get raycluster", shell=True, check=False)
        time.sleep(period)


def period_update_cr(cr_name, period, diff_pods, num_iter):
    for i in range(num_iter):
        create_ray_cluster(RAYCLUSTER_TEMPLATE, cr_name, (i + 1) * diff_pods)
        subprocess.run("kubectl get raycluster", shell=True, check=False)
        time.sleep(period)


# [Experiment 1]: Create a 1-node (1 head + 0 worker) RayCluster every 20 seconds until there are 150 RayCluster custom resources.
period_create_cr(150, 30, 1)

# [Experiment 2]: In the Kubernetes cluster, there is only 1 RayCluster. Add 5 new worker Pods to this RayCluster every 60 seconds until the total reaches 150 Pods.
# period_update_cr("raycluster-0", 60, 5, 30)

# [Experiment 3]: Create a 5-node (1 head + 4 workers) RayCluster every 60 seconds until there are 30 RayCluster custom resources.
# period_create_cr(30, 60, 5)
