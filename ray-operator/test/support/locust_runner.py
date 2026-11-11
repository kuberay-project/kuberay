"""
This script is based on: https://raw.githubusercontent.com/ray-project/serve_workloads/main/microbenchmarks/locust_runner.py

Run Locust on Ray cluster.

Run this script on a Ray cluster's head node to launch one Locust worker per
CPU across the Ray cluster's nodes.

Example command:

$ python locust_runner.py -f locustfile.py -u 200 -r 50 --host [HOST_URL]
"""

import os
import ray
import json
import time
import argparse
import sys
import subprocess
from tqdm import tqdm


ray.init()

HTML_RESULTS_DIR = os.environ.get("HTML_RESULTS_DIR", "locust_results")
DEFAULT_RESULT_FILENAME = \
    f"{time.strftime('%Y-%m-%d-%p-%H-%M-%S-results.html')}"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--html",
    default=DEFAULT_RESULT_FILENAME,
    type=str,
    help="HTML file to save results to.",
)
parser.add_argument(
    "-t",
    "--run-time",
    default=None,
    type=str,
    help="Test duration. Same option as Locust's --run-time.",
)

args, locust_args = parser.parse_known_args()

num_locust_workers = int(ray.available_resources()["CPU"])
master_address = ray.util.get_node_ip_address()

if not os.path.exists(HTML_RESULTS_DIR):
    os.mkdir(HTML_RESULTS_DIR)

# Required locust args: -f, -u, -r, --host, and any custom locustfile args
base_locust_cmd = [
    "locust",
    "--headless",
    f"--html={HTML_RESULTS_DIR}/{args.html}",
    f"--csv={HTML_RESULTS_DIR}/test",
    *locust_args,
]


@ray.remote(num_cpus=1)
class LocustWorker:
    def __init__(self):
        self.proc = None

    def start(self):
        worker_locust_cmd = base_locust_cmd + [
            "--worker",
            f"--master-host={master_address}",
        ]
        self.proc = subprocess.Popen(worker_locust_cmd)


print(f"Spawning {num_locust_workers} Locust worker Ray tasks.")

# Hold reference to each locust worker to prevent them from being torn down
locust_workers = []
start_refs = []
for _ in tqdm(range(num_locust_workers)):
    locust_worker = LocustWorker.remote()
    locust_workers.append(locust_worker)
    start_refs.append(locust_worker.start.remote())

print("Waiting for Locust worker processes to start.")


def wait_for_locust_workers(start_refs):
    """Generator that yields whenever a worker process starts.

    Use with tqdm to track how many workers have started. If you don't need
    tqdm, use ray.get(start_refs) instead of calling this function.
    """

    remaining_start_refs = start_refs
    while remaining_start_refs:
        finished_start_refs, remaining_start_refs = \
            ray.wait(remaining_start_refs)
        for ref in finished_start_refs:
            yield ray.get(ref)


# No-op for-loop to let tqdm track wait_for_locust_workers() progress
for _ in tqdm(wait_for_locust_workers(start_refs), total=num_locust_workers):
    pass

master_locust_cmd = base_locust_cmd + [
    "--master",
    f"--expect-workers={num_locust_workers}",
    "--json",
]
print(f"Locust command: {master_locust_cmd}")

if args.run_time is not None:
    master_locust_cmd += [f"--run-time={args.run_time}"]
proc = subprocess.Popen(master_locust_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
stdout, stderr = proc.communicate()

print("STDOUT:", stdout.decode())
print("STDERR:", stderr.decode())

data = json.loads(stdout.decode())
assert len(data) == 1, f"data_len: {len(data)}"

num_failures = data[0]["num_failures"]
num_requests = data[0]["num_requests"]

assert num_failures == 0, f"num_failures: {num_failures}"
assert num_requests != 0, f"num_requests: {num_requests}"

print("returncode:", proc.returncode)
# We are not propagating proc.returncode because Locust can exit on code 2
# on a gevent/SIGINT race during shutdown even when the run succeeded.
# The assertions above already verify correctness.
# See a more detailed discussion at https://github.com/ray-project/kuberay/issues/5109
sys.exit(0)
