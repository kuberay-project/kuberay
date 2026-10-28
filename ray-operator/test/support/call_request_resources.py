import ray
import time
import argparse
from ray.autoscaler.sdk import request_resources

parser = argparse.ArgumentParser()
parser.add_argument("--num-cpus", type=int, default=1)
args = parser.parse_args()

ray.init()
request_resources(num_cpus=args.num_cpus)
