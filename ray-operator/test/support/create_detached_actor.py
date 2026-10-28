import ray
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('name')
parser.add_argument('--num-cpus', type=float, default=1)
parser.add_argument('--num-gpus', type=float, default=0)
parser.add_argument('--custom-resource-name', type=str, default="CustomResource")
parser.add_argument('--num-custom-resources', type=float, default=0)
parser.add_argument('--count', type=int, default=1)
args = parser.parse_args()

@ray.remote(num_cpus=args.num_cpus, num_gpus=args.num_gpus, resources={args.custom_resource_name: args.num_custom_resources})
class Actor:
    pass


ray.init(namespace="default_namespace")
for i in range(args.count):
    name = f"{args.name}{i}" if args.count > 1 else args.name
    Actor.options(name=name, lifetime="detached").remote()
