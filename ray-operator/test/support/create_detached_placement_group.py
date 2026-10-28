import ray
import argparse
from ray.util.placement_group import placement_group

parser = argparse.ArgumentParser()
parser.add_argument('--name', type=str, default='detached_pg')
parser.add_argument('--num-cpus-per-bundle', type=int, default=1)
parser.add_argument('--num-bundles', type=int, default=1)
parser.add_argument('--strategy', type=str, default='STRICT_PACK')
args = parser.parse_args()

ray.init(namespace="pg_namespace")
pg = placement_group([{"CPU": args.num_cpus_per_bundle}] * args.num_bundles, strategy=args.strategy, lifetime="detached", name=args.name)
