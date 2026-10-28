import ray
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--name', type=str, default='detached_pg')
args = parser.parse_args()

ray.init(namespace="pg_namespace")
pg = ray.util.get_placement_group(args.name)
ray.get(pg.ready())
