import ray
from ray.util.placement_group import placement_group, remove_placement_group

def get_alive_nodes():
    return {n["NodeID"] for n in ray.nodes() if n["alive"]}

pg1 = placement_group([{"CPU": 1}] * 1, strategy="STRICT_SPREAD")
ray.get(pg1.ready())
nodes = get_alive_nodes()
assert len(nodes) == 2 # 1 head + 1 worker
remove_placement_group(pg1)

# This pg2 should rely on the worker previously used by pg1, plus a new worker to be created.
# So, the autoscaler should not remove the old worker while creating the new one.
# We assert that the previous nodes should be a subset (< operator) of the new nodes. This assertion only works for Ray >= 2.45.0.
pg2 = placement_group([{"CPU": 1}] * 2, strategy="STRICT_SPREAD")
ray.get(pg2.ready())
nodes2 = get_alive_nodes()
assert nodes < nodes2, "some of nodes are unexpectedly removed from the cluster."
assert len(nodes2) == 3 # 1 head + 2 worker
remove_placement_group(pg2)
