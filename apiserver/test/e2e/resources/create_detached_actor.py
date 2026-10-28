import ray

@ray.remote(num_cpus=1)
class Actor:
    pass

ray.init(namespace="default_namespace")
Actor.options(name="detached_actor", lifetime="detached").remote()
