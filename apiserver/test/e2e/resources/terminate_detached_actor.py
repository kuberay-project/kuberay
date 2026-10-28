import ray

ray.init(namespace="default_namespace")
detached_actor = ray.get_actor("detached_actor")
ray.kill(detached_actor)
