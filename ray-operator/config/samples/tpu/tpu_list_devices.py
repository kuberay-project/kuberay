import os
import ray
import jax

from jax.experimental import multihost_utils

ray.init()

@ray.remote(resources={"TPU": 4})
def tpu_cores():
    multihost_utils.sync_global_devices("sync")
    print(f"TPU Worker: {os.environ.get('TPU_WORKER_ID')}")
    return f"TPU cores: {jax.device_count()}"

num_workers = int(ray.available_resources()["TPU"]) // 4
print(f"Number of TPU Workers: {num_workers}")
result = [tpu_cores.remote() for _ in range(num_workers)]
print(ray.get(result))
