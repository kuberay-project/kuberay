"""This script create a number of tasks at roughly the same time, and wait for their completion."""

import ray
import time
import random

# The task number should be large enough, so the autoscalar is triggered to scale to max replica.
_TASK_NUM = 30
# The min task duration should be long enough, which passes the autoscaling stage of the test.
_TASK_MIN_DUR_SEC = 5
# The max task duration should be reasonable to have a cap on overal test duration.
_TASK_MAX_DUR_SEC = 10

@ray.remote(num_cpus=1)
def f():
  sleep_time_sec = random.randint(_TASK_MIN_DUR_SEC, _TASK_MAX_DUR_SEC)
  time.sleep(sleep_time_sec)

ray.get([f.remote() for _ in range(_TASK_NUM)])
