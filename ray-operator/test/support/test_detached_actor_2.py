import ray
import time
import sys

def retry_with_timeout(func, timeout=90):
    err = None
    start = time.time()
    i = 0
    while time.time() - start <= timeout:
        try:
            print(f"retry iter: {i}", flush=True)
            i += 1
            return func()
        except BaseException as e:
            err = e
        finally:
            time.sleep(1)
    raise err

def get_detached_actor():
    return ray.get_actor("testCounter")

# Try to connect to Ray cluster.
print("Try to connect to Ray cluster.", flush=True)
retry_with_timeout(lambda: ray.init(address='ray://127.0.0.1:10001', namespace=sys.argv[1]), timeout = 180)

# Get TestCounter actor
print("Get TestCounter actor.", flush=True)
tc = retry_with_timeout(get_detached_actor)

print("Try to call remote function \'increment\'.", flush=True)
val = retry_with_timeout(lambda: ray.get(tc.increment.remote()))
print(f"val: {val}", flush=True)

assert(val == int(sys.argv[2]))
