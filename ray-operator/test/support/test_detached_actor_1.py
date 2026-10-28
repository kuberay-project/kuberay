import ray
import sys

ray.init(namespace=sys.argv[1])

@ray.remote
class TestCounter:
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

tc = TestCounter.options(name="testCounter", lifetime="detached", max_restarts=-1).remote()
val1 = ray.get(tc.increment.remote())
val2 = ray.get(tc.increment.remote())
print(f"val1: {val1}, val2: {val2}")

assert(val1 == 1)
assert(val2 == 2)
