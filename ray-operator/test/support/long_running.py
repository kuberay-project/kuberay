"""`long_running.py` is used to test RayJob `suspend` and `resume`."""
import time
for i in range(10000):
    print(i)
    time.sleep(1)
