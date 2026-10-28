import ray
import os
import emoji
import pyjokes

ray.init()

@ray.remote
def f():
    assert emoji.__version__ == "2.14.0"
    assert pyjokes.__version__ == "0.6.0"

    first_env_var = os.getenv("test_env_var")
    second_env_var = os.getenv("another_env_var")

    assert first_env_var == "first_env_var"
    assert second_env_var == "second_env_var"

ray.get(f.remote())
