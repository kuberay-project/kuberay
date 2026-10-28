"""
Demonstrates running Python code in Agent Sandbox pods from a Ray job.
"""

import sys
import time
import ray

# A few sample snippets to demonstrate code execution
CODE_SNIPPETS = [
    (
        "compute_fib.py",
        "a, b = 0, 1\n"
        "for _ in range(20):\n"
        "    a, b = b, a + b\n"
        "print(f'fib(20) = {a}')",
    ),
    (
        "json_aggregation.py",
        "import json\n"
        "data = [1, 4, 9, 16, 25]\n"
        "print(json.dumps({'mean': sum(data) / len(data), 'max': max(data)}))",
    ),
]

@ray.remote(num_cpus=0)
class SandboxExecutor:
    """A Ray actor that claims and manages a single sandbox."""

    def __init__(self, worker_id: int):
        from k8s_agent_sandbox import SandboxClient
        from k8s_agent_sandbox.models import SandboxInClusterConnectionConfig

        self.worker_id = worker_id

        self.client = SandboxClient(
            connection_config=SandboxInClusterConnectionConfig(
                use_pod_ip=True,
                server_port=8888,
            ),
            cleanup=True,
        )

        t_claim = time.time()
        self.sandbox = self.client.create_sandbox(
            warmpool="python-runtime-pool",
        )
        print(f"[executor-{worker_id}] claimed sandbox '{self.sandbox.claim_name}' in {time.time() - t_claim:.3f}s")

    def execute(self, name: str, code: str, timeout: int = 5) -> dict:
        """Write the code to the sandbox and run it."""
        try:
            self.sandbox.files.write(name, code)
            t0 = time.time()
            res = self.sandbox.commands.run(f"python {name}", timeout=timeout)

            return {
                "name": name,
                "exit_code": res.exit_code,
                "stdout": res.stdout,
                "stderr": res.stderr,
                "duration": time.time() - t0,
            }
        except Exception as e:
            return {
                "name": name,
                "exit_code": None,
                "error": str(e),
            }

    def cleanup(self):
        """Release the sandbox back to be deleted/replaced."""
        self.client.delete_sandbox(self.sandbox.claim_name)


def main() -> int:
    ray.init()

    NUM_EXECUTORS = 2
    executors = []

    try:
        print(f"Starting {NUM_EXECUTORS} SandboxExecutors...")
        executors = [SandboxExecutor.remote(worker_id=i) for i in range(NUM_EXECUTORS)]

        print(f"Dispatching {len(CODE_SNIPPETS)} code executors...")

        futures = [
            executors[i % NUM_EXECUTORS].execute.remote(name, code)
            for i, (name, code) in enumerate(CODE_SNIPPETS)
        ]

        results = ray.get(futures)

        print("\n--- Execution Results ---")
        for result in results:
            print(f"\n[{result['name']}] (Exit Code: {result.get('exit_code')})")
            if result.get("stdout"):
                print(f"  Stdout: {result['stdout'].strip()}")
            if result.get("stderr"):
                print(f"  Stderr: {result['stderr'].strip()}")
            if result.get("error"):
                print(f"  Error:  {result['error']}")

    finally:
        if executors:
            print("\nCleaning up sandboxes...")
            try:
                ray.get([e.cleanup.remote() for e in executors])
            except Exception as e:
                print(f"Cleanup error: {e}", file=sys.stderr)

        ray.shutdown()
        print("Done.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
