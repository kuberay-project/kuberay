"""
Demonstrates suspending and resuming Agent Sandbox pods with GKE Pod
Snapshots from a Ray job.

Each SandboxExecutor actor drives a simulated multi-turn agent rollout: at
the turn boundary — the moment it has a command result and is waiting on the
model — it suspends its sandbox (memory snapshot + pod termination, freeing
the pod's resource reservation) and resumes it before the next turn. The
snapshot restores the sandbox intact: the second turn reads state that the
first turn wrote to /tmp, which only survives because the whole guest
(memory, process tree, tmpfs) was checkpointed — a plain pod restart would
have wiped it.
"""

import sys
import time
import ray

# Turn 0 writes state to /tmp (lost on a plain pod restart); turn 1 reads it
# back after a suspend/resume cycle, proving the snapshot restored the guest.
CODE_SNIPPETS = [
    (
        "write_state.py",
        "a, b = 0, 1\n"
        "for _ in range(20):\n"
        "    a, b = b, a + b\n"
        "with open('/tmp/agent_state', 'w') as f:\n"
        "    f.write(str(a))\n"
        "print(f'fib(20) = {a} (saved to /tmp)')",
    ),
    (
        "read_state.py",
        "state = open('/tmp/agent_state').read()\n"
        "print(f'state from the previous turn survived the snapshot: fib(20) = {state}')",
    ),
]

# Simulated model inference latency between turns. The sandboxes stay
# suspended for this window, so their resources are free for other work.
MODEL_LATENCY_SECONDS = 10


@ray.remote(num_cpus=0)
class SandboxExecutor:
    """A Ray actor that claims one sandbox and suspends/resumes it between turns."""

    def __init__(self, worker_id: int):
        from k8s_agent_sandbox.gke_extensions.snapshots import PodSnapshotSandboxClient
        from k8s_agent_sandbox.models import SandboxInClusterConnectionConfig

        self.worker_id = worker_id

        # PodSnapshotSandboxClient validates that the GKE Pod Snapshot CRDs
        # are installed and hands out sandboxes with suspend()/resume().
        self.client = PodSnapshotSandboxClient(
            connection_config=SandboxInClusterConnectionConfig(
                use_pod_ip=True,
                server_port=8888,
            ),
            cleanup=True,
        )

        t_claim = time.time()
        self.sandbox = self.client.create_sandbox(
            warmpool="python-snapshot-pool",
        )
        print(f"[executor-{worker_id}] claimed sandbox '{self.sandbox.claim_name}' in {time.time() - t_claim:.3f}s")

    def execute(self, name: str, code: str, timeout: int = 10) -> dict:
        """Write the code to the sandbox and run it (one agent turn)."""
        try:
            self.sandbox.files.write(name, code)
            res = self.sandbox.commands.run(f"python {name}", timeout=timeout)
            return {"name": name, "exit_code": res.exit_code, "stdout": res.stdout, "stderr": res.stderr}
        except Exception as e:
            return {"name": name, "exit_code": None, "error": str(e)}

    def suspend(self) -> float:
        """Snapshot the sandbox, terminate its pod, and return the seconds taken."""
        t0 = time.time()
        res = self.sandbox.suspend(snapshot_before_suspend=True)
        if not res.success:
            raise RuntimeError(f"suspend failed: {res.error_reason}")
        # Wait until the snapshot is Ready: a resume issued before the upload
        # completes would cold-start the pod instead of restoring it.
        snapshot_uid = res.snapshot_response.snapshot_uid
        for _ in range(30):
            listed = self.sandbox.snapshots.list()
            if listed.success and any(s.snapshot_uid == snapshot_uid for s in listed.snapshots):
                return time.time() - t0
            time.sleep(2)
        raise RuntimeError(f"snapshot {snapshot_uid} did not become ready in time")

    def resume(self) -> float:
        """Recreate the pod restored from the snapshot and return the seconds taken."""
        t0 = time.time()
        res = self.sandbox.resume()
        if not res.success:
            raise RuntimeError(f"resume failed: {res.error_reason}")
        if not res.restored_from_snapshot:
            raise RuntimeError("sandbox was cold-started instead of restored from the snapshot")
        return time.time() - t0

    def cleanup(self):
        """Delete the snapshot triggers and release the sandbox."""
        self.sandbox.terminate()


def main() -> int:
    ray.init()

    NUM_EXECUTORS = 2
    executors = []

    try:
        print(f"Starting {NUM_EXECUTORS} SandboxExecutors...")
        executors = [SandboxExecutor.remote(worker_id=i) for i in range(NUM_EXECUTORS)]

        for turn, (name, code) in enumerate(CODE_SNIPPETS):
            if turn > 0:
                # Turn boundary: suspend every sandbox while the "model is thinking".
                for i, s in enumerate(ray.get([e.suspend.remote() for e in executors])):
                    print(f"[executor-{i}] suspended in {s:.1f}s")
                print(f"Sandboxes suspended, waiting {MODEL_LATENCY_SECONDS}s for 'model inference'...")
                time.sleep(MODEL_LATENCY_SECONDS)
                for i, s in enumerate(ray.get([e.resume.remote() for e in executors])):
                    print(f"[executor-{i}] resumed from snapshot in {s:.1f}s")

            results = ray.get([e.execute.remote(name, code) for e in executors])
            for i, r in enumerate(results):
                print(f"[turn {turn}][executor-{i}] {r['name']} (exit={r.get('exit_code')}): "
                      f"{(r.get('stdout') or r.get('error') or '').strip()}")
                if r.get("exit_code") != 0:
                    # Turn 1 reading /tmp is the snapshot-restore proof: a
                    # failure here must fail the job, not just get printed.
                    raise RuntimeError(
                        f"turn {turn} failed on executor-{i}: {r.get('stderr') or r.get('error')}"
                    )

        print("\nAll turns completed: /tmp state survived the suspend/resume cycle.")

    finally:
        if executors:
            print("\nCleaning up sandboxes and snapshot triggers...")
            try:
                ray.get([e.cleanup.remote() for e in executors])
            except Exception as e:
                print(f"Cleanup error: {e}", file=sys.stderr)

        ray.shutdown()
        print("Done.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
