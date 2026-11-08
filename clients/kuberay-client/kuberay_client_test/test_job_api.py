import time
import unittest
from kuberay_client import kuberay_job_api, kuberay_cluster_api
from kuberay_client.utils import kuberay_cluster_builder
from helpers import create_job_with_cluster_selector, create_job_with_ray_cluster_spec

namespace = "default"

class TestJobApi(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)

        self.api = kuberay_job_api.RayjobApi()
        self.cluster_api = kuberay_cluster_api.RayClusterApi()
        self.director = kuberay_cluster_builder.Director()

    def test_submit_ray_job_to_existing_cluster(self):
        """Test submitting a job to an existing cluster using clusterSelector."""
        cluster_name = "premade"

        cluster_body = self.director.build_small_cluster(
            name=cluster_name,
            k8s_namespace=namespace,
            labels={"ray.io/cluster": cluster_name},
        )

        self.assertIsNotNone(cluster_body, "Cluster should be built successfully")
        self.assertEqual(cluster_body["metadata"]["name"], cluster_name)

        created_cluster = self.cluster_api.create_ray_cluster(
            body=cluster_body, k8s_namespace=namespace
        )

        self.assertIsNotNone(created_cluster, "Cluster should be created successfully")

        self.cluster_api.wait_until_ray_cluster_running(cluster_name, namespace, 60, 10)
        job_name = "premade-cluster-job"
        try:
            job_body = create_job_with_cluster_selector(
                job_name,
                namespace,
                cluster_name,
            )
            submitted_job = self.api.submit_job(
                job=job_body,
                k8s_namespace=namespace,
            )

            self.assertIsNotNone(submitted_job, "Job should be submitted successfully")
            self.assertEqual(submitted_job["metadata"]["name"], job_name)
            self.assertEqual(
                submitted_job["spec"]["clusterSelector"]["ray.io/cluster"], cluster_name
            )

            self.api.wait_until_job_finished(job_name, namespace, 120, 10)
        finally:
            self.cluster_api.delete_ray_cluster(
                name=cluster_name, k8s_namespace=namespace
            )

            self.api.delete_job(job_name, namespace)

    def test_get_job_status(self):
        """Test getting job status for a running job."""
        cluster_name = "status-test-cluster"

        cluster_body = self.director.build_small_cluster(
            name=cluster_name,
            k8s_namespace=namespace,
            labels={"ray.io/cluster": cluster_name},
        )

        created_cluster = self.cluster_api.create_ray_cluster(
            body=cluster_body, k8s_namespace=namespace
        )
        self.assertIsNotNone(created_cluster, "Cluster should be created successfully")

        self.cluster_api.wait_until_ray_cluster_running(cluster_name, namespace, 60, 10)

        job_name = "status-test-job"
        try:
            job_body = create_job_with_cluster_selector(
                job_name,
                namespace,
                cluster_name,
            )

            submitted_job = self.api.submit_job(
                job=job_body,
                k8s_namespace=namespace,
            )
            self.assertIsNotNone(submitted_job, "Job should be submitted successfully")

            status = self.api.get_job_status(
                job_name, namespace, timeout=30, delay_between_attempts=2
            )
            self.assertIsNotNone(status, "Job status should be retrieved")

            # Verify expected status fields
            self.assertIn(
                "jobDeploymentStatus",
                status,
                "Status should contain jobDeploymentStatus field",
            )
            self.assertIn("jobId", status, "Status should contain jobId field")
            self.assertIn(
                "rayClusterName", status, "Status should contain rayClusterName field"
            )

            self.api.wait_until_job_finished(job_name, namespace, 60, 5)

            final_status = self.api.get_job_status(
                job_name, namespace, timeout=10, delay_between_attempts=1
            )
            self.assertIsNotNone(final_status, "Final job status should be retrieved")

            self.assertIn(
                "jobDeploymentStatus",
                final_status,
                "Final status should contain jobDeploymentStatus field",
            )

        finally:
            self.cluster_api.delete_ray_cluster(
                name=cluster_name, k8s_namespace=namespace
            )
            self.api.delete_job(job_name, namespace)

    def test_wait_until_job_finished(self):
        """Test waiting for job completion."""
        cluster_name = "wait-test-cluster"

        cluster_body = self.director.build_small_cluster(
            name=cluster_name,
            k8s_namespace=namespace,
            labels={"ray.io/cluster": cluster_name},
        )

        created_cluster = self.cluster_api.create_ray_cluster(
            body=cluster_body, k8s_namespace=namespace
        )
        self.assertIsNotNone(created_cluster, "Cluster should be created successfully")

        self.cluster_api.wait_until_ray_cluster_running(
            cluster_name, namespace, 180, 10
        )

        job_name = "wait-test-job"
        try:
            job_body = create_job_with_cluster_selector(
                job_name,
                namespace,
                cluster_name,
            )

            submitted_job = self.api.submit_job(
                job=job_body,
                k8s_namespace=namespace,
            )
            self.assertIsNotNone(submitted_job, "Job should be submitted successfully")

            result = self.api.wait_until_job_finished(
                job_name, namespace, timeout=180, delay_between_attempts=2
            )
            self.assertTrue(result, "Job should complete successfully within timeout")

        finally:
            self.cluster_api.delete_ray_cluster(
                name=cluster_name, k8s_namespace=namespace
            )
            self.api.delete_job(job_name, namespace)

    def test_delete_job(self):
        """Test deleting a job."""
        cluster_name = "delete-test-cluster"

        cluster_body = self.director.build_small_cluster(
            name=cluster_name,
            k8s_namespace=namespace,
            labels={"ray.io/cluster": cluster_name},
        )

        created_cluster = self.cluster_api.create_ray_cluster(
            body=cluster_body, k8s_namespace=namespace
        )
        self.assertIsNotNone(created_cluster, "Cluster should be created successfully")

        self.cluster_api.wait_until_ray_cluster_running(cluster_name, namespace, 60, 10)

        job_name = "delete-test-job"
        try:
            job_body = create_job_with_cluster_selector(
                job_name,
                namespace,
                cluster_name,
            )

            submitted_job = self.api.submit_job(
                job=job_body,
                k8s_namespace=namespace,
            )
            self.assertIsNotNone(submitted_job, "Job should be submitted successfully")

            self.api.wait_until_job_finished(job_name, namespace, 60, 5)

            delete_result = self.api.delete_job(job_name, namespace)
            self.assertTrue(delete_result, "Job should be deleted successfully")

        finally:
            self.cluster_api.delete_ray_cluster(
                name=cluster_name, k8s_namespace=namespace
            )

    def test_get_job_status_nonexistent_job(self):
        """Test getting status for a non-existent job."""
        status = self.api.get_job_status(
            "nonexistent-job", namespace, timeout=2, delay_between_attempts=2
        )
        self.assertIsNone(status, "Status should be None for non-existent job")

    def test_wait_until_job_finished_nonexistent_job(self):
        """Test waiting for completion of a non-existent job."""
        result = self.api.wait_until_job_finished(
            "nonexistent-job", namespace, timeout=2, delay_between_attempts=2
        )
        self.assertFalse(result, "Should return False for non-existent job")

    def test_delete_job_nonexistent_job(self):
        """Test deleting a non-existent job."""
        result = self.api.delete_job("nonexistent-job", namespace)
        self.assertFalse(result, "Should return False for non-existent job")

    def test_submit_job_invalid_spec(self):
        """Test submitting a job with invalid specification."""
        invalid_job = {
            "apiVersion": "invalid/version",
            "kind": "InvalidKind",
            "metadata": {
                "name": "invalid-job",
                "namespace": namespace,
            },
            "spec": {
                "invalidField": "invalidValue",
            },
        }

        result = self.api.submit_job(job=invalid_job, k8s_namespace=namespace)
        self.assertIsNone(result, "Should return None for invalid job specification")

    def test_submit_job_with_ray_cluster_spec(self):
        """Test submitting a job with rayClusterSpec - KubeRay will create and manage the cluster lifecycle."""
        job_name = "cluster-spec-job"

        try:
            job_body = create_job_with_ray_cluster_spec(
                job_name=job_name,
                namespace=namespace,
            )

            submitted_job = self.api.submit_job(
                job=job_body,
                k8s_namespace=namespace,
            )

            self.assertIsNotNone(submitted_job, "Job should be submitted successfully")
            self.assertEqual(submitted_job["metadata"]["name"], job_name)

            # Verify rayClusterSpec structure
            self.assertIn(
                "rayClusterSpec",
                submitted_job["spec"],
                "Job should have rayClusterSpec",
            )
            self.assertIn(
                "headGroupSpec",
                submitted_job["spec"]["rayClusterSpec"],
                "rayClusterSpec should have headGroupSpec",
            )
            self.assertIn(
                "workerGroupSpecs",
                submitted_job["spec"]["rayClusterSpec"],
                "rayClusterSpec should have workerGroupSpecs",
            )

            result = self.api.wait_until_job_finished(job_name, namespace, 300, 10)
            self.assertTrue(result, "Job should complete successfully within timeout")

            final_status = self.api.get_job_status(
                job_name, namespace, timeout=10, delay_between_attempts=1
            )
            self.assertIsNotNone(final_status, "Final job status should be retrieved")
            self.assertIn(
                "jobDeploymentStatus",
                final_status,
                "Final status should contain jobDeploymentStatus field",
            )

        finally:
            self.api.delete_job(job_name, namespace)

    def test_suspend_job(self):
        """Test stopping a running job."""
        job_name = "stop-test-job"

        try:
            job_body = create_job_with_ray_cluster_spec(
                job_name=job_name,
                namespace=namespace,
            )

            submitted_job = self.api.submit_job(
                job=job_body,
                k8s_namespace=namespace,
            )
            self.assertIsNotNone(submitted_job, "Job should be submitted successfully")

            result = self.api.wait_until_job_running(
                job_name, namespace, timeout=120, delay_between_attempts=5
            )
            self.assertTrue(result, "Job should reach running state before suspension")

            stop_result = self.api.suspend_job(job_name, namespace)
            self.assertTrue(stop_result, "Job should be suspended successfully")

            suspended = self.wait_for_job_status(job_name, namespace, "Suspended", timeout=30)
            self.assertTrue(suspended, "Job deployment status should be Suspended")

        finally:
            self.api.delete_job(job_name, namespace)

    def test_resubmit_job(self):
        """Test resubmitting a suspended job."""
        job_name = "resubmit-test-job"

        try:
            job_body = create_job_with_ray_cluster_spec(
                job_name=job_name,
                namespace=namespace,
            )

            submitted_job = self.api.submit_job(
                job=job_body,
                k8s_namespace=namespace,
            )
            self.assertIsNotNone(submitted_job, "Job should be submitted successfully")

            result = self.api.wait_until_job_running(
                job_name, namespace, timeout=120, delay_between_attempts=5
            )
            self.assertTrue(result, "Job should reach running state before suspension")

            stop_result = self.api.suspend_job(job_name, namespace)
            self.assertTrue(stop_result, "Job should be suspended successfully")

            suspended = self.wait_for_job_status(job_name, namespace, "Suspended", timeout=30)
            self.assertTrue(suspended, "Job should be in Suspended status before resubmission")

            resubmit_result = self.api.resubmit_job(job_name, namespace)
            self.assertTrue(resubmit_result, "Job should be resubmitted successfully")

            result = self.api.wait_until_job_finished(
                job_name, namespace, timeout=120, delay_between_attempts=5
            )
            self.assertTrue(result, "Resubmitted job should complete successfully")

        finally:
            self.api.delete_job(job_name, namespace)

    def test_stop_and_resubmit_job(self):
        """Test the full stop and resubmit cycle."""
        job_name = "stop-resubmit-job"

        try:
            job_body = create_job_with_ray_cluster_spec(
                job_name=job_name,
                namespace=namespace,
            )

            submitted_job = self.api.submit_job(
                job=job_body,
                k8s_namespace=namespace,
            )
            self.assertIsNotNone(submitted_job, "Job should be submitted successfully")

            result = self.api.wait_until_job_running(
                job_name, namespace, timeout=120, delay_between_attempts=5
            )
            self.assertTrue(result, "Job should reach running state before suspension, completion, or failure")

            stop_result = self.api.suspend_job(job_name, namespace)
            self.assertTrue(stop_result, "Job should be suspended successfully")

            suspended = self.wait_for_job_status(job_name, namespace, "Suspended", timeout=30)
            self.assertTrue(suspended, "Job should reach Suspended status within 30 seconds")

            resubmit_result = self.api.resubmit_job(job_name, namespace)
            self.assertTrue(resubmit_result, "Job should be resubmitted successfully")

            result = self.api.wait_until_job_finished(
                job_name, namespace, timeout=120, delay_between_attempts=5
            )
            self.assertTrue(result, "Resubmitted job should complete successfully")

        finally:
            self.api.delete_job(job_name, namespace)

    def test_suspend_job_nonexistent(self):
        """Test stopping a non-existent job."""
        result = self.api.suspend_job("nonexistent-job", namespace)
        self.assertFalse(result, "Should return False for non-existent job")

    def test_resubmit_job_nonexistent(self):
        """Test resubmitting a non-existent job."""
        result = self.api.resubmit_job("nonexistent-job", namespace)
        self.assertFalse(result, "Should return False for non-existent job")

    def test_wait_until_job_running(self):
        """Test waiting for a job to reach running state."""
        job_name = "wait-running-test-job"

        try:
            job_body = create_job_with_ray_cluster_spec(
                job_name=job_name,
                namespace=namespace,
            )

            submitted_job = self.api.submit_job(
                job=job_body,
                k8s_namespace=namespace,
            )
            self.assertIsNotNone(submitted_job, "Job should be submitted successfully")

            result = self.api.wait_until_job_running(
                job_name, namespace, timeout=60, delay_between_attempts=3
            )
            self.assertTrue(result, "Job should reach running state")

            self.api.wait_until_job_finished(job_name, namespace, 60, 5)

        finally:
            self.api.delete_job(job_name, namespace)

    def test_get_job(self):
        """Test getting a job."""
        job_name = "get-test-job"

        try:
            job_body = create_job_with_ray_cluster_spec(
                job_name=job_name,
                namespace=namespace,
            )

            submitted_job = self.api.submit_job(
                job=job_body,
                k8s_namespace=namespace,
            )
            self.assertIsNotNone(submitted_job, "Job should be submitted successfully")

            status = self.api.get_job_status(
                job_name, namespace, timeout=30, delay_between_attempts=2
            )
            self.assertIsNotNone(status, "Job status should be available")

            job = self.api.get_job(job_name, namespace)
            self.assertIsNotNone(job, "Job should be retrieved successfully")
            self.assertEqual(job["metadata"]["name"], job_name)
        finally:
            self.api.delete_job(job_name, namespace)

    def test_list_jobs(self):
        """Test listing all jobs in a namespace."""
        created_jobs = []

        try:
            initial_result = self.api.list_jobs(k8s_namespace=namespace)
            self.assertIsNotNone(initial_result, "List jobs should return a result")
            self.assertIn("items", initial_result, "Result should contain 'items' field")
            initial_count = len(initial_result.get("items", []))

            test_jobs = [
                {
                    "name": "list-test-job-1",
                    "type": "cluster_spec"
                },
                {
                    "name": "list-test-job-2",
                    "type": "cluster_spec"
                },
                {
                    "name": "list-test-job-3",
                    "type": "cluster_spec"
                }
            ]

            for job_info in test_jobs:
                job_body = create_job_with_ray_cluster_spec(
                    job_name=job_info["name"],
                    namespace=namespace,
                )

                submitted_job = self.api.submit_job(
                    job=job_body,
                    k8s_namespace=namespace,
                )
                self.assertIsNotNone(submitted_job, f"Job {job_info['name']} should be submitted successfully")
                created_jobs.append(job_info["name"])

                status = self.api.get_job_status(
                    job_info["name"], namespace, timeout=10, delay_between_attempts=1
                )
                self.assertIsNotNone(status, f"Job {job_info['name']} status should be available")

            result = self.api.list_jobs(k8s_namespace=namespace)
            self.assertIsNotNone(result, "List jobs should return a result")
            self.assertIn("items", result, "Result should contain 'items' field")

            items = result.get("items", [])
            current_count = len(items)

            self.assertGreaterEqual(
                current_count,
                initial_count + len(test_jobs),
                f"Should have at least {len(test_jobs)} more jobs than initially"
            )

            job_names_in_list = [item.get("metadata", {}).get("name") for item in items]
            for job_name in created_jobs:
                self.assertIn(
                    job_name,
                    job_names_in_list,
                    f"Job {job_name} should be in the list"
                )

        finally:
            for job_name in created_jobs:
                try:
                    self.api.delete_job(job_name, namespace)
                except Exception as e:
                    print(f"Failed to delete job {job_name}: {e}")

    def wait_for_job_status(
        self, job_name, namespace, expected_status, timeout=60, check_interval=3
    ):
        """Wait for a job to reach a specific status with polling."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self.api.get_job_status(
                job_name, namespace, timeout=5, delay_between_attempts=1
            )
            current_status = status.get("jobDeploymentStatus") if status else None

            if current_status == expected_status:
                return True

            time.sleep(check_interval)

        return False
