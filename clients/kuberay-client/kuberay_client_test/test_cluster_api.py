import unittest
from kuberay_client import kuberay_cluster_api, constants
from kuberay_client.utils import kuberay_cluster_builder


# Keep the original test cluster body for reference if needed
test_cluster_body: dict = {
    "apiVersion": "ray.io/v1",
    "kind": "RayCluster",
    "metadata": {
        "labels": {"controller-tools.k8s.io": "1.0"},
        "name": "raycluster-complete-raw",
    },
    "spec": {
        "rayVersion": "2.46.0",
        "headGroupSpec": {
            "rayStartParams": {"dashboard-host": "0.0.0.0"},
            "template": {
                "metadata": {"labels": {}},
                "spec": {
                    "containers": [
                        {
                            "name": "ray-head",
                            "image": "rayproject/ray:2.46.0",
                            "ports": [
                                {"containerPort": 6379, "name": "gcs"},
                                {"containerPort": 8265, "name": "dashboard"},
                                {"containerPort": 10001, "name": "client"},
                            ],
                            "lifecycle": {
                                "preStop": {
                                    "exec": {"command": ["/bin/sh", "-c", "ray stop"]}
                                }
                            },
                            "volumeMounts": [
                                {"mountPath": "/tmp/ray", "name": "ray-logs"}
                            ],
                            "resources": {
                                "limits": {"cpu": "1", "memory": "2G"},
                                "requests": {"cpu": "500m", "memory": "2G"},
                            },
                        }
                    ],
                    "volumes": [{"name": "ray-logs", "emptyDir": {}}],
                },
            },
        },
        "workerGroupSpecs": [
            {
                "replicas": 1,
                "minReplicas": 1,
                "maxReplicas": 10,
                "groupName": "small-group",
                "rayStartParams": {},
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "name": "ray-worker",
                                "image": "rayproject/ray:2.46.0",
                                "lifecycle": {
                                    "preStop": {
                                        "exec": {
                                            "command": ["/bin/sh", "-c", "ray stop"]
                                        }
                                    }
                                },
                                "volumeMounts": [
                                    {"mountPath": "/tmp/ray", "name": "ray-logs"}
                                ],
                                "resources": {
                                    "limits": {"cpu": "1", "memory": "1G"},
                                    "requests": {"cpu": "500m", "memory": "1G"},
                                },
                            },
                            {
                                "name": "side-car",
                                "image": "rayproject/ray:2.46.0",
                                "resources": {
                                    "limits": {"cpu": "1", "memory": "1G"},
                                    "requests": {"cpu": "500m", "memory": "1G"},
                                },
                            }
                        ],
                        "volumes": [{"name": "ray-logs", "emptyDir": {}}],
                    }
                },
            }
        ],
    },
    "status": {
        "state": "ready",
        "availableWorkerReplicas": 2,
        "desiredWorkerReplicas": 1,
        "endpoints": {
            "client": "10001",
            "dashboard": "8265",
            "gcs-server": "6379"
        },
        "head": {
            "serviceIP": "10.152.183.194"
        },
        "lastUpdateTime": "2023-02-16T05:15:17Z",
        "maxWorkerReplicas": 2
    }
}

class TestClusterApi(unittest.TestCase):
    """Comprehensive test suite for RayClusterApi functionality."""

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.api = kuberay_cluster_api.RayClusterApi()
        self.director = kuberay_cluster_builder.Director()


    def test_create_and_get_ray_cluster(self):
        """Test creating a cluster and retrieving it."""
        cluster_name = "test-create-cluster"
        namespace = "default"

        # Build a small cluster using the director
        cluster_body = self.director.build_small_cluster(
            name=cluster_name,
            k8s_namespace=namespace,
            labels={"test": "create-cluster"},
        )

        # Ensure cluster was built successfully
        self.assertIsNotNone(cluster_body, "Cluster should be built successfully")
        self.assertEqual(cluster_body["metadata"]["name"], cluster_name)

        try:
            # Create the cluster
            created_cluster = self.api.create_ray_cluster(
                body=cluster_body, k8s_namespace=namespace
            )
            self.assertIsNotNone(created_cluster, "Cluster should be created successfully")
            self.assertEqual(created_cluster["metadata"]["name"], cluster_name)

            # Get the cluster and verify it exists
            retrieved_cluster = self.api.get_ray_cluster(
                name=cluster_name, k8s_namespace=namespace
            )
            self.assertIsNotNone(retrieved_cluster, "Cluster should be retrieved successfully")
            self.assertEqual(retrieved_cluster["metadata"]["name"], cluster_name)
            self.assertEqual(retrieved_cluster["spec"]["rayVersion"], cluster_body["spec"]["rayVersion"])

        finally:
            # Clean up
            self.api.delete_ray_cluster(name=cluster_name, k8s_namespace=namespace)

    def test_list_ray_clusters(self):
        """Test listing Ray clusters in a namespace."""
        cluster_name_1 = "test-list-cluster-1"
        cluster_name_2 = "test-list-cluster-2"
        namespace = "default"
        test_label = "test-list-clusters"

        # Build two small clusters
        cluster_body_1 = self.director.build_small_cluster(
            name=cluster_name_1,
            k8s_namespace=namespace,
            labels={"test": test_label},
        )
        cluster_body_2 = self.director.build_small_cluster(
            name=cluster_name_2,
            k8s_namespace=namespace,
            labels={"test": test_label},
        )

        try:
            # Create both clusters
            created_cluster_1 = self.api.create_ray_cluster(
                body=cluster_body_1, k8s_namespace=namespace
            )
            created_cluster_2 = self.api.create_ray_cluster(
                body=cluster_body_2, k8s_namespace=namespace
            )

            self.assertIsNotNone(created_cluster_1, "First cluster should be created")
            self.assertIsNotNone(created_cluster_2, "Second cluster should be created")

            # List all clusters
            clusters_list = self.api.list_ray_clusters(k8s_namespace=namespace)
            self.assertIsNotNone(clusters_list, "Should be able to list clusters")
            self.assertIn("items", clusters_list, "Response should contain items")

            # Verify our test clusters are in the list
            cluster_names = [item["metadata"]["name"] for item in clusters_list["items"]]
            self.assertIn(cluster_name_1, cluster_names, "First test cluster should be in the list")
            self.assertIn(cluster_name_2, cluster_names, "Second test cluster should be in the list")

            # Test listing with label selector
            labeled_clusters = self.api.list_ray_clusters(
                k8s_namespace=namespace,
                label_selector=f"test={test_label}"
            )
            self.assertIsNotNone(labeled_clusters, "Should be able to list clusters with label selector")
            labeled_cluster_names = [item["metadata"]["name"] for item in labeled_clusters["items"]]
            self.assertIn(cluster_name_1, labeled_cluster_names, "First test cluster should match label")
            self.assertIn(cluster_name_2, labeled_cluster_names, "Second test cluster should match label")

        finally:
            # Clean up both clusters
            self.api.delete_ray_cluster(name=cluster_name_1, k8s_namespace=namespace)
            self.api.delete_ray_cluster(name=cluster_name_2, k8s_namespace=namespace)

    def test_cluster_status_and_wait_until_running(self):
        """Test getting cluster status and waiting for cluster to be ready."""
        cluster_name = "test-status-cluster"
        namespace = "default"

        # Build a small cluster
        cluster_body = self.director.build_small_cluster(
            name=cluster_name,
            k8s_namespace=namespace,
            labels={"test": "status-cluster"},
        )

        try:
            # Create the cluster
            created_cluster = self.api.create_ray_cluster(
                body=cluster_body, k8s_namespace=namespace
            )
            self.assertIsNotNone(created_cluster, "Cluster should be created successfully")

            # Test getting cluster status (may take some time to populate)
            status = self.api.get_ray_cluster_status(
                name=cluster_name,
                k8s_namespace=namespace,
                timeout=120,
                delay_between_attempts=5
            )
            self.assertIsNotNone(status, "Cluster status should be retrieved")

            # Test waiting for cluster to be running
            is_running = self.api.wait_until_ray_cluster_running(
                name=cluster_name,
                k8s_namespace=namespace,
                timeout=180,
                delay_between_attempts=10
            )
            self.assertTrue(is_running, "Cluster should become ready within timeout")

            # Verify final status after cluster is ready
            final_status = self.api.get_ray_cluster_status(
                name=cluster_name,
                k8s_namespace=namespace,
                timeout=10,
                delay_between_attempts=2
            )
            self.assertIsNotNone(final_status, "Final status should be available")
            self.assertIn("state", final_status, "Status should contain state field")
            self.assertEqual(final_status["state"], "ready", "Cluster should be in ready state")

        finally:
            # Clean up
            self.api.delete_ray_cluster(name=cluster_name, k8s_namespace=namespace)

    def test_patch_ray_cluster(self):
        """Test patching an existing Ray cluster."""
        cluster_name = "test-patch-cluster"
        namespace = "default"

        # Build a small cluster
        cluster_body = self.director.build_small_cluster(
            name=cluster_name,
            k8s_namespace=namespace,
            labels={"test": "patch-cluster"},
        )

        try:
            # Create the cluster
            created_cluster = self.api.create_ray_cluster(
                body=cluster_body, k8s_namespace=namespace
            )
            self.assertIsNotNone(created_cluster, "Cluster should be created successfully")

            # Wait for cluster to be ready before patching
            self.api.wait_until_ray_cluster_running(
                name=cluster_name, k8s_namespace=namespace, timeout=180, delay_between_attempts=10
            )

            # Create a patch to update the cluster (e.g., add a label)
            patch_data = {
                "metadata": {
                    "labels": {
                        "test": "patch-cluster",
                        "patched": "true"
                    }
                }
            }

            # Apply the patch
            patch_result = self.api.patch_ray_cluster(
                name=cluster_name,
                ray_patch=patch_data,
                k8s_namespace=namespace
            )
            self.assertTrue(patch_result, "Patch operation should succeed")

            # Verify the patch was applied
            updated_cluster = self.api.get_ray_cluster(
                name=cluster_name, k8s_namespace=namespace
            )
            self.assertIsNotNone(updated_cluster, "Updated cluster should be retrieved")
            self.assertIn("patched", updated_cluster["metadata"]["labels"], "Patched label should be present")
            self.assertEqual(updated_cluster["metadata"]["labels"]["patched"], "true", "Patched label should have correct value")

        finally:
            # Clean up
            self.api.delete_ray_cluster(name=cluster_name, k8s_namespace=namespace)
