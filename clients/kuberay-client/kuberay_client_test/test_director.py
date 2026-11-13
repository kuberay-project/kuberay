import unittest
from kuberay_client.utils import kuberay_cluster_builder


class TestDirector(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.director = kuberay_cluster_builder.Director()

    def test_build_basic_cluster(self):
        cluster = self.director.build_basic_cluster(name="basic-cluster")
        # testing meta
        actual = cluster["metadata"]["name"]
        expected = "basic-cluster"
        self.assertEqual(actual, expected)

        actual = cluster["metadata"]["namespace"]
        expected = "default"
        self.assertEqual(actual, expected)

        # testing the head pod
        actual = cluster["spec"]["headGroupSpec"]["template"]["spec"]["containers"][0][
            "resources"
        ]["requests"]["cpu"]
        expected = "2"
        self.assertEqual(actual, expected)


    def test_build_small_cluster(self):
        cluster = self.director.build_small_cluster(name="small-cluster")
        # testing meta
        actual = cluster["metadata"]["name"]
        expected = "small-cluster"
        self.assertEqual(actual, expected)

        actual = cluster["metadata"]["namespace"]
        expected = "default"
        self.assertEqual(actual, expected)

        # testing the head pod
        actual = cluster["spec"]["headGroupSpec"]["template"]["spec"]["containers"][0][
            "resources"
        ]["requests"]["cpu"]
        expected = "2"
        self.assertEqual(actual, expected)

        # testing the workergroup
        actual = cluster["spec"]["workerGroupSpecs"][0]["replicas"]
        expected = 1
        self.assertEqual(actual, expected)

        actual = cluster["spec"]["workerGroupSpecs"][0]["template"]["spec"][
            "containers"
        ][0]["resources"]["requests"]["cpu"]
        expected = "1"
        self.assertEqual(actual, expected)

    def test_build_medium_cluster(self):
        cluster = self.director.build_medium_cluster(name="medium-cluster")
        # testing meta
        actual = cluster["metadata"]["name"]
        expected = "medium-cluster"
        self.assertEqual(actual, expected)

        actual = cluster["metadata"]["namespace"]
        expected = "default"
        self.assertEqual(actual, expected)

        # testing the head pod
        actual = cluster["spec"]["headGroupSpec"]["template"]["spec"]["containers"][0][
            "resources"
        ]["requests"]["cpu"]
        expected = "2"
        self.assertEqual(actual, expected)

        # testing the workergroup
        actual = cluster["spec"]["workerGroupSpecs"][0]["replicas"]
        expected = 3
        self.assertEqual(actual, expected)

        actual = cluster["spec"]["workerGroupSpecs"][0]["groupName"]
        expected = "medium-cluster-workers"
        self.assertEqual(actual, expected)

        actual = cluster["spec"]["workerGroupSpecs"][0]["template"]["spec"][
            "containers"
        ][0]["resources"]["requests"]["cpu"]
        expected = "2"
        self.assertEqual(actual, expected)

    def test_build_large_cluster(self):
        cluster = self.director.build_large_cluster(name="large-cluster")
        # testing meta
        actual = cluster["metadata"]["name"]
        expected = "large-cluster"
        self.assertEqual(actual, expected)

        actual = cluster["metadata"]["namespace"]
        expected = "default"
        self.assertEqual(actual, expected)

        # testing the head pod
        actual = cluster["spec"]["headGroupSpec"]["template"]["spec"]["containers"][0][
            "resources"
        ]["requests"]["cpu"]
        expected = "2"
        self.assertEqual(actual, expected)

        # testing the workergroup
        actual = cluster["spec"]["workerGroupSpecs"][0]["replicas"]
        expected = 6
        self.assertEqual(actual, expected)

        actual = cluster["spec"]["workerGroupSpecs"][0]["groupName"]
        expected = "large-cluster-workers"
        self.assertEqual(actual, expected)

        actual = cluster["spec"]["workerGroupSpecs"][0]["template"]["spec"][
            "containers"
        ][0]["resources"]["requests"]["cpu"]
        expected = "3"
        self.assertEqual(actual, expected)


class TestClusterBuilder(unittest.TestCase):
    def test_build_worker_without_meta_reports_failure(self):
        builder = kuberay_cluster_builder.ClusterBuilder()

        with self.assertLogs(kuberay_cluster_builder.log, level="ERROR") as logs:
            builder.build_worker(group_name="workers")

        self.assertFalse(builder.succeeded)
        self.assertIn(
            "call build_meta() and build_head() before build_worker()", logs.output[0]
        )

    def test_build_worker_with_metadata_but_no_spec_reports_failure(self):
        # build_meta() also adds the spec, so set up a cluster that has metadata only.
        builder = kuberay_cluster_builder.ClusterBuilder()
        builder.cluster = {"metadata": {"name": "raycluster"}}

        with self.assertLogs(kuberay_cluster_builder.log, level="ERROR") as logs:
            builder.build_worker(group_name="workers")

        self.assertFalse(builder.succeeded)
        self.assertIn("'name': 'raycluster'", logs.output[0])
        self.assertIn(
            "the spec section is missing, did you run build_head()?", logs.output[0]
        )
