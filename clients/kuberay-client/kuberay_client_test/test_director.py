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
