from pathlib import Path
import unittest

from ci.flaky_tracker.parser import FailureKind, TestStatus, parse_go_test_log


FIXTURES = Path(__file__).with_name("fixtures")


class GoTestLogParserTest(unittest.TestCase):
    def test_parses_kuberay_formatted_parent_and_subtest_failures(self):
        result = parse_go_test_log(
            (FIXTURES / "formatted_failure.txt").read_text(encoding="utf-8")
        )

        self.assertEqual(2, len(result.test_failures))
        parent, subtest = result.test_failures
        self.assertEqual("TestRayClusterAutoscalerV2IdleTimeout", parent.test_name)
        self.assertFalse(parent.is_subtest)
        self.assertEqual("84.30s", parent.duration)
        self.assertEqual(FailureKind.TEST, parent.kind)
        self.assertEqual(
            "github.com/ray-project/kuberay/ray-operator/test/e2eautoscaler",
            parent.package,
        )
        self.assertEqual(
            "TestRayClusterAutoscalerV2IdleTimeout/"
            "Create_a_RayCluster_with_autoscaler_v2_enabled",
            subtest.test_name,
        )
        self.assertTrue(subtest.is_subtest)
        self.assertFalse(result.package_failures)

    def test_parses_unformatted_go_failure_marker(self):
        result = parse_go_test_log(
            "\n".join(
                [
                    "=== RUN   TestCreateRayJob",
                    "--- FAIL: TestCreateRayJob (1.25s)",
                    "FAIL\texample.com/kuberay/test/e2erayjob\t1.300s",
                ]
            )
        )

        self.assertEqual(1, len(result.test_failures))
        failure = result.test_failures[0]
        self.assertEqual("TestCreateRayJob", failure.test_name)
        self.assertEqual("example.com/kuberay/test/e2erayjob", failure.package)
        self.assertEqual("1.25s", failure.duration)

    def test_parses_formatted_pass_and_skip_results(self):
        result = parse_go_test_log(
            (FIXTURES / "formatted_results.txt").read_text(encoding="utf-8")
        )

        self.assertEqual(3, len(result.test_results))
        self.assertEqual(2, len(result.test_passes))
        self.assertEqual(1, len(result.test_skips))
        self.assertFalse(result.test_failures)
        self.assertEqual(
            [TestStatus.PASS, TestStatus.SKIP, TestStatus.PASS],
            [test_result.status for test_result in result.test_results],
        )
        self.assertEqual(
            "github.com/ray-project/kuberay/ray-operator/test/e2erayjob",
            result.test_results[0].package,
        )

    def test_parses_build_and_setup_failures(self):
        result = parse_go_test_log(
            (FIXTURES / "package_failures.txt").read_text(encoding="utf-8")
        )

        self.assertFalse(result.test_failures)
        self.assertEqual(
            [FailureKind.BUILD, FailureKind.SETUP],
            [failure.kind for failure in result.package_failures],
        )
        self.assertEqual(
            "github.com/ray-project/kuberay/ray-operator/controllers/ray",
            result.package_failures[0].package,
        )

    def test_parses_test_timeout(self):
        result = parse_go_test_log(
            (FIXTURES / "timeout.txt").read_text(encoding="utf-8")
        )

        self.assertEqual(1, len(result.test_failures))
        failure = result.test_failures[0]
        self.assertEqual("TestRayServiceUpgrade", failure.test_name)
        self.assertEqual(FailureKind.TIMEOUT, failure.kind)
        self.assertEqual("30m0s", failure.duration)
        self.assertEqual(
            "github.com/ray-project/kuberay/ray-operator/test/e2erayservice",
            failure.package,
        )
        self.assertFalse(result.package_failures)

    def test_preserves_failure_when_package_summary_is_missing(self):
        result = parse_go_test_log("### FAIL: TestTruncatedLog (2.00s)\n")

        self.assertEqual(1, len(result.test_failures))
        self.assertIsNone(result.test_failures[0].package)

    def test_strips_ansi_and_buildkite_control_sequences(self):
        log = (
            "\x1b[31m### FAIL: TestColored (0.10s)\x1b[0m\n"
            "\x1b_bk;t=1710000000\x07"
            "FAIL example.com/colored 0.20s\n"
        )

        result = parse_go_test_log(log)

        self.assertEqual(1, len(result.test_failures))
        self.assertEqual("example.com/colored", result.test_failures[0].package)

    def test_deduplicates_timeout_and_failure_marker(self):
        log = "\n".join(
            [
                "panic: test timed out after 10m0s",
                "running tests:",
                "    TestSlow (10m0s)",
                "--- FAIL: TestSlow (600.00s)",
                "FAIL example.com/slow 600.01s",
            ]
        )

        result = parse_go_test_log(log)

        self.assertEqual(1, len(result.test_failures))
        self.assertEqual(FailureKind.TIMEOUT, result.test_failures[0].kind)

    def test_preserves_repeated_test_executions(self):
        log = "\n".join(
            [
                "--- PASS: TestRepeated (0.01s)",
                "--- FAIL: TestRepeated (0.02s)",
                "FAIL example.com/repeated 0.04s",
            ]
        )

        result = parse_go_test_log(log)

        self.assertEqual(2, len(result.test_results))
        self.assertEqual(
            [TestStatus.PASS, TestStatus.FAIL],
            [test_result.status for test_result in result.test_results],
        )

    def test_records_unattributed_package_failure(self):
        result = parse_go_test_log("FAIL example.com/unknown 0.01s\n")

        self.assertTrue(result.has_failures)
        self.assertEqual(FailureKind.UNKNOWN, result.package_failures[0].kind)

    def test_ignores_passing_output_and_failure_text_inside_messages(self):
        result = parse_go_test_log(
            "\n".join(
                [
                    "=== RUN   TestHealthy",
                    "logger: --- FAIL: this is application text",
                    "--- PASS: TestHealthy (0.01s)",
                    "PASS",
                    "ok example.com/healthy 0.02s",
                ]
            )
        )

        self.assertFalse(result.has_failures)


if __name__ == "__main__":
    unittest.main()
