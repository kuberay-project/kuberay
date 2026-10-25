import io
import json
import unittest
from http.client import IncompleteRead
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request

from ci.flaky_tracker.buildkite import (
    BuildkiteAPIError,
    BuildkiteResponseError,
    BuildkiteTestEngineClient,
    ExecutionCounts,
    _SameOriginRedirectHandler,
    TESTS_API_VERSION,
)


def _test_payload(test_id="test-1", name="TestRayServiceUpgrade"):
    return {
        "id": test_id,
        "url": f"https://api.buildkite.test/tests/{test_id}",
        "web_url": f"https://buildkite.test/tests/{test_id}",
        "scope": "github.com/ray-project/kuberay/ray-operator/test/e2e",
        "name": name,
        "location": "rayservice_upgrade_test.go:42",
        "file_name": "rayservice_upgrade_test.go",
        "labels": ["flaky"],
        "reliability": 0.8,
        "duration_avg": 1.5,
        "duration_sum": 15.0,
        "duration_min": 1.0,
        "duration_max": 2.0,
        "executions_count": 10,
        "executions_count_by_result": {
            "passed": 8,
            "failed": 2,
            "skipped": 1,
        },
    }


class _FakeResponse:
    def __init__(self, payload, *, headers=None, raw=False):
        self._body = payload if raw else json.dumps(payload).encode("utf-8")
        self.headers = headers or {}

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class _ReadErrorResponse(_FakeResponse):
    def read(self):
        raise IncompleteRead(b"partial response")


class _ReadErrorFile:
    def read(self, amount=None):
        raise IncompleteRead(b"partial error response")

    def close(self):
        pass


class _FakeOpener:
    def __init__(self, *responses):
        self._responses = list(responses)
        self.calls = []

    def __call__(self, request, *, timeout):
        self.calls.append((request, timeout))
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _headers(request):
    return {name.lower(): value for name, value in request.header_items()}


class BuildkiteTestEngineClientTest(unittest.TestCase):
    def test_lists_tests_with_filters_headers_and_metrics(self):
        opener = _FakeOpener(_FakeResponse([_test_payload()]))
        client = BuildkiteTestEngineClient(
            "secret-token",
            "ray project",
            "KubeRay/E2E",
            base_url="https://api.buildkite.test/v2",
            timeout=12.5,
            opener=opener,
        )

        tests = client.list_tests(
            labels=("flaky", "slow"),
            branch="master/next",
            period="28days",
            tags=("result:~failed",),
            sort_by="reliability",
            order="asc",
        )

        self.assertEqual(1, len(tests))
        test = tests[0]
        self.assertEqual("TestRayServiceUpgrade", test.name)
        self.assertEqual(0.8, test.reliability)
        self.assertEqual(
            ExecutionCounts(passed=8, failed=2, skipped=1),
            test.executions_count_by_result,
        )

        request, timeout = opener.calls[0]
        self.assertEqual(12.5, timeout)
        parsed_url = urlsplit(request.full_url)
        self.assertEqual(
            "/v2/analytics/organizations/ray%20project/"
            "suites/KubeRay%2FE2E/tests",
            parsed_url.path,
        )
        self.assertEqual(
            {
                "labels": ["flaky,slow"],
                "branch": ["master/next"],
                "period": ["28days"],
                "tags": ["result:~failed"],
                "sort_by": ["reliability"],
                "order": ["asc"],
                "per_page": ["100"],
            },
            parse_qs(parsed_url.query),
        )
        headers = _headers(request)
        self.assertEqual("Bearer secret-token", headers["authorization"])
        self.assertEqual(TESTS_API_VERSION, headers["buildkite-version"])
        self.assertEqual("application/json", headers["accept"])
        self.assertEqual("kuberay-flaky-tracker", headers["user-agent"])

    def test_lists_tests_from_every_page(self):
        next_url = (
            "https://api.buildkite.test/v2/analytics/organizations/ray-project/"
            "suites/kuberay/tests?page=2&per_page=100"
        )
        opener = _FakeOpener(
            _FakeResponse(
                [_test_payload("test-1", "TestOne")],
                headers={
                    "Link": (
                        f'<{next_url}>; rel="next", '
                        f'<{next_url}>; rel="last"'
                    )
                },
            ),
            _FakeResponse([_test_payload("test-2", "TestTwo")]),
        )
        client = BuildkiteTestEngineClient(
            "token",
            "ray-project",
            "kuberay",
            base_url="https://api.buildkite.test/v2",
            opener=opener,
        )

        tests = client.list_tests(branch="master")

        self.assertEqual(["TestOne", "TestTwo"], [test.name for test in tests])
        self.assertEqual(2, len(opener.calls))
        self.assertEqual(next_url, opener.calls[1][0].full_url)
        self.assertEqual(
            "Bearer token", _headers(opener.calls[1][0])["authorization"]
        )

    def test_lists_only_tests_already_labeled_flaky(self):
        opener = _FakeOpener(_FakeResponse([]))
        client = BuildkiteTestEngineClient(
            "token",
            "ray-project",
            "kuberay",
            base_url="https://api.buildkite.test/v2",
            opener=opener,
        )

        self.assertEqual((), client.list_flaky_tests(period="7days"))

        query = parse_qs(urlsplit(opener.calls[0][0].full_url).query)
        self.assertEqual(["flaky"], query["labels"])
        self.assertEqual(["master"], query["branch"])
        self.assertEqual(["7days"], query["period"])

    def test_defaults_optional_result_counts_to_zero(self):
        payload = _test_payload()
        payload["reliability"] = None
        payload["location"] = None
        payload["file_name"] = None
        payload["executions_count_by_result"] = {"passed": 0, "failed": 3}
        opener = _FakeOpener(_FakeResponse([payload]))
        client = BuildkiteTestEngineClient(
            "token",
            "ray-project",
            "kuberay",
            base_url="https://api.buildkite.test/v2",
            opener=opener,
        )

        test = client.list_tests()[0]

        self.assertIsNone(test.reliability)
        self.assertIsNone(test.location)
        self.assertEqual(
            ExecutionCounts(passed=0, failed=3),
            test.executions_count_by_result,
        )

    def test_reports_http_error_message_and_retry_hint(self):
        error = HTTPError(
            "https://api.buildkite.test/tests",
            429,
            "Too Many Requests",
            {"Retry-After": "60"},
            io.BytesIO(b'{"message":"rate limit exceeded"}'),
        )
        opener = _FakeOpener(error)
        client = BuildkiteTestEngineClient(
            "token",
            "ray-project",
            "kuberay",
            base_url="https://api.buildkite.test/v2",
            opener=opener,
        )

        with self.assertRaises(BuildkiteAPIError) as context:
            client.list_tests()

        self.assertEqual(429, context.exception.status_code)
        self.assertEqual("60", context.exception.retry_after)
        self.assertIn("rate limit exceeded", str(context.exception))
        self.assertNotIn("token", str(context.exception))

    def test_reports_truncated_http_error_body_as_api_error(self):
        error = HTTPError(
            "https://api.buildkite.test/tests",
            502,
            "Bad Gateway",
            {"Retry-After": "10"},
            _ReadErrorFile(),
        )
        client = BuildkiteTestEngineClient(
            "token",
            "ray-project",
            "kuberay",
            base_url="https://api.buildkite.test/v2",
            opener=_FakeOpener(error),
        )

        with self.assertRaises(BuildkiteAPIError) as context:
            client.list_tests()

        self.assertEqual(502, context.exception.status_code)
        self.assertEqual("10", context.exception.retry_after)
        self.assertIsInstance(context.exception.__cause__, IncompleteRead)

    def test_reports_network_error(self):
        opener = _FakeOpener(URLError("connection refused"))
        client = BuildkiteTestEngineClient(
            "token",
            "ray-project",
            "kuberay",
            base_url="https://api.buildkite.test/v2",
            opener=opener,
        )

        with self.assertRaisesRegex(BuildkiteAPIError, "connection refused"):
            client.list_tests()

    def test_reports_truncated_response_as_api_error(self):
        opener = _FakeOpener(_ReadErrorResponse([]))
        client = BuildkiteTestEngineClient(
            "token",
            "ray-project",
            "kuberay",
            base_url="https://api.buildkite.test/v2",
            opener=opener,
        )

        with self.assertRaises(BuildkiteAPIError) as context:
            client.list_tests()

        self.assertIsInstance(context.exception.__cause__, IncompleteRead)

    def test_rejects_cross_origin_redirect(self):
        handler = _SameOriginRedirectHandler(("https", "api.buildkite.test"))
        request = Request(
            "https://api.buildkite.test/v2/tests",
            headers={"Authorization": "Bearer secret-token"},
        )

        with self.assertRaisesRegex(BuildkiteResponseError, "redirect"):
            handler.redirect_request(
                request,
                None,
                302,
                "Found",
                {},
                "https://example.com/steal-token",
            )

    def test_rejects_invalid_json_and_response_shapes(self):
        invalid_responses = (
            _FakeResponse(b"not-json", raw=True),
            _FakeResponse({"tests": []}),
            _FakeResponse([{"id": "missing-fields"}]),
        )

        for response in invalid_responses:
            with self.subTest(response=response):
                client = BuildkiteTestEngineClient(
                    "token",
                    "ray-project",
                    "kuberay",
                    base_url="https://api.buildkite.test/v2",
                    opener=_FakeOpener(response),
                )
                with self.assertRaises(BuildkiteResponseError):
                    client.list_tests()

    def test_rejects_cross_origin_and_repeated_pagination_links(self):
        cross_origin = _FakeResponse(
            [],
            headers={
                "Link": '<https://example.com/tests?page=2>; rel="next"'
            },
        )
        client = BuildkiteTestEngineClient(
            "token",
            "ray-project",
            "kuberay",
            base_url="https://api.buildkite.test/v2",
            opener=_FakeOpener(cross_origin),
        )
        with self.assertRaisesRegex(BuildkiteResponseError, "cross-origin"):
            client.list_tests()

        first_url = (
            "https://api.buildkite.test/v2/analytics/organizations/ray-project/"
            "suites/kuberay/tests?per_page=100"
        )
        repeated = _FakeResponse(
            [], headers={"Link": f'<{first_url}>; rel="next"'}
        )
        client = BuildkiteTestEngineClient(
            "token",
            "ray-project",
            "kuberay",
            base_url="https://api.buildkite.test/v2",
            opener=_FakeOpener(repeated),
        )
        with self.assertRaisesRegex(BuildkiteResponseError, "repeated"):
            client.list_tests()

    def test_validates_configuration_and_filters(self):
        invalid_configurations = (
            {"token": "", "organization": "org", "suite": "suite"},
            {"token": "token", "organization": "", "suite": "suite"},
            {"token": "token", "organization": "org", "suite": ""},
            {
                "token": "token",
                "organization": "org",
                "suite": "suite",
                "base_url": "http://api.buildkite.test/v2",
            },
        )
        for configuration in invalid_configurations:
            with self.subTest(configuration=configuration):
                with self.assertRaises(ValueError):
                    BuildkiteTestEngineClient(**configuration)

        client = BuildkiteTestEngineClient(
            "token", "org", "suite", opener=_FakeOpener(_FakeResponse([]))
        )
        for kwargs in (
            {"per_page": 0},
            {"per_page": 101},
            {"labels": ("",)},
            {"branch": ""},
            {"period": ""},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    client.list_tests(**kwargs)


if __name__ == "__main__":
    unittest.main()
