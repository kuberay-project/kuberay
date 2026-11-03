"""Read test history from the Buildkite Test Engine API.

The client deliberately covers only read operations.  Test result collection,
flaky-test policy, and GitHub issue automation are separate concerns that can
be added after KubeRay has a Buildkite test suite.
"""

from __future__ import annotations

from dataclasses import dataclass
from http.client import HTTPException
import json
import re
from typing import Any, Callable, Iterable, Mapping, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


DEFAULT_API_BASE_URL = "https://api.buildkite.com/v2"
TESTS_API_VERSION = "2026-08-01"
_USER_AGENT = "kuberay-flaky-tracker"
_MAX_ERROR_BODY_BYTES = 64 * 1024
_LINK_RE = re.compile(r'<([^>]+)>\s*;\s*rel="([^"]+)"')


class BuildkiteAPIError(RuntimeError):
    """A Buildkite HTTP or network request failed."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        retry_after: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class BuildkiteResponseError(BuildkiteAPIError):
    """Buildkite returned a response that does not match the API contract."""


@dataclass(frozen=True)
class ExecutionCounts:
    """Test execution counts grouped by result."""

    passed: int
    failed: int
    skipped: int = 0
    pending: int = 0
    unknown: int = 0


@dataclass(frozen=True)
class BuildkiteTest:
    """A test and its aggregated metrics from Buildkite Test Engine."""

    id: str
    url: str
    web_url: str
    scope: str
    name: str
    location: Optional[str]
    file_name: Optional[str]
    labels: Tuple[str, ...]
    reliability: Optional[float]
    duration_avg: Optional[float]
    duration_sum: Optional[float]
    duration_min: Optional[float]
    duration_max: Optional[float]
    executions_count: int
    executions_count_by_result: ExecutionCounts


def _required_string(payload: Mapping[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str):
        raise BuildkiteResponseError(
            f"Buildkite test field {field!r} must be a string"
        )
    return value


def _optional_string(payload: Mapping[str, Any], field: str) -> Optional[str]:
    value = payload.get(field)
    if value is not None and not isinstance(value, str):
        raise BuildkiteResponseError(
            f"Buildkite test field {field!r} must be a string or null"
        )
    return value


def _required_int(payload: Mapping[str, Any], field: str) -> int:
    value = payload.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise BuildkiteResponseError(
            f"Buildkite test field {field!r} must be an integer"
        )
    return value


def _optional_float(payload: Mapping[str, Any], field: str) -> Optional[float]:
    value = payload.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BuildkiteResponseError(
            f"Buildkite test field {field!r} must be a number or null"
        )
    return float(value)


def _parse_execution_counts(payload: Mapping[str, Any]) -> ExecutionCounts:
    raw_counts = payload.get("executions_count_by_result")
    if not isinstance(raw_counts, Mapping):
        raise BuildkiteResponseError(
            "Buildkite test field 'executions_count_by_result' must be an object"
        )

    return ExecutionCounts(
        passed=_required_int(raw_counts, "passed"),
        failed=_required_int(raw_counts, "failed"),
        skipped=_required_int(raw_counts, "skipped") if "skipped" in raw_counts else 0,
        pending=_required_int(raw_counts, "pending") if "pending" in raw_counts else 0,
        unknown=_required_int(raw_counts, "unknown") if "unknown" in raw_counts else 0,
    )


def _parse_test(payload: Any) -> BuildkiteTest:
    if not isinstance(payload, Mapping):
        raise BuildkiteResponseError("Buildkite test entries must be objects")

    raw_labels = payload.get("labels")
    if not isinstance(raw_labels, list) or not all(
        isinstance(label, str) for label in raw_labels
    ):
        raise BuildkiteResponseError(
            "Buildkite test field 'labels' must be an array of strings"
        )

    return BuildkiteTest(
        id=_required_string(payload, "id"),
        url=_required_string(payload, "url"),
        web_url=_required_string(payload, "web_url"),
        scope=_required_string(payload, "scope"),
        name=_required_string(payload, "name"),
        location=_optional_string(payload, "location"),
        file_name=_optional_string(payload, "file_name"),
        labels=tuple(raw_labels),
        reliability=_optional_float(payload, "reliability"),
        duration_avg=_optional_float(payload, "duration_avg"),
        duration_sum=_optional_float(payload, "duration_sum"),
        duration_min=_optional_float(payload, "duration_min"),
        duration_max=_optional_float(payload, "duration_max"),
        executions_count=_required_int(payload, "executions_count"),
        executions_count_by_result=_parse_execution_counts(payload),
    )


def _csv_parameter(values: Iterable[str], field: str) -> Optional[str]:
    if isinstance(values, str):
        normalized = (values,)
    else:
        normalized = tuple(values)
    if not normalized:
        return None
    if any(not isinstance(value, str) or not value for value in normalized):
        raise ValueError(f"{field} values must be non-empty strings")
    return ",".join(normalized)


def _next_link(link_header: Optional[str]) -> Optional[str]:
    if not link_header:
        return None
    for match in _LINK_RE.finditer(link_header):
        if "next" in match.group(2).split():
            return match.group(1)
    return None


def _error_message(error: HTTPError, body: bytes) -> str:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        payload = None

    if isinstance(payload, Mapping) and isinstance(payload.get("message"), str):
        return payload["message"]
    return error.reason or "request failed"


class _SameOriginRedirectHandler(HTTPRedirectHandler):
    """Prevent authenticated requests from redirecting to another origin."""

    def __init__(self, origin: Tuple[str, str]) -> None:
        super().__init__()
        self._origin = origin

    def redirect_request(
        self,
        request: Request,
        file_pointer: Any,
        code: int,
        message: str,
        headers: Mapping[str, str],
        new_url: str,
    ) -> Optional[Request]:
        redirect_url = urljoin(request.full_url, new_url)
        parsed_redirect_url = urlsplit(redirect_url)
        redirect_origin = (
            parsed_redirect_url.scheme.lower(),
            parsed_redirect_url.netloc.lower(),
        )
        if redirect_origin != self._origin:
            raise BuildkiteResponseError(
                "Buildkite API returned a cross-origin redirect"
            )
        return super().redirect_request(
            request,
            file_pointer,
            code,
            message,
            headers,
            redirect_url,
        )


class BuildkiteTestEngineClient:
    """A dependency-free, read-only client for Buildkite Test Engine tests."""

    def __init__(
        self,
        token: str,
        organization: str,
        suite: str,
        *,
        base_url: str = DEFAULT_API_BASE_URL,
        timeout: float = 30.0,
        opener: Optional[Callable[..., Any]] = None,
    ) -> None:
        if not isinstance(token, str) or not token:
            raise ValueError("token must not be empty")
        if not isinstance(organization, str) or not organization:
            raise ValueError("organization must not be empty")
        if not isinstance(suite, str) or not suite:
            raise ValueError("suite must not be empty")
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or timeout <= 0
        ):
            raise ValueError("timeout must be greater than zero")
        if not isinstance(base_url, str):
            raise ValueError("base_url must be an absolute HTTPS URL")

        normalized_base_url = base_url.rstrip("/")
        parsed_base_url = urlsplit(normalized_base_url)
        if (
            parsed_base_url.scheme.lower() != "https"
            or not parsed_base_url.netloc
        ):
            raise ValueError("base_url must be an absolute HTTPS URL")

        self._token = token
        self._organization = organization
        self._suite = suite
        self._base_url = normalized_base_url
        self._origin = (parsed_base_url.scheme.lower(), parsed_base_url.netloc.lower())
        self._timeout = timeout
        self._opener = opener or build_opener(
            _SameOriginRedirectHandler(self._origin)
        ).open

    def list_tests(
        self,
        *,
        labels: Iterable[str] = (),
        branch: Optional[str] = None,
        period: Optional[str] = None,
        tags: Iterable[str] = (),
        sort_by: Optional[str] = None,
        order: Optional[str] = None,
        per_page: int = 100,
    ) -> Tuple[BuildkiteTest, ...]:
        """List tests and their metrics, following all result pages."""

        if (
            isinstance(per_page, bool)
            or not isinstance(per_page, int)
            or not 1 <= per_page <= 100
        ):
            raise ValueError("per_page must be between 1 and 100")

        parameters = []
        labels_value = _csv_parameter(labels, "labels")
        tags_value = _csv_parameter(tags, "tags")
        if labels_value is not None:
            parameters.append(("labels", labels_value))
        if branch is not None:
            if not isinstance(branch, str) or not branch:
                raise ValueError("branch must not be empty")
            parameters.append(("branch", branch))
        if period is not None:
            if not isinstance(period, str) or not period:
                raise ValueError("period must not be empty")
            parameters.append(("period", period))
        if tags_value is not None:
            parameters.append(("tags", tags_value))
        if sort_by is not None:
            if not isinstance(sort_by, str) or not sort_by:
                raise ValueError("sort_by must not be empty")
            parameters.append(("sort_by", sort_by))
        if order is not None:
            if not isinstance(order, str) or not order:
                raise ValueError("order must not be empty")
            parameters.append(("order", order))
        parameters.append(("per_page", str(per_page)))

        organization = quote(self._organization, safe="")
        suite = quote(self._suite, safe="")
        url = (
            f"{self._base_url}/analytics/organizations/{organization}/"
            f"suites/{suite}/tests?{urlencode(parameters)}"
        )

        tests = []
        seen_urls = set()
        while url is not None:
            if url in seen_urls:
                raise BuildkiteResponseError(
                    "Buildkite pagination returned a repeated next-page URL"
                )
            seen_urls.add(url)

            page, link_header = self._get_page(url)
            tests.extend(_parse_test(test) for test in page)

            next_url = _next_link(link_header)
            if next_url is None:
                url = None
                continue
            next_url = urljoin(url, next_url)
            parsed_next_url = urlsplit(next_url)
            next_origin = (
                parsed_next_url.scheme.lower(),
                parsed_next_url.netloc.lower(),
            )
            if next_origin != self._origin:
                raise BuildkiteResponseError(
                    "Buildkite pagination returned a cross-origin next-page URL"
                )
            url = next_url

        return tuple(tests)

    def list_flaky_tests(
        self,
        *,
        branch: Optional[str] = "master",
        period: Optional[str] = None,
    ) -> Tuple[BuildkiteTest, ...]:
        """List tests that Buildkite has already labeled as flaky.

        This method does not classify tests itself.  The suite must have a
        Buildkite workflow that applies the ``flaky`` label.
        """

        return self.list_tests(labels=("flaky",), branch=branch, period=period)

    def _get_page(self, url: str) -> Tuple[Any, Optional[str]]:
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._token}",
                "Buildkite-Version": TESTS_API_VERSION,
                "User-Agent": _USER_AGENT,
            },
            method="GET",
        )

        try:
            with self._opener(request, timeout=self._timeout) as response:
                body = response.read()
                link_header = response.headers.get("Link")
        except HTTPError as error:
            headers = error.headers or {}
            retry_after = (
                headers.get("Retry-After")
                or headers.get("RateLimit-Reset")
                or headers.get("RateLimit-User-Reset")
            )
            try:
                try:
                    body = error.read(_MAX_ERROR_BODY_BYTES)
                except (OSError, HTTPException) as read_error:
                    raise BuildkiteAPIError(
                        "Buildkite API request failed with HTTP "
                        f"{error.code}: response body could not be read",
                        status_code=error.code,
                        retry_after=retry_after,
                    ) from read_error
            finally:
                error.close()
            message = _error_message(error, body)
            raise BuildkiteAPIError(
                f"Buildkite API request failed with HTTP {error.code}: {message}",
                status_code=error.code,
                retry_after=retry_after,
            ) from error
        except (URLError, TimeoutError, OSError, HTTPException) as error:
            reason = getattr(error, "reason", str(error))
            raise BuildkiteAPIError(
                f"Buildkite API request failed: {reason}"
            ) from error

        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise BuildkiteResponseError(
                "Buildkite API returned invalid JSON"
            ) from error
        if not isinstance(payload, list):
            raise BuildkiteResponseError(
                "Buildkite tests API response must be an array"
            )
        return payload, link_header
