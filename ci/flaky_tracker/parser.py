"""Parse test outcomes from Go test logs produced by KubeRay's Buildkite jobs.

KubeRay pipes its verbose Go test output through ``.buildkite/format.awk``.
That formatter changes top-level ``=== RUN`` lines to ``--- RUN`` and Go's
``--- FAIL`` markers to ``### FAIL``.  This parser intentionally accepts both
the original Go output and the formatted output so it can also process logs
from jobs that do not use the formatter.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
import re
from typing import Iterable, Optional, Tuple


class FailureKind(str, Enum):
    """The failure category that can be inferred from a Go test log."""

    TEST = "test"
    TIMEOUT = "timeout"
    BUILD = "build"
    SETUP = "setup"
    UNKNOWN = "unknown"


class TestStatus(str, Enum):
    """The terminal status reported by Go for a test or subtest."""

    PASS = "pass"
    FAIL = "fail"
    SKIP = "skip"


@dataclass(frozen=True)
class TestResult:
    """A completed Go test or subtest."""

    test_name: str
    package: Optional[str]
    duration: Optional[str]
    status: TestStatus
    kind: Optional[FailureKind]
    line_number: int

    @property
    def is_subtest(self) -> bool:
        """Return whether this record represents a Go subtest."""

        return "/" in self.test_name

    @property
    def is_failure(self) -> bool:
        """Return whether this result represents a failed test."""

        return self.status == TestStatus.FAIL


@dataclass(frozen=True)
class PackageFailure:
    """A package-level failure not attributed to an individual test."""

    package: str
    kind: FailureKind
    line_number: int


@dataclass(frozen=True)
class GoTestLogParseResult:
    """Structured test results and package failures from one Go test log."""

    test_results: Tuple[TestResult, ...]
    package_failures: Tuple[PackageFailure, ...]

    @property
    def test_failures(self) -> Tuple[TestResult, ...]:
        """Return all failed tests and subtests."""

        return tuple(result for result in self.test_results if result.is_failure)

    @property
    def test_passes(self) -> Tuple[TestResult, ...]:
        """Return all passing tests and subtests."""

        return tuple(
            result for result in self.test_results if result.status == TestStatus.PASS
        )

    @property
    def test_skips(self) -> Tuple[TestResult, ...]:
        """Return all skipped tests and subtests."""

        return tuple(
            result for result in self.test_results if result.status == TestStatus.SKIP
        )

    @property
    def has_failures(self) -> bool:
        """Return whether the log contains any recognized failure."""

        return bool(self.test_failures or self.package_failures)


# Terminal escape sequences appear in logs downloaded from Buildkite.  The
# first expression covers OSC and Buildkite's private ``ESC _ ... BEL``
# sequences.  The second covers colors and other CSI sequences.
_STRING_ESCAPE_RE = re.compile(r"\x1b(?:\]|_).*?(?:\x07|\x1b\\)")
_CSI_ESCAPE_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")

_TEST_RESULT_RE = re.compile(
    r"^\s*(?:---|###)\s+(?P<status>PASS|FAIL|SKIP):\s+"
    r"(?P<test>\S+)"
    r"(?:\s+\((?P<duration>[^)]+)\))?\s*$"
)
_PACKAGE_RESULT_RE = re.compile(
    r"^\s*(?P<status>ok|FAIL)\s+(?P<package>\S+)"
    r"(?:\s+(?P<detail>[0-9.]+s|\(cached\)|"
    r"\[(?P<reason>build|setup) failed\]))?\s*$"
)
_TIMEOUT_RE = re.compile(r"^\s*panic:\s+test timed out after\s+(?P<duration>\S+)")
_RUNNING_TEST_RE = re.compile(r"^\s*(?P<test>Test\S+)\s+\((?P<duration>[^)]+)\)\s*$")


def _clean_lines(log: str) -> Iterable[Tuple[int, str]]:
    for line_number, line in enumerate(log.splitlines(), start=1):
        line = _STRING_ESCAPE_RE.sub("", line)
        line = _CSI_ESCAPE_RE.sub("", line)
        yield line_number, line.rstrip("\r")


def _package_failure_kind(reason: Optional[str]) -> FailureKind:
    if reason == "build":
        return FailureKind.BUILD
    if reason == "setup":
        return FailureKind.SETUP
    return FailureKind.UNKNOWN


def _status_from_marker(marker: str) -> TestStatus:
    return {
        "PASS": TestStatus.PASS,
        "FAIL": TestStatus.FAIL,
        "SKIP": TestStatus.SKIP,
    }[marker]


def _merge_timeout_duplicates(
    results: Iterable[TestResult],
) -> Tuple[TestResult, ...]:
    """Merge duplicate timeout and failure markers while preserving retries.

    A timeout can be reported both in Go's ``running tests`` section and in a
    later failure marker.  Two ordinary results for the same test are retained
    because they may represent separate executions made with ``-count``.
    """

    merged = []
    unmatched_timeout_positions = {}
    for result in results:
        key = (result.package, result.test_name)
        if result.kind == FailureKind.TIMEOUT:
            unmatched_timeout_positions[key] = len(merged)
            merged.append(result)
            continue

        timeout_position = unmatched_timeout_positions.pop(key, None)
        if result.is_failure and timeout_position is not None:
            timeout = merged[timeout_position]
            merged[timeout_position] = replace(
                timeout,
                duration=timeout.duration or result.duration,
            )
            continue

        merged.append(result)

    return tuple(merged)


def parse_go_test_log(log: str) -> GoTestLogParseResult:
    """Parse test outcomes and package failures from ``go test`` output.

    The parser associates test markers with the next Go package summary.  A
    marker remains package-less when the supplied log is truncated before that
    summary, which is preferable to inventing a package name.
    """

    test_results = []
    package_failures = []
    unassigned_start = 0
    waiting_for_running_tests = False
    reading_running_tests = False

    for line_number, line in _clean_lines(log):
        test_match = _TEST_RESULT_RE.match(line)
        if test_match:
            status = _status_from_marker(test_match.group("status"))
            test_results.append(
                TestResult(
                    test_name=test_match.group("test"),
                    package=None,
                    duration=test_match.group("duration"),
                    status=status,
                    kind=FailureKind.TEST if status == TestStatus.FAIL else None,
                    line_number=line_number,
                )
            )
            continue

        timeout_match = _TIMEOUT_RE.match(line)
        if timeout_match:
            waiting_for_running_tests = True
            reading_running_tests = False
            continue

        if waiting_for_running_tests and line.strip() == "running tests:":
            waiting_for_running_tests = False
            reading_running_tests = True
            continue

        if reading_running_tests:
            running_test_match = _RUNNING_TEST_RE.match(line)
            if running_test_match:
                test_results.append(
                    TestResult(
                        test_name=running_test_match.group("test"),
                        package=None,
                        duration=running_test_match.group("duration"),
                        status=TestStatus.FAIL,
                        kind=FailureKind.TIMEOUT,
                        line_number=line_number,
                    )
                )
                continue
            reading_running_tests = False

        package_match = _PACKAGE_RESULT_RE.match(line)
        if not package_match:
            continue

        package = package_match.group("package")
        newly_assigned = test_results[unassigned_start:]
        for index in range(unassigned_start, len(test_results)):
            test_results[index] = replace(test_results[index], package=package)

        reason = package_match.group("reason")
        has_test_failure = any(result.is_failure for result in newly_assigned)
        if package_match.group("status") == "FAIL" and (
            reason or not has_test_failure
        ):
            package_failures.append(
                PackageFailure(
                    package=package,
                    kind=_package_failure_kind(reason),
                    line_number=line_number,
                )
            )
        unassigned_start = len(test_results)

    return GoTestLogParseResult(
        test_results=_merge_timeout_duplicates(test_results),
        package_failures=tuple(package_failures),
    )
