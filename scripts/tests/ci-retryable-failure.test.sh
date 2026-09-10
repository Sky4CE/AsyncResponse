#!/usr/bin/env bash
# Self-test for scripts/ci-retryable-failure.sh: one fixture log per verdict, including the mixed
# log (a fixture-boot flake AND an executed-test assertion failure) that the in-job retry used to
# retry into green. Runs in the build-and-test CI job; no .NET involved.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
classifier="$here/../ci-retryable-failure.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

failures=0
expect() {
  local name="$1" expected_code="$2" expected_prefix="$3" content="$4"
  local file="$work/$name.log"
  printf '%b' "$content" > "$file"
  local output code=0
  output=$("$classifier" "$file") || code=$?
  if [ "$code" != "$expected_code" ] || [[ "$output" != "$expected_prefix"* ]]; then
    echo "FAIL $name: expected exit $expected_code with '$expected_prefix…', got exit $code with '$output'"
    failures=$((failures + 1))
  else
    echo "ok   $name: exit $code ($output)"
  fi
}

expect pure-fixture-flake 0 "flake: Fixture' threw in InitializeAsync" \
  "  failed AsyncResponse.IntegrationTests.Foo\n  Xunit.Sdk.TestPipelineException: Class fixture type 'AsyncResponse.IntegrationTests.DataBatchFixture' threw in InitializeAsync\n"
expect matrix-fixture-flake 0 "flake: Fixture' threw in InitializeAsync" \
  "Xunit.Sdk.TestPipelineException: Collection fixture type 'AsyncResponse.IntegrationTests.MatrixDatabaseLightFixture' threw in InitializeAsync\n"
expect sqlite-locked 0 "flake: SQLite Error 5: 'database is locked'" \
  "Microsoft.Data.Sqlite.SqliteException : SQLite Error 5: 'database is locked'.\n"
expect runner-lost 0 "flake: The runner has received a shutdown signal" \
  "The runner has received a shutdown signal. This can happen when the runner service is stopped.\n"
# The finding: a boot flake in the same log as an executed test that failed on its merits.
expect mixed-fixture-and-assertion 1 "real: Assert.Equal() Failure" \
  "Class fixture type 'AsyncResponse.IntegrationTests.BrokersBatchFixture' threw in InitializeAsync\n  failed AsyncResponse.IntegrationTests.Bar\n  Assert.Equal() Failure: Values differ\nExpected: 2\nActual:   1\n"
expect mixed-fixture-and-xunit-exception 1 "real: Xunit.Sdk.XunitException" \
  "Fixture' threw in InitializeAsync\nXunit.Sdk.XunitException: timed out waiting for the flow\n"
expect pure-assertion 1 "real: Assert.True() Failure" \
  "  failed AsyncResponse.IntegrationTests.Baz\n  Assert.True() Failure\n"
expect assert-fail 1 "real: Assert.Fail()" \
  "  Assert.Fail(): Timed out waiting for a log message\n"
expect build-error 1 "real: error CS1002" \
  "Program.cs(12,4): error CS1002: ; expected\n"
expect ansi-coloured-assertion 1 "real: Assert.Equal() Failure" \
  "\e[31mFixture' threw in InitializeAsync\e[0m\n\e[91m  Assert.Equal() Failure: Values differ\e[0m\n"
expect unmatched 2 "unmatched" \
  "  failed AsyncResponse.IntegrationTests.Qux\n  System.TimeoutException: the container did not answer\n"
expect empty 2 "unmatched" ""

code=0
"$classifier" "$work/does-not-exist.log" > "$work/missing.out" || code=$?
if [ "$code" != 3 ] || [ "$(cat "$work/missing.out")" != "unreadable" ]; then
  echo "FAIL missing-log: expected exit 3 'unreadable', got exit $code '$(cat "$work/missing.out")'"
  failures=$((failures + 1))
else
  echo "ok   missing-log: exit 3 (unreadable)"
fi

if [ "$failures" -ne 0 ]; then
  echo "$failures classifier self-test(s) failed"
  exit 1
fi
echo "all classifier self-tests passed"
