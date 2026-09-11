#!/usr/bin/env bash
# Self-test for scripts/ci-retryable-failure.sh: one fixture log per verdict, including the mixed
# log (a fixture-boot flake AND an executed-test assertion failure) that the in-job retry used to
# retry into green, the executed-test NullReferenceException the whole-log scan could not see, and
# the ten-thousand-assertion log that killed the old `grep | head` pipeline with SIGPIPE. Runs in
# the build-and-test CI job; no .NET involved.
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
# A test that EXECUTED and failed for a reason no signature explains is a real failure, not an
# unmatched one: its block has neither an assertion nor a flake signature.
expect executed-test-unexplained-exception 1 "real: System.TimeoutException: the container did not answer in AsyncResponse.IntegrationTests.Qux" \
  "  failed AsyncResponse.IntegrationTests.Qux\n  System.TimeoutException: the container did not answer\n"
expect unmatched-without-test-blocks 2 "unmatched" \
  "System.TimeoutException: the container did not answer\n"
expect empty 2 "unmatched" ""

# ---- Round 37: the two inputs the classifier got wrong, plus the block splitter's edge cases. ----

# 1. A fixture-boot flake (retryable on its own) next to an executed test that died with a
#    NullReferenceException. No assertion signature anywhere, so the whole-log scan saw only the
#    flake and retried the job — the correctness failure could pass on attempt 2.
expect fixture-flake-plus-executed-nre 1 "real: System.NullReferenceException: Object reference not set to an instance of an object. in AsyncResponse.IntegrationTests.Foo.Bar" \
  "  failed AsyncResponse.IntegrationTests.Boot.Baz (12ms)\n  Xunit.Sdk.TestPipelineException: Class fixture type 'AsyncResponse.IntegrationTests.DataBatchFixture' threw in InitializeAsync\n    at Xunit.v3.FixtureMappingManager.GetFixture(Type)\n\n  failed AsyncResponse.IntegrationTests.Foo.Bar (3s 40ms)\n  System.NullReferenceException: Object reference not set to an instance of an object.\n     at AsyncResponse.IntegrationTests.Foo.Bar() in /src/tests/Foo.cs:line 42\n"

# 2. Ten thousand assertion failures in one log. `grep -o … | head -n 1` under pipefail: head
#    closed the pipe while grep was still writing, grep died of SIGPIPE, the pipeline "failed",
#    the `if` skipped the real-failure branch, and the fixture-flake line further down retried it.
ten_thousand="Class fixture type 'AsyncResponse.IntegrationTests.BrokersBatchFixture' threw in InitializeAsync\n"
for _ in $(seq 1 10000); do
  ten_thousand+="  failed AsyncResponse.IntegrationTests.Many (1ms)\n  Assert.Equal() Failure: Values differ\n"
done
expect ten-thousand-assertions 1 "real: Assert.Equal() Failure in AsyncResponse.IntegrationTests.Many" "$ten_thousand"

# The boot error's own exception text lives INSIDE the fixture block; it does not make the block real.
expect fixture-block-with-inner-exception 0 "flake: Fixture' threw in InitializeAsync" \
  "  failed AsyncResponse.IntegrationTests.Boot.Baz (12ms)\n  Xunit.Sdk.TestPipelineException: Class fixture type 'AsyncResponse.IntegrationTests.DataBatchFixture' threw in InitializeAsync\n  ---- System.Net.Http.HttpRequestException: Connection refused (localhost:8080)\n  ---- System.TimeoutException: the container did not answer\n     at Aspire.Hosting.DistributedApplication.StartAsync()\n"

# The "failed" line itself wrapped in colour codes still opens a block.
expect ansi-failed-line-with-nre 1 "real: System.NullReferenceException" \
  "\e[31m  failed AsyncResponse.IntegrationTests.Boot.Baz\e[0m\n\e[91m  Xunit.Sdk.TestPipelineException: Class fixture type 'XFixture' threw in InitializeAsync\e[0m\n\e[31m  failed AsyncResponse.IntegrationTests.Foo.Bar (3s)\e[0m\n\e[91m  System.NullReferenceException: boom\e[0m\n"

# A passed/skipped line or the summary ends a block: the assertion after the summary belongs to no
# test, and the whole-log scan still reports it.
expect block-ends-at-summary 1 "real: Assert.True() Failure" \
  "  failed AsyncResponse.IntegrationTests.Boot.Baz\n  Xunit.Sdk.TestPipelineException: Class fixture type 'XFixture' threw in InitializeAsync\n  passed AsyncResponse.IntegrationTests.Ok (1ms)\nTest run summary: Failed!\n  Assert.True() Failure\n"

# Build errors outrank everything, blocks included.
expect build-error-with-flake-blocks 1 "real: error MSB3027" \
  "  failed AsyncResponse.IntegrationTests.Boot.Baz\n  Xunit.Sdk.TestPipelineException: Class fixture type 'XFixture' threw in InitializeAsync\nerror MSB3027: Could not copy\n"

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
