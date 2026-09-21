#!/usr/bin/env bash
# Self-test for scripts/ci-retryable-failure.sh: one fixture log per verdict, including the mixed
# log (a fixture-boot flake AND an executed-test assertion failure) that the in-job retry used to
# retry into green, the executed-test NullReferenceException the whole-log scan could not see, and
# the ten-thousand-assertion log that killed the old `grep | head` pipeline with SIGPIPE. Runs in
# the build-and-test CI job; no .NET involved.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Overridable so a new fixture can be shown to FAIL against an older copy of the classifier.
classifier="${CI_RETRY_CLASSIFIER:-$here/../ci-retryable-failure.sh}"
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

# ---- The jobs/logs API shape: what auto-retry.yml actually feeds the classifier. ----
#
# Every fixture above is console text, the shape the in-job retry tees. The log auto-retry.yml
# downloads is different: a UTF-8 BOM opens it, every line starts with a timestamp, the colour
# codes come AFTER the timestamp, and a "from <assembly>" line sits between a failed test and its
# exception. The line-anchored block splitter matched nothing in it, so the verdict fell back to the
# whole-log scan and the mixed log below — a boot flake next to an executed test's
# NullReferenceException — was retried. Shapes copied from a real failed integration-tests leg.
ts="2026-09-21T16:02:31.9876543Z "
dll="/home/runner/work/AsyncResponse/AsyncResponse/tests/AsyncResponse.IntegrationTests/bin/Release/net10.0/AsyncResponse.IntegrationTests.dll (net10.0|x64)"
api_flake_block="${ts}\e[m\e[31mfailed\e[m AsyncResponse.IntegrationTests.Boot.Baz \e[90m(12ms)\e[m\n${ts}  from ${dll}\n${ts}\e[31m  Xunit.Sdk.TestPipelineException: Class fixture type 'AsyncResponse.IntegrationTests.DataBatchFixture' threw in InitializeAsync\n${ts}\e[m\e[90m    at Xunit.v3.FixtureMappingManager.GetFixture(Type)\n"
api_nre_block="${ts}\e[m\e[31mfailed\e[m AsyncResponse.IntegrationTests.Foo.Bar \e[90m(3s 040ms)\e[m\n${ts}  from ${dll}\n${ts}\e[31m  System.NullReferenceException: Object reference not set to an instance of an object.\n${ts}\e[m\e[90m    at AsyncResponse.IntegrationTests.Foo.Bar() in /src/tests/Foo.cs:line 42\n"
api_summary() { printf '%s' "${ts}\e[m\e[m${dll} \e[31mfailed with $1 error(s)\e[m \e[90m(3m 24s)\e[m\n${ts}Exit code: 2\n${ts}\e[31mTest run summary: Failed!\n${ts}\e[m  total: 333\n${ts}\e[31m  failed: $1\n${ts}\e[m  succeeded: 330\n${ts}  skipped: 0\n"; }

# The reproduced finding. The evidence line is the exception, not the "from <assembly>" line.
expect api-log-fixture-flake-plus-executed-nre 1 "real: System.NullReferenceException: Object reference not set to an instance of an object. in AsyncResponse.IntegrationTests.Foo.Bar" \
  "\xEF\xBB\xBF${ts}Current runner version: '2.337.0'\n${api_flake_block}${api_nre_block}$(api_summary 2)"
# The timestamp strip must not cost the flake verdict its retry.
expect api-log-pure-fixture-flake 0 "flake: Fixture' threw in InitializeAsync" \
  "\xEF\xBB\xBF${ts}Current runner version: '2.337.0'\n${api_flake_block}$(api_summary 1)"
# The BOM sits in front of the FIRST line's timestamp; left in place it hides that line's test.
expect api-log-bom-on-the-failed-line 1 "real: System.NullReferenceException" \
  "\xEF\xBB\xBF${api_nre_block}${api_flake_block}$(api_summary 2)"
expect console-log-bom-on-the-failed-line 1 "real: System.NullReferenceException" \
  "\xEF\xBB\xBF  failed AsyncResponse.IntegrationTests.Foo.Bar (3s)\n  System.NullReferenceException: boom\n  failed AsyncResponse.IntegrationTests.Boot.Baz\n  Xunit.Sdk.TestPipelineException: Class fixture type 'XFixture' threw in InitializeAsync\n"
# An in-job retry puts two runs — two summaries — in one job log; their counts add up to the blocks.
expect api-log-two-attempts-both-flakes 0 "flake: Fixture' threw in InitializeAsync" \
  "${api_flake_block}${api_flake_block}$(api_summary 2)${api_flake_block}$(api_summary 1)"

# Fail closed. The runner says tests failed, and the splitter cannot find them (a format this
# script does not know, a prefix nobody anticipated): the whole-log scan is NOT a substitute for
# per-test judgement, so a flake signature elsewhere in the log earns no retry.
expect summary-reports-failures-but-no-blocks 1 "real: the log reports 'failed: 1'" \
  "Class fixture type 'AsyncResponse.IntegrationTests.DataBatchFixture' threw in InitializeAsync\n[x] AsyncResponse.IntegrationTests.Foo.Bar\n  System.NullReferenceException: boom\nTest run summary: Failed!\n  total: 10\n  failed: 1\n  succeeded: 9\n"
expect assembly-result-reports-errors-but-no-blocks 1 "real: the log reports 'failed with 2 error(s)'" \
  "Class fixture type 'AsyncResponse.IntegrationTests.DataBatchFixture' threw in InitializeAsync\n${dll} failed with 2 error(s) (3m 24s)\n"
expect summary-reports-more-failures-than-blocks 1 "real: the log reports" \
  "${api_flake_block}[x] AsyncResponse.IntegrationTests.Foo.Bar\n  System.NullReferenceException: boom\n$(api_summary 2)"

# The per-assembly result line ends the last failed test: the test host's captured output follows
# it, and a flake signature logged in there is not that test's exception.
expect host-output-after-the-assembly-result-is-not-the-last-test 1 "real: System.NullReferenceException: boom in AsyncResponse.IntegrationTests.Foo.Bar" \
  "  failed AsyncResponse.IntegrationTests.Foo.Bar (3s)\n  System.NullReferenceException: boom\n${dll} failed with 1 error(s) (3m 24s)\nExit code: 2\n  Standard output: warn: Microsoft.EntityFrameworkCore.Database.Command[0]\n        SQLite Error 5: 'database is locked'.\nTest run summary: Failed!\n  total: 10\n  failed: 1\n"

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
