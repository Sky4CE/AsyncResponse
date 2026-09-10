#!/usr/bin/env bash
# Classify one CI job log: is this failure a known infrastructure flake that a re-run may retry?
#
#   ./scripts/ci-retryable-failure.sh <job-log>
#
# The ONE retry classifier, shared by ci.yml's in-job integration retry and auto-retry.yml's
# whole-run re-run. The two used to carry separate policies, and the in-job one was weaker: it
# retried on the fixture-boot signature alone, so a log carrying BOTH a fixture that failed to boot
# and an executed test's assertion failure entered the retry branch, and a passing second attempt
# turned a real correctness failure into a green job — one the standalone workflow could never
# see, because the job had already succeeded.
#
# Exit code / first stdout line:
#   0  flake: <evidence>      — matches a flake signature and carries no real-failure signature
#   1  real: <evidence>       — an executed test failed on its merits, or the build broke (never retried)
#   2  unmatched              — failed for a reason no signature explains (treated as real)
#   3  unreadable             — the log cannot be read (an unreadable failure cannot be proven a flake)
#
# Signatures are matched with grep -E over the raw log, ANSI colour codes and all: every signature
# is a substring that sits between colour escapes in the runner's output, never across them.
set -euo pipefail

# Known hosted-runner infrastructure loss. "Fixture' threw in InitializeAsync" is a batch or matrix
# fixture (the original *BatchFixture types and the cross-product shards' *Fixture types, which
# carry no "Batch" infix) failing to boot its containers on a starved runner; "database is locked"
# is SQLite on a slow runner disk during the EF Core storm tests (see those tests' own comments);
# the last two are the runner itself going away.
FLAKE_SIGNATURES="Fixture' threw in InitializeAsync|SQLite Error 5: 'database is locked'|lost communication with the server|The runner has received a shutdown signal"

# Evidence that a test EXECUTED and failed on its own merits, or that the build broke: xunit.v3
# assertion messages ("Assert.Equal() Failure: …", "Assert.Fail(): …"), the assertion exception
# base type, and compiler/MSBuild/NuGet error codes. A fixture that throws in InitializeAsync fails
# its tests through TestPipelineException, which none of these match, so a pure boot flake still
# qualifies for a re-run — and a boot flake alongside any of these does not.
REAL_FAILURE_SIGNATURES="Assert\\.[A-Za-z]+\\(\\) Failure|Assert\\.Fail\\(\\)|Xunit\\.Sdk\\.XunitException|error CS[0-9]{4}|error MSB[0-9]{4}|error NU[0-9]{4}"

log="${1:-}"
if [ -z "$log" ] || [ ! -r "$log" ]; then
  echo "unreadable"
  exit 3
fi

if real=$(grep -Eo "$REAL_FAILURE_SIGNATURES" "$log" | head -n 1) && [ -n "$real" ]; then
  echo "real: $real"
  exit 1
fi

if flake=$(grep -Eo "$FLAKE_SIGNATURES" "$log" | head -n 1) && [ -n "$flake" ]; then
  echo "flake: $flake"
  exit 0
fi

echo "unmatched"
exit 2
