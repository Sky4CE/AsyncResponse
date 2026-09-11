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
#   0  flake: <evidence>      — every failed test is explained by a flake signature (or the runner
#                               itself went away) and nothing executed and failed on its merits
#   1  real: <evidence>       — an executed test failed on its merits, or the build broke (never retried)
#   2  unmatched              — failed for a reason no signature explains (treated as real)
#   3  unreadable             — the log cannot be read (an unreadable failure cannot be proven a flake)
#
# Two rules make the verdict:
#
#  1. PER FAILED TEST, not per log. The test runner prints one "failed <name> (<duration>)" line
#     per failed test followed by that test's exception and stack trace, so the log splits into
#     failed-test blocks and each block is judged on its own: an assertion or XunitException is a
#     real failure; a flake signature explains it (a fixture that failed to boot fails every test
#     in its class through TestPipelineException, whose block also carries the boot error's own
#     exception text); NEITHER means a test EXECUTED and failed for a reason no signature covers —
#     a NullReferenceException, a timeout inside the test body — and that is real too. The earlier
#     whole-log scan recognized only assertion-shaped failures, so a boot flake anywhere in the
#     same log got a job with such a failure retried into green.
#
#  2. NO `grep | head` PIPELINES. Under `set -o pipefail`, `grep -o … | head -n 1` fails whenever
#     head closes the pipe while grep is still writing (SIGPIPE, exit 141) — which a log carrying
#     thousands of assertion failures does — and the failed pipeline made the `if` skip the
#     real-failure check entirely, so exactly the loudest correctness failures were classified as
#     retryable. Every match here is a single grep whose output is trimmed in bash.
#
# Signatures are matched over the raw log, ANSI colour codes and all: every signature is a
# substring that sits between colour escapes in the runner's output, never across them; the
# block splitter strips the escapes before looking for the "failed" line.
set -euo pipefail

# Known hosted-runner infrastructure loss. "Fixture' threw in InitializeAsync" is a batch or matrix
# fixture (the original *BatchFixture types and the cross-product shards' *Fixture types, which
# carry no "Batch" infix) failing to boot its containers on a starved runner; "database is locked"
# is SQLite on a slow runner disk during the EF Core storm tests (see those tests' own comments);
# the last two are the runner itself going away.
FLAKE_SIGNATURES="Fixture' threw in InitializeAsync|SQLite Error 5: 'database is locked'|lost communication with the server|The runner has received a shutdown signal"

# Evidence that a test EXECUTED and failed on its own merits: xunit.v3 assertion messages
# ("Assert.Equal() Failure: …", "Assert.Fail(): …") and the assertion exception base type.
REAL_FAILURE_SIGNATURES="Assert\\.[A-Za-z]+\\(\\) Failure|Assert\\.Fail\\(\\)|Xunit\\.Sdk\\.XunitException"

# The build broke: compiler/MSBuild/NuGet error codes. Never a flake, whatever else the log holds.
BUILD_FAILURE_SIGNATURES="error CS[0-9]{4}|error MSB[0-9]{4}|error NU[0-9]{4}"

log="${1:-}"
if [ -z "$log" ] || [ ! -r "$log" ]; then
  echo "unreadable"
  exit 3
fi

# The first match of an extended regex in the log, or empty. `-m 1` stops grep at the first
# matching LINE (it may print several matches from that one line); the first is kept in bash, so
# no second process ever closes a pipe on grep.
first_match() {
  local found
  found=$(grep -Eo -m 1 "$1" "$log" || true)
  printf '%s' "${found%%$'\n'*}"
}

# Splits the log into failed-test blocks and counts them by kind. Prints one record:
#   blocks US real US flake US unexplained US real-evidence US flake-evidence US unexplained-evidence
# (US = the unit separator, so empty evidence fields survive `read`). The regexes travel through
# the environment, not -v: awk applies escape processing to -v values and would turn `\(` into `(`.
classify_blocks() {
  CI_REAL_RE="$REAL_FAILURE_SIGNATURES" CI_FLAKE_RE="$FLAKE_SIGNATURES" awk '
    BEGIN {
      esc = sprintf("%c", 27)
      ansi_re = esc "\\[[0-9;]*[A-Za-z]"
      real_re = ENVIRON["CI_REAL_RE"]
      flake_re = ENVIRON["CI_FLAKE_RE"]
    }
    function flush() {
      if (in_block) {
        blocks++
        if (block_real != "") {
          real_blocks++
          if (real_evidence == "") real_evidence = block_real " in " block_name
        } else if (block_flake != "") {
          flake_blocks++
          if (flake_evidence == "") flake_evidence = block_flake
        } else {
          unexplained_blocks++
          if (unexplained_evidence == "")
            unexplained_evidence = (block_first != "" ? block_first : "no exception line") " in " block_name " (an executed test failed with no infrastructure signature)"
        }
      }
      in_block = 0; block_real = ""; block_flake = ""; block_first = ""; block_name = ""
    }
    { gsub(ansi_re, "") }
    /^[[:space:]]*failed[[:space:]]+[^[:space:]]/ {
      flush()
      in_block = 1
      block_name = $0
      sub(/^[[:space:]]*failed[[:space:]]+/, "", block_name)
      sub(/[[:space:]]+\([^()]*\)[[:space:]]*$/, "", block_name)
      next
    }
    /^[[:space:]]*(passed|skipped)[[:space:]]/ || /^[[:space:]]*(Test run summary|Test summary|Passed!|Failed!|Zero tests ran|total:)/ {
      flush()
      next
    }
    in_block {
      if (block_first == "" && $0 ~ /[^[:space:]]/) { block_first = $0; sub(/^[[:space:]]+/, "", block_first) }
      if (block_real == "" && match($0, real_re)) block_real = substr($0, RSTART, RLENGTH)
      if (block_flake == "" && match($0, flake_re)) block_flake = substr($0, RSTART, RLENGTH)
    }
    END {
      flush()
      printf "%d\037%d\037%d\037%d\037%s\037%s\037%s\n", blocks, real_blocks, flake_blocks, unexplained_blocks, real_evidence, flake_evidence, unexplained_evidence
    }' "$log"
}

if build=$(first_match "$BUILD_FAILURE_SIGNATURES") && [ -n "$build" ]; then
  echo "real: $build"
  exit 1
fi

IFS=$'\037' read -r blocks real_blocks flake_blocks unexplained_blocks real_evidence flake_evidence unexplained_evidence < <(classify_blocks)

if [ "$real_blocks" -gt 0 ]; then
  echo "real: $real_evidence"
  exit 1
fi
if [ "$unexplained_blocks" -gt 0 ]; then
  echo "real: $unexplained_evidence"
  exit 1
fi

# No failed-test block carries a real failure. An assertion OUTSIDE any block (a fixture's own
# assertion, output the runner did not attribute to a test) is still one.
if real=$(first_match "$REAL_FAILURE_SIGNATURES") && [ -n "$real" ]; then
  echo "real: $real"
  exit 1
fi

if [ "$flake_blocks" -gt 0 ]; then
  echo "flake: $flake_evidence"
  exit 0
fi
if flake=$(first_match "$FLAKE_SIGNATURES") && [ -n "$flake" ]; then
  echo "flake: $flake"
  exit 0
fi

echo "unmatched"
exit 2
