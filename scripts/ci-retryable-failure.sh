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
#  3. THE SPLITTER MUST ENGAGE, OR THE VERDICT IS "REAL". Rule 1 is only as good as the block
#     splitter, and the splitter anchors on the START of a line. The log auto-retry.yml fetches
#     from the jobs/logs API is not the console text the in-job retry tees: it opens with a UTF-8
#     BOM and every line carries a "2026-09-21T16:02:31.9876543Z " prefix, so the anchored
#     "failed" pattern matched nothing, zero blocks were found, and the verdict silently fell
#     back to the whole-log scan rule 1 replaced — a fixture flake next to an executed test's
#     NullReferenceException was retried toward green on main. The BOM and the timestamp are
#     stripped before anything is matched, and the fallback no longer trusts an empty split: when
#     the runner's own summary reports more failed tests than the splitter found blocks for
#     ("failed: 5", or "failed with 5 error(s)" and no block at all), the log has a shape this
#     script cannot read, and a failure it cannot read is never retried.
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
#   blocks US real US flake US unexplained US reported-failed US real-evidence US flake-evidence
#   US unexplained-evidence US reported-evidence
# (US = the unit separator, so empty evidence fields survive `read`). The regexes travel through
# the environment, not -v: awk applies escape processing to -v values and would turn `\(` into `(`.
# The BOM travels the same way, as literal bytes: one built inside awk (`sprintf("%c", 239)`) is a
# byte in a byte-oriented awk (mawk, the hosted runner's) and a different character altogether in
# a UTF-8-aware one, while a literal is read under the same rules as the log it is compared with.
classify_blocks() {
  CI_REAL_RE="$REAL_FAILURE_SIGNATURES" CI_FLAKE_RE="$FLAKE_SIGNATURES" CI_BOM=$'\xEF\xBB\xBF' awk '
    BEGIN {
      esc = sprintf("%c", 27)
      ansi_re = esc "\\[[0-9;]*[A-Za-z]"
      real_re = ENVIRON["CI_REAL_RE"]
      flake_re = ENVIRON["CI_FLAKE_RE"]
      bom = ENVIRON["CI_BOM"]
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
    # The jobs/logs API shape (rule 3): a BOM opens the file and a timestamp opens every line, in
    # front of the colour codes. All of it goes before any anchored pattern looks at the line.
    NR == 1 && bom != "" && index($0, bom) == 1 { $0 = substr($0, length(bom) + 1) }
    { gsub(ansi_re, ""); sub(/^[0-9-]+T[0-9:.]+Z /, "") }
    /^[[:space:]]*failed[[:space:]]+[^[:space:]]/ {
      flush()
      in_block = 1
      block_name = $0
      sub(/^[[:space:]]*failed[[:space:]]+/, "", block_name)
      sub(/[[:space:]]+\([^()]*\)[[:space:]]*$/, "", block_name)
      next
    }
    # The per-assembly result line ("…/AsyncResponse.IntegrationTests.dll (net10.0|x64) failed with
    # 5 error(s)") ends the last failed test. What follows it is the whole captured output of the
    # test host — thousands of lines of AppHost and sample-app logging — not the exception of
    # that test, and a flake signature logged in there explained away whichever executed-test
    # failure happened to be printed last. (No apostrophes in here: this program is single-quoted.)
    / failed with [0-9]+ error\(s\)/ {
      flush()
      if (reported_evidence == "" && match($0, /failed with [0-9]+ error\(s\)/)) reported_evidence = substr($0, RSTART, RLENGTH)
      next
    }
    /^[[:space:]]*(passed|skipped)[[:space:]]/ || /^[[:space:]]*(Test run summary|Test summary|Passed!|Failed!|Zero tests ran|total:)/ {
      flush()
      # The count of failed tests the runner itself prints ("  failed: 5", within the few lines
      # under "Test run summary") is what rule 3 holds the splitter to.
      if ($0 ~ /^[[:space:]]*Test run summary/) summary_lines = 6
      next
    }
    summary_lines > 0 {
      summary_lines--
      if ($0 ~ /^[[:space:]]*failed:[[:space:]]*[0-9]+[[:space:]]*$/) {
        count = $0
        gsub(/[^0-9]/, "", count)
        reported_failed += count
        if (count + 0 > 0 && reported_evidence == "") reported_evidence = "failed: " count
      }
    }
    in_block {
      # "from <assembly>.dll (net10.0|x64)" sits between the test name and its exception; it is
      # not the line anyone wants to read as the evidence of the verdict.
      if (block_first == "" && $0 ~ /[^[:space:]]/ && $0 !~ /^[[:space:]]*from [^[:space:]]+\.dll/) { block_first = $0; sub(/^[[:space:]]+/, "", block_first) }
      if (block_real == "" && match($0, real_re)) block_real = substr($0, RSTART, RLENGTH)
      if (block_flake == "" && match($0, flake_re)) block_flake = substr($0, RSTART, RLENGTH)
    }
    END {
      flush()
      printf "%d\037%d\037%d\037%d\037%d\037%s\037%s\037%s\037%s\n", blocks, real_blocks, flake_blocks, unexplained_blocks, reported_failed, real_evidence, flake_evidence, unexplained_evidence, reported_evidence
    }' "$log"
}

if build=$(first_match "$BUILD_FAILURE_SIGNATURES") && [ -n "$build" ]; then
  echo "real: $build"
  exit 1
fi

IFS=$'\037' read -r blocks real_blocks flake_blocks unexplained_blocks reported_failed real_evidence flake_evidence unexplained_evidence reported_evidence < <(classify_blocks)

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

# Rule 3: the runner says tests failed and the splitter did not find them (or not all of them).
# Per-test judgement did not happen for the missing ones, so no flake signature elsewhere in the
# log can vouch for them.
if [ "$reported_failed" -gt "$blocks" ] || { [ "$blocks" -eq 0 ] && [ -n "$reported_evidence" ]; }; then
  echo "real: the log reports '$reported_evidence' ($reported_failed failed test(s) in its run summaries) but only $blocks failed-test block(s) could be read from it; a test failure this script cannot read is never retried"
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
