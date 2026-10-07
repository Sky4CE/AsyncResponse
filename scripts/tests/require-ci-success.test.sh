#!/usr/bin/env bash
# Self-test for scripts/require-ci-success.sh, publish.yml's "CI passed for this commit" gate. A
# stub `gh` on PATH answers from fixture files (one per call, so a poll can see a run progress) and
# records every call; jq is the real one. Runs in the build-and-test CI job; no .NET, no network.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
gate="${CI_GATE_SCRIPT:-$here/../require-ci-success.sh}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

sha="0123456789abcdef0123456789abcdef01234567"
other="fedcba9876543210fedcba9876543210fedcba98"

mkdir -p "$work/bin"
cat > "$work/bin/gh" <<'STUB'
#!/usr/bin/env bash
# Answers call N with $STUB_DIR/response.N (the last one repeats); exit code from response.N.exit.
n=$(( $(cat "$STUB_DIR/calls" 2>/dev/null || echo 0) + 1 ))
echo "$n" > "$STUB_DIR/calls"
printf '%s\n' "$*" >> "$STUB_DIR/args"
file="$STUB_DIR/response.$n"
[ -e "$file" ] || file="$STUB_DIR/response.$(cat "$STUB_DIR/count")"
code=0
[ -e "$file.exit" ] && code=$(cat "$file.exit")
[ -e "$file" ] && cat "$file"
exit "$code"
STUB
chmod +x "$work/bin/gh"

# run <head_sha> <event> <branch> <run_number> <status> <conclusion|null> [attempt]
run() {
  local conclusion="$6"
  [ "$conclusion" = "null" ] || conclusion="\"$conclusion\""
  printf '{"id":%s,"run_number":%s,"run_attempt":%s,"head_sha":"%s","event":"%s","head_branch":"%s","status":"%s","conclusion":%s,"html_url":"https://github.com/o/r/actions/runs/%s"}' \
    "$4" "$4" "${7:-1}" "$1" "$2" "$3" "$5" "$conclusion" "$4"
}
runs() { local IFS=,; printf '{"total_count":%s,"workflow_runs":[%s]}' "$#" "$*"; }

failures=0
case_no=0
# expect <name> <exit> <last-output-line prefix> [env assignments...] -- <response> [<response>...]
# A response of EXIT:<n> makes that gh call fail with exit code n and no output.
expect() {
  local name="$1" expected_code="$2" expected_prefix="$3"; shift 3
  local -a env_args=()
  while [ "$1" != "--" ]; do env_args+=("$1"); shift; done
  shift
  case_no=$((case_no + 1))
  local dir="$work/case$case_no" i=0
  mkdir -p "$dir"
  for response in "$@"; do
    i=$((i + 1))
    if [[ "$response" == EXIT:* ]]; then
      echo "${response#EXIT:}" > "$dir/response.$i.exit"; : > "$dir/response.$i"
    else
      printf '%s' "$response" > "$dir/response.$i"
    fi
  done
  echo "$i" > "$dir/count"
  local output code=0
  # (The ${a[@]+...} form: bash 3.2, macOS's, treats an empty array as unbound under set -u.)
  output=$(env PATH="$work/bin:$PATH" STUB_DIR="$dir" GH_REPO="o/r" COMMIT_SHA="$sha" CI_POLL_SECONDS=0 ${env_args[@]+"${env_args[@]}"} bash "$gate" 2>&1) || code=$?
  # The verdict is the last line; progress lines while waiting come before it.
  local first="${output##*$'\n'}"
  if [ "$code" != "$expected_code" ] || [[ "$first" != "$expected_prefix"* ]]; then
    echo "FAIL $name: expected exit $expected_code with '$expected_prefix…', got exit $code with:"; printf '%s\n' "$output" | sed 's/^/    /'
    failures=$((failures + 1))
  else
    echo "ok   $name: exit $code (${first:0:110})"
  fi
  LAST_DIR="$dir"
}

expect green-run 0 "ci.yml passed for $sha on main (attempt 1)" -- \
  "$(runs "$(run "$sha" push main 10 completed success)")"
# The query names the commit, the push event and the branch.
if ! grep -q "repos/o/r/actions/workflows/ci.yml/runs?head_sha=$sha&event=push&branch=main&per_page=100" "$LAST_DIR/args"; then
  echo "FAIL query-shape: gh was called with '$(cat "$LAST_DIR/args")'"; failures=$((failures + 1))
else
  echo "ok   query-shape"
fi

expect red-run 1 "::error::The ci.yml run for $sha on main concluded 'failure'" -- \
  "$(runs "$(run "$sha" push main 10 completed failure 2)")"
expect cancelled-run 1 "::error::The ci.yml run for $sha on main concluded 'cancelled'" -- \
  "$(runs "$(run "$sha" push main 10 completed cancelled)")"
expect no-run 1 "::error::No ci.yml run for $sha was triggered by a push to main" -- \
  "$(runs)"
# A server that ignored a filter still cannot let another commit, event or branch vouch for this one.
expect only-a-dispatch-run 1 "::error::No ci.yml run" -- \
  "$(runs "$(run "$sha" workflow_dispatch main 11 completed success)")"
expect only-a-pull-request-run 1 "::error::No ci.yml run" -- \
  "$(runs "$(run "$sha" pull_request feature 11 completed success)")"
expect only-another-commit 1 "::error::No ci.yml run" -- \
  "$(runs "$(run "$other" push main 11 completed success)")"
# The newest push run decides, whichever order the API lists them in.
expect newest-run-red 1 "::error::The ci.yml run for $sha on main concluded 'failure'" -- \
  "$(runs "$(run "$sha" push main 10 completed success)" "$(run "$sha" push main 12 completed failure)")"
expect newest-run-green 0 "ci.yml passed" -- \
  "$(runs "$(run "$sha" push main 12 completed success)" "$(run "$sha" push main 10 completed failure)")"
# Still running: no wait budget refuses at once; a budget polls until the run finishes.
expect in-progress-no-wait 1 "::error::The ci.yml run for $sha on main is still 'in_progress'" -- \
  "$(runs "$(run "$sha" push main 10 in_progress null)")"
expect in-progress-then-green 0 "ci.yml passed for $sha on main" CI_WAIT_SECONDS=30 -- \
  "$(runs "$(run "$sha" push main 10 queued null)")" \
  "$(runs "$(run "$sha" push main 10 in_progress null)")" \
  "$(runs "$(run "$sha" push main 10 completed success)")"
if [ "$(cat "$LAST_DIR/calls")" != 3 ]; then
  echo "FAIL in-progress-then-green-polls: expected 3 gh calls, saw $(cat "$LAST_DIR/calls")"; failures=$((failures + 1))
else
  echo "ok   in-progress-then-green-polls: 3 gh calls"
fi
expect in-progress-then-red 1 "::error::The ci.yml run for $sha on main concluded 'failure'" CI_WAIT_SECONDS=30 -- \
  "$(runs "$(run "$sha" push main 10 in_progress null)")" \
  "$(runs "$(run "$sha" push main 10 completed failure)")"
# A failed attempt gets a few polls for auto-retry.yml to re-run it: a publish polling in the gap
# between the failure and the re-run must not fail a release the re-run turns green.
expect red-then-retried-green 0 "ci.yml passed for $sha on main (attempt 2)" CI_WAIT_SECONDS=30 -- \
  "$(runs "$(run "$sha" push main 10 completed failure 1)")" \
  "$(runs "$(run "$sha" push main 10 in_progress null 2)")" \
  "$(runs "$(run "$sha" push main 10 completed success 2)")"
# …but only a few: a red attempt nobody re-runs is refused once its grace polls are spent.
expect red-not-retried 1 "::error::The ci.yml run for $sha on main concluded 'failure' (attempt 1)" CI_WAIT_SECONDS=30 CI_RETRY_GRACE_POLLS=2 -- \
  "$(runs "$(run "$sha" push main 10 completed failure 1)")"
if [ "$(cat "$LAST_DIR/calls")" != 3 ]; then
  echo "FAIL red-not-retried-polls: expected 3 gh calls (1 + 2 grace), saw $(cat "$LAST_DIR/calls")"; failures=$((failures + 1))
else
  echo "ok   red-not-retried-polls: 3 gh calls"
fi
# The wait is bounded: a run that never finishes is refused once the budget lapses.
expect in-progress-forever 1 "::error::The ci.yml run for $sha on main is still 'in_progress'" CI_WAIT_SECONDS=1 CI_POLL_SECONDS=1 -- \
  "$(runs "$(run "$sha" push main 10 in_progress null)")"
# Fail closed when the verdict cannot be read.
expect api-error 1 "::error::Could not list the ci.yml runs of $sha" -- "EXIT:1"
expect unparseable-response 1 "::error::Could not parse the ci.yml runs of $sha" -- "<html>rate limited</html>"
# Inputs are validated before they reach a URL; gh is never called with them.
expect bad-sha 1 "::error::COMMIT_SHA 'v1.0.0;rm' is not a full 40-character commit sha." COMMIT_SHA='v1.0.0;rm' -- \
  "$(runs "$(run "$sha" push main 10 completed success)")"
if [ -e "$LAST_DIR/calls" ]; then
  echo "FAIL bad-sha-no-call: gh was called"; failures=$((failures + 1))
else
  echo "ok   bad-sha-no-call"
fi
expect bad-repo 1 "::error::GH_REPO 'o/r?x=1' is not an owner/name pair." GH_REPO='o/r?x=1' -- \
  "$(runs "$(run "$sha" push main 10 completed success)")"

if [ "$failures" -ne 0 ]; then
  echo "$failures release-gate self-test(s) failed"
  exit 1
fi
echo "all release-gate self-tests passed"
