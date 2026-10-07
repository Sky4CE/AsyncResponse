#!/usr/bin/env bash
# Release gate: succeeds only when the CI workflow PASSED for this exact commit, on a push to main.
#
#   GH_REPO=<owner/name> COMMIT_SHA=<40-hex sha> ./scripts/require-ci-success.sh
#
# Why publish.yml needs it: `main` takes direct pushes, so "the tagged commit is on main" proves
# only that the commit is there — not that anything tested it. CI runs AFTER a commit lands on
# main, so a tag pushed (or a release published) on a commit whose integration batches, matrix
# shards, Native AOT legs or cross-platform units failed — or had not finished — went to nuget.org
# on the strength of the unit suite alone. This asks GitHub for the commit's CI run instead.
#
# The verdict is the LATEST CI run for the commit that was triggered by a push to main (a re-run is
# a new attempt of the same run, so auto-retry.yml's re-runs are judged by their last attempt):
#   completed + success   → exit 0
#   completed + anything  → exit 1 (failure, cancelled — e.g. superseded in the concurrency queue —,
#                           timed_out, …): a release needs a green run of THIS commit. While wait
#                           budget remains, a failed attempt is first re-polled CI_RETRY_GRACE_POLLS
#                           times: auto-retry.yml re-runs an infrastructure flake a few seconds to a
#                           minute after it fails, and a publish polling in that gap must not fail
#                           a release the re-run turns green.
#   not completed         → wait, polling every CI_POLL_SECONDS, up to CI_WAIT_SECONDS; then exit 1
#   no such run           → exit 1 (never assume a commit was tested)
#   the API call fails    → exit 1 (fail closed: an unreadable verdict is not a pass)
# Every refusal says why and that re-running the publish job after CI is green is enough — the
# job's commit does not change on a re-run.
#
# Environment:
#   GH_REPO           owner/name of the repository (required)
#   COMMIT_SHA        full 40-character commit sha (required)
#   GH_TOKEN          token for `gh` with actions: read (the workflow passes github.token)
#   CI_WORKFLOW_FILE  workflow file name to judge (default: ci.yml)
#   CI_BRANCH         branch whose push runs count (default: main)
#   CI_WAIT_SECONDS   how long to wait for a run still in progress (default: 0 = do not wait)
#   CI_POLL_SECONDS   poll interval while waiting (default: 60)
#   CI_RETRY_GRACE_POLLS  polls a failed attempt gets for auto-retry to start a new one, within the
#                     wait budget (default: 2)
# Inputs arrive through the environment, never interpolated into this script by the workflow, and
# are validated before they reach a URL.
set -euo pipefail

repo="${GH_REPO:-}"
sha="${COMMIT_SHA:-}"
workflow="${CI_WORKFLOW_FILE:-ci.yml}"
branch="${CI_BRANCH:-main}"
wait_seconds="${CI_WAIT_SECONDS:-0}"
poll_seconds="${CI_POLL_SECONDS:-60}"
retry_grace_polls="${CI_RETRY_GRACE_POLLS:-2}"

fail() {
  echo "::error::$1"
  exit 1
}

[[ "$repo" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]] || fail "GH_REPO '$repo' is not an owner/name pair."
[[ "$sha" =~ ^[0-9a-f]{40}$ ]] || fail "COMMIT_SHA '$sha' is not a full 40-character commit sha."
[[ "$workflow" =~ ^[A-Za-z0-9_.-]+\.ya?ml$ ]] || fail "CI_WORKFLOW_FILE '$workflow' is not a workflow file name."
[[ "$branch" =~ ^[A-Za-z0-9_./-]+$ ]] || fail "CI_BRANCH '$branch' is not a plain branch name."
[[ "$wait_seconds" =~ ^[0-9]+$ ]] || fail "CI_WAIT_SECONDS '$wait_seconds' is not a whole number of seconds."
[[ "$poll_seconds" =~ ^[0-9]+$ ]] || fail "CI_POLL_SECONDS '$poll_seconds' is not a whole number of seconds."
[[ "$retry_grace_polls" =~ ^[0-9]+$ ]] || fail "CI_RETRY_GRACE_POLLS '$retry_grace_polls' is not a whole number."

rerun_hint="Re-run this job once the $workflow run for $sha on $branch is green."
deadline=$((SECONDS + wait_seconds))
# The failed attempt the grace is being spent on, and the grace polls it has left.
graced_attempt=""
grace_left=0

while :; do
  # The API filters by sha, event and branch; jq filters again, so a server that ignored a filter
  # still cannot let another commit's run vouch for this one. Newest run first (highest run_number).
  if ! response=$(gh api "repos/$repo/actions/workflows/$workflow/runs?head_sha=$sha&event=push&branch=$branch&per_page=100"); then
    fail "Could not list the $workflow runs of $sha; a CI verdict that cannot be read is not a pass. $rerun_hint"
  fi
  if ! latest=$(printf '%s' "$response" | jq -r --arg sha "$sha" --arg branch "$branch" '
        [.workflow_runs[]? | select(.head_sha == $sha and .event == "push" and .head_branch == $branch)]
        | sort_by(.run_number) | last
        | if . == null then "none" else "\(.status) \(.conclusion // "none") \(.run_attempt // 1) \(.html_url)" end'); then
    fail "Could not parse the $workflow runs of $sha. $rerun_hint"
  fi

  if [ "$latest" = "none" ]; then
    fail "No $workflow run for $sha was triggered by a push to $branch, so nothing shows this commit was tested. Publish only a commit whose CI ran green on $branch."
  fi

  read -r status conclusion attempt url <<< "$latest"
  if [ "$status" = "completed" ]; then
    if [ "$conclusion" = "success" ]; then
      echo "$workflow passed for $sha on $branch (attempt $attempt): $url"
      exit 0
    fi
    if [ "$attempt" != "$graced_attempt" ]; then
      graced_attempt="$attempt"
      grace_left="$retry_grace_polls"
    fi
    if [ "$grace_left" -gt 0 ] && [ "$SECONDS" -lt "$deadline" ]; then
      grace_left=$((grace_left - 1))
      echo "The $workflow run for $sha on $branch concluded '$conclusion' (attempt $attempt); polling again in case auto-retry re-runs it: $url"
      sleep "$poll_seconds"
      continue
    fi
    fail "The $workflow run for $sha on $branch concluded '$conclusion' (attempt $attempt): $url. A release needs a green CI run of this exact commit. $rerun_hint"
  fi

  if [ "$SECONDS" -ge "$deadline" ]; then
    fail "The $workflow run for $sha on $branch is still '$status' (attempt $attempt): $url. $rerun_hint"
  fi
  echo "The $workflow run for $sha on $branch is '$status' (attempt $attempt); waiting up to $((deadline - SECONDS)) more second(s): $url"
  sleep "$poll_seconds"
done
