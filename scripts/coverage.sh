#!/usr/bin/env bash
# Run every test suite with coverage and open a browsable, dotCover-style HTML report.
#
#   ./scripts/coverage.sh          # everything: unit + integration, then opens the report
#
# Runs what CI runs — the unit suite on net8.0 and net10.0 plus the integration suite — so the
# only difference from the published badge is the build configuration.
#
# Options (none are needed for the normal case):
#   --like-ci          also switch to Release, reproducing the README badge exactly
#   --unit-only        skip the container-backed integration suite (fast, needs no Docker)
#   --framework <tfm>  narrow the unit suite to one framework (default: net8.0 and net10.0)
#   -c, --configuration <cfg>   build configuration (default: Debug — see below)
#   --no-open          write the report but don't launch a browser
#
# Debug is the deliberate local default even though CI publishes the badge from Release, so the two
# numbers are close but never identical. An optimised build emits fewer sequence points: on the unit
# suite alone Release counts ~5,400 fewer coverable lines than Debug (27.6k against 33.0k) — they are
# not uncovered, they are uncounted. The branch denominator barely moves (9,580 against 9,694), since
# a branch survives optimisation even where its lines are folded away. Debug therefore sees more of
# the code, which makes it the better view for finding untested paths; Release is what the badge must
# report because Release is what ships. Use --like-ci to reproduce the badge.
#
# Coverage is collected by coverlet and rendered by ReportGenerator. Everything lands in
# TestResults/, which is gitignored. The collector needs the tool CI installs:
#
#   dotnet tool install --global coverlet.console --version 10.0.1 --framework net8.0
#
# The --framework is a hard requirement, not tidiness — see scripts/coverage-collect.sh, which owns
# the collector invocation and is shared with CI.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CONFIGURATION="Debug"
# Mirrors CI's framework set so the only difference between this run and the published badge is the
# build configuration. The integration suite is net10.0-only by project, here and in CI alike.
FRAMEWORKS=("net8.0" "net10.0")
RUN_INTEGRATION=1
OPEN_REPORT=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --like-ci) CONFIGURATION="Release"; FRAMEWORKS=("net8.0" "net10.0"); shift ;;
    --unit-only|--no-integration) RUN_INTEGRATION=0; shift ;;
    --framework) FRAMEWORKS=("$2"); shift 2 ;;
    --configuration|-c) CONFIGURATION="$2"; shift 2 ;;
    --no-open) OPEN_REPORT=0; shift ;;
    # Prints the header block by shape rather than by line number, so editing the comment above
    # cannot silently truncate --help (it already did once).
    -h|--help) awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown option: $1 (try --help)" >&2; exit 2 ;;
  esac
done

# Global dotnet tools live here; non-login shells (CI, cron) don't get it on PATH.
export PATH="$PATH:$HOME/.dotnet/tools"
if ! command -v reportgenerator >/dev/null 2>&1; then
  echo "reportgenerator not found. Install it with:" >&2
  echo "  dotnet tool install --global dotnet-reportgenerator-globaltool" >&2
  exit 1
fi

RESULTS_DIR="$REPO_ROOT/TestResults/coverage"
REPORT_DIR="$REPO_ROOT/TestResults/coverage-report"
HISTORY_DIR="$REPO_ROOT/TestResults/coverage-history"

rm -rf "$RESULTS_DIR" "$REPORT_DIR"
mkdir -p "$RESULTS_DIR" "$HISTORY_DIR"

FAILED_RUNS=()
PARTIAL=""

run_with_coverage() {
  local project="$1" framework="$2" label="$3"
  shift 3
  echo "==> Testing $label"
  # MTP surfaces failures via exit code; keep going so a red test still produces a report. The TRX
  # is the durable record of pass/fail — console output is easy to lose to a pipe.
  #
  # This goes through the same collector script CI does, so a contributor's branch percentage and the
  # published one are produced by the same instrumenter. Do not "simplify" either side back to MTP's
  # --coverage flag: it measures no branches at all, and on linux-x64 it corrupts IL (details in
  # scripts/coverage-collect.sh).
  if ! "$REPO_ROOT/scripts/coverage-collect.sh" \
    "$RESULTS_DIR/$label.cobertura.xml" \
    "$(dirname "$project")/bin/$CONFIGURATION/$framework" \
    "$@" \
    -- dotnet test --project "$project" \
    --configuration "$CONFIGURATION" --framework "$framework" --no-build \
    --report-trx \
    --report-trx-filename "$label.trx" \
    --results-directory "$RESULTS_DIR"; then
    FAILED_RUNS+=("$label")
    echo "!! $label had failing tests — report still generated"
  fi
}

# Coverage needs the PDBs of the assemblies under test; building the test projects pulls in every
# src/** project reference.
echo "==> Building ($CONFIGURATION)"
dotnet build tests/AsyncResponse.Tests/AsyncResponse.Tests.csproj -c "$CONFIGURATION" -v q --nologo

for fw in "${FRAMEWORKS[@]}"; do
  run_with_coverage tests/AsyncResponse.Tests/AsyncResponse.Tests.csproj "$fw" "unit-$fw"
done

if [[ $RUN_INTEGRATION -eq 1 ]]; then
  if ! docker info >/dev/null 2>&1; then
    # The integration suite is most of the coverage, so a missing Docker must not look like a
    # normal run that happens to score lower.
    PARTIAL="Docker is not running — the integration suite was skipped, so this report understates coverage."
    echo "!! $PARTIAL" >&2
  else
    dotnet build tests/AsyncResponse.IntegrationTests/AsyncResponse.IntegrationTests.csproj \
      -c "$CONFIGURATION" -v q --nologo
    # The AppHost and the sample apps it launches are separate processes loading their own copies of
    # the product assemblies; --share puts them on the instrumented ones (see coverage-collect.sh).
    run_with_coverage tests/AsyncResponse.IntegrationTests/AsyncResponse.IntegrationTests.csproj \
      net10.0 "integration-net10.0" \
      --share "tests/AsyncResponse.IntegrationTests.AppHost/bin/$CONFIGURATION/net10.0" \
      --share "samples/AsyncResponse.Sample/bin/$CONFIGURATION/net10.0"
  fi
else
  PARTIAL="--unit-only: the integration suite was skipped, so this report understates coverage."
fi

echo "==> Rendering report"
# What this run actually measured, stamped into the report heading so an open report is never
# ambiguous about which configuration produced its numbers.
SUITES="unit"
[[ $RUN_INTEGRATION -eq 1 && -z "$PARTIAL" ]] && SUITES="unit+integration"
FRAMEWORK_LABEL="$(IFS='+'; echo "${FRAMEWORKS[*]}")"
RUN_SHAPE="$CONFIGURATION / $FRAMEWORK_LABEL / $SUITES"
CI_SHAPE="Release / net8.0+net10.0 / unit+integration"

# Report shape (filters, report types, badges) lives in coverage-report.sh, shared with CI so the
# README badge and this report can never disagree about what is being *measured*. They can still
# differ on what was *built* — hence the shape comparison below.
COVERAGE_LABEL="$RUN_SHAPE" "$REPO_ROOT/scripts/coverage-report.sh" \
  "$RESULTS_DIR/*.cobertura.xml" \
  "$REPORT_DIR" \
  "$HISTORY_DIR"

head -20 "$REPORT_DIR/Summary.txt"
echo
echo "Full summary: $REPORT_DIR/Summary.txt"
echo "Report:       $REPORT_DIR/index.html"

[[ -n "$PARTIAL" ]] && { echo; echo "!! $PARTIAL"; }

# Coverage totals are only comparable between identical build shapes. Say so up front rather than
# leaving someone to discover it by noticing the README badge disagrees with what they just ran.
if [[ "$RUN_SHAPE" != "$CI_SHAPE" ]]; then
  echo
  echo "Note: these numbers are NOT comparable to the README badge."
  echo "  this run: $RUN_SHAPE"
  echo "  badge:    $CI_SHAPE"
  if [[ "$CONFIGURATION" != "Release" ]]; then
    echo "  $CONFIGURATION counts ~5,400 more coverable lines than Release on the unit suite, which"
    echo "  emits fewer sequence points; the branch denominator is nearly unchanged. Expect the line"
    echo "  percentage within about a point of the badge, either side. It sees more of the code,"
    echo "  which makes it the better view for finding gaps."
  fi
  echo "  Run './scripts/coverage.sh --like-ci' to reproduce the badge exactly."
fi

if [[ ${#FAILED_RUNS[@]} -gt 0 ]]; then
  echo
  echo "!! Coverage is from a RED run — failing suites: ${FAILED_RUNS[*]}"
  echo "!! See $RESULTS_DIR/*.trx"
fi

if [[ $OPEN_REPORT -eq 1 ]]; then
  # Platform-portable open: `open` is macOS, Linux/WSL have xdg-open, and anywhere else just
  # print the path. Under `set -e`, neither a MISSING opener nor a FAILING one (headless session,
  # no display) may turn a green coverage run red — hence the || fallbacks.
  if command -v open >/dev/null 2>&1; then
    open "$REPORT_DIR/index.html" || echo "Coverage report: $REPORT_DIR/index.html"
  elif command -v xdg-open >/dev/null 2>&1; then
    xdg-open "$REPORT_DIR/index.html" || echo "Coverage report: $REPORT_DIR/index.html"
  else
    echo "Coverage report: $REPORT_DIR/index.html"
  fi
fi

# Exit red if any suite failed, so the numbers are never read as a green run.
[[ ${#FAILED_RUNS[@]} -eq 0 ]]
