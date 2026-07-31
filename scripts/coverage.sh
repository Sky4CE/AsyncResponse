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
# Debug is the deliberate local default even though CI publishes the badge from Release, so expect
# a percentage a point or two BELOW the badge on identical code. An optimised build emits fewer
# sequence points, so ~4,900 lines and ~300 branches stop being instrumented and drop out of the
# denominator entirely — they are not covered, they are uncounted. Debug counts them, which makes
# it both the more conservative number and the better view for finding untested code. Release is
# what the badge must report because Release is what ships. Use --like-ci to reproduce the badge.
#
# Coverage is collected by Microsoft.Testing.Extensions.CodeCoverage (already referenced by both
# test projects) and rendered by ReportGenerator. Everything lands in TestResults/, which is
# gitignored.
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
  echo "==> Testing $label"
  # MTP surfaces failures via exit code; keep going so a red test still produces a report. The TRX
  # is the durable record of pass/fail — console output is easy to lose to a pipe.
  if ! dotnet test --project "$project" \
    --configuration "$CONFIGURATION" --framework "$framework" --no-build \
    --coverage \
    --coverage-output-format cobertura \
    --coverage-output "$label.cobertura.xml" \
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
    run_with_coverage tests/AsyncResponse.IntegrationTests/AsyncResponse.IntegrationTests.csproj \
      net10.0 "integration-net10.0"
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
    echo "  $CONFIGURATION instruments ~4,900 more lines and ~300 more branches than Release, which"
    echo "  emits fewer sequence points — so this run reports a LOWER percentage on identical code"
    echo "  (~1 point on lines, ~3 on branches). That makes it the better view for finding gaps."
  fi
  echo "  Run './scripts/coverage.sh --like-ci' to reproduce the badge exactly."
fi

if [[ ${#FAILED_RUNS[@]} -gt 0 ]]; then
  echo
  echo "!! Coverage is from a RED run — failing suites: ${FAILED_RUNS[*]}"
  echo "!! See $RESULTS_DIR/*.trx"
fi

if [[ $OPEN_REPORT -eq 1 ]]; then
  open "$REPORT_DIR/index.html"
fi

# Exit red if any suite failed, so the numbers are never read as a green run.
[[ ${#FAILED_RUNS[@]} -eq 0 ]]
