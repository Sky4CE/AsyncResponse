#!/usr/bin/env bash
# Run every test suite with coverage and open a browsable, dotCover-style HTML report.
#
#   ./scripts/coverage.sh          # everything: unit + integration, then opens the report
#
# Options (none are needed for the normal case):
#   --unit-only        skip the container-backed integration suite (fast, ~1 min, needs no Docker)
#   --all-frameworks   also run the unit suite on net8.0 (default is net10.0 only)
#   --framework <tfm>  run the unit suite on one specific framework
#   -c, --configuration <cfg>   build configuration (default: Debug — accurate line mapping)
#   --no-open          write the report but don't launch a browser
#
# Coverage is collected by Microsoft.Testing.Extensions.CodeCoverage (already referenced by both
# test projects) and rendered by ReportGenerator. Everything lands in TestResults/, which is
# gitignored.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

CONFIGURATION="Debug"
FRAMEWORKS=("net10.0")
RUN_INTEGRATION=1
OPEN_REPORT=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --unit-only|--no-integration) RUN_INTEGRATION=0; shift ;;
    --all-frameworks) FRAMEWORKS=("net8.0" "net10.0"); shift ;;
    --framework) FRAMEWORKS=("$2"); shift 2 ;;
    --configuration|-c) CONFIGURATION="$2"; shift 2 ;;
    --no-open) OPEN_REPORT=0; shift ;;
    # -E: BSD sed (macOS) has no \? in basic regex.
    -h|--help) sed -n '2,15p' "${BASH_SOURCE[0]}" | sed -E 's/^# ?//'; exit 0 ;;
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
# Report shape (filters, report types, badges) lives in coverage-report.sh, shared with CI so the
# README badge and this report can never disagree about what is being measured.
"$REPO_ROOT/scripts/coverage-report.sh" \
  "$RESULTS_DIR/*.cobertura.xml" \
  "$REPORT_DIR" \
  "$HISTORY_DIR"

head -20 "$REPORT_DIR/Summary.txt"
echo
echo "Full summary: $REPORT_DIR/Summary.txt"
echo "Report:       $REPORT_DIR/index.html"

[[ -n "$PARTIAL" ]] && { echo; echo "!! $PARTIAL"; }

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
