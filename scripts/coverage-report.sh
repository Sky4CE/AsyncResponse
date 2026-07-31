#!/usr/bin/env bash
# Render a merged coverage report from one or more cobertura files.
#
#   ./scripts/coverage-report.sh <cobertura-glob> <target-dir> [history-dir]
#
# The single owner of the report's shape — which assemblies count, which files are excluded, and
# the shields.io badge JSON. Both scripts/coverage.sh (local) and .github/workflows/ci.yml (the
# published badge) go through here, so a locally rendered report and the badge on the README can
# never disagree about what "coverage" means.
set -euo pipefail

REPORTS="${1:?usage: coverage-report.sh <cobertura-glob> <target-dir> [history-dir]}"
TARGET_DIR="${2:?usage: coverage-report.sh <cobertura-glob> <target-dir> [history-dir]}"
HISTORY_DIR="${3:-}"

# Global dotnet tools live here; non-login shells (CI, cron) don't get it on PATH.
export PATH="$PATH:$HOME/.dotnet/tools"
if ! command -v reportgenerator >/dev/null 2>&1; then
  echo "reportgenerator not found. Install it with:" >&2
  echo "  dotnet tool install --global dotnet-reportgenerator-globaltool" >&2
  exit 1
fi

# The glob is deliberately non-recursive at the call sites: --report-trx also copies each coverage
# file into its attachment folder, and a recursive glob would parse every one of them twice.
# Filters keep the report to shipped code — test, benchmark and sample assemblies and generated
# sources are not the thing being measured.
reportgenerator \
  -reports:"$REPORTS" \
  -targetdir:"$TARGET_DIR" \
  ${HISTORY_DIR:+-historydir:"$HISTORY_DIR"} \
  -reporttypes:"Html;TextSummary;JsonSummary;MarkdownSummaryGithub" \
  -title:"AsyncResponse" \
  -assemblyfilters:"+AsyncResponse*;-AsyncResponse.Tests;-AsyncResponse.IntegrationTests*;-AsyncResponse.LoadTests;-AsyncResponse.Sample*" \
  -filefilters:"-*/obj/*;-*.g.cs;-*.Designer.cs"

# shields.io endpoint badges, one per metric, written next to the report so publishing the report
# directory publishes the badges with it.
python3 - "$TARGET_DIR" <<'PY'
import json, sys, pathlib

target = pathlib.Path(sys.argv[1])
summary = json.loads((target / "Summary.json").read_text())["summary"]


def color(pct):
    for threshold, name in ((90, "brightgreen"), (80, "green"), (70, "yellowgreen"),
                            (60, "yellow"), (50, "orange")):
        if pct >= threshold:
            return name
    return "red"


for key, label, filename in (
    ("linecoverage", "line coverage", "badge-line.json"),
    ("branchcoverage", "branch coverage", "badge-branch.json"),
):
    pct = summary[key]
    (target / filename).write_text(json.dumps({
        "schemaVersion": 1,
        "label": label,
        "message": f"{pct}%",
        "color": color(pct),
    }))
    print(f"{label}: {pct}%")
PY
