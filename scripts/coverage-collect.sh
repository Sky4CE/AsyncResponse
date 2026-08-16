#!/usr/bin/env bash
# Run one test command under coverlet and write a single cobertura file.
#
#   ./scripts/coverage-collect.sh <output.cobertura.xml> <instrument-dir> [--share <dir>]... -- <command> [args...]
#
# The single owner of WHAT gets instrumented, shared by scripts/coverage.sh and both CI legs, so the
# unit and integration cobertura files always describe the same set of assemblies.
#
# Why coverlet and not Microsoft's code coverage: on linux-x64 there is no Microsoft configuration
# that reports branches AND leaves IL intact. `dotnet-coverage instrument`, the pre-instrumentation
# the published report used to run on, drops condition data outright — an A/B on one test class emits
# 3,917 branch="True" lines through the collector and 0 through pre-instrumentation, and
# ReportGenerator, which counts per-line conditions rather than the derived (and vacuous) root
# branches-valid attribute, then omits branchcoverage from Summary.json altogether. Collecting
# without pre-instrumentation does emit conditions, but on linux-x64 that means the dynamic CLR
# profiler, whose JIT-time rewriter emitted invalid IL for
# RedisAsyncResponseChannel.SetResponseCore<T> (23 red RedisChannelConformanceTests plus a SqlServer
# worker-roundtrip timeout); dotnet-coverage 18.10.0 is still the current release with no fix, and
# the CLR Instrumentation Engine has a documented history of miscomputing maxstack into
# InvalidProgramException. Forcing the static engine through the settings XML instruments nothing at
# all and reports "Profiler was not initialized". Coverlet rewrites IL ahead of time with Cecil,
# never loads a CLR profiler, counts real branches, and measures identically on every OS and
# architecture — which is what the pre-instrumentation workaround was reaching for.
#
# The pipeline is still validated by JitProbeTests, which force-JITs every AsyncResponse method under
# the active collector — the probe that surfaces corrupt IL from ANY instrumenter, not just Microsoft's.
set -euo pipefail

OUTPUT="${1:?usage: coverage-collect.sh <output.cobertura.xml> <instrument-dir> [--share <dir>]... -- <command> [args...]}"
shift
INSTRUMENT_DIR="${1:?usage: coverage-collect.sh <output.cobertura.xml> <instrument-dir> [--share <dir>]... -- <command> [args...]}"
shift

SHARE_DIRS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --share) SHARE_DIRS+=("$2"); shift 2 ;;
    --) shift; break ;;
    *) echo "coverage-collect: unknown option: $1" >&2; exit 2 ;;
  esac
done

[[ $# -gt 0 ]] || { echo "coverage-collect: no command after --" >&2; exit 2; }

# coverlet writes the report after the target exits, so a target that dies before creating its own
# results directory would otherwise turn a test failure into a confusing write failure.
mkdir -p "$(dirname "$OUTPUT")"

# Global dotnet tools live here; non-login shells (CI, cron) don't get it on PATH.
export PATH="$PATH:$HOME/.dotnet/tools"

# The `--framework net8.0` in the install command is load-bearing, not tidiness. coverlet copies its
# hit tracker's IL out of the coverlet build that is RUNNING, so every assembly it rewrites picks up
# the System.Runtime version of the tool's OWN host framework. Install the tool on net10.0 and the
# net8.0 unit leg dies on the first typed publish with "Could not load file or assembly
# 'System.Runtime, Version=10.0.0.0'" — coverlet #1990, and the signature to look for if that
# message ever appears. The net8.0-hosted tool instruments both of this repo's target frameworks,
# because down-level framework references resolve upward and not the other way round.
COVERLET_INSTALL="dotnet tool install --global coverlet.console --version 10.0.1 --framework net8.0"
if ! command -v coverlet >/dev/null 2>&1; then
  echo "coverlet not found. Install it with:" >&2
  echo "  $COVERLET_INSTALL" >&2
  exit 1
fi

# The Aspire AppHost launches the sample apps as separate processes that load their OWN copies of the
# product assemblies, and coverlet instruments one file per module NAME — a second copy under another
# directory is silently skipped, so --include-directory cannot reach them. Linking those copies at the
# instrumented one puts every process on a single module, and the hits file path is baked into the
# module at instrumentation time, so all of them accumulate into it. The links are materialised back
# into real files on the way out, after coverlet has restored the originals.
SHARED_LINKS=()
materialise_shared_links() {
  local link target
  for link in ${SHARED_LINKS[@]+"${SHARED_LINKS[@]}"}; do
    [[ -L "$link" ]] || continue
    target="$(readlink "$link")"
    [[ -f "$target" ]] || { rm -f "$link"; continue; }
    rm -f "$link"
    cp "$target" "$link"
  done
}
trap materialise_shared_links EXIT

INSTRUMENT_DIR_ABS="$(cd "$INSTRUMENT_DIR" && pwd)"
for share_dir in ${SHARE_DIRS[@]+"${SHARE_DIRS[@]}"}; do
  if [[ ! -d "$share_dir" ]]; then
    echo "coverage-collect: --share directory does not exist: $share_dir" >&2
    exit 1
  fi
  for dll in "$share_dir"/AsyncResponse*.dll; do
    [[ -e "$dll" ]] || continue
    source_dll="$INSTRUMENT_DIR_ABS/$(basename "$dll")"
    [[ -f "$source_dll" ]] || continue
    # A divergent copy means the two projects no longer build the same assembly, and linking would
    # silently swap one binary for another. Stop rather than measure the wrong thing.
    if ! cmp -s "$source_dll" "$dll"; then
      echo "coverage-collect: $dll differs from $source_dll — refusing to link." >&2
      exit 1
    fi
    ln -sfn "$source_dll" "$dll"
    SHARED_LINKS+=("$dll")
  done
done

# --exclude-assemblies-without-sources None: CI sets ContinuousIntegrationBuild, which turns on
#   DeterministicSourcePaths, so every product PDB names its documents /_/src/... . Coverlet's default
#   heuristic reads that as "all sources missing" and drops the assembly without saying so.
# The two test assemblies stay uninstrumented, matching what the Microsoft extension excluded by
#   default. It is not cosmetic: coverlet's hit tracker roots itself on AppDomain.ProcessExit, so an
#   instrumented AsyncResponse.Tests pins the collectible AssemblyLoadContext that
#   CollectibleContextTypes_AreNotPinnedByResolutionCaches exists to prove is released.
coverlet "$INSTRUMENT_DIR" \
  --include "[AsyncResponse*]*" \
  --exclude "[AsyncResponse.Tests]*" \
  --exclude "[AsyncResponse.IntegrationTests]*" \
  --exclude-assemblies-without-sources None \
  --format cobertura \
  --output "$OUTPUT" \
  --target "$1" \
  --targetargs "${*:2}"
