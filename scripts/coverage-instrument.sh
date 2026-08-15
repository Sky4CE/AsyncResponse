#!/usr/bin/env bash
# Statically instruments the AsyncResponse product assemblies in the given bin directories for
# code-coverage collection under `dotnet-coverage collect --session-id <id>`.
#
# Why not MTP's `--coverage` flag: on linux-x64 the Microsoft.Testing.Extensions.CodeCoverage
# extension prefers DYNAMIC instrumentation — a CLR profiler (libInstrumentationEngine.so,
# "vanguard") that rewrites method IL at JIT time — while on arm64 no native engine ships and the
# same flag silently uses static managed instrumentation. After round-24 innocently reshuffled
# method tokens in AsyncResponse.Channels.Redis, the dynamic rewriter began emitting invalid IL
# for RedisAsyncResponseChannel.SetResponseCore<T>'s async state machine: every typed publish
# threw System.InvalidProgramException — 23 red RedisChannelConformanceTests plus the SqlServer
# worker-roundtrip timeout in the CI data leg, reproducible only where the profiler actually
# runs (real x64 hardware; emulation loads no profiler, arm64 has none). Disabling the dynamic
# engine through `--coverage-settings` does not work: the mode flags reach the child process
# (CODE_COVERAGE_FLAGS drops the dynamic bit) but the controller still skips static
# instrumentation of every module on platforms it considers dynamic-capable, which would leave
# the leg silently uncovered. Pre-instrumenting on disk with dotnet-coverage and collecting via
# a session sidesteps the profiler entirely and measures identically on every OS/arch. The
# static pipeline is validated by JitProbeTests, which force-JITs every AsyncResponse method
# under the active coverage session.
#
# Usage: coverage-instrument.sh <session-id> <bin-dir> [<bin-dir>...]
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: $0 <session-id> <bin-dir> [<bin-dir>...]" >&2
  exit 2
fi

session="$1"
shift

for dir in "$@"; do
  if [ ! -d "$dir" ]; then
    echo "coverage-instrument: skipping missing directory $dir" >&2
    continue
  fi
  for dll in "$dir"/AsyncResponse*.dll; do
    [ -e "$dll" ] || continue
    case "$(basename "$dll")" in
      # The two test assemblies stay uninstrumented, matching the MTP extension's default of
      # excluding the test module itself. Everything else (Sample, ServiceDefaults, AppHost)
      # is part of the published coverage number, exactly as before.
      AsyncResponse.Tests.dll | AsyncResponse.IntegrationTests.dll) continue ;;
    esac
    dotnet-coverage instrument --session-id "$session" --output "$dll.instrumented" "$dll" > /dev/null
    mv "$dll.instrumented" "$dll"
    echo "instrumented $dll"
  done
done
