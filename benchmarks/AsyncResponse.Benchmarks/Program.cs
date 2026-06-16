using AsyncResponse.Benchmarks;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Exporters.Json;
using BenchmarkDotNet.Running;

// AsyncResponse benchmarks + load/stress harness.
//
//   dotnet run -c Release                                            → run all BenchmarkDotNet benchmarks
//   dotnet run -c Release -- --filter *Channel*                      → run a subset (BDN filter)
//   dotnet run -c Release -- stress                                  → run the concurrency/load scenarios
//   dotnet run -c Release -- stress --concurrency 512 --count 100000 --progress 5
//
// Benchmarks measure per-operation latency + allocations + GC (BenchmarkDotNet, [MemoryDiagnoser]).
// The stress runner hammers the library at high concurrency, asserts correctness (no lost/crossed
// responses, no duplicate work, no hangs), and reports throughput, latency percentiles, allocations,
// GC counts and working set. Its process exit code is non-zero if any correctness check fails.
if (args is [var command, ..] && string.Equals(command, "stress", StringComparison.OrdinalIgnoreCase))
{
    return await StressRunner.RunAsync(args[1..]);
}

// Always emit the full-compressed JSON report that github-action-benchmark consumes, on top of the
// default (markdown/HTML) exporters.
var config = ManualConfig.Create(DefaultConfig.Instance).AddExporter(JsonExporter.FullCompressed);
BenchmarkSwitcher.FromAssembly(typeof(StressRunner).Assembly).Run(args, config);
return 0;
