using BenchmarkDotNet.Attributes;

namespace AsyncResponse.Benchmarks;

/// <summary>Costs of the optional context propagation layer used by broker workers and recovery.</summary>
[MemoryDiagnoser]
public class ContextPropagationBenchmarks
{
    private AsyncResponseContextPropagation _none = null!;
    private AsyncResponseContextPropagation _twoPropagators = null!;
    private Dictionary<string, string> _carrier = null!;

    [GlobalSetup]
    public void Setup()
    {
        _none = new AsyncResponseContextPropagation([]);
        _twoPropagators = new AsyncResponseContextPropagation(
        [
            new BenchTracePropagator(),
            new BenchTenantPropagator()
        ]);

        BenchAmbient.Trace.Value = "trace-bench";
        BenchAmbient.Tenant.Value = "tenant-bench";
        _carrier = _twoPropagators.Capture()!;
    }

    [Benchmark(Baseline = true)]
    public Dictionary<string, string>? Capture_NoPropagators() => _none.Capture();

    [Benchmark]
    public Dictionary<string, string>? Capture_TwoPropagators() => _twoPropagators.Capture();

    [Benchmark]
    public string? Restore_TwoPropagators()
    {
        using var _ = _twoPropagators.Restore(_carrier);
        return BenchAmbient.Trace.Value;
    }

    private static class BenchAmbient
    {
        public static readonly AsyncLocal<string?> Trace = new();
        public static readonly AsyncLocal<string?> Tenant = new();
    }

    private sealed class BenchTracePropagator : IAsyncResponseContextPropagator
    {
        public void Capture(IDictionary<string, string> carrier)
        {
            if (BenchAmbient.Trace.Value is { } trace)
                carrier["trace"] = trace;
        }

        public IDisposable Restore(IReadOnlyDictionary<string, string> carrier)
            => new RestoreScope(BenchAmbient.Trace, carrier.GetValueOrDefault("trace"));
    }

    private sealed class BenchTenantPropagator : IAsyncResponseContextPropagator
    {
        public void Capture(IDictionary<string, string> carrier)
        {
            if (BenchAmbient.Tenant.Value is { } tenant)
                carrier["tenant"] = tenant;
        }

        public IDisposable Restore(IReadOnlyDictionary<string, string> carrier)
            => new RestoreScope(BenchAmbient.Tenant, carrier.GetValueOrDefault("tenant"));
    }

    private sealed class RestoreScope : IDisposable
    {
        private readonly AsyncLocal<string?> _slot;
        private readonly string? _previous;

        public RestoreScope(AsyncLocal<string?> slot, string? value)
        {
            _slot = slot;
            _previous = slot.Value;
            slot.Value = value;
        }

        public void Dispose() => _slot.Value = _previous;
    }
}
