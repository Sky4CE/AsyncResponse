using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Unit tests for <see cref="AsyncResponseContextPropagation"/>, which composes the registered
/// propagators into a single capture/restore step.
/// </summary>
public class ContextPropagationAggregatorTests
{
    [Fact]
    public void Capture_AggregatesAllPropagators()
    {
        var events = new List<string>();
        var propagation = new AsyncResponseContextPropagation(
        [
            new RecordingPropagator("p1", "k1", "v1", events),
            new RecordingPropagator("p2", "k2", "v2", events),
        ]);

        var carrier = propagation.Capture();

        Assert.NotNull(carrier);
        Assert.Equal("v1", carrier!["k1"]);
        Assert.Equal("v2", carrier["k2"]);
        Assert.Equal(["p1:capture", "p2:capture"], events);
    }

    [Fact]
    public void Capture_ReturnsNull_WhenNoPropagators()
    {
        var propagation = new AsyncResponseContextPropagation([]);
        Assert.Null(propagation.Capture());
    }

    [Fact]
    public void Capture_ReturnsNull_WhenNothingWritten()
    {
        // A propagator with no ambient value writes nothing → empty carrier → omitted from the wire.
        var propagation = new AsyncResponseContextPropagation([new RecordingPropagator("p1", "k1", value: null, [])]);
        Assert.Null(propagation.Capture());
    }

    [Fact]
    public void Restore_DisposesScopesInReverseOrder()
    {
        var events = new List<string>();
        var propagation = new AsyncResponseContextPropagation(
        [
            new RecordingPropagator("p1", "k1", "v1", events),
            new RecordingPropagator("p2", "k2", "v2", events),
        ]);
        var carrier = propagation.Capture()!;
        events.Clear();

        using (propagation.Restore(carrier))
        {
            Assert.Equal(["p1:restore", "p2:restore"], events);
        }

        // Disposed in reverse registration order — nested-using semantics.
        Assert.Equal(["p1:restore", "p2:restore", "p2:dispose", "p1:dispose"], events);
    }

    [Fact]
    public void Restore_IsNoOp_WhenCarrierNullOrEmpty()
    {
        var events = new List<string>();
        var propagation = new AsyncResponseContextPropagation([new RecordingPropagator("p1", "k1", "v1", events)]);

        propagation.Restore(null).Dispose();
        propagation.Restore(new Dictionary<string, string>()).Dispose();

        Assert.Empty(events); // the propagators are never touched
    }

    [Fact]
    public void Restore_SwallowsScopeDisposeExceptions()
    {
        var events = new List<string>();
        var propagation = new AsyncResponseContextPropagation(
        [
            new RecordingPropagator("p1", "k1", "v1", events),
            new ThrowingScopePropagator("k2", "v2"),
        ]);
        var carrier = propagation.Capture()!;
        var scope = propagation.Restore(carrier);
        events.Clear();

        var ex = Record.Exception(scope.Dispose);

        Assert.Null(ex);                       // a misbehaving scope must not surface
        Assert.Equal(["p1:dispose"], events);  // and must not prevent the others from disposing
    }
}

/// <summary>Records capture/restore/dispose events into a shared list; instance-based (parallel-safe).</summary>
public sealed class RecordingPropagator(string name, string key, string? value, List<string> events) : IAsyncResponseContextPropagator
{
    public void Capture(IDictionary<string, string> carrier)
    {
        events.Add($"{name}:capture");
        if (value is not null) carrier[key] = value;
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> carrier)
    {
        events.Add($"{name}:restore");
        return new Scope(name, events);
    }

    private sealed class Scope(string _name, List<string> _events) : IDisposable
    {
        public void Dispose() => _events.Add($"{_name}:dispose");
    }
}

/// <summary>A propagator whose restore scope throws on dispose, to test composite resilience.</summary>
public sealed class ThrowingScopePropagator(string key, string value) : IAsyncResponseContextPropagator
{
    public void Capture(IDictionary<string, string> carrier) => carrier[key] = value;

    public IDisposable Restore(IReadOnlyDictionary<string, string> carrier) => new ThrowingScope();

    private sealed class ThrowingScope : IDisposable
    {
        public void Dispose() => throw new InvalidOperationException("scope dispose failed");
    }
}
