using System.Collections;
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
    public void Capture_CarrierSupportsDictionaryOperationsAndIsReusable()
    {
        var captures = new List<string>();
        var propagation = new AsyncResponseContextPropagation([new CarrierExercisePropagator(captures)]);

        var first = propagation.Capture();
        var second = propagation.Capture();

        Assert.NotNull(first);
        Assert.Equal("value", first!["final"]);
        Assert.NotNull(second);
        Assert.Equal("value", second!["final"]);
        Assert.Equal(["capture", "capture"], captures);
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
    public void Restore_WithThreeScopes_DisposesAllScopesInReverseOrderAndOnlyOnce()
    {
        var events = new List<string>();
        var propagation = new AsyncResponseContextPropagation(
        [
            new RecordingPropagator("p1", "k1", "v1", events),
            new RecordingPropagator("p2", "k2", "v2", events),
            new RecordingPropagator("p3", "k3", "v3", events),
        ]);

        var scope = propagation.Restore(new Dictionary<string, string> { ["present"] = "yes" });

        Assert.Equal(["p1:restore", "p2:restore", "p3:restore"], events);

        scope.Dispose();
        scope.Dispose();

        Assert.Equal(
            ["p1:restore", "p2:restore", "p3:restore", "p3:dispose", "p2:dispose", "p1:dispose"],
            events);
    }

    [Fact]
    public void Restore_WithSingleScope_DisposesThatScope()
    {
        var events = new List<string>();
        var propagation = new AsyncResponseContextPropagation(
            [new RecordingPropagator("p1", "k1", "v1", events)]);

        using (propagation.Restore(new Dictionary<string, string> { ["present"] = "yes" }))
        {
            Assert.Equal(["p1:restore"], events);
        }

        Assert.Equal(["p1:restore", "p1:dispose"], events);
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
    public void Restore_IsNoOp_WhenNoPropagatorsOrPropagatorsReturnNullScopes()
    {
        new AsyncResponseContextPropagation([])
            .Restore(new Dictionary<string, string> { ["present"] = "yes" })
            .Dispose();

        var events = new List<string>();
        var propagation = new AsyncResponseContextPropagation([new NullScopePropagator("p1", events)]);

        propagation
            .Restore(new Dictionary<string, string> { ["present"] = "yes" })
            .Dispose();

        Assert.Equal(["p1:restore"], events);
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

/// <summary>Exercises the capture carrier through the public dictionary contract.</summary>
public sealed class CarrierExercisePropagator(List<string> captures) : IAsyncResponseContextPropagator
{
    public void Capture(IDictionary<string, string> carrier)
    {
        Assert.False(carrier.IsReadOnly);
        Assert.Empty(carrier.Keys);
        Assert.Empty(carrier.Values);
        Assert.Empty(carrier);
        Assert.Empty(((IEnumerable)carrier).Cast<object>());
        Assert.True(carrier.Count == 0);
        Assert.False(carrier.ContainsKey("missing"));
        Assert.False(carrier.TryGetValue("missing", out var missing));
        Assert.Null(missing);
        Assert.False(carrier.Remove("missing"));
        Assert.False(carrier.Remove(new KeyValuePair<string, string>("missing", "value")));
        Assert.Throws<KeyNotFoundException>(() => _ = carrier["missing"]);
        carrier.CopyTo([], 0);

        carrier.Add("first", "1");
        carrier.Add(new KeyValuePair<string, string>("second", "2"));
        carrier["first"] = "1a";

        Assert.Equal(2, carrier.Count);
        Assert.Contains("first", carrier.Keys);
        Assert.Contains("second", carrier.Keys);
        Assert.Contains("1a", carrier.Values);
        Assert.Contains("2", carrier.Values);
        Assert.True(carrier.ContainsKey("first"));
        Assert.True(carrier.TryGetValue("first", out var first));
        Assert.Equal("1a", first);
        Assert.True(carrier.Contains(new KeyValuePair<string, string>("second", "2")));
        Assert.False(carrier.Contains(new KeyValuePair<string, string>("second", "wrong")));
        Assert.Contains(carrier, entry => entry.Key == "first" && entry.Value == "1a");
        Assert.Contains(
            ((IEnumerable)carrier).Cast<KeyValuePair<string, string>>(),
            entry => entry.Key == "second" && entry.Value == "2");

        var copied = new KeyValuePair<string, string>[2];
        carrier.CopyTo(copied, 0);
        Assert.Contains(copied, entry => entry.Key == "first" && entry.Value == "1a");
        Assert.Contains(copied, entry => entry.Key == "second" && entry.Value == "2");

        Assert.False(carrier.Remove(new KeyValuePair<string, string>("first", "wrong")));
        Assert.True(carrier.Remove(new KeyValuePair<string, string>("first", "1a")));
        Assert.False(carrier.ContainsKey("first"));
        Assert.True(carrier.Remove("second"));
        Assert.True(carrier.Count == 0);

        carrier.Add("clear-me", "x");
        carrier.Clear();
        Assert.True(carrier.Count == 0);

        carrier["final"] = "value";
        captures.Add("capture");
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> carrier) => throw new NotSupportedException();
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

/// <summary>A propagator that participates in restore but contributes no active scope.</summary>
public sealed class NullScopePropagator(string name, List<string> events) : IAsyncResponseContextPropagator
{
    public void Capture(IDictionary<string, string> carrier) { }

    public IDisposable Restore(IReadOnlyDictionary<string, string> carrier)
    {
        events.Add($"{name}:restore");
        return null!;
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
