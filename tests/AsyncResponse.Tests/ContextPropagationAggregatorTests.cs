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

    [Fact]
    public void Constructor_EnumeratesNonListInput_AndFourScopesContainDisposeFailures()
    {
        var events = new List<string>();
        var propagators = Enumerable.Range(1, 4).Select(index =>
            index == 3
                ? (IAsyncResponseContextPropagator)new ThrowingScopePropagator($"k{index}", $"v{index}")
                : new RecordingPropagator($"p{index}", $"k{index}", $"v{index}", events));
        var propagation = new AsyncResponseContextPropagation(propagators);

        var scope = propagation.Restore(new Dictionary<string, string> { ["present"] = "yes" });

        Assert.Null(Record.Exception(scope.Dispose));
        Assert.Null(Record.Exception(scope.Dispose));
        Assert.Equal(
            ["p1:restore", "p2:restore", "p4:restore", "p4:dispose", "p2:dispose", "p1:dispose"],
            events);
    }

    [Fact]
    public void Restore_WithTwoThrowingScopes_SwallowsBothFailuresAndDisposesOnlyOnce()
    {
        var propagation = new AsyncResponseContextPropagation(
        [
            new ThrowingScopePropagator("first", "1"),
            new ThrowingScopePropagator("second", "2")
        ]);
        var scope = propagation.Restore(new Dictionary<string, string> { ["present"] = "yes" });

        Assert.Null(Record.Exception(scope.Dispose));
        Assert.Null(Record.Exception(scope.Dispose));
    }

    // ---------------------------------------------------------------------------------------
    // Restore is all-or-nothing. A propagator that throws part-way leaves the earlier ones'
    // ambient state (principal, tenant, logging scope) attached with no scope in any caller's
    // hands to take it back off — the aggregator never returned one.
    // ---------------------------------------------------------------------------------------

    [Theory]
    [InlineData(1)] // the throw lands after `first` only
    [InlineData(2)] // after `first` + `second` — the fixed-field pair
    [InlineData(3)] // after the List-backed composite has formed
    public void Restore_WhenAPropagatorThrows_UnwindsTheAlreadyRestoredScopesInReverse(int restoredBeforeThrow)
    {
        var events = new List<string>();
        var propagators = new List<IAsyncResponseContextPropagator>();
        for (var i = 1; i <= restoredBeforeThrow; i++)
            propagators.Add(new RecordingPropagator($"p{i}", $"k{i}", $"v{i}", events));
        propagators.Add(new ThrowingRestorePropagator());

        var thrown = Record.Exception(
            () => propagation(propagators).Restore(new Dictionary<string, string> { ["present"] = "yes" }));

        // The fault still reaches the caller — a half-restored context is not a dispatch that
        // should quietly continue.
        Assert.IsType<InvalidOperationException>(thrown);
        Assert.Equal("restore failed", thrown.Message);

        // ...and everything that did restore was unwound, newest first.
        var expected = new List<string>();
        for (var i = 1; i <= restoredBeforeThrow; i++)
            expected.Add($"p{i}:restore");
        for (var i = restoredBeforeThrow; i >= 1; i--)
            expected.Add($"p{i}:dispose");

        Assert.Equal(expected, events);
    }

    [Fact]
    public void Restore_WhenUnwindingHitsAThrowingScope_StillDisposesTheRest()
    {
        // The unwind path gets the same treatment as normal disposal: one bad scope cannot strand
        // the others, and the original restore failure — not the disposal noise — is what surfaces.
        var events = new List<string>();
        var thrown = Record.Exception(() => propagation(
        [
            new RecordingPropagator("p1", "k1", "v1", events),
            new ThrowingScopePropagator("k2", "v2"),
            new ThrowingRestorePropagator(),
        ]).Restore(new Dictionary<string, string> { ["present"] = "yes" }));

        Assert.IsType<InvalidOperationException>(thrown);
        Assert.Equal("restore failed", thrown.Message);
        Assert.Equal(["p1:restore", "p1:dispose"], events);
    }

    [Fact]
    public void Restore_WithASingleThrowingScope_SwallowsTheDisposeFailureLikeEveryOtherCount()
    {
        // Disposal behavior must not depend on how many propagators happen to be registered.
        // Returning the lone scope unwrapped meant one propagator threw out of the caller's
        // `using` — at the worker ingress, failing (and so redelivering) a job that had already
        // run to completion — while two or more swallowed the same fault.
        var propagation = new AsyncResponseContextPropagation([new ThrowingScopePropagator("only", "1")]);

        var scope = propagation.Restore(new Dictionary<string, string> { ["present"] = "yes" });

        Assert.Null(Record.Exception(scope.Dispose));
        Assert.Null(Record.Exception(scope.Dispose)); // and stays idempotent
    }

    private static AsyncResponseContextPropagation propagation(IEnumerable<IAsyncResponseContextPropagator> propagators)
        => new(propagators);
}

/// <summary>A propagator that fails while restoring, to test partial-restore unwinding.</summary>
public sealed class ThrowingRestorePropagator : IAsyncResponseContextPropagator
{
    public void Capture(IDictionary<string, string> carrier) { }

    public IDisposable Restore(IReadOnlyDictionary<string, string> carrier)
        => throw new InvalidOperationException("restore failed");
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
