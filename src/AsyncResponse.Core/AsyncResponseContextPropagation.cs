namespace AsyncResponse;

/// <summary>
/// Aggregates the registered <see cref="IAsyncResponseContextPropagator"/>s into a single
/// capture/restore step used by the worker and lost-subscriber paths. With no propagators
/// registered it is a zero-overhead no-op, so the feature has no effect on apps that don't use it.
/// </summary>
internal sealed class AsyncResponseContextPropagation
{
    private readonly IReadOnlyList<IAsyncResponseContextPropagator> _propagators;

    public AsyncResponseContextPropagation(IEnumerable<IAsyncResponseContextPropagator> propagators)
        => _propagators = propagators as IReadOnlyList<IAsyncResponseContextPropagator> ?? propagators.ToArray();

    /// <summary>
    /// Captures the current ambient context from every propagator into a serializable carrier.
    /// Returns <c>null</c> when there are no propagators or none wrote anything, so the carrier is
    /// left off the wire payload entirely.
    /// </summary>
    public Dictionary<string, string>? Capture()
    {
        if (_propagators.Count == 0)
            return null;

        var carrier = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var propagator in _propagators)
            propagator.Capture(carrier);

        return carrier.Count == 0 ? null : carrier;
    }

    /// <summary>
    /// Restores ambient context from <paramref name="carrier"/> for the lifetime of the returned
    /// scope. A no-op when there are no propagators or the carrier is null/empty.
    /// </summary>
    public IDisposable Restore(IReadOnlyDictionary<string, string>? carrier)
    {
        if (_propagators.Count == 0 || carrier is null || carrier.Count == 0)
            return NullScope.Instance;

        List<IDisposable>? scopes = null;
        foreach (var propagator in _propagators)
        {
            var scope = propagator.Restore(carrier);
            if (scope is not null && !ReferenceEquals(scope, NullScope.Instance))
                (scopes ??= []).Add(scope);
        }

        return scopes is null ? NullScope.Instance : new CompositeScope(scopes);
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose() { }
    }

    private sealed class CompositeScope(List<IDisposable> _scopes) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            // Dispose in reverse order, mirroring nested using semantics.
            for (int i = _scopes.Count - 1; i >= 0; i--)
            {
                try { _scopes[i].Dispose(); }
                catch { /* a misbehaving propagator scope must not mask the others */ }
            }
        }
    }
}
