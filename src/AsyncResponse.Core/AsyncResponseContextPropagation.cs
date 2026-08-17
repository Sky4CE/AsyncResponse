using Microsoft.Extensions.Logging;
using System.Collections;

namespace AsyncResponse;

/// <summary>
/// Aggregates the registered <see cref="IAsyncResponseContextPropagator"/>s into a single
/// capture/restore step used by the worker and lost-subscriber paths. With no propagators
/// registered it is a zero-overhead no-op, so the feature has no effect on apps that don't use it.
/// </summary>
internal sealed class AsyncResponseContextPropagation
{
    private readonly IReadOnlyList<IAsyncResponseContextPropagator> _propagators;
    private readonly ILogger? _logger;

    /// <summary>Runs the AsyncResponseContextPropagation operation.</summary>
    public AsyncResponseContextPropagation(
        IEnumerable<IAsyncResponseContextPropagator> propagators,
        ILogger<AsyncResponseContextPropagation>? logger = null)
    {
        _propagators = propagators as IReadOnlyList<IAsyncResponseContextPropagator> ?? propagators.ToArray();
        _logger = logger;
    }

    /// <summary>
    /// Captures the current ambient context from every propagator into a serializable carrier.
    /// Returns <c>null</c> when there are no propagators or none wrote anything, so the carrier is
    /// left off the wire payload entirely.
    /// </summary>
    public Dictionary<string, string>? Capture()
    {
        var propagators = _propagators;
        var count = propagators.Count;
        if (count == 0)
            return null;

        // Hand each propagator a lazy carrier instead of a fully-formed Dictionary. The backing
        // dictionary is allocated only when a propagator actually writes a value, so the very
        // common "nothing to capture" path (no ambient trace/tenant/etc. set) allocates nothing.
        // The carrier wrapper itself is pooled per thread and never escapes this synchronous call,
        // so even when a propagator does write, the only surviving allocation is the dictionary
        // that gets returned — no wrapper and no enumerator. Indexing the list avoids the
        // IEnumerator<T> boxing that a foreach over an interface-typed list would incur.
        var carrier = LazyCapturingCarrier.Rent(count);
        for (var i = 0; i < count; i++)
            propagators[i].Capture(carrier);

        return carrier.Detach();
    }

    /// <summary>
    /// Restores ambient context from <paramref name="carrier"/> for the lifetime of the returned
    /// scope. A no-op when there are no propagators or the carrier is null/empty.
    /// </summary>
    public IDisposable Restore(IReadOnlyDictionary<string, string>? carrier)
    {
        var propagators = _propagators;
        var count = propagators.Count;
        if (count == 0 || carrier is null || carrier.Count == 0)
            return NullScope.Instance;

        // Collect the real scopes without allocating until we genuinely need a composite. The 0/1/2
        // active-scope cases — by far the most common — return either the shared no-op, a single
        // guarded scope, or a fixed two-field CompositeScope2. Only 3+ active propagators fall back
        // to the List-backed CompositeScope. Indexing the list avoids the foreach enumerator alloc.
        IDisposable? first = null;
        IDisposable? second = null;
        List<IDisposable>? more = null;

        for (var i = 0; i < count; i++)
        {
            IDisposable? scope;
            try
            {
                scope = propagators[i].Restore(carrier);
            }
            catch
            {
                // Propagator i threw part-way through the set. Everything 0..i-1 already mutated
                // ambient state (principal, tenant, logging scope) and only its scope can put that
                // back — and no caller can dispose what was never returned. Unwind here, in reverse,
                // then let the fault out: an aborted dispatch must not leave a half-restored
                // identity attached to the thread for whatever the pool runs next.
                DisposeInReverse(first, second, more);
                throw;
            }

            if (scope is null || ReferenceEquals(scope, NullScope.Instance))
                continue;

            if (more is not null)
                more.Add(scope);
            else if (first is null)
                first = scope;
            else if (second is null)
                second = scope;
            else
                more = [first, second, scope];
        }

        if (more is not null)
            return new CompositeScope(more, _logger);

        if (second is not null)
            return new CompositeScope2(first!, second, _logger);

        // Wrapped even though it is a single scope: returning it raw made disposal behavior depend
        // on how many propagators happen to be registered — with one, a throwing Dispose escaped
        // the caller's `using` and failed a dispatch whose work had already completed (at the
        // worker ingress, that redelivers an executed job); with two or more it vanished silently.
        // One rule for every count: cleanup never fails the dispatch, and never fails invisibly.
        return first is null ? NullScope.Instance : new GuardedScope(first, _logger);
    }

    private void DisposeInReverse(IDisposable? first, IDisposable? second, List<IDisposable>? more)
    {
        if (more is not null)
        {
            for (var i = more.Count - 1; i >= 0; i--)
                SafeDispose(more[i], _logger);

            return;
        }

        if (second is not null)
            SafeDispose(second, _logger);
        if (first is not null)
            SafeDispose(first, _logger);
    }

    /// <summary>
    /// Disposes one scope without letting its failure mask the others or the dispatch. Logged
    /// rather than swallowed: a propagator that cannot detach its ambient state is leaking it into
    /// whatever the thread runs next, which is precisely the kind of fault that has to be visible.
    /// </summary>
    private static void SafeDispose(IDisposable scope, ILogger? logger)
    {
        try
        {
            scope.Dispose();
        }
        catch (Exception ex)
        {
            logger?.LogError(
                ex,
                "An AsyncResponse context propagator scope ({ScopeType}) failed to dispose; ambient context it restored may still be attached to this thread.",
                scope.GetType().FullName);
        }
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        /// <summary>Releases resources held by this instance.</summary>
        public void Dispose() { }
    }

    /// <summary>
    /// Guards a single propagator scope so its disposal obeys the same rule as the multi-scope
    /// cases: a failure is reported, never thrown at the dispatch that was already finishing.
    /// </summary>
    private sealed class GuardedScope(IDisposable _scope, ILogger? _logger) : IDisposable
    {
        private int _disposed;

        /// <summary>Releases resources held by this instance.</summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            SafeDispose(_scope, _logger);
        }
    }

    /// <summary>
    /// Disposes exactly two scopes in reverse order, mirroring nested <c>using</c> semantics without
    /// the <see cref="List{T}"/> + general <see cref="CompositeScope"/> allocation that the 1–2
    /// propagator case (the overwhelmingly common one) would otherwise pay.
    /// </summary>
    private sealed class CompositeScope2(IDisposable _first, IDisposable _second, ILogger? _logger) : IDisposable
    {
        private int _disposed;

        /// <summary>Releases resources held by this instance.</summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            // A misbehaving propagator scope must not mask the other — but it must not vanish
            // either, so SafeDispose logs what it absorbs.
            SafeDispose(_second, _logger);
            SafeDispose(_first, _logger);
        }
    }

    private sealed class CompositeScope(List<IDisposable> _scopes, ILogger? _logger) : IDisposable
    {
        private int _disposed;

        /// <summary>Releases resources held by this instance.</summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            // Dispose in reverse order, mirroring nested using semantics.
            for (int i = _scopes.Count - 1; i >= 0; i--)
                SafeDispose(_scopes[i], _logger);
        }
    }

    /// <summary>
    /// An <see cref="IDictionary{TKey,TValue}"/> facade that defers creating its backing
    /// <see cref="Dictionary{TKey,TValue}"/> until the first mutation. Reads before any write see an
    /// empty map; the first write allocates a right-sized dictionary. A single instance is reused per
    /// thread (the carrier never escapes the synchronous <see cref="Capture"/> call), so capturing
    /// context adds no wrapper allocation on the hot path — only the backing dictionary, and only
    /// when a propagator writes.
    /// </summary>
    private sealed class LazyCapturingCarrier : IDictionary<string, string>
    {
        [ThreadStatic] private static LazyCapturingCarrier? _cached;

        // Shared, never-mutated empty map backing every read taken before the first write.
        private static readonly Dictionary<string, string> EmptyView = new(0, StringComparer.Ordinal);

        private Dictionary<string, string>? _inner;
        private int _capacity;

        private LazyCapturingCarrier(int capacity) => _capacity = capacity;

        /// <summary>
        /// Borrows the thread's pooled carrier (or allocates one on first use / under re-entrancy).
        /// The slot is cleared while borrowed so a re-entrant capture gets its own instance rather
        /// than corrupting the borrowed one.
        /// </summary>
        public static LazyCapturingCarrier Rent(int capacity)
        {
            var carrier = _cached;
            if (carrier is null)
                return new LazyCapturingCarrier(capacity);

            _cached = null;
            carrier._capacity = capacity;
            return carrier;
        }

        /// <summary>
        /// Returns the captured dictionary (or <c>null</c> when nothing was written), resets the
        /// carrier, and returns it to the thread pool for the next capture.
        /// </summary>
        public Dictionary<string, string>? Detach()
        {
            var inner = _inner;
            _inner = null;
            _cached = this;
            return inner is { Count: > 0 } ? inner : null;
        }

        private Dictionary<string, string> EnsureWritable()
            => _inner ??= new Dictionary<string, string>(_capacity, StringComparer.Ordinal);

        private Dictionary<string, string> ReadView => _inner ?? EmptyView;

        public string this[string key]
        {
            get => ReadView[key];
            set => EnsureWritable()[key] = value;
        }

        public ICollection<string> Keys => ReadView.Keys;
        public ICollection<string> Values => ReadView.Values;
        public int Count => _inner?.Count ?? 0;
        public bool IsReadOnly => false;

        /// <summary>Runs the Add operation.</summary>
        public void Add(string key, string value) => EnsureWritable().Add(key, value);

        /// <summary>Runs the Add operation.</summary>
        public void Add(KeyValuePair<string, string> item)
            => ((ICollection<KeyValuePair<string, string>>)EnsureWritable()).Add(item);

        /// <summary>Runs the ContainsKey operation.</summary>
        public bool ContainsKey(string key) => _inner?.ContainsKey(key) ?? false;

        /// <summary>Runs the Contains operation.</summary>
        public bool Contains(KeyValuePair<string, string> item)
            => _inner is { } inner && ((ICollection<KeyValuePair<string, string>>)inner).Contains(item);

        /// <summary>Runs the TryGetValue operation.</summary>
        public bool TryGetValue(string key, out string value)
        {
            if (_inner is { } inner)
                return inner.TryGetValue(key, out value!);

            value = null!;
            return false;
        }

        /// <summary>Runs the Remove operation.</summary>
        public bool Remove(string key) => _inner?.Remove(key) ?? false;

        /// <summary>Runs the Remove operation.</summary>
        public bool Remove(KeyValuePair<string, string> item)
            => _inner is { } inner && ((ICollection<KeyValuePair<string, string>>)inner).Remove(item);

        /// <summary>Runs the Clear operation.</summary>
        public void Clear() => _inner?.Clear();

        /// <summary>Runs the CopyTo operation.</summary>
        public void CopyTo(KeyValuePair<string, string>[] array, int arrayIndex)
            => ((ICollection<KeyValuePair<string, string>>)ReadView).CopyTo(array, arrayIndex);

        /// <summary>Runs the GetEnumerator operation.</summary>
        public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => ReadView.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
