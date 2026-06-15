namespace AsyncResponse;

/// <summary>
/// Captures application ambient context — trace id, principal/tenant, culture, a logging scope, … —
/// when an async-response wait is armed or a worker job is enqueued, and restores it before the
/// matching worker job or lost-subscriber recovery callback runs. The carrier is string→string so it
/// serializes across process and deployment boundaries (broker-backed workers, recovery after a
/// redeploy).
/// <para>
/// In-process hops (the built-in response handlers and the in-memory worker) flow ambient
/// <see cref="System.Threading.AsyncLocal{T}"/> state automatically via a captured
/// <see cref="System.Threading.ExecutionContext"/>, so propagators are only needed for context that
/// must cross serialization. Each propagator should namespace its carrier keys to avoid collisions
/// with others. Register propagators with <c>AddAsyncResponse().WithContextPropagator&lt;T&gt;()</c>.
/// </para>
/// </summary>
public interface IAsyncResponseContextPropagator
{
    /// <summary>
    /// Writes the current ambient values into <paramref name="carrier"/>. Called once when the wait
    /// is armed or the worker job is enqueued; write nothing when there is no ambient context.
    /// </summary>
    void Capture(IDictionary<string, string> carrier);

    /// <summary>
    /// Restores ambient state from <paramref name="carrier"/> for the lifetime of the returned
    /// scope, which the library disposes once processing completes. Return a no-op disposable when
    /// the carrier holds nothing relevant to this propagator.
    /// </summary>
    IDisposable Restore(IReadOnlyDictionary<string, string> carrier);
}
