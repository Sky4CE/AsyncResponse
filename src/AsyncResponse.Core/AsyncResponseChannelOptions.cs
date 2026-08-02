namespace AsyncResponse;

/// <summary>
/// Shared options for an async-response channel (the response/recovery substrate). Concrete channels
/// extend this with their own transport-specific settings (key/subject prefixes, bucket names, …).
/// </summary>
public abstract class AsyncResponseChannelOptions
{
    /// <summary>
    /// How long persisted <see cref="RecoveryState"/> entries live, bounding how long after a
    /// crash/redeploy a late response can still trigger the lost-subscriber callbacks. Set it
    /// comfortably above your longest-running flow. (For the in-memory channel this is process-local
    /// and lost on exit.)
    /// </summary>
    public TimeSpan RecoveryStateExpiry { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Default timeout applied to waiters that do not specify <c>WithTimeout</c>. When <c>null</c>
    /// (the default), <see cref="RecoveryStateExpiry"/> is used — waits are never infinite, so a
    /// response that never arrives faults the waiter with a <see cref="TimeoutException"/> instead of
    /// hanging forever.
    /// </summary>
    public TimeSpan? DefaultTimeout { get; set; }

    /// <summary>
    /// How long disposing a waiter may DRAIN an in-flight delivery before abandoning it. Disposal
    /// settles the response task only after a dispatch that already claimed a message — an
    /// <c>Until</c> predicate running user code — has finished, so a delivered response is never
    /// reported as canceled. If that dispatch is still running when this budget lapses, the
    /// response task is faulted with <see cref="AsyncResponseIndeterminateDeliveryException"/>
    /// rather than canceled: the delivery outcome is unknown, and a cancellation would invite
    /// re-attaching to a correlation id whose response may already be consumed (durable flows
    /// instead restart the idempotent step fresh). Default: 30 seconds — comfortably above any
    /// healthy predicate, well below a stuck one holding host shutdown hostage.
    /// </summary>
    public TimeSpan DisposalDrainTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The largest delay the BCL's timer plumbing accepts (<c>uint.MaxValue - 1</c> ms, ~49.7
    /// days): CancellationTokenSource timers, <see cref="Task.Delay(TimeSpan)"/>, and
    /// <c>Task.WaitAsync</c> all reject longer values at arming — which, for a waiter, is AFTER
    /// the subscription and recovery state already exist. Timer-armed values are therefore
    /// bounded here and at waiter creation instead.
    /// </summary>
    internal static readonly TimeSpan MaxTimerBackedTimeout = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>
    /// Guards a RESOLVED per-waiter timeout (explicit, <see cref="DefaultTimeout"/>, or the
    /// <see cref="RecoveryStateExpiry"/> fallback) before any subscribe/persist side effect:
    /// positive (one rule for every channel — zero and the never-firing -1 ms sentinel included)
    /// and under the timer ceiling.
    /// </summary>
    internal static void EnsureWaiterTimeoutSupported(TimeSpan timeout)
    {
        if (timeout <= TimeSpan.Zero || timeout > MaxTimerBackedTimeout)
            throw new ArgumentOutOfRangeException(
                nameof(timeout),
                timeout,
                $"Waiter timeout must be positive and at most {MaxTimerBackedTimeout.TotalDays:0.#} days (the .NET timer ceiling).");
    }

    /// <summary>
    /// Validates the shared channel settings, throwing an actionable
    /// <see cref="InvalidOperationException"/> on misconfiguration. Concrete channels call this from
    /// their own <c>Validate()</c> so every channel fails fast at registration/startup instead of
    /// misbehaving at the first wait (a non-positive expiry silently disables recovery; a bad
    /// timer-armed timeout otherwise surfaces only after waiter-registration side effects).
    /// </summary>
    internal void ValidateShared(string optionsName)
    {
        static string CeilingRule(string optionsName, string knob)
            => $"{optionsName}.{knob} must be positive and at most {MaxTimerBackedTimeout.TotalDays:0.#} days (the .NET timer ceiling).";

        if (RecoveryStateExpiry <= TimeSpan.Zero)
            throw new InvalidOperationException($"{optionsName}.{nameof(RecoveryStateExpiry)} must be positive.");

        // The ceiling applies to the expiry only in its TIMER-ARMED role — the waiter-timeout
        // fallback when DefaultTimeout is not configured. As a pure persistence TTL (with a
        // DefaultTimeout set) it may legitimately exceed it: e.g. 90-day recovery retention with
        // a 12-hour default timeout.
        if (DefaultTimeout is null && RecoveryStateExpiry > MaxTimerBackedTimeout)
            throw new InvalidOperationException(
                $"{optionsName}.{nameof(RecoveryStateExpiry)} is the waiter-timeout fallback while {nameof(DefaultTimeout)} is " +
                $"not configured, and must then be at most {MaxTimerBackedTimeout.TotalDays:0.#} days (the .NET timer ceiling). " +
                $"Configure {nameof(DefaultTimeout)} to keep a longer recovery retention.");

        if (DefaultTimeout is { } defaultTimeout && (defaultTimeout <= TimeSpan.Zero || defaultTimeout > MaxTimerBackedTimeout))
            throw new InvalidOperationException(CeilingRule(optionsName, nameof(DefaultTimeout)));
        if (DisposalDrainTimeout <= TimeSpan.Zero || DisposalDrainTimeout > MaxTimerBackedTimeout)
            throw new InvalidOperationException(CeilingRule(optionsName, nameof(DisposalDrainTimeout)));
    }
}

/// <summary>
/// Shared options for a <em>durable</em> async-response channel that serializes failures onto the
/// wire (Redis, NATS). Adds the remote stack-trace policy on top of
/// <see cref="AsyncResponseChannelOptions"/>.
/// </summary>
public abstract class DurableAsyncResponseChannelOptions : AsyncResponseChannelOptions
{
    /// <summary>
    /// Whether a failed response envelope carries the remote exception's stack trace on the wire
    /// (surfaced to the waiter via <c>Exception.Data["RemoteStackTrace"]</c>). Stack traces aid
    /// debugging but can carry file paths; set to <c>false</c> to omit them. Default: <c>true</c>.
    /// </summary>
    public bool IncludeRemoteStackTrace { get; set; } = true;

    /// <summary>
    /// Maximum length, in characters, of a remote stack trace placed on the wire and restored on the
    /// waiter side; longer traces are truncated with a marker. Bounds what a buggy or hostile remote
    /// can push into logs (a multi-megabyte trace). Applied on both publish and receive. Default: 16384.
    /// </summary>
    public int MaxRemoteStackTraceLength { get; set; } = 16 * 1024;
}
