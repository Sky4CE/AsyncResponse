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
    /// Upper bound for persisted-TTL knobs (10 years): far beyond any practical retention, small
    /// enough that the "now + TTL" expiry stamp every store computes can never overflow
    /// <see cref="DateTime"/>/<see cref="DateTimeOffset"/> arithmetic — a
    /// <see cref="TimeSpan.MaxValue"/> expiry used to pass validation and then throw
    /// <see cref="ArgumentOutOfRangeException"/> at the first recovery-state save.
    /// </summary>
    internal static readonly TimeSpan MaxPersistenceTtl = TimeSpan.FromDays(3650);

    /// <summary>
    /// The portable correlation-id length in UTF-16 code units — the width of the
    /// <c>correlation_id</c> column in the SQL Server channel's tables, and the tightest bound any
    /// bundled channel imposes. Validated where a correlation id enters the library so an
    /// over-long id is rejected at the call site instead of truncating or failing at its first
    /// database write, halfway through a conversation.
    /// </summary>
    public const int MaxCorrelationIdLength = 400;

    /// <summary>
    /// Applies the portable correlation-id contract, returning the rejection message or
    /// <c>null</c>. Length is bounded by the relational column; surrounding spaces are rejected
    /// because SQL Server pads the shorter operand of an equality comparison — even under a binary
    /// collation — so <c>"abc "</c> and <c>"abc"</c> are ONE key to the database while the library
    /// compares them ordinally and would route their responses to different waiters; control
    /// characters are rejected because the stores disagree about them (see
    /// <see cref="PortableText.IndexOfControlCharacter"/>).
    /// <para>
    /// The rules kept in <see cref="PortableText"/> are the ones this contract genuinely SHARES
    /// with <c>FlowStateConcurrency.FlowIdNotPortable</c>. Two flow-id rules are deliberately not
    /// mirrored here because correlation ids have no such sink: the UTF-8 byte cap exists for
    /// Cosmos DB item ids, and the <c>/ \ ? #</c> character set is Cosmos's id syntax — no bundled
    /// channel persists a correlation id to Cosmos. Adding either here would reject ids that every
    /// shipped channel handles correctly.
    /// </para>
    /// </summary>
    internal static string? CorrelationIdNotPortable(string correlationId)
    {
        // Length-guarded before the [0]/[^1] probe below: this method's contract is to RETURN a
        // rejection for an unusable id, and an empty one indexing off the end would throw
        // IndexOutOfRangeException out of the very method whose job is to explain bad ids.
        if (correlationId.Length == 0)
            return "CorrelationId is empty. An id must carry enough information to route a response back to its waiter.";

        if (correlationId.Length > MaxCorrelationIdLength)
        {
            return $"CorrelationId is {correlationId.Length} UTF-16 code units; the portable maximum is {MaxCorrelationIdLength} " +
                $"({nameof(AsyncResponseChannelOptions)}.{nameof(MaxCorrelationIdLength)} — the correlation_id column width in the " +
                "SQL Server channel). A longer id is truncated or rejected at its first database write.";
        }

        if (correlationId[0] == ' ' || correlationId[^1] == ' ')
        {
            return "CorrelationId begins or ends with a space. SQL Server pads the shorter operand of an equality comparison " +
                "(binary collations included), so an id with surrounding spaces is the SAME key as one without to the database, " +
                "while the library compares ids ordinally — a response could reach the wrong waiter. Trim the id.";
        }

        if (PortableText.IndexOfIllFormedUtf16(correlationId) is var illFormed and >= 0)
        {
            return PortableText.IllFormedUtf16Rejection(
                "CorrelationId",
                PortableText.Excerpt(correlationId),
                correlationId[illFormed],
                illFormed);
        }

        if (PortableText.IndexOfControlCharacter(correlationId) is var control and >= 0)
        {
            return PortableText.ControlCharacterRejection(
                "CorrelationId",
                PortableText.Excerpt(correlationId),
                correlationId[control],
                control);
        }

        return null;
    }

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
    /// Guards a timer-armed interval knob (<see cref="Task.Delay(TimeSpan)"/>, CTS timers,
    /// <c>WaitAsync</c>): positive and under the .NET timer ceiling. An over-ceiling interval
    /// otherwise throws at the FIRST arming inside a background loop, where the loop's own
    /// retry-delay throws again — killing dispatch instead of surfacing a config error.
    /// </summary>
    internal static void EnsureTimerBacked(TimeSpan value, string optionsName, string knob)
    {
        if (value <= TimeSpan.Zero || value > MaxTimerBackedTimeout)
            throw new InvalidOperationException(
                $"{optionsName}.{knob} must be positive and at most {MaxTimerBackedTimeout.TotalDays:0.#} days (the .NET timer ceiling).");
    }

    /// <summary>
    /// <see cref="EnsureTimerBacked"/> for knobs where zero legitimately means "skip the wait"
    /// (e.g. a startup delay): non-negative and under the .NET timer ceiling.
    /// </summary>
    internal static void EnsureTimerBackedAllowZero(TimeSpan value, string optionsName, string knob)
    {
        if (value < TimeSpan.Zero || value > MaxTimerBackedTimeout)
            throw new InvalidOperationException(
                $"{optionsName}.{knob} must be non-negative and at most {MaxTimerBackedTimeout.TotalDays:0.#} days (the .NET timer ceiling).");
    }

    /// <summary>
    /// Guards a persisted-TTL/deadline-stamp knob: positive and small enough that the
    /// "now + value" stamp every consumer computes can never overflow date arithmetic — a
    /// <see cref="TimeSpan.MaxValue"/> here otherwise passes registration and throws only at the
    /// first stamp, after side effects (a publisher's overflow lands AFTER its insert, reporting
    /// failure for a response that may have been delivered).
    /// </summary>
    internal static void EnsurePersistedTtl(TimeSpan value, string optionsName, string knob)
    {
        if (value <= TimeSpan.Zero || value > MaxPersistenceTtl)
            throw new InvalidOperationException(
                $"{optionsName}.{knob} must be positive and at most {MaxPersistenceTtl.TotalDays:0} days — " +
                "expiry/deadline stamps are computed as \"now + value\", and larger values overflow.");
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
        if (RecoveryStateExpiry > MaxPersistenceTtl)
            throw new InvalidOperationException(
                $"{optionsName}.{nameof(RecoveryStateExpiry)} must be at most {MaxPersistenceTtl.TotalDays:0} days — " +
                "expiry stamps are computed as \"now + expiry\", and larger values overflow at the first save.");

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
