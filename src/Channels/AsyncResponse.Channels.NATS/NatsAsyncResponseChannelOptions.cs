namespace AsyncResponse.Channels.NATS;

/// <summary>
/// Options for the NATS-backed async-response channel.
/// <para>
/// The channel delivers responses over NATS Core <em>request/reply</em>: a waiter subscribes to a
/// per-correlation subject and replies to confirm receipt, and the publisher uses a request so the
/// NATS "no responders" signal tells it precisely when nobody is listening (the moment that triggers
/// lost-subscriber recovery). Durable <see cref="RecoveryState"/> lives in a NATS JetStream
/// Key-Value bucket so a response that arrives after the waiter died (e.g. a redeploy) can still be
/// routed to the resume or failure callback.
/// </para>
/// </summary>
public sealed class NatsAsyncResponseChannelOptions : DurableAsyncResponseChannelOptions
{
    /// <summary>The channel name reported to the startup validator.</summary>
    public const string ChannelName = "NATS";

    internal const string DefaultSubjectPrefix = "asyncresponse";
    internal const string DefaultRecoveryBucket = "asyncresponse-recovery";

    /// <summary>
    /// Subject prefix for every response subject created by the channel. A response subject is
    /// <c>{SubjectPrefix}.response.{encodedCorrelationId}</c>, where the correlation id is encoded
    /// to a NATS-safe token. Change it — together with <see cref="RecoveryBucket"/> — to isolate
    /// multiple applications or environments sharing one NATS system: the prefix scopes the
    /// response subjects only, while recovery registrations are keyed by correlation id alone in
    /// the bucket. Treat it as a deployment-wide contract: publishers and subscribers must agree on
    /// it.
    /// </summary>
    public string SubjectPrefix { get; set; } = DefaultSubjectPrefix;

    /// <summary>
    /// Name of the JetStream Key-Value bucket that stores durable <see cref="RecoveryState"/>.
    /// Must be a valid bucket name (alphanumeric, dash, underscore). Changing it orphans existing
    /// recovery state. The backing JetStream stream is <c>KV_{RecoveryBucket}</c>.
    /// <para>
    /// Give every application or environment sharing one NATS system its own bucket, as well as
    /// its own <see cref="SubjectPrefix"/>: registrations are keyed by correlation id only, not by
    /// the prefix, so deployments sharing a bucket see each other's registrations — each one's
    /// watchdog reports the other's long waits as stale, and a correlation id both use can have its
    /// registration consumed by the wrong deployment. A non-default prefix with the default bucket
    /// logs a startup warning.
    /// </para>
    /// </summary>
    public string RecoveryBucket { get; set; } = DefaultRecoveryBucket;

    /// <summary>
    /// Replica count for the recovery Key-Value bucket. Use a value greater than <c>1</c> on a NATS
    /// cluster so recovery state survives a single node loss. Default: <c>1</c>.
    /// </summary>
    public int RecoveryBucketReplicas { get; set; } = 1;

    // RecoveryStateExpiry and DefaultTimeout are inherited from AsyncResponseChannelOptions (the NATS
    // expiry is also applied as the Key-Value bucket's MaxAge ceiling — see the recovery store).

    /// <summary>
    /// How long a publish waits for a waiter to acknowledge receipt. A publish that finds a
    /// subscriber but gets no acknowledgement in time is still treated as <em>delivered</em>: the
    /// publish succeeds (the transport acknowledges the broker message), lost-subscriber recovery
    /// is not consulted, and the response is not kept anywhere else. That is right for a live
    /// waiter whose acknowledgement was merely slow, but NATS cannot tell it apart from a waiter
    /// whose host died or was partitioned without closing its connection: the server keeps routing
    /// to that stale subscription until its own ping timeout (by default two missed pings, about
    /// four minutes), and a response published to it in that window is lost — the owning flow
    /// finds out only when its own wait times out. The definitive "nobody is listening" signal is
    /// NATS no-responders, which returns immediately, is independent of this timeout, and does
    /// route to recovery. Keep it short. Default: 5 seconds.
    /// </summary>
    public TimeSpan DeliveryConfirmationTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long a presence ping waits for a live waiter to answer. NATS Core does not expose exact
    /// subscriber counts to clients, so the probe reports presence: <c>1</c> when a waiter answers,
    /// <c>0</c> only when NATS reports no responders (nothing is subscribed), and unprobeable
    /// (<c>-1</c>) when a subscriber exists but does not answer in time — a waiter busy in a slow
    /// <c>Until</c> predicate answers late, so a timeout is never read as "dead" and never
    /// consumes a recovery registration. Bounds both the watchdog's liveness probe and the re-check
    /// a publish makes before routing a response to recovery. Default: 2 seconds.
    /// </summary>
    public TimeSpan PresenceProbeTimeout { get; set; } = TimeSpan.FromSeconds(2);

    // IncludeRemoteStackTrace and MaxRemoteStackTraceLength are inherited from
    // DurableAsyncResponseChannelOptions.

    /// <summary>
    /// Validates the options, throwing <see cref="InvalidOperationException"/> on a misconfiguration.
    /// Called by the channel, the recovery store, and the DI registration so a bad configuration
    /// fails fast rather than at first use.
    /// </summary>
    public void Validate()
    {
        // Shared channel knobs (RecoveryStateExpiry, DefaultTimeout, DisposalDrainTimeout) go
        // through the ONE base guard set — a bespoke duplicate here silently missed every knob
        // added to the base later (DisposalDrainTimeout was validated nowhere on this provider).
        ValidateShared(nameof(NatsAsyncResponseChannelOptions));

        Required(SubjectPrefix, nameof(SubjectPrefix));
        Required(RecoveryBucket, nameof(RecoveryBucket));

        // nats-server rejects num_replicas outside 1..5 when the bucket is created — at the first
        // waiter registration, failing every one — so the bound is a named startup error here
        // (the transport's StreamReplicas has the same bound).
        if (RecoveryBucketReplicas is < 1 or > 5)
            throw new InvalidOperationException(
                $"{nameof(NatsAsyncResponseChannelOptions)}.{nameof(RecoveryBucketReplicas)} must be between 1 and 5 (JetStream's replica limit); it is {RecoveryBucketReplicas}.");

        // Both feed NatsSubOpts.Timeout on the reply subscription (publish confirmation and the
        // watchdog's presence probe). The NATS client arms a timer from that value — an
        // over-ceiling timeout throws at subscribe time, mid-operation, and TimeSpan.MaxValue
        // disables the timeout entirely, hanging the probe — so both are bounded here instead.
        EnsureTimerBacked(DeliveryConfirmationTimeout, nameof(NatsAsyncResponseChannelOptions), nameof(DeliveryConfirmationTimeout));
        EnsureTimerBacked(PresenceProbeTimeout, nameof(NatsAsyncResponseChannelOptions), nameof(PresenceProbeTimeout));

        if (MaxRemoteStackTraceLength < 0)
            throw new InvalidOperationException($"{nameof(NatsAsyncResponseChannelOptions)}.{nameof(MaxRemoteStackTraceLength)} must not be negative.");

        // A NATS bucket name must be a single token of [A-Za-z0-9_-]. A dotted/whitespace value would
        // silently produce an unusable backing stream, so reject it explicitly.
        foreach (var c in RecoveryBucket)
        {
            if (!(char.IsAsciiLetterOrDigit(c) || c is '-' or '_'))
                throw new InvalidOperationException(
                    $"{nameof(NatsAsyncResponseChannelOptions)}.{nameof(RecoveryBucket)} '{RecoveryBucket}' is not a valid NATS bucket name " +
                    "(allowed characters: letters, digits, '-', '_').");
        }

        // The bucket is backed by the JetStream stream KV_{bucket}, and nats-server caps stream
        // names at 255 characters. NATS.Net's bucket-name check has no length rule, and the bucket
        // is created lazily, so a longer name passed startup and then failed every registration's
        // stream creation instead (the transport validator enforces the same cap on its streams).
        if (RecoveryBucket.Length > MaxRecoveryBucketLength)
            throw new InvalidOperationException(
                $"{nameof(NatsAsyncResponseChannelOptions)}.{nameof(RecoveryBucket)} is {RecoveryBucket.Length} characters; its backing JetStream stream " +
                $"'KV_{{bucket}}' must fit NATS's 255-character stream-name limit, so the bucket name can be at most {MaxRecoveryBucketLength} characters.");

        // A subject prefix becomes leading tokens of every response subject; it must not contain the
        // NATS subject wildcards or whitespace that would break addressing.
        if (SubjectPrefix.IndexOfAny([' ', '\t', '*', '>', '\r', '\n']) >= 0)
            throw new InvalidOperationException(
                $"{nameof(NatsAsyncResponseChannelOptions)}.{nameof(SubjectPrefix)} '{SubjectPrefix}' must not contain whitespace or the NATS wildcards '*'/'>'.");

        // A dotted prefix is the intended way to namespace ("my.app" -> my.app.response.<id>), but
        // a leading, trailing or doubled '.' yields an EMPTY token — a subject nats-server rejects
        // on SUB with a non-fatal -ERR that NATS.Net never surfaces. Startup passed, every waiter
        // registered with no server-side interest, and every SetResponse got NoResponders and
        // took the lost-subscriber path while the live waiter ran to its timeout.
        if (SubjectPrefix.StartsWith('.') || SubjectPrefix.EndsWith('.') || SubjectPrefix.Contains("..", StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"{nameof(NatsAsyncResponseChannelOptions)}.{nameof(SubjectPrefix)} '{SubjectPrefix}' must not begin or end with '.' or contain '..' (an empty NATS subject token).");
    }

    /// <summary>Longest <see cref="RecoveryBucket"/>: JetStream's 255-character stream-name cap less the <c>KV_</c> prefix of its backing stream.</summary>
    private const int MaxRecoveryBucketLength = 255 - 3;

    private static void Required(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{nameof(NatsAsyncResponseChannelOptions)}.{name} must be configured.");
    }
}
