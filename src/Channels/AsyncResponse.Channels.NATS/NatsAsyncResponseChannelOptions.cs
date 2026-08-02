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

    /// <summary>
    /// Subject prefix for every response subject created by the channel. A response subject is
    /// <c>{SubjectPrefix}.response.{encodedCorrelationId}</c>, where the correlation id is encoded
    /// to a NATS-safe token. Change it to isolate multiple applications or environments sharing one
    /// NATS system. Treat it as a deployment-wide contract: publishers and subscribers must agree on
    /// it.
    /// </summary>
    public string SubjectPrefix { get; set; } = "asyncresponse";

    /// <summary>
    /// Name of the JetStream Key-Value bucket that stores durable <see cref="RecoveryState"/>.
    /// Must be a valid bucket name (alphanumeric, dash, underscore). Changing it orphans existing
    /// recovery state. The backing JetStream stream is <c>KV_{RecoveryBucket}</c>.
    /// </summary>
    public string RecoveryBucket { get; set; } = "asyncresponse-recovery";

    /// <summary>
    /// Replica count for the recovery Key-Value bucket. Use a value greater than <c>1</c> on a NATS
    /// cluster so recovery state survives a single node loss. Default: <c>1</c>.
    /// </summary>
    public int RecoveryBucketReplicas { get; set; } = 1;

    // RecoveryStateExpiry and DefaultTimeout are inherited from AsyncResponseChannelOptions (the NATS
    // expiry is also applied as the Key-Value bucket's MaxAge ceiling — see the recovery store).

    /// <summary>
    /// How long a publish waits for a waiter to acknowledge receipt before concluding that, although
    /// at least one subscriber had interest, none confirmed in time — which is still treated as
    /// <em>delivered</em> (the live subscriber received the message; only its ack was slow). The
    /// definitive "nobody is listening" signal is NATS no-responders, which returns immediately and
    /// is independent of this timeout. Keep it short. Default: 5 seconds.
    /// </summary>
    public TimeSpan DeliveryConfirmationTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long the active-subscriber probe (used by the watchdog) waits for a live waiter to answer
    /// a presence ping before reporting zero live subscribers. NATS Core does not expose exact
    /// subscriber counts to clients, so the probe reports presence (0 or 1). Default: 2 seconds.
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

        if (RecoveryBucketReplicas <= 0)
            throw new InvalidOperationException($"{nameof(NatsAsyncResponseChannelOptions)}.{nameof(RecoveryBucketReplicas)} must be positive.");

        Positive(DeliveryConfirmationTimeout, nameof(DeliveryConfirmationTimeout));
        Positive(PresenceProbeTimeout, nameof(PresenceProbeTimeout));

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

        // A subject prefix becomes leading tokens of every response subject; it must not contain the
        // NATS subject wildcards or separators that would break addressing.
        if (SubjectPrefix.IndexOfAny([' ', '\t', '*', '>', '\r', '\n']) >= 0)
            throw new InvalidOperationException(
                $"{nameof(NatsAsyncResponseChannelOptions)}.{nameof(SubjectPrefix)} '{SubjectPrefix}' must not contain whitespace or the NATS wildcards '*'/'>'.");
    }

    private static void Required(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{nameof(NatsAsyncResponseChannelOptions)}.{name} must be configured.");
    }

    private static void Positive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(NatsAsyncResponseChannelOptions)}.{name} must be positive.");
    }
}
