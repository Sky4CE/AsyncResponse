namespace AsyncResponse.Transports.GooglePubSub;

internal static class GooglePubSubOptionsValidator
{
    /// <summary>Validates the supplied options.</summary>
    public static string Required(string? value, string name)
        => !string.IsNullOrWhiteSpace(value)
            ? value
            : throw new InvalidOperationException($"{nameof(GooglePubSubAsyncResponseOptions)}.{name} must be configured.");

    /// <summary>
    /// Common validation shared by the publisher and both subscribers: requires the
    /// correlation-id attribute name and bounds every timer-armed timeout — the subscriber retry
    /// delays feed <c>Task.Delay</c>, and <c>ShutdownTimeout</c> feeds the publisher's
    /// <c>ShutdownAsync</c> and the subscriber's stop timeout in every ack mode. The timeouts
    /// previously had NO validation at all — a negative or over-ceiling value surfaced as a raw
    /// timer exception inside the retry loop.
    /// </summary>
    public static void ValidateTimeouts(GooglePubSubAsyncResponseOptions options)
    {
        // Both the publish and consume paths index the message-attribute map with
        // CorrelationIdAttribute; a null/empty value throws from the protobuf map on the very
        // first message (or, behind the dispatcher's catch-all, turns into a perpetual NACK
        // loop), so fail fast here — the ASB sibling validates its CorrelationIdProperty the
        // same way.
        _ = Required(options.CorrelationIdAttribute, nameof(options.CorrelationIdAttribute));

        // Every correlated publish writes this attribute key, and Pub/Sub rejects a publish whose
        // key starts with "goog" or exceeds 256 bytes — so such a name failed every correlated
        // publish (and every external responder told to use it) after a clean startup.
        if (!IsValidAttributeKey(options.CorrelationIdAttribute))
        {
            throw new InvalidOperationException(
                $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(options.CorrelationIdAttribute)} '{options.CorrelationIdAttribute}' is not a valid " +
                $"Pub/Sub attribute key: it must not start with 'goog' (reserved by Google) and must not exceed {MaxAttributeKeyBytes} bytes in UTF-8.");
        }

        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryBaseDelay, nameof(GooglePubSubAsyncResponseOptions), nameof(options.SubscriberRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryMaxDelay, nameof(GooglePubSubAsyncResponseOptions), nameof(options.SubscriberRetryMaxDelay));
        if (options.SubscriberRetryBaseDelay > options.SubscriberRetryMaxDelay)
            throw new InvalidOperationException(
                $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(options.SubscriberRetryBaseDelay)} cannot exceed {nameof(options.SubscriberRetryMaxDelay)}.");
        AsyncResponseChannelOptions.EnsureTimerBacked(options.ShutdownTimeout, nameof(GooglePubSubAsyncResponseOptions), nameof(options.ShutdownTimeout));
        if (options.HostShutdownTimeout is { } hostShutdownTimeout && hostShutdownTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(options.HostShutdownTimeout)} must be positive when set.");
    }

    /// <summary>The Pub/Sub attribute-key size limit, in UTF-8 bytes.</summary>
    internal const int MaxAttributeKeyBytes = 256;

    /// <summary>
    /// Whether Pub/Sub accepts <paramref name="key"/> as a message-attribute key: at most 256 bytes
    /// in UTF-8, and not starting with <c>goog</c> (reserved by Google).
    /// </summary>
    internal static bool IsValidAttributeKey(string key)
        => !key.StartsWith("goog", StringComparison.Ordinal)
            && System.Text.Encoding.UTF8.GetByteCount(key) <= MaxAttributeKeyBytes;

    /// <summary>
    /// The first lease already lasts the client's 60-second ack deadline, so a smaller total
    /// extension cannot make Pub/Sub redeliver sooner — it would only shrink the in-flight ceiling
    /// the worker transport advertises, and with it every in-process durable-flow wait.
    /// </summary>
    internal static readonly TimeSpan MinimumMaxTotalAckExtension = TimeSpan.FromMinutes(1);

    /// <summary>The SDK's own <c>ClientCount</c> range; outside it the client build throws.</summary>
    internal const int MaximumClientCount = 256;

    /// <summary>
    /// Bounds the ack-extension ceiling. The SDK arms one timer with it per pulled batch, so an
    /// over-ceiling value surfaces as a raw timer exception inside the streaming pull (and a
    /// perpetual subscriber restart loop) instead of failing startup.
    /// </summary>
    public static void ValidateMaxTotalAckExtension(GooglePubSubSubscriberOptions subscriberOptions, string optionPath)
    {
        AsyncResponseChannelOptions.EnsureTimerBacked(subscriberOptions.MaxTotalAckExtension, optionPath, nameof(GooglePubSubSubscriberOptions.MaxTotalAckExtension));
        if (subscriberOptions.MaxTotalAckExtension < MinimumMaxTotalAckExtension)
            throw new InvalidOperationException(
                $"{optionPath}.{nameof(GooglePubSubSubscriberOptions.MaxTotalAckExtension)} must be at least {MinimumMaxTotalAckExtension.TotalMinutes:0} minute: " +
                "the first lease already lasts the Pub/Sub client's 60-second ack deadline, so a smaller value cannot speed up redelivery.");
    }

    /// <summary>
    /// Validates the streaming-pull settings handed to the SDK's subscriber client. The SDK checks
    /// them only when the client is built — inside the supervised retry loop, where a bad value
    /// turns into an endless rebuild-and-fail cycle rather than a startup failure.
    /// </summary>
    public static void ValidateStreamingPull(GooglePubSubSubscriberOptions subscriberOptions, string optionPath)
    {
        ValidateMaxTotalAckExtension(subscriberOptions, optionPath);

        if (subscriberOptions.ClientCount is < 1 or > MaximumClientCount)
            throw new InvalidOperationException(
                $"{optionPath}.{nameof(GooglePubSubSubscriberOptions.ClientCount)} must be between 1 and {MaximumClientCount} (the Pub/Sub client's range).");

        if (subscriberOptions.MaxOutstandingMessages <= 0)
            throw new InvalidOperationException(
                $"{optionPath}.{nameof(GooglePubSubSubscriberOptions.MaxOutstandingMessages)} must be positive.");

        if (subscriberOptions.MaxOutstandingBytes <= 0)
            throw new InvalidOperationException(
                $"{optionPath}.{nameof(GooglePubSubSubscriberOptions.MaxOutstandingBytes)} must be positive.");
    }
}
