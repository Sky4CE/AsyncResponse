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
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryBaseDelay, nameof(GooglePubSubAsyncResponseOptions), nameof(options.SubscriberRetryBaseDelay));
        AsyncResponseChannelOptions.EnsureTimerBacked(options.SubscriberRetryMaxDelay, nameof(GooglePubSubAsyncResponseOptions), nameof(options.SubscriberRetryMaxDelay));
        if (options.SubscriberRetryBaseDelay > options.SubscriberRetryMaxDelay)
            throw new InvalidOperationException(
                $"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(options.SubscriberRetryBaseDelay)} cannot exceed {nameof(options.SubscriberRetryMaxDelay)}.");
        AsyncResponseChannelOptions.EnsureTimerBacked(options.ShutdownTimeout, nameof(GooglePubSubAsyncResponseOptions), nameof(options.ShutdownTimeout));
        if (options.HostShutdownTimeout is { } hostShutdownTimeout && hostShutdownTimeout <= TimeSpan.Zero)
            throw new InvalidOperationException($"{nameof(GooglePubSubAsyncResponseOptions)}.{nameof(options.HostShutdownTimeout)} must be positive when set.");
    }
}
