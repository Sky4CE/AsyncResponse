namespace AsyncResponse;

/// <summary>
/// Engine-level options for AsyncResponse, configured by <c>AddAsyncResponse()</c>. These apply
/// regardless of which channel, transport, or durable-flow store is registered. Component-specific
/// settings live on that component's own <c>With*</c> registration — see
/// <see cref="InMemoryAsyncResponseOptions"/> and the Redis channel package's <c>RedisAsyncResponseOptions</c>.
/// </summary>
public sealed class AsyncResponseOptions
{
    /// <summary>
    /// Settings for the built-in recovery-state watchdog, which runs by default and periodically
    /// scans the configured recovery store for flows that look stuck (persisted recovery state
    /// with no live waiter). Set <see cref="AsyncResponseWatchdogOptions.Enabled"/> to
    /// <c>false</c> to turn it off — for example in all but one host when several hosts share one
    /// durable store, to avoid duplicate scans and warnings.
    /// </summary>
    public AsyncResponseWatchdogOptions Watchdog { get; set; } = new();

    /// <summary>
    /// Largest inbound message the ingress will process, in UTF-16 code units. Larger messages are
    /// acknowledged without dispatch, with an error log and the
    /// <c>asyncresponse.ingress.oversized_messages</c> counter. Default: 8 Mi characters
    /// (comfortably above every broker's own payload ceiling); <c>null</c> removes the limit.
    /// <para>
    /// The backstop exists because the transport adapters materialize whatever the broker or
    /// database handed them as a string and parse it immediately. Without a bound, a handful of
    /// oversized messages could drive string, DOM, serializer and dead-letter allocations far past
    /// what the host budgeted for — and being poison, retry them into the same allocation over and
    /// over. This is a memory guard, not a business rule: put real payloads behind a claim check
    /// rather than raising it.
    /// </para>
    /// <para>
    /// Acknowledged rather than failed on purpose: an oversized message never becomes smaller, so
    /// redelivering it hot-loops (RabbitMQ's default <c>MaxDeliveryAttempts = 0</c> has no cap) or
    /// burns dead-letter attempts on brokers that do. Same answer this ingress already gives an
    /// unroutable correlation id, for the same reason.
    /// </para>
    /// </summary>
    public int? MaxInboundMessageChars { get; set; } = 8 * 1024 * 1024;
}
