namespace AsyncResponse.Transports.Redis;

/// <summary>
/// Options for the Redis Streams AsyncResponse transport. Runs against Redis 6.2+ (the pending-entry
/// reclaim uses <c>XPENDING … IDLE</c>, added in 6.2) and any RESP-compatible server that implements
/// Redis Streams and consumer groups — validated on Redis 8, Valkey, and Dragonfly. (Garnet does not implement stream commands, so it works as a <em>channel</em> but not as
/// this transport.) Dead-letter trimming uses plain <c>XADD … MAXLEN ~ N</c> (no Redis 8 trim-mode
/// token), and the worker stream's capacity uses <c>XTRIM … MINID</c> (Redis 6.2+), so both stay
/// portable across all of these servers.
/// </summary>
public sealed class RedisAsyncResponseTransportOptions
{
    public const string TransportName = "redis";

    /// <summary>
    /// Prefix used when a stream name is not explicitly configured. The default worker stream is
    /// <c>{KeyPrefix}:transport:worker</c>, the default response stream is
    /// <c>{KeyPrefix}:transport:response</c>, and the default dead-letter stream is
    /// <c>{KeyPrefix}:transport:deadletter</c>. Use a unique prefix per app/environment when several
    /// deployments share one Redis.
    /// </summary>
    public string KeyPrefix { get; set; } = "asyncresponse";

    /// <summary>
    /// Redis stream used by <see cref="RedisWorkerTransport"/> to publish worker jobs. When null,
    /// <see cref="KeyPrefix"/> determines the stream name.
    /// </summary>
    public string? WorkerStream { get; set; }

    /// <summary>Consumer group used by the hosted worker subscriber.</summary>
    public string WorkerConsumerGroup { get; set; } = "asyncresponse-workers";

    /// <summary>Worker stream handling options.</summary>
    public RedisSubscriberOptions WorkerSubscriber { get; } = new();

    /// <summary>
    /// Redis stream remote systems can append response payloads to. The hosted response-ingress
    /// subscriber reads this stream and forwards payloads into <see cref="IAsyncResponseIngress"/>.
    /// When null, <see cref="KeyPrefix"/> determines the stream name.
    /// </summary>
    public string? ResponseStream { get; set; }

    /// <summary>Consumer group used by the hosted response-ingress subscriber.</summary>
    public string ResponseConsumerGroup { get; set; } = "asyncresponse-responses";

    /// <summary>Response stream handling options.</summary>
    public RedisSubscriberOptions ResponseSubscriber { get; } = new();

    /// <summary>
    /// Consumer name used inside Redis consumer groups. When null, the package generates a stable
    /// process-local name, <c>{machine}-{pid}-{guid}</c>, kept within 64 characters by shortening
    /// the MACHINE NAME only — the process id and the GUID are what make it unique, so a long host
    /// name (a Kubernetes pod name, a host at HOST_NAME_MAX) never costs two processes on one host
    /// their separate identities. The subscriber role is appended to either form. Consumers that
    /// share a name share one pending-entry list, so configure this only when your orchestrator
    /// guarantees uniqueness per running process.
    /// </summary>
    public string? ConsumerName { get; set; }

    /// <summary>
    /// Creates the worker and response consumer groups on subscriber startup. The groups start at
    /// the beginning of the stream so messages published before the first subscriber starts are not
    /// skipped. Corollary: pointing a brand-new consumer group at a stream that already holds history
    /// replays that entire backlog (re-running old worker jobs, re-ingesting old responses). Use a
    /// fresh <see cref="KeyPrefix"/>/stream per deployment, or only rename groups while the stream is empty.
    /// </summary>
    public bool CreateConsumerGroups { get; set; } = true;

    /// <summary>
    /// Capacity of the worker stream: the most entries it may hold that the worker consumer group
    /// (<see cref="WorkerConsumerGroup"/>) has not settled. Set null for an unbounded stream.
    /// <para>
    /// A full stream <b>refuses the publish</b> — it never evicts work (NATS
    /// <c>Discard=New</c> parity). Settling a worker entry (handled, dead-lettered, or ACKed at
    /// enqueue in early-ACK mode) deletes it (<c>XACK</c> + <c>XDEL</c> in one step), so the stream
    /// holds only jobs nobody has read and jobs pending in a handler. A publish that finds the
    /// stream at capacity first drops whatever the group has already settled (entries left by an
    /// older version, which only ACKed, or ACKed by an operator) with <c>XTRIM … MINID</c> below
    /// both the group's last-delivered id and its oldest pending id, and is refused with a Redis
    /// server error starting with <c>ASYNCRESPONSE_STREAM_FULL</c> when that frees nothing. The
    /// refusal is retried within the publish budget (<see cref="PublishMaxAttempts"/>) and then
    /// thrown to the publisher; a durable flow's wake-up publish fails loudly instead of being
    /// trimmed away later. Until the group exists (no subscriber has started), nothing counts as
    /// settled, so the stream fills to the capacity and then refuses.
    /// </para>
    /// <para>
    /// Earlier versions appended with <c>XADD … MAXLEN ~ N</c>, which trimmed by length alone and
    /// deleted unread and in-flight jobs, with no dead-letter copy, once the backlog passed the cap.
    /// Only the worker group is consulted: another consumer group an operator attaches to the
    /// worker stream does not hold entries back, and loses each entry once the worker group
    /// settles it.
    /// </para>
    /// <para>
    /// The library bounds only its own worker publishes. The response stream is written by the
    /// response producers, which must apply their own <c>XADD … MAXLEN ~</c>; the response ingress
    /// only ACKs, and never deletes, its entries.
    /// </para>
    /// </summary>
    public long? StreamMaxLength { get; set; } = 100_000;

    /// <summary>
    /// Uses approximate MAXLEN trimming for <see cref="DeadLetterStreamMaxLength"/>. Approximate
    /// trimming is much cheaper on hot streams and is the recommended default. The worker stream
    /// is never length-trimmed (see <see cref="StreamMaxLength"/>), so this does not affect it.
    /// </summary>
    public bool UseApproximateStreamTrimming { get; set; } = true;

    /// <summary>
    /// Enables dead-lettering when a message reaches <see cref="RedisSubscriberOptions.MaxDeliveryAttempts"/>
    /// or a background handler fails after early ACK. When true and <see cref="DeadLetterStream"/> is
    /// null, <see cref="KeyPrefix"/> determines the stream name.
    /// </summary>
    public bool DeadLetterEnabled { get; set; } = true;

    /// <summary>Redis stream that receives poison messages and already-ACKed background failures.</summary>
    public string? DeadLetterStream { get; set; }

    /// <summary>Maximum dead-letter stream length. Set null to disable dead-letter stream trimming.</summary>
    public long? DeadLetterStreamMaxLength { get; set; } = 100_000;

    /// <summary>The logical reply target name used by <c>WithReplyTarget()</c>. Default: <c>default</c>.</summary>
    public string DefaultReplyTargetName { get; set; } = "default";

    /// <summary>
    /// Named reply targets exposed to Core through <see cref="IAsyncResponseReplyTargetProvider"/>.
    /// When empty, the resolved <see cref="ResponseStream"/> becomes the default target.
    /// </summary>
    public Dictionary<string, RedisReplyTargetOptions> ReplyTargets { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Redis stream field carrying the AsyncResponse correlation id. Response messages may omit it
    /// when the id is present in the JSON body via <see cref="CorrelationIdJsonPaths"/>.
    /// </summary>
    public string CorrelationIdField { get; set; } = "correlationId";

    /// <summary>
    /// Redis stream field containing the serialized JSON payload. Worker messages contain a
    /// serialized <see cref="WorkerJobEnvelope"/>; response messages contain the remote payload JSON.
    /// </summary>
    public string PayloadField { get; set; } = "payload";

    /// <summary>
    /// JSON paths inspected when a response message does not carry the correlation id in
    /// <see cref="CorrelationIdField"/>. Paths are case-insensitive and support nested JSON strings.
    /// </summary>
    public string[] CorrelationIdJsonPaths { get; set; } =
    [
        "CorrelationId",
        "CustomParameters",
        "CustomParameters.CorrelationId",
        "PubSubParams.CustomParameters",
        "PubSubParams.CustomParameters.CorrelationId",
        "DagJsonParameters.CorrelationId"
    ];

    /// <summary>Per-command timeout applied by the transport wrapper before retry/backoff logic.</summary>
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Maximum attempts for Redis publish commands. Set to 1 to disable publish retries.</summary>
    public int PublishMaxAttempts { get; set; } = 3;

    /// <summary>Initial delay before retrying a failed publish command.</summary>
    public TimeSpan PublishRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Maximum delay between publish retry attempts.</summary>
    public TimeSpan PublishRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Initial delay after a subscriber loop Redis failure.</summary>
    public TimeSpan SubscriberRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Maximum delay after repeated subscriber loop Redis failures.</summary>
    public TimeSpan SubscriberRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The hosting shutdown budget that must contain
    /// <see cref="RedisSubscriberOptions.BackgroundDrainTimeout"/> when a subscriber uses
    /// <see cref="RedisAckMode.AckAfterEnqueue"/>.
    /// </summary>
    public TimeSpan? HostShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Adds or replaces a named Redis reply target.</summary>
    public RedisAsyncResponseTransportOptions AddReplyTarget(string name, string responseStream)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(responseStream);

        ReplyTargets[name] = new RedisReplyTargetOptions { ResponseStream = responseStream };
        return this;
    }
}

/// <summary>Options for one named Redis Streams async-response reply target.</summary>
public sealed class RedisReplyTargetOptions
{
    /// <summary>Redis stream remote systems should XADD response payloads to.</summary>
    public string? ResponseStream { get; set; }

    /// <summary>Consumer group that receives responses for this target. Optional metadata.</summary>
    public string? ConsumerGroup { get; set; }

    /// <summary>Additional values copied to the transport-neutral reply target.</summary>
    public Dictionary<string, string> Properties { get; } = new(StringComparer.Ordinal);
}
