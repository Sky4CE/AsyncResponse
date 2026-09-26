namespace AsyncResponse.Transports.NATS;

/// <summary>
/// Options for the NATS JetStream AsyncResponse transport. Requires a NATS server with JetStream
/// enabled.
/// </summary>
public sealed class NatsAsyncResponseTransportOptions
{
    /// <summary>The transport name reported to reply targets and the startup validator.</summary>
    public const string TransportName = "NATS";

    /// <summary>
    /// Prefix used to derive subject and stream names that are not explicitly configured. The default
    /// worker subject is <c>{SubjectPrefix}.transport.worker</c>, the response subject is
    /// <c>{SubjectPrefix}.transport.response</c>, and the dead-letter subject is
    /// <c>{SubjectPrefix}.transport.deadletter</c>. Stream names replace the dots with underscores
    /// (NATS stream names cannot contain dots). Use a unique prefix per app/environment when several
    /// deployments share one NATS system.
    /// </summary>
    public string SubjectPrefix { get; set; } = "asyncresponse";

    /// <summary>NATS subject worker jobs are published to. When null, <see cref="SubjectPrefix"/> determines it.</summary>
    public string? WorkerSubject { get; set; }

    /// <summary>JetStream stream that captures the worker subject. When null, <see cref="SubjectPrefix"/> determines it.</summary>
    public string? WorkerStream { get; set; }

    /// <summary>Durable JetStream consumer used by the hosted worker subscriber.</summary>
    public string WorkerConsumer { get; set; } = "asyncresponse-workers";

    /// <summary>Worker subject handling options.</summary>
    public NatsSubscriberOptions WorkerSubscriber { get; } = new();

    /// <summary>
    /// NATS subject remote systems publish response payloads to. The hosted response-ingress
    /// subscriber consumes it and forwards payloads into <see cref="IAsyncResponseIngress"/>. When
    /// null, <see cref="SubjectPrefix"/> determines it.
    /// </summary>
    public string? ResponseSubject { get; set; }

    /// <summary>JetStream stream that captures the response subject. When null, <see cref="SubjectPrefix"/> determines it.</summary>
    public string? ResponseStream { get; set; }

    /// <summary>Durable JetStream consumer used by the hosted response-ingress subscriber.</summary>
    public string ResponseConsumer { get; set; } = "asyncresponse-responses";

    /// <summary>Response subject handling options.</summary>
    public NatsSubscriberOptions ResponseSubscriber { get; } = new();

    /// <summary>
    /// Creates the worker and response JetStream streams (and the dead-letter stream when enabled) on
    /// subscriber startup when they do not exist. A stream that already exists is never modified:
    /// the subscriber refuses it when it does not capture the configured subject or (worker/response
    /// streams) does not use work-queue retention, and logs a warning when its discard policy or
    /// message limit differs from these options — settings tuned on the live stream (replicas, max
    /// age, max bytes, …) are left alone. Disable when streams are provisioned out of band. The
    /// durable consumers follow the same rule whatever this is set to: created when missing, never
    /// modified — the subscriber refuses an existing one that is a push consumer, does not use
    /// explicit acks, or has a max deliver at or below
    /// <see cref="NatsSubscriberOptions.MaxDeliveryAttempts"/>. A refusal does not fail host
    /// startup: every subscriber attempt fails with an error naming the stream or consumer, logged
    /// as a warning and retried with backoff, and nothing is consumed until it is fixed. A consumer
    /// whose ack wait differs from <see cref="AckWait"/> is used as it is — the in-progress
    /// heartbeat renews at a third of the shorter of the two — and the drift is logged once.
    /// </summary>
    public bool CreateStreams { get; set; } = true;

    /// <summary>
    /// Maximum message count retained per worker/response stream. Set null to disable the limit.
    /// Applied when this library creates the stream; a later change is reported as drift, not
    /// applied to the existing stream.
    /// </summary>
    public long? StreamMaxMessages { get; set; } = 100_000;

    /// <summary>
    /// Replica count (JetStream <c>num_replicas</c>) for the streams this library creates: worker,
    /// response, and dead-letter. <c>1</c>–<c>5</c>; use an odd number (3 or 5) on a clustered
    /// JetStream — an even count tolerates no more failures than the odd count below it. Values
    /// above 1 require a JetStream cluster with at least that many servers. Only applied when a
    /// stream is created; an existing stream keeps its replica count. Default: <c>1</c>.
    /// </summary>
    public int StreamReplicas { get; set; } = 1;

    /// <summary>
    /// How long the server waits for an ACK before redelivering a message (the JetStream AckWait).
    /// Applied when this library creates the durable consumer; an existing consumer keeps its own
    /// ack wait (change it on the consumer), and the in-progress heartbeat follows whichever of the
    /// two is shorter.
    /// </summary>
    public TimeSpan AckWait { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Enables dead-lettering when a message reaches
    /// <see cref="NatsSubscriberOptions.MaxDeliveryAttempts"/> or a background handler fails after an
    /// early ACK. When true and <see cref="DeadLetterSubject"/> is null, <see cref="SubjectPrefix"/>
    /// determines the subject/stream names.
    /// </summary>
    public bool DeadLetterEnabled { get; set; } = true;

    /// <summary>NATS subject that receives poison messages and already-ACKed background failures.</summary>
    public string? DeadLetterSubject { get; set; }

    /// <summary>JetStream stream that captures the dead-letter subject.</summary>
    public string? DeadLetterStream { get; set; }

    /// <summary>Maximum dead-letter stream message count. Set null to disable the limit.</summary>
    public long? DeadLetterStreamMaxMessages { get; set; } = 100_000;

    /// <summary>The logical reply target name used by <c>WithReplyTarget()</c>. Default: <c>default</c>.</summary>
    public string DefaultReplyTargetName { get; set; } = "default";

    /// <summary>
    /// Named reply targets exposed to Core through <see cref="IAsyncResponseReplyTargetProvider"/>.
    /// When empty, the resolved <see cref="ResponseSubject"/> becomes the default target.
    /// </summary>
    public Dictionary<string, NatsReplyTargetOptions> ReplyTargets { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// NATS message header carrying the AsyncResponse correlation id. Response messages may omit it
    /// when the id is present in the JSON body via <see cref="CorrelationIdJsonPaths"/>.
    /// </summary>
    public string CorrelationIdHeader { get; set; } = "AR-Correlation-Id";

    /// <summary>
    /// JSON paths inspected when a response message does not carry the correlation id in
    /// <see cref="CorrelationIdHeader"/>. Paths are case-insensitive and support nested JSON strings.
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

    /// <summary>Maximum attempts for JetStream publish commands. Set to 1 to disable publish retries.</summary>
    public int PublishMaxAttempts { get; set; } = 3;

    /// <summary>Initial delay before retrying a failed publish command.</summary>
    public TimeSpan PublishRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Maximum delay between publish retry attempts.</summary>
    public TimeSpan PublishRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Initial delay after a subscriber consume-loop failure.</summary>
    public TimeSpan SubscriberRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Maximum delay after repeated subscriber consume-loop failures.</summary>
    public TimeSpan SubscriberRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The hosting shutdown budget that must contain
    /// <see cref="NatsSubscriberOptions.BackgroundDrainTimeout"/> when a subscriber uses
    /// <see cref="NatsAckMode.AckAfterEnqueue"/> — and the SUM of both subscribers'
    /// <see cref="NatsSubscriberOptions.BackgroundDrainTimeout"/> when the worker and response
    /// subscribers both use it, because the host stops them one after the other inside this one
    /// budget. Defaults to the Generic Host default of 30 seconds. Set to <c>null</c> only when
    /// this budget is validated externally.
    /// </summary>
    public TimeSpan? HostShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Adds or replaces a named NATS reply target.</summary>
    public NatsAsyncResponseTransportOptions AddReplyTarget(string name, string responseSubject)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(responseSubject);

        ReplyTargets[name] = new NatsReplyTargetOptions { ResponseSubject = responseSubject };
        return this;
    }
}

/// <summary>Options for one named NATS JetStream async-response reply target.</summary>
public sealed class NatsReplyTargetOptions
{
    /// <summary>NATS subject remote systems should publish response payloads to.</summary>
    public string? ResponseSubject { get; set; }

    /// <summary>Durable consumer that receives responses for this target. Optional metadata.</summary>
    public string? Consumer { get; set; }

    /// <summary>Additional values copied to the transport-neutral reply target.</summary>
    public Dictionary<string, string> Properties { get; } = new(StringComparer.Ordinal);
}
