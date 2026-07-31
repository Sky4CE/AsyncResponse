namespace AsyncResponse.Transports.SqlServer;

/// <summary>Options for the Microsoft SQL Server AsyncResponse transport.</summary>
public sealed class SqlServerAsyncResponseTransportOptions
{
    /// <summary>The transport name reported to reply targets and the startup validator.</summary>
    public const string TransportName = "SqlServer";

    /// <summary>
    /// SQL Server connection string used for every queue-table operation. Required. The database it
    /// targets must already exist; the transport creates only its schema, table, and indexes.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>Database schema that contains the transport table. Default: <c>dbo</c>.</summary>
    public string SchemaName { get; set; } = "dbo";

    /// <summary>Table storing worker, response-ingress, and dead-letter queue rows.</summary>
    public string MessageTable { get; set; } = "asyncresponse_transport_messages";

    /// <summary>Creates the schema, table, and indexes on first use. Disable when migrations own DDL.</summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>Logical queue name used by <see cref="SqlServerWorkerTransport"/>.</summary>
    public string WorkerQueue { get; set; } = "worker";

    /// <summary>Worker queue handling options.</summary>
    public SqlServerSubscriberOptions WorkerSubscriber { get; } = new();

    /// <summary>Logical queue name consumed by the hosted response-ingress subscriber.</summary>
    public string ResponseQueue { get; set; } = "response";

    /// <summary>Response queue handling options.</summary>
    public SqlServerSubscriberOptions ResponseSubscriber { get; } = new();

    /// <summary>Logical queue name that receives poison messages and already-ACKed background failures.</summary>
    public string DeadLetterQueue { get; set; } = "deadletter";

    /// <summary>Enables dead-lettering when a message exhausts attempts or fails after early ACK.</summary>
    public bool DeadLetterEnabled { get; set; } = true;

    /// <summary>
    /// How long dead-letter rows are retained before being pruned opportunistically. The dead-letter
    /// queue has no consumer by default, so without a retention its rows accumulate indefinitely.
    /// Leave <c>null</c> (the default) to keep them forever for manual inspection.
    /// </summary>
    public TimeSpan? DeadLetterRetention { get; set; }

    /// <summary>How long a claimed row remains locked before another subscriber may retry it.</summary>
    public TimeSpan LockTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>The logical reply target name used by <c>WithReplyTarget()</c>. Default: <c>default</c>.</summary>
    public string DefaultReplyTargetName { get; set; } = "default";

    /// <summary>
    /// Named reply targets exposed to Core through <see cref="IAsyncResponseReplyTargetProvider"/>.
    /// When empty, <see cref="ResponseQueue"/> becomes the default target.
    /// </summary>
    public Dictionary<string, SqlServerReplyTargetOptions> ReplyTargets { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Header key stored in the row metadata for the AsyncResponse correlation id. Response messages
    /// may omit it when the id is present in the JSON body via <see cref="CorrelationIdJsonPaths"/>.
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

    /// <summary>Maximum attempts for SQL Server publish commands. Set to 1 to disable publish retries.</summary>
    public int PublishMaxAttempts { get; set; } = 3;

    /// <summary>Initial delay before retrying a failed publish command.</summary>
    public TimeSpan PublishRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Maximum delay between publish retry attempts.</summary>
    public TimeSpan PublishRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Initial delay after a subscriber loop failure.</summary>
    public TimeSpan SubscriberRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Maximum delay after repeated subscriber loop failures.</summary>
    public TimeSpan SubscriberRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The hosting shutdown budget that must contain
    /// <see cref="SqlServerSubscriberOptions.BackgroundDrainTimeout"/> when a subscriber uses
    /// <see cref="SqlServerAckMode.AckAfterEnqueue"/>. Defaults to the Generic Host default of
    /// 30 seconds. Set to <c>null</c> only when this budget is validated externally.
    /// </summary>
    public TimeSpan? HostShutdownTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Adds or replaces a named SQL Server reply target.</summary>
    public SqlServerAsyncResponseTransportOptions AddReplyTarget(string name, string responseQueue)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentException.ThrowIfNullOrWhiteSpace(responseQueue);

        ReplyTargets[name] = new SqlServerReplyTargetOptions { ResponseQueue = responseQueue };
        return this;
    }
}

/// <summary>Options for one named SQL Server async-response reply target.</summary>
public sealed class SqlServerReplyTargetOptions
{
    /// <summary>Queue remote systems should insert response payload rows into.</summary>
    public string? ResponseQueue { get; set; }

    /// <summary>Additional values copied to the transport-neutral reply target.</summary>
    public Dictionary<string, string> Properties { get; } = new(StringComparer.Ordinal);
}

/// <summary>Controls when a SQL Server transport row is acknowledged relative to handling.</summary>
public enum SqlServerAckMode
{
    /// <summary>
    /// Delete the row only after the AsyncResponse handler completes successfully. Handler failures
    /// reschedule the row until <see cref="SqlServerSubscriberOptions.MaxDeliveryAttempts"/> is reached.
    /// </summary>
    AckAfterHandlerCompletes = 0,

    /// <summary>
    /// Delete the row immediately after it is accepted into a bounded in-process background queue.
    /// Handler failures are logged, reported, and dead-lettered when enabled because the row has
    /// already been acknowledged.
    /// </summary>
    AckAfterEnqueue = 1
}

/// <summary>Describes a handler failure that happened after a SQL Server row was already acknowledged.</summary>
public sealed class SqlServerBackgroundFailureContext
{
    internal SqlServerBackgroundFailureContext(string queue, string subscriberRole, int attempt, string? correlationId, Exception exception)
    {
        Queue = queue;
        SubscriberRole = subscriberRole;
        Attempt = attempt;
        CorrelationId = correlationId;
        Exception = exception;
    }

    /// <summary>The logical queue the row came from.</summary>
    public string Queue { get; }

    /// <summary>The logical subscriber role, such as <c>Worker</c> or <c>ResponseIngress</c>.</summary>
    public string SubscriberRole { get; }

    /// <summary>The delivery attempt count for the row.</summary>
    public int Attempt { get; }

    /// <summary>The AsyncResponse correlation id, when one was available.</summary>
    public string? CorrelationId { get; }

    /// <summary>The exception thrown by the background handler.</summary>
    public Exception Exception { get; }
}

/// <summary>Per-queue SQL Server subscriber behavior.</summary>
public sealed class SqlServerSubscriberOptions
{
    /// <summary>Controls when a row is acknowledged. Defaults to <see cref="SqlServerAckMode.AckAfterHandlerCompletes"/>.</summary>
    public SqlServerAckMode AckMode { get; set; } = SqlServerAckMode.AckAfterHandlerCompletes;

    /// <summary>
    /// Maximum rows claimed per subscriber loop pass. In the default
    /// <see cref="SqlServerAckMode.AckAfterHandlerCompletes"/> mode the claimed rows are handled
    /// one at a time, so this bounds claim round-trips, not handler concurrency. Use
    /// <see cref="SqlServerAckMode.AckAfterEnqueue"/> (or run multiple subscriber instances) to
    /// process messages in parallel. Default: <c>16</c>.
    /// </summary>
    public int BatchSize { get; set; } = 16;

    /// <summary>
    /// Maximum delivery attempts before a failing row is deleted and written to the dead-letter
    /// queue. <c>0</c> means unlimited retries. Default: <c>5</c>.
    /// </summary>
    public int MaxDeliveryAttempts { get; set; } = 5;

    /// <summary>Delay before a failed row becomes available for redelivery. Default: <c>5s</c>.</summary>
    public TimeSpan RedeliveryDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Delay after an empty poll before checking again. SQL Server has no server-push notification
    /// (unlike PostgreSQL <c>LISTEN/NOTIFY</c>), so cross-process publishes are picked up within this
    /// delay; same-process publishes wake the subscriber immediately. Default: <c>250ms</c>.
    /// </summary>
    public TimeSpan EmptyPollDelay { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>Number of background workers used by <see cref="SqlServerAckMode.AckAfterEnqueue"/>.</summary>
    public int BackgroundWorkerCount { get; set; }

    /// <summary>Maximum number of ACKed rows waiting in the background queue.</summary>
    public int BackgroundQueueCapacity { get; set; }

    /// <summary>Maximum time to wait for queued/running background handlers while stopping.</summary>
    public TimeSpan BackgroundDrainTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>Optional callback invoked when a background handler fails after the row was already acknowledged.</summary>
    public Func<SqlServerBackgroundFailureContext, ValueTask>? OnBackgroundFailure { get; set; }

    /// <summary>Explicitly opts this subscriber into ACK-after-enqueue behavior.</summary>
    public SqlServerSubscriberOptions UseAckAfterEnqueue(
        int backgroundWorkerCount,
        int backgroundQueueCapacity,
        TimeSpan? backgroundDrainTimeout = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundWorkerCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(backgroundQueueCapacity);
        if (backgroundDrainTimeout is { } timeout && timeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(backgroundDrainTimeout), timeout, "Drain timeout must be positive.");

        AckMode = SqlServerAckMode.AckAfterEnqueue;
        BackgroundWorkerCount = backgroundWorkerCount;
        BackgroundQueueCapacity = backgroundQueueCapacity;
        if (backgroundDrainTimeout is not null)
            BackgroundDrainTimeout = backgroundDrainTimeout.Value;
        return this;
    }
}
