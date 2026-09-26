using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System.Text;

namespace AsyncResponse.Transports.Kafka;

/// <summary>One Kafka message header. Values are raw bytes, per the Kafka wire model.</summary>
internal readonly record struct KafkaTransportHeader(string Key, byte[]? Value)
{
    public static KafkaTransportHeader Utf8(string key, string value)
        => new(key, Encoding.UTF8.GetBytes(value));

    public string? ValueUtf8 => Value is null ? null : Encoding.UTF8.GetString(Value);
}

/// <summary>One message pulled from a Kafka topic, decoupled from the vendor client types.</summary>
internal sealed record KafkaIncomingMessage(
    string Topic,
    int Partition,
    long Offset,
    byte[]? Payload,
    IReadOnlyList<KafkaTransportHeader> Headers);

/// <summary>Broker coordinates assigned to a produced message.</summary>
internal sealed record KafkaPublishResult(string Topic, int Partition, long Offset);

/// <summary>
/// Adapter seam over the Kafka producer so the publish and dead-letter paths are unit-testable
/// on fakes. One producer instance is shared process-wide: Kafka producers are thread-safe and
/// expensive to create.
/// </summary>
internal interface IKafkaProducerClient : IDisposable
{
    Task<KafkaPublishResult> PublishAsync(
        string topic,
        string? key,
        byte[] payload,
        IReadOnlyList<KafkaTransportHeader> headers,
        CancellationToken cancellationToken);
}

/// <summary>
/// Adapter seam over one Kafka consumer. Not thread-safe: each hosted subscriber owns one
/// consumer and touches it only from its own poll loop — detached handlers never call it; their
/// completions are settled by the poll thread (and, after the loop has exited, by the
/// dispatcher's disposal, sequentially).
/// </summary>
internal interface IKafkaConsumerClient : IDisposable
{
    /// <summary>Subscribes the consumer group member to the given topic.</summary>
    void Subscribe(string topic);

    /// <summary>
    /// Polls for one message, waiting at most <paramref name="maxWait"/>. Returns <c>null</c> when
    /// no message arrived in time so the caller can re-check cancellation and backpressure state.
    /// </summary>
    KafkaIncomingMessage? Consume(TimeSpan maxWait);

    /// <summary>
    /// Marks the message resolved by storing <c>offset + 1</c> for its partition. The stored
    /// offset is committed by the consumer's auto-committer on the configured interval, and on
    /// <see cref="Close"/>. Throws <see cref="KafkaPartitionNotAssignedException"/> when the
    /// partition is no longer assigned to this consumer (a rebalance took it — routine).
    /// </summary>
    void StoreOffset(string topic, int partition, long offset);

    /// <summary>
    /// A counter that changes every time the partition is revoked from (or lost by) this consumer.
    /// A value captured while a message was consumed and compared later tells whether this member
    /// still holds the SAME assignment of the partition: after it moved away — even if it has come
    /// back since — another member may have committed past the message, so its offset must not be
    /// stored from here unless the new assignment shows the group did not move past it (a stored
    /// offset is committed as-is, and Kafka's offset commit is not monotonic: storing it rewinds
    /// the group). Read and advanced on the poll thread only (the rebalance callbacks run inside
    /// <see cref="Consume"/> and <see cref="Close"/>).
    /// </summary>
    long GetAssignmentGeneration(string topic, int partition);

    /// <summary>
    /// Pauses fetching on all currently assigned partitions (backpressure). The pause holds until
    /// <see cref="ResumeAssignment"/>, across a revoke and a re-assignment of the partition too, so
    /// a partition handed back while it holds does not fetch into the full queue.
    /// </summary>
    void PauseAssignment();

    /// <summary>
    /// Lifts the <see cref="PauseAssignment"/> pause: at once on the currently assigned partitions,
    /// and on a partition it covered that is not assigned right now (an eager rebalance revoked it
    /// and has not handed it back yet) as soon as that partition is assigned to this consumer again.
    /// </summary>
    void ResumeAssignment();

    /// <summary>
    /// Pauses fetching on one partition while a detached handler runs its message, so the
    /// partition's order holds with nothing buffered in-process. Throws when the partition is not
    /// currently assigned (revoked by a rebalance); callers treat that as informational. This
    /// pause lasts at most for the current assignment of the partition: a revoke (or loss) lifts
    /// it, so a partition handed back to this member later — straight away by an eager rebalance,
    /// or after a peer held it — fetches again until the caller re-asserts the pause.
    /// </summary>
    void PausePartition(string topic, int partition);

    /// <summary>Resumes fetching on one partition once its detached handler has settled. Throws when it is no longer assigned.</summary>
    void ResumePartition(string topic, int partition);

    /// <summary>Leaves the group cleanly, committing stored offsets.</summary>
    void Close();
}

/// <summary>Creates one consumer per hosted subscriber role.</summary>
internal interface IKafkaConsumerClientFactory
{
    IKafkaConsumerClient Create(KafkaSubscriberRole role);
}

/// <summary>Adapter seam over the Kafka admin client for startup topic provisioning.</summary>
internal interface IKafkaAdminClient
{
    /// <summary>Creates any missing topics; existing topics are left untouched.</summary>
    Task EnsureTopicsAsync(
        IReadOnlyList<string> topics,
        int numPartitions,
        short replicationFactor,
        CancellationToken cancellationToken);
}

internal sealed class KafkaProducerClientAdapter : IKafkaProducerClient
{
    private readonly KafkaAsyncResponseTransportOptions _options;
    private readonly object _producerGate = new();
    private IProducer<string?, byte[]>? _producer;
    private bool _disposed;

    /// <summary>Runs the KafkaProducerClientAdapter operation.</summary>
    public KafkaProducerClientAdapter(KafkaAsyncResponseTransportOptions options)
    {
        _options = options;
    }

    // Built lazily so constructing the adapter (e.g. during DI validation) never dials the broker,
    // and assigned only on SUCCESS so a faulted build attempt is not cached: this adapter is a
    // process-lifetime singleton, and Lazy<T>'s ExecutionAndPublication mode would rethrow one
    // transient construction failure on every later publish until restart. Parity with the
    // RabbitMQ/Pub-Sub/SQS worker transports, whose comments pin the same rule.
    private IProducer<string?, byte[]> Producer
    {
        get
        {
            if (Volatile.Read(ref _producer) is { } existing)
                return existing;

            lock (_producerGate)
            {
                // Checked under the same gate Dispose latches under: a build racing Dispose must
                // either be flushed and disposed by Dispose (build won the gate) or never happen
                // (Dispose won) — a producer built after the latch would leak its librdkafka
                // threads and silently drop its buffered jobs at shutdown.
                ObjectDisposedException.ThrowIf(_disposed, this);
                return _producer ??= CreateProducer();
            }
        }
    }

    /// <summary>Publishes the supplied message.</summary>
    public async Task<KafkaPublishResult> PublishAsync(
        string topic,
        string? key,
        byte[] payload,
        IReadOnlyList<KafkaTransportHeader> headers,
        CancellationToken cancellationToken)
    {
        var message = new Message<string?, byte[]>
        {
            Key = key,
            Value = payload
        };

        if (headers.Count > 0)
        {
            message.Headers = [];
            foreach (var header in headers)
                message.Headers.Add(header.Key, header.Value);
        }

        var result = await Producer
            .ProduceAsync(topic, message, cancellationToken)
            .ConfigureAwait(false);

        return new KafkaPublishResult(result.Topic, result.Partition.Value, result.Offset.Value);
    }

    /// <summary>Releases resources held by this instance.</summary>
    public void Dispose()
    {
        // The whole read/flush/dispose runs under the build gate: reading the field outside it
        // raced a concurrent build — whichever the read missed was never flushed (dropping its
        // buffered jobs and leaking librdkafka threads), and the reverse order left later
        // publishes producing onto a disposed handle.
        lock (_producerGate)
        {
            if (_disposed)
                return;

            _disposed = true;
            var producer = _producer;
            _producer = null;
            if (producer is null)
                return;

            try
            {
                producer.Flush(_options.OperationTimeout);
            }
            catch (KafkaException)
            {
                // Best-effort drain of in-flight messages on shutdown; Dispose below always runs.
            }

            producer.Dispose();
        }
    }

    /// <summary>librdkafka's <c>message.max.bytes</c> default.</summary>
    internal const int DefaultMessageMaxBytes = 1_000_000;

    /// <summary>
    /// The producer's <c>message.max.bytes</c> as the package will build it (defaults, then
    /// <see cref="KafkaAsyncResponseTransportOptions.ConfigureProducer"/>): librdkafka rejects a
    /// record whose key + value + headers exceed it before it is ever sent, so the dead-letter copy
    /// budgets its forensic headers against it. Falls back to the librdkafka default when the hook
    /// throws — this is a size estimate, never a reason to fail.
    /// </summary>
    internal static int ResolveMessageMaxBytes(KafkaAsyncResponseTransportOptions options)
    {
        try
        {
            return BuildConfig(options).MessageMaxBytes is { } configured && configured > 0
                ? configured
                : DefaultMessageMaxBytes;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return DefaultMessageMaxBytes;
        }
    }

    private IProducer<string?, byte[]> CreateProducer()
        => new ProducerBuilder<string?, byte[]>(BuildConfig(_options)).Build();

    private static ProducerConfig BuildConfig(KafkaAsyncResponseTransportOptions options)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = KafkaTransportOptionsValidator.Required(
                options.BootstrapServers,
                nameof(options.BootstrapServers)),
            ClientId = KafkaTransportClientDefaults.ResolveClientId(options),
            // Worker jobs must not be silently reordered or duplicated by producer-side retries.
            Acks = Acks.All,
            EnableIdempotence = true
        };

        options.ConfigureProducer?.Invoke(config);
        return config;
    }
}

/// <summary>
/// The partition was not assigned to the consumer when its offset was stored: a rebalance revoked
/// it (or the member lost its assignment) — routine in a consumer group. Vendor-free, so the
/// dispatcher can tell it from a real store failure without referencing the Kafka client.
/// </summary>
internal sealed class KafkaPartitionNotAssignedException(string topic, int partition, Exception innerException)
    : Exception($"Kafka partition {topic}[{partition}] is not assigned to this consumer.", innerException);

/// <summary>
/// Per-partition assignment generations for one consumer, advanced by its revoked/lost rebalance
/// handlers (see <see cref="IKafkaConsumerClient.GetAssignmentGeneration"/>).
/// </summary>
internal sealed class KafkaAssignmentGenerations
{
    private readonly object _gate = new();
    private readonly Dictionary<(string Topic, int Partition), long> _generations = [];

    public long Get(string topic, int partition)
    {
        lock (_gate)
        {
            return _generations.TryGetValue((topic, partition), out var generation) ? generation : 0;
        }
    }

    public void Advance(IEnumerable<(string Topic, int Partition)> partitions)
    {
        lock (_gate)
        {
            foreach (var key in partitions)
                _generations[key] = (_generations.TryGetValue(key, out var generation) ? generation : 0) + 1;
        }
    }
}

internal sealed class KafkaConsumerClientAdapter(
    IConsumer<string?, byte[]> _consumer,
    KafkaAssignmentGenerations? _generations = null) : IKafkaConsumerClient
{
    private readonly KafkaAssignmentGenerations _assignmentGenerations = _generations ?? new KafkaAssignmentGenerations();

    /// <summary>
    /// Partitions paused one by one (<see cref="PausePartition"/>) and not resumed since.
    /// Poll-thread only, like the generations: the rebalance callbacks that read it run inside
    /// <see cref="Consume"/> and <see cref="Close"/>.
    /// </summary>
    private readonly HashSet<(string Topic, int Partition)> _pausedPartitions = [];

    /// <summary>
    /// Partitions the backpressure pause (<see cref="PauseAssignment"/>) set librdkafka's application
    /// pause on and no <see cref="ResumeAssignment"/> has lifted since — assigned right now or not:
    /// librdkafka keeps that pause on the partition across a revoke and a re-assignment, so one taken
    /// before an eager revoke is still in force when the partition comes back. Poll-thread only.
    /// </summary>
    private readonly HashSet<(string Topic, int Partition)> _backpressurePaused = [];

    /// <summary>Whether the poll loop's backpressure pause is in force (between PauseAssignment and ResumeAssignment).</summary>
    private bool _backpressure;

    /// <summary>
    /// Partitions assigned again while still carrying a backpressure pause that was lifted while
    /// they were unassigned: resumed at the next poll, once the assignment has taken effect (the
    /// assigned callback runs BEFORE the client assigns them). Poll-thread only.
    /// </summary>
    private List<TopicPartition>? _resumeOnceAssigned;

    /// <summary>Subscribes the consumer group member to the given topic.</summary>
    public void Subscribe(string topic)
        => _consumer.Subscribe(topic);

    /// <summary>Polls for one message.</summary>
    public KafkaIncomingMessage? Consume(TimeSpan maxWait)
    {
        ResumeReassigned();
        var result = _consumer.Consume(maxWait);
        if (result is null || result.IsPartitionEOF)
            return null;

        return new KafkaIncomingMessage(
            result.Topic,
            result.Partition.Value,
            result.Offset.Value,
            result.Message?.Value,
            ReadHeaders(result.Message?.Headers));
    }

    /// <summary>Marks the message resolved by storing the next offset for its partition.</summary>
    public void StoreOffset(string topic, int partition, long offset)
    {
        try
        {
            _consumer.StoreOffset(new TopicPartitionOffset(topic, new Partition(partition), new Offset(offset + 1)));
        }
        catch (KafkaException ex) when (ex.Error.Code == ErrorCode.Local_State)
        {
            // librdkafka refuses a store for a partition that is not currently assigned
            // (RD_KAFKA_RESP_ERR__STATE).
            throw new KafkaPartitionNotAssignedException(topic, partition, ex);
        }
    }

    /// <inheritdoc />
    public long GetAssignmentGeneration(string topic, int partition)
        => _assignmentGenerations.Get(topic, partition);

    /// <summary>Pauses fetching on all currently assigned partitions.</summary>
    public void PauseAssignment()
    {
        var assignment = _consumer.Assignment;
        _consumer.Pause(assignment);
        _backpressure = true;
        foreach (var partition in assignment)
            _backpressurePaused.Add((partition.Topic, partition.Partition.Value));
    }

    /// <summary>
    /// Resumes fetching on all currently assigned partitions. A partition the backpressure pause
    /// covered that is not assigned right now stays marked, and is resumed once it is assigned
    /// again (<see cref="OnPartitionsAssigned"/>): resuming only the current assignment — empty
    /// between an eager revoke and the re-assignment that follows it — left the partitions handed
    /// back paused for the life of the consumer, since nothing was fetched into the queue again to
    /// trip another pause-and-resume.
    /// </summary>
    public void ResumeAssignment()
    {
        var assignment = _consumer.Assignment;
        _consumer.Resume(assignment);
        _backpressure = false;
        foreach (var partition in assignment)
            _backpressurePaused.Remove((partition.Topic, partition.Partition.Value));
    }

    /// <summary>Pauses fetching on one partition.</summary>
    public void PausePartition(string topic, int partition)
    {
        _consumer.Pause([new TopicPartition(topic, new Partition(partition))]);
        _pausedPartitions.Add((topic, partition));
    }

    /// <summary>Resumes fetching on one partition.</summary>
    public void ResumePartition(string topic, int partition)
    {
        _consumer.Resume([new TopicPartition(topic, new Partition(partition))]);
        _pausedPartitions.Remove((topic, partition));
    }

    /// <summary>
    /// The revoked/lost rebalance callback: advances the partitions' assignment generations and
    /// RESUMES those this adapter paused one by one, so such a pause never outlives the assignment
    /// it was taken in (see <see cref="IKafkaConsumerClient.PausePartition"/>). librdkafka keeps an
    /// application pause on the partition object across a revoke and a later re-assignment — only
    /// its own internal pause is reset on assign — so a partition paused behind a detached handler
    /// came back to this member still paused: nothing of the new assignment was fetched, and once
    /// the stale handler finished (an orphan, which never resumes) the partition stayed parked for
    /// the life of the consumer. The callback runs while the partitions are still assigned, just
    /// before the client unassigns them; the unassign discards whatever the resume might still
    /// fetch for the old assignment. The assignment-wide backpressure pause
    /// (<see cref="PauseAssignment"/>) is left in force while it holds — a partition handed back
    /// during backpressure must not fetch into the full queue — and is lifted by
    /// <see cref="ResumeAssignment"/>, on a partition that is away at that moment once it comes back
    /// (<see cref="OnPartitionsAssigned"/>).
    /// </summary>
    internal void OnPartitionsRemoved(List<TopicPartitionOffset> partitions)
    {
        _assignmentGenerations.Advance(partitions.Select(p => (p.Topic, p.Partition.Value)));

        List<TopicPartition>? paused = null;
        foreach (var partition in partitions)
        {
            var key = (partition.Topic, partition.Partition.Value);
            if (_pausedPartitions.Remove(key))
                (paused ??= []).Add(partition.TopicPartition);

            // Assigned and revoked again inside one poll, before the resume it was due: lifted
            // here, while it is still assigned, like a pause taken behind a detached handler.
            if (_resumeOnceAssigned?.RemoveAll(pending => pending.Topic == key.Topic && pending.Partition.Value == key.Value) > 0)
                (paused ??= []).Add(partition.TopicPartition);
        }

        if (paused is null)
            return;

        try
        {
            _consumer.Resume(paused);
        }
        catch (KafkaException)
        {
            // Includes the per-partition TopicPartitionException. A throw here would escape the
            // rebalance callback and fault the poll loop; a partition that is still assigned (as it
            // is during this callback) resumes without error.
        }
    }

    /// <summary>
    /// The assigned rebalance callback, run inside <see cref="Consume"/> just BEFORE the client
    /// assigns <paramref name="partitions"/>. A partition coming back with a backpressure pause
    /// that <see cref="ResumeAssignment"/> lifted while it was away (librdkafka kept the pause on it
    /// across the revoke) is queued for resuming at the next poll, once it is assigned. While the
    /// backpressure pause still holds, one it covered simply comes back paused, and the loop's
    /// resume lifts it with the rest of the assignment.
    /// </summary>
    internal void OnPartitionsAssigned(List<TopicPartition> partitions)
    {
        if (_backpressure)
            return;

        foreach (var partition in partitions)
        {
            if (_backpressurePaused.Remove((partition.Topic, partition.Partition.Value)))
                (_resumeOnceAssigned ??= []).Add(partition);
        }
    }

    /// <summary>
    /// Resumes the partitions <see cref="OnPartitionsAssigned"/> queued, now that the assignment
    /// the previous poll ran has taken effect — unless the backpressure pause was taken again in
    /// between, which then covers them (they are assigned, so PauseAssignment marked them).
    /// </summary>
    private void ResumeReassigned()
    {
        if (_resumeOnceAssigned is not { } partitions)
            return;

        _resumeOnceAssigned = null;
        if (_backpressure || partitions.Count == 0)
            return;

        try
        {
            _consumer.Resume(partitions);
        }
        catch (KafkaException)
        {
            // Includes the per-partition TopicPartitionException: a partition no longer assigned
            // has nothing to fetch here, and a throw would fault the poll loop.
        }
    }

    /// <summary>Leaves the group cleanly, committing stored offsets.</summary>
    public void Close()
        => _consumer.Close();

    /// <summary>Releases resources held by this instance.</summary>
    public void Dispose()
        => _consumer.Dispose();

    private static KafkaTransportHeader[] ReadHeaders(Headers? headers)
    {
        if (headers is null || headers.Count == 0)
            return [];

        var result = new KafkaTransportHeader[headers.Count];
        for (var i = 0; i < headers.Count; i++)
            result[i] = new KafkaTransportHeader(headers[i].Key, headers[i].GetValueBytes());

        return result;
    }
}

internal sealed class KafkaConsumerClientFactory(KafkaAsyncResponseTransportOptions _options) : IKafkaConsumerClientFactory
{
    /// <summary>Creates one consumer for the given subscriber role.</summary>
    public IKafkaConsumerClient Create(KafkaSubscriberRole role)
    {
        var config = BuildConfig(_options, role);

        // Revocation-aware settlement: every revoke/loss advances the partition's generation, so a
        // handler that outlived its partition's assignment can tell it must not store its offset
        // (see IKafkaConsumerClient.GetAssignmentGeneration), and lifts the partition's own pause
        // (see KafkaConsumerClientAdapter.OnPartitionsRemoved); every assign lets a backpressure
        // pause lifted while the partition was away be lifted on it too (OnPartitionsAssigned). The
        // Action overloads keep the client's own (incremental) assign and unassign after each
        // callback. The callbacks fire only inside Consume and Close, after the adapter below exists.
        KafkaConsumerClientAdapter? adapter = null;
        var consumer = new ConsumerBuilder<string?, byte[]>(config)
            .SetPartitionsAssignedHandler((_, assigned) => adapter!.OnPartitionsAssigned(assigned))
            .SetPartitionsRevokedHandler((_, revoked) => adapter!.OnPartitionsRemoved(revoked))
            .SetPartitionsLostHandler((_, lost) => adapter!.OnPartitionsRemoved(lost))
            .Build();
        adapter = new KafkaConsumerClientAdapter(consumer);
        return adapter;
    }

    /// <summary>
    /// The consumer configuration for <paramref name="role"/> exactly as <see cref="Create"/>
    /// applies it — package defaults, then <see cref="KafkaAsyncResponseTransportOptions.ConfigureConsumer"/>
    /// — so startup validation can check the final values without building a consumer.
    /// </summary>
    internal static ConsumerConfig BuildConfig(KafkaAsyncResponseTransportOptions options, KafkaSubscriberRole role)
        => BuildConfig(
            options,
            role,
            role is KafkaSubscriberRole.Worker ? options.WorkerSubscriber : options.ResponseSubscriber);

    /// <summary>librdkafka's <c>max.poll.interval.ms</c> default.</summary>
    private const int LibrdkafkaDefaultMaxPollIntervalMs = 300_000;

    /// <summary>
    /// The <c>max.poll.interval.ms</c> the role's consumer will actually run with:
    /// <paramref name="subscriberOptions"/>'s <see cref="KafkaSubscriberOptions.MaxPollInterval"/>,
    /// unless <see cref="KafkaAsyncResponseTransportOptions.ConfigureConsumer"/> overrides it (the
    /// hook runs last and may set <c>MaxPollIntervalMs</c>). The poll-gap rule and the dead-letter
    /// budget are derived from this, not from the option: judged against the option, a 60 s
    /// override passed validation for a 5 min interval, and a burial blocking the poll thread for up
    /// to a quarter of 5 min got the consumer evicted. Falls back to the option when the hook throws
    /// (startup reports that failure itself, when it builds the configuration).
    /// </summary>
    internal static TimeSpan ResolveMaxPollInterval(
        KafkaAsyncResponseTransportOptions options,
        KafkaSubscriberRole role,
        KafkaSubscriberOptions subscriberOptions)
    {
        try
        {
            // A hook that clears the setting leaves librdkafka on its own default.
            var milliseconds = BuildConfig(options, role, subscriberOptions).MaxPollIntervalMs ?? LibrdkafkaDefaultMaxPollIntervalMs;
            return milliseconds > 0
                ? TimeSpan.FromMilliseconds(milliseconds)
                : subscriberOptions.MaxPollInterval;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return subscriberOptions.MaxPollInterval;
        }
    }

    private static ConsumerConfig BuildConfig(
        KafkaAsyncResponseTransportOptions options,
        KafkaSubscriberRole role,
        KafkaSubscriberOptions subscriberOptions)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = KafkaTransportOptionsValidator.Required(
                options.BootstrapServers,
                nameof(options.BootstrapServers)),
            GroupId = role is KafkaSubscriberRole.Worker
                ? options.WorkerConsumerGroup
                : options.ResponseConsumerGroup,
            ClientId = $"{KafkaTransportClientDefaults.ResolveClientId(options)}-{role.ToString().ToLowerInvariant()}",
            // Manual offset management: offsets are stored per resolved message (StoreOffset) and
            // flushed by the auto-committer, so a crash redelivers at-least-once instead of losing work.
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false,
            AutoCommitIntervalMs = (int)Math.Max(1, options.OffsetCommitInterval.TotalMilliseconds),
            // The poll thread's longest gap (one inline handler wait of DetachHandlerAfter plus
            // one poll) is validated against this deadline at startup, so it is set explicitly
            // instead of trusting the librdkafka default to line up.
            MaxPollIntervalMs = (int)Math.Max(1, subscriberOptions.MaxPollInterval.TotalMilliseconds),
            // Start new consumer groups at the beginning of the topic so messages published before
            // the first subscriber starts are not skipped (mirrors the other transports).
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = false
        };

        options.ConfigureConsumer?.Invoke(config);
        return config;
    }
}

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class KafkaAdminClientAdapter(KafkaAsyncResponseTransportOptions _options) : IKafkaAdminClient
{
    /// <summary>Creates any missing topics; existing topics are left untouched.</summary>
    public async Task EnsureTopicsAsync(
        IReadOnlyList<string> topics,
        int numPartitions,
        short replicationFactor,
        CancellationToken cancellationToken)
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = KafkaTransportOptionsValidator.Required(
                _options.BootstrapServers,
                nameof(_options.BootstrapServers)),
            ClientId = $"{KafkaTransportClientDefaults.ResolveClientId(_options)}-admin"
        };

        _options.ConfigureAdminClient?.Invoke(config);
        using var adminClient = new AdminClientBuilder(config).Build();

        var specifications = topics
            .Select(topic => new TopicSpecification
            {
                Name = topic,
                NumPartitions = numPartitions,
                ReplicationFactor = replicationFactor
            })
            .ToArray();

        try
        {
            await adminClient.CreateTopicsAsync(
                specifications,
                new CreateTopicsOptions { RequestTimeout = _options.OperationTimeout })
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(result =>
            result.Error.Code is ErrorCode.NoError or ErrorCode.TopicAlreadyExists))
        {
            // The topics already exist; this is the expected path after the first app instance.
        }
    }
}

internal static class KafkaTransportClientDefaults
{
    private static readonly string GeneratedClientId = $"asyncresponse-{Environment.MachineName}-{Environment.ProcessId}";

    /// <summary>Runs the ResolveClientId operation.</summary>
    public static string ResolveClientId(KafkaAsyncResponseTransportOptions options)
        => !string.IsNullOrWhiteSpace(options.ClientId)
            ? options.ClientId!
            : GeneratedClientId;
}

internal static class KafkaTransportRetry
{
    /// <summary>Runs this background operation until cancellation is requested.</summary>
    public static Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> action,
        int maxAttempts,
        TimeSpan baseDelay,
        TimeSpan maxDelay,
        CancellationToken cancellationToken)
        => AsyncResponseRetry.ExecuteAsync(action, IsTransient, maxAttempts, baseDelay, maxDelay, cancellationToken);

    /// <summary>
    /// Whether a failed produce is worth another attempt: timeouts and non-fatal Kafka errors,
    /// except those a retry can only repeat. <c>Local_MsgTimedOut</c> is raised only AFTER
    /// librdkafka already retried internally for <c>message.timeout.ms</c> (5 minutes by default),
    /// so a re-produce multiplied one enqueue into many minutes — and may land a second copy of a
    /// record the broker had already persisted. The size and authorization errors never succeed
    /// on retry.
    /// </summary>
    public static bool IsTransient(Exception exception)
        => exception is TimeoutException
            || (exception is KafkaException kafkaException
                && !kafkaException.Error.IsFatal
                && kafkaException.Error.Code is not (
                    ErrorCode.Local_MsgTimedOut
                    or ErrorCode.MsgSizeTooLarge
                    or ErrorCode.RecordListTooLarge
                    or ErrorCode.TopicAuthorizationFailed
                    or ErrorCode.ClusterAuthorizationFailed));
}
