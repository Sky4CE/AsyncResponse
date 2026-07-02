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
/// consumer and touches it only from its own poll loop.
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
    /// <see cref="Close"/>.
    /// </summary>
    void StoreOffset(string topic, int partition, long offset);

    /// <summary>Pauses fetching on all currently assigned partitions (backpressure).</summary>
    void PauseAssignment();

    /// <summary>Resumes fetching on all currently assigned partitions.</summary>
    void ResumeAssignment();

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
    private readonly Lazy<IProducer<string?, byte[]>> _producer;

    /// <summary>Runs the KafkaProducerClientAdapter operation.</summary>
    public KafkaProducerClientAdapter(KafkaAsyncResponseTransportOptions options)
    {
        _options = options;
        // Lazy so constructing the adapter (e.g. during DI validation) never dials the broker.
        _producer = new Lazy<IProducer<string?, byte[]>>(CreateProducer, LazyThreadSafetyMode.ExecutionAndPublication);
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

        var result = await _producer.Value
            .ProduceAsync(topic, message, cancellationToken)
            .ConfigureAwait(false);

        return new KafkaPublishResult(result.Topic, result.Partition.Value, result.Offset.Value);
    }

    /// <summary>Releases resources held by this instance.</summary>
    public void Dispose()
    {
        if (!_producer.IsValueCreated)
            return;

        try
        {
            _producer.Value.Flush(_options.OperationTimeout);
        }
        catch (KafkaException)
        {
            // Best-effort drain of in-flight messages on shutdown; Dispose below always runs.
        }

        _producer.Value.Dispose();
    }

    private IProducer<string?, byte[]> CreateProducer()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = KafkaTransportOptionsValidator.Required(
                _options.BootstrapServers,
                nameof(_options.BootstrapServers)),
            ClientId = KafkaTransportClientDefaults.ResolveClientId(_options),
            // Worker jobs must not be silently reordered or duplicated by producer-side retries.
            Acks = Acks.All,
            EnableIdempotence = true
        };

        _options.ConfigureProducer?.Invoke(config);
        return new ProducerBuilder<string?, byte[]>(config).Build();
    }
}

internal sealed class KafkaConsumerClientAdapter(IConsumer<string?, byte[]> _consumer) : IKafkaConsumerClient
{
    /// <summary>Subscribes the consumer group member to the given topic.</summary>
    public void Subscribe(string topic)
        => _consumer.Subscribe(topic);

    /// <summary>Polls for one message.</summary>
    public KafkaIncomingMessage? Consume(TimeSpan maxWait)
    {
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
        => _consumer.StoreOffset(new TopicPartitionOffset(topic, new Partition(partition), new Offset(offset + 1)));

    /// <summary>Pauses fetching on all currently assigned partitions.</summary>
    public void PauseAssignment()
        => _consumer.Pause(_consumer.Assignment);

    /// <summary>Resumes fetching on all currently assigned partitions.</summary>
    public void ResumeAssignment()
        => _consumer.Resume(_consumer.Assignment);

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
        var config = new ConsumerConfig
        {
            BootstrapServers = KafkaTransportOptionsValidator.Required(
                _options.BootstrapServers,
                nameof(_options.BootstrapServers)),
            GroupId = role is KafkaSubscriberRole.Worker
                ? _options.WorkerConsumerGroup
                : _options.ResponseConsumerGroup,
            ClientId = $"{KafkaTransportClientDefaults.ResolveClientId(_options)}-{role.ToString().ToLowerInvariant()}",
            // Manual offset management: offsets are stored per resolved message (StoreOffset) and
            // flushed by the auto-committer, so a crash redelivers at-least-once instead of losing work.
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false,
            AutoCommitIntervalMs = (int)Math.Max(1, _options.OffsetCommitInterval.TotalMilliseconds),
            // Start new consumer groups at the beginning of the topic so messages published before
            // the first subscriber starts are not skipped (mirrors the other transports).
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = false
        };

        _options.ConfigureConsumer?.Invoke(config);
        return new KafkaConsumerClientAdapter(new ConsumerBuilder<string?, byte[]>(config).Build());
    }
}

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

    /// <summary>Runs the IsTransient operation.</summary>
    public static bool IsTransient(Exception exception)
        => exception is TimeoutException
            || (exception is KafkaException kafkaException && !kafkaException.Error.IsFatal);
}
