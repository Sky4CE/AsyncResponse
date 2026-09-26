using AsyncResponse.Transports.Kafka;
using System.Text;

namespace AsyncResponse.Tests;

/// <summary>
/// Hand-rolled fakes over the Kafka transport adapter seam. The real adapters wrap
/// Confluent.Kafka; these record calls and let tests inject failures deterministically.
/// </summary>
internal sealed class FakeKafkaProducerClient : IKafkaProducerClient
{
    private readonly object _gate = new();
    private long _offset;

    public List<PublishCall> Publishes { get; } = [];
    public int PublishAttempts { get; private set; }

    /// <summary>Number of leading publish attempts that throw a retriable Kafka error.</summary>
    public int TransientPublishFailuresBeforeSuccess { get; set; }

    /// <summary>When set, every publish attempt throws this exception.</summary>
    public Exception? PublishException { get; set; }

    /// <summary>
    /// When set, every publish awaits it before it is recorded — a slow or wedged broker the test
    /// releases.
    /// </summary>
    public Func<CancellationToken, Task>? PublishDelay { get; set; }

    public bool Disposed { get; private set; }

    public Task<KafkaPublishResult> PublishAsync(
        string topic,
        string? key,
        byte[] payload,
        IReadOnlyList<KafkaTransportHeader> headers,
        CancellationToken cancellationToken)
    {
        if (PublishDelay is { } delay)
            return DelayedPublishAsync(delay, topic, key, payload, headers, cancellationToken);

        return Record(topic, key, payload, headers);
    }

    private async Task<KafkaPublishResult> DelayedPublishAsync(
        Func<CancellationToken, Task> delay,
        string topic,
        string? key,
        byte[] payload,
        IReadOnlyList<KafkaTransportHeader> headers,
        CancellationToken cancellationToken)
    {
        await delay(cancellationToken);
        return await Record(topic, key, payload, headers);
    }

    private Task<KafkaPublishResult> Record(
        string topic,
        string? key,
        byte[] payload,
        IReadOnlyList<KafkaTransportHeader> headers)
    {
        lock (_gate)
        {
            PublishAttempts++;
            if (PublishAttempts <= TransientPublishFailuresBeforeSuccess)
                throw new Confluent.Kafka.KafkaException(new Confluent.Kafka.Error(Confluent.Kafka.ErrorCode.Local_Transport));
            if (PublishException is not null)
                throw PublishException;

            Publishes.Add(new PublishCall(topic, key, Encoding.UTF8.GetString(payload), headers));
            return Task.FromResult(new KafkaPublishResult(topic, 0, ++_offset));
        }
    }

    public void Dispose() => Disposed = true;

    internal sealed record PublishCall(
        string Topic,
        string? Key,
        string Payload,
        IReadOnlyList<KafkaTransportHeader> Headers);

    internal static string? Header(IReadOnlyList<KafkaTransportHeader> headers, string name)
        => headers.Where(header => header.Key == name).Select(header => header.ValueUtf8).FirstOrDefault();
}

internal sealed class FakeKafkaConsumerClient : IKafkaConsumerClient
{
    private readonly object _gate = new();
    private readonly List<KafkaIncomingMessage> _messages = [];
    private readonly HashSet<int> _pausedPartitions = [];
    private int _consumeCalls;

    public List<string> Subscriptions { get; } = [];
    public List<StoredOffset> StoredOffsets { get; } = [];
    public int PauseCount { get; private set; }
    public int ResumeCount { get; private set; }
    public bool Paused { get; private set; }
    public bool Closed { get; private set; }
    public bool Disposed { get; private set; }

    /// <summary>How many times Consume was called — the poll loop's liveness, as the broker sees it.</summary>
    public int ConsumeCalls => Volatile.Read(ref _consumeCalls);

    /// <summary>Per-partition pause/resume calls (the ack-after-handler detach path).</summary>
    public List<int> PartitionPauses { get; } = [];
    public List<int> PartitionResumes { get; } = [];

    public bool IsPartitionPaused(int partition)
    {
        lock (_gate)
        {
            return _pausedPartitions.Contains(partition);
        }
    }

    /// <summary>
    /// When set, a paused partition still delivers — what a rebalance does when it hands the
    /// partition back with its pause state reset, or a client delivering a message it had already
    /// fetched before the pause.
    /// </summary>
    public bool IgnorePartitionPause { get; set; }

    /// <summary>
    /// When set, a paused assignment still delivers — a client handing out a record it had already
    /// fetched before the pause took hold.
    /// </summary>
    public bool IgnoreAssignmentPause { get; set; }

    /// <summary>Messages enqueued and not consumed yet.</summary>
    public int PendingMessages
    {
        get
        {
            lock (_gate)
            {
                return _messages.Count;
            }
        }
    }

    /// <summary>When set, PausePartition/ResumePartition throw it (the partition is no longer assigned).</summary>
    public Exception? PartitionPauseException { get; set; }

    /// <summary>When set, the next Consume call throws this exception once.</summary>
    public Exception? NextConsumeException { get; set; }

    /// <summary>When set, every StoreOffset call throws this exception.</summary>
    public Exception? StoreOffsetException { get; set; }

    /// <summary>When set, Close throws this exception.</summary>
    public Exception? CloseException { get; set; }

    public void Enqueue(KafkaIncomingMessage message)
    {
        lock (_gate)
        {
            _messages.Add(message);
        }
    }

    public void Subscribe(string topic)
    {
        lock (_gate)
        {
            Subscriptions.Add(topic);
        }
    }

    public KafkaIncomingMessage? Consume(TimeSpan maxWait)
    {
        Interlocked.Increment(ref _consumeCalls);
        lock (_gate)
        {
            if (NextConsumeException is { } consumeException)
            {
                NextConsumeException = null;
                throw consumeException;
            }

            // Paused partitions deliver nothing, mirroring librdkafka semantics; the rest deliver
            // in the order they were enqueued.
            if (!Paused || IgnoreAssignmentPause)
            {
                for (var i = 0; i < _messages.Count; i++)
                {
                    var candidate = _messages[i];
                    if (!IgnorePartitionPause && _pausedPartitions.Contains(candidate.Partition))
                        continue;

                    _messages.RemoveAt(i);
                    return candidate;
                }
            }
        }

        // Keep the poll loop from spinning hot in tests while staying responsive.
        Thread.Sleep(TimeSpan.FromMilliseconds(Math.Clamp(maxWait.TotalMilliseconds, 1, 5)));
        return null;
    }

    public void StoreOffset(string topic, int partition, long offset)
    {
        if (StoreOffsetException is not null)
            throw StoreOffsetException;

        lock (_gate)
        {
            StoredOffsets.Add(new StoredOffset(topic, partition, offset));
        }
    }

    private readonly Dictionary<int, long> _generations = [];

    public long GetAssignmentGeneration(string topic, int partition)
    {
        lock (_gate)
        {
            return _generations.TryGetValue(partition, out var generation) ? generation : 0;
        }
    }

    /// <summary>
    /// Simulates a rebalance revoking (and, for the dispatcher's purposes, possibly handing back)
    /// the partitions: their assignment generation advances and their pause state resets. That is
    /// the ADAPTER's contract for a per-partition pause (<see cref="KafkaConsumerClientAdapter.OnPartitionsRemoved"/>
    /// resumes the revoked partitions it paused), not librdkafka's own behaviour: librdkafka keeps an
    /// application pause across a revoke and a re-assignment (verified against a real broker, r1).
    /// </summary>
    public void Revoke(params int[] partitions)
    {
        lock (_gate)
        {
            foreach (var partition in partitions)
            {
                _generations[partition] = (_generations.TryGetValue(partition, out var generation) ? generation : 0) + 1;
                _pausedPartitions.Remove(partition);
            }
        }
    }

    public void PauseAssignment()
    {
        lock (_gate)
        {
            Paused = true;
            PauseCount++;
        }
    }

    public void ResumeAssignment()
    {
        lock (_gate)
        {
            Paused = false;
            ResumeCount++;
        }
    }

    public void PausePartition(string topic, int partition)
    {
        if (PartitionPauseException is not null)
            throw PartitionPauseException;

        lock (_gate)
        {
            _pausedPartitions.Add(partition);
            PartitionPauses.Add(partition);
        }
    }

    public void ResumePartition(string topic, int partition)
    {
        if (PartitionPauseException is not null)
            throw PartitionPauseException;

        lock (_gate)
        {
            _pausedPartitions.Remove(partition);
            PartitionResumes.Add(partition);
        }
    }

    public void Close()
    {
        Closed = true;
        if (CloseException is not null)
            throw CloseException;
    }

    public void Dispose() => Disposed = true;

    internal sealed record StoredOffset(string Topic, int Partition, long Offset);
}

/// <summary>
/// A scripted stand-in for librdkafka under the REAL <see cref="KafkaConsumerClientAdapter"/>: an
/// assignment that a rebalance empties and refills, and an application pause per partition that —
/// as in librdkafka (verified against a real broker, r1) — survives the partition leaving and
/// rejoining the assignment. Rebalances are queued by the test and run inside the next Consume, on
/// the poll thread, through the adapter's own callbacks: exactly where the real client runs them.
/// </summary>
internal sealed class RebalancingKafkaConsumer
{
    private readonly object _gate = new();
    private readonly string _topic;
    private readonly List<Confluent.Kafka.TopicPartition> _assignment = [];
    private readonly HashSet<Confluent.Kafka.TopicPartition> _paused = [];
    private readonly List<Confluent.Kafka.ConsumeResult<string?, byte[]>> _messages = [];
    private readonly System.Collections.Concurrent.ConcurrentQueue<Action> _rebalances = new();
    private int _resumesWhileUnassigned;

    public RebalancingKafkaConsumer(string topic, params int[] partitions)
    {
        _topic = topic;
        _assignment.AddRange(partitions.Select(Partition));

        var consumer = new Moq.Mock<Confluent.Kafka.IConsumer<string?, byte[]>>();
        consumer.SetupGet(c => c.Assignment).Returns(() =>
        {
            lock (_gate)
            {
                return [.. _assignment];
            }
        });
        consumer.Setup(c => c.Pause(Moq.It.IsAny<IEnumerable<Confluent.Kafka.TopicPartition>>()))
            .Callback<IEnumerable<Confluent.Kafka.TopicPartition>>(pausing =>
            {
                lock (_gate)
                {
                    _paused.UnionWith(pausing);
                }
            });
        consumer.Setup(c => c.Resume(Moq.It.IsAny<IEnumerable<Confluent.Kafka.TopicPartition>>()))
            .Callback<IEnumerable<Confluent.Kafka.TopicPartition>>(resuming =>
            {
                lock (_gate)
                {
                    if (_assignment.Count == 0)
                        _resumesWhileUnassigned++;

                    _paused.ExceptWith(resuming);
                }
            });
        consumer.Setup(c => c.Consume(Moq.It.IsAny<TimeSpan>())).Returns((TimeSpan maxWait) => Consume(maxWait)!); // null: nothing arrived in time
        Adapter = new KafkaConsumerClientAdapter(consumer.Object);
    }

    /// <summary>The real adapter over this scripted client — what the subscriber polls.</summary>
    public KafkaConsumerClientAdapter Adapter { get; }

    public int AssignedCount
    {
        get
        {
            lock (_gate)
            {
                return _assignment.Count;
            }
        }
    }

    /// <summary>Messages enqueued and not consumed yet.</summary>
    public int PendingMessages
    {
        get
        {
            lock (_gate)
            {
                return _messages.Count;
            }
        }
    }

    /// <summary>Resume calls made while nothing was assigned (between an eager revoke and the re-assignment).</summary>
    public int ResumesWhileUnassigned
    {
        get
        {
            lock (_gate)
            {
                return _resumesWhileUnassigned;
            }
        }
    }

    public bool IsPaused(int partition)
    {
        lock (_gate)
        {
            return _paused.Contains(Partition(partition));
        }
    }

    public void Enqueue(int partition, long offset, string payload)
    {
        lock (_gate)
        {
            _messages.Add(new Confluent.Kafka.ConsumeResult<string?, byte[]>
            {
                Topic = _topic,
                Partition = new Confluent.Kafka.Partition(partition),
                Offset = new Confluent.Kafka.Offset(offset),
                Message = new Confluent.Kafka.Message<string?, byte[]> { Value = Encoding.UTF8.GetBytes(payload) }
            });
        }
    }

    /// <summary>An eager rebalance revoking the whole assignment, run inside the next Consume.</summary>
    public void RevokeAll()
        => _rebalances.Enqueue(() =>
        {
            List<Confluent.Kafka.TopicPartitionOffset> revoked;
            lock (_gate)
            {
                revoked = [.. _assignment.Select(partition => new Confluent.Kafka.TopicPartitionOffset(partition, Confluent.Kafka.Offset.Unset))];
            }

            Adapter.OnPartitionsRemoved(revoked); // while still assigned, as the client calls it
            lock (_gate)
            {
                _assignment.Clear();
            }
        });

    /// <summary>The group handing <paramref name="partitions"/> (back) to this member, run inside the next Consume.</summary>
    public void Assign(params int[] partitions)
        => _rebalances.Enqueue(() =>
        {
            var assigned = partitions.Select(Partition).ToList();
            Adapter.OnPartitionsAssigned(assigned); // before the client assigns them, as it calls it
            lock (_gate)
            {
                _assignment.AddRange(assigned);
            }
        });

    private Confluent.Kafka.TopicPartition Partition(int partition) => new(_topic, new Confluent.Kafka.Partition(partition));

    private Confluent.Kafka.ConsumeResult<string?, byte[]>? Consume(TimeSpan maxWait)
    {
        while (_rebalances.TryDequeue(out var rebalance))
            rebalance();

        lock (_gate)
        {
            for (var i = 0; i < _messages.Count; i++)
            {
                var candidate = _messages[i];
                if (!_assignment.Contains(candidate.TopicPartition) || _paused.Contains(candidate.TopicPartition))
                    continue;

                _messages.RemoveAt(i);
                return candidate;
            }
        }

        Thread.Sleep(TimeSpan.FromMilliseconds(Math.Clamp(maxWait.TotalMilliseconds, 1, 5)));
        return null;
    }
}

internal sealed class FakeKafkaConsumerClientFactory : IKafkaConsumerClientFactory
{
    private readonly Queue<IKafkaConsumerClient> _consumers = new();

    public List<KafkaSubscriberRole> CreatedRoles { get; } = [];

    public FakeKafkaConsumerClientFactory(params FakeKafkaConsumerClient[] consumers)
        : this(consumers.Cast<IKafkaConsumerClient>().ToArray())
    {
    }

    /// <summary>Hands out any consumer client — e.g. the real adapter over a scripted librdkafka.</summary>
    public FakeKafkaConsumerClientFactory(params IKafkaConsumerClient[] consumers)
    {
        foreach (var consumer in consumers)
            _consumers.Enqueue(consumer);
    }

    public IKafkaConsumerClient Create(KafkaSubscriberRole role)
    {
        CreatedRoles.Add(role);
        return _consumers.Count > 0
            ? _consumers.Dequeue()
            : new FakeKafkaConsumerClient();
    }
}

internal sealed class FakeKafkaAdminClient : IKafkaAdminClient
{
    public List<EnsureTopicsCall> EnsureTopicsCalls { get; } = [];

    /// <summary>When set, every EnsureTopicsAsync call throws this exception.</summary>
    public Exception? EnsureTopicsException { get; set; }

    public Task EnsureTopicsAsync(
        IReadOnlyList<string> topics,
        int numPartitions,
        short replicationFactor,
        CancellationToken cancellationToken)
    {
        if (EnsureTopicsException is not null)
            throw EnsureTopicsException;

        lock (EnsureTopicsCalls)
        {
            EnsureTopicsCalls.Add(new EnsureTopicsCall([.. topics], numPartitions, replicationFactor));
        }

        return Task.CompletedTask;
    }

    internal sealed record EnsureTopicsCall(string[] Topics, int NumPartitions, short ReplicationFactor);
}

internal static class KafkaTestData
{
    public static KafkaAsyncResponseTransportOptions NewOptions()
        => new() { BootstrapServers = "localhost:9092" };

    public static KafkaIncomingMessage Message(
        string topic,
        long offset,
        string payload,
        params (string Key, string Value)[] headers)
        => MessageOn(topic, partition: 0, offset, payload, headers);

    public static KafkaIncomingMessage MessageOn(
        string topic,
        int partition,
        long offset,
        string payload,
        params (string Key, string Value)[] headers)
        => new(
            topic,
            partition,
            offset,
            Encoding.UTF8.GetBytes(payload),
            headers.Select(header => KafkaTransportHeader.Utf8(header.Key, header.Value)).ToArray());

    public static KafkaDelivery Delivery(
        string topic,
        long offset,
        string payload = "payload-json",
        string? correlationId = "corr",
        int partition = 0)
        => new(
            topic,
            partition,
            offset,
            payload,
            correlationId,
            correlationId is null
                ? []
                : [KafkaTransportHeader.Utf8("correlationId", correlationId)]);

    /// <summary>Polls until <paramref name="condition"/> holds or the timeout elapses.</summary>
    public static async Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTimeOffset.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (!condition())
        {
            if (DateTimeOffset.UtcNow > deadline)
                throw new TimeoutException("Condition was not reached within the timeout.");

            await Task.Delay(10);
        }
    }
}
