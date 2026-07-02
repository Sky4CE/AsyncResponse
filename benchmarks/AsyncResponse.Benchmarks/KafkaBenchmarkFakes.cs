using AsyncResponse.Transports.Kafka;

namespace AsyncResponse.Benchmarks;

internal sealed class BenchmarkKafkaConsumerClient : IKafkaConsumerClient
{
    private int _storedOffsets;

    public int StoredOffsets => Volatile.Read(ref _storedOffsets);

    public void Subscribe(string topic)
    {
    }

    public KafkaIncomingMessage? Consume(TimeSpan maxWait) => null;

    public void StoreOffset(string topic, int partition, long offset)
        => Interlocked.Increment(ref _storedOffsets);

    public void PauseAssignment()
    {
    }

    public void ResumeAssignment()
    {
    }

    public void Close()
    {
    }

    public void Dispose()
    {
    }
}

internal sealed class BenchmarkKafkaProducerClient : IKafkaProducerClient
{
    private long _publishes;

    public long Publishes => Volatile.Read(ref _publishes);

    public Task<KafkaPublishResult> PublishAsync(
        string topic,
        string? key,
        byte[] payload,
        IReadOnlyList<KafkaTransportHeader> headers,
        CancellationToken cancellationToken)
        => Task.FromResult(new KafkaPublishResult(topic, 0, Interlocked.Increment(ref _publishes)));

    public void Dispose()
    {
    }
}
