using AsyncResponse.Transports.NATS;
using System.Runtime.CompilerServices;

namespace AsyncResponse.Benchmarks;

/// <summary>In-memory <see cref="INatsJetStreamTransport"/> for dispatcher microbenchmarks (no real NATS).</summary>
internal sealed class BenchmarkNatsJetStreamTransport : INatsJetStreamTransport
{
    private int _publishes;
    public int Publishes => Volatile.Read(ref _publishes);

    public Task EnsureStreamAsync(string stream, string subject, long? maxMessages, CancellationToken cancellationToken)
        => Task.CompletedTask;

    public Task EnsureConsumerAsync(string stream, string durable, TimeSpan ackWait, CancellationToken cancellationToken)
        => Task.CompletedTask;

    public Task<string> PublishAsync(string subject, string payload, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _publishes);
        return Task.FromResult(Publishes.ToString());
    }

    public async IAsyncEnumerable<NatsJobDelivery> ConsumeAsync(string stream, string durable, int batchSize, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}
