using AsyncResponse.Transports.RabbitMQ;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using System.Text;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// RabbitMQ subscriber callback dispatch overhead for the two ACK modes. These are deliberately
/// in-process microbenchmarks: integration/load tests cover the broker and HTTP-facing paths.
/// </summary>
[MemoryDiagnoser]
public class RabbitMqAckDispatchBenchmarks
{
    private static readonly RabbitMqDelivery Delivery = new(
        ConsumerTag: "benchmark-consumer",
        DeliveryTag: 1,
        Redelivered: false,
        Exchange: "benchmark-exchange",
        RoutingKey: "benchmark-route",
        BasicProperties: new BasicProperties
        {
            MessageId = "benchmark-message",
            ContentType = "application/json"
        },
        Body: Encoding.UTF8.GetBytes("{}"),
        CancellationToken: CancellationToken.None);

    private RabbitMqMessageDispatcher _awaiting = null!;
    private RabbitMqMessageDispatcher _queued = null!;
    private FakeRabbitMqChannel _channel = null!;

    [Params(1, 8)]
    public int BackgroundWorkers;

    [GlobalSetup]
    public void Setup()
    {
        _channel = new FakeRabbitMqChannel();
        var transportOptions = new RabbitMqAsyncResponseOptions
        {
            HostShutdownTimeout = TimeSpan.FromSeconds(60)
        };

        _awaiting = RabbitMqMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            transportOptions,
            new RabbitMqSubscriberOptions(),
            NullLogger.Instance,
            "benchmark-queue",
            RabbitMqSubscriberRole.Worker);

        _queued = RabbitMqMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            transportOptions,
            new RabbitMqSubscriberOptions().UseAckAfterEnqueue(
                backgroundWorkerCount: BackgroundWorkers,
                backgroundQueueCapacity: 16_384,
                backgroundDrainTimeout: TimeSpan.FromSeconds(30)),
            NullLogger.Instance,
            "benchmark-queue",
            RabbitMqSubscriberRole.Worker);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _queued.DisposeAsync().ConfigureAwait(false);
        await _awaiting.DisposeAsync().ConfigureAwait(false);
        await _channel.DisposeAsync().ConfigureAwait(false);
    }

    [Benchmark(Baseline = true)]
    public Task AckAfterHandlerCompletes_Callback()
        => _awaiting.HandleAsync(Delivery, _channel, CancellationToken.None);

    [Benchmark]
    public Task AckAfterEnqueue_Callback()
        => _queued.HandleAsync(Delivery, _channel, CancellationToken.None);

    private sealed class FakeRabbitMqChannel : IRabbitMqChannel
    {
        public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object?>? arguments = null, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task QueueBindAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public ValueTask BasicPublishAsync(string exchange, string routingKey, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
            => ValueTask.CompletedTask;

        public Task<string> BasicConsumeAsync(string queue, Func<RabbitMqDelivery, Task> handler, CancellationToken cancellationToken = default)
            => Task.FromResult("benchmark-consumer");

        public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public ValueTask BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken = default)
            => ValueTask.CompletedTask;

        public ValueTask BasicNackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default)
            => ValueTask.CompletedTask;

        public Task CloseAsync(CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public ValueTask DisposeAsync()
            => ValueTask.CompletedTask;
    }
}
