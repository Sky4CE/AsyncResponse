using AsyncResponse.Transports.RabbitMQ;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using System.Text;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public class RabbitMqTransportTests
{
    [Fact]
    public void WithRabbitMqTransport_ReplacesWorkerTransportAndReplyTargetProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));

        var provider = services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithRabbitMqTransport(options =>
            {
                options.ConnectionString = "amqp://guest:guest@localhost:5672/";
                options.WorkerExchange = "workers";
                options.WorkerQueue = "workers";
                options.WorkerRoutingKey = "workers";
                options.ResponseExchange = "responses";
                options.ResponseQueue = "responses";
                options.ResponseRoutingKey = "responses";
            })
            .Services
            .BuildServiceProvider();

        Assert.IsType<RabbitMqWorkerTransport>(provider.GetRequiredService<IWorkerTransport>());
        Assert.IsType<RabbitMqReplyTargetProvider>(provider.GetRequiredService<IAsyncResponseReplyTargetProvider>());
        Assert.Equal("RabbitMQ", provider.GetRequiredService<AsyncResponseTransportMarker>().Name);
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is RabbitMqWorkerSubscriber);
        Assert.Contains(provider.GetServices<IHostedService>(), service => service is RabbitMqResponseIngressSubscriber);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void WorkerTransport_RequiresWorkerExchange(string value)
    {
        var options = Options.Create(new RabbitMqAsyncResponseOptions
        {
            WorkerExchange = value
        });

        Assert.Throws<InvalidOperationException>(() => new RabbitMqWorkerTransport(options, new FakeConnectionFactory()));
    }

    [Fact]
    public async Task WorkerTransport_PublishesSerializedJobAndDeclaresTopology()
    {
        var channel = new FakeRabbitMqChannel();
        var transport = new RabbitMqWorkerTransport(
            Options.Create(new RabbitMqAsyncResponseOptions
            {
                WorkerExchange = "ar.worker",
                WorkerQueue = "ar.worker.q",
                WorkerRoutingKey = "ar.worker.rk",
                CorrelationIdHeader = "cid"
            }),
            new FakeConnectionFactory(channel));

        await transport.PublishAsync(WorkerJob("corr-rabbit", 42));

        var publish = Assert.Single(channel.Published);
        Assert.Equal("ar.worker", publish.Exchange);
        Assert.Equal("ar.worker.rk", publish.RoutingKey);
        Assert.Equal("corr-rabbit", publish.Properties.CorrelationId);
        Assert.Equal("corr-rabbit", AssertHeader(publish.Properties, "cid"));
        Assert.Contains(channel.ExchangeDeclares, item => item.Exchange == "ar.worker");
        Assert.Contains(channel.QueueDeclares, item => item.Queue == "ar.worker.q");
        Assert.Contains(channel.QueueBinds, item => item.Queue == "ar.worker.q" && item.Exchange == "ar.worker" && item.RoutingKey == "ar.worker.rk");

        var job = JsonSerializer.Deserialize<WorkerJobEnvelope>(Encoding.UTF8.GetString(publish.Body.ToArray()));
        Assert.Equal("corr-rabbit", job!.CorrelationId);
        Assert.Equal(nameof(IRecoverySpy.OnWorkerJob), job.Call.MethodName);
    }

    [Fact]
    public void ReplyTargetProvider_UsesResponseExchangeAsDefaultTarget()
    {
        var provider = new RabbitMqReplyTargetProvider(Options.Create(new RabbitMqAsyncResponseOptions
        {
            ResponseExchange = "ar.response",
            ResponseQueue = "ar.response.q",
            ResponseRoutingKey = "ar.response.rk"
        }));

        var target = provider.GetReplyTarget();

        Assert.Equal("default", target.Name);
        Assert.Equal(RabbitMqAsyncResponseOptions.TransportName, target.Transport);
        Assert.Equal("ar.response:ar.response.rk", target.Address);
        Assert.Equal("ar.response", target.Properties["exchange"]);
        Assert.Equal("ar.response.rk", target.Properties["routingKey"]);
        Assert.Equal("ar.response.q", target.Properties["queue"]);
    }

    [Fact]
    public void ReplyTargetProvider_ResolvesNamedTargets()
    {
        var options = new RabbitMqAsyncResponseOptions();
        options.AddReplyTarget("regional", "regional.exchange", "regional.route");
        options.ReplyTargets["regional"].Queue = "regional.queue";
        options.ReplyTargets["regional"].Properties["tenant"] = "acme";

        var target = new RabbitMqReplyTargetProvider(Options.Create(options)).GetReplyTarget("regional");

        Assert.Equal("regional.exchange:regional.route", target.Address);
        Assert.Equal("regional.queue", target.Properties["queue"]);
        Assert.Equal("acme", target.Properties["tenant"]);
    }

    [Fact]
    public void CorrelationExtractor_ReadsPropertyThenHeaderThenJson()
    {
        var options = new RabbitMqAsyncResponseOptions { CorrelationIdHeader = "cid" };

        Assert.Equal("from-property", RabbitMqCorrelationIdExtractor.Extract(
            Delivery("{}", new BasicProperties { CorrelationId = "from-property" }),
            "{}",
            options));

        Assert.Equal("from-header", RabbitMqCorrelationIdExtractor.Extract(
            Delivery("{}", new BasicProperties
            {
                Headers = new Dictionary<string, object?> { ["cid"] = Encoding.UTF8.GetBytes("from-header") }
            }),
            "{}",
            options));

        Assert.Equal("from-json", RabbitMqCorrelationIdExtractor.Extract(
            Delivery("""{"CustomParameters":{"CorrelationId":"from-json"}}"""),
            """{"CustomParameters":{"CorrelationId":"from-json"}}""",
            options));
    }

    [Fact]
    public async Task WorkerSubscriber_ForwardsMessageBodyAndAcks()
    {
        var channel = new FakeRabbitMqChannel();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress
            .Setup(i => i.HandleWorkerMessageAsync("worker-json"))
            .Returns(Task.CompletedTask);
        var subscriber = new RabbitMqWorkerSubscriber(
            Options.Create(new RabbitMqAsyncResponseOptions
            {
                WorkerExchange = "worker.ex",
                WorkerQueue = "worker.q",
                WorkerRoutingKey = "worker.rk"
            }),
            ingress.Object,
            NullLogger<RabbitMqWorkerSubscriber>.Instance,
            new FakeConnectionFactory(channel));

        await using var host = new HostedServiceRun(subscriber);
        await channel.WaitForConsumerAsync();
        await channel.DeliverAsync(Delivery("worker-json", deliveryTag: 7));

        ingress.Verify(i => i.HandleWorkerMessageAsync("worker-json"), Times.Once);
        Assert.Contains(7UL, channel.Acks);
    }

    [Fact]
    public async Task ResponseSubscriber_ExtractsCorrelationAndForwardsMessageBody()
    {
        var channel = new FakeRabbitMqChannel();
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress
            .Setup(i => i.HandleResponseMessageAsync("response-json", "corr-response"))
            .Returns(Task.CompletedTask);
        var subscriber = new RabbitMqResponseIngressSubscriber(
            Options.Create(new RabbitMqAsyncResponseOptions
            {
                ResponseExchange = "response.ex",
                ResponseQueue = "response.q",
                ResponseRoutingKey = "response.rk",
                CorrelationIdHeader = "cid"
            }),
            ingress.Object,
            NullLogger<RabbitMqResponseIngressSubscriber>.Instance,
            new FakeConnectionFactory(channel));

        await using var host = new HostedServiceRun(subscriber);
        await channel.WaitForConsumerAsync();
        await channel.DeliverAsync(Delivery(
            "response-json",
            new BasicProperties { Headers = new Dictionary<string, object?> { ["cid"] = "corr-response" } },
            deliveryTag: 9));

        ingress.Verify(i => i.HandleResponseMessageAsync("response-json", "corr-response"), Times.Once);
        Assert.Contains(9UL, channel.Acks);
    }

    private static WorkerJobEnvelope WorkerJob(string correlationId, int orderId)
        => new()
        {
            CorrelationId = correlationId,
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
                MethodName = nameof(IRecoverySpy.OnWorkerJob),
                Params = [CallbackParam.ForValue(orderId)]
            }
        };

    private static RabbitMqDelivery Delivery(
        string body,
        BasicProperties? properties = null,
        ulong deliveryTag = 1)
        => new(
            "consumer",
            deliveryTag,
            Redelivered: false,
            "exchange",
            "route",
            properties ?? new BasicProperties(),
            Encoding.UTF8.GetBytes(body),
            CancellationToken.None);

    private static string? AssertHeader(BasicProperties properties, string key)
    {
        Assert.NotNull(properties.Headers);
        Assert.True(properties.Headers!.TryGetValue(key, out var value));
        return value switch
        {
            byte[] bytes => Encoding.UTF8.GetString(bytes),
            string s => s,
            _ => value?.ToString()
        };
    }

    private sealed class HostedServiceRun : IAsyncDisposable
    {
        private readonly IHostedService _service;
        private readonly CancellationTokenSource _cts = new();

        public HostedServiceRun(IHostedService service)
        {
            _service = service;
            _service.StartAsync(_cts.Token).GetAwaiter().GetResult();
        }

        public async ValueTask DisposeAsync()
        {
            await _service.StopAsync(CancellationToken.None);
            _cts.Dispose();
        }
    }

    private sealed class FakeConnectionFactory(IRabbitMqChannel? channel = null) : IRabbitMqConnectionFactory
    {
        public FakeRabbitMqConnection Connection { get; } = new(channel ?? new FakeRabbitMqChannel());

        public Task<IRabbitMqConnection> CreateConnectionAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IRabbitMqConnection>(Connection);
    }

    private sealed class FakeRabbitMqConnection(IRabbitMqChannel channel) : IRabbitMqConnection
    {
        public Task<IRabbitMqChannel> CreateChannelAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(channel);

        public Task CloseAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FakeRabbitMqChannel : IRabbitMqChannel
    {
        private readonly TaskCompletionSource _consumerReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private Func<RabbitMqDelivery, Task>? _handler;

        public List<(string Exchange, string Type, bool Durable, bool AutoDelete)> ExchangeDeclares { get; } = [];
        public List<(string Queue, bool Durable, bool Exclusive, bool AutoDelete)> QueueDeclares { get; } = [];
        public List<(string Queue, string Exchange, string RoutingKey)> QueueBinds { get; } = [];
        public List<(string Exchange, string RoutingKey, BasicProperties Properties, ReadOnlyMemory<byte> Body)> Published { get; } = [];
        public List<ulong> Acks { get; } = [];
        public List<(ulong DeliveryTag, bool Requeue)> Nacks { get; } = [];
        public ushort PrefetchCount { get; private set; }

        public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, CancellationToken cancellationToken = default)
        {
            ExchangeDeclares.Add((exchange, type, durable, autoDelete));
            return Task.CompletedTask;
        }

        public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, CancellationToken cancellationToken = default)
        {
            QueueDeclares.Add((queue, durable, exclusive, autoDelete));
            return Task.CompletedTask;
        }

        public Task QueueBindAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken = default)
        {
            QueueBinds.Add((queue, exchange, routingKey));
            return Task.CompletedTask;
        }

        public Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken = default)
        {
            PrefetchCount = prefetchCount;
            return Task.CompletedTask;
        }

        public ValueTask BasicPublishAsync(string exchange, string routingKey, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
        {
            Published.Add((exchange, routingKey, properties, body));
            return ValueTask.CompletedTask;
        }

        public Task<string> BasicConsumeAsync(string queue, Func<RabbitMqDelivery, Task> handler, CancellationToken cancellationToken = default)
        {
            _handler = handler;
            _consumerReady.TrySetResult();
            return Task.FromResult("consumer-tag");
        }

        public Task WaitForConsumerAsync() => _consumerReady.Task.WaitAsync(TimeSpan.FromSeconds(5));

        public Task DeliverAsync(RabbitMqDelivery delivery)
            => (_handler ?? throw new InvalidOperationException("Consumer was not started."))(delivery);

        public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public ValueTask BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken = default)
        {
            Acks.Add(deliveryTag);
            return ValueTask.CompletedTask;
        }

        public ValueTask BasicNackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default)
        {
            Nacks.Add((deliveryTag, requeue));
            return ValueTask.CompletedTask;
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
