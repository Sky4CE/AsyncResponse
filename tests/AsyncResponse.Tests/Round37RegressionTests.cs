using AsyncResponse.Channels.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regressions for round 37 (external holistic review of ba63beb): behavior pins that compile
/// against the pre-fix tree and fail there. Pins over API this round introduced
/// (<c>WorkerJobTooLargeException</c>, the estimator, the overload constructor and counter,
/// <c>KafkaSubscriberOptions.DetachHandlerAfter</c>) live in <see cref="Round37NewApiTests"/>;
/// the Kafka poll-loop pins live in <c>KafkaSubscriberTests</c> / <c>KafkaDispatcherTests</c>
/// and the Cosmos lease-parameter pin in <c>CosmosDurableFlowStateStoreTests</c>.
/// </summary>
public sealed class Round37RegressionTests
{
    // ---------------------------------------------------------------------------------------------
    // F2 — a worker envelope the consuming ingress would refuse was published anyway. The ingress
    //      acknowledges a message over MaxInboundMessageChars WITHOUT executing it (an oversized
    //      message never gets smaller, so redelivery would hot-loop), so the producer's publish
    //      "succeeded", the caller kept a flow id for a Running ledger with Attempts = 0, and the
    //      work silently never ran. The budget is now enforced before the publish, in the
    //      caller's stack, measured on the serialized envelope the way the ingress measures it.

    public interface IR37Probe
    {
        Task RunAsync(string payload);
    }

    public sealed class R37Probe : IR37Probe
    {
        public int Calls;

        public Task RunAsync(string payload)
        {
            Interlocked.Increment(ref Calls);
            return Task.CompletedTask;
        }
    }

    public sealed record R37Input(string Payload);

    public sealed class R37Flow : IDurableFlow<R37Input>
    {
        public Task ExecuteAsync(IDurableFlowContext flow, R37Input input) => Task.CompletedTask;
    }

    /// <summary>A transport that accepts everything, as every database transport and most brokers do.</summary>
    internal sealed class CapturingWorkerTransport : IWorkerTransport
    {
        private int _attempts;

        public List<WorkerJobEnvelope> Published { get; } = [];
        public int Attempts => Volatile.Read(ref _attempts);

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _attempts);
            lock (Published)
            {
                Published.Add(job);
            }

            return Task.CompletedTask;
        }
    }

    private static ServiceProvider BuildProducer(CapturingWorkerTransport transport, int limit, R37Probe? probe = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IWorkerTransport>(transport);
        if (probe is not null)
            services.AddSingleton<IR37Probe>(probe);
        services.AddAsyncResponse(o => o.MaxInboundMessageChars = limit).WithInMemoryChannel().WithInMemoryDurableFlows();
        return services.BuildServiceProvider();
    }

    [Fact]
    public async Task EnqueueWorkerAsync_EnvelopeOverTheIngressBudget_ThrowsBeforePublishing()
    {
        var transport = new CapturingWorkerTransport();
        await using var provider = BuildProducer(transport, limit: 4096);
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();

        await Assert.ThrowsAnyAsync<InvalidOperationException>(() =>
            builder.EnqueueWorkerAsync<IR37Probe>(probe => probe.RunAsync(new string('x', 8192))));

        Assert.Empty(transport.Published);
    }

    [Fact]
    public async Task EnqueueWorkerAsync_JsonEscapingCountsTowardTheBudget()
    {
        // 3000 quote characters are under the 4096 budget as a string and over it once serialized
        // (each becomes \" on the wire) — the ingress measures the wire form.
        var transport = new CapturingWorkerTransport();
        await using var provider = BuildProducer(transport, limit: 4096);
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();

        await Assert.ThrowsAnyAsync<InvalidOperationException>(() =>
            builder.EnqueueWorkerAsync<IR37Probe>(probe => probe.RunAsync(new string('"', 3000))));

        Assert.Empty(transport.Published);
    }

    [Fact]
    public async Task EnqueueWorkerAsync_EnvelopeWithinTheBudget_PublishesAndTheIngressExecutesTheSameJson()
    {
        // The producer/ingress boundary end to end: what the producer let through, the ingress
        // runs — the same serialization on both sides, measured the same way.
        var transport = new CapturingWorkerTransport();
        var probe = new R37Probe();
        await using var provider = BuildProducer(transport, limit: 4096, probe);
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();

        await builder.EnqueueWorkerAsync<IR37Probe>(p => p.RunAsync(new string('x', 2048)));

        var envelope = Assert.Single(transport.Published);
        var json = AsyncResponseJson.Serialize(envelope);
        Assert.True(json.Length <= 4096, $"Published envelope is {json.Length} characters.");

        await provider.GetRequiredService<IAsyncResponseIngress>().HandleWorkerMessageAsync(json);
        Assert.Equal(1, probe.Calls);
    }

    [Fact]
    public async Task DurableFlowStart_InputOverTheIngressBudget_ThrowsWithNoAttemptAndNothingPersisted()
    {
        // The start job carries the initial ledger, input included. Before: the transport took
        // it, the ingress dropped it, and the caller held an id for a run nothing would execute.
        // Now: no publish attempt at all (the failure is deterministic, so the retry ladder is
        // not entered) and no ledger.
        var transport = new CapturingWorkerTransport();
        await using var provider = BuildProducer(transport, limit: 4096);
        var flows = provider.GetRequiredService<IDurableFlows>();

        await Assert.ThrowsAnyAsync<InvalidOperationException>(() =>
            flows.StartAsync<R37Flow, R37Input>(new R37Input(new string('x', 8192)), "r37-oversized"));

        Assert.Equal(0, transport.Attempts);
        Assert.Null(await flows.GetStateAsync("r37-oversized"));
    }

    // ---------------------------------------------------------------------------------------------
    // F4 — the Redis channel buffered a progress flood without bound. Its subscription handler
    //      awaited admission to the bounded per-correlation-id executor, which does not slow the
    //      publisher (Redis pub/sub is fire-and-forget): it only parked the SDK's message loop,
    //      and the SDK's ChannelMessageQueue behind it is unbounded — 20,000 messages sat there
    //      against an executor of 1,024. Admission is non-blocking now, and a message that finds
    //      the buffer full faults the wait as indeterminate and tears the subscription down.

    private sealed class RedisHarness
    {
        public Mock<IConnectionMultiplexer> Multiplexer { get; } = new();
        public Mock<ISubscriber> RedisSubscriber { get; } = new();
        public Mock<IRecoveryStateStore> Store { get; } = new();
        public FakeRedisChannelSubscriber Subscriber { get; } = new();
        public ServiceProvider Services { get; } = new ServiceCollection().BuildServiceProvider();

        public RedisHarness()
        {
            Multiplexer.Setup(m => m.GetSubscriber(It.IsAny<object?>())).Returns(RedisSubscriber.Object);
            Store
                .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            Store
                .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);
        }

        public RedisAsyncResponseChannel CreateChannel() => new(
            Services.GetRequiredService<IServiceScopeFactory>(),
            Multiplexer.Object,
            Store.Object,
            Options.Create(new RedisAsyncResponseOptions
            {
                DefaultTimeout = TimeSpan.FromSeconds(30),
                RecoveryStateExpiry = TimeSpan.FromMinutes(5)
            }),
            new AsyncResponseContextPropagation([]),
            NullLogger<RedisAsyncResponseChannel>.Instance,
            Subscriber);
    }

    /// <summary>The channel's async subscribe seam: captures the handler so the test can push messages through it.</summary>
    private sealed class FakeRedisChannelSubscriber : IRedisChannelSubscriber
    {
        private int _unsubscribeCount;

        public RedisChannel SubscribedChannel { get; private set; }
        public Func<RedisChannel, RedisValue, Task>? Handler { get; private set; }
        public int UnsubscribeCount => Volatile.Read(ref _unsubscribeCount);

        public Task<IRedisChannelSubscription> SubscribeAsync(RedisChannel channel, Func<RedisChannel, RedisValue, Task> onMessage)
        {
            SubscribedChannel = channel;
            Handler = onMessage;
            return Task.FromResult<IRedisChannelSubscription>(new Subscription(this));
        }

        private sealed class Subscription(FakeRedisChannelSubscriber owner) : IRedisChannelSubscription
        {
            public ValueTask DisposeAsync()
            {
                Interlocked.Increment(ref owner._unsubscribeCount);
                return ValueTask.CompletedTask;
            }
        }
    }

    private const string ProgressEnvelope =
        """{"SchemaVersion":1,"Success":true,"Payload":{"Status":1,"Message":"progress"},"ExceptionMessage":null,"ExceptionStackTrace":null}""";

    [Fact]
    public async Task RedisChannel_ProgressFloodBehindASlowPredicate_FaultsTheWaitAsIndeterminate_InsteadOfBufferingWithoutBound()
    {
        var harness = new RedisHarness();
        var channel = harness.CreateChannel();
        var predicateEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePredicate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-flood",
            completionPredicate: async _ =>
            {
                predicateEntered.TrySetResult();
                await releasePredicate.Task;
                return false;
            },
            timeout: TimeSpan.FromSeconds(30));
        try
        {
            var handler = harness.Subscriber.Handler!;
            var subscribedChannel = harness.Subscriber.SubscribedChannel;

            // The first message parks the executor's reader inside the predicate.
            await handler(subscribedChannel, ProgressEnvelope).WaitAsync(TimeSpan.FromSeconds(5));
            await predicateEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // The flood. Every delivery must return promptly — admitted or refused — because a
            // handler parked on admission is exactly what let the SDK queue grow without bound.
            // The old handler parks on the delivery that finds the executor full.
            var delivered = 0;
            while (!waiter.ResponseTask.IsCompleted && delivered < 1100)
            {
                await handler(subscribedChannel, ProgressEnvelope).WaitAsync(TimeSpan.FromSeconds(2));
                delivered++;
            }

            Assert.True(waiter.ResponseTask.IsFaulted, $"The wait was not faulted after {delivered} deliveries.");
            var overload = await Assert.ThrowsAsync<AsyncResponseIndeterminateDeliveryException>(() => waiter.ResponseTask);
            Assert.Equal("corr-flood", overload.CorrelationId);

            // Torn down, so the flood stops here: unsubscribed and the recovery registration gone.
            await WaitUntilAsync(() => harness.Subscriber.UnsubscribeCount == 1);
            harness.Store.Verify(s => s.TryDeleteAsync("corr-flood", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.AtLeastOnce);
        }
        finally
        {
            releasePredicate.TrySetResult();
        }
    }

    [Fact]
    public async Task RedisChannel_TerminalResponseAheadOfTheBuffer_StillCompletesTheWait()
    {
        // The bound never costs a response that was admitted: a terminal message admitted while
        // the predicate is slow completes the wait once the executor reaches it.
        var harness = new RedisHarness();
        var channel = harness.CreateChannel();
        var releasePredicate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var predicateCalls = 0;

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-admitted",
            completionPredicate: async payload =>
            {
                if (Interlocked.Increment(ref predicateCalls) == 1)
                    await releasePredicate.Task;
                return payload.Status == OperationStatus.Completed;
            },
            timeout: TimeSpan.FromSeconds(30));

        var handler = harness.Subscriber.Handler!;
        await handler(harness.Subscriber.SubscribedChannel, ProgressEnvelope);
        await WaitUntilAsync(() => Volatile.Read(ref predicateCalls) == 1);
        await handler(
            harness.Subscriber.SubscribedChannel,
            """{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"done"},"ExceptionMessage":null,"ExceptionStackTrace":null}""");

        releasePredicate.SetResult();
        Assert.Equal(OperationStatus.Completed, (await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5))).Status);
    }

    private static async Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null)
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
