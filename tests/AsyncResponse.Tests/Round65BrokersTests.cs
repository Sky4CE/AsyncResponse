using System.Collections.Concurrent;
using AsyncResponse.Testing;
using AsyncResponse.Transports.AzureServiceBus;
using AsyncResponse.Transports.GooglePubSub;
using AsyncResponse.Transports.Kafka;
using AsyncResponse.Transports.RabbitMQ;
using AsyncResponse.Transports.SQS;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using RabbitMQ.Client;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 65 regressions for the broker transports (RabbitMQ, Kafka, Pub/Sub, Service Bus, SQS).
/// Every fake here models what the real client does, not what the package assumes: RabbitMQ.Client
/// keeps dispatching a dead channel's buffered deliveries and ignores a close token while open,
/// librdkafka still delivers a record whose ProduceAsync was cancelled, and the Pub/Sub SDK's stop
/// completes only once every handler it started has returned.
/// </summary>
public class Round65BrokersTests
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(10);

    // ---------------------------------------------------------------- F-03: RabbitMQ dead-channel tail

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RabbitMq_DeliveriesTheClientBufferedBeforeTheChannelDied_AreNeverStartedOrSettled(bool ackAfterEnqueue)
    {
        // Red-on-old: RabbitMQ.Client 7.x keeps dispatching the deliveries it had buffered after
        // a channel shuts down (its consumer dispatcher only refuses NEW ones and cancels each
        // buffered delivery's token), while the broker already requeued them. Both dispatchers ran
        // them — a second execution beside the redelivery on every channel fault under load.
        var first = new ClientLikeChannel();
        var second = new ClientLikeChannel();
        var factory = new ClientLikeConnectionFactory(first, second);
        var handled = new ConcurrentQueue<string>();
        var m1Started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseM1 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sentinelHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress
            .Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>()))
            .Returns<string>(async body =>
            {
                handled.Enqueue(body);
                if (body == "m1")
                {
                    m1Started.TrySetResult();
                    await releaseM1.Task.ConfigureAwait(false);
                }

                if (body == "sentinel")
                    sentinelHandled.TrySetResult();
            });
        var options = RabbitOptions();
        if (ackAfterEnqueue)
            options.WorkerSubscriber.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
        var subscriber = new RabbitMqWorkerSubscriber(Options.Create(options), ingress.Object, NullLogger<RabbitMqWorkerSubscriber>.Instance, factory);

        await subscriber.StartAsync(CancellationToken.None);
        try
        {
            await first.WaitForConsumerAsync();
            var m1 = first.DeliverAsync(first.NewDelivery(1, "m1")); // runs (inline, or ACKed and queued)
            await m1Started.Task.WaitAsync(Wait);

            // The broker closes the channel (connection loss, consumer_timeout, a 406): every
            // un-ACKed delivery is requeued and the subscriber rebuilds on the next channel …
            first.ShutDown("320 CONNECTION_FORCED");
            await second.WaitForConsumerAsync();

            // … while the client still hands over the tail it had buffered on the dead channel.
            await first.DeliverAsync(first.NewDelivery(2, "m2"));
            await first.DeliverAsync(first.NewDelivery(3, "m3"));

            // A sentinel on the live channel: with one background worker (FIFO) it runs only after
            // anything queued before it, so its completion proves m2/m3 were never queued.
            releaseM1.TrySetResult();
            await m1.WaitAsync(Wait);
            await second.DeliverAsync(second.NewDelivery(1, "sentinel"));
            await sentinelHandled.Task.WaitAsync(Wait);

            Assert.Equal(["m1", "sentinel"], handled.ToArray());
            Assert.DoesNotContain(2UL, first.SettledTags);
            Assert.DoesNotContain(3UL, first.SettledTags);
        }
        finally
        {
            releaseM1.TrySetResult();
            await subscriber.StopAsync(CancellationToken.None).WaitAsync(Wait);
        }
    }

    [Fact]
    public async Task RabbitMq_Queued_ADeliveryParkedAfterItsAttemptUnwound_IsHandedBackInsteadOfLandingLater()
    {
        // Red-on-old: the park on a full queue was cancelled by AttachmentEnded(channel), which
        // returned CancellationToken.None for a channel that is no longer attached — so a delivery
        // of an attempt that had already unwound parked for ever, landed in the queue once a slot
        // freed, and ran beside the broker's redelivery. It is now handed back at once.
        var channel = new ClientLikeChannel();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handled = new ConcurrentQueue<ulong>();
        var options = RabbitOptions();
        options.WorkerSubscriber.UseAckAfterEnqueue(1, 1, TimeSpan.FromSeconds(5));
        await using var dispatcher = RabbitMqMessageDispatcher.Create(
            async (delivery, _) =>
            {
                handled.Enqueue(delivery.DeliveryTag);
                await gate.Task.ConfigureAwait(false);
            },
            options,
            options.WorkerSubscriber,
            NullLogger.Instance,
            "worker.q",
            RabbitMqSubscriberRole.Worker);

        var attachment = dispatcher.AttachChannel(channel);
        await dispatcher.HandleAsync(channel.NewDelivery(1, "m1"), channel, CancellationToken.None);
        await KafkaTestData.WaitUntilAsync(() => handled.Count == 1, Wait); // m1 running
        await dispatcher.HandleAsync(channel.NewDelivery(2, "m2"), channel, CancellationToken.None); // fills the slot
        attachment.Dispose(); // the attempt unwound (its channel is not attached any more)

        // The delivery token is deliberately still live: this pins the AttachmentEnded half alone.
        await dispatcher.HandleAsync(channel.NewDelivery(3, "m3"), channel, CancellationToken.None).WaitAsync(Wait);

        gate.TrySetResult();
        await KafkaTestData.WaitUntilAsync(() => handled.Count >= 2, Wait);
        Assert.Equal([1UL, 2UL], channel.Acks);
        Assert.Equal([(3UL, true)], channel.Nacks);
        Assert.DoesNotContain(3UL, handled);
    }

    // ---------------------------------------------------------------- L-13: RabbitMQ bounded closes

    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task RabbitMq_SubscriberStop_BoundsACloseTheClientNeverCompletes_AndAbortsTheConnection(bool channelCloseHangs, bool connectionCloseHangs)
    {
        // Red-on-old: RabbitMQ.Client ignores the close token while the channel/connection is open
        // (channel close waits ContinuationTimeout, 20 s; connection close at least 30 s), so a
        // broker that stopped answering held the stop ~50 s against the 5 s budgeted. Modelled
        // here as a close that never completes: the old stop never returned at all.
        var channel = new ClientLikeChannel { CloseHangs = channelCloseHangs };
        var factory = new ClientLikeConnectionFactory(channel);
        factory.Connection.CloseHangs = connectionCloseHangs;
        var options = RabbitOptions();
        options.ShutdownTimeout = TimeSpan.FromMilliseconds(200);
        var logger = new ListLogger<RabbitMqWorkerSubscriber>();
        var subscriber = new RabbitMqWorkerSubscriber(Options.Create(options), new Mock<IAsyncResponseIngress>().Object, logger, factory);

        await subscriber.StartAsync(CancellationToken.None);
        await channel.WaitForConsumerAsync();
        await subscriber.StopAsync(CancellationToken.None).WaitAsync(Wait);

        Assert.Equal(1, factory.Connection.AbortCalls);
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Warning
            && entry.Message.Contains("did not complete within ShutdownTimeout", StringComparison.Ordinal));
    }

    [Fact]
    public async Task RabbitMq_WorkerTransportDispose_BoundsAChannelCloseTheClientNeverCompletes()
    {
        // Red-on-old: same root cause on the publisher: each close was handed a token the client
        // ignores while open, so a dispose against an unresponsive broker never returned here.
        var channel = new ClientLikeChannel { CloseHangs = true };
        var factory = new ClientLikeConnectionFactory(channel);
        var options = RabbitOptions();
        options.ShutdownTimeout = TimeSpan.FromMilliseconds(200);
        var transport = new RabbitMqWorkerTransport(Options.Create(options), factory);
        await transport.PublishAsync(new WorkerJobEnvelope
        {
            CorrelationId = "c1",
            Call = new ReflectionCallDto { ServiceInterfaceFullName = "Svc", MethodName = "Run", Params = [] }
        });

        await transport.DisposeAsync().AsTask().WaitAsync(Wait);

        Assert.Equal(1, factory.Connection.AbortCalls);
        await KafkaTestData.WaitUntilAsync(() => factory.Connection.DisposeCalls == 1, Wait); // after the abort
    }

    // ---------------------------------------------------------------- F-09: Pub/Sub bounded client stop

    [Fact]
    public async Task PubSub_Stop_AbandonsTheClientStopAtShutdownTimeout_WhenAHandlerIgnoresCancellation()
    {
        // Red-on-old: the SDK's StopAsync completes only once every handler it started has
        // returned (its Timeout only decides when it cancels them), and the ingress takes no token
        // — so one handler outliving the drain held the stop for the whole host budget.
        var clock = new VirtualTimeProvider();
        var client = new SdkLikeSubscriberClient();
        var handlerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(i => i.HandleWorkerMessageAsync(It.IsAny<string>())).Returns(async () =>
        {
            handlerStarted.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan).ConfigureAwait(false); // ignores every token
        });
        var logger = new ListLogger<GooglePubSubWorkerSubscriber>();
        var subscriber = new GooglePubSubWorkerSubscriber(
            Options.Create(new GooglePubSubAsyncResponseOptions
            {
                ProjectId = "project-a",
                WorkerSubscriptionId = "workers",
                ShutdownTimeout = TimeSpan.FromSeconds(5),
                WorkerSubscriber = { BackgroundDrainTimeout = TimeSpan.FromSeconds(2) }
            }),
            ingress.Object,
            logger,
            (_, _, _) => Task.FromResult<IGooglePubSubSubscriberClient>(client))
        {
            Clock = clock
        };

        await subscriber.StartAsync(CancellationToken.None);
        var handler = await client.WaitForHandlerAsync();
        _ = handler(new PubsubMessage { MessageId = "m1", Data = ByteString.CopyFromUtf8("stuck") }, CancellationToken.None);
        await handlerStarted.Task.WaitAsync(Wait);

        var stopping = subscriber.StopAsync(CancellationToken.None);
        await Eventually(() => clock.NextTimerDueAt is not null); // the in-flight drain
        clock.Advance(TimeSpan.FromSeconds(2));
        await Eventually(() => client.StopCalls == 1 && clock.NextTimerDueAt is not null); // the client stop's bound
        clock.Advance(TimeSpan.FromSeconds(5));
        await stopping.WaitAsync(Wait);

        Assert.True(client.HandlerTokenCancelled); // the SDK was still asked to cancel (NackImmediately)
        Assert.Contains(logger.Entries, entry => entry.Level == LogLevel.Warning
            && entry.Message.Contains("did not stop within ShutdownTimeout", StringComparison.Ordinal)
            && entry.Message.Contains("1 handler(s)", StringComparison.Ordinal));
    }

    // ---------------------------------------------------------------- L-14: Kafka unconfirmed burial

    [Fact]
    public async Task Kafka_ADeadLetterProduceStillPendingAtItsBound_IsReportedUnconfirmed_AndItsLateDeliveryIsLogged()
    {
        // Red-on-old: cancelling ProduceAsync only cancels the handle on the delivery report;
        // librdkafka keeps the record and may deliver it later. The burial was reported failed
        // (the inner exception a plain TaskCanceledException), so a restart buried it a second
        // time and nothing ever said the first copy had landed.
        var producer = new LibrdkafkaLikeProducerClient();
        var logger = new ListLogger<Round65BrokersTests>();
        await using var dispatcher = KafkaMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new FakeKafkaConsumerClient(),
            producer,
            KafkaTestData.NewOptions(),
            new KafkaSubscriberOptions
            {
                MaxDeliveryAttempts = 0,
                PollTimeout = TimeSpan.FromMilliseconds(10),
                DetachHandlerAfter = TimeSpan.FromMilliseconds(50),
                MaxPollInterval = TimeSpan.FromSeconds(4) // a 1 s dead-letter bound: wide enough that a loaded runner reaches the produce inside it
            },
            logger,
            "worker-topic",
            "worker-group",
            KafkaSubscriberRole.Worker);

        var failed = await Assert.ThrowsAsync<KafkaDeadLetterPublishFailedException>(() => dispatcher.DiscardUnprocessableAsync(
            KafkaTestData.Message("worker-topic", offset: 4, payload: ""),
            new InvalidDataException("no payload"),
            CancellationToken.None).WaitAsync(Wait));

        var unconfirmed = Assert.IsType<KafkaDeadLetterUnconfirmedException>(failed.InnerException);
        Assert.Contains("may still deliver it", unconfirmed.Message, StringComparison.Ordinal);

        producer.DeliverPending();
        await KafkaTestData.WaitUntilAsync(() => logger.Entries.Any(entry => entry.Level == LogLevel.Warning
            && entry.Message.Contains("worker-topic[0]@4 was delivered after all", StringComparison.Ordinal)), Wait);
        Assert.Single(producer.Delivered);
    }

    [Fact]
    public async Task Kafka_EarlyAck_AnUnconfirmedBurial_IsNotLoggedAsALoss()
    {
        // Red-on-old: the early-ACK path logged "its dead-letter copy could not be written, so the
        // message is lost unless OnBackgroundFailure records it" for a produce librdkafka could
        // still deliver — an operator replaying from OnBackgroundFailure then made a duplicate.
        var producer = new LibrdkafkaLikeProducerClient();
        var logger = new ListLogger<Round65BrokersTests>();
        var subscriber = new KafkaSubscriberOptions
        {
            MaxDeliveryAttempts = 1,
            PollTimeout = TimeSpan.FromMilliseconds(10),
            DetachHandlerAfter = TimeSpan.FromMilliseconds(50),
            MaxPollInterval = TimeSpan.FromSeconds(4) // a 1 s dead-letter bound: wide enough that a loaded runner reaches the produce inside it
        }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5));
        await using var dispatcher = KafkaMessageDispatcher.Create(
            (_, _) => throw new InvalidOperationException("handler boom"),
            new FakeKafkaConsumerClient(),
            producer,
            KafkaTestData.NewOptions(),
            subscriber,
            logger,
            "worker-topic",
            "worker-group",
            KafkaSubscriberRole.Worker);

        await dispatcher.HandleAsync(KafkaTestData.Delivery("worker-topic", 7), CancellationToken.None);

        await KafkaTestData.WaitUntilAsync(() => logger.Entries.Any(entry => entry.Message.Contains("worker-topic[0]@7 after 1 attempt(s)", StringComparison.Ordinal)), Wait);
        var report = Assert.Single(logger.Entries, entry => entry.Message.Contains("worker-topic[0]@7 after 1 attempt(s)", StringComparison.Ordinal));
        Assert.Contains("its dead-letter copy is unconfirmed", report.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("is lost", report.Message, StringComparison.Ordinal);

        producer.DeliverPending();
        await KafkaTestData.WaitUntilAsync(() => logger.Entries.Any(entry => entry.Message.Contains("worker-topic[0]@7 was delivered after all", StringComparison.Ordinal)), Wait);
    }

    // ---------------------------------------------------------------- L-01: throwing logger at early-ACK construction

    [Fact]
    public async Task ServiceBus_EarlyAckDispatcher_ConstructsDespiteAThrowingLogger()
    {
        // Red-on-old: the constructor logged unguarded AFTER starting its workers, so a throwing
        // provider aborted construction (faulting the subscriber at startup) and leaked them.
        await using var dispatcher = AzureServiceBusMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new AzureServiceBusAsyncResponseOptions { WorkerQueue = "workers", ResponseQueue = "responses" },
            new AzureServiceBusSubscriberOptions
            {
                AckMode = AzureServiceBusAckMode.AckAfterEnqueue,
                BackgroundWorkerCount = 1,
                BackgroundQueueCapacity = 4,
                BackgroundDrainTimeout = TimeSpan.FromSeconds(1)
            },
            new ThrowingLogger(),
            "workers",
            AzureServiceBusSubscriberRole.Worker);

        Assert.IsType<QueuedAzureServiceBusMessageDispatcher>(dispatcher);
    }

    [Fact]
    public async Task PubSub_EarlyAckDispatcher_ConstructsDespiteAThrowingLogger()
    {
        // Red-on-old: same unguarded constructor log as Service Bus.
        await using var dispatcher = GooglePubSubMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            new GooglePubSubAsyncResponseOptions { ProjectId = "project-a", WorkerSubscriptionId = "workers" },
            new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(1, 4, TimeSpan.FromSeconds(1)),
            new ThrowingLogger(),
            "workers",
            GooglePubSubSubscriberRole.Worker);

        Assert.IsType<QueuedGooglePubSubMessageDispatcher>(dispatcher);
    }

    // ---------------------------------------------------------------- L-12: SQS half-set credentials

    [Theory]
    [InlineData("AKIA-the-access-key-value", null, "SecretKey")]
    [InlineData(null, "the-secret-key-value", "AccessKey")]
    [InlineData("AKIA-the-access-key-value", "   ", "SecretKey")]
    public void Sqs_AHalfSetStaticCredentialPair_IsRejected_NamingTheOptionsNeverTheValues(string? accessKey, string? secretKey, string missing)
    {
        // Red-on-old: half a pair fell back to the ambient AWS credential chain without a word.
        var options = new SqsAsyncResponseOptions { AccessKey = accessKey, SecretKey = secretKey };

        var ex = Assert.Throws<InvalidOperationException>(() => SqsOptionsValidator.ValidateCommon(options));

        Assert.Contains($"{nameof(SqsAsyncResponseOptions)}.{missing} is not", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("the-access-key-value", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("the-secret-key-value", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData("AKIA-the-access-key-value", "the-secret-key-value")]
    public void Sqs_ABothOrNeitherCredentialPair_StillValidates(string? accessKey, string? secretKey)
        => SqsOptionsValidator.ValidateCommon(new SqsAsyncResponseOptions { AccessKey = accessKey, SecretKey = secretKey });

    // ---------------------------------------------------------------- helpers and client-faithful fakes

    private static RabbitMqAsyncResponseOptions RabbitOptions()
        => new()
        {
            WorkerExchange = "worker.ex",
            WorkerQueue = "worker.q",
            WorkerRoutingKey = "worker.rk",
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(1),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(5)
        };

    private static async Task Eventually(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(Wait);
        while (!condition())
            await Task.Delay(10, cts.Token);
    }

    /// <summary>
    /// A consumer channel that behaves like RabbitMQ.Client 7.x: a shutdown cancels the token every
    /// delivery it handed out (the consumer dispatcher's <c>_shutdownCts</c>), and deliveries the
    /// client had buffered are still handed over afterwards (<see cref="DeliverAsync"/> after
    /// <see cref="ShutDown"/>); settling on a closed channel throws; a close can hang regardless of
    /// its token (<see cref="CloseHangs"/>: the client ignores the token while the channel is open).
    /// </summary>
    private sealed class ClientLikeChannel : IRabbitMqChannel
    {
        private readonly CancellationTokenSource _shutdown = new();
        private readonly TaskCompletionSource _consumerReady = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<string> _terminated = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly object _gate = new();
        private readonly List<ulong> _acks = [];
        private readonly List<(ulong DeliveryTag, bool Requeue)> _nacks = [];
        private Func<RabbitMqDelivery, Task>? _handler;
        private volatile bool _isOpen = true;

        public bool IsOpen => _isOpen;
        public bool CloseHangs { get; init; }

        public ulong[] Acks
        {
            get
            {
                lock (_gate)
                    return [.. _acks];
            }
        }

        public (ulong DeliveryTag, bool Requeue)[] Nacks
        {
            get
            {
                lock (_gate)
                    return [.. _nacks];
            }
        }

        public ulong[] SettledTags => [.. Acks, .. Nacks.Select(nack => nack.DeliveryTag)];

        public RabbitMqDelivery NewDelivery(ulong tag, string body)
            => new("consumer", tag, false, "worker.ex", "worker.rk", new BasicProperties(), System.Text.Encoding.UTF8.GetBytes(body), _shutdown.Token);

        public Task WaitForConsumerAsync() => _consumerReady.Task.WaitAsync(Wait);

        public Task DeliverAsync(RabbitMqDelivery delivery)
            => (_handler ?? throw new InvalidOperationException("Consumer was not started."))(delivery);

        /// <summary>The broker closed the channel: Quiesce (tokens cancelled), then the shutdown event.</summary>
        public void ShutDown(string reason)
        {
            _isOpen = false;
            _shutdown.Cancel();
            _terminated.TrySetResult($"the channel shut down ({reason})");
        }

        public Task<RabbitMqConsumer> BasicConsumeAsync(string queue, Func<RabbitMqDelivery, Task> handler, CancellationToken cancellationToken = default)
        {
            _handler = handler;
            _consumerReady.TrySetResult();
            return Task.FromResult(new RabbitMqConsumer("consumer-tag", _terminated.Task));
        }

        public Task BasicCancelAsync(string consumerTag, CancellationToken cancellationToken = default)
        {
            _terminated.TrySetResult("client-initiated cancel");
            return Task.CompletedTask;
        }

        public ValueTask BasicAckAsync(ulong deliveryTag, CancellationToken cancellationToken = default)
        {
            if (!_isOpen)
                throw new InvalidOperationException("Already closed");

            lock (_gate)
                _acks.Add(deliveryTag);
            return ValueTask.CompletedTask;
        }

        public ValueTask BasicNackAsync(ulong deliveryTag, bool requeue, CancellationToken cancellationToken = default)
        {
            if (!_isOpen)
                throw new InvalidOperationException("Already closed");

            lock (_gate)
                _nacks.Add((deliveryTag, requeue));
            return ValueTask.CompletedTask;
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            if (CloseHangs)
                return new TaskCompletionSource().Task;

            _isOpen = false;
            return Task.CompletedTask;
        }

        public Task ExchangeDeclareAsync(string exchange, string type, bool durable, bool autoDelete, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task QueueDeclareAsync(string queue, bool durable, bool exclusive, bool autoDelete, IDictionary<string, object?>? arguments = null, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task QueueBindAsync(string queue, string exchange, string routingKey, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task BasicQosAsync(ushort prefetchCount, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public ValueTask BasicPublishAsync(string exchange, string routingKey, BasicProperties properties, ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default) => ValueTask.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ClientLikeConnectionFactory(params IRabbitMqChannel[] channels) : IRabbitMqConnectionFactory
    {
        public ClientLikeConnection Connection { get; } = new(channels);

        public Task<IRabbitMqConnection> CreateConnectionAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<IRabbitMqConnection>(Connection);
    }

    private sealed class ClientLikeConnection(IRabbitMqChannel[] channels) : IRabbitMqConnection
    {
        private int _created;
        private int _abortCalls;
        private int _disposeCalls;

        public bool IsOpen => true;
        public bool CloseHangs { get; set; }
        public int AbortCalls => Volatile.Read(ref _abortCalls);
        public int DisposeCalls => Volatile.Read(ref _disposeCalls);

        public Task<IRabbitMqChannel> CreateChannelAsync(bool publisherConfirmations = false, CancellationToken cancellationToken = default)
            => Task.FromResult(channels[Math.Min(Interlocked.Increment(ref _created) - 1, channels.Length - 1)]);

        public Task CloseAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
            => CloseHangs ? new TaskCompletionSource().Task : Task.CompletedTask;

        public Task AbortAsync()
        {
            Interlocked.Increment(ref _abortCalls);
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref _disposeCalls);
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// The Pub/Sub SDK's stop (Google.Cloud.PubSub.V1 3.x <c>SubscriberClientImpl</c>): StopAsync
    /// cancels the handlers' token and returns the main task, which completes only after every
    /// handler the client started has returned.
    /// </summary>
    private sealed class SdkLikeSubscriberClient : IGooglePubSubSubscriberClient
    {
        private readonly TaskCompletionSource _main = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>>> _handler = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly CancellationTokenSource _handlerCancellation = new();
        private readonly List<Task> _running = [];
        private int _stopCalls;

        public int StopCalls => Volatile.Read(ref _stopCalls);
        public bool HandlerTokenCancelled => _handlerCancellation.IsCancellationRequested;

        public Task<Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>>> WaitForHandlerAsync() => _handler.Task.WaitAsync(Wait);

        public Task StartAsync(Func<PubsubMessage, CancellationToken, Task<SubscriberClient.Reply>> handler)
        {
            _handler.TrySetResult((message, _) =>
            {
                var reply = handler(message, _handlerCancellation.Token);
                lock (_running)
                    _running.Add(reply);
                return reply;
            });
            return _main.Task;
        }

        public Task StopAsync(SubscriberClient.ShutdownOptions options, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _stopCalls);
            _handlerCancellation.Cancel();
            Task[] running;
            lock (_running)
                running = [.. _running];
            _ = Task.WhenAll(running).ContinueWith(_ => _main.TrySetResult(), TaskScheduler.Default);
            return _main.Task.ContinueWith(static _ => { }, TaskScheduler.Default);
        }
    }

    /// <summary>
    /// librdkafka through Confluent.Kafka: cancelling ProduceAsync only cancels the returned task
    /// (the delivery-report handle); the record stays queued and is delivered later
    /// (<see cref="DeliverPending"/>).
    /// </summary>
    private sealed class LibrdkafkaLikeProducerClient : IKafkaProducerClient
    {
        private readonly ConcurrentQueue<(string Topic, TaskCompletionSource<KafkaPublishResult> Report)> _pending = new();
        private long _offset;

        public ConcurrentQueue<string> Delivered { get; } = new();

        public Task<KafkaPublishResult> PublishAsync(
            string topic,
            string? key,
            byte[] payload,
            IReadOnlyList<KafkaTransportHeader> headers,
            CancellationToken cancellationToken)
        {
            var report = new TaskCompletionSource<KafkaPublishResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (cancellationToken.CanBeCanceled)
                cancellationToken.Register(() => report.TrySetCanceled(cancellationToken));
            _pending.Enqueue((topic, report));
            return report.Task;
        }

        public void DeliverPending()
        {
            while (_pending.TryDequeue(out var record))
            {
                Delivered.Enqueue(record.Topic);
                record.Report.TrySetResult(new KafkaPublishResult(record.Topic, 0, Interlocked.Increment(ref _offset)));
            }
        }

        public void Dispose()
        {
        }
    }

    private sealed class ListLogger<T> : ILogger<T>
    {
        private readonly ConcurrentQueue<(LogLevel Level, string Message)> _entries = new();

        public IReadOnlyList<(LogLevel Level, string Message)> Entries => [.. _entries];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
            => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => _entries.Enqueue((logLevel, formatter(state, exception)));
    }

    private sealed class ThrowingLogger : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull
            => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => throw new InvalidOperationException("log sink boom");
    }
}
