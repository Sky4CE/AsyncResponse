using Amazon.Runtime;
using Amazon.SQS;
using AsyncResponse.Transports.AzureServiceBus;
using AsyncResponse.Transports.GooglePubSub;
using AsyncResponse.Transports.Kafka;
using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.NATS;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.RabbitMQ;
using AsyncResponse.Transports.Redis;
using AsyncResponse.Transports.SqlServer;
using Azure.Messaging.ServiceBus;
using Confluent.Kafka;
using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Npgsql;
using System.Collections.Concurrent;
using Xunit;

namespace AsyncResponse.Conformance;

/// <summary>
/// The worker-transport behavioral contract: one shared suite of <c>Contract_*</c> facts run against
/// every transport, so the transports stop each having their own ad-hoc test list and start meeting
/// one enforced spec — the same move <see cref="ChannelConformanceSuite"/> made for channels.
/// <para>
/// Before this existed the per-transport suites disagreed on what they checked: some asserted domain
/// failures, some did not; one had late recovery, most did not; and <em>none</em> of them covered
/// dead-lettering, redelivery, shutdown drain, large payloads, or duplicate delivery. Those are the
/// behaviors an operator depends on and they were the least tested part of the library.
/// </para>
/// <para>
/// Transports genuinely differ — SQS and Pub/Sub have no library-side redelivery bound, and four
/// brokers dead-letter natively rather than through a destination the library creates. That is
/// modelled explicitly in <see cref="TransportCapabilities"/>: a fact needing an absent capability
/// skips with the reason named, rather than quietly not existing for that transport.
/// </para>
/// </summary>
public abstract class TransportConformanceSuite
{
    /// <summary>How often the eventually-loops re-check their condition.</summary>
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(50);

    /// <summary>The transport under test. Every fact builds its own host for it.</summary>
    protected abstract MatrixTransport Transport { get; }

    /// <summary>Backends the derived fixture resolved. Only the transport's own is required.</summary>
    protected abstract MatrixBackends Backends { get; }

    /// <summary>
    /// Per-transport latency budget. A polling database queue under a loaded fixture needs far longer
    /// than an in-process one; assertions never require exact timing, the budget only bounds waits.
    /// </summary>
    protected virtual TimeSpan Generous => TimeSpan.FromSeconds(30);

    private TransportCapabilities Capabilities => TransportCapabilities.For(Transport);

    /// <summary>
    /// The transport is paired with the in-memory channel and store throughout: the point here is the
    /// transport's own behavior, and the cross-provider interactions are the combination matrix's job.
    /// </summary>
    private MatrixCell Cell => new(MatrixChannel.InMemory, Transport, MatrixStore.InMemory);

    // ----- a. Worker job delivery -----

    [Fact]
    public async Task Contract_WorkerJob_ExecutesOnceWithRestoredCorrelationId()
    {
        await using var harness = await CreateHarnessAsync();
        var probe = harness.Provider.GetRequiredService<TransportProbe>();
        var correlationId = harness.Names.NewCorrelationId("worker");

        await EnqueueAsync(harness, correlationId, token: 11);

        var observed = await ExpectAsync(probe.FirstCall.Task, harness, "the worker job");
        Assert.Equal(11, observed.Token);
        Assert.Equal(correlationId, observed.CorrelationId);

        // Exactly once in the absence of failure: a transport that redelivers a successfully handled
        // job is the duplicate-execution bug every settlement design exists to prevent.
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.Equal(1, probe.CallCount);
    }

    [Fact]
    public async Task Contract_WorkerJob_RestoresApplicationAmbientContext()
    {
        await using var harness = await CreateHarnessAsync();
        var probe = harness.Provider.GetRequiredService<TransportProbe>();
        var correlationId = harness.Names.NewCorrelationId("ctx");

        TransportTenantContext.Set("tenant-acme");
        try
        {
            await EnqueueAsync(harness, correlationId, token: 5);
        }
        finally
        {
            TransportTenantContext.Set(null);
        }

        var observed = await ExpectAsync(probe.FirstCall.Task, harness, "the worker job");

        // The propagator's value has to cross the wire in the envelope's context bag and be restored
        // before the handler runs — the transport carrying only the correlation id is a silent
        // multi-tenancy bug, not a missing feature.
        Assert.Equal("tenant-acme", observed.Tenant);
    }

    [Fact]
    public async Task Contract_WorkerJob_ResponsePublishedFromTheWorkerReachesAWaiter()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = harness.Names.NewCorrelationId("reply");

        await using var waiter = await harness.Subscriber.CreateResponseWaiter<MatrixResult>(
            correlationId, timeout: Generous * 2);
        await EnqueueAsync(harness, correlationId, token: 7, respond: true);

        var result = await ExpectAsync(waiter.ResponseTask, harness, "the response from the worker");
        Assert.Equal(MatrixStatus.Completed, result.Status);
    }

    // ----- b. Concurrency and payload size -----

    [Fact]
    public async Task Contract_ConcurrentJobs_EachExecuteExactlyOnce()
    {
        await using var harness = await CreateHarnessAsync();
        var probe = harness.Provider.GetRequiredService<TransportProbe>();
        const int count = 25;

        for (var i = 0; i < count; i++)
            await EnqueueAsync(harness, harness.Names.NewCorrelationId($"c{i}"), token: i);

        await EventuallyAsync(() => probe.CallCount >= count, $"all {count} jobs execute", harness);

        // No duplicates and no losses: the tokens seen must be exactly 0..count-1.
        await Task.Delay(TimeSpan.FromSeconds(2));
        var tokens = probe.Calls.Select(call => call.Token).OrderBy(token => token).ToArray();
        Assert.Equal(Enumerable.Range(0, count), tokens);
    }

    [Fact]
    public async Task Contract_LargePayload_RoundTripsIntact()
    {
        await using var harness = await CreateHarnessAsync();
        var probe = harness.Provider.GetRequiredService<TransportProbe>();

        // Sized per transport (see TransportCapabilities.LargePayloadBytes) — the brokers' ceilings
        // differ by two orders of magnitude. Large enough everywhere to catch a transport that
        // truncates, or that base64-inflates a payload past a limit it was told it fits under.
        var payload = new string('x', Capabilities.LargePayloadBytes);
        await EnqueueLargeAsync(harness, harness.Names.NewCorrelationId("large"), payload);

        var observed = await ExpectAsync(probe.FirstLargeCall.Task, harness, "the large-payload job");
        Assert.Equal(payload.Length, observed.Length);
        Assert.Equal(payload, observed);
    }

    // ----- c. Failure handling -----

    [Fact]
    public async Task Contract_TransientHandlerFailure_IsRedeliveredAndThenSucceeds()
    {
        await using var harness = await CreateHarnessAsync(
            new MatrixTransportTuning { MaxDeliveryAttempts = 5, RedeliveryDelay = TimeSpan.FromSeconds(1) });
        var probe = harness.Provider.GetRequiredService<TransportProbe>();
        probe.FailNextCalls(1);

        await EnqueueAsync(harness, harness.Names.NewCorrelationId("transient"), token: 42);

        // The second delivery must succeed: an at-least-once transport whose retry never happens
        // silently drops work whenever a handler hits a transient fault.
        await EventuallyAsync(() => probe.SuccessCount >= 1, "the redelivered job succeeds", harness);

        // At-least-once permits more than two deliveries, so assert the invariants rather than a
        // count: a redelivery happened, and every delivery carried the job that was enqueued.
        await EventuallyAsync(() => probe.CallCount >= 2, "the failed delivery is retried", harness);
        Assert.All(probe.Calls, call => Assert.Equal(42, call.Token));
    }

    [Fact]
    public async Task Contract_PoisonJob_StopsBeingRedeliveredOnceAttemptsAreExhausted()
    {
        if (!Capabilities.BoundedRedelivery)
        {
            Assert.Skip(
                $"{Transport} exposes no delivery bound at all — neither a subscriber cap nor a " +
                "dead-letter policy on the subscription it creates — so a poison job redelivers " +
                "indefinitely by design. See TransportCapabilities.");
        }

        // The bound actually in force, which is not always the one asked for: RabbitMQ can only
        // enforce 2 without an application-owned TTL-retry topology, and a Pub/Sub DeadLetterPolicy
        // refuses anything under 5. Asserting the requested number instead would read the deliveries
        // in between as a runaway poison message.
        var bound = Math.Clamp(
            3,
            Capabilities.MinDeliveryAttempts,
            Capabilities.MaxCountableDeliveryAttempts ?? int.MaxValue);

        await using var harness = await CreateHarnessAsync(
            new MatrixTransportTuning
            {
                MaxDeliveryAttempts = bound, DeadLetterEnabled = true, RedeliveryDelay = TimeSpan.FromSeconds(1)
            });
        var probe = harness.Provider.GetRequiredService<TransportProbe>();
        probe.FailNextCalls(int.MaxValue);

        await EnqueueAsync(harness, harness.Names.NewCorrelationId("poison"), token: 99);

        await EventuallyAsync(() => probe.CallCount >= bound, "the job is retried up to the bound", harness);

        // The bound is what stops a poison message from looping forever and starving the queue. Let
        // it settle well past the last attempt, then assert it stopped rather than kept going.
        var settled = probe.CallCount;
        await Task.Delay(TimeSpan.FromSeconds(5));
        Assert.True(
            probe.CallCount == settled,
            $"the poison job kept being redelivered past its bound: {settled} → {probe.CallCount}.");

        // Stopping is NOT the whole contract: silently dropping or acking the poison job also
        // "stops". The exhausted job must be sitting in the transport's dead-letter destination —
        // the library-created one or the broker's native one, per the capability table.
        if (Capabilities.LibraryDeadLetter || Capabilities.BrokerDeadLetter)
        {
            var deadLettered = 0L;
            var deadline = DateTime.UtcNow + Generous;
            while (DateTime.UtcNow < deadline)
            {
                deadLettered = await CountDeadLetteredJobsAsync(harness);
                if (deadLettered >= 1)
                    break;

                await Task.Delay(PollInterval);
            }

            Assert.True(
                deadLettered >= 1,
                $"{Transport}: the exhausted poison job never appeared in the dead-letter destination — " +
                "it was dropped or acked instead of dead-lettered.");
        }
    }

    // ----- d. Ack modes -----

    [Fact]
    public async Task Contract_EarlyAckMode_StillExecutesTheJob()
    {
        if (!Capabilities.EarlyAck)
        {
            // Rather than skip, pin the absence: the in-process queue exposes no AckMode, and a job
            // enqueued on it still executes. If an early-ACK mode is ever added here, this fails and
            // the capability table has to be updated with it.
            Assert.DoesNotContain(
                typeof(InMemoryWorkerTransportOptions).GetProperties(),
                property => property.Name.Contains("AckMode", StringComparison.Ordinal));

            await using var noAckMode = await CreateHarnessAsync();
            var probeWithoutAckMode = noAckMode.Provider.GetRequiredService<TransportProbe>();
            await EnqueueAsync(noAckMode, noAckMode.Names.NewCorrelationId("noackmode"), token: 13);
            Assert.Equal(13, (await ExpectAsync(probeWithoutAckMode.FirstCall.Task, noAckMode, "the job")).Token);
            return;
        }

        await using var harness = await CreateHarnessAsync(new MatrixTransportTuning { EarlyAck = true });
        var probe = harness.Provider.GetRequiredService<TransportProbe>();
        var correlationId = harness.Names.NewCorrelationId("earlyack");

        await EnqueueAsync(harness, correlationId, token: 13);

        // Early ACK settles the message before the handler runs; the handler must still run, with the
        // context restored, on the background worker the mode hands off to.
        var observed = await ExpectAsync(probe.FirstCall.Task, harness, "the early-ACK job");
        Assert.Equal(13, observed.Token);
        Assert.Equal(correlationId, observed.CorrelationId);
    }

    // ----- e. Durability across a consumer outage -----

    [Fact]
    public async Task Contract_JobPublishedWhileNoConsumerIsRunning_IsDeliveredWhenOneReturns()
    {
        if (!Capabilities.DurableWhileConsumerIsDown)
        {
            // Pin the absence rather than skip. The in-process queue dies with its host, so a second
            // host sees nothing the first one enqueued — that is the property an application has to
            // know about before choosing this transport, and it deserves an assertion.
            var processLocal = new MatrixNames(Cell, Guid.NewGuid().ToString("N"));
            await using (var publisher = await CreateHarnessAsync(
                sharedNames: processLocal, startSubscribers: false, teardownNamespaces: false))
            {
                await EnqueueAsync(publisher, processLocal.NewCorrelationId("lost"), token: 77);
            }

            await using var successor = await CreateHarnessAsync(sharedNames: processLocal);
            var successorProbe = successor.Provider.GetRequiredService<TransportProbe>();
            await Task.Delay(TimeSpan.FromSeconds(2));
            Assert.Equal(0, successorProbe.CallCount);
            return;
        }

        // Two hosts over one queue: the first publishes with its own subscribers stopped, the second
        // starts fresh and must pick the work up. This is the consumer-restart path — a deploy, a
        // crash, a scale-to-zero — without needing to restart the broker container itself.
        var names = new MatrixNames(Cell);
        await using (var publisher = await CreateHarnessAsync(
            sharedNames: names, startSubscribers: false, teardownNamespaces: false))
        {
            // teardownNamespaces: false is the whole point — the publisher must not drop the schema,
            // stream, or keys holding the message the consumer below is supposed to find.
            await EnqueueAsync(publisher, names.NewCorrelationId("offline"), token: 77);
        }

        await using var consumer = await CreateHarnessAsync(sharedNames: names);
        var probe = consumer.Provider.GetRequiredService<TransportProbe>();

        var observed = await ExpectAsync(probe.FirstCall.Task, consumer, "the job published while offline");
        Assert.Equal(77, observed.Token);
    }

    // ----- f. Shutdown -----

    [Fact]
    public async Task Contract_ShutdownWhileIdle_CompletesWellInsideTheBudget()
    {
        // No `await using`: this fact times the disposal itself, so it disposes exactly once.
        var harness = await CreateHarnessAsync(
            new MatrixTransportTuning { HostShutdownTimeout = TimeSpan.FromSeconds(10) });

        // An idle transport must not sit out its whole drain budget on shutdown — that budget is the
        // ceiling for a host that is stuck, not the cost of every deploy.
        var started = DateTime.UtcNow;
        await harness.DisposeAsync();
        var elapsed = DateTime.UtcNow - started;

        Assert.True(
            elapsed < TimeSpan.FromSeconds(10),
            $"an idle {Transport} host took {elapsed.TotalSeconds:F1}s to stop, i.e. its whole budget.");
    }

    [Fact]
    public async Task Contract_ShutdownWithAnInFlightJob_DrainsItInsteadOfDroppingIt()
    {
        // Idle shutdown latency says nothing about accepted work: a transport that drops an
        // in-flight (or early-ACKed, where supported — the acutest at-most-once risk) job during
        // shutdown passes the idle fact. Hold a job mid-handler, start disposal while it is held,
        // then release it and require that shutdown DRAINED it to completion.
        await using var harness = await CreateHarnessAsync(new MatrixTransportTuning
        {
            HostShutdownTimeout = TimeSpan.FromSeconds(30),
            EarlyAck = Capabilities.EarlyAck
        });
        var probe = harness.Provider.GetRequiredService<TransportProbe>();
        probe.Gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await EnqueueAsync(harness, harness.Names.NewCorrelationId("drain"), token: 21);
        await ExpectAsync(probe.GateReached.Task, harness, "the in-flight job to reach its hold");

        var disposal = harness.DisposeAsync().AsTask();
        await Task.Delay(500);
        Assert.False(disposal.IsCompleted, "shutdown finished while a delivered job was still executing");

        probe.Gate.TrySetResult();
        await disposal.WaitAsync(TimeSpan.FromSeconds(60));

        Assert.Equal(1, probe.GatedCompletions);
    }

    /// <summary>
    /// Counts poison jobs in this transport's dead-letter destination, reading the effective
    /// entity names from the harness's own resolved options so the count inspects exactly what
    /// the transport wrote. Library dead-letter destinations are rows/streams/topics the library
    /// created; broker ones are the native DLQ sub-queue/redrive target/dead-letter topic.
    /// </summary>
    private async Task<long> CountDeadLetteredJobsAsync(MatrixHarness harness)
    {
        switch (Transport)
        {
            case MatrixTransport.PostgreSql:
            {
                var options = harness.Provider.GetRequiredService<IOptions<PostgreSqlAsyncResponseTransportOptions>>().Value;
                await using var dataSource = NpgsqlDataSource.Create(Backends.Require(Backends.PostgreSql, "postgresql"));
                await using var command = dataSource.CreateCommand(
                    $"SELECT count(*) FROM \"{options.SchemaName}\".\"{options.MessageTable}\" WHERE queue = @queue;");
                command.Parameters.AddWithValue("queue", options.DeadLetterQueue);
                return (long)(await command.ExecuteScalarAsync() ?? 0L);
            }

            case MatrixTransport.SqlServer:
            {
                var options = harness.Provider.GetRequiredService<IOptions<SqlServerAsyncResponseTransportOptions>>().Value;
                await using var connection = new SqlConnection(Backends.Require(Backends.SqlServer, "sqlserver"));
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = $"SELECT COUNT(*) FROM [{options.SchemaName}].[{options.MessageTable}] WHERE queue = @queue;";
                command.Parameters.AddWithValue("@queue", options.DeadLetterQueue);
                return Convert.ToInt64(await command.ExecuteScalarAsync());
            }

            case MatrixTransport.MongoDb:
            {
                var options = harness.Provider.GetRequiredService<IOptions<MongoDbAsyncResponseTransportOptions>>().Value;
                var database = new MongoClient(Backends.Require(Backends.MongoDb, "mongodb")).GetDatabase(options.DatabaseName);
                return await database.GetCollection<BsonDocument>(options.MessageCollection)
                    .CountDocumentsAsync(Builders<BsonDocument>.Filter.Eq("queue", options.DeadLetterQueue));
            }

            case MatrixTransport.Redis:
            {
                var options = harness.Provider.GetRequiredService<IOptions<RedisAsyncResponseTransportOptions>>().Value;
                await using var multiplexer = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(
                    Backends.Require(Backends.Redis, "redis"));
                return await multiplexer.GetDatabase().StreamLengthAsync($"{options.KeyPrefix}:transport:deadletter");
            }

            case MatrixTransport.Nats:
            {
                var options = harness.Provider.GetRequiredService<IOptions<NatsAsyncResponseTransportOptions>>().Value;
                var schema = new NatsTransportSubjectSchema(options);
                await using var connection = new NatsConnection(new NatsOpts { Url = Backends.Require(Backends.Nats, "nats") });
                var stream = await new NatsJSContext(connection).GetStreamAsync(schema.DeadLetterStream);
                return (long)stream.Info.State.Messages;
            }

            case MatrixTransport.Kafka:
            {
                var options = harness.Provider.GetRequiredService<IOptions<KafkaAsyncResponseTransportOptions>>().Value;
                var topic = new KafkaTransportTopicSchema(options).WorkerTopic + options.DeadLetterTopicSuffix;
                using var consumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
                {
                    BootstrapServers = Backends.Require(Backends.Kafka, "kafka"),
                    GroupId = $"dlq-count-{Guid.NewGuid():N}",
                    AutoOffsetReset = AutoOffsetReset.Earliest,
                    EnablePartitionEof = true
                }).Build();
                consumer.Subscribe(topic);
                var count = 0L;
                var idleUntil = DateTime.UtcNow + TimeSpan.FromSeconds(5);
                while (DateTime.UtcNow < idleUntil)
                {
                    var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                    if (result is null)
                        continue;
                    if (result.IsPartitionEOF)
                        break;
                    count++;
                }

                consumer.Close();
                return count;
            }

            case MatrixTransport.RabbitMq:
            {
                var options = harness.Provider.GetRequiredService<IOptions<RabbitMqAsyncResponseOptions>>().Value;
                var factory = new global::RabbitMQ.Client.ConnectionFactory { Uri = new Uri(Backends.Require(Backends.RabbitMq, "rabbitmq")) };
                await using var connection = await factory.CreateConnectionAsync();
                await using var channel = await connection.CreateChannelAsync();
                var declared = await channel.QueueDeclarePassiveAsync(options.DeadLetterQueue!);
                return declared.MessageCount;
            }

            case MatrixTransport.Sqs:
            {
                var options = harness.Provider.GetRequiredService<IOptions<AsyncResponse.Transports.SQS.SqsAsyncResponseOptions>>().Value;
                using var client = new AmazonSQSClient(
                    new BasicAWSCredentials("test", "test"),
                    new AmazonSQSConfig { ServiceURL = Backends.Require(Backends.LocalStack, "localstack"), AuthenticationRegion = "us-east-1" });
                var queueUrl = (await client.GetQueueUrlAsync($"{options.WorkerQueue}{options.DeadLetterQueueSuffix}")).QueueUrl;
                var attributes = await client.GetQueueAttributesAsync(queueUrl, ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"]);
                return attributes.ApproximateNumberOfMessages + attributes.ApproximateNumberOfMessagesNotVisible;
            }

            case MatrixTransport.AzureServiceBus:
            {
                var options = harness.Provider.GetRequiredService<IOptions<AzureServiceBusAsyncResponseOptions>>().Value;
                await using var client = new ServiceBusClient(Backends.Require(Backends.AzureServiceBus, "servicebus"));
                await using var receiver = client.CreateReceiver(
                    options.WorkerQueue,
                    new ServiceBusReceiverOptions { SubQueue = SubQueue.DeadLetter, ReceiveMode = ServiceBusReceiveMode.PeekLock });
                var messages = await receiver.PeekMessagesAsync(50);
                return messages.Count;
            }

            case MatrixTransport.GooglePubSub:
            {
                var options = harness.Provider.GetRequiredService<IOptions<GooglePubSubAsyncResponseOptions>>().Value;
                var subscriber = await new SubscriberServiceApiClientBuilder { EmulatorDetection = EmulatorDetection.EmulatorOnly }.BuildAsync();
                var subscription = SubscriptionName.FromProjectSubscription(
                    options.ProjectId!, $"{harness.Names.PubSubBase}-dead-sub");
                var response = await subscriber.PullAsync(subscription, maxMessages: 25);
                return response.ReceivedMessages.Count;
            }

            default:
                return 0;
        }
    }

    // ----- Harness -----

    private async Task<MatrixHarness> CreateHarnessAsync(
        MatrixTransportTuning? tuning = null,
        MatrixNames? sharedNames = null,
        bool startSubscribers = true,
        bool teardownNamespaces = true)
        => await MatrixHarness.CreateAsync(
            Cell,
            Backends,
            services =>
            {
                services.AddSingleton<TransportProbe>();
                services.AddSingleton<ITransportWorkerService, TransportWorkerService>();
            },
            // Every fact in a suite runs the same cell, so without a per-host discriminator they would
            // all address one queue — and a fact that deliberately parks a poison message would then
            // deliver it to whichever fact ran next.
            sharedNames: sharedNames ?? new MatrixNames(Cell, Guid.NewGuid().ToString("N")),
            // The harness always registers a durable-flow store, and startup refuses early ACK
            // alongside one unless the trade-off is accepted explicitly — flow wake-ups ride the
            // worker queue, so an early ACK can strand a run. The early-ACK fact is about the ack
            // mode, not that guard (which AsyncResponseStartupValidator's own tests cover), so it
            // accepts it here.
            configureDurableFlows: options => options.AllowEarlyAckWorkerSubscriber = tuning?.EarlyAck ?? false,
            tuning: tuning,
            startSubscribers: startSubscribers,
            teardownNamespaces: teardownNamespaces,
            configureRegistration: builder => builder.WithContextPropagator<TransportTenantPropagator>());

    private static async Task EnqueueAsync(
        MatrixHarness harness, string correlationId, int token, bool respond = false)
    {
        AsyncResponseContext.SetCorrelationId(correlationId);
        try
        {
            await harness.Provider.GetRequiredService<IAsyncResponseBuilder>()
                .EnqueueWorkerAsync<ITransportWorkerService>(service => service.HandleAsync(token, respond));
        }
        finally
        {
            AsyncResponseContext.ClearCorrelationId();
        }
    }

    private static async Task EnqueueLargeAsync(MatrixHarness harness, string correlationId, string payload)
    {
        AsyncResponseContext.SetCorrelationId(correlationId);
        try
        {
            await harness.Provider.GetRequiredService<IAsyncResponseBuilder>()
                .EnqueueWorkerAsync<ITransportWorkerService>(service => service.HandleLargeAsync(payload));
        }
        finally
        {
            AsyncResponseContext.ClearCorrelationId();
        }
    }

    private async Task EventuallyAsync(Func<bool> condition, string description, MatrixHarness? harness = null)
    {
        var deadline = DateTime.UtcNow + Generous * 2;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;

            await Task.Delay(PollInterval);
        }

        Assert.Fail(
            $"Timed out after {Generous * 2} waiting until {description} ({Transport}). " +
            $"Subscriber diagnostics: {harness?.Diagnostics.Summary() ?? "not captured"}.");
    }

    /// <summary>
    /// Awaits a delivery, reporting what the transport logged when it never arrives. Every transport
    /// subscriber catches its own failures and retries with backoff, so "nothing arrived" and "the
    /// subscriber could not start" look identical without this.
    /// </summary>
    private async Task<T> ExpectAsync<T>(Task<T> delivery, MatrixHarness harness, string what)
    {
        try
        {
            return await delivery.WaitAsync(Generous);
        }
        catch (TimeoutException)
        {
            Assert.Fail(
                $"{Transport}: {what} did not arrive within {Generous}. " +
                $"Subscriber diagnostics: {harness.Diagnostics.Summary()}.");
            throw;
        }
    }
}

/// <summary>What one worker invocation observed. Compared against what the publisher set.</summary>
public sealed record TransportCall(int Token, string? CorrelationId, string? Tenant);

/// <summary>Records worker invocations and can be told to fail the next N of them.</summary>
public sealed class TransportProbe
{
    private readonly ConcurrentQueue<TransportCall> _calls = new();
    private int _failuresRemaining;
    private int _successCount;

    public TaskCompletionSource<TransportCall> FirstCall { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource<string> FirstLargeCall { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public IReadOnlyList<TransportCall> Calls => [.. _calls];

    public int CallCount => _calls.Count;

    public int SuccessCount => Volatile.Read(ref _successCount);

    /// <summary>
    /// When set, every invocation signals <see cref="GateReached"/> and then holds until the gate
    /// completes — the shutdown-drain contract's way of keeping a delivered job in flight while
    /// disposal runs. <see cref="GatedCompletions"/> counts holds that ran to completion.
    /// </summary>
    public TaskCompletionSource? Gate { get; set; }

    public TaskCompletionSource<bool> GateReached { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private int _gatedCompletions;

    public int GatedCompletions => Volatile.Read(ref _gatedCompletions);

    internal async Task HoldAtGateAsync()
    {
        if (Gate is not { } gate)
            return;

        GateReached.TrySetResult(true);
        await gate.Task;
        Interlocked.Increment(ref _gatedCompletions);
    }

    /// <summary>Makes the next <paramref name="count"/> invocations throw, to drive redelivery.</summary>
    public void FailNextCalls(int count) => Volatile.Write(ref _failuresRemaining, count);

    internal void Record(TransportCall call)
    {
        _calls.Enqueue(call);
        FirstCall.TrySetResult(call);

        // Decrement-and-test rather than a lock: concurrent subscribers may both be in here, and the
        // only thing that matters is that exactly `count` invocations throw.
        if (Interlocked.Decrement(ref _failuresRemaining) >= 0)
            throw new InvalidOperationException("transport conformance: deliberate handler failure");

        Interlocked.Increment(ref _successCount);
    }

    internal void RecordLarge(string payload) => FirstLargeCall.TrySetResult(payload);
}

/// <summary>The service whose methods the suite offloads onto the transport.</summary>
public interface ITransportWorkerService
{
    /// <summary>Records the invocation, optionally answering over the response channel.</summary>
    Task HandleAsync(int token, bool respond);

    /// <summary>Records a large argument, for the payload-size contract.</summary>
    Task HandleLargeAsync(string payload);
}

/// <inheritdoc />
public sealed class TransportWorkerService(TransportProbe probe, IAsyncResponsePublisher publisher)
    : ITransportWorkerService
{
    /// <inheritdoc />
    public async Task HandleAsync(int token, bool respond)
    {
        var correlationId = AsyncResponseContext.CorrelationId;

        // Record before responding: the response is what unblocks the waiter, so publishing first
        // would let a fact observe completion before the call was visible to the probe.
        probe.Record(new TransportCall(token, correlationId, TransportTenantContext.Current));
        await probe.HoldAtGateAsync();

        // A missing correlation id is itself a contract failure, recorded above; throwing here would
        // instead surface as a redelivery. Only the respond path needs a non-null id.
        if (respond && correlationId is not null)
            await publisher.SetResponse(new MatrixResult { Status = MatrixStatus.Completed }, correlationId);
    }

    /// <inheritdoc />
    public Task HandleLargeAsync(string payload)
    {
        probe.RecordLarge(payload);
        return Task.CompletedTask;
    }
}

/// <summary>Ambient application state the propagator carries across the transport hop.</summary>
public static class TransportTenantContext
{
    private static readonly AsyncLocal<string?> Value = new();

    public static string? Current => Value.Value;

    public static void Set(string? tenant) => Value.Value = tenant;
}

/// <summary>
/// Carries <see cref="TransportTenantContext"/> into the worker envelope and back out on the far
/// side — the application-context contract every transport has to honour.
/// </summary>
public sealed class TransportTenantPropagator : IAsyncResponseContextPropagator
{
    private const string Key = "conformance-tenant";

    /// <inheritdoc />
    public void Capture(IDictionary<string, string> carrier)
    {
        if (TransportTenantContext.Current is { Length: > 0 } tenant)
            carrier[Key] = tenant;
    }

    /// <inheritdoc />
    public IDisposable Restore(IReadOnlyDictionary<string, string> carrier)
    {
        var previous = TransportTenantContext.Current;
        TransportTenantContext.Set(carrier.TryGetValue(Key, out var tenant) ? tenant : null);
        return new Restoration(previous);
    }

    /// <summary>Puts the ambient value back when the worker's scope ends, as the contract requires.</summary>
    private sealed class Restoration(string? previous) : IDisposable
    {
        public void Dispose() => TransportTenantContext.Set(previous);
    }
}
