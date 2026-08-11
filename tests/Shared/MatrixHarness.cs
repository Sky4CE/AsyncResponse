using Amazon.DynamoDBv2;
using Amazon.Runtime;
using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Channels.NATS;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.Redis;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.DurableFlows.Cosmos;
using AsyncResponse.DurableFlows.DynamoDB;
using AsyncResponse.DurableFlows.EFCore;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.DurableFlows.MySql;
using AsyncResponse.DurableFlows.Oracle;
using AsyncResponse.DurableFlows.PostgreSQL;
using AsyncResponse.DurableFlows.Sqlite;
using AsyncResponse.DurableFlows.SqlServer;
using AsyncResponse.Transports.AzureServiceBus;
using AsyncResponse.Transports.GooglePubSub;
using AsyncResponse.Transports.Kafka;
using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.NATS;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.RabbitMQ;
using AsyncResponse.Transports.Redis;
using AsyncResponse.Transports.SQS;
using AsyncResponse.Transports.SqlServer;
using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Microsoft.Azure.Cosmos;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MongoDB.Driver;
using MySqlConnector;
using NATS.Client.Core;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using StackExchange.Redis;

namespace AsyncResponse.Conformance;

/// <summary>
/// One fully wired host for a single <see cref="MatrixCell"/>: the channel, transport, and
/// durable-flow store an application would select, registered exactly the way an application
/// registers them — <c>AddAsyncResponse().With…Channel().With…Transport().With…DurableFlows()</c> —
/// pointed at the real servers the shard's fixture booted.
/// <para>
/// The harness owns three things the tests should not have to: per-cell namespace isolation (see
/// <see cref="MatrixNames"/>), the hosted-service lifecycle (transports run their subscribers as
/// hosted services, and a bare <c>BuildServiceProvider</c> never starts them), and teardown of
/// whatever the cell provisioned.
/// </para>
/// </summary>
public sealed class MatrixHarness : IAsyncDisposable
{
    /// <summary>
    /// How long to keep trying to get a probe job through before declaring the transport unusable.
    /// Generous because it covers a cold broker on a loaded runner; it costs nothing when the first
    /// probe succeeds, which is the normal case.
    /// </summary>
    private static readonly TimeSpan ReadinessBudget = TimeSpan.FromSeconds(90);

    /// <summary>How often the readiness probe re-publishes while waiting.</summary>
    private static readonly TimeSpan ReadinessRepublishInterval = TimeSpan.FromSeconds(2);

    private readonly List<IHostedService> _started = [];

    /// <summary>Destroys what the cell provisioned (schemas, streams, queues). Skippable — see <see cref="TeardownNamespaces"/>.</summary>
    private readonly List<Func<Task>> _teardown = [];

    /// <summary>
    /// Runs on every disposal regardless: closing clients and returning pooled leases. Skipping these
    /// would leak a connection per cell across 660 of them, and a Service Bus lease never returned
    /// deadlocks the next cell that asks for one.
    /// </summary>
    private readonly List<Func<Task>> _always = [];

    private ServiceProvider? _provider;

    private MatrixHarness(MatrixCell cell, MatrixNames names)
    {
        Cell = cell;
        Names = names;
    }

    /// <summary>The combination under test.</summary>
    public MatrixCell Cell { get; }

    /// <summary>The isolated namespace names this cell provisioned.</summary>
    public MatrixNames Names { get; }

    /// <summary>Subscriber knobs applied to whichever transport this cell selected.</summary>
    public MatrixTransportTuning Tuning { get; private init; } = new();

    /// <summary>
    /// Warnings and errors this host logged. Include it in a timeout message: a transport whose
    /// subscriber cannot start retries forever behind a caught exception, and this is the only place
    /// that says so.
    /// </summary>
    public MatrixDiagnostics Diagnostics { get; } = new();

    /// <summary>
    /// False builds a publish-only host: the transport is wired and its queues provisioned, but no
    /// subscriber runs. The durability contract needs one to enqueue work nobody is listening for.
    /// </summary>
    private bool StartSubscribers { get; init; } = true;

    /// <summary>
    /// False leaves the cell's schemas, queues, and streams in place on disposal. Two hosts sharing a
    /// namespace must not have the first one out drop what the second is still using; the surviving
    /// host does the cleanup.
    /// </summary>
    private bool TeardownNamespaces { get; init; } = true;

    /// <summary>The built host. Valid from <see cref="CreateAsync"/> returning until disposal.</summary>
    public ServiceProvider Provider =>
        _provider ?? throw new InvalidOperationException("The harness has not finished building.");

    public IAsyncResponsePublisher Publisher => Provider.GetRequiredService<IAsyncResponsePublisher>();
    public IAsyncResponseSubscriber Subscriber => Provider.GetRequiredService<IAsyncResponseSubscriber>();
    public IWorkerTransport WorkerTransport => Provider.GetRequiredService<IWorkerTransport>();
    public IDurableFlows Flows => Provider.GetRequiredService<IDurableFlows>();
    public IFlowStateStore FlowStore => Provider.GetRequiredService<IFlowStateStore>();

    /// <summary>
    /// Builds and starts a host for <paramref name="cell"/>. <paramref name="configureServices"/> runs
    /// before the AsyncResponse registration so a suite can add its own flows, handlers, and probes;
    /// <paramref name="configureDurableFlows"/> runs last on the shared durable-flow options so a fact
    /// can tighten a knob (lease duration, early-ACK acceptance) on any cell.
    /// </summary>
    public static async Task<MatrixHarness> CreateAsync(
        MatrixCell cell,
        MatrixBackends backends,
        Action<IServiceCollection>? configureServices = null,
        Action<DurableFlowOptions>? configureDurableFlows = null,
        Action<AsyncResponseRegistrationBuilder>? configureRegistration = null,
        MatrixTransportTuning? tuning = null,
        MatrixNames? sharedNames = null,
        bool startSubscribers = true,
        bool teardownNamespaces = true,
        CancellationToken cancellationToken = default)
    {
        // sharedNames lets two hosts address the same queue — which is how the durability contract
        // publishes from one host with its subscribers stopped and consumes from another. Passing the
        // names in also means the second host does not re-derive (and then tear down) a fresh set.
        var harness = new MatrixHarness(cell, sharedNames ?? new MatrixNames(cell))
        {
            Tuning = tuning ?? new MatrixTransportTuning(),
            StartSubscribers = startSubscribers,
            TeardownNamespaces = teardownNamespaces
        };
        try
        {
            await harness.BuildAsync(
                backends, configureServices, configureDurableFlows, configureRegistration, cancellationToken);
            return harness;
        }
        catch
        {
            await harness.DisposeAsync();
            throw;
        }
    }

    private async Task BuildAsync(
        MatrixBackends backends,
        Action<IServiceCollection>? configureServices,
        Action<DurableFlowOptions>? configureDurableFlows,
        Action<AsyncResponseRegistrationBuilder>? configureRegistration,
        CancellationToken cancellationToken)
    {
        var services = new ServiceCollection();

        // Warnings and errors are captured rather than discarded. Every transport subscriber runs its
        // loop inside a catch-log-retry-with-backoff, so a subscriber that cannot start at all fails
        // silently and every fact then dies on its own timeout with nothing to explain it — which is
        // exactly how a CI-only failure became undiagnosable from the logs. Diagnostics surfaces what
        // was logged when an assertion times out.
        services.AddSingleton(Diagnostics);
        services.AddSingleton<ILoggerProvider>(new MatrixLoggerProvider(Diagnostics));
        services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));

        // The readiness probe is its own service so its jobs never land in a suite's own counters.
        services.AddSingleton<MatrixReadinessProbe>();
        services.AddSingleton<IMatrixReadinessProbe>(provider => provider.GetRequiredService<MatrixReadinessProbe>());

        configureServices?.Invoke(services);

        AddBackendClients(services, backends);

        var builder = services.AddAsyncResponse(options =>
        {
            // The matrix never exercises the watchdog; keeping it idle removes a background scanner
            // from every one of the 660 cells.
            options.Watchdog.StartupDelay = TimeSpan.FromHours(1);
            options.Watchdog.Interval = TimeSpan.FromHours(1);
        });

        AddChannel(builder, backends);
        AddTransport(builder, backends);
        await AddStoreAsync(builder, services, backends, configureDurableFlows, cancellationToken);

        // Last, so a suite can register its flows (WithDurableFlow records the statically-typed
        // execution route the executor prefers over reflection) and any extra propagators.
        configureRegistration?.Invoke(builder);

        _provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        // The Pub/Sub emulator boots empty while the transport assumes its topics and subscriptions
        // already exist, so they have to be in place before the subscribers below attach.
        if (Cell.Transport == MatrixTransport.GooglePubSub)
            await ProvisionPubSubAsync(backends, cancellationToken);

        // Transports run their worker/response subscribers as hosted services. Nothing consumes a
        // worker job until these start, so the harness owns the host lifecycle the same way a real
        // application host would.
        foreach (var hosted in _provider.GetServices<IHostedService>())
        {
            // A publish-only host still needs its provisioners — SQS creates its queues from a hosted
            // service, and publishing to a queue that does not exist fails. Only the subscribers are
            // held back, which is what "no consumer is running" means.
            if (!StartSubscribers && hosted.GetType().Name.Contains("Subscriber", StringComparison.Ordinal))
                continue;

            await hosted.StartAsync(cancellationToken);
            _started.Add(hosted);
        }

        if (StartSubscribers)
            await WaitForTransportReadyAsync(cancellationToken);
    }

    /// <summary>
    /// Blocks until a job published now is actually consumed. Every transport subscriber is a
    /// <see cref="Microsoft.Extensions.Hosting.BackgroundService"/>, so <c>StartAsync</c> returns at
    /// the first await inside its loop — before the consumer group, JetStream consumer, or queue
    /// receiver it needs exists. A test that publishes immediately after the harness is built is
    /// therefore racing the subscriber, and loses that race on a loaded runner.
    /// <para>
    /// The probe re-publishes rather than publishing once: for the transports whose subscriber
    /// establishes its position at startup, a message sent into the gap can legitimately never be
    /// delivered, so a single unlucky publish would hang the whole harness.
    /// </para>
    /// </summary>
    private async Task WaitForTransportReadyAsync(CancellationToken cancellationToken)
    {
        var probe = Provider.GetRequiredService<MatrixReadinessProbe>();
        var builder = Provider.GetRequiredService<IAsyncResponseBuilder>();
        var deadline = DateTime.UtcNow + ReadinessBudget;
        Exception? lastPublishFailure = null;

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                await builder.EnqueueWorkerAsync<IMatrixReadinessProbe>(
                    service => service.PingAsync(), cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A broker that is up but not yet answering its admin API fails the PUBLISH, not
                // just the delivery — which is the not-ready condition this budget exists to
                // absorb. Keep probing, but keep the exception so a genuine misconfiguration still
                // surfaces with its cause when the budget runs out.
                lastPublishFailure = ex;
            }

            var completed = await Task.WhenAny(
                probe.Delivered.Task,
                Task.Delay(ReadinessRepublishInterval, cancellationToken));
            if (completed == probe.Delivered.Task)
                return;
        }

        throw new InvalidOperationException(
            $"The {Cell.Transport} transport did not deliver a probe job within {ReadinessBudget}, so the " +
            $"harness for {Cell} is not usable. Subscriber diagnostics: {Diagnostics.Summary()}." +
            (lastPublishFailure is null ? string.Empty : " The last probe publish also failed; see the inner exception."),
            lastPublishFailure);
    }

    // --- Backend client singletons ----------------------------------------------------------------
    // Registered exactly once per backend even when a cell uses the same server for two roles (the
    // MongoDB channel + MongoDB transport case), which is also how the sample app wires them.

    private void AddBackendClients(IServiceCollection services, MatrixBackends backends)
    {
        var cell = Cell;

        if (cell.Channel == MatrixChannel.Redis || cell.Transport == MatrixTransport.Redis)
        {
            var connectionString = backends.Require(backends.Redis, "redis");
            var multiplexer = ConnectionMultiplexer.Connect(connectionString);
            services.AddSingleton<IConnectionMultiplexer>(multiplexer);
            _teardown.Add(() => PurgeRedisAsync(multiplexer, Names.RedisPrefix));
            _always.Add(async () => await multiplexer.DisposeAsync());
        }

        if (cell.Channel == MatrixChannel.Nats || cell.Transport == MatrixTransport.Nats)
        {
            var url = backends.Require(backends.Nats, "nats");
            var connection = new NatsConnection(new NatsOpts { Url = url });
            services.AddSingleton<INatsConnection>(connection);
            _always.Add(async () => await connection.DisposeAsync());
        }

        if (cell.Channel == MatrixChannel.PostgreSql ||
            cell.Transport == MatrixTransport.PostgreSql ||
            cell.Store == MatrixStore.PostgreSql)
        {
            var connectionString = backends.Require(backends.PostgreSql, "postgres");
            var dataSource = NpgsqlDataSource.Create(connectionString);
            services.AddSingleton(dataSource);
            _always.Add(async () => await dataSource.DisposeAsync());
        }

        if (cell.Channel == MatrixChannel.MongoDb ||
            cell.Transport == MatrixTransport.MongoDb ||
            cell.Store == MatrixStore.MongoDb)
        {
            var connectionString = backends.Require(backends.MongoDb, "mongodb");
            var client = new MongoClient(connectionString);
            services.AddSingleton<IMongoClient>(client);
            services.AddSingleton(client.GetDatabase(Names.MongoDatabase));
            _teardown.Add(() => client.DropDatabaseAsync(Names.MongoDatabase));
            _always.Add(() =>
            {
                client.Dispose();
                return Task.CompletedTask;
            });
        }

        if (cell.Store == MatrixStore.DynamoDb)
        {
            var serviceUrl = backends.Require(backends.LocalStack, "localstack");
            var client = new AmazonDynamoDBClient(
                new BasicAWSCredentials("test", "test"),
                new AmazonDynamoDBConfig { ServiceURL = serviceUrl, AuthenticationRegion = "us-east-1" });
            services.AddSingleton<IAmazonDynamoDB>(client);
            _teardown.Add(async () =>
            {
                try
                {
                    await client.DeleteTableAsync(Names.StoreTable);
                }
                catch (Amazon.DynamoDBv2.Model.ResourceNotFoundException)
                {
                }
            });
            _always.Add(() =>
            {
                client.Dispose();
                return Task.CompletedTask;
            });
        }

        if (cell.Store == MatrixStore.Cosmos)
        {
            var connectionString = backends.Require(backends.Cosmos, "cosmos");
            var client = new CosmosClient(connectionString, CosmosClientOptionsFor(connectionString));
            services.AddSingleton(client);
            _teardown.Add(async () =>
            {
                try
                {
                    await client.GetDatabase(Names.CosmosDatabase).DeleteAsync();
                }
                catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                }
            });
            _always.Add(() =>
            {
                client.Dispose();
                return Task.CompletedTask;
            });
        }
    }

    // --- Channel ----------------------------------------------------------------------------------

    private void AddChannel(AsyncResponseRegistrationBuilder builder, MatrixBackends backends)
    {
        switch (Cell.Channel)
        {
            case MatrixChannel.InMemory:
                builder.WithInMemoryChannel(options => options.DefaultTimeout = TimeSpan.FromSeconds(30));
                break;

            case MatrixChannel.Redis:
                builder.WithRedisChannel(options =>
                {
                    options.KeyPrefix = Names.RedisPrefix;
                    options.DefaultTimeout = TimeSpan.FromSeconds(30);
                    options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
                });
                break;

            case MatrixChannel.Nats:
                builder.WithNatsChannel(options =>
                {
                    options.SubjectPrefix = Names.NatsSubjectPrefix;
                    options.RecoveryBucket = Names.NatsRecoveryBucket;
                    options.DefaultTimeout = TimeSpan.FromSeconds(30);
                    options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
                });
                break;

            // The three polling database channels take the same pacing, but each options class is
            // sealed and declares the knobs itself (only the recovery/timeout knobs live on the shared
            // base), so the assignments are repeated rather than factored behind a base-typed helper.
            // Production defaults use a multi-second confirmation budget; a cell wants the same
            // semantics at test speed, and a too-tight budget would steal live-but-slow deliveries
            // into the recovery path under a loaded fixture.
            case MatrixChannel.PostgreSql:
                builder.WithPostgreSqlChannel(options =>
                {
                    options.SchemaName = Names.ChannelSchema;
                    options.NotificationChannel = $"{Names.ChannelSchema}_notify";
                    options.DefaultTimeout = TimeSpan.FromSeconds(30);
                    options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
                    options.MessageRetention = TimeSpan.FromMinutes(5);
                    options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5);
                    options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(10);
                    options.ListenerPollInterval = TimeSpan.FromMilliseconds(25);
                    options.SubscriberHeartbeatInterval = TimeSpan.FromMilliseconds(50);
                    options.SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(5);
                    options.PruneInterval = TimeSpan.Zero;
                });
                _teardown.Add(() => DropPostgreSqlSchemaAsync(backends, Names.ChannelSchema));
                break;

            case MatrixChannel.SqlServer:
                builder.WithSqlServerChannel(options =>
                {
                    options.ConnectionString = backends.Require(backends.SqlServer, "sqlserver");
                    options.SchemaName = Names.ChannelSchema;
                    options.DefaultTimeout = TimeSpan.FromSeconds(30);
                    options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
                    options.MessageRetention = TimeSpan.FromMinutes(5);
                    options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5);
                    options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(10);
                    options.ActivePollInterval = TimeSpan.FromMilliseconds(25);
                    options.IdlePollInterval = TimeSpan.FromMilliseconds(100);
                    options.SubscriberHeartbeatInterval = TimeSpan.FromMilliseconds(50);
                    options.SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(5);
                    options.PruneInterval = TimeSpan.Zero;
                });
                _teardown.Add(() => DropSqlServerSchemaAsync(backends, Names.ChannelSchema));
                break;

            case MatrixChannel.MongoDb:
                builder.WithMongoDbChannel(options =>
                {
                    options.DatabaseName = Names.MongoDatabase;
                    options.DefaultTimeout = TimeSpan.FromSeconds(30);
                    options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
                    options.MessageRetention = TimeSpan.FromMinutes(5);
                    options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5);
                    options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(10);
                    options.ListenerPollInterval = TimeSpan.FromMilliseconds(50);
                    options.SubscriberHeartbeatInterval = TimeSpan.FromMilliseconds(100);
                    options.SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(5);
                });
                // The database itself is dropped by the Mongo client teardown.
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(Cell), Cell.Channel, null);
        }
    }

    // --- Transport --------------------------------------------------------------------------------

    private void AddTransport(AsyncResponseRegistrationBuilder builder, MatrixBackends backends)
    {
        switch (Cell.Transport)
        {
            case MatrixTransport.InMemory:
                builder.WithInMemoryTransport(options =>
                {
                    // The in-process queue's redelivery bound lives on the transport options rather
                    // than on a subscriber, but it is the same contract: retry a transient failure,
                    // stop retrying a poison one.
                    if (Tuning.MaxDeliveryAttempts is { } attempts)
                        options.MaxDeliveryAttempts = attempts;
                    if (Tuning.RedeliveryDelay is { } delay)
                    {
                        // Cap the ceiling too: retries back off exponentially toward RetryMaxDelay,
                        // which defaults to 5s and would dominate a bounded-retry assertion.
                        options.RetryBaseDelay = delay;
                        options.RetryMaxDelay = delay;
                    }
                });
                break;

            case MatrixTransport.Redis:
                builder.WithRedisTransport(options =>
                {
                    options.KeyPrefix = Names.RedisPrefix;
                    options.CreateConsumerGroups = true;
                    options.WorkerConsumerGroup = "arm-worker";
                    options.ResponseConsumerGroup = "arm-response";
                    options.DeadLetterEnabled = Tuning.DeadLetterEnabled ?? options.DeadLetterEnabled;
                    if (Tuning.MaxDeliveryAttempts is { } attempts)
                        options.WorkerSubscriber.MaxDeliveryAttempts = attempts;
                    if (Tuning.EarlyAck)
                        options.WorkerSubscriber.UseAckAfterEnqueue(backgroundWorkerCount: 4, backgroundQueueCapacity: 256);
                    if (Tuning.RedeliveryDelay is { } redisRedelivery)
                    {
                        // Redis redelivery is XAUTOCLAIM of pending entries idle for at least
                        // PendingMessageMinIdleTime, scanned every PendingClaimInterval — both have to
                        // come down or the reclaim lands after any reasonable test budget.
                        options.WorkerSubscriber.PendingMessageMinIdleTime = redisRedelivery;
                        options.WorkerSubscriber.PendingClaimInterval = redisRedelivery;
                    }
                    if (Tuning.HostShutdownTimeout is { } redisShutdown)
                        options.HostShutdownTimeout = redisShutdown;
                });
                break;

            case MatrixTransport.Nats:
                builder.WithNatsTransport(options =>
                {
                    options.SubjectPrefix = Names.NatsSubjectPrefix;
                    options.CreateStreams = true;
                    options.WorkerConsumer = "arm-worker";
                    options.ResponseConsumer = "arm-response";
                    options.DeadLetterEnabled = Tuning.DeadLetterEnabled ?? options.DeadLetterEnabled;
                    if (Tuning.MaxDeliveryAttempts is { } attempts)
                        options.WorkerSubscriber.MaxDeliveryAttempts = attempts;
                    if (Tuning.EarlyAck)
                        options.WorkerSubscriber.UseAckAfterEnqueue(backgroundWorkerCount: 4, backgroundQueueCapacity: 256);
                    if (Tuning.RedeliveryDelay is { } natsRedelivery)
                        options.WorkerSubscriber.RedeliveryDelay = natsRedelivery;
                    if (Tuning.HostShutdownTimeout is { } natsShutdown)
                        options.HostShutdownTimeout = natsShutdown;
                });
                _teardown.Add(() => DeleteNatsStreamsAsync(backends));
                break;

            case MatrixTransport.PostgreSql:
                builder.WithPostgreSqlTransport(options =>
                {
                    options.SchemaName = Names.TransportSchema;
                    options.NotificationChannel = $"{Names.TransportSchema}_notify";
                    options.DeadLetterEnabled = Tuning.DeadLetterEnabled ?? options.DeadLetterEnabled;
                    if (Tuning.MaxDeliveryAttempts is { } attempts)
                        options.WorkerSubscriber.MaxDeliveryAttempts = attempts;
                    if (Tuning.EarlyAck)
                        options.WorkerSubscriber.UseAckAfterEnqueue(backgroundWorkerCount: 4, backgroundQueueCapacity: 256);
                    if (Tuning.RedeliveryDelay is { } postgresqlRedelivery)
                        options.WorkerSubscriber.RedeliveryDelay = postgresqlRedelivery;
                    if (Tuning.HostShutdownTimeout is { } pgShutdown)
                        options.HostShutdownTimeout = pgShutdown;
                });
                _teardown.Add(() => DropPostgreSqlSchemaAsync(backends, Names.TransportSchema));
                break;

            case MatrixTransport.SqlServer:
                builder.WithSqlServerTransport(options =>
                {
                    options.ConnectionString = backends.Require(backends.SqlServer, "sqlserver");
                    options.SchemaName = Names.TransportSchema;
                    options.DeadLetterEnabled = Tuning.DeadLetterEnabled ?? options.DeadLetterEnabled;
                    if (Tuning.MaxDeliveryAttempts is { } attempts)
                        options.WorkerSubscriber.MaxDeliveryAttempts = attempts;
                    if (Tuning.EarlyAck)
                        options.WorkerSubscriber.UseAckAfterEnqueue(backgroundWorkerCount: 4, backgroundQueueCapacity: 256);
                    if (Tuning.RedeliveryDelay is { } sqlserverRedelivery)
                        options.WorkerSubscriber.RedeliveryDelay = sqlserverRedelivery;
                    if (Tuning.HostShutdownTimeout is { } sqlShutdown)
                        options.HostShutdownTimeout = sqlShutdown;
                });
                _teardown.Add(() => DropSqlServerSchemaAsync(backends, Names.TransportSchema));
                break;

            case MatrixTransport.MongoDb:
                builder.WithMongoDbTransport(options =>
                {
                    options.DatabaseName = Names.MongoDatabase;
                    options.MessageCollection = "arm_transport";
                    options.DeadLetterEnabled = Tuning.DeadLetterEnabled ?? options.DeadLetterEnabled;
                    if (Tuning.MaxDeliveryAttempts is { } attempts)
                        options.WorkerSubscriber.MaxDeliveryAttempts = attempts;
                    if (Tuning.EarlyAck)
                        options.WorkerSubscriber.UseAckAfterEnqueue(backgroundWorkerCount: 4, backgroundQueueCapacity: 256);
                    if (Tuning.RedeliveryDelay is { } mongodbRedelivery)
                        options.WorkerSubscriber.RedeliveryDelay = mongodbRedelivery;
                    if (Tuning.HostShutdownTimeout is { } mongoShutdown)
                        options.HostShutdownTimeout = mongoShutdown;
                });
                break;

            case MatrixTransport.Kafka:
                builder.WithKafkaTransport(options =>
                {
                    options.BootstrapServers = backends.Require(backends.Kafka, "kafka");
                    options.TopicPrefix = Names.KafkaTopicPrefix;
                    options.CreateTopics = true;
                    options.TopicNumPartitions = 1;
                    options.TopicReplicationFactor = 1;
                    options.WorkerConsumerGroup = Names.KafkaGroup("worker");
                    options.ResponseConsumerGroup = Names.KafkaGroup("response");
                    options.DeadLetterEnabled = Tuning.DeadLetterEnabled ?? options.DeadLetterEnabled;
                    if (Tuning.MaxDeliveryAttempts is { } attempts)
                        options.WorkerSubscriber.MaxDeliveryAttempts = attempts;
                    if (Tuning.EarlyAck)
                        options.WorkerSubscriber.UseAckAfterEnqueue(backgroundWorkerCount: 4, backgroundQueueCapacity: 256);
                    if (Tuning.HostShutdownTimeout is { } kafkaShutdown)
                        options.HostShutdownTimeout = kafkaShutdown;
                });
                break;

            case MatrixTransport.RabbitMq:
                builder.WithRabbitMqTransport(options =>
                {
                    options.ConnectionString = backends.Require(backends.RabbitMq, "rabbitmq");
                    options.WorkerExchange = $"{Names.RabbitBase}.worker";
                    options.WorkerQueue = $"{Names.RabbitBase}.worker";
                    options.WorkerRoutingKey = $"{Names.RabbitBase}.worker";
                    options.ResponseExchange = $"{Names.RabbitBase}.response";
                    options.ResponseQueue = $"{Names.RabbitBase}.response";
                    options.ResponseRoutingKey = $"{Names.RabbitBase}.response";
                    if (Tuning.MaxDeliveryAttempts is { } attempts)
                    {
                        options.WorkerSubscriber.MaxDeliveryAttempts = attempts;

                        // RabbitMQ does not count plain basic.nack requeues, so any bound above 2
                        // needs the TTL-retry dead-letter cycle to make attempts countable — without
                        // it a poison message requeues forever (docs/troubleshooting.md). Declaring
                        // the dead-letter exchange is what the docs prescribe for production, and the
                        // contract asserts the production shape.
                        options.DeadLetterExchange = $"{Names.RabbitBase}.dlx";
                        options.DeadLetterQueue = $"{Names.RabbitBase}.dlq";
                        options.DeadLetterRoutingKey = $"{Names.RabbitBase}.dead";
                    }
                    if (Tuning.EarlyAck)
                        options.WorkerSubscriber.UseAckAfterEnqueue(backgroundWorkerCount: 4, backgroundQueueCapacity: 256);
                    if (Tuning.HostShutdownTimeout is { } rabbitShutdown)
                        options.HostShutdownTimeout = rabbitShutdown;
                });
                break;

            case MatrixTransport.Sqs:
                builder.WithSqsTransport(options =>
                {
                    options.ServiceUrl = backends.Require(backends.LocalStack, "localstack");
                    options.Region = "us-east-1";
                    options.AccessKey = "test";
                    options.SecretKey = "test";
                    options.CreateQueues = true;
                    options.WorkerQueue = $"{Names.SqsBase}-worker";
                    options.ResponseQueue = $"{Names.SqsBase}-response";
                    // SQS has no library-side attempt bound: redelivery is the queue's visibility
                    // timeout and its redrive policy, so the attempt count lands on MaxReceiveCount.
                    if (Tuning.MaxDeliveryAttempts is { } attempts)
                        options.MaxReceiveCount = attempts;
                    if (Tuning.EarlyAck)
                        options.WorkerSubscriber.UseAckAfterEnqueue(backgroundWorkerCount: 4, backgroundQueueCapacity: 256);
                    if (Tuning.HostShutdownTimeout is { } sqsShutdown)
                        options.HostShutdownTimeout = sqsShutdown;
                });
                break;

            case MatrixTransport.AzureServiceBus:
                // The Service Bus emulator provisions its entities from a static config file and has
                // no management API, so per-cell queues are impossible. Cells lease a pair from the
                // pool the emulator config declares (see servicebus-emulator-config.json) and rely on
                // per-cell correlation ids plus the drain in teardown for isolation.
                var lease = AzureServiceBusQueuePool.Lease();
                var serviceBusConnection = backends.Require(backends.AzureServiceBus, "servicebus");
                _always.Add(async () =>
                {
                    // Drain worker/response queues AND their dead-letter sub-queues before the
                    // pair goes back to the pool: a failed or interrupted fact must not leak its
                    // messages into the next fact that leases the same pair. EXCEPT when this
                    // harness was created with teardownNamespaces: false — that flag means "a
                    // successor host consumes what I published" (the consumer-outage durability
                    // fact hands the pair from a stopped publisher to a fresh consumer), and
                    // draining here would eat the very message under test. The successor harness
                    // tears namespaces down normally and drains the pair on its own disposal.
                    try
                    {
                        if (TeardownNamespaces)
                            await DrainAzureServiceBusQueuePairAsync(serviceBusConnection, lease);
                    }
                    finally
                    {
                        AzureServiceBusQueuePool.Release(lease);
                    }
                });
                builder.WithAzureServiceBusTransport(options =>
                {
                    options.ConnectionString = backends.Require(backends.AzureServiceBus, "servicebus");
                    options.WorkerQueue = lease.WorkerQueue;
                    options.ResponseQueue = lease.ResponseQueue;
                    if (Tuning.MaxDeliveryAttempts is { } attempts)
                        options.WorkerSubscriber.MaxDeliveryAttempts = attempts;
                    if (Tuning.EarlyAck)
                        options.WorkerSubscriber.UseAckAfterEnqueue(backgroundWorkerCount: 4, backgroundQueueCapacity: 256);
                    if (Tuning.HostShutdownTimeout is { } busShutdown)
                        options.HostShutdownTimeout = busShutdown;
                });
                break;

            case MatrixTransport.GooglePubSub:
                builder.WithGooglePubSubTransport(options =>
                {
                    options.ProjectId = backends.Require(backends.PubSubProjectId, "pubsub");
                    options.WorkerTopicId = $"{Names.PubSubBase}-worker";
                    options.WorkerSubscriptionId = $"{Names.PubSubBase}-worker-sub";
                    options.ResponseTopicId = $"{Names.PubSubBase}-response";
                    options.ResponseSubscriptionId = $"{Names.PubSubBase}-response-sub";
                    // Pub/Sub redelivery is the subscription's ack deadline plus its dead-letter
                    // policy — there is no library-side attempt bound to tune here.
                    if (Tuning.EarlyAck)
                        options.WorkerSubscriber.UseAckAfterEnqueue(backgroundWorkerCount: 4, backgroundQueueCapacity: 256);
                    if (Tuning.HostShutdownTimeout is { } pubSubShutdown)
                        options.HostShutdownTimeout = pubSubShutdown;
                });
                // The emulator boots empty and the transport assumes its topics already exist, so the
                // harness provisions them before the subscribers start — same contract as the sample
                // app's PubSubEmulatorProvisioner, which is internal to the sample.
                _teardown.Add(() => DeletePubSubEntitiesAsync(backends));
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(Cell), Cell.Transport, null);
        }
    }

    // --- Durable-flow store -----------------------------------------------------------------------

    private async Task AddStoreAsync(
        AsyncResponseRegistrationBuilder builder,
        IServiceCollection services,
        MatrixBackends backends,
        Action<DurableFlowOptions>? configure,
        CancellationToken cancellationToken)
    {
        switch (Cell.Store)
        {
            case MatrixStore.InMemory:
                builder.WithInMemoryDurableFlows(options => configure?.Invoke(options));
                break;

            case MatrixStore.Sqlite:
                builder.WithSqliteDurableFlows(options =>
                {
                    options.ConnectionString = $"Data Source={Names.SqliteDatabasePath}";
                    options.TableName = Names.StoreTable;
                    configure?.Invoke(options);
                });
                _teardown.Add(() =>
                {
                    File.Delete(Names.SqliteDatabasePath);
                    return Task.CompletedTask;
                });
                break;

            case MatrixStore.PostgreSql:
                builder.WithPostgreSqlDurableFlows(options =>
                {
                    options.SchemaName = Names.StoreSchema;
                    configure?.Invoke(options);
                });
                _teardown.Add(() => DropPostgreSqlSchemaAsync(backends, Names.StoreSchema));
                break;

            case MatrixStore.SqlServer:
                builder.WithSqlServerDurableFlows(options =>
                {
                    options.ConnectionString = backends.Require(backends.SqlServer, "sqlserver");
                    options.SchemaName = Names.StoreSchema;
                    configure?.Invoke(options);
                });
                _teardown.Add(() => DropSqlServerSchemaAsync(backends, Names.StoreSchema));
                break;

            case MatrixStore.MySql:
                builder.WithMySqlDurableFlows(options =>
                {
                    options.ConnectionString = backends.Require(backends.MySql, "mysql");
                    options.TableName = Names.StoreTable;
                    configure?.Invoke(options);
                });
                _teardown.Add(() => DropMySqlTableAsync(backends, Names.StoreTable));
                break;

            case MatrixStore.MongoDb:
                builder.WithMongoDbDurableFlows(options =>
                {
                    options.CollectionName = Names.StoreCollection;
                    configure?.Invoke(options);
                });
                break;

            case MatrixStore.DynamoDb:
                builder.WithDynamoDbDurableFlows(options =>
                {
                    options.TableName = Names.StoreTable;
                    configure?.Invoke(options);
                });
                break;

            case MatrixStore.EFCore:
                // The EF Core store deliberately runs no DDL — an application owns its migrations. The
                // harness therefore creates the table from the package's own model mapping, which is
                // exactly what a generated migration would produce.
                services.AddSingleton(new MatrixEFCoreSchema(Names.StoreSchema));
                services.AddDbContextFactory<MatrixFlowDbContext>(options => options
                    .UseSqlServer(backends.Require(backends.SqlServer, "sqlserver"))
                    .ReplaceService<IModelCacheKeyFactory, MatrixModelCacheKeyFactory>());
                builder.WithEFCoreDurableFlows<MatrixFlowDbContext>(options => configure?.Invoke(options));
                _teardown.Add(() => DropSqlServerSchemaAsync(backends, Names.StoreSchema));
                break;

            case MatrixStore.Cosmos:
                builder.WithCosmosDurableFlows(options =>
                {
                    options.DatabaseName = Names.CosmosDatabase;
                    options.ContainerName = "flow_state";
                    configure?.Invoke(options);
                });
                break;

            case MatrixStore.Oracle:
                builder.WithOracleDurableFlows(options =>
                {
                    options.ConnectionString = backends.Require(backends.Oracle, "oracle");
                    options.TableName = Names.OracleStoreTable;
                    configure?.Invoke(options);
                });
                _teardown.Add(() => DropOracleTableAsync(backends, Names.OracleStoreTable));
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(Cell), Cell.Store, null);
        }

        if (Cell.Store == MatrixStore.EFCore)
            await CreateEFCoreTableAsync(backends, cancellationToken);
    }

    private async Task CreateEFCoreTableAsync(MatrixBackends backends, CancellationToken cancellationToken)
    {
        var services = new ServiceCollection();
        services.AddSingleton(new MatrixEFCoreSchema(Names.StoreSchema));
        services.AddDbContextFactory<MatrixFlowDbContext>(options => options
            .UseSqlServer(backends.Require(backends.SqlServer, "sqlserver"))
            .ReplaceService<IModelCacheKeyFactory, MatrixModelCacheKeyFactory>());
        await using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IDbContextFactory<MatrixFlowDbContext>>();
        await using var context = await factory.CreateDbContextAsync(cancellationToken);
        await context.GetService<IRelationalDatabaseCreator>().CreateTablesAsync(cancellationToken);
    }

    // --- Provisioning that the backends cannot do for themselves -----------------------------------

    /// <summary>
    /// Creates the Pub/Sub emulator's topics and subscriptions. The emulator boots empty while the
    /// transport assumes its entities exist; this is the harness equivalent of the sample app's
    /// provisioner, and it must run before the transport's subscribers attach.
    /// </summary>
    internal async Task ProvisionPubSubAsync(MatrixBackends backends, CancellationToken cancellationToken = default)
    {
        var projectId = backends.Require(backends.PubSubProjectId, "pubsub");
        var publisher = await new PublisherServiceApiClientBuilder
        {
            EmulatorDetection = EmulatorDetection.EmulatorOnly
        }.BuildAsync(cancellationToken);
        var subscriber = await new SubscriberServiceApiClientBuilder
        {
            EmulatorDetection = EmulatorDetection.EmulatorOnly
        }.BuildAsync(cancellationToken);

        await CreateTopicAsync(publisher, projectId, $"{Names.PubSubBase}-worker");
        await CreateTopicAsync(publisher, projectId, $"{Names.PubSubBase}-response");

        // The Pub/Sub transport deliberately exposes no delivery cap and no library-managed
        // dead-letter destination: on GCP that job belongs to the subscription's own DeadLetterPolicy,
        // and the subscriber logs a startup warning telling operators to configure one. Provisioning
        // the subscription that way here is what the library documents, so the poison-message contract
        // holds on Pub/Sub too rather than being skipped for want of infrastructure.
        var deadLetterTopic = $"{Names.PubSubBase}-dead";
        if (Tuning.MaxDeliveryAttempts is { } maxAttempts)
        {
            await CreateTopicAsync(publisher, projectId, deadLetterTopic);

            // A topic without a subscription DROPS messages: without this observer subscription
            // the dead-letter policy would forward exhausted messages into the void and no test
            // could ever verify dead-lettering happened.
            await CreateSubscriptionAsync(subscriber, projectId, $"{deadLetterTopic}-sub", deadLetterTopic, null);
        }

        await CreateSubscriptionAsync(
            subscriber, projectId, $"{Names.PubSubBase}-worker-sub", $"{Names.PubSubBase}-worker",
            Tuning.MaxDeliveryAttempts is { } attempts
                ? new DeadLetterPolicy
                {
                    DeadLetterTopic = TopicName.FormatProjectTopic(projectId, deadLetterTopic),
                    // Passed through as-is: the caller already clamped to Pub/Sub's floor of 5 via
                    // TransportCapabilities.MinDeliveryAttempts, so the policy and the assertion agree.
                    MaxDeliveryAttempts = attempts
                }
                : null);
        await CreateSubscriptionAsync(
            subscriber, projectId, $"{Names.PubSubBase}-response-sub", $"{Names.PubSubBase}-response", null);

        static async Task CreateTopicAsync(PublisherServiceApiClient client, string projectId, string topicId)
        {
            try
            {
                await client.CreateTopicAsync(TopicName.FromProjectTopic(projectId, topicId));
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
            {
            }
        }

        static async Task CreateSubscriptionAsync(
            SubscriberServiceApiClient client,
            string projectId,
            string subscriptionId,
            string topicId,
            DeadLetterPolicy? deadLetterPolicy)
        {
            try
            {
                await client.CreateSubscriptionAsync(new Subscription
                {
                    SubscriptionName = SubscriptionName.FromProjectSubscription(projectId, subscriptionId),
                    TopicAsTopicName = TopicName.FromProjectTopic(projectId, topicId),
                    AckDeadlineSeconds = 60,
                    DeadLetterPolicy = deadLetterPolicy
                });
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.AlreadyExists)
            {
            }
        }
    }

    /// <summary>
    /// Empties both queues of a leased Service Bus pair, dead-letter sub-queues included. The
    /// emulator has no management API to delete or recreate entities, so receive-and-delete until
    /// silence is the only isolation the pool can guarantee between leases.
    /// </summary>
    private static async Task DrainAzureServiceBusQueuePairAsync(string connectionString, AzureServiceBusQueueLease lease)
    {
        await using var client = new Azure.Messaging.ServiceBus.ServiceBusClient(connectionString);
        foreach (var queue in new[] { lease.WorkerQueue, lease.ResponseQueue })
        {
            foreach (var subQueue in new[] { Azure.Messaging.ServiceBus.SubQueue.None, Azure.Messaging.ServiceBus.SubQueue.DeadLetter })
            {
                await using var receiver = client.CreateReceiver(queue, new Azure.Messaging.ServiceBus.ServiceBusReceiverOptions
                {
                    SubQueue = subQueue,
                    ReceiveMode = Azure.Messaging.ServiceBus.ServiceBusReceiveMode.ReceiveAndDelete
                });
                while (true)
                {
                    var drained = await receiver.ReceiveMessagesAsync(50, TimeSpan.FromMilliseconds(400));
                    if (drained.Count == 0)
                        break;
                }
            }
        }
    }

    // --- Teardown ---------------------------------------------------------------------------------

    private int _disposed;

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        // Idempotent: facts that time or interleave disposal explicitly also sit inside
        // `await using`, and a second pass must not re-run teardown — most acutely the Service
        // Bus queue-pool release, where a duplicate release pushed a duplicate lease.
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        foreach (var hosted in Enumerable.Reverse(_started))
        {
            try
            {
                using var stopping = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                await hosted.StopAsync(stopping.Token);
            }
            catch
            {
                // A subscriber that cannot stop cleanly must not mask the test's own outcome, and the
                // provider disposal below tears the rest down regardless.
            }
        }

        if (_provider is not null)
            await _provider.DisposeAsync();

        // Teardown in reverse registration order: the store's objects go before the transport's,
        // which go before the channel's, mirroring how they were provisioned.
        if (TeardownNamespaces)
            await RunBestEffortAsync(_teardown);

        await RunBestEffortAsync(_always);
    }

    /// <summary>
    /// Runs cleanup in reverse registration order, swallowing failures. Best-effort on purpose: a
    /// leaked schema costs disk in a disposable container, whereas a throwing teardown would report a
    /// passing cell as failed.
    /// </summary>
    private static async Task RunBestEffortAsync(List<Func<Task>> steps)
    {
        foreach (var step in Enumerable.Reverse(steps))
        {
            try
            {
                await step();
            }
            catch
            {
            }
        }
    }

    private static async Task PurgeRedisAsync(IConnectionMultiplexer multiplexer, string prefix)
    {
        foreach (var endpoint in multiplexer.GetEndPoints())
        {
            var server = multiplexer.GetServer(endpoint);
            if (!server.IsConnected || server.IsReplica)
                continue;

            var database = multiplexer.GetDatabase();
            await foreach (var key in server.KeysAsync(pattern: $"{prefix}*"))
                await database.KeyDeleteAsync(key);
        }
    }

    private async Task DeleteNatsStreamsAsync(MatrixBackends backends)
    {
        // JetStream streams outlive the connection, and a rerun of the same cell would otherwise
        // re-attach to a stream still holding the previous run's messages.
        await using var connection = new NatsConnection(
            new NatsOpts { Url = backends.Require(backends.Nats, "nats") });
        var context = new NATS.Client.JetStream.NatsJSContext(connection);
        await foreach (var stream in context.ListStreamNamesAsync())
        {
            if (!stream.Contains(Cell.Token, StringComparison.Ordinal))
                continue;

            try
            {
                await context.DeleteStreamAsync(stream);
            }
            catch
            {
            }
        }
    }

    private async Task DeletePubSubEntitiesAsync(MatrixBackends backends)
    {
        var projectId = backends.Require(backends.PubSubProjectId, "pubsub");
        var publisher = await new PublisherServiceApiClientBuilder
        {
            EmulatorDetection = EmulatorDetection.EmulatorOnly
        }.BuildAsync();
        var subscriber = await new SubscriberServiceApiClientBuilder
        {
            EmulatorDetection = EmulatorDetection.EmulatorOnly
        }.BuildAsync();

        foreach (var subscriptionId in new[] { $"{Names.PubSubBase}-worker-sub", $"{Names.PubSubBase}-response-sub" })
        {
            try
            {
                await subscriber.DeleteSubscriptionAsync(
                    SubscriptionName.FromProjectSubscription(projectId, subscriptionId));
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
            {
            }
        }

        foreach (var topicId in new[] { $"{Names.PubSubBase}-worker", $"{Names.PubSubBase}-response" })
        {
            try
            {
                await publisher.DeleteTopicAsync(TopicName.FromProjectTopic(projectId, topicId));
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.NotFound)
            {
            }
        }
    }

    private static async Task DropPostgreSqlSchemaAsync(MatrixBackends backends, string schema)
    {
        await using var dataSource = NpgsqlDataSource.Create(backends.Require(backends.PostgreSql, "postgres"));
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"""DROP SCHEMA IF EXISTS "{schema}" CASCADE;""";
        await command.ExecuteNonQueryAsync();
    }

    private static async Task DropSqlServerSchemaAsync(MatrixBackends backends, string schema)
    {
        await using var connection = new SqlConnection(backends.Require(backends.SqlServer, "sqlserver"));
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        // SQL Server has no DROP SCHEMA ... CASCADE: drop the schema's tables and sequences first
        // (the channel keeps a monotonic ack sequence), then the schema itself.
        command.CommandText =
            """
            DECLARE @drop nvarchar(max) = N'';
            SELECT @drop += N'DROP TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N';'
            FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = @schema;
            SELECT @drop += N'DROP SEQUENCE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(q.name) + N';'
            FROM sys.sequences q JOIN sys.schemas s ON q.schema_id = s.schema_id WHERE s.name = @schema;
            IF SCHEMA_ID(@schema) IS NOT NULL SET @drop += N'DROP SCHEMA ' + QUOTENAME(@schema) + N';';
            EXEC sp_executesql @drop;
            """;
        command.Parameters.AddWithValue("@schema", schema);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task DropMySqlTableAsync(MatrixBackends backends, string table)
    {
        await using var connection = new MySqlConnection(backends.Require(backends.MySql, "mysql"));
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
        await command.ExecuteNonQueryAsync();
    }

    private static async Task DropOracleTableAsync(MatrixBackends backends, string table)
    {
        await using var connection = new OracleConnection(backends.Require(backends.Oracle, "oracle"));
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"DROP TABLE {table} PURGE";
        try
        {
            await command.ExecuteNonQueryAsync();
        }
        catch (OracleException ex) when (ex.Number == 942)
        {
            // ORA-00942: the cell never got as far as creating its table.
        }
    }

    private static CosmosClientOptions CosmosClientOptionsFor(string connectionString)
    {
        var options = new CosmosClientOptions();
        if (!connectionString.Contains("localhost", StringComparison.OrdinalIgnoreCase) &&
            !connectionString.Contains("127.0.0.1", StringComparison.Ordinal))
        {
            return options;
        }

        // The emulator serves a self-signed certificate and advertises addresses that only resolve
        // inside its own container, so gateway mode pinned to the endpoint is the only usable shape.
        options.ConnectionMode = ConnectionMode.Gateway;
        options.LimitToEndpoint = true;
        options.HttpClientFactory = () => new HttpClient(new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
        });
        return options;
    }
}

/// <summary>
/// Subscriber knobs the transport contract needs to set. Each is optional; unset means the package
/// default, which is what the combination matrix runs. The knobs land on differently-named options
/// per transport (and on the queue rather than the subscriber for SQS), so
/// <see cref="MatrixHarness"/> maps them one transport at a time — see
/// <see cref="TransportCapabilities"/> for which transports honour which.
/// </summary>
public sealed record MatrixTransportTuning
{
    /// <summary>
    /// How many times a failing handler is retried before the message is dead-lettered. Maps to the
    /// subscriber's <c>MaxDeliveryAttempts</c>, or to the queue's <c>MaxReceiveCount</c> on SQS.
    /// Ignored by Google Pub/Sub, which has no library-side bound.
    /// </summary>
    public int? MaxDeliveryAttempts { get; init; }

    /// <summary>
    /// Toggles the library-created dead-letter destination. Ignored by the transports whose broker
    /// dead-letters natively (RabbitMQ, Service Bus, SQS, Pub/Sub).
    /// </summary>
    public bool? DeadLetterEnabled { get; init; }

    /// <summary>Runs the worker subscriber in <c>AckAfterEnqueue</c> mode.</summary>
    public bool EarlyAck { get; init; }

    /// <summary>
    /// How long a failed message waits before its next delivery. The redelivery facts set this low so
    /// they assert the <em>semantics</em> — a failed job comes back, a poison job stops coming back —
    /// rather than each transport's default latency, which varies by two orders of magnitude (Redis
    /// reclaims pending stream entries after a 30-second idle window; the database queues reschedule
    /// in seconds).
    /// <para>
    /// Where the wait is the broker's rather than the library's — SQS visibility timeout, Service Bus
    /// lock duration, Pub/Sub ack deadline, RabbitMQ requeue — there is nothing to set and the fact
    /// simply gets a budget long enough for the real thing.
    /// </para>
    /// </summary>
    public TimeSpan? RedeliveryDelay { get; init; }

    /// <summary>Shortens the transport's shutdown budget so a drain assertion does not wait it out.</summary>
    public TimeSpan? HostShutdownTimeout { get; init; }
}

/// <summary>
/// Collects the warnings and errors one host logged, so a timed-out assertion can say what the
/// transport was actually doing instead of only that nothing arrived.
/// </summary>
public sealed class MatrixDiagnostics
{
    private readonly System.Collections.Concurrent.ConcurrentQueue<string> _entries = new();

    /// <summary>Everything logged at warning or above, oldest first.</summary>
    public IReadOnlyList<string> Entries => [.. _entries];

    internal void Add(string entry) => _entries.Enqueue(entry);

    /// <summary>
    /// A short report for an assertion message. Deduplicated because a failing subscriber logs the
    /// same warning on every backoff iteration and the raw list is then hundreds of identical lines.
    /// </summary>
    public string Summary()
    {
        var distinct = _entries.Distinct(StringComparer.Ordinal).Take(5).ToArray();
        return distinct.Length == 0
            ? "no warnings or errors were logged"
            : string.Join(" | ", distinct);
    }
}

/// <summary>Routes warning-and-above log entries into <see cref="MatrixDiagnostics"/>.</summary>
internal sealed class MatrixLoggerProvider(MatrixDiagnostics diagnostics) : ILoggerProvider
{
    /// <inheritdoc />
    public ILogger CreateLogger(string categoryName) => new MatrixLogger(categoryName, diagnostics);

    /// <inheritdoc />
    public void Dispose()
    {
    }

    private sealed class MatrixLogger(string category, MatrixDiagnostics diagnostics) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
                return;

            var message = formatter(state, exception);
            diagnostics.Add(exception is null
                ? $"{logLevel} {category}: {message}"
                : $"{logLevel} {category}: {message} -> {exception.GetType().Name}: {exception.Message}");
        }
    }
}

/// <summary>
/// The service the readiness handshake offloads. Deliberately separate from anything a suite
/// registers, so probe jobs never appear in a suite's own call counts.
/// </summary>
public interface IMatrixReadinessProbe
{
    /// <summary>Signals that the worker transport is delivering.</summary>
    Task PingAsync();
}

/// <inheritdoc />
public sealed class MatrixReadinessProbe : IMatrixReadinessProbe
{
    /// <summary>Completes the first time a probe job is delivered.</summary>
    internal TaskCompletionSource Delivered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <inheritdoc />
    public Task PingAsync()
    {
        // TrySet: the handshake re-publishes, so several probe jobs can legitimately arrive.
        Delivered.TrySetResult();
        return Task.CompletedTask;
    }
}

/// <summary>Schema name for <see cref="MatrixFlowDbContext"/>, supplied through DI per cell.</summary>
public sealed record MatrixEFCoreSchema(string Name);

/// <summary>
/// The application-owned <see cref="DbContext"/> the EF Core store maps through. Applications own
/// this type (and its migrations); the matrix supplies the minimum an application must write.
/// </summary>
public sealed class MatrixFlowDbContext(DbContextOptions<MatrixFlowDbContext> options, MatrixEFCoreSchema schema)
    : DbContext(options)
{
    /// <summary>The per-cell schema this context maps into. Part of the model cache key.</summary>
    internal string Schema => schema.Name;

    /// <inheritdoc />
    protected override void OnModelCreating(ModelBuilder modelBuilder)
        => modelBuilder.ConfigureAsyncResponseDurableFlows(schema: schema.Name);
}

/// <summary>
/// Keys EF Core's compiled-model cache on the schema as well as the context type. Without this every
/// cell after the first reuses the first cell's model — EF caches per context type and provider, and
/// the schema is injected rather than baked into the type — so each cell tries to create its table in
/// a previous cell's schema and fails with "there is already an object named …".
/// <para>
/// Applications never hit this: they have one schema for the life of the process. It is a
/// consequence of running 36 differently-mapped cells in one process, so the fix belongs here.
/// </para>
/// </summary>
internal sealed class MatrixModelCacheKeyFactory : IModelCacheKeyFactory
{
    /// <inheritdoc />
    public object Create(DbContext context, bool designTime)
        => context is MatrixFlowDbContext matrix
            ? (context.GetType(), matrix.Schema, designTime)
            : (object)(context.GetType(), designTime);
}

/// <summary>
/// A fixed pool of Service Bus queue pairs, leased per cell. The emulator provisions entities only
/// from its static config file — it exposes no management API — so unlike every other transport the
/// matrix cannot create a queue per cell. The pool is declared in
/// <c>servicebus-emulator-config.json</c> and this type hands out one pair at a time.
/// </summary>
internal static class AzureServiceBusQueuePool
{
    /// <summary>
    /// Pool size. Integration execution is serial (parallelization is disabled assembly-wide) and
    /// the free list is LIFO, so exactly one pair is ever in use — leasing from a single pair
    /// keeps the drain-between-leases guarantee airtight and leaves the other emulator-declared
    /// pairs untouched. Raise toward the emulator config's pair count only together with
    /// parallel execution.
    /// </summary>
    internal const int Size = 1;

    private static readonly SemaphoreSlim Available = new(Size, Size);
    private static readonly Stack<int> Free = new(Enumerable.Range(0, Size));

    internal static AzureServiceBusQueueLease Lease()
    {
        Available.Wait();
        int index;
        lock (Free)
            index = Free.Pop();
        return new AzureServiceBusQueueLease(index);
    }

    internal static void Release(AzureServiceBusQueueLease lease)
    {
        lock (Free)
            Free.Push(lease.Index);
        Available.Release();
    }
}

/// <summary>One leased Service Bus queue pair from <see cref="AzureServiceBusQueuePool"/>.</summary>
internal readonly record struct AzureServiceBusQueueLease(int Index)
{
    internal string WorkerQueue => $"arm-asb-worker-{Index:00}";

    internal string ResponseQueue => $"arm-asb-response-{Index:00}";
}
