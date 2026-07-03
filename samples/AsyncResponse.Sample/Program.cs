using AsyncResponse;
using AsyncResponse.Channels.NATS;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.Redis;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Sample;
using AsyncResponse.Transports.AzureServiceBus;
using AsyncResponse.Transports.GooglePubSub;
using AsyncResponse.Transports.Kafka;
using AsyncResponse.Transports.NATS;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.RabbitMQ;
using AsyncResponse.Transports.Redis;
using AsyncResponse.Transports.SqlServer;
using Confluent.Kafka;
using NATS.Client.Core;
using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using System.Text;
using StackExchange.Redis;
using System.Text.Json;
using Npgsql;
using Azure.Messaging.ServiceBus;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.Services.AddOpenApi();

// --- Provider selection (configuration-driven) ----------------------------------------------
// Channel = the response/recovery substrate (exactly one); Transport = worker dispatch (exactly one).
// Defaults are fully in-memory so `dotnet run` works with no external dependencies; the AppHosts
// override them to Redis + broker transports to exercise the durable, broker-backed stack.
var channel = builder.Configuration["AsyncResponse:Channel"] ?? "InMemory";      // InMemory | Redis | NATS | PostgreSQL | SqlServer
var transport = builder.Configuration["AsyncResponse:Transport"] ?? "InMemory";  // InMemory | AzureServiceBus | GooglePubSub | Kafka | RabbitMQ | Redis | NATS | PostgreSQL | SqlServer
var useInMemoryChannel = string.Equals(channel, "InMemory", StringComparison.OrdinalIgnoreCase);
var useRedis = string.Equals(channel, "Redis", StringComparison.OrdinalIgnoreCase);
var useNats = string.Equals(channel, "NATS", StringComparison.OrdinalIgnoreCase);
var usePostgreSql = string.Equals(channel, "PostgreSQL", StringComparison.OrdinalIgnoreCase);
var useSqlServer = string.Equals(channel, "SqlServer", StringComparison.OrdinalIgnoreCase);
var useInMemoryTransport = string.Equals(transport, "InMemory", StringComparison.OrdinalIgnoreCase);
var useAzureServiceBus = string.Equals(transport, "AzureServiceBus", StringComparison.OrdinalIgnoreCase);
var useGooglePubSub = string.Equals(transport, "GooglePubSub", StringComparison.OrdinalIgnoreCase);
var useKafka = string.Equals(transport, "Kafka", StringComparison.OrdinalIgnoreCase);
var useRabbitMq = string.Equals(transport, "RabbitMQ", StringComparison.OrdinalIgnoreCase);
var useRedisTransport = string.Equals(transport, "Redis", StringComparison.OrdinalIgnoreCase);
var useNatsTransport = string.Equals(transport, "NATS", StringComparison.OrdinalIgnoreCase);
var usePostgreSqlTransport = string.Equals(transport, "PostgreSQL", StringComparison.OrdinalIgnoreCase);
var useSqlServerTransport = string.Equals(transport, "SqlServer", StringComparison.OrdinalIgnoreCase);

if (useRedis || useRedisTransport)
{
    var redisConnectionString = builder.Configuration.GetConnectionString("Redis") ?? "localhost:6379";
    builder.Services.AddSingleton<IConnectionMultiplexer>(_ => ConnectionMultiplexer.Connect(redisConnectionString));
}

if (useNats || useNatsTransport)
{
    // One shared NATS connection backs both the NATS channel and the NATS transport.
    var natsUrl = builder.Configuration["Nats:Url"] ?? "nats://localhost:4222";
    builder.Services.AddSingleton<INatsConnection>(_ => new NatsConnection(new NatsOpts { Url = natsUrl }));
}

if (usePostgreSql || usePostgreSqlTransport)
{
    var postgresConnectionString = builder.Configuration.GetConnectionString("PostgreSQL")
        ?? builder.Configuration["PostgreSQL:ConnectionString"]
        ?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postgres";
    // Recommended Npgsql tuning for the table-backed channel/transport: skip the per-checkin DISCARD
    // ALL and keep auto-prepared statements across pooled reuse. Together they roughly halve
    // server-side statements and parse/plan CPU under load. Applied unless the caller set them
    // explicitly, so a provided connection string still wins.
    var postgresBuilder = new NpgsqlConnectionStringBuilder(postgresConnectionString);
    if (!postgresConnectionString.Contains("Reset On Close", StringComparison.OrdinalIgnoreCase))
        postgresBuilder.NoResetOnClose = true;
    if (postgresBuilder.MaxAutoPrepare == 0)
        postgresBuilder.MaxAutoPrepare = 20;
    builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(postgresBuilder.ConnectionString));
}

if (useSqlServer || useSqlServerTransport)
{
    // Create the target database on startup if it does not exist (dev/container convenience). The
    // AsyncResponse SQL Server packages create their own schema and tables, but cannot connect to a
    // missing database. Registered before the AsyncResponse wiring so it runs before any subscriber.
    builder.Services.AddHostedService<SqlServerDatabaseProvisioner>();
}

// Provision the emulator's topics/subscriptions before the transport's subscribers start
// (emulator-only; no-op against real GCP). Registered first so its StartAsync completes first.
if (useAzureServiceBus)
{
    ConfigureHostShutdownBudget(builder.Configuration, builder.Services, "AzureServiceBus");
}
else if (useGooglePubSub)
{
    ConfigureHostShutdownBudget(builder.Configuration, builder.Services);
    builder.Services.AddHostedService<PubSubEmulatorProvisioner>();
    builder.Services.AddSingleton(_ => new Lazy<Task<PublisherServiceApiClient>>(() => new PublisherServiceApiClientBuilder
    {
        EmulatorDetection = EmulatorDetection.EmulatorOrProduction
    }.BuildAsync()));
}
else if (useKafka)
{
    ConfigureHostShutdownBudget(builder.Configuration, builder.Services, "Kafka");
}
else if (useRabbitMq)
{
    ConfigureHostShutdownBudget(builder.Configuration, builder.Services, "RabbitMQ");
}
else if (useRedisTransport)
{
    ConfigureHostShutdownBudget(builder.Configuration, builder.Services, "Redis");
}
else if (usePostgreSqlTransport)
{
    ConfigureHostShutdownBudget(builder.Configuration, builder.Services, "PostgreSQL");
}
else if (useSqlServerTransport)
{
    ConfigureHostShutdownBudget(builder.Configuration, builder.Services, "SqlServer");
}

var asyncResponse = builder.Services.AddAsyncResponse(options =>
{
    // Aggressive watchdog so the stale-recovery demo/health scenario resolves quickly; defaults 6h/24h/5m.
    options.Watchdog.StartupDelay = TimeSpan.FromSeconds(1);
    options.Watchdog.Interval = TimeSpan.FromSeconds(1);
    options.Watchdog.StaleAfter = TimeSpan.FromSeconds(2);
});

// Exactly one channel (enforced at host startup).
if (useRedis)
{
    asyncResponse.WithRedisChannel(options =>
    {
        options.KeyPrefix = builder.Configuration["AsyncResponse:KeyPrefix"] ?? "sample";
        options.DefaultTimeout = TimeSpan.FromSeconds(30);
    });
}
else if (useNats)
{
    asyncResponse.WithNatsChannel(options =>
    {
        options.SubjectPrefix = builder.Configuration["Nats:SubjectPrefix"] ?? options.SubjectPrefix;
        options.RecoveryBucket = builder.Configuration["Nats:RecoveryBucket"] ?? options.RecoveryBucket;
        options.DefaultTimeout = TimeSpan.FromSeconds(30);
    });
}
else if (usePostgreSql)
{
    asyncResponse.WithPostgreSqlChannel(options =>
    {
        options.SchemaName = builder.Configuration["PostgreSQL:SchemaName"] ?? options.SchemaName;
        options.RecoveryStateTable = builder.Configuration["PostgreSQL:RecoveryStateTable"] ?? options.RecoveryStateTable;
        options.MessageTable = builder.Configuration["PostgreSQL:ChannelMessageTable"] ?? options.MessageTable;
        options.SubscriberTable = builder.Configuration["PostgreSQL:SubscriberTable"] ?? options.SubscriberTable;
        options.NotificationChannel = builder.Configuration["PostgreSQL:ChannelNotificationChannel"] ?? options.NotificationChannel;
        options.DefaultTimeout = TimeSpan.FromSeconds(30);
        ApplyOptionalTimeout(builder.Configuration, "PostgreSQL:ChannelDeliveryConfirmationTimeoutSeconds", value => options.DeliveryConfirmationTimeout = value);
    });
}
else if (useSqlServer)
{
    asyncResponse.WithSqlServerChannel(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("SqlServer")
            ?? builder.Configuration["SqlServer:ConnectionString"];
        options.SchemaName = builder.Configuration["SqlServer:SchemaName"] ?? options.SchemaName;
        options.RecoveryStateTable = builder.Configuration["SqlServer:RecoveryStateTable"] ?? options.RecoveryStateTable;
        options.MessageTable = builder.Configuration["SqlServer:ChannelMessageTable"] ?? options.MessageTable;
        options.SubscriberTable = builder.Configuration["SqlServer:SubscriberTable"] ?? options.SubscriberTable;
        options.DefaultTimeout = TimeSpan.FromSeconds(30);
        ApplyOptionalTimeout(builder.Configuration, "SqlServer:ChannelDeliveryConfirmationTimeoutSeconds", value => options.DeliveryConfirmationTimeout = value);
        ApplyOptionalMilliseconds(builder.Configuration, "SqlServer:ChannelActivePollIntervalMs", value => options.ActivePollInterval = value);
        ApplyOptionalMilliseconds(builder.Configuration, "SqlServer:ChannelIdlePollIntervalMs", value => options.IdlePollInterval = value);
    });
}
else if (useInMemoryChannel)
{
    asyncResponse.WithInMemoryChannel();
}
else
{
    throw new InvalidOperationException(
        "Unsupported AsyncResponse:Channel value. Use 'InMemory', 'Redis', 'NATS', 'PostgreSQL', or 'SqlServer'.");
}

// Exactly one worker transport (enforced at host startup).
if (useGooglePubSub)
{
    asyncResponse.WithGooglePubSubTransport(options =>
    {
        options.ProjectId = builder.Configuration["PubSub:ProjectId"];
        options.WorkerTopicId = builder.Configuration["PubSub:WorkerTopicId"];
        options.WorkerSubscriptionId = builder.Configuration["PubSub:WorkerSubscriptionId"];
        options.ResponseTopicId = builder.Configuration["PubSub:ResponseTopicId"];
        options.ResponseSubscriptionId = builder.Configuration["PubSub:ResponseSubscriptionId"];
        ConfigurePubSubShutdownBudget(builder.Configuration, options);
        ConfigurePubSubSubscriberAckMode(builder.Configuration, "PubSub:Worker", options.WorkerSubscriber);
        ConfigurePubSubSubscriberAckMode(builder.Configuration, "PubSub:Response", options.ResponseSubscriber);
    });
}
else if (useAzureServiceBus)
{
    asyncResponse.WithAzureServiceBusTransport(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("AzureServiceBus")
            ?? builder.Configuration["AzureServiceBus:ConnectionString"];
        options.WorkerQueue = builder.Configuration["AzureServiceBus:WorkerQueue"] ?? options.WorkerQueue;
        options.ResponseQueue = builder.Configuration["AzureServiceBus:ResponseQueue"] ?? options.ResponseQueue;
        options.CorrelationIdProperty = builder.Configuration["AzureServiceBus:CorrelationIdProperty"] ?? options.CorrelationIdProperty;

        if (int.TryParse(builder.Configuration["AzureServiceBus:MaxMessagesPerReceive"], out var maxMessagesPerReceive))
            options.MaxMessagesPerReceive = maxMessagesPerReceive;
        if (int.TryParse(builder.Configuration["AzureServiceBus:PublishMaxAttempts"], out var publishMaxAttempts))
            options.PublishMaxAttempts = publishMaxAttempts;

        ApplyOptionalTimeout(builder.Configuration, "AzureServiceBus:ReceiveWaitTimeSeconds", value => options.ReceiveWaitTime = value);
        ApplyOptionalMilliseconds(builder.Configuration, "AzureServiceBus:PublishRetryBaseDelayMs", value => options.PublishRetryBaseDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "AzureServiceBus:PublishRetryMaxDelayMs", value => options.PublishRetryMaxDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "AzureServiceBus:SubscriberRetryBaseDelayMs", value => options.SubscriberRetryBaseDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "AzureServiceBus:SubscriberRetryMaxDelayMs", value => options.SubscriberRetryMaxDelay = value);
        ConfigureAzureServiceBusShutdownBudget(builder.Configuration, options);
        ConfigureAzureServiceBusSubscriber(builder.Configuration, "AzureServiceBus:Worker", options.WorkerSubscriber);
        ConfigureAzureServiceBusSubscriber(builder.Configuration, "AzureServiceBus:Response", options.ResponseSubscriber);
    });
}
else if (useRabbitMq)
{
    asyncResponse.WithRabbitMqTransport(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("RabbitMQ")
            ?? builder.Configuration["RabbitMQ:ConnectionString"];
        options.HostName = builder.Configuration["RabbitMQ:HostName"] ?? options.HostName;
        options.VirtualHost = builder.Configuration["RabbitMQ:VirtualHost"] ?? options.VirtualHost;
        options.UserName = builder.Configuration["RabbitMQ:UserName"] ?? options.UserName;
        options.Password = builder.Configuration["RabbitMQ:Password"] ?? options.Password;
        options.ClientProvidedName = builder.Configuration["RabbitMQ:ClientProvidedName"] ?? options.ClientProvidedName;
        options.WorkerExchange = builder.Configuration["RabbitMQ:WorkerExchange"] ?? options.WorkerExchange;
        options.WorkerQueue = builder.Configuration["RabbitMQ:WorkerQueue"] ?? options.WorkerQueue;
        options.WorkerRoutingKey = builder.Configuration["RabbitMQ:WorkerRoutingKey"] ?? options.WorkerRoutingKey;
        options.ResponseExchange = builder.Configuration["RabbitMQ:ResponseExchange"] ?? options.ResponseExchange;
        options.ResponseQueue = builder.Configuration["RabbitMQ:ResponseQueue"] ?? options.ResponseQueue;
        options.ResponseRoutingKey = builder.Configuration["RabbitMQ:ResponseRoutingKey"] ?? options.ResponseRoutingKey;
        options.CorrelationIdHeader = builder.Configuration["RabbitMQ:CorrelationIdHeader"] ?? options.CorrelationIdHeader;

        if (int.TryParse(builder.Configuration["RabbitMQ:Port"], out var port))
            options.Port = port;

        ConfigureRabbitMqShutdownBudget(builder.Configuration, options);
        ConfigureRabbitMqSubscriberAckMode(builder.Configuration, "RabbitMQ:Worker", options.WorkerSubscriber);
        ConfigureRabbitMqSubscriberAckMode(builder.Configuration, "RabbitMQ:Response", options.ResponseSubscriber);
    });
}
else if (useKafka)
{
    asyncResponse.WithKafkaTransport(options =>
    {
        options.BootstrapServers = builder.Configuration.GetConnectionString("Kafka")
            ?? builder.Configuration["Kafka:BootstrapServers"]
            ?? "localhost:9092";
        options.TopicPrefix = builder.Configuration["Kafka:TopicPrefix"] ?? options.TopicPrefix;
        options.WorkerTopic = builder.Configuration["Kafka:WorkerTopic"] ?? options.WorkerTopic;
        options.WorkerConsumerGroup = builder.Configuration["Kafka:WorkerConsumerGroup"] ?? options.WorkerConsumerGroup;
        options.ResponseTopic = builder.Configuration["Kafka:ResponseTopic"] ?? options.ResponseTopic;
        options.ResponseConsumerGroup = builder.Configuration["Kafka:ResponseConsumerGroup"] ?? options.ResponseConsumerGroup;
        options.DeadLetterTopic = builder.Configuration["Kafka:DeadLetterTopic"] ?? options.DeadLetterTopic;
        options.CorrelationIdHeader = builder.Configuration["Kafka:CorrelationIdHeader"] ?? options.CorrelationIdHeader;
        options.ClientId = builder.Configuration["Kafka:ClientId"] ?? options.ClientId;

        if (bool.TryParse(builder.Configuration["Kafka:CreateTopics"], out var createTopics))
            options.CreateTopics = createTopics;
        if (bool.TryParse(builder.Configuration["Kafka:DeadLetterEnabled"], out var deadLetterEnabled))
            options.DeadLetterEnabled = deadLetterEnabled;
        if (int.TryParse(builder.Configuration["Kafka:TopicNumPartitions"], out var topicNumPartitions))
            options.TopicNumPartitions = topicNumPartitions;
        if (short.TryParse(builder.Configuration["Kafka:TopicReplicationFactor"], out var topicReplicationFactor))
            options.TopicReplicationFactor = topicReplicationFactor;
        if (int.TryParse(builder.Configuration["Kafka:PublishMaxAttempts"], out var publishMaxAttempts))
            options.PublishMaxAttempts = publishMaxAttempts;

        ApplyOptionalMilliseconds(builder.Configuration, "Kafka:OffsetCommitIntervalMs", value => options.OffsetCommitInterval = value);
        ApplyOptionalMilliseconds(builder.Configuration, "Kafka:PublishRetryBaseDelayMs", value => options.PublishRetryBaseDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "Kafka:PublishRetryMaxDelayMs", value => options.PublishRetryMaxDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "Kafka:SubscriberRetryBaseDelayMs", value => options.SubscriberRetryBaseDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "Kafka:SubscriberRetryMaxDelayMs", value => options.SubscriberRetryMaxDelay = value);

        ConfigureKafkaShutdownBudget(builder.Configuration, options);
        ConfigureKafkaSubscriber(builder.Configuration, "Kafka:Worker", options.WorkerSubscriber);
        ConfigureKafkaSubscriber(builder.Configuration, "Kafka:Response", options.ResponseSubscriber);
    });
}
else if (useRedisTransport)
{
    asyncResponse.WithRedisTransport(options =>
    {
        options.KeyPrefix = builder.Configuration["Redis:KeyPrefix"]
            ?? builder.Configuration["AsyncResponse:KeyPrefix"]
            ?? "sample";
        options.WorkerStream = builder.Configuration["Redis:WorkerStream"] ?? options.WorkerStream;
        options.WorkerConsumerGroup = builder.Configuration["Redis:WorkerConsumerGroup"] ?? options.WorkerConsumerGroup;
        options.ResponseStream = builder.Configuration["Redis:ResponseStream"] ?? options.ResponseStream;
        options.ResponseConsumerGroup = builder.Configuration["Redis:ResponseConsumerGroup"] ?? options.ResponseConsumerGroup;
        options.DeadLetterStream = builder.Configuration["Redis:DeadLetterStream"] ?? options.DeadLetterStream;
        options.ConsumerName = builder.Configuration["Redis:ConsumerName"] ?? options.ConsumerName;
        options.CorrelationIdField = builder.Configuration["Redis:CorrelationIdField"] ?? options.CorrelationIdField;
        options.PayloadField = builder.Configuration["Redis:PayloadField"] ?? options.PayloadField;

        if (bool.TryParse(builder.Configuration["Redis:CreateConsumerGroups"], out var createConsumerGroups))
            options.CreateConsumerGroups = createConsumerGroups;
        if (bool.TryParse(builder.Configuration["Redis:DeadLetterEnabled"], out var deadLetterEnabled))
            options.DeadLetterEnabled = deadLetterEnabled;
        if (bool.TryParse(builder.Configuration["Redis:UseApproximateStreamTrimming"], out var approximateTrim))
            options.UseApproximateStreamTrimming = approximateTrim;
        if (long.TryParse(builder.Configuration["Redis:StreamMaxLength"], out var streamMaxLength))
            options.StreamMaxLength = streamMaxLength;
        if (long.TryParse(builder.Configuration["Redis:DeadLetterStreamMaxLength"], out var deadLetterStreamMaxLength))
            options.DeadLetterStreamMaxLength = deadLetterStreamMaxLength;
        if (int.TryParse(builder.Configuration["Redis:PublishMaxAttempts"], out var publishMaxAttempts))
            options.PublishMaxAttempts = publishMaxAttempts;

        ApplyOptionalTimeout(builder.Configuration, "Redis:OperationTimeoutSeconds", value => options.OperationTimeout = value);
        ApplyOptionalMilliseconds(builder.Configuration, "Redis:PublishRetryBaseDelayMs", value => options.PublishRetryBaseDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "Redis:PublishRetryMaxDelayMs", value => options.PublishRetryMaxDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "Redis:SubscriberRetryBaseDelayMs", value => options.SubscriberRetryBaseDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "Redis:SubscriberRetryMaxDelayMs", value => options.SubscriberRetryMaxDelay = value);

        ConfigureRedisShutdownBudget(builder.Configuration, options);
        ConfigureRedisSubscriber(builder.Configuration, "Redis:Worker", options.WorkerSubscriber);
        ConfigureRedisSubscriber(builder.Configuration, "Redis:Response", options.ResponseSubscriber);
    });
}
else if (useNatsTransport)
{
    asyncResponse.WithNatsTransport(options =>
    {
        options.SubjectPrefix = builder.Configuration["Nats:SubjectPrefix"] ?? options.SubjectPrefix;
        options.WorkerConsumer = builder.Configuration["Nats:WorkerConsumer"] ?? options.WorkerConsumer;
        options.ResponseConsumer = builder.Configuration["Nats:ResponseConsumer"] ?? options.ResponseConsumer;
        ConfigureNatsSubscriber(builder.Configuration, "Nats:Worker", options.WorkerSubscriber);
        ConfigureNatsSubscriber(builder.Configuration, "Nats:Response", options.ResponseSubscriber);
    });
}
else if (usePostgreSqlTransport)
{
    asyncResponse.WithPostgreSqlTransport(options =>
    {
        options.SchemaName = builder.Configuration["PostgreSQL:SchemaName"] ?? options.SchemaName;
        options.MessageTable = builder.Configuration["PostgreSQL:TransportMessageTable"] ?? options.MessageTable;
        options.NotificationChannel = builder.Configuration["PostgreSQL:TransportNotificationChannel"] ?? options.NotificationChannel;
        options.WorkerQueue = builder.Configuration["PostgreSQL:WorkerQueue"] ?? options.WorkerQueue;
        options.ResponseQueue = builder.Configuration["PostgreSQL:ResponseQueue"] ?? options.ResponseQueue;
        options.DeadLetterQueue = builder.Configuration["PostgreSQL:DeadLetterQueue"] ?? options.DeadLetterQueue;
        options.CorrelationIdHeader = builder.Configuration["PostgreSQL:CorrelationIdHeader"] ?? options.CorrelationIdHeader;

        if (bool.TryParse(builder.Configuration["PostgreSQL:DeadLetterEnabled"], out var deadLetterEnabled))
            options.DeadLetterEnabled = deadLetterEnabled;
        if (int.TryParse(builder.Configuration["PostgreSQL:PublishMaxAttempts"], out var publishMaxAttempts))
            options.PublishMaxAttempts = publishMaxAttempts;

        ApplyOptionalTimeout(builder.Configuration, "PostgreSQL:LockTimeoutSeconds", value => options.LockTimeout = value);
        ApplyOptionalMilliseconds(builder.Configuration, "PostgreSQL:PublishRetryBaseDelayMs", value => options.PublishRetryBaseDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "PostgreSQL:PublishRetryMaxDelayMs", value => options.PublishRetryMaxDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "PostgreSQL:SubscriberRetryBaseDelayMs", value => options.SubscriberRetryBaseDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "PostgreSQL:SubscriberRetryMaxDelayMs", value => options.SubscriberRetryMaxDelay = value);

        ConfigurePostgreSqlSubscriber(builder.Configuration, "PostgreSQL:Worker", options.WorkerSubscriber);
        ConfigurePostgreSqlSubscriber(builder.Configuration, "PostgreSQL:Response", options.ResponseSubscriber);
    });
}
else if (useSqlServerTransport)
{
    asyncResponse.WithSqlServerTransport(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("SqlServer")
            ?? builder.Configuration["SqlServer:ConnectionString"];
        options.SchemaName = builder.Configuration["SqlServer:SchemaName"] ?? options.SchemaName;
        options.MessageTable = builder.Configuration["SqlServer:TransportMessageTable"] ?? options.MessageTable;
        options.WorkerQueue = builder.Configuration["SqlServer:WorkerQueue"] ?? options.WorkerQueue;
        options.ResponseQueue = builder.Configuration["SqlServer:ResponseQueue"] ?? options.ResponseQueue;
        options.DeadLetterQueue = builder.Configuration["SqlServer:DeadLetterQueue"] ?? options.DeadLetterQueue;
        options.CorrelationIdHeader = builder.Configuration["SqlServer:CorrelationIdHeader"] ?? options.CorrelationIdHeader;

        if (bool.TryParse(builder.Configuration["SqlServer:DeadLetterEnabled"], out var deadLetterEnabled))
            options.DeadLetterEnabled = deadLetterEnabled;
        if (int.TryParse(builder.Configuration["SqlServer:PublishMaxAttempts"], out var publishMaxAttempts))
            options.PublishMaxAttempts = publishMaxAttempts;

        ApplyOptionalTimeout(builder.Configuration, "SqlServer:LockTimeoutSeconds", value => options.LockTimeout = value);
        ApplyOptionalMilliseconds(builder.Configuration, "SqlServer:PublishRetryBaseDelayMs", value => options.PublishRetryBaseDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "SqlServer:PublishRetryMaxDelayMs", value => options.PublishRetryMaxDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "SqlServer:SubscriberRetryBaseDelayMs", value => options.SubscriberRetryBaseDelay = value);
        ApplyOptionalMilliseconds(builder.Configuration, "SqlServer:SubscriberRetryMaxDelayMs", value => options.SubscriberRetryMaxDelay = value);

        ConfigureSqlServerSubscriber(builder.Configuration, "SqlServer:Worker", options.WorkerSubscriber);
        ConfigureSqlServerSubscriber(builder.Configuration, "SqlServer:Response", options.ResponseSubscriber);
    });
}
else if (useInMemoryTransport)
{
    asyncResponse.WithInMemoryTransport();
}
else
{
    throw new InvalidOperationException(
        "Unsupported AsyncResponse:Transport value. Use 'InMemory', 'AzureServiceBus', 'GooglePubSub', 'Kafka', 'RabbitMQ', 'Redis', 'NATS', 'PostgreSQL', or 'SqlServer'.");
}

// Ambient-context propagators — trace and tenant compose, each carrying its own key across hops.
asyncResponse
    .WithContextPropagator<SampleTracePropagator>()
    .WithContextPropagator<SampleTenantPropagator>();

builder.Services.AddHealthChecks().AddAsyncResponseRecoveryCheck();

builder.Services.AddSingleton<FlowRecorder>();
builder.Services.AddSingleton<ISampleFlowService, SampleFlowService>();
builder.Services.AddSingleton<RemoteWorkSimulator>();
builder.Services.AddScoped<SampleProvisioningFlow>(); // durable flow: resolved by type name on execute/resume

var app = builder.Build();
app.Logger.LogInformation("AsyncResponse sample started: channel={Channel}, transport={Transport}.", channel, transport);
app.MapOpenApi();
app.UseSwaggerUI(options => options.SwaggerEndpoint("/openapi/v1.json", "AsyncResponse sample v1"));

app.MapGet("/", () => Results.Text(
    $"""
    AsyncResponse sample — channel: {channel}, transport: {transport}.

      GET  /swagger                                              interactive request playground
      POST /request-response?behavior=Succeed|FailDomain|Fail|Timeout   active-waiter round-trip
      POST /attach                                               wait by a known correlation id (no trigger)
      POST /multi-step?first=Succeed&second=Succeed              sequential two-step flow
      POST /ambient-exception                                    SetException via ambient correlation id
      POST /shared-correlation-exception                         one SetException faults multiple waiters
      POST /worker?token=42                                      fire-and-forget background worker job
      POST /arm  + POST /crash + POST /publish                   lost-subscriber recovery flow (durable channels)
      POST /lost-subscriber-flow?outcome=Completed|Failed|Exception  composed recovery flow (Redis/PostgreSQL)
      GET  /reply-target                                         provider-resolved reply target (broker transports)
      GET  /config                                               resolved channel/transport/ACK mode
      GET  /healthz                                              health report incl. the recovery watchdog
    """)).ExcludeFromDescription();

// Liveness probe (used by the AppHost health check) — always 200 while the process is up,
// independent of the recovery health check (which may go Degraded in the watchdog scenario).
app.MapGet("/alive", () => Results.Ok("alive")).ExcludeFromDescription();

// Reports the providers this instance resolved from configuration. The in-process tests assert
// InMemory/InMemory, and the Aspire SUTs assert Redis plus each broker transport variation, so the
// selectable providers stay exercised even though one app process boots only one pair.
app.MapGet("/config", (IServiceProvider services) =>
{
    object? pubsub = null;
    object? kafka = null;
    object? rabbitmq = null;
    object? redis = null;
    object? nats = null;
    object? postgres = null;
    object? sqlserver = null;
    object? azureServiceBus = null;
    if (useAzureServiceBus)
    {
        var serviceBusOptions = services.GetRequiredService<IOptions<AzureServiceBusAsyncResponseOptions>>().Value;
        azureServiceBus = new
        {
            workerQueue = serviceBusOptions.WorkerQueue,
            responseQueue = serviceBusOptions.ResponseQueue,
            workerAckMode = serviceBusOptions.WorkerSubscriber.AckMode.ToString(),
            workerBackgroundWorkerCount = serviceBusOptions.WorkerSubscriber.BackgroundWorkerCount,
            workerBackgroundQueueCapacity = serviceBusOptions.WorkerSubscriber.BackgroundQueueCapacity,
            workerMaxDeliveryAttempts = serviceBusOptions.WorkerSubscriber.MaxDeliveryAttempts,
            responseAckMode = serviceBusOptions.ResponseSubscriber.AckMode.ToString(),
            responseBackgroundWorkerCount = serviceBusOptions.ResponseSubscriber.BackgroundWorkerCount,
            responseBackgroundQueueCapacity = serviceBusOptions.ResponseSubscriber.BackgroundQueueCapacity,
            responseMaxDeliveryAttempts = serviceBusOptions.ResponseSubscriber.MaxDeliveryAttempts
        };
    }

    if (useGooglePubSub)
    {
        var googleOptions = services.GetRequiredService<IOptions<GooglePubSubAsyncResponseOptions>>().Value;
        pubsub = new
        {
            workerAckMode = googleOptions.WorkerSubscriber.AckMode.ToString(),
            workerBackgroundWorkerCount = googleOptions.WorkerSubscriber.BackgroundWorkerCount,
            workerBackgroundQueueCapacity = googleOptions.WorkerSubscriber.BackgroundQueueCapacity,
            responseAckMode = googleOptions.ResponseSubscriber.AckMode.ToString(),
            responseBackgroundWorkerCount = googleOptions.ResponseSubscriber.BackgroundWorkerCount,
            responseBackgroundQueueCapacity = googleOptions.ResponseSubscriber.BackgroundQueueCapacity
        };
    }

    if (useKafka)
    {
        var kafkaOptions = services.GetRequiredService<IOptions<KafkaAsyncResponseTransportOptions>>().Value;
        var workerTopic = ResolveKafkaTopic(kafkaOptions.WorkerTopic, kafkaOptions.TopicPrefix, "worker");
        var responseTopic = ResolveKafkaTopic(kafkaOptions.ResponseTopic, kafkaOptions.TopicPrefix, "response");
        kafka = new
        {
            workerTopic,
            workerConsumerGroup = kafkaOptions.WorkerConsumerGroup,
            responseTopic,
            responseConsumerGroup = kafkaOptions.ResponseConsumerGroup,
            deadLetterTopic = kafkaOptions.DeadLetterEnabled
                ? !string.IsNullOrWhiteSpace(kafkaOptions.DeadLetterTopic)
                    ? kafkaOptions.DeadLetterTopic
                    : workerTopic + kafkaOptions.DeadLetterTopicSuffix
                : null,
            workerAckMode = kafkaOptions.WorkerSubscriber.AckMode.ToString(),
            workerBackgroundWorkerCount = kafkaOptions.WorkerSubscriber.BackgroundWorkerCount,
            workerBackgroundQueueCapacity = kafkaOptions.WorkerSubscriber.BackgroundQueueCapacity,
            workerMaxDeliveryAttempts = kafkaOptions.WorkerSubscriber.MaxDeliveryAttempts,
            responseAckMode = kafkaOptions.ResponseSubscriber.AckMode.ToString(),
            responseBackgroundWorkerCount = kafkaOptions.ResponseSubscriber.BackgroundWorkerCount,
            responseBackgroundQueueCapacity = kafkaOptions.ResponseSubscriber.BackgroundQueueCapacity,
            responseMaxDeliveryAttempts = kafkaOptions.ResponseSubscriber.MaxDeliveryAttempts
        };
    }

    if (useRabbitMq)
    {
        var rabbitOptions = services.GetRequiredService<IOptions<RabbitMqAsyncResponseOptions>>().Value;
        rabbitmq = new
        {
            workerExchange = rabbitOptions.WorkerExchange,
            workerQueue = rabbitOptions.WorkerQueue,
            workerRoutingKey = rabbitOptions.WorkerRoutingKey,
            responseExchange = rabbitOptions.ResponseExchange,
            responseQueue = rabbitOptions.ResponseQueue,
            responseRoutingKey = rabbitOptions.ResponseRoutingKey,
            workerAckMode = rabbitOptions.WorkerSubscriber.AckMode.ToString(),
            workerBackgroundWorkerCount = rabbitOptions.WorkerSubscriber.BackgroundWorkerCount,
            workerBackgroundQueueCapacity = rabbitOptions.WorkerSubscriber.BackgroundQueueCapacity,
            responseAckMode = rabbitOptions.ResponseSubscriber.AckMode.ToString(),
            responseBackgroundWorkerCount = rabbitOptions.ResponseSubscriber.BackgroundWorkerCount,
            responseBackgroundQueueCapacity = rabbitOptions.ResponseSubscriber.BackgroundQueueCapacity
        };
    }

    if (useRedisTransport)
    {
        var redisOptions = services.GetRequiredService<IOptions<RedisAsyncResponseTransportOptions>>().Value;
        var workerStream = ResolveRedisStream(redisOptions.WorkerStream, redisOptions.KeyPrefix, "worker");
        var responseStream = ResolveRedisStream(redisOptions.ResponseStream, redisOptions.KeyPrefix, "response");
        var deadLetterStream = ResolveRedisStream(redisOptions.DeadLetterStream, redisOptions.KeyPrefix, "deadletter");
        redis = new
        {
            workerStream,
            workerConsumerGroup = redisOptions.WorkerConsumerGroup,
            responseStream,
            responseConsumerGroup = redisOptions.ResponseConsumerGroup,
            deadLetterStream = redisOptions.DeadLetterEnabled ? deadLetterStream : null,
            workerAckMode = redisOptions.WorkerSubscriber.AckMode.ToString(),
            workerBackgroundWorkerCount = redisOptions.WorkerSubscriber.BackgroundWorkerCount,
            workerBackgroundQueueCapacity = redisOptions.WorkerSubscriber.BackgroundQueueCapacity,
            workerMaxDeliveryAttempts = redisOptions.WorkerSubscriber.MaxDeliveryAttempts,
            responseAckMode = redisOptions.ResponseSubscriber.AckMode.ToString(),
            responseBackgroundWorkerCount = redisOptions.ResponseSubscriber.BackgroundWorkerCount,
            responseBackgroundQueueCapacity = redisOptions.ResponseSubscriber.BackgroundQueueCapacity,
            responseMaxDeliveryAttempts = redisOptions.ResponseSubscriber.MaxDeliveryAttempts
        };
    }

    if (useNats || useNatsTransport)
    {
        string? subjectPrefix = null;
        string? recoveryBucket = null;
        string? workerSubject = null;
        string? responseSubject = null;
        var workerAckMode = "n/a";
        var responseAckMode = "n/a";
        var workerBackgroundWorkerCount = 0;
        var workerBackgroundQueueCapacity = 0;

        if (useNatsTransport)
        {
            var natsOptions = services.GetRequiredService<IOptions<NatsAsyncResponseTransportOptions>>().Value;
            subjectPrefix = natsOptions.SubjectPrefix;
            workerSubject = string.IsNullOrWhiteSpace(natsOptions.WorkerSubject) ? $"{natsOptions.SubjectPrefix}.transport.worker" : natsOptions.WorkerSubject;
            responseSubject = string.IsNullOrWhiteSpace(natsOptions.ResponseSubject) ? $"{natsOptions.SubjectPrefix}.transport.response" : natsOptions.ResponseSubject;
            workerAckMode = natsOptions.WorkerSubscriber.AckMode.ToString();
            workerBackgroundWorkerCount = natsOptions.WorkerSubscriber.BackgroundWorkerCount;
            workerBackgroundQueueCapacity = natsOptions.WorkerSubscriber.BackgroundQueueCapacity;
            responseAckMode = natsOptions.ResponseSubscriber.AckMode.ToString();
        }

        if (useNats)
        {
            var natsChannelOptions = services.GetRequiredService<IOptions<NatsAsyncResponseChannelOptions>>().Value;
            recoveryBucket = natsChannelOptions.RecoveryBucket;
            subjectPrefix ??= natsChannelOptions.SubjectPrefix;
        }

        nats = new
        {
            subjectPrefix,
            recoveryBucket,
            workerSubject,
            responseSubject,
            workerAckMode,
            workerBackgroundWorkerCount,
            workerBackgroundQueueCapacity,
            responseAckMode
        };
    }

    if (usePostgreSql || usePostgreSqlTransport)
    {
        string? schemaName = null;
        string? recoveryStateTable = null;
        string? channelMessageTable = null;
        string? transportMessageTable = null;
        string? workerQueue = null;
        string? responseQueue = null;
        var workerAckMode = "n/a";
        var responseAckMode = "n/a";
        var workerBackgroundWorkerCount = 0;
        var workerBackgroundQueueCapacity = 0;

        if (usePostgreSql)
        {
            var channelOptions = services.GetRequiredService<IOptions<PostgreSqlAsyncResponseChannelOptions>>().Value;
            schemaName = channelOptions.SchemaName;
            recoveryStateTable = channelOptions.RecoveryStateTable;
            channelMessageTable = channelOptions.MessageTable;
        }

        if (usePostgreSqlTransport)
        {
            var transportOptions = services.GetRequiredService<IOptions<PostgreSqlAsyncResponseTransportOptions>>().Value;
            schemaName ??= transportOptions.SchemaName;
            transportMessageTable = transportOptions.MessageTable;
            workerQueue = transportOptions.WorkerQueue;
            responseQueue = transportOptions.ResponseQueue;
            workerAckMode = transportOptions.WorkerSubscriber.AckMode.ToString();
            workerBackgroundWorkerCount = transportOptions.WorkerSubscriber.BackgroundWorkerCount;
            workerBackgroundQueueCapacity = transportOptions.WorkerSubscriber.BackgroundQueueCapacity;
            responseAckMode = transportOptions.ResponseSubscriber.AckMode.ToString();
        }

        postgres = new
        {
            schemaName,
            recoveryStateTable,
            channelMessageTable,
            transportMessageTable,
            workerQueue,
            responseQueue,
            workerAckMode,
            workerBackgroundWorkerCount,
            workerBackgroundQueueCapacity,
            responseAckMode
        };
    }

    if (useSqlServer || useSqlServerTransport)
    {
        string? schemaName = null;
        string? recoveryStateTable = null;
        string? channelMessageTable = null;
        string? transportMessageTable = null;
        string? workerQueue = null;
        string? responseQueue = null;
        var workerAckMode = "n/a";
        var responseAckMode = "n/a";
        var workerBackgroundWorkerCount = 0;
        var workerBackgroundQueueCapacity = 0;

        if (useSqlServer)
        {
            var channelOptions = services.GetRequiredService<IOptions<SqlServerAsyncResponseChannelOptions>>().Value;
            schemaName = channelOptions.SchemaName;
            recoveryStateTable = channelOptions.RecoveryStateTable;
            channelMessageTable = channelOptions.MessageTable;
        }

        if (useSqlServerTransport)
        {
            var transportOptions = services.GetRequiredService<IOptions<SqlServerAsyncResponseTransportOptions>>().Value;
            schemaName ??= transportOptions.SchemaName;
            transportMessageTable = transportOptions.MessageTable;
            workerQueue = transportOptions.WorkerQueue;
            responseQueue = transportOptions.ResponseQueue;
            workerAckMode = transportOptions.WorkerSubscriber.AckMode.ToString();
            workerBackgroundWorkerCount = transportOptions.WorkerSubscriber.BackgroundWorkerCount;
            workerBackgroundQueueCapacity = transportOptions.WorkerSubscriber.BackgroundQueueCapacity;
            responseAckMode = transportOptions.ResponseSubscriber.AckMode.ToString();
        }

        sqlserver = new
        {
            schemaName,
            recoveryStateTable,
            channelMessageTable,
            transportMessageTable,
            workerQueue,
            responseQueue,
            workerAckMode,
            workerBackgroundWorkerCount,
            workerBackgroundQueueCapacity,
            responseAckMode
        };
    }

    return Results.Ok(new { channel, transport, azureServiceBus, pubsub, kafka, rabbitmq, redis, nats, postgres, sqlserver });
}).WithTags("Observability");

static string NormalizeBehavior(string? behavior)
    => (behavior ?? "Succeed").Trim().ToLowerInvariant() switch
    {
        "fail" => "fail",
        "failtechnical" => "fail",
        "technical" => "fail",
        "faildomain" => "faildomain",
        "domain" => "faildomain",
        "timeout" => "timeout",
        _ => "succeed"
    };

static void ConfigurePubSubShutdownBudget(
    IConfiguration configuration,
    GooglePubSubAsyncResponseOptions options)
{
    var timeout = ReadOptionalPositiveTimeout(configuration, "PubSub:HostShutdownTimeoutSeconds");
    if (timeout is not null)
        options.HostShutdownTimeout = timeout.Value;
}

static void ConfigureRabbitMqShutdownBudget(
    IConfiguration configuration,
    RabbitMqAsyncResponseOptions options)
{
    var timeout = ReadOptionalPositiveTimeout(configuration, "RabbitMQ:HostShutdownTimeoutSeconds");
    if (timeout is not null)
        options.HostShutdownTimeout = timeout.Value;
}

static void ConfigureRedisShutdownBudget(
    IConfiguration configuration,
    RedisAsyncResponseTransportOptions options)
{
    var timeout = ReadOptionalPositiveTimeout(configuration, "Redis:HostShutdownTimeoutSeconds");
    if (timeout is not null)
        options.HostShutdownTimeout = timeout.Value;
}

static void ConfigureAzureServiceBusShutdownBudget(
    IConfiguration configuration,
    AzureServiceBusAsyncResponseOptions options)
{
    var timeout = ReadOptionalPositiveTimeout(configuration, "AzureServiceBus:HostShutdownTimeoutSeconds");
    if (timeout is not null)
        options.HostShutdownTimeout = timeout.Value;
}

static void ConfigureHostShutdownBudget(
    IConfiguration configuration,
    IServiceCollection services,
    string prefix = "PubSub")
{
    var timeout = ReadOptionalPositiveTimeout(configuration, $"{prefix}:HostShutdownTimeoutSeconds");
    if (timeout is not null)
        services.Configure<HostOptions>(options => options.ShutdownTimeout = timeout.Value);
}

static TimeSpan? ReadOptionalPositiveTimeout(IConfiguration configuration, string key)
{
    var rawValue = configuration[key];
    if (string.IsNullOrWhiteSpace(rawValue))
        return null;

    if (!int.TryParse(rawValue, out var seconds) || seconds <= 0)
        throw new InvalidOperationException($"{key} must be a positive integer when set.");

    return TimeSpan.FromSeconds(seconds);
}

static void ApplyOptionalTimeout(IConfiguration configuration, string key, Action<TimeSpan> apply)
{
    var timeout = ReadOptionalPositiveTimeout(configuration, key);
    if (timeout is not null)
        apply(timeout.Value);
}

static void ApplyOptionalMilliseconds(IConfiguration configuration, string key, Action<TimeSpan> apply)
{
    var rawValue = configuration[key];
    if (string.IsNullOrWhiteSpace(rawValue))
        return;

    if (!int.TryParse(rawValue, out var milliseconds) || milliseconds <= 0)
        throw new InvalidOperationException($"{key} must be a positive integer when set.");

    apply(TimeSpan.FromMilliseconds(milliseconds));
}

static void ConfigureAzureServiceBusSubscriber(
    IConfiguration configuration,
    string prefix,
    AzureServiceBusSubscriberOptions subscriberOptions)
{
    if (int.TryParse(configuration[$"{prefix}:MaxDeliveryAttempts"], out var maxDeliveryAttempts))
        subscriberOptions.MaxDeliveryAttempts = maxDeliveryAttempts;
    if (int.TryParse(configuration[$"{prefix}:PrefetchCount"], out var prefetchCount))
        subscriberOptions.PrefetchCount = prefetchCount;

    var rawMode = configuration[$"{prefix}:AckMode"];
    if (string.IsNullOrWhiteSpace(rawMode))
        return;

    if (!Enum.TryParse<AzureServiceBusAckMode>(rawMode, ignoreCase: true, out var mode))
        throw new InvalidOperationException($"{prefix}:AckMode must be one of: {string.Join(", ", Enum.GetNames<AzureServiceBusAckMode>())}.");

    if (mode is AzureServiceBusAckMode.AckAfterHandlerCompletes)
    {
        subscriberOptions.AckMode = AzureServiceBusAckMode.AckAfterHandlerCompletes;
        return;
    }

    if (mode is not AzureServiceBusAckMode.AckAfterReceive)
        throw new InvalidOperationException($"{prefix}:AckMode has unsupported value '{rawMode}'.");

    var workerCount = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundWorkerCount");
    var queueCapacity = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundQueueCapacity");
    var drainTimeoutSeconds = configuration[$"{prefix}:BackgroundDrainTimeoutSeconds"];
    TimeSpan? drainTimeout = null;
    if (!string.IsNullOrWhiteSpace(drainTimeoutSeconds))
    {
        if (!int.TryParse(drainTimeoutSeconds, out var seconds) || seconds <= 0)
            throw new InvalidOperationException($"{prefix}:BackgroundDrainTimeoutSeconds must be a positive integer when set.");

        drainTimeout = TimeSpan.FromSeconds(seconds);
    }

    subscriberOptions.UseAckAfterReceive(workerCount, queueCapacity, drainTimeout);
}

static void ConfigurePubSubSubscriberAckMode(
    IConfiguration configuration,
    string prefix,
    GooglePubSubSubscriberOptions subscriberOptions)
{
    var rawMode = configuration[$"{prefix}:AckMode"];
    if (string.IsNullOrWhiteSpace(rawMode))
        return;

    if (!Enum.TryParse<GooglePubSubAckMode>(rawMode, ignoreCase: true, out var mode))
        throw new InvalidOperationException($"{prefix}:AckMode must be one of: {string.Join(", ", Enum.GetNames<GooglePubSubAckMode>())}.");

    if (mode is GooglePubSubAckMode.AckAfterHandlerCompletes)
    {
        subscriberOptions.AckMode = GooglePubSubAckMode.AckAfterHandlerCompletes;
        return;
    }

    if (mode is not GooglePubSubAckMode.AckAfterEnqueue)
        throw new InvalidOperationException($"{prefix}:AckMode has unsupported value '{rawMode}'.");

    var workerCount = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundWorkerCount");
    var queueCapacity = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundQueueCapacity");
    var drainTimeoutSeconds = configuration[$"{prefix}:BackgroundDrainTimeoutSeconds"];
    TimeSpan? drainTimeout = null;
    if (!string.IsNullOrWhiteSpace(drainTimeoutSeconds))
    {
        if (!int.TryParse(drainTimeoutSeconds, out var seconds) || seconds <= 0)
            throw new InvalidOperationException($"{prefix}:BackgroundDrainTimeoutSeconds must be a positive integer when set.");

        drainTimeout = TimeSpan.FromSeconds(seconds);
    }

    subscriberOptions.UseAckAfterEnqueue(workerCount, queueCapacity, drainTimeout);
}

static void ConfigureKafkaShutdownBudget(
    IConfiguration configuration,
    KafkaAsyncResponseTransportOptions options)
{
    var timeout = ReadOptionalPositiveTimeout(configuration, "Kafka:HostShutdownTimeoutSeconds");
    if (timeout is not null)
        options.HostShutdownTimeout = timeout.Value;
}

static void ConfigureKafkaSubscriber(
    IConfiguration configuration,
    string prefix,
    KafkaSubscriberOptions subscriberOptions)
{
    if (int.TryParse(configuration[$"{prefix}:MaxDeliveryAttempts"], out var maxDeliveryAttempts))
        subscriberOptions.MaxDeliveryAttempts = maxDeliveryAttempts;

    ApplyOptionalMilliseconds(configuration, $"{prefix}:PollTimeoutMs", value => subscriberOptions.PollTimeout = value);
    ApplyOptionalMilliseconds(configuration, $"{prefix}:BackpressurePollDelayMs", value => subscriberOptions.BackpressurePollDelay = value);
    ApplyOptionalMilliseconds(configuration, $"{prefix}:HandlerRetryBaseDelayMs", value => subscriberOptions.HandlerRetryBaseDelay = value);
    ApplyOptionalMilliseconds(configuration, $"{prefix}:HandlerRetryMaxDelayMs", value => subscriberOptions.HandlerRetryMaxDelay = value);

    var rawMode = configuration[$"{prefix}:AckMode"];
    if (string.IsNullOrWhiteSpace(rawMode))
        return;

    if (!Enum.TryParse<KafkaAckMode>(rawMode, ignoreCase: true, out var mode))
        throw new InvalidOperationException($"{prefix}:AckMode must be one of: {string.Join(", ", Enum.GetNames<KafkaAckMode>())}.");

    if (mode is KafkaAckMode.AckAfterHandlerCompletes)
    {
        subscriberOptions.AckMode = KafkaAckMode.AckAfterHandlerCompletes;
        return;
    }

    if (mode is not KafkaAckMode.AckAfterEnqueue)
        throw new InvalidOperationException($"{prefix}:AckMode has unsupported value '{rawMode}'.");

    var workerCount = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundWorkerCount");
    var queueCapacity = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundQueueCapacity");
    var drainTimeoutSeconds = configuration[$"{prefix}:BackgroundDrainTimeoutSeconds"];
    TimeSpan? drainTimeout = null;
    if (!string.IsNullOrWhiteSpace(drainTimeoutSeconds))
    {
        if (!int.TryParse(drainTimeoutSeconds, out var seconds) || seconds <= 0)
            throw new InvalidOperationException($"{prefix}:BackgroundDrainTimeoutSeconds must be a positive integer when set.");

        drainTimeout = TimeSpan.FromSeconds(seconds);
    }

    subscriberOptions.UseAckAfterEnqueue(workerCount, queueCapacity, drainTimeout);
}

static void ConfigureRabbitMqSubscriberAckMode(
    IConfiguration configuration,
    string prefix,
    RabbitMqSubscriberOptions subscriberOptions)
{
    var rawMode = configuration[$"{prefix}:AckMode"];
    if (string.IsNullOrWhiteSpace(rawMode))
        return;

    if (!Enum.TryParse<RabbitMqAckMode>(rawMode, ignoreCase: true, out var mode))
        throw new InvalidOperationException($"{prefix}:AckMode must be one of: {string.Join(", ", Enum.GetNames<RabbitMqAckMode>())}.");

    if (mode is RabbitMqAckMode.AckAfterHandlerCompletes)
    {
        subscriberOptions.AckMode = RabbitMqAckMode.AckAfterHandlerCompletes;
        return;
    }

    if (mode is not RabbitMqAckMode.AckAfterEnqueue)
        throw new InvalidOperationException($"{prefix}:AckMode has unsupported value '{rawMode}'.");

    var workerCount = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundWorkerCount");
    var queueCapacity = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundQueueCapacity");
    var drainTimeoutSeconds = configuration[$"{prefix}:BackgroundDrainTimeoutSeconds"];
    TimeSpan? drainTimeout = null;
    if (!string.IsNullOrWhiteSpace(drainTimeoutSeconds))
    {
        if (!int.TryParse(drainTimeoutSeconds, out var seconds) || seconds <= 0)
            throw new InvalidOperationException($"{prefix}:BackgroundDrainTimeoutSeconds must be a positive integer when set.");

        drainTimeout = TimeSpan.FromSeconds(seconds);
    }

    subscriberOptions.UseAckAfterEnqueue(workerCount, queueCapacity, drainTimeout);
}

static void ConfigureRedisSubscriber(
    IConfiguration configuration,
    string prefix,
    RedisSubscriberOptions subscriberOptions)
{
    if (int.TryParse(configuration[$"{prefix}:BatchSize"], out var batchSize))
        subscriberOptions.BatchSize = batchSize;
    if (int.TryParse(configuration[$"{prefix}:PendingClaimBatchSize"], out var pendingBatchSize))
        subscriberOptions.PendingClaimBatchSize = pendingBatchSize;
    if (int.TryParse(configuration[$"{prefix}:MaxDeliveryAttempts"], out var maxDeliveryAttempts))
        subscriberOptions.MaxDeliveryAttempts = maxDeliveryAttempts;

    ApplyOptionalMilliseconds(configuration, $"{prefix}:EmptyPollDelayMs", value => subscriberOptions.EmptyPollDelay = value);
    ApplyOptionalTimeout(configuration, $"{prefix}:PendingMessageMinIdleTimeSeconds", value => subscriberOptions.PendingMessageMinIdleTime = value);
    ApplyOptionalTimeout(configuration, $"{prefix}:PendingClaimIntervalSeconds", value => subscriberOptions.PendingClaimInterval = value);

    var rawMode = configuration[$"{prefix}:AckMode"];
    if (string.IsNullOrWhiteSpace(rawMode))
        return;

    if (!Enum.TryParse<RedisAckMode>(rawMode, ignoreCase: true, out var mode))
        throw new InvalidOperationException($"{prefix}:AckMode must be one of: {string.Join(", ", Enum.GetNames<RedisAckMode>())}.");

    if (mode is RedisAckMode.AckAfterHandlerCompletes)
    {
        subscriberOptions.AckMode = RedisAckMode.AckAfterHandlerCompletes;
        return;
    }

    if (mode is not RedisAckMode.AckAfterEnqueue)
        throw new InvalidOperationException($"{prefix}:AckMode has unsupported value '{rawMode}'.");

    var workerCount = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundWorkerCount");
    var queueCapacity = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundQueueCapacity");
    var drainTimeoutSeconds = configuration[$"{prefix}:BackgroundDrainTimeoutSeconds"];
    TimeSpan? drainTimeout = null;
    if (!string.IsNullOrWhiteSpace(drainTimeoutSeconds))
    {
        if (!int.TryParse(drainTimeoutSeconds, out var seconds) || seconds <= 0)
            throw new InvalidOperationException($"{prefix}:BackgroundDrainTimeoutSeconds must be a positive integer when set.");

        drainTimeout = TimeSpan.FromSeconds(seconds);
    }

    subscriberOptions.UseAckAfterEnqueue(workerCount, queueCapacity, drainTimeout);
}

static void ConfigureNatsSubscriber(
    IConfiguration configuration,
    string prefix,
    NatsSubscriberOptions subscriberOptions)
{
    if (int.TryParse(configuration[$"{prefix}:BatchSize"], out var batchSize))
        subscriberOptions.BatchSize = batchSize;
    if (int.TryParse(configuration[$"{prefix}:MaxDeliveryAttempts"], out var maxDeliveryAttempts))
        subscriberOptions.MaxDeliveryAttempts = maxDeliveryAttempts;

    var rawMode = configuration[$"{prefix}:AckMode"];
    if (string.IsNullOrWhiteSpace(rawMode))
        return;

    if (!Enum.TryParse<NatsAckMode>(rawMode, ignoreCase: true, out var mode))
        throw new InvalidOperationException($"{prefix}:AckMode must be one of: {string.Join(", ", Enum.GetNames<NatsAckMode>())}.");

    if (mode is NatsAckMode.AckAfterHandlerCompletes)
    {
        subscriberOptions.AckMode = NatsAckMode.AckAfterHandlerCompletes;
        return;
    }

    if (mode is not NatsAckMode.AckAfterReceive)
        throw new InvalidOperationException($"{prefix}:AckMode has unsupported value '{rawMode}'.");

    var workerCount = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundWorkerCount");
    var queueCapacity = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundQueueCapacity");
    var drainTimeoutSeconds = configuration[$"{prefix}:BackgroundDrainTimeoutSeconds"];
    TimeSpan? drainTimeout = null;
    if (!string.IsNullOrWhiteSpace(drainTimeoutSeconds))
    {
        if (!int.TryParse(drainTimeoutSeconds, out var seconds) || seconds <= 0)
            throw new InvalidOperationException($"{prefix}:BackgroundDrainTimeoutSeconds must be a positive integer when set.");

        drainTimeout = TimeSpan.FromSeconds(seconds);
    }

    subscriberOptions.UseAckAfterReceive(workerCount, queueCapacity, drainTimeout);
}

static void ConfigureSqlServerSubscriber(
    IConfiguration configuration,
    string prefix,
    SqlServerSubscriberOptions subscriberOptions)
{
    if (int.TryParse(configuration[$"{prefix}:BatchSize"], out var batchSize))
        subscriberOptions.BatchSize = batchSize;
    if (int.TryParse(configuration[$"{prefix}:MaxDeliveryAttempts"], out var maxDeliveryAttempts))
        subscriberOptions.MaxDeliveryAttempts = maxDeliveryAttempts;

    ApplyOptionalMilliseconds(configuration, $"{prefix}:EmptyPollDelayMs", value => subscriberOptions.EmptyPollDelay = value);

    var rawMode = configuration[$"{prefix}:AckMode"];
    if (string.IsNullOrWhiteSpace(rawMode))
        return;

    if (!Enum.TryParse<SqlServerAckMode>(rawMode, ignoreCase: true, out var mode))
        throw new InvalidOperationException($"{prefix}:AckMode must be one of: {string.Join(", ", Enum.GetNames<SqlServerAckMode>())}.");

    if (mode is SqlServerAckMode.AckAfterHandlerCompletes)
    {
        subscriberOptions.AckMode = SqlServerAckMode.AckAfterHandlerCompletes;
        return;
    }

    if (mode is not SqlServerAckMode.AckAfterEnqueue)
        throw new InvalidOperationException($"{prefix}:AckMode has unsupported value '{rawMode}'.");

    var workerCount = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundWorkerCount");
    var queueCapacity = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundQueueCapacity");
    var drainTimeoutSeconds = configuration[$"{prefix}:BackgroundDrainTimeoutSeconds"];
    TimeSpan? drainTimeout = null;
    if (!string.IsNullOrWhiteSpace(drainTimeoutSeconds))
    {
        if (!int.TryParse(drainTimeoutSeconds, out var seconds) || seconds <= 0)
            throw new InvalidOperationException($"{prefix}:BackgroundDrainTimeoutSeconds must be a positive integer when set.");

        drainTimeout = TimeSpan.FromSeconds(seconds);
    }

    subscriberOptions.UseAckAfterEnqueue(workerCount, queueCapacity, drainTimeout);
}

static void ConfigurePostgreSqlSubscriber(
    IConfiguration configuration,
    string prefix,
    PostgreSqlSubscriberOptions subscriberOptions)
{
    if (int.TryParse(configuration[$"{prefix}:BatchSize"], out var batchSize))
        subscriberOptions.BatchSize = batchSize;
    if (int.TryParse(configuration[$"{prefix}:MaxDeliveryAttempts"], out var maxDeliveryAttempts))
        subscriberOptions.MaxDeliveryAttempts = maxDeliveryAttempts;

    ApplyOptionalMilliseconds(configuration, $"{prefix}:EmptyPollDelayMs", value => subscriberOptions.EmptyPollDelay = value);

    var rawMode = configuration[$"{prefix}:AckMode"];
    if (string.IsNullOrWhiteSpace(rawMode))
        return;

    if (!Enum.TryParse<PostgreSqlAckMode>(rawMode, ignoreCase: true, out var mode))
        throw new InvalidOperationException($"{prefix}:AckMode must be one of: {string.Join(", ", Enum.GetNames<PostgreSqlAckMode>())}.");

    if (mode is PostgreSqlAckMode.AckAfterHandlerCompletes)
    {
        subscriberOptions.AckMode = PostgreSqlAckMode.AckAfterHandlerCompletes;
        return;
    }

    if (mode is not PostgreSqlAckMode.AckAfterReceive)
        throw new InvalidOperationException($"{prefix}:AckMode has unsupported value '{rawMode}'.");

    var workerCount = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundWorkerCount");
    var queueCapacity = ReadRequiredPositiveInt(configuration, $"{prefix}:BackgroundQueueCapacity");
    var drainTimeoutSeconds = configuration[$"{prefix}:BackgroundDrainTimeoutSeconds"];
    TimeSpan? drainTimeout = null;
    if (!string.IsNullOrWhiteSpace(drainTimeoutSeconds))
    {
        if (!int.TryParse(drainTimeoutSeconds, out var seconds) || seconds <= 0)
            throw new InvalidOperationException($"{prefix}:BackgroundDrainTimeoutSeconds must be a positive integer when set.");

        drainTimeout = TimeSpan.FromSeconds(seconds);
    }

    subscriberOptions.UseAckAfterReceive(workerCount, queueCapacity, drainTimeout);
}

static string ResolveRedisStream(string? configured, string keyPrefix, string role)
    => !string.IsNullOrWhiteSpace(configured)
        ? configured
        : $"{keyPrefix}:transport:{role}";

static string ResolveKafkaTopic(string? configured, string topicPrefix, string role)
    => !string.IsNullOrWhiteSpace(configured)
        ? configured
        : $"{topicPrefix}.transport.{role}";

static int ReadRequiredPositiveInt(IConfiguration configuration, string key)
{
    var rawValue = configuration[key];
    if (!int.TryParse(rawValue, out var value) || value <= 0)
        throw new InvalidOperationException($"{key} must be explicitly set to a positive integer.");

    return value;
}

static async Task<StepOutcome> RunStepAsync(
    IAsyncResponseBuilder asyncResponse,
    IAsyncResponsePublisher publisher,
    RemoteWorkSimulator remote,
    FlowRecorder recorder,
    string stepName,
    string? behavior)
{
    var kind = NormalizeBehavior(behavior);
    string? correlationId = null;

    try
    {
        var result = await asyncResponse
            .For<OperationResult>()
            .WithTimeout(kind == "timeout" ? TimeSpan.FromSeconds(2) : TimeSpan.FromSeconds(30))
            .Until(response => response.Status != OperationStatus.Running)
            .WaitAsync(context =>
            {
                correlationId = context.CorrelationId;
                return kind switch
                {
                    "faildomain" => StartRemoteAsync(remote, context.CorrelationId, RemoteBehavior.FailDomain),
                    "fail" => publisher.SetException(new InvalidOperationException($"{stepName} technical error"), context.CorrelationId),
                    "timeout" => StartRemoteAsync(remote, context.CorrelationId, RemoteBehavior.Slow),
                    _ => StartRemoteAsync(remote, context.CorrelationId, RemoteBehavior.Succeed)
                };
            });

        var succeeded = result.Status == OperationStatus.Completed;
        recorder.Record($"multi:{correlationId}", new FlowCall(
            $"step-{stepName}",
            correlationId,
            SampleTraceContext.Current,
            SampleTenantContext.Current,
            result.Status,
            result.Message));

        return new StepOutcome(
            stepName,
            correlationId!,
            succeeded,
            result.Status,
            result.Message,
            null,
            succeeded ? null : result.Message);
    }
    catch (Exception ex)
    {
        recorder.Record($"multi:{correlationId}", new FlowCall(
            $"step-{stepName}-faulted",
            correlationId,
            SampleTraceContext.Current,
            SampleTenantContext.Current,
            null,
            ex.Message));

        return new StepOutcome(
            stepName,
            correlationId ?? string.Empty,
            false,
            null,
            null,
            ex.GetType().Name,
            ex.Message);
    }
}

static Task StartRemoteAsync(RemoteWorkSimulator remote, string correlationId, RemoteBehavior behavior)
{
    remote.Start(correlationId, behavior);
    return Task.CompletedTask;
}

static async Task<string> CaptureFailureAsync(Task<OperationResult> task)
{
    try
    {
        var result = await task.ConfigureAwait(false);
        return $"completed:{result.Status}";
    }
    catch (Exception ex)
    {
        return $"{ex.GetType().Name}: {ex.Message}";
    }
}

// 1) Request/response with an active waiter and a selectable terminal behavior. For<T>() generates
//    the correlation id; the required WaitAsync trigger guarantees subscribe-before-send.
app.MapPost("/request-response", async (
    IAsyncResponseBuilder asyncResponse, IAsyncResponsePublisher publisher, RemoteWorkSimulator remote,
    string? behavior, string? trace, ILoggerFactory loggerFactory) =>
{
    var logger = loggerFactory.CreateLogger("RequestResponse");
    var kind = (behavior ?? "Succeed").ToLowerInvariant();

    // Per-request ambient context flows into the response handler (a subscriber-thread callback) via
    // the captured ExecutionContext, so the HANDLER log lines carry it.
    SampleTraceContext.Set(trace ?? $"trace-{Guid.NewGuid().ToString("N")[..8]}");
    SampleTenantContext.Set("tenant-acme");

    try
    {
        var result = await asyncResponse
            .For<OperationResult>()
            .WithTimeout(kind == "timeout" ? TimeSpan.FromSeconds(2) : TimeSpan.FromSeconds(30))
            .Until(response =>
            {
                logger.LogInformation("HANDLER: progress {Status} (traceId: {TraceId}, tenant: {Tenant})",
                    response.Status, SampleTraceContext.Current, SampleTenantContext.Current);
                return response.Status != OperationStatus.Running; // consume progress messages
            })
            .WaitAsync(async context =>
            {
                switch (kind)
                {
                    case "faildomain":
                        remote.Start(context.CorrelationId, RemoteBehavior.FailDomain);
                        break;
                    case "fail":
                        await publisher.SetException(new InvalidOperationException("remote technical error"), context.CorrelationId);
                        break;
                    case "timeout":
                        remote.Start(context.CorrelationId, RemoteBehavior.Slow); // never terminal in time
                        break;
                    default:
                        remote.Start(context.CorrelationId, RemoteBehavior.Succeed);
                        break;
                }
            });

        return Results.Ok(new { result.Status, result.Message });
    }
    catch (TimeoutException)
    {
        return Results.StatusCode(StatusCodes.Status504GatewayTimeout);
    }
    catch (Exception ex)
    {
        return Results.Problem(detail: ex.Message, statusCode: StatusCodes.Status500InternalServerError);
    }
})
.WithTags("Request/response");

// 2) Attach to an in-flight operation by a known correlation id. For<T>(correlationId) is wait-only
//    (no trigger): the operation was started elsewhere; here we just await its response.
app.MapPost("/attach", async (IAsyncResponseBuilder asyncResponse, RemoteWorkSimulator remote) =>
{
    var correlationId = AsyncResponseContext.GenerateCorrelationId();
    remote.Start(correlationId, RemoteBehavior.Succeed); // a 400ms head start before the first delivery

    var result = await asyncResponse
        .For<OperationResult>(correlationId)
        .WithTimeout(TimeSpan.FromSeconds(30))
        .Until(response => response.Status != OperationStatus.Running)
        .WaitAsync();

    return Results.Ok(new { correlationId, result.Status, result.Message });
})
.WithTags("Request/response");

// 2b) Sequential multi-step flow. Step 2 is only started if step 1 completes successfully, which
//     makes fail-fast behavior visible over HTTP instead of hidden inside a unit-only helper.
app.MapPost("/multi-step", async (
    IAsyncResponseBuilder asyncResponse,
    IAsyncResponsePublisher publisher,
    RemoteWorkSimulator remote,
    FlowRecorder recorder,
    string? first,
    string? second,
    string? trace) =>
{
    SampleTraceContext.Set(trace ?? $"trace-{Guid.NewGuid().ToString("N")[..8]}");
    SampleTenantContext.Set("tenant-acme");

    var steps = new List<StepOutcome>(capacity: 2);

    var firstStep = await RunStepAsync(asyncResponse, publisher, remote, recorder, "first", first);
    steps.Add(firstStep);
    if (!firstStep.Succeeded)
        return Results.Ok(new MultiStepFlowResult(false, firstStep.Name, steps));

    var secondStep = await RunStepAsync(asyncResponse, publisher, remote, recorder, "second", second);
    steps.Add(secondStep);

    return Results.Ok(new MultiStepFlowResult(secondStep.Succeeded, secondStep.Succeeded ? null : secondStep.Name, steps));
})
.WithTags("Flows");

// 2b-durable) The multi-step idea through the first-class durable-flows API: checkpointed steps,
//     awaited remote steps with progress, a domain-failure route, and crash-safe resume. Start a
//     run (optionally with a caller-supplied id for idempotent starts), observe its state, kick it.
app.MapPost("/durable-flow", async (IDurableFlows flows, string? name, bool? failAtImport, bool? includeAudit, string? flowId) =>
{
    var id = await flows.StartAsync<SampleProvisioningFlow, ProvisioningFlowInput>(
        new ProvisioningFlowInput(name ?? "acme", includeAudit ?? true, failAtImport ?? false),
        flowId);
    return Results.Ok(new StartFlowResult(id));
})
.WithTags("Flows");

app.MapGet("/durable-flow/{flowId}", async (IDurableFlows flows, string flowId) =>
{
    var state = await flows.GetStateAsync(flowId);
    return state is null ? Results.NotFound() : Results.Ok(state);
})
.WithTags("Flows");

app.MapPost("/durable-flow/{flowId}/resume", async (IDurableFlows flows, string flowId) =>
{
    await flows.ResumeAsync(flowId);
    return Results.Ok();
})
.WithTags("Flows");

// 2c) Publishes a technical failure from inside the trigger, addressing the response channel with
//     the request context's correlation id (the explicit, recommended way).
app.MapPost("/ambient-exception", async (
    IAsyncResponseBuilder asyncResponse,
    IAsyncResponsePublisher publisher,
    string? message) =>
{
    try
    {
        await asyncResponse
            .For<OperationResult>()
            .WithTimeout(TimeSpan.FromSeconds(10))
            .WaitAsync(ctx => publisher.SetException(new InvalidOperationException(message ?? "trigger technical error"), ctx.CorrelationId));

        return Results.Problem("The waiter completed successfully; it was expected to fault.");
    }
    catch (Exception ex)
    {
        return Results.Ok(new { faulted = true, exceptionType = ex.GetType().Name, detail = ex.Message });
    }
})
.WithTags("Request/response");

// 2d) Multiple waiters attached to the same correlation id should all fault when a technical
//     failure is published for that id.
app.MapPost("/shared-correlation-exception", async (
    IAsyncResponseSubscriber subscriber,
    IAsyncResponsePublisher publisher,
    string? message) =>
{
    var correlationId = AsyncResponseContext.GenerateCorrelationId();
    await using var first = await subscriber.CreateResponseWaiter<OperationResult>(
        correlationId,
        timeout: TimeSpan.FromSeconds(10));
    await using var second = await subscriber.CreateResponseWaiter<OperationResult>(
        correlationId,
        timeout: TimeSpan.FromSeconds(10));

    var firstResponse = first.ResponseTask;
    var secondResponse = second.ResponseTask;

    await publisher.SetException(new InvalidOperationException(message ?? "shared technical error"), correlationId);
    var failures = await Task.WhenAll(CaptureFailureAsync(firstResponse), CaptureFailureAsync(secondResponse));

    return Results.Ok(new SharedExceptionResult(correlationId, failures));
})
.WithTags("Request/response");

// 3) Reply target resolved by the registered transport's provider. Returns the target the trigger
//    observed; falls back to a clear 409 when no provider is registered (e.g. in-memory).
app.MapGet("/reply-target", async (IAsyncResponseBuilder asyncResponse, IAsyncResponsePublisher publisher) =>
{
    AsyncResponseReplyTarget? observed = null;
    try
    {
        await asyncResponse
            .For<OperationResult>()
            .WithReplyTarget()
            .WithTimeout(TimeSpan.FromSeconds(20))
            .Until(r => r.Status != OperationStatus.Running)
            .WaitAsync(async ctx =>
            {
                observed = ctx.ReplyTarget;
                await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, ctx.CorrelationId);
            });
    }
    catch (InvalidOperationException ex)
    {
        return Results.Conflict(ex.Message); // no reply-target provider registered for this transport
    }

    return Results.Ok(new { observed?.Transport, observed?.Address });
})
.WithTags("Request/response");

// 4) Fire-and-forget worker job over the configured transport. Returns the correlation id it set so a
//    round-trip test can assert it is restored on the consuming side.
app.MapPost("/worker", async (IAsyncResponseBuilder asyncResponse, string token, string? trace) =>
{
    var correlationId = AsyncResponseContext.CreateCorrelationId();
    SampleTraceContext.Set(trace);
    SampleTenantContext.Set("tenant-acme");
    await asyncResponse.EnqueueWorkerAsync<ISampleFlowService>(flow => flow.ProcessWorkAsync(token));
    return Results.Ok(new { correlationId });
})
.WithTags("Workers");

// 5a) Lost-subscriber recovery — arm: register a waiter with recovery callbacks and keep it waiting
//     in the background. The HTTP request returns immediately; the subscription and persisted
//     recovery state stay alive. The propagators capture the trace/tenant into the recovery state.
app.MapPost("/arm", async (IServiceProvider services, FlowRecorder recorder, string? trace) =>
{
    var asyncResponse = services.GetService<IRecoverableAsyncResponseBuilder>();
    if (asyncResponse is null)
        return Results.Conflict("Lost-subscriber recovery requires a durable channel such as Redis, NATS, or PostgreSQL.");

    SampleTraceContext.Set(trace);
    SampleTenantContext.Set("tenant-acme");
    var armed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

    var waitTask = asyncResponse
        .For<OperationResult>()
        .WithTimeout(TimeSpan.FromMinutes(2))
        .Until(r => r.Status != OperationStatus.Running)
        .OnLostSubscriberResume<ISampleFlowService>(flow =>
            flow.ResumeFlowAsync("sample-flow", Placeholder.Payload<OperationResult>(), Placeholder.CorrelationId()))
        .OnLostSubscriberFailure<ISampleFlowService>(flow =>
            flow.FailFlowAsync(Placeholder.Exception(), Placeholder.CorrelationId()))
        .WaitAsync(context =>
        {
            armed.SetResult(context.CorrelationId);
            return Task.CompletedTask;
        });

    var correlationId = await armed.Task;
    _ = waitTask.ContinueWith(t => recorder.RecordWaiterResult(correlationId, t), TaskScheduler.Default);
    return Results.Ok(new { correlationId });
})
.WithTags("Recovery");

// 5b) Lost-subscriber recovery — crash: drop every local subscription, like a redeploy would. The
//     durable recovery state stays in the channel store; only the in-memory waiters die.
app.MapPost("/crash", async (IServiceProvider services, CancellationToken cancellationToken) =>
{
    var multiplexer = services.GetService<IConnectionMultiplexer>();
    if (multiplexer is null)
    {
        var postgres = services.GetService<PostgreSqlAsyncResponseChannel>();
        if (postgres is not null)
        {
            await postgres.DropLocalSubscriptionsAsync(cancellationToken);
            return Results.Ok();
        }

        var sqlServer = services.GetService<SqlServerAsyncResponseChannel>();
        if (sqlServer is not null)
        {
            await sqlServer.DropLocalSubscriptionsAsync(cancellationToken);
            return Results.Ok();
        }

        return Results.Conflict("Crash simulation requires a durable channel such as Redis, PostgreSQL, or SQL Server.");
    }

    multiplexer.GetSubscriber().UnsubscribeAll();
    return Results.Ok();
})
.WithTags("Recovery");

// 5c) Deliver a late response/exception for a correlation id through the configured channel (used by
//     the lost-subscriber recovery scenarios after /crash, and by the active-waiter scenarios).
app.MapPost("/publish", async (IAsyncResponsePublisher publisher, string correlationId, string? status, string? message, string? exception) =>
{
    if (exception is not null)
    {
        await publisher.SetException(new InvalidOperationException(exception), correlationId);
    }
    else
    {
        var parsedStatus = Enum.TryParse<OperationStatus>(status, ignoreCase: true, out var s) ? s : OperationStatus.Completed;
        await publisher.SetResponse(new OperationResult { Status = parsedStatus, Message = message }, correlationId);
    }

    return Results.Accepted();
})
.WithTags("Recovery");

// 5d) Composed lost-subscriber recovery: arm, simulate the crash, publish the late terminal signal,
//     and wait for the recovery callback in one request. This endpoint complements the lower-level
//     /arm + /crash + /publish endpoints that integration tests can still drive step by step.
app.MapPost("/lost-subscriber-flow", async (
    IAsyncResponsePublisher publisher,
    FlowRecorder recorder,
    IServiceProvider services,
    string? outcome,
    string? trace,
    CancellationToken cancellationToken) =>
{
    var asyncResponse = services.GetService<IRecoverableAsyncResponseBuilder>();
    if (asyncResponse is null)
        return Results.Conflict("Composed lost-subscriber recovery requires a durable channel such as Redis, NATS, or PostgreSQL.");

    SampleTraceContext.Set(trace ?? $"trace-{Guid.NewGuid().ToString("N")[..8]}");
    SampleTenantContext.Set("tenant-acme");
    var armed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

    var waitTask = asyncResponse
        .For<OperationResult>()
        .WithTimeout(TimeSpan.FromMinutes(2))
        .Until(r => r.Status != OperationStatus.Running)
        .OnLostSubscriberResume<ISampleFlowService>(flow =>
            flow.ResumeFlowAsync("sample-flow", Placeholder.Payload<OperationResult>(), Placeholder.CorrelationId()))
        .OnLostSubscriberFailure<ISampleFlowService>(flow =>
            flow.FailFlowAsync(Placeholder.Exception(), Placeholder.CorrelationId()))
        .WaitAsync(context =>
        {
            armed.SetResult(context.CorrelationId);
            return Task.CompletedTask;
        });

    var correlationId = await armed.Task.WaitAsync(TimeSpan.FromSeconds(10));
    _ = waitTask.ContinueWith(t => recorder.RecordWaiterResult(correlationId, t), TaskScheduler.Default);

    var crashed = await DropLocalSubscriptionAsync(services, correlationId, cancellationToken).ConfigureAwait(false);
    if (!crashed)
        return Results.Conflict("Composed lost-subscriber recovery currently supports Redis or PostgreSQL channels.");

    var normalized = (outcome ?? "Completed").Trim().ToLowerInvariant();
    if (normalized is "exception" or "failtechnical" or "technical")
    {
        await publisher.SetException(new InvalidOperationException("lost-subscriber technical error"), correlationId);
        normalized = "exception";
    }
    else
    {
        var status = normalized switch
        {
            "failed" or "faildomain" or "domain" => OperationStatus.Failed,
            "running" => OperationStatus.Running,
            _ => OperationStatus.Completed
        };
        normalized = status.ToString();
        await publisher.SetResponse(new OperationResult { Status = status, Message = "late" }, correlationId);
    }

    var callbackKind = normalized is nameof(OperationStatus.Completed) or nameof(OperationStatus.Running)
        ? "resume"
        : "fail";
    var callback = await recorder.WaitForAsync($"{callbackKind}:{correlationId}").WaitAsync(TimeSpan.FromSeconds(30));

    return Results.Ok(new LostSubscriberFlowResult(correlationId, normalized, callback));
})
.WithTags("Recovery");

static async Task<bool> DropLocalSubscriptionAsync(
    IServiceProvider services,
    string correlationId,
    CancellationToken cancellationToken)
{
    var multiplexer = services.GetService<IConnectionMultiplexer>();
    if (multiplexer is not null)
    {
        var redisOptions = services.GetRequiredService<IOptions<RedisAsyncResponseOptions>>();
        var responseChannel = new RedisChannel(
            $"{redisOptions.Value.KeyPrefix}:response:{correlationId}",
            RedisChannel.PatternMode.Literal);
        await multiplexer.GetSubscriber().UnsubscribeAsync(responseChannel).ConfigureAwait(false);
        await Task.Delay(100, cancellationToken).ConfigureAwait(false);
        return true;
    }

    var postgres = services.GetService<PostgreSqlAsyncResponseChannel>();
    if (postgres is not null)
    {
        await postgres.DropLocalSubscriptionsAsync(cancellationToken).ConfigureAwait(false);
        await Task.Delay(100, cancellationToken).ConfigureAwait(false);
        return true;
    }

    var sqlServer = services.GetService<SqlServerAsyncResponseChannel>();
    if (sqlServer is not null)
    {
        await sqlServer.DropLocalSubscriptionsAsync(cancellationToken).ConfigureAwait(false);
        await Task.Delay(100, cancellationToken).ConfigureAwait(false);
        return true;
    }

    return false;
}

// 6) Publish a raw response to the configured broker response destination, acting as the remote
//    system. With useAttribute the correlation id rides broker metadata; otherwise it goes in the
//    JSON body so the extractor's JSON-path fallback is exercised. (broker transports only.)
app.MapPost("/emit-response", async (
    IServiceProvider services,
    string correlationId,
    string? status,
    bool useAttribute,
    string? message,
    CancellationToken cancellationToken) =>
{
    var parsedStatus = Enum.TryParse<OperationStatus>(status, ignoreCase: true, out var s) ? s : OperationStatus.Completed;

    if (useGooglePubSub)
        return await EmitPubSubResponseAsync(services, correlationId, parsedStatus, useAttribute, message).ConfigureAwait(false);

    if (useAzureServiceBus)
        return await EmitAzureServiceBusResponseAsync(services, correlationId, parsedStatus, useAttribute, message, cancellationToken).ConfigureAwait(false);

    if (useKafka)
        return await EmitKafkaResponseAsync(services, correlationId, parsedStatus, useAttribute, message, cancellationToken).ConfigureAwait(false);

    if (useRabbitMq)
        return await EmitRabbitMqResponseAsync(services, correlationId, parsedStatus, useAttribute, message, cancellationToken).ConfigureAwait(false);

    if (useRedisTransport)
        return await EmitRedisResponseAsync(services, correlationId, parsedStatus, useAttribute, message, cancellationToken).ConfigureAwait(false);

    if (useNatsTransport)
        return await EmitNatsResponseAsync(services, correlationId, parsedStatus, useAttribute, message, cancellationToken).ConfigureAwait(false);

    if (usePostgreSqlTransport)
        return await EmitPostgreSqlResponseAsync(services, correlationId, parsedStatus, useAttribute, message, cancellationToken).ConfigureAwait(false);

    if (useSqlServerTransport)
        return await EmitSqlServerResponseAsync(services, correlationId, parsedStatus, useAttribute, message, cancellationToken).ConfigureAwait(false);

    return Results.Conflict("Raw response ingress requires a transport such as AzureServiceBus, GooglePubSub, Kafka, RabbitMQ, Redis, NATS, PostgreSQL, or SqlServer.");
})
.WithTags("Workers");

static async Task<IResult> EmitPubSubResponseAsync(
    IServiceProvider services,
    string correlationId,
    OperationStatus status,
    bool useAttribute,
    string? message)
{
    var options = services.GetService<IOptions<GooglePubSubAsyncResponseOptions>>();
    if (options is null)
        return Results.Conflict("Pub/Sub response ingress requires the Google Pub/Sub transport.");
    var publisherFactory = services.GetService<Lazy<Task<PublisherServiceApiClient>>>();
    if (publisherFactory is null)
        return Results.Conflict("Pub/Sub publisher client is not registered.");

    var o = options.Value;
    var json = useAttribute
        ? JsonSerializer.Serialize(new OperationResult { Status = status, Message = message })
        : JsonSerializer.Serialize(new { CorrelationId = correlationId, Status = (int)status, Message = message });

    var pubsubMessage = new PubsubMessage { Data = ByteString.CopyFromUtf8(json) };
    if (useAttribute)
        pubsubMessage.Attributes[o.CorrelationIdAttribute] = correlationId;

    var publisher = await publisherFactory.Value.ConfigureAwait(false);
    await publisher.PublishAsync(TopicName.FromProjectTopic(o.ProjectId!, o.ResponseTopicId!), [pubsubMessage]);

    return Results.Accepted();
}

static async Task<IResult> EmitAzureServiceBusResponseAsync(
    IServiceProvider services,
    string correlationId,
    OperationStatus status,
    bool useAttribute,
    string? message,
    CancellationToken cancellationToken)
{
    var options = services.GetService<IOptions<AzureServiceBusAsyncResponseOptions>>();
    if (options is null)
        return Results.Conflict("Azure Service Bus response ingress requires the Azure Service Bus transport.");

    var o = options.Value;
    var connectionString = o.ConnectionString;
    if (string.IsNullOrWhiteSpace(connectionString))
        return Results.Conflict("Azure Service Bus response ingress requires AzureServiceBus:ConnectionString.");

    var json = useAttribute
        ? JsonSerializer.Serialize(new OperationResult { Status = status, Message = message })
        : JsonSerializer.Serialize(new { CorrelationId = correlationId, Status = (int)status, Message = message });
    var serviceBusMessage = new ServiceBusMessage(BinaryData.FromString(json))
    {
        ContentType = "application/json",
        MessageId = Guid.NewGuid().ToString("N")
    };

    if (useAttribute)
    {
        serviceBusMessage.CorrelationId = correlationId;
        serviceBusMessage.ApplicationProperties[o.CorrelationIdProperty] = correlationId;
    }

    await using var client = new ServiceBusClient(connectionString);
    await using var sender = client.CreateSender(o.ResponseQueue);
    await sender.SendMessageAsync(serviceBusMessage, cancellationToken).ConfigureAwait(false);

    return Results.Accepted();
}

static async Task<IResult> EmitKafkaResponseAsync(
    IServiceProvider services,
    string correlationId,
    OperationStatus status,
    bool useAttribute,
    string? message,
    CancellationToken cancellationToken)
{
    var options = services.GetService<IOptions<KafkaAsyncResponseTransportOptions>>();
    if (options is null)
        return Results.Conflict("Kafka response ingress requires the Kafka transport.");

    var o = options.Value;
    if (string.IsNullOrWhiteSpace(o.BootstrapServers))
        return Results.Conflict("Kafka response ingress requires Kafka:BootstrapServers.");

    var responseTopic = ResolveKafkaTopic(o.ResponseTopic, o.TopicPrefix, "response");
    var json = useAttribute
        ? JsonSerializer.Serialize(new OperationResult { Status = status, Message = message })
        : JsonSerializer.Serialize(new { CorrelationId = correlationId, Status = (int)status, Message = message });

    var kafkaMessage = new Message<string?, byte[]> { Value = System.Text.Encoding.UTF8.GetBytes(json) };
    if (useAttribute)
    {
        kafkaMessage.Key = correlationId;
        kafkaMessage.Headers = [];
        kafkaMessage.Headers.Add(o.CorrelationIdHeader, System.Text.Encoding.UTF8.GetBytes(correlationId));
    }

    // A one-off producer mirrors how a remote system would feed the response topic; the hosted
    // response-ingress subscriber (which pre-created the topic) picks the message up from there.
    var config = new ProducerConfig { BootstrapServers = o.BootstrapServers, Acks = Acks.All };
    using var producer = new ProducerBuilder<string?, byte[]>(config).Build();
    await producer.ProduceAsync(responseTopic, kafkaMessage, cancellationToken).ConfigureAwait(false);

    return Results.Accepted();
}

static async Task<IResult> EmitRabbitMqResponseAsync(
    IServiceProvider services,
    string correlationId,
    OperationStatus status,
    bool useAttribute,
    string? message,
    CancellationToken cancellationToken)
{
    var options = services.GetService<IOptions<RabbitMqAsyncResponseOptions>>();
    if (options is null)
        return Results.Conflict("RabbitMQ response ingress requires the RabbitMQ transport.");

    var o = options.Value;
    var json = useAttribute
        ? JsonSerializer.Serialize(new OperationResult { Status = status, Message = message })
        : JsonSerializer.Serialize(new { CorrelationId = correlationId, Status = (int)status, Message = message });

    var factory = CreateRabbitMqConnectionFactory(o);
    await using var connection = await factory.CreateConnectionAsync(cancellationToken).ConfigureAwait(false);
    await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

    if (o.DeclareTopology)
    {
        await channel.ExchangeDeclareAsync(o.ResponseExchange, "direct", durable: true, autoDelete: false, cancellationToken: cancellationToken).ConfigureAwait(false);
        await channel.QueueDeclareAsync(o.ResponseQueue, durable: true, exclusive: false, autoDelete: false, cancellationToken: cancellationToken).ConfigureAwait(false);
        await channel.QueueBindAsync(o.ResponseQueue, o.ResponseExchange, o.ResponseRoutingKey, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    var properties = new BasicProperties
    {
        ContentType = "application/json",
        DeliveryMode = DeliveryModes.Persistent,
        Persistent = true,
        MessageId = Guid.NewGuid().ToString("N"),
        Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds())
    };

    if (useAttribute)
    {
        properties.CorrelationId = correlationId;
        properties.Headers = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            [o.CorrelationIdHeader] = correlationId
        };
    }

    await channel.BasicPublishAsync(
        o.ResponseExchange,
        o.ResponseRoutingKey,
        mandatory: true,
        properties,
        System.Text.Encoding.UTF8.GetBytes(json),
        cancellationToken).ConfigureAwait(false);

    return Results.Accepted();
}

static async Task<IResult> EmitRedisResponseAsync(
    IServiceProvider services,
    string correlationId,
    OperationStatus status,
    bool useAttribute,
    string? message,
    CancellationToken cancellationToken)
{
    var options = services.GetService<IOptions<RedisAsyncResponseTransportOptions>>();
    if (options is null)
        return Results.Conflict("Redis response ingress requires the Redis transport.");
    var multiplexer = services.GetService<IConnectionMultiplexer>();
    if (multiplexer is null)
        return Results.Conflict("Redis response ingress requires an IConnectionMultiplexer.");

    var o = options.Value;
    var responseStream = ResolveRedisStream(o.ResponseStream, o.KeyPrefix, "response");
    var json = useAttribute
        ? JsonSerializer.Serialize(new OperationResult { Status = status, Message = message })
        : JsonSerializer.Serialize(new { CorrelationId = correlationId, Status = (int)status, Message = message });

    var fields = useAttribute
        ?
        [
            new NameValueEntry(o.PayloadField, json),
            new NameValueEntry(o.CorrelationIdField, correlationId)
        ]
        : new[] { new NameValueEntry(o.PayloadField, json) };

    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    timeout.CancelAfter(o.OperationTimeout);
    await multiplexer.GetDatabase().StreamAddAsync(
        responseStream,
        fields,
        messageId: null,
        maxLength: o.StreamMaxLength,
        useApproximateMaxLength: o.UseApproximateStreamTrimming,
        limit: null,
        trimMode: StreamTrimMode.KeepReferences).WaitAsync(timeout.Token).ConfigureAwait(false);

    return Results.Accepted();
}

static async Task<IResult> EmitNatsResponseAsync(
    IServiceProvider services,
    string correlationId,
    OperationStatus status,
    bool useAttribute,
    string? message,
    CancellationToken cancellationToken)
{
    var options = services.GetService<IOptions<NatsAsyncResponseTransportOptions>>();
    if (options is null)
        return Results.Conflict("NATS response ingress requires the NATS transport.");
    var connection = services.GetService<INatsConnection>();
    if (connection is null)
        return Results.Conflict("NATS response ingress requires an INatsConnection.");

    var o = options.Value;
    var responseSubject = string.IsNullOrWhiteSpace(o.ResponseSubject)
        ? $"{o.SubjectPrefix}.transport.response"
        : o.ResponseSubject;
    var json = useAttribute
        ? JsonSerializer.Serialize(new OperationResult { Status = status, Message = message })
        : JsonSerializer.Serialize(new { CorrelationId = correlationId, Status = (int)status, Message = message });

    // The response stream (created by the hosted response-ingress subscriber on startup) captures any
    // message on its subject, so a Core publish is sufficient to feed the transport's response ingress.
    NatsHeaders? headers = useAttribute ? new NatsHeaders { [o.CorrelationIdHeader] = correlationId } : null;
    await connection.PublishAsync(responseSubject, json, headers: headers, cancellationToken: cancellationToken).ConfigureAwait(false);

    return Results.Accepted();
}

static async Task<IResult> EmitPostgreSqlResponseAsync(
    IServiceProvider services,
    string correlationId,
    OperationStatus status,
    bool useAttribute,
    string? message,
    CancellationToken cancellationToken)
{
    var options = services.GetService<IOptions<PostgreSqlAsyncResponseTransportOptions>>();
    if (options is null)
        return Results.Conflict("PostgreSQL response ingress requires the PostgreSQL transport.");
    var store = services.GetService<PostgreSqlTransportStore>();
    if (store is null)
        return Results.Conflict("PostgreSQL response ingress requires the transport store.");

    var o = options.Value;
    var json = useAttribute
        ? JsonSerializer.Serialize(new OperationResult { Status = status, Message = message })
        : JsonSerializer.Serialize(new { CorrelationId = correlationId, Status = (int)status, Message = message });
    var headers = useAttribute
        ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { [o.CorrelationIdHeader] = correlationId }
        : null;

    await store.PublishAsync(Guid.NewGuid(), o.ResponseQueue, json, headers, cancellationToken).ConfigureAwait(false);
    return Results.Accepted();
}

static async Task<IResult> EmitSqlServerResponseAsync(
    IServiceProvider services,
    string correlationId,
    OperationStatus status,
    bool useAttribute,
    string? message,
    CancellationToken cancellationToken)
{
    var options = services.GetService<IOptions<SqlServerAsyncResponseTransportOptions>>();
    if (options is null)
        return Results.Conflict("SQL Server response ingress requires the SQL Server transport.");
    var store = services.GetService<SqlServerTransportStore>();
    if (store is null)
        return Results.Conflict("SQL Server response ingress requires the transport store.");

    var o = options.Value;
    var json = useAttribute
        ? JsonSerializer.Serialize(new OperationResult { Status = status, Message = message })
        : JsonSerializer.Serialize(new { CorrelationId = correlationId, Status = (int)status, Message = message });
    var headers = useAttribute
        ? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { [o.CorrelationIdHeader] = correlationId }
        : null;

    await store.PublishAsync(Guid.NewGuid(), o.ResponseQueue, json, headers, cancellationToken).ConfigureAwait(false);
    return Results.Accepted();
}

static ConnectionFactory CreateRabbitMqConnectionFactory(RabbitMqAsyncResponseOptions options)
{
    var factory = new ConnectionFactory
    {
        AutomaticRecoveryEnabled = options.AutomaticRecoveryEnabled,
        TopologyRecoveryEnabled = options.TopologyRecoveryEnabled,
        NetworkRecoveryInterval = options.NetworkRecoveryInterval,
        RequestedHeartbeat = options.RequestedHeartbeat,
        ClientProvidedName = options.ClientProvidedName,
        HostName = options.HostName,
        Port = options.Port,
        VirtualHost = options.VirtualHost,
        UserName = options.UserName,
        Password = options.Password
    };

    if (!string.IsNullOrWhiteSpace(options.ConnectionString))
        factory.Uri = new Uri(options.ConnectionString);

    return factory;
}

// --- Observability / test affordances --------------------------------------------------------

// Long-poll the flow recorder for a recorded call (e.g. worker:{token}, resume:{cid}, waiter:{cid}).
app.MapGet("/calls", async (FlowRecorder recorder, string key, int? timeoutMs) =>
{
    try
    {
        var call = await recorder.WaitForAsync(key).WaitAsync(TimeSpan.FromMilliseconds(timeoutMs ?? 15000));
        return Results.Ok(call);
    }
    catch (TimeoutException)
    {
        return Results.StatusCode(StatusCodes.Status408RequestTimeout);
    }
})
.WithTags("Observability");

// Seed a stale recovery entry (no live subscriber) so the watchdog surfaces it as Degraded health.
app.MapPost("/seed-recovery", async (IRecoveryStateStore store, string correlationId, int? ageMinutes) =>
{
    await store.SaveAsync(correlationId, new RecoveryState
    {
        CorrelationId = correlationId,
        PayloadTypeFullName = typeof(OperationResult).FullName,
        RegisteredAtUtc = DateTime.UtcNow.AddMinutes(-(ageMinutes ?? 5))
    }, TimeSpan.FromMinutes(10));

    return Results.Accepted();
})
.WithTags("Observability");

app.MapDelete("/test/recovery/{correlationId}", async (IRecoveryStateStore store, string correlationId) =>
{
    var deleted = await store.TryDeleteAsync(correlationId);
    return Results.Ok(new { deleted });
})
.WithTags("Observability");

app.MapPost("/test/reset", async (IRecoveryStateScanner scanner, IRecoveryStateStore store, FlowRecorder recorder, CancellationToken cancellationToken) =>
{
    var deleted = 0;
    await foreach (var state in scanner.ScanAsync(cancellationToken))
    {
        if (!string.IsNullOrWhiteSpace(state.CorrelationId)
            && await store.TryDeleteAsync(state.CorrelationId, cancellationToken))
        {
            deleted++;
        }
    }

    recorder.Clear();
    return Results.Ok(new { deleted });
})
.WithTags("Observability");

// Health endpoint with full JSON details, including the recovery check's data payload.
app.MapHealthChecks("/healthz", new HealthCheckOptions
{
    ResponseWriter = async (context, report) =>
    {
        context.Response.ContentType = "application/json; charset=utf-8";
        await context.Response.WriteAsync(JsonSerializer.Serialize(new
        {
            status = report.Status.ToString(),
            checks = report.Entries.Select(entry => new
            {
                name = entry.Key,
                status = entry.Value.Status.ToString(),
                description = entry.Value.Description,
                data = entry.Value.Data
            })
        }, new JsonSerializerOptions { WriteIndented = true }));
    }
}).WithTags("Health");

app.MapDefaultEndpoints();
app.Run();

/// <summary>Exposed so in-process integration tests can boot the app with WebApplicationFactory.</summary>
public partial class Program;
