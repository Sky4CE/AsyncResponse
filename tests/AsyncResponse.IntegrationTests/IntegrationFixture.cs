using Aspire.Hosting;
using Aspire.Hosting.Testing;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Boots the Aspire AppHost once for the whole collection — real Redis, a Google Pub/Sub emulator,
/// Azure Service Bus emulator, Kafka, RabbitMQ, NATS, PostgreSQL, and the system-under-test sample
/// apps, all orchestrated by the dedicated integration AppHost. Tests drive the SUTs entirely over
/// HTTP via <see cref="Client"/>.
/// </summary>
public sealed class IntegrationFixture : IAsyncLifetime
{
    /// <summary>Response topic id the AppHost configures — asserted by the reply-target scenario.</summary>
    public const string ResponseTopicId = "response-topic";
    public const string RabbitMqResponseExchange = "asyncresponse.itest.response";
    public const string RabbitMqResponseRoutingKey = "asyncresponse.itest.response";

    private static readonly TimeSpan StartupTimeout = TimeSpan.FromMinutes(3);
    private const string OracleConnectionStringEnvironmentVariable = "ASYNCRESPONSE_ITEST_ORACLE_CONNECTION_STRING";
    private const string CosmosConnectionStringEnvironmentVariable = "ASYNCRESPONSE_ITEST_COSMOS_CONNECTION_STRING";
    private const string CosmosEmulatorAccountKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    private DistributedApplication? _app;
    private string? _previousOracleConnectionString;
    private string? _previousCosmosConnectionString;

    public HttpClient Client { get; private set; } = null!;
    public HttpClient EarlyAckClient { get; private set; } = null!;
    public HttpClient AzureServiceBusClient { get; private set; } = null!;
    public HttpClient AzureServiceBusEarlyAckClient { get; private set; } = null!;
    public HttpClient SqsClient { get; private set; } = null!;
    public HttpClient SqsEarlyAckClient { get; private set; } = null!;
    public HttpClient RabbitMqClient { get; private set; } = null!;
    public HttpClient RabbitMqEarlyAckClient { get; private set; } = null!;
    public HttpClient KafkaClient { get; private set; } = null!;
    public HttpClient KafkaEarlyAckClient { get; private set; } = null!;
    public HttpClient RedisTransportClient { get; private set; } = null!;
    public HttpClient RedisTransportEarlyAckClient { get; private set; } = null!;
    public HttpClient NatsClient { get; private set; } = null!;
    public HttpClient NatsEarlyAckClient { get; private set; } = null!;
    public HttpClient PostgreSqlClient { get; private set; } = null!;
    public HttpClient PostgreSqlEarlyAckClient { get; private set; } = null!;
    public string PostgreSqlConnectionString { get; private set; } = null!;
    public string MySqlConnectionString { get; private set; } = null!;
    public string MongoDbConnectionString { get; private set; } = null!;
    public HttpClient SqlServerClient { get; private set; } = null!;
    public HttpClient SqlServerEarlyAckClient { get; private set; } = null!;
    public string SqlServerConnectionString { get; private set; } = null!;
    public HttpClient MongoDbClient { get; private set; } = null!;
    public HttpClient MongoDbEarlyAckClient { get; private set; } = null!;
    public string LocalStackServiceUrl { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        _previousOracleConnectionString = Environment.GetEnvironmentVariable(OracleConnectionStringEnvironmentVariable);
        _previousCosmosConnectionString = Environment.GetEnvironmentVariable(CosmosConnectionStringEnvironmentVariable);

        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.AsyncResponse_IntegrationTests_AppHost>();
        _app = await appHost.BuildAsync().WaitAsync(StartupTimeout);
        await _app.StartAsync().WaitAsync(StartupTimeout);

        // Wait until the SUT reports healthy (its /alive probe) — i.e. it has connected to Redis and the
        // emulator and provisioned its topics/subscriptions — before driving any scenario.
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-early-ack")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-azure-servicebus")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-azure-servicebus-early-ack")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-sqs")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-sqs-early-ack")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-rabbitmq")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-rabbitmq-early-ack")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-kafka")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-kafka-early-ack")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-redis")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-redis-early-ack")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-nats")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-nats-early-ack")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-postgresql")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-postgresql-early-ack")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-sqlserver")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-sqlserver-early-ack")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-mongodb")
            .WaitAsync(StartupTimeout);
        await _app.ResourceNotifications
            .WaitForResourceHealthyAsync("itest-app-mongodb-early-ack")
            .WaitAsync(StartupTimeout);

        Client = _app.CreateHttpClient("itest-app");
        EarlyAckClient = _app.CreateHttpClient("itest-app-early-ack");
        AzureServiceBusClient = _app.CreateHttpClient("itest-app-azure-servicebus");
        AzureServiceBusEarlyAckClient = _app.CreateHttpClient("itest-app-azure-servicebus-early-ack");
        SqsClient = _app.CreateHttpClient("itest-app-sqs");
        SqsEarlyAckClient = _app.CreateHttpClient("itest-app-sqs-early-ack");
        RabbitMqClient = _app.CreateHttpClient("itest-app-rabbitmq");
        RabbitMqEarlyAckClient = _app.CreateHttpClient("itest-app-rabbitmq-early-ack");
        KafkaClient = _app.CreateHttpClient("itest-app-kafka");
        KafkaEarlyAckClient = _app.CreateHttpClient("itest-app-kafka-early-ack");
        RedisTransportClient = _app.CreateHttpClient("itest-app-redis");
        RedisTransportEarlyAckClient = _app.CreateHttpClient("itest-app-redis-early-ack");
        NatsClient = _app.CreateHttpClient("itest-app-nats");
        NatsEarlyAckClient = _app.CreateHttpClient("itest-app-nats-early-ack");
        PostgreSqlClient = _app.CreateHttpClient("itest-app-postgresql");
        PostgreSqlEarlyAckClient = _app.CreateHttpClient("itest-app-postgresql-early-ack");
        SqlServerClient = _app.CreateHttpClient("itest-app-sqlserver");
        SqlServerEarlyAckClient = _app.CreateHttpClient("itest-app-sqlserver-early-ack");
        MongoDbClient = _app.CreateHttpClient("itest-app-mongodb");
        MongoDbEarlyAckClient = _app.CreateHttpClient("itest-app-mongodb-early-ack");

        var postgresEndpoint = _app.GetEndpoint("postgres", "postgres");
        PostgreSqlConnectionString =
            $"Host={postgresEndpoint.Host};Port={postgresEndpoint.Port};Username=postgres;Password=postgres;Database=asyncresponse;" +
            "Maximum Pool Size=40;No Reset On Close=true;Max Auto Prepare=20";

        var mysqlEndpoint = _app.GetEndpoint("mysql", "mysql");
        MySqlConnectionString =
            $"Server={mysqlEndpoint.Host};Port={mysqlEndpoint.Port};User ID=root;Password=mysql;Database=asyncresponse;Connection Timeout=30;";

        // directConnection: the container is a single-node replica set (change streams need one), and
        // without it the driver would chase the replica-set-advertised container hostname.
        var mongoDbEndpoint = _app.GetEndpoint("mongodb", "mongodb");
        MongoDbConnectionString = $"mongodb://{mongoDbEndpoint.Host}:{mongoDbEndpoint.Port}/?directConnection=true";

        // The AppHost omits the Oracle and Cosmos containers when ASYNCRESPONSE_ITEST_SKIP_ORACLE_COSMOS
        // is set (their endpoints then don't exist), and a user-supplied connection string always wins
        // over the container-derived one. When neither var ends up set, the opt-in durable-flow store
        // contract tests Assert.Skip.
        var skipOracleCosmos = string.Equals(
            Env("ASYNCRESPONSE_ITEST_SKIP_ORACLE_COSMOS", "false"), "true", StringComparison.OrdinalIgnoreCase);

        if (!skipOracleCosmos && string.IsNullOrEmpty(_previousOracleConnectionString))
        {
            var oracleEndpoint = _app.GetEndpoint("oracle", "oracle");
            Environment.SetEnvironmentVariable(
                OracleConnectionStringEnvironmentVariable,
                $"User Id=asyncresponse;Password={Env("ASYNCRESPONSE_ITEST_ORACLE_APP_PASSWORD", "AsyncResponse12345")};Data Source={oracleEndpoint.Host}:{oracleEndpoint.Port}/FREEPDB1;");
        }

        if (!skipOracleCosmos && string.IsNullOrEmpty(_previousCosmosConnectionString))
        {
            var cosmosEndpoint = _app.GetEndpoint("cosmos", "gateway");
            Environment.SetEnvironmentVariable(
                CosmosConnectionStringEnvironmentVariable,
                $"AccountEndpoint=https://{cosmosEndpoint.Host}:{cosmosEndpoint.Port}/;AccountKey={CosmosEmulatorAccountKey};");
        }

        // The SUT apps have already provisioned the asyncresponse database by the time they report
        // healthy, so direct tests can connect straight to it.
        var sqlServerEndpoint = _app.GetEndpoint("sqlserver", "sqlserver");
        var sqlServerPassword = Environment.GetEnvironmentVariable("ASYNCRESPONSE_ITEST_SQLSERVER_PASSWORD") is { Length: > 0 } configured
            ? configured
            : "P@ssword12345";
        SqlServerConnectionString =
            $"Server={sqlServerEndpoint.Host},{sqlServerEndpoint.Port};User ID=sa;Password={sqlServerPassword};" +
            "Database=asyncresponse;TrustServerCertificate=True;Max Pool Size=40";

        var localStackEndpoint = _app.GetEndpoint("localstack", "edge");
        LocalStackServiceUrl = $"http://{localStackEndpoint.Host}:{localStackEndpoint.Port}";

        await ResetTestStateAsync(Client).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(EarlyAckClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(AzureServiceBusClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(AzureServiceBusEarlyAckClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(SqsClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(SqsEarlyAckClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(RabbitMqClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(RabbitMqEarlyAckClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(KafkaClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(KafkaEarlyAckClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(RedisTransportClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(RedisTransportEarlyAckClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(NatsClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(NatsEarlyAckClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(PostgreSqlClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(PostgreSqlEarlyAckClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(SqlServerClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(SqlServerEarlyAckClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(MongoDbClient).WaitAsync(StartupTimeout);
        await ResetTestStateAsync(MongoDbEarlyAckClient).WaitAsync(StartupTimeout);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            Client?.Dispose();
            EarlyAckClient?.Dispose();
            AzureServiceBusClient?.Dispose();
            AzureServiceBusEarlyAckClient?.Dispose();
            SqsClient?.Dispose();
            SqsEarlyAckClient?.Dispose();
            RabbitMqClient?.Dispose();
            RabbitMqEarlyAckClient?.Dispose();
            KafkaClient?.Dispose();
            KafkaEarlyAckClient?.Dispose();
            RedisTransportClient?.Dispose();
            RedisTransportEarlyAckClient?.Dispose();
            NatsClient?.Dispose();
            NatsEarlyAckClient?.Dispose();
            PostgreSqlClient?.Dispose();
            PostgreSqlEarlyAckClient?.Dispose();
            SqlServerClient?.Dispose();
            SqlServerEarlyAckClient?.Dispose();
            MongoDbClient?.Dispose();
            MongoDbEarlyAckClient?.Dispose();
            if (_app is not null)
                await _app.DisposeAsync();
        }
        finally
        {
            Environment.SetEnvironmentVariable(OracleConnectionStringEnvironmentVariable, _previousOracleConnectionString);
            Environment.SetEnvironmentVariable(CosmosConnectionStringEnvironmentVariable, _previousCosmosConnectionString);
        }
    }

    private static string Env(string name, string fallback)
        => Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : fallback;

    private static async Task ResetTestStateAsync(HttpClient client)
    {
        var response = await client.PostAsync("/test/reset", content: null);
        response.EnsureSuccessStatusCode();
    }
}
