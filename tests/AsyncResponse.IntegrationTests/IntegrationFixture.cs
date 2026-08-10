using Aspire.Hosting;
using Aspire.Hosting.Testing;
using AsyncResponse.Conformance;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Boots the Aspire AppHost once per <em>batch</em>. A batch is a named subset of the fleet: the
/// AppHost declares only that batch's containers and sample apps, and this fixture only resolves
/// endpoints for what its batch declared. Because collections run sequentially (the assembly-level
/// <c>DisableTestParallelization</c> in Batches.cs), xUnit disposes one batch's fixture — tearing its
/// containers down — before the next batch's fixture initializes. Peak footprint is therefore the
/// largest batch, not the whole fleet.
/// <para>
/// Derive a fixture per batch: override <see cref="Batch"/> with a name the AppHost understands and
/// <see cref="WireAsync"/> to resolve just that batch's resources. Touching a resource the batch did
/// not declare throws, which is the intended failure — it means the test is in the wrong batch.
/// </para>
/// </summary>
public abstract class IntegrationFixture : IAsyncLifetime
{
    /// <summary>Selects which resources the AppHost declares. Must match a batch it knows.</summary>
    protected abstract string Batch { get; }

    private const string BatchEnvironmentVariable = "ASYNCRESPONSE_ITEST_BATCH";
    private string? _previousBatch;

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

    public HttpClient Client { get; private protected set; } = null!;
    public HttpClient EarlyAckClient { get; private protected set; } = null!;
    public HttpClient AzureServiceBusClient { get; private protected set; } = null!;
    public HttpClient AzureServiceBusEarlyAckClient { get; private protected set; } = null!;
    public HttpClient SqsClient { get; private protected set; } = null!;
    public HttpClient SqsEarlyAckClient { get; private protected set; } = null!;
    public HttpClient RabbitMqClient { get; private protected set; } = null!;
    public HttpClient RabbitMqEarlyAckClient { get; private protected set; } = null!;
    public HttpClient KafkaClient { get; private protected set; } = null!;
    public HttpClient KafkaEarlyAckClient { get; private protected set; } = null!;
    public HttpClient RedisTransportClient { get; private protected set; } = null!;
    public HttpClient RedisTransportEarlyAckClient { get; private protected set; } = null!;
    public HttpClient NatsClient { get; private protected set; } = null!;
    public HttpClient NatsEarlyAckClient { get; private protected set; } = null!;
    /// <summary>StackExchange.Redis configuration for the fixture's Redis (used by Direct-style tests).</summary>
    public string RedisConnectionString { get; private protected set; } = null!;
    /// <summary>NATS URL for the fixture's JetStream-enabled NATS server (used by Direct-style tests).</summary>
    public string NatsConnectionString { get; private protected set; } = null!;
    public HttpClient PostgreSqlClient { get; private protected set; } = null!;
    public HttpClient PostgreSqlEarlyAckClient { get; private protected set; } = null!;
    public string PostgreSqlConnectionString { get; private protected set; } = null!;
    public string MySqlConnectionString { get; private protected set; } = null!;
    public string MongoDbConnectionString { get; private protected set; } = null!;
    public HttpClient SqlServerClient { get; private protected set; } = null!;
    public HttpClient SqlServerEarlyAckClient { get; private protected set; } = null!;
    public string SqlServerConnectionString { get; private protected set; } = null!;
    public HttpClient MongoDbClient { get; private protected set; } = null!;
    public HttpClient MongoDbEarlyAckClient { get; private protected set; } = null!;
    public string LocalStackServiceUrl { get; private protected set; } = null!;

    // --- Broker endpoints for driver-level suites --------------------------------------------------
    // The app-driven tests never needed these (Aspire injected them into the sample apps), but the
    // transport contract builds its hosts in this process and has to address the brokers itself.

    /// <summary>Kafka bootstrap servers, when the batch declared Kafka.</summary>
    public string? KafkaBootstrapServers { get; private protected set; }

    /// <summary>RabbitMQ AMQP URI, when the batch declared RabbitMQ.</summary>
    public string? RabbitMqConnectionString { get; private protected set; }

    /// <summary>Service Bus emulator connection string, when the batch declared it.</summary>
    public string? AzureServiceBusConnectionString { get; private protected set; }

    /// <summary>The Pub/Sub emulator's project, when the batch declared the emulator.</summary>
    public string? PubSubProjectId { get; private protected set; }

    /// <summary>
    /// Everything this batch wired, in the shape the driver-level suites consume. Unwired backends stay
    /// null, and asking for one names it in the failure — the signal that a test is in the wrong batch.
    /// </summary>
    public MatrixBackends Backends => new()
    {
        Redis = RedisConnectionString,
        Nats = NatsConnectionString,
        PostgreSql = PostgreSqlConnectionString,
        SqlServer = SqlServerConnectionString,
        MongoDb = MongoDbConnectionString,
        MySql = MySqlConnectionString,
        Kafka = KafkaBootstrapServers,
        RabbitMq = RabbitMqConnectionString,
        LocalStack = LocalStackServiceUrl,
        AzureServiceBus = AzureServiceBusConnectionString,
        PubSubProjectId = PubSubProjectId,
        Oracle = Environment.GetEnvironmentVariable(OracleConnectionStringEnvironmentVariable),
        Cosmos = Environment.GetEnvironmentVariable(CosmosConnectionStringEnvironmentVariable)
    };

    /// <summary>Resolves Kafka and RabbitMQ for a batch that declared them.</summary>
    private protected async Task WireBrokerConnectionStringsAsync()
    {
        KafkaBootstrapServers = await App.GetConnectionStringAsync("kafka")
            ?? throw new InvalidOperationException("The Aspire 'kafka' resource reported no connection string.");
        var amqp = App.GetEndpoint("rabbitmq", "amqp");
        RabbitMqConnectionString = $"amqp://guest:guest@{amqp.Host}:{amqp.Port}/";
    }

    /// <summary>Resolves the Service Bus emulator for a batch that declared it.</summary>
    private protected void WireAzureServiceBusConnectionString()
    {
        var endpoint = App.GetEndpoint("servicebus", "amqp");
        AzureServiceBusConnectionString =
            $"Endpoint=sb://{endpoint.Host}:{endpoint.Port};SharedAccessKeyName=RootManageSharedAccessKey;" +
            "SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;";
    }

    /// <summary>
    /// Points this process at the Pub/Sub emulator. The client libraries discover it only through the
    /// process-wide PUBSUB_EMULATOR_HOST variable — there is no per-client override — so a batch that
    /// wants driver-level Pub/Sub has to set it.
    /// </summary>
    private protected void WirePubSubEmulator(string projectId)
    {
        var endpoint = App.GetEndpoint("pubsub", "pubsub");
        Environment.SetEnvironmentVariable("PUBSUB_EMULATOR_HOST", $"{endpoint.Host}:{endpoint.Port}");
        PubSubProjectId = projectId;
    }

    public async ValueTask InitializeAsync()
    {
        _previousOracleConnectionString = Environment.GetEnvironmentVariable(OracleConnectionStringEnvironmentVariable);
        _previousCosmosConnectionString = Environment.GetEnvironmentVariable(CosmosConnectionStringEnvironmentVariable);

        // DistributedApplicationTestingBuilder invokes the AppHost's entry point *in this process*, so
        // the AppHost reads this variable straight out of our environment. Setting it here is only safe
        // because batches never run concurrently — see the class remarks.
        _previousBatch = Environment.GetEnvironmentVariable(BatchEnvironmentVariable);
        Environment.SetEnvironmentVariable(BatchEnvironmentVariable, Batch);

        var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.AsyncResponse_IntegrationTests_AppHost>();
        _app = await appHost.BuildAsync().WaitAsync(StartupTimeout);
        await _app.StartAsync().WaitAsync(StartupTimeout);

        await WireAsync();
    }

    /// <summary>Resolves the endpoints, clients, and connection strings this batch declared.</summary>
    protected abstract ValueTask WireAsync();

    /// <summary>The running AppHost. Only valid from <see cref="WireAsync"/> onward.</summary>
    private protected DistributedApplication App =>
        _app ?? throw new InvalidOperationException("The AppHost has not been started yet.");

    /// <summary>
    /// Waits for each sample app's <c>/alive</c> probe, then hands back a client per app and resets
    /// its test state. Waiting for an app the batch did not declare hangs until the startup timeout,
    /// so the names here must match what the AppHost's branch for this batch actually added.
    /// </summary>
    private protected async Task<HttpClient[]> WireAppsAsync(params string[] names)
    {
        foreach (var name in names)
            await App.ResourceNotifications.WaitForResourceHealthyAsync(name).WaitAsync(StartupTimeout);

        var clients = names.Select(name => App.CreateHttpClient(name)).ToArray();
        foreach (var client in clients)
            await ResetTestStateAsync(client).WaitAsync(StartupTimeout);

        return clients;
    }

    private protected async Task WireRedisConnectionStringAsync()
        // AddRedis is a full Aspire resource, so ask it for its connection string (it may carry a
        // password) rather than composing one from the endpoint.
        => RedisConnectionString = await App.GetConnectionStringAsync("redis")
            ?? throw new InvalidOperationException("The Aspire 'redis' resource reported no connection string.");

    private protected void WireNatsConnectionString()
    {
        var endpoint = App.GetEndpoint("nats", "nats");
        NatsConnectionString = $"nats://{endpoint.Host}:{endpoint.Port}";
    }

    // --- Per-resource wiring ------------------------------------------------------------------
    // Each batch calls only the ones whose containers it declared; calling one for an absent
    // resource throws from GetEndpoint, which is the signal that a test landed in the wrong batch.

    private protected void WirePostgreSqlConnectionString()
    {
        var endpoint = App.GetEndpoint("postgres", "postgres");
        PostgreSqlConnectionString =
            $"Host={endpoint.Host};Port={endpoint.Port};Username=postgres;Password=postgres;Database=asyncresponse;" +
            "Maximum Pool Size=40;No Reset On Close=true;Max Auto Prepare=20";
    }

    private protected void WireMySqlConnectionString()
    {
        var endpoint = App.GetEndpoint("mysql", "mysql");
        MySqlConnectionString =
            $"Server={endpoint.Host};Port={endpoint.Port};User ID=root;Password=mysql;Database=asyncresponse;Connection Timeout=30;";
    }

    // directConnection: the container is a single-node replica set (change streams need one), and
    // without it the driver would chase the replica-set-advertised container hostname.
    private protected void WireMongoDbConnectionString()
    {
        var endpoint = App.GetEndpoint("mongodb", "mongodb");
        MongoDbConnectionString = $"mongodb://{endpoint.Host}:{endpoint.Port}/?directConnection=true";
    }

    private protected void WireSqlServerConnectionString()
    {
        var endpoint = App.GetEndpoint("sqlserver", "sqlserver");
        SqlServerConnectionString =
            $"Server={endpoint.Host},{endpoint.Port};User ID=sa;Password={Env("ASYNCRESPONSE_ITEST_SQLSERVER_PASSWORD", "P@ssword12345")};" +
            "Database=asyncresponse;TrustServerCertificate=True;Max Pool Size=40";
    }

    private protected void WireLocalStackServiceUrl()
    {
        var endpoint = App.GetEndpoint("localstack", "edge");
        LocalStackServiceUrl = $"http://{endpoint.Host}:{endpoint.Port}";
    }

    /// <summary>
    /// The AppHost omits the Oracle and Cosmos containers when ASYNCRESPONSE_ITEST_SKIP_ORACLE_COSMOS
    /// resolves to skip (their endpoints then don't exist), and a user-supplied connection string
    /// always wins over the container-derived one. When neither var ends up set, the opt-in
    /// durable-flow store contract tests Assert.Skip.
    /// </summary>
    private protected void WireOracleAndCosmosConnectionStrings()
    {
        WireOracleConnectionString();
        WireCosmosConnectionString();
    }

    /// <summary>
    /// Oracle alone. The cross-product shards declare exactly one of the two heavyweight containers —
    /// together they are 3.2 GiB — so they wire them one at a time; asking Aspire for an endpoint the
    /// batch never declared throws.
    /// </summary>
    private protected void WireOracleConnectionString()
    {
        if (SkipOracleCosmos() || !string.IsNullOrEmpty(_previousOracleConnectionString))
            return;

        var oracleEndpoint = App.GetEndpoint("oracle", "oracle");
        Environment.SetEnvironmentVariable(
            OracleConnectionStringEnvironmentVariable,
            $"User Id=asyncresponse;Password={Env("ASYNCRESPONSE_ITEST_ORACLE_APP_PASSWORD", "AsyncResponse12345")};Data Source={oracleEndpoint.Host}:{oracleEndpoint.Port}/FREEPDB1;");
    }

    /// <summary>Cosmos alone. See <see cref="WireOracleConnectionString"/> for why they are separable.</summary>
    private protected void WireCosmosConnectionString()
    {
        if (SkipOracleCosmos() || !string.IsNullOrEmpty(_previousCosmosConnectionString))
            return;

        var cosmosEndpoint = App.GetEndpoint("cosmos", "gateway");
        Environment.SetEnvironmentVariable(
            CosmosConnectionStringEnvironmentVariable,
            $"AccountEndpoint=https://{cosmosEndpoint.Host}:{cosmosEndpoint.Port}/;AccountKey={CosmosEmulatorAccountKey};");
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
            Environment.SetEnvironmentVariable(BatchEnvironmentVariable, _previousBatch);
        }
    }

    /// <summary>
    /// Mirrors the AppHost's decision — the two must agree, because the endpoints only exist when the
    /// AppHost created the containers. Oracle and Cosmos run by default everywhere (both images ship
    /// linux/arm64 and boot natively on Apple silicon); set the flag to "true" to leave them out.
    /// </summary>
    private static bool SkipOracleCosmos()
        => string.Equals(
            Env("ASYNCRESPONSE_ITEST_SKIP_ORACLE_COSMOS", "false"),
            "true",
            StringComparison.OrdinalIgnoreCase);

    private protected static string Env(string name, string fallback)
        => Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : fallback;

    private static async Task ResetTestStateAsync(HttpClient client)
    {
        var response = await client.PostAsync("/test/reset", content: null);
        response.EnsureSuccessStatusCode();
    }
}
