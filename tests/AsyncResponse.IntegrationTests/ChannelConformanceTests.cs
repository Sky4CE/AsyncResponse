using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Channels.NATS;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.Redis;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Conformance;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
using NATS.Client.Core;
using Npgsql;
using StackExchange.Redis;
using Xunit;

namespace AsyncResponse.IntegrationTests;

// The channel behavioral contract (see ChannelConformanceSuite) run "Direct"-style against the real
// containers the Aspire fixture boots: each fact builds an in-test-process DI provider with the
// channel package registered exactly the way an application would, pointed at the container. Every
// harness isolates itself with a GUID-suffixed key prefix / subject prefix / schema / database, so
// the classes are re-runnable and safe alongside the SUT-app-driven suites sharing the containers.

/// <summary>Contract run against real Redis (pub/sub channel + recovery keys).</summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class RedisChannelConformanceTests(DataBatchFixture fixture) : ChannelConformanceSuite
{
    // Broker round-trips over loopback: well under a second healthy; 5s absorbs CI noise.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(5);

    protected override async Task<ChannelConformanceHarness> CreateHarnessAsync(
        Action<AsyncResponseChannelOptions>? configureChannel = null)
    {
        var multiplexer = await ConnectionMultiplexer.ConnectAsync(fixture.RedisConnectionString);
        var services = NewConformanceServices();
        services.AddSingleton<IConnectionMultiplexer>(multiplexer);
        services.AddAsyncResponse().WithRedisChannel(options =>
        {
            // Unique prefix per harness: keys and pub/sub channels never collide across facts or
            // reruns; recovery keys expire via their TTL.
            options.KeyPrefix = $"conformance:{Guid.NewGuid():N}";
            options.RecoveryStateExpiry = TimeSpan.FromMinutes(2);
            configureChannel?.Invoke(options);
        });
        return new ChannelConformanceHarness(services.BuildServiceProvider(), () =>
        {
            multiplexer.Dispose();
            return ValueTask.CompletedTask;
        });
    }
}

/// <summary>Contract run against real NATS (core request/reply channel + JetStream KV recovery).</summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class NatsChannelConformanceTests(DataBatchFixture fixture) : ChannelConformanceSuite
{
    protected override TimeSpan Generous => TimeSpan.FromSeconds(5);

    protected override async Task<ChannelConformanceHarness> CreateHarnessAsync(
        Action<AsyncResponseChannelOptions>? configureChannel = null)
    {
        var connection = new NatsConnection(new NatsOpts { Url = fixture.NatsConnectionString });
        await connection.ConnectAsync();

        var services = NewConformanceServices();
        services.AddSingleton<INatsConnection>(connection);
        var runId = Guid.NewGuid().ToString("N");
        services.AddAsyncResponse().WithNatsChannel(options =>
        {
            // Unique subject prefix + KV bucket per harness; the container is discarded with the
            // fixture, so per-run buckets do not accumulate beyond one fixture lifetime.
            options.SubjectPrefix = $"conformance.{runId}";
            options.RecoveryBucket = $"conformance-{runId}";
            options.RecoveryStateExpiry = TimeSpan.FromMinutes(2);
            configureChannel?.Invoke(options);
        });
        return new ChannelConformanceHarness(
            services.BuildServiceProvider(),
            async () => await connection.DisposeAsync());
    }
}

/// <summary>Contract run against real PostgreSQL (LISTEN/NOTIFY + tables channel).</summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class PostgreSqlChannelConformanceTests(DataBatchFixture fixture) : ChannelConformanceSuite
{
    // Polling database channel under the loaded shared container: 15s budget.
    protected override TimeSpan Generous => TimeSpan.FromSeconds(15);

    protected override Task<ChannelConformanceHarness> CreateHarnessAsync(
        Action<AsyncResponseChannelOptions>? configureChannel = null)
    {
        var schema = $"ar_conformance_{Guid.NewGuid():N}";
        var dataSource = NpgsqlDataSource.Create(fixture.PostgreSqlConnectionString);
        var services = NewConformanceServices();
        services.AddSingleton(dataSource);
        services.AddAsyncResponse().WithPostgreSqlChannel(options =>
        {
            options.SchemaName = schema;
            options.NotificationChannel = $"{schema}_channel";
            options.RecoveryStateExpiry = TimeSpan.FromMinutes(2);
            options.MessageRetention = TimeSpan.FromMinutes(2);
            // Delivery confirmation stays generous (a too-tight budget steals live-but-slow local
            // deliveries to the recovery path under a loaded CI database); polling is aggressive so
            // the suite runs fast.
            options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5);
            options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(10);
            options.ListenerPollInterval = TimeSpan.FromMilliseconds(25);
            options.SubscriberHeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(2);
            options.PruneInterval = TimeSpan.Zero;
            configureChannel?.Invoke(options);
        });
        return Task.FromResult(new ChannelConformanceHarness(services.BuildServiceProvider(), async () =>
        {
            try
            {
                await using var connection = await dataSource.OpenConnectionAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE;";
                await command.ExecuteNonQueryAsync();
            }
            finally
            {
                await dataSource.DisposeAsync();
            }
        }));
    }
}

/// <summary>Contract run against real SQL Server (adaptive-polling channel).</summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class SqlServerChannelConformanceTests(DataBatchFixture fixture) : ChannelConformanceSuite
{
    protected override TimeSpan Generous => TimeSpan.FromSeconds(15);

    protected override Task<ChannelConformanceHarness> CreateHarnessAsync(
        Action<AsyncResponseChannelOptions>? configureChannel = null)
    {
        var schema = $"ar_conformance_{Guid.NewGuid():N}";
        var services = NewConformanceServices();
        services.AddAsyncResponse().WithSqlServerChannel(options =>
        {
            options.ConnectionString = fixture.SqlServerConnectionString;
            options.SchemaName = schema;
            options.RecoveryStateExpiry = TimeSpan.FromMinutes(2);
            options.MessageRetention = TimeSpan.FromMinutes(2);
            options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5);
            options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(10);
            options.ActivePollInterval = TimeSpan.FromMilliseconds(25);
            options.IdlePollInterval = TimeSpan.FromMilliseconds(100);
            options.SubscriberHeartbeatInterval = TimeSpan.FromMilliseconds(50);
            options.SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(2);
            options.PruneInterval = TimeSpan.Zero;
            configureChannel?.Invoke(options);
        });
        return Task.FromResult(new ChannelConformanceHarness(services.BuildServiceProvider(), async () =>
        {
            // SQL Server has no DROP SCHEMA ... CASCADE: drop the schema's tables and sequences
            // (the channel's monotonic ack sequence) first, then the schema.
            await using var connection = new SqlConnection(fixture.SqlServerConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                """
                DECLARE @drop nvarchar(max) = N'';
                SELECT @drop += N'DROP TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N';'
                FROM sys.tables t
                JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = @schema;
                SELECT @drop += N'DROP SEQUENCE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(seq.name) + N';'
                FROM sys.sequences seq
                JOIN sys.schemas s ON seq.schema_id = s.schema_id
                WHERE s.name = @schema;
                IF SCHEMA_ID(@schema) IS NOT NULL
                    SET @drop += N'DROP SCHEMA ' + QUOTENAME(@schema) + N';';
                EXEC sp_executesql @drop;
                """;
            command.Parameters.AddWithValue("@schema", schema);
            await command.ExecuteNonQueryAsync();
        }));
    }
}

/// <summary>Contract run against real MongoDB (change-stream channel on the single-node replica set).</summary>
[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class MongoDbChannelConformanceTests(DataBatchFixture fixture) : ChannelConformanceSuite
{
    protected override TimeSpan Generous => TimeSpan.FromSeconds(15);

    protected override Task<ChannelConformanceHarness> CreateHarnessAsync(
        Action<AsyncResponseChannelOptions>? configureChannel = null)
    {
        var databaseName = $"conformance_{Guid.NewGuid():N}";
        var client = new MongoClient(fixture.MongoDbConnectionString);
        var services = NewConformanceServices();
        services.AddSingleton<IMongoClient>(client);
        services.AddAsyncResponse().WithMongoDbChannel(options =>
        {
            // Unique database per harness; dropped on teardown.
            options.DatabaseName = databaseName;
            options.RecoveryStateExpiry = TimeSpan.FromMinutes(2);
            options.MessageRetention = TimeSpan.FromMinutes(2);
            options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5);
            options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(10);
            options.ListenerPollInterval = TimeSpan.FromMilliseconds(50);
            options.SubscriberHeartbeatInterval = TimeSpan.FromMilliseconds(100);
            options.SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(2);
            configureChannel?.Invoke(options);
        });
        return Task.FromResult(new ChannelConformanceHarness(services.BuildServiceProvider(), async () =>
        {
            try
            {
                await client.DropDatabaseAsync(databaseName);
            }
            finally
            {
                client.Dispose();
            }
        }));
    }
}
