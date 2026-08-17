using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using AsyncResponse.DurableFlows.Cosmos;
using AsyncResponse.DurableFlows.DynamoDB;
using AsyncResponse.DurableFlows.EFCore;
using AsyncResponse.DurableFlows.MongoDB;
using AsyncResponse.DurableFlows.MySql;
using AsyncResponse.DurableFlows.Oracle;
using AsyncResponse.DurableFlows.PostgreSQL;
using AsyncResponse.DurableFlows.SqlServer;
using AsyncResponse.DurableFlows.Sqlite;
using Microsoft.Azure.Cosmos;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using System.Text.Json;
using Xunit;
using static AsyncResponse.IntegrationTests.FlowStoreContract;

namespace AsyncResponse.IntegrationTests;

[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class DurableFlowStateStorePackageIntegrationTests(DataBatchFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task SqlitePackageStore_RoundTrips_Expires_Deletes()
    {
        var databasePath = Path.Combine(Path.GetTempPath(), $"ar-durable-flow-itest-{Guid.NewGuid():N}.db");
        try
        {
            var store = new SqliteFlowStateStore(
                Options.Create(new SqliteDurableFlowOptions
                {
                    ConnectionString = $"Data Source={databasePath}",
                    TableName = "flow_state"
                }));

            await AssertStoreContractAsync(store);
        }
        finally
        {
            File.Delete(databasePath);
        }
    }

    [Fact]
    public async Task PostgreSqlPackageStore_RoundTrips_Expires_Deletes()
    {
        var schema = NewIdentifier("df_pg", 32);
        await using var dataSource = NpgsqlDataSource.Create(Fixture.PostgreSqlConnectionString);
        try
        {
            var store = new PostgreSqlFlowStateStore(
                dataSource,
                Options.Create(new PostgreSqlDurableFlowOptions
                {
                    SchemaName = schema,
                    TableName = "flow_state"
                }));

            await AssertStoreContractAsync(store, seedRawStateAsync: async (flowId, stateJson) =>
            {
                await using var connection = await dataSource.OpenConnectionAsync();
                await using var seed = connection.CreateCommand();
                seed.CommandText =
                    $"""INSERT INTO "{schema}"."flow_state" (flow_id, state_json, expires_at_utc, updated_at_utc, revision) VALUES (@id, @json::jsonb, now() + interval '5 minutes', now(), 0);""";
                seed.Parameters.AddWithValue("id", flowId);
                seed.Parameters.AddWithValue("json", stateJson);
                await seed.ExecuteNonQueryAsync();
            });

        }
        finally
        {
            await using var cleanup = await dataSource.OpenConnectionAsync();
            await using var command = cleanup.CreateCommand();
            command.CommandText = $"""DROP SCHEMA IF EXISTS "{schema}" CASCADE;""";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task SqlServerPackageStore_RoundTrips_Expires_Deletes()
    {
        var schema = NewIdentifier("df_sql", 32);
        try
        {
            var store = new SqlServerFlowStateStore(
                Options.Create(new SqlServerDurableFlowOptions
                {
                    ConnectionString = Fixture.SqlServerConnectionString,
                    SchemaName = schema,
                    TableName = "flow_state"
                }));

            await AssertStoreContractAsync(store, seedRawStateAsync: async (flowId, stateJson) =>
            {
                await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
                await connection.OpenAsync();
                await using var seed = connection.CreateCommand();
                seed.CommandText =
                    $"INSERT INTO [{schema}].[flow_state] (flow_id, state_json, expires_at_utc, updated_at_utc, revision) " +
                    "VALUES (@id, @json, DATEADD(MINUTE, 5, SYSUTCDATETIME()), SYSUTCDATETIME(), 0);";
                seed.Parameters.AddWithValue("@id", flowId);
                seed.Parameters.AddWithValue("@json", stateJson);
                await seed.ExecuteNonQueryAsync();
            });
        }
        finally
        {
            await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"""
                IF OBJECT_ID(N'{schema}.flow_state', N'U') IS NOT NULL
                    DROP TABLE [{schema}].[flow_state];
                IF SCHEMA_ID(N'{schema}') IS NOT NULL
                    EXEC(N'DROP SCHEMA [{schema}]');
                """;
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task EFCorePackageStore_RoundTrips_Expires_Deletes_AndSurvivesStorm()
    {
        var schema = NewIdentifier("df_ef", 32);
        var services = new ServiceCollection();
        // The mapping is the application's, and so is the collation: this context points at SQL
        // Server, whose default collation is case-insensitive and would fold two distinct flow ids
        // onto one primary key. The contract below is what proves the seam actually works.
        services.AddSingleton(new EFCoreFlowSchema(schema));
        services.AddDbContextFactory<EFCoreFlowDbContext>(options => options.UseSqlServer(Fixture.SqlServerConnectionString));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        try
        {
            // The store never runs DDL: create the table exactly the way an application migration
            // would — from the ConfigureAsyncResponseDurableFlows model mapping.
            var factory = provider.GetRequiredService<IDbContextFactory<EFCoreFlowDbContext>>();
            await using (var context = await factory.CreateDbContextAsync())
                await context.GetService<IRelationalDatabaseCreator>().CreateTablesAsync();

            var store = new EFCoreFlowStateStore<EFCoreFlowDbContext>(
                provider.GetRequiredService<IServiceScopeFactory>(),
                Options.Create(new EFCoreDurableFlowOptions()));

            await AssertStoreContractAsync(store);

            // Concurrency storm: parallel save/load/delete against the real database must never
            // share a DbContext (the store leases one per operation).
            await Parallel.ForEachAsync(
                Enumerable.Range(0, 64),
                new ParallelOptions { MaxDegreeOfParallelism = 8 },
                async (i, _) =>
                {
                    var flowId = $"flow-storm-{i}";
                    Assert.True(await store.TryCreateAsync(flowId, CreateState(flowId), TimeSpan.FromMinutes(5)));
                    Assert.NotNull(await store.LoadAsync(flowId));
                    Assert.True(await store.TryDeleteAsync(flowId));
                    Assert.Null(await store.LoadAsync(flowId));
                });
        }
        finally
        {
            await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"""
                IF OBJECT_ID(N'{schema}.asyncresponse_flow_state', N'U') IS NOT NULL
                    DROP TABLE [{schema}].[asyncresponse_flow_state];
                IF SCHEMA_ID(N'{schema}') IS NOT NULL
                    EXEC(N'DROP SCHEMA [{schema}]');
                """;
            await command.ExecuteNonQueryAsync();
        }
    }

    private sealed record EFCoreFlowSchema(string Name);

    private sealed class EFCoreFlowDbContext(DbContextOptions<EFCoreFlowDbContext> options, EFCoreFlowSchema schema)
        : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.ConfigureAsyncResponseDurableFlows(
                schema: schema.Name,
                flowIdCollation: AsyncResponseFlowIdCollations.SqlServer);
    }

    [Fact]
    public async Task MySqlPackageStore_RoundTrips_Expires_Deletes()
    {
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql", 64);
        try
        {
            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table
                }));

            await AssertStoreContractAsync(store);
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task MySqlPackageStore_RejectsAnExistingCaseInsensitiveFlowIdColumn(bool autoCreateSchema)
    {
        // The COLLATE clause in this store's DDL only ever protects a table THIS build created:
        // CREATE TABLE IF NOT EXISTS leaves an earlier build's table exactly as it was, and
        // AutoCreateSchema = false issues no DDL at all. MySQL's default collation is
        // case-insensitive (utf8mb4_0900_ai_ci on 8.x), which makes two flow ids differing only in
        // case one primary key — so the effective collation is verified through information_schema
        // on both paths, not inferred from having run the DDL.
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql_ci", 64);
        try
        {
            await using (var connection = new MySqlConnection(Fixture.MySqlConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    CREATE TABLE `{table}` (
                        flow_id varchar(400) NOT NULL PRIMARY KEY,
                        state_json longtext NOT NULL,
                        expires_at_utc datetime(6) NOT NULL,
                        updated_at_utc datetime(6) NOT NULL,
                        revision bigint NOT NULL DEFAULT 0,
                        lease_id varchar(64) NULL,
                        lease_expires_at_utc datetime(6) NULL,
                        INDEX `{table}_expires_idx` (expires_at_utc)
                    );
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table,
                    AutoCreateSchema = autoCreateSchema
                }));

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.TryCreateAsync("case-check", CreateState("case-check"), TimeSpan.FromMinutes(5)));
            Assert.Contains("is not binary", exception.Message, StringComparison.Ordinal);
            Assert.Contains("COLLATE utf8mb4_bin", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task MySqlPackageStore_RejectsIncompleteExistingSchema()
    {
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql_legacy", 64);
        try
        {
            await using (var connection = new MySqlConnection(Fixture.MySqlConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    CREATE TABLE `{table}` (
                        flow_id varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
                        state_json longtext NOT NULL,
                        expires_at_utc datetime(6) NOT NULL,
                        updated_at_utc datetime(6) NOT NULL,
                        INDEX `{table}_expires_idx` (expires_at_utc)
                    );
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table
                }));

            // A raw provider error ("Unknown column 'revision'") tells the operator what broke but
            // not what to do; startup verification names the shape it needs instead.
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.TryCreateAsync("incomplete", CreateState("incomplete"), TimeSpan.FromMinutes(5)));
            Assert.Contains("no 'revision' column", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task MySqlPackageStore_RoundTripsAFlowIdWithSupplementaryCharacters()
    {
        // The reason the character-set check exists, stated as a fact rather than as a rejection:
        // an emoji is a four-byte utf8mb4 character, and this store's own DDL has to actually carry
        // one. On the latin1 table the sibling test refuses, this id would fail or be mangled on
        // insert — and on MySQL's older three-byte `utf8` it fails too, which is why the check
        // names utf8mb4 specifically instead of "any Unicode set".
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql_astral", 64);
        try
        {
            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table
                }));

            var flowId = "flow-\U0001F600-世";
            Assert.True(await store.TryCreateAsync(flowId, CreateState(flowId), TimeSpan.FromMinutes(5)));

            var loaded = await store.LoadAsync(flowId);
            Assert.NotNull(loaded);
            // And it is still the SAME key: a mangled id would read back as a different flow, so a
            // second create would succeed instead of reporting the duplicate.
            Assert.False(await store.TryCreateAsync(flowId, CreateState(flowId), TimeSpan.FromMinutes(5)));
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task MySqlPackageStore_RejectsANarrowCharacterSetOnStateJson()
    {
        // The ledger JSON needs the same alphabet as the state it embeds: on a latin1-default
        // server a table that inherited the database charset stores state_json in latin1, so any
        // non-Latin-1 state (a name, an emoji in a step result) hard-fails every update under
        // strict mode or is silently truncated at the first bad byte otherwise — malformed JSON
        // that deserializes to null, a flow that can neither load nor be re-created. Before r22
        // only flow_id's character set was verified; a compliant flow_id column hid a latin1
        // state_json.
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql_json1", 64);
        try
        {
            await using (var connection = new MySqlConnection(Fixture.MySqlConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    CREATE TABLE `{table}` (
                        flow_id varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
                        state_json longtext CHARACTER SET latin1 NOT NULL,
                        expires_at_utc datetime(6) NOT NULL,
                        updated_at_utc datetime(6) NOT NULL,
                        revision bigint NOT NULL DEFAULT 0,
                        lease_id varchar(64) NULL,
                        lease_expires_at_utc datetime(6) NULL
                    );
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table,
                    AutoCreateSchema = false
                }));

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.TryCreateAsync("json", CreateState("json"), TimeSpan.FromMinutes(5)));
            Assert.Contains("state_json", ex.Message, StringComparison.Ordinal);
            Assert.Contains("character set 'latin1'", ex.Message, StringComparison.Ordinal);
            Assert.Contains("utf8mb4", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task MySqlPackageStore_AutoCreateSchema_RepairsAPreR22StateJsonCharsetInPlace()
    {
        // Regression (r23): the state_json charset verification hard-failed every table this
        // library ITSELF created before the charset was pinned — pre-r22 DDL declared state_json
        // with no CHARACTER SET, inheriting the server default. CREATE TABLE IF NOT EXISTS cannot
        // alter an existing table, so an upgrade had no way forward short of a manual ALTER on a
        // table with live rows. Under AutoCreateSchema the store now repairs the column in place
        // (MODIFY converts the stored text) and carries on; operator-managed schemas keep the
        // throw, pinned by MySqlPackageStore_RejectsANarrowCharacterSetOnStateJson above.
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql_repair", 64);
        try
        {
            await using (var connection = new MySqlConnection(Fixture.MySqlConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                // The pre-r22 shape: flow_id was already pinned, state_json inherited the server
                // default — modeled here as latin1 so the test is deterministic on any server.
                command.CommandText =
                    $"""
                    CREATE TABLE `{table}` (
                        flow_id varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
                        state_json longtext CHARACTER SET latin1 NOT NULL,
                        expires_at_utc datetime(6) NOT NULL,
                        updated_at_utc datetime(6) NOT NULL,
                        revision bigint NOT NULL DEFAULT 0,
                        lease_id varchar(64) NULL,
                        lease_expires_at_utc datetime(6) NULL,
                        INDEX `{table}_expires_idx` (expires_at_utc)
                    );
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table,
                    AutoCreateSchema = true
                }));

            // On the old code this threw the state_json charset error before any operation ran.
            Assert.True(await store.TryCreateAsync("repair", CreateState("repair"), TimeSpan.FromMinutes(5)));

            await using (var verify = new MySqlConnection(Fixture.MySqlConnectionString))
            {
                await verify.OpenAsync();
                await using var check = verify.CreateCommand();
                check.CommandText =
                    """
                    SELECT CHARACTER_SET_NAME FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @table AND COLUMN_NAME = 'state_json';
                    """;
                check.Parameters.AddWithValue("@table", table);
                Assert.Equal("utf8mb4", (string?)await check.ExecuteScalarAsync());
            }
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task MySqlPackageStore_RejectsANarrowCharacterSetOnFlowId()
    {
        // latin1_bin ends in _bin and passes the collation check, yet the column holds almost
        // nothing: an emoji, a Han character, most non-Latin text — every one of them a legal flow
        // id — fails or is mangled on insert. Character set and collation are two questions, and
        // only asking the second one let this schema through.
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql_latin1", 64);
        try
        {
            await using (var connection = new MySqlConnection(Fixture.MySqlConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    CREATE TABLE `{table}` (
                        flow_id varchar(400) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL PRIMARY KEY,
                        state_json longtext NOT NULL,
                        expires_at_utc datetime(6) NOT NULL,
                        updated_at_utc datetime(6) NOT NULL,
                        revision bigint NOT NULL DEFAULT 0,
                        lease_id varchar(64) NULL,
                        lease_expires_at_utc datetime(6) NULL
                    );
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table,
                    AutoCreateSchema = false
                }));

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.TryCreateAsync("latin", CreateState("latin"), TimeSpan.FromMinutes(5)));
            Assert.Contains("character set 'latin1'", ex.Message, StringComparison.Ordinal);
            Assert.Contains("utf8mb4", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Theory]
    // An extra NOT NULL column with no default: this store never names it, so EVERY create fails —
    // at the first flow, not at startup, which is the wrong end of the deployment.
    [InlineData("tenant_id bigint NOT NULL", true)]
    // The same column the database can fill in for itself is harmless, and refusing it would stop
    // applications from adding perfectly reasonable bookkeeping to their own table.
    [InlineData("tenant_id bigint NOT NULL DEFAULT 0", false)]
    [InlineData("tenant_id bigint NULL", false)]
    [InlineData("flow_id_len int GENERATED ALWAYS AS (CHAR_LENGTH(flow_id)) STORED NOT NULL", false)]
    public async Task MySqlPackageStore_RejectsOnlyExtraColumnsItCannotLeaveUnwritten(string extraColumn, bool rejected)
    {
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql_extra", 64);
        try
        {
            await using (var connection = new MySqlConnection(Fixture.MySqlConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    CREATE TABLE `{table}` (
                        flow_id varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
                        state_json longtext NOT NULL,
                        expires_at_utc datetime(6) NOT NULL,
                        updated_at_utc datetime(6) NOT NULL,
                        revision bigint NOT NULL DEFAULT 0,
                        lease_id varchar(64) NULL,
                        lease_expires_at_utc datetime(6) NULL,
                        {extraColumn}
                    );
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table,
                    AutoCreateSchema = false
                }));

            if (rejected)
            {
                var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                    () => store.TryCreateAsync("extra", CreateState("extra"), TimeSpan.FromMinutes(5)));
                Assert.Contains("extra column 'tenant_id'", ex.Message, StringComparison.Ordinal);
            }
            else
            {
                Assert.True(await store.TryCreateAsync("extra", CreateState("extra"), TimeSpan.FromMinutes(5)));
            }
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task MySqlPackageStore_DoesNotReportADuplicateWhenSomeOtherKeyRaised1062()
    {
        // A table with BOTH the required key and a legacy prefix one. Startup verification is happy
        // — the full key it needs is there — but the prefix key still fires 1062 for a DIFFERENT id
        // that happens to share the first 100 characters. Reading every 1062 as "this flow already
        // exists" would report a successful start for a flow with no row and no run: the caller
        // gets false, believes another replica owns it, and nothing ever executes it.
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql_1062", 64);
        try
        {
            await using (var connection = new MySqlConnection(Fixture.MySqlConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    CREATE TABLE `{table}` (
                        flow_id varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
                        state_json longtext NOT NULL,
                        expires_at_utc datetime(6) NOT NULL,
                        updated_at_utc datetime(6) NOT NULL,
                        revision bigint NOT NULL DEFAULT 0,
                        lease_id varchar(64) NULL,
                        lease_expires_at_utc datetime(6) NULL,
                        UNIQUE KEY `{table}_legacy_prefix` (flow_id(100))
                    );
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table,
                    AutoCreateSchema = false
                }));

            var shared = new string('p', 100);
            Assert.True(await store.TryCreateAsync(shared + "-first", CreateState(shared + "-first"), TimeSpan.FromMinutes(5)));

            // Different flow, same first 100 characters. The prefix key rejects it — and the store
            // must say so rather than claim the ledger already exists.
            var second = shared + "-second";
            await Assert.ThrowsAsync<MySqlException>(
                () => store.TryCreateAsync(second, CreateState(second), TimeSpan.FromMinutes(5)));
            Assert.Null(await store.LoadAsync(second));
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task MySqlPackageStore_AcceptsAMoreGenerousColumnShape()
    {
        // The false-positive guard for the shape check: widths and precisions are MINIMA, not exact
        // matches. A schema that gives flow_id more room than the contract needs, or a wider
        // lease_id, satisfies every promise the store makes — and had the check been written as
        // equality it would pass every rejection case above while failing this perfectly good
        // table at startup, which is the worse bug of the two.
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql_wide", 64);
        try
        {
            await using (var connection = new MySqlConnection(Fixture.MySqlConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    CREATE TABLE `{table}` (
                        flow_id varchar(700) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
                        state_json longtext NOT NULL,
                        expires_at_utc datetime(6) NOT NULL,
                        updated_at_utc datetime(6) NOT NULL,
                        revision bigint NOT NULL DEFAULT 0,
                        lease_id varchar(128) NULL,
                        lease_expires_at_utc datetime(6) NULL,
                        INDEX `{table}_expires_idx` (expires_at_utc)
                    );
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table,
                    AutoCreateSchema = false
                }));

            // Starts, and works: the whole ledger contract on the more generous shape.
            Assert.True(await store.TryCreateAsync("wide", CreateState("wide"), TimeSpan.FromMinutes(5)));
            Assert.NotNull(await store.LoadAsync("wide"));
            Assert.True(await store.TryAcquireLeaseAsync("wide", "owner", TimeSpan.FromMinutes(1)));
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task MySqlPackageStore_AcceptsAUniqueIndexInsteadOfAPrimaryKey()
    {
        // The false-positive guard for the rejection below, and the reason the check asks about
        // unique KEYS rather than the PRIMARY one: any single-column unique index raises the 1062
        // that TryCreateAsync reads as "already exists", so a table keyed that way is correct and
        // must start. A check that looked for INDEX_NAME = 'PRIMARY' would fail every one of them
        // at startup — a worse outcome than the bug it fixes.
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql_uq", 64);
        try
        {
            await using (var connection = new MySqlConnection(Fixture.MySqlConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    CREATE TABLE `{table}` (
                        flow_id varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                        state_json longtext NOT NULL,
                        expires_at_utc datetime(6) NOT NULL,
                        updated_at_utc datetime(6) NOT NULL,
                        revision bigint NOT NULL DEFAULT 0,
                        lease_id varchar(64) NULL,
                        lease_expires_at_utc datetime(6) NULL,
                        UNIQUE KEY `{table}_flow_uq` (flow_id),
                        INDEX `{table}_expires_idx` (expires_at_utc)
                    );
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table,
                    AutoCreateSchema = false
                }));

            // Starts, and the insert-if-absent contract actually holds on this shape.
            Assert.True(await store.TryCreateAsync("uq", CreateState("uq"), TimeSpan.FromMinutes(5)));
            Assert.False(await store.TryCreateAsync("uq", CreateState("uq"), TimeSpan.FromMinutes(5)));
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task MySqlPackageStore_DoesNotFailStartupWhenTheManualTableHasNotBeenCreatedYet()
    {
        // The documented "provision it yourself, later" workflow: with AutoCreateSchema off and no
        // table yet, verification has nothing to inspect and must stay out of the way. Failing here
        // would turn a migration that has not run into a startup crash — the table's absence is
        // already reported, clearly, by the first query that needs it.
        await WaitForMySqlAsync();
        var store = new MySqlFlowStateStore(
            Options.Create(new MySqlDurableFlowOptions
            {
                ConnectionString = Fixture.MySqlConnectionString,
                TableName = NewIdentifier("df_mysql_absent", 64),
                AutoCreateSchema = false
            }));

        var ex = await Record.ExceptionAsync(
            () => store.TryCreateAsync("absent", CreateState("absent"), TimeSpan.FromMinutes(5)));

        // MySQL's own "table doesn't exist", not one of this store's verification errors.
        Assert.IsType<MySqlException>(ex);
    }

    [Theory]
    // Narrower than the public flow-id contract: passes a name-only check, then truncates or errors
    // on the first long id. 400 characters is what the engine promises to carry.
    [InlineData("flow_id varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY", "flow_id", "at least 400")]
    // datetime with no sub-second precision: every lease and expiry comparison this store makes runs
    // on UTC_TIMESTAMP(6), so a whole-second column rounds the fencing arithmetic and two workers
    // can hold one lease.
    [InlineData("expires_at_utc datetime NOT NULL", "expires_at_utc", "at least 6")]
    // NOT NULL where the store writes NULL: releasing a lease sets lease_id = NULL and would fail.
    [InlineData("lease_id varchar(64) NOT NULL", "lease_id", "NOT NULL")]
    // Wrong type outright, which is the first rule the check applies: a `text` state_json tops out
    // at 64 KiB where the store writes ledgers into a `longtext`.
    [InlineData("state_json text NOT NULL", "state_json", "is a 'text'")]
    public async Task MySqlPackageStore_RejectsAColumnWhoseShapeCannotServeTheStore(
        string columnDeclaration,
        string columnName,
        string expectedReason)
    {
        // Names alone are not a shape. Every one of these tables has all seven columns with the
        // right names and a binary-collated primary key, and every one of them breaks the store at
        // runtime rather than at startup.
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql_shape", 64);
        var columns = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["flow_id"] = "flow_id varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY",
            ["state_json"] = "state_json longtext NOT NULL",
            ["expires_at_utc"] = "expires_at_utc datetime(6) NOT NULL",
            ["updated_at_utc"] = "updated_at_utc datetime(6) NOT NULL",
            ["revision"] = "revision bigint NOT NULL DEFAULT 0",
            ["lease_id"] = "lease_id varchar(64) NULL",
            ["lease_expires_at_utc"] = "lease_expires_at_utc datetime(6) NULL"
        };
        columns[columnName] = columnDeclaration;

        try
        {
            await using (var connection = new MySqlConnection(Fixture.MySqlConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText = $"CREATE TABLE `{table}` ({string.Join(", ", columns.Values)});";
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table,
                    AutoCreateSchema = false
                }));

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.TryCreateAsync("shape", CreateState("shape"), TimeSpan.FromMinutes(5)));
            Assert.Contains(columnName, ex.Message, StringComparison.Ordinal);
            Assert.Contains(expectedReason, ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Theory]
    // No key at all, and a COMPOSITE primary key: both permit two rows with the same flow_id.
    [InlineData("")]
    [InlineData(", PRIMARY KEY (flow_id, revision)")]
    // A PREFIX key fails the OPPOSITE way: it constrains only the first 100 characters, so two
    // distinct 101-character ids collide on 1062 and the second flow never starts. Prefix keys are
    // a common way to fit an index under MySQL's key-length limit, so this is a plausible schema.
    [InlineData(", UNIQUE KEY flow_prefix_uq (flow_id(100))")]
    public async Task MySqlPackageStore_RejectsATableWithoutAUniqueKeyOnFlowIdAlone(string keyClause)
    {
        // Starting a flow is an insert-if-absent, and this store learns "it already exists" from
        // MySQL's duplicate-key error 1062. Nothing else reports it — so on a table with no unique
        // key on flow_id, two concurrent starts of ONE flow id both INSERT and both return true,
        // and the flow runs twice off two ledgers. The collation check alone passed this table.
        await WaitForMySqlAsync();
        var table = NewIdentifier("df_mysql_nokey", 64);
        try
        {
            await using (var connection = new MySqlConnection(Fixture.MySqlConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    CREATE TABLE `{table}` (
                        flow_id varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                        state_json longtext NOT NULL,
                        expires_at_utc datetime(6) NOT NULL,
                        updated_at_utc datetime(6) NOT NULL,
                        revision bigint NOT NULL DEFAULT 0,
                        lease_id varchar(64) NULL,
                        lease_expires_at_utc datetime(6) NULL,
                        INDEX `{table}_expires_idx` (expires_at_utc){keyClause}
                    );
                    """;
                await command.ExecuteNonQueryAsync();
            }

            var store = new MySqlFlowStateStore(
                Options.Create(new MySqlDurableFlowOptions
                {
                    ConnectionString = Fixture.MySqlConnectionString,
                    TableName = table,
                    // The point of the check: it must not depend on having run the DDL, because a
                    // table this build did not create is precisely the case it exists for.
                    AutoCreateSchema = false
                }));

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.TryCreateAsync("dup", CreateState("dup"), TimeSpan.FromMinutes(5)));
            Assert.Contains("no unique key on the whole of flow_id", ex.Message, StringComparison.Ordinal);
            Assert.Contains("ADD PRIMARY KEY (flow_id)", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS `{table}`;";
            await command.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task MongoDbPackageStore_RoundTrips_Expires_Deletes()
    {
        await WaitForMongoDbAsync();
        var databaseName = NewIdentifier("df_mongo", 63);
        var client = new MongoClient(Fixture.MongoDbConnectionString);
        try
        {
            var store = new MongoDbFlowStateStore(
                client.GetDatabase(databaseName),
                Options.Create(new MongoDbDurableFlowOptions
                {
                    CollectionName = "flow_state"
                }));

            await AssertStoreContractAsync(store, seedRawStateAsync: async (flowId, stateJson) =>
                await client.GetDatabase(databaseName).GetCollection<BsonDocument>("flow_state").InsertOneAsync(new BsonDocument
                {
                    ["_id"] = flowId,
                    ["state_json"] = stateJson,
                    ["expires_at_utc"] = DateTime.UtcNow.AddMinutes(5),
                    ["updated_at_utc"] = DateTime.UtcNow,
                    ["revision"] = 0L
                }));

            var legacyFlowId = "legacy-mongo-flow";
            await client.GetDatabase(databaseName).GetCollection<BsonDocument>("flow_state").InsertOneAsync(new BsonDocument
            {
                ["_id"] = legacyFlowId,
                ["state_json"] = JsonSerializer.Serialize(CreateState(legacyFlowId)),
                ["expires_at_utc"] = DateTime.UtcNow.AddMinutes(5),
                ["updated_at_utc"] = DateTime.UtcNow
            });
            // The row predates the revision column: present, and not interpretable by this build.
            await Assert.ThrowsAsync<FlowStateUnreadableException>(() => store.LoadAsync(legacyFlowId));
            Assert.False(await store.TryAcquireLeaseAsync(legacyFlowId, "owner", TimeSpan.FromMinutes(1)));
        }
        finally
        {
            await client.DropDatabaseAsync(databaseName);
        }
    }

    [Fact]
    public async Task DynamoDbPackageStore_RoundTrips_Expires_Deletes()
    {
        using var client = CreateDynamoDbClient();
        var table = "AsyncResponseFlowState" + Guid.NewGuid().ToString("N");
        try
        {
            var store = new DynamoDbFlowStateStore(
                client,
                Options.Create(new DynamoDbDurableFlowOptions
                {
                    TableName = table
                }));

            // DynamoDB TTL has whole-second granularity and the store now rounds the expiry epoch
            // UP (never shorter than requested), so the read-filter can consider a 1s-TTL item live
            // for up to ~2s after the save — wait past that worst case.
            await AssertStoreContractAsync(store, TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(2500));

            var legacyFlowId = "legacy-dynamo-flow";
            var now = DateTimeOffset.UtcNow;
            await client.PutItemAsync(new PutItemRequest
            {
                TableName = table,
                Item = new Dictionary<string, AttributeValue>
                {
                    ["flow_id"] = new() { S = legacyFlowId },
                    ["state_json"] = new() { S = JsonSerializer.Serialize(CreateState(legacyFlowId)) },
                    ["expires_at"] = new() { N = now.AddMinutes(5).ToUnixTimeSeconds().ToString(System.Globalization.CultureInfo.InvariantCulture) },
                    ["updated_at"] = new() { N = now.ToUnixTimeSeconds().ToString(System.Globalization.CultureInfo.InvariantCulture) }
                }
            });
            // The row predates the revision column: present, and not interpretable by this build.
            await Assert.ThrowsAsync<FlowStateUnreadableException>(() => store.LoadAsync(legacyFlowId));
            Assert.False(await store.TryAcquireLeaseAsync(legacyFlowId, "owner", TimeSpan.FromMinutes(1)));
        }
        finally
        {
            try
            {
                await client.DeleteTableAsync(table);
            }
            catch (ResourceNotFoundException)
            {
            }
        }
    }



    private async Task WaitForMySqlAsync()
        => await EventuallyAsync(async () =>
        {
            await using var connection = new MySqlConnection(Fixture.MySqlConnectionString);
            await connection.OpenAsync();
        });

    private async Task WaitForMongoDbAsync()
        => await EventuallyAsync(async () =>
        {
            var client = new MongoClient(Fixture.MongoDbConnectionString);
            using var cursor = await client.ListDatabaseNamesAsync();
            _ = await cursor.AnyAsync();
        });



    private AmazonDynamoDBClient CreateDynamoDbClient()
        => new(
            new BasicAWSCredentials("test", "test"),
            new AmazonDynamoDBConfig
            {
                ServiceURL = Fixture.LocalStackServiceUrl,
                AuthenticationRegion = "us-east-1"
            });




}
