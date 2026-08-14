using AsyncResponse.DurableFlows.Cosmos;
using AsyncResponse.DurableFlows.Oracle;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;
using Xunit;
using static AsyncResponse.IntegrationTests.FlowStoreContract;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Oracle and Cosmos durable-flow store contracts. These two live in their own batch because their
/// containers are by far the largest in the suite — measured 2,180 MiB and 1,031 MiB, together more
/// than half of a default Docker VM. Kept alongside the other store contracts they made that batch
/// 5.8 GiB, which is what tipped it over whenever anything else ran at the same time.
/// </summary>
[Collection(OracleCosmosCollection.Name)]
[Trait(Batches.Trait, Batches.OracleCosmos)]
public sealed class OracleCosmosStoreContractTests(OracleCosmosBatchFixture fixture) : IntegrationTestBase(fixture)
{
    /// <summary>
    /// Both servers here take minutes to become usable on a cold container — Oracle provisions its
    /// database on first boot, and the Cosmos emulator serves 503s until its pgcosmos extension is up.
    /// The 30s default that the cheaper stores use is nowhere near enough for either.
    /// </summary>
    private static readonly TimeSpan StoreReadyBudget = TimeSpan.FromMinutes(3);

    [Fact]
    public async Task OraclePackageStore_RoundTrips_Expires_Deletes_WhenConnectionStringIsConfigured()
    {
        var connectionString = Environment.GetEnvironmentVariable("ASYNCRESPONSE_ITEST_ORACLE_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString))
            Assert.Skip("Set ASYNCRESPONSE_ITEST_ORACLE_CONNECTION_STRING to run the Oracle durable-flow store contract test.");

        await WaitForOracleAsync(connectionString);
        var table = NewIdentifier("DF_ORACLE", 18).ToUpperInvariant();
        try
        {
            var store = new OracleFlowStateStore(
                Options.Create(new OracleDurableFlowOptions
                {
                    ConnectionString = connectionString,
                    TableName = table
                }));

            await AssertStoreContractAsync(store);

            // A second process can provision against an already-created schema. Oracle reports
            // both CREATE statements as "already exists"; the store must treat that as success.
            var secondStore = new OracleFlowStateStore(
                Options.Create(new OracleDurableFlowOptions
                {
                    ConnectionString = connectionString,
                    TableName = table
                }));
            Assert.Null(await secondStore.LoadAsync($"missing-{Guid.NewGuid():N}"));

            var mismatchedRevisionFlowId = $"revision-{Guid.NewGuid():N}";
            Assert.True(await store.TryCreateAsync(
                mismatchedRevisionFlowId,
                CreateState(mismatchedRevisionFlowId),
                TimeSpan.FromMinutes(1)));

            await using var revisionConnection = new OracleConnection(connectionString);
            await revisionConnection.OpenAsync();
            await using var revisionCommand = revisionConnection.CreateCommand();
            revisionCommand.BindByName = true;
            revisionCommand.CommandText = $"UPDATE {table} SET revision = revision + 1 WHERE flow_id = :flow_id";
            revisionCommand.Parameters.Add(new OracleParameter("flow_id", mismatchedRevisionFlowId));
            Assert.Equal(1, await revisionCommand.ExecuteNonQueryAsync());
            Assert.Null(await store.LoadAsync(mismatchedRevisionFlowId));
        }
        finally
        {
            await using var connection = new OracleConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE {table} PURGE";
            try
            {
                await command.ExecuteNonQueryAsync();
            }
            catch (OracleException ex) when (ex.Number == 942)
            {
            }
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task OraclePackageStore_RejectsAnExistingTableMissingTheFlowIdKey(bool autoCreateSchema)
    {
        // The PRIMARY KEY in this store's DDL only ever protects a table THIS build created:
        // CREATE TABLE swallows ORA-00955 for a pre-existing one, and AutoCreateSchema = false
        // issues no DDL at all. Without a unique key the MERGE's ORA-00001 duplicate detection is
        // gone — two concurrent creates of one flow id both insert — so the key is verified through
        // USER_CONSTRAINTS/USER_INDEXES on both paths, not inferred from having run the DDL.
        var connectionString = RequireOracle();
        await WaitForOracleAsync(connectionString);
        var table = NewIdentifier("DF_ORA_NOKEY", 24).ToUpperInvariant();
        try
        {
            await ExecuteOracleAsync(
                connectionString,
                $"""
                CREATE TABLE {table} (
                    flow_id NVARCHAR2(400) NOT NULL,
                    state_json NCLOB NOT NULL,
                    expires_at_utc TIMESTAMP(6) NOT NULL,
                    updated_at_utc TIMESTAMP(6) NOT NULL,
                    revision NUMBER(19) DEFAULT 0 NOT NULL,
                    lease_id NVARCHAR2(64) NULL,
                    lease_expires_at_utc TIMESTAMP(6) NULL
                )
                """);

            var store = new OracleFlowStateStore(
                Options.Create(new OracleDurableFlowOptions
                {
                    ConnectionString = connectionString,
                    TableName = table,
                    AutoCreateSchema = autoCreateSchema
                }));

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.TryCreateAsync("key-check", CreateState("key-check"), TimeSpan.FromMinutes(5)));
            Assert.Contains("no enabled unique key", exception.Message, StringComparison.Ordinal);
            Assert.Contains("ADD PRIMARY KEY (flow_id)", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            await DropOracleTableAsync(connectionString, table);
        }
    }

    [Fact]
    public async Task OraclePackageStore_ReachesTheTableThroughAPrivateSynonym()
    {
        // The standard "app user reaches the schema owner's table through a synonym" deployment:
        // TableName is a bare identifier, so a synonym is the only way to point the store at a
        // table living under another name. Verification must resolve the synonym chain the way
        // Oracle's own name resolution does and verify the BASE table — not reject the name
        // because the catalog says SYNONYM (r21 did exactly that), and not silently skip the
        // checks because the owner-scoped views go blank.
        var connectionString = RequireOracle();
        await WaitForOracleAsync(connectionString);
        var baseTable = NewIdentifier("DF_ORA_SYNB", 24).ToUpperInvariant();
        var synonym = $"{baseTable}_S";
        try
        {
            await ExecuteOracleAsync(
                connectionString,
                $"""
                CREATE TABLE {baseTable} (
                    flow_id NVARCHAR2(400) NOT NULL PRIMARY KEY,
                    state_json NCLOB NOT NULL,
                    expires_at_utc TIMESTAMP(6) NOT NULL,
                    updated_at_utc TIMESTAMP(6) NOT NULL,
                    revision NUMBER(19) DEFAULT 0 NOT NULL,
                    lease_id NVARCHAR2(64) NULL,
                    lease_expires_at_utc TIMESTAMP(6) NULL
                )
                """);
            await ExecuteOracleAsync(connectionString, $"CREATE SYNONYM {synonym} FOR {baseTable}");

            var store = new OracleFlowStateStore(
                Options.Create(new OracleDurableFlowOptions
                {
                    ConnectionString = connectionString,
                    TableName = synonym,
                    AutoCreateSchema = false
                }));

            // Verification passes through the synonym to the base table's shape and key, and the
            // store's DML works through it — including ORA-00001 duplicate detection off the base
            // table's primary key.
            var flowId = "syn-flow";
            Assert.True(await store.TryCreateAsync(flowId, CreateState(flowId), TimeSpan.FromMinutes(5)));
            Assert.False(await store.TryCreateAsync(flowId, CreateState(flowId), TimeSpan.FromMinutes(5)));
            Assert.NotNull(await store.LoadAsync(flowId));
        }
        finally
        {
            await ExecuteOracleIgnoringMissingAsync(connectionString, $"DROP SYNONYM {synonym}");
            await DropOracleTableAsync(connectionString, baseTable);
        }
    }

    [Fact]
    public async Task OraclePackageStore_VerifiesATableCreatedAfterFirstUse()
    {
        // AutoCreateSchema = false and the migration has not run: the first operation fails on
        // the missing table, and — the r22 fix — verification must NOT latch as done on that
        // empty catalog result. When the table appears later with a broken shape (a composite
        // key never raises ORA-00001 for a duplicate flow id, so two starts of one flow both
        // insert), the next operation must still verify and reject it instead of trusting the
        // pre-migration skip for the process lifetime.
        var connectionString = RequireOracle();
        await WaitForOracleAsync(connectionString);
        var table = NewIdentifier("DF_ORA_LATE", 24).ToUpperInvariant();
        var store = new OracleFlowStateStore(
            Options.Create(new OracleDurableFlowOptions
            {
                ConnectionString = connectionString,
                TableName = table,
                AutoCreateSchema = false
            }));
        try
        {
            // Missing table: the operation fails on the table itself (ORA-00942), not on verification.
            await Assert.ThrowsAsync<OracleException>(
                () => store.TryCreateAsync("late", CreateState("late"), TimeSpan.FromMinutes(5)));

            await ExecuteOracleAsync(
                connectionString,
                $"""
                CREATE TABLE {table} (
                    flow_id NVARCHAR2(400) NOT NULL,
                    state_json NCLOB NOT NULL,
                    expires_at_utc TIMESTAMP(6) NOT NULL,
                    updated_at_utc TIMESTAMP(6) NOT NULL,
                    revision NUMBER(19) DEFAULT 0 NOT NULL,
                    lease_id NVARCHAR2(64) NULL,
                    lease_expires_at_utc TIMESTAMP(6) NULL,
                    PRIMARY KEY (flow_id, revision)
                )
                """);

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.TryCreateAsync("late", CreateState("late"), TimeSpan.FromMinutes(5)));
            Assert.Contains("no enabled unique key", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            await DropOracleTableAsync(connectionString, table);
        }
    }

    [Fact]
    public async Task OraclePackageStore_RejectsAViewHoldingTheTableName()
    {
        // The name can be held by something that is not a table at all. A simple view over a
        // keyless base table is the nastiest shape: reads work, MERGE inserts pass through to the
        // base, and nothing ever raises ORA-00001 — duplicate flows run twice with no error
        // anywhere. AutoCreateSchema = false: with DDL on, the expiry-index CREATE already fails
        // loudly against a view (ORA-01702) before verification gets a turn.
        var connectionString = RequireOracle();
        await WaitForOracleAsync(connectionString);
        var view = NewIdentifier("DF_ORA_VIEW", 24).ToUpperInvariant();
        var baseTable = $"{view}_B";
        try
        {
            await ExecuteOracleAsync(
                connectionString,
                $"""
                CREATE TABLE {baseTable} (
                    flow_id NVARCHAR2(400) NOT NULL,
                    state_json NCLOB NOT NULL,
                    expires_at_utc TIMESTAMP(6) NOT NULL,
                    updated_at_utc TIMESTAMP(6) NOT NULL,
                    revision NUMBER(19) DEFAULT 0 NOT NULL,
                    lease_id NVARCHAR2(64) NULL,
                    lease_expires_at_utc TIMESTAMP(6) NULL
                )
                """);
            await ExecuteOracleAsync(connectionString, $"CREATE VIEW {view} AS SELECT * FROM {baseTable}");

            var store = new OracleFlowStateStore(
                Options.Create(new OracleDurableFlowOptions
                {
                    ConnectionString = connectionString,
                    TableName = view,
                    AutoCreateSchema = false
                }));

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.TryCreateAsync("view-check", CreateState("view-check"), TimeSpan.FromMinutes(5)));
            Assert.Contains("resolves to a VIEW", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            await ExecuteOracleIgnoringMissingAsync(connectionString, $"DROP VIEW {view}");
            await DropOracleTableAsync(connectionString, baseTable);
        }
    }

    [Fact]
    public async Task OraclePackageStore_RejectsIncompleteExistingSchema()
    {
        var connectionString = RequireOracle();
        await WaitForOracleAsync(connectionString);
        var table = NewIdentifier("DF_ORA_LEGACY", 24).ToUpperInvariant();
        try
        {
            await ExecuteOracleAsync(
                connectionString,
                $"""
                CREATE TABLE {table} (
                    flow_id NVARCHAR2(400) NOT NULL PRIMARY KEY,
                    state_json NCLOB NOT NULL,
                    expires_at_utc TIMESTAMP(6) NOT NULL,
                    updated_at_utc TIMESTAMP(6) NOT NULL
                )
                """);

            var store = new OracleFlowStateStore(
                Options.Create(new OracleDurableFlowOptions
                {
                    ConnectionString = connectionString,
                    TableName = table
                }));

            // A raw provider error ("ORA-00904: invalid identifier") tells the operator what broke
            // but not what to do; startup verification names the shape it needs instead.
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.TryCreateAsync("incomplete", CreateState("incomplete"), TimeSpan.FromMinutes(5)));
            Assert.Contains("no 'revision' column", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            await DropOracleTableAsync(connectionString, table);
        }
    }

    [Theory]
    // An extra NOT NULL column with no default: the store never names it, so EVERY create fails —
    // at the first flow, not at startup, which is the wrong end of the deployment.
    [InlineData("tenant_id NUMBER(19) NOT NULL", true)]
    // The same column the database can fill in for itself is harmless, and refusing it would stop
    // applications from adding perfectly reasonable bookkeeping to their own table.
    [InlineData("tenant_id NUMBER(19) DEFAULT 0 NOT NULL", false)]
    [InlineData("tenant_id NUMBER(19) NULL", false)]
    [InlineData("flow_id_len NUMBER GENERATED ALWAYS AS (LENGTH(flow_id)) VIRTUAL NOT NULL", false)]
    public async Task OraclePackageStore_RejectsOnlyExtraColumnsItCannotLeaveUnwritten(string extraColumn, bool rejected)
    {
        var connectionString = RequireOracle();
        await WaitForOracleAsync(connectionString);
        var table = NewIdentifier("DF_ORA_EXTRA", 24).ToUpperInvariant();
        try
        {
            await ExecuteOracleAsync(
                connectionString,
                $"""
                CREATE TABLE {table} (
                    flow_id NVARCHAR2(400) NOT NULL PRIMARY KEY,
                    state_json NCLOB NOT NULL,
                    expires_at_utc TIMESTAMP(6) NOT NULL,
                    updated_at_utc TIMESTAMP(6) NOT NULL,
                    revision NUMBER(19) DEFAULT 0 NOT NULL,
                    lease_id NVARCHAR2(64) NULL,
                    lease_expires_at_utc TIMESTAMP(6) NULL,
                    {extraColumn}
                )
                """);

            var store = new OracleFlowStateStore(
                Options.Create(new OracleDurableFlowOptions
                {
                    ConnectionString = connectionString,
                    TableName = table,
                    AutoCreateSchema = false
                }));

            if (rejected)
            {
                var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                    () => store.TryCreateAsync("extra", CreateState("extra"), TimeSpan.FromMinutes(5)));
                // The catalog reports the unquoted name upper-cased — exactly as ALTER TABLE needs it.
                Assert.Contains("extra column 'TENANT_ID'", exception.Message, StringComparison.Ordinal);
            }
            else
            {
                // The accepted rows double as the hand-written-DDL acceptance proof: a schema an
                // operator creates from the docs, plus their own harmless bookkeeping, starts clean.
                Assert.True(await store.TryCreateAsync("extra", CreateState("extra"), TimeSpan.FromMinutes(5)));
            }
        }
        finally
        {
            await DropOracleTableAsync(connectionString, table);
        }
    }

    [Fact]
    public async Task OraclePackageStore_RejectsALinguisticComparisonSession()
    {
        // NLS_COMP/NLS_SORT are SESSION state, normally inherited from a logon trigger or client
        // NLS configuration. Installing a real logon trigger here would poison every other test's
        // pooled sessions, so this drives the store's internal session check directly on a
        // deliberately mis-set unpooled connection — same catalog query, same decision — instead
        // of round-tripping through a trigger. Pooling=false so the linguistic session dies with
        // this connection rather than returning to the shared pool.
        var connectionString = RequireOracle();
        await WaitForOracleAsync(connectionString);
        var builder = new OracleConnectionStringBuilder(connectionString) { Pooling = false };
        await using var connection = new OracleConnection(builder.ConnectionString);
        await connection.OpenAsync();

        await using (var alter = connection.CreateCommand())
        {
            alter.CommandText = "ALTER SESSION SET NLS_COMP = LINGUISTIC NLS_SORT = BINARY_CI";
            await alter.ExecuteNonQueryAsync();
        }

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => OracleFlowStateStore.VerifyComparisonSemanticsAsync(connection, CancellationToken.None));
        Assert.Contains("NLS_SORT='BINARY_CI'", exception.Message, StringComparison.Ordinal);
        Assert.Contains("ALTER SESSION SET NLS_COMP = BINARY", exception.Message, StringComparison.Ordinal);

        // NLS_COMP=BINARY compares bytes no matter what NLS_SORT says — restoring it alone (the
        // documented remediation) must satisfy the check even with the case-insensitive sort left.
        await using (var alter = connection.CreateCommand())
        {
            alter.CommandText = "ALTER SESSION SET NLS_COMP = BINARY";
            await alter.ExecuteNonQueryAsync();
        }

        await OracleFlowStateStore.VerifyComparisonSemanticsAsync(connection, CancellationToken.None);
    }

    private static string RequireOracle()
    {
        var connectionString = Environment.GetEnvironmentVariable("ASYNCRESPONSE_ITEST_ORACLE_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString))
            Assert.Skip("Set ASYNCRESPONSE_ITEST_ORACLE_CONNECTION_STRING to run the Oracle durable-flow store contract test.");
        return connectionString;
    }

    private static async Task ExecuteOracleAsync(string connectionString, string sql)
    {
        await using var connection = new OracleConnection(connectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task ExecuteOracleIgnoringMissingAsync(string connectionString, string sql)
    {
        try
        {
            await ExecuteOracleAsync(connectionString, sql);
        }
        catch (OracleException ex) when (ex.Number is 942 or 4043)
        {
            // ORA-00942 (table or view does not exist) / ORA-04043 (object does not exist): the
            // test failed before creating it; there is nothing to clean up.
        }
    }

    private static Task DropOracleTableAsync(string connectionString, string table)
        => ExecuteOracleIgnoringMissingAsync(connectionString, $"DROP TABLE {table} PURGE");

    [Fact]
    public async Task CosmosPackageStore_RoundTrips_Expires_Deletes_WhenConnectionStringIsConfigured()
    {
        var connectionString = Environment.GetEnvironmentVariable("ASYNCRESPONSE_ITEST_COSMOS_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString))
            Assert.Skip("Set ASYNCRESPONSE_ITEST_COSMOS_CONNECTION_STRING to run the Cosmos DB durable-flow store contract test.");

        var databaseName = NewIdentifier("df_cosmos", 63);
        using var client = new CosmosClient(connectionString, GetCosmosClientOptions(connectionString));
        await WaitForCosmosAsync(client);
        try
        {
            var store = new CosmosFlowStateStore(
                client,
                Options.Create(new CosmosDurableFlowOptions
                {
                    DatabaseName = databaseName,
                    ContainerName = "flow_state"
                }));

            await AssertStoreContractAsync(store);

            await client.GetDatabase(databaseName).CreateContainerAsync(
                new ContainerProperties("flow_state_without_ttl", "/flowId"));
            var manualStore = new CosmosFlowStateStore(
                client,
                Options.Create(new CosmosDurableFlowOptions
                {
                    DatabaseName = databaseName,
                    ContainerName = "flow_state_without_ttl",
                    AutoCreateContainer = false
                }));
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => manualStore.LoadAsync("flow"));
            Assert.Contains("TTL", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            try
            {
                await client.GetDatabase(databaseName).DeleteAsync();
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
            }
        }
    }

    private static async Task WaitForOracleAsync(string connectionString)
        // Oracle creates its database on first boot and needs far longer than the 30s default.
        => await EventuallyAsync(async () =>
        {
            await using var connection = new OracleConnection(connectionString);
            await connection.OpenAsync();
        }, StoreReadyBudget);

    // The emulator answers on its gateway well before it can serve requests — it replies 503
    // "pgcosmos extension is still starting" for a while after that. On a cold CI runner this ran past
    // the 30s default and failed the whole test, so it gets the same budget Oracle does.
    private static async Task WaitForCosmosAsync(CosmosClient client)
        => await EventuallyAsync(async () => _ = await client.ReadAccountAsync(), StoreReadyBudget);

    private static CosmosClientOptions GetCosmosClientOptions(string connectionString)
    {
        var options = new CosmosClientOptions();
        if (!IsLocalCosmosEndpoint(connectionString))
            return options;

        options.ConnectionMode = ConnectionMode.Gateway;
        options.LimitToEndpoint = true;
        options.HttpClientFactory = () => new HttpClient(new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
        });
        return options;
    }

    private static bool IsLocalCosmosEndpoint(string connectionString)
    {
        foreach (var segment in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var pair = segment.Split('=', 2, StringSplitOptions.TrimEntries);
            if (pair.Length == 2 &&
                pair[0].Equals("AccountEndpoint", StringComparison.OrdinalIgnoreCase) &&
                Uri.TryCreate(pair[1], UriKind.Absolute, out var endpoint))
            {
                return endpoint.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase) ||
                       endpoint.Host is "127.0.0.1" or "::1";
            }
        }

        return false;
    }
}
