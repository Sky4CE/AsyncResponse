using System.Reflection;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Sample;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class SqlServerDirectIntegrationTests(DataBatchFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task MaximumLengthMessageTableName_CreatesADistinctSequence_AndDrawsFromIt()
    {
        // A 128-character table name used to collide with its own generated sequence name
        // (whole-name truncation): CREATE SEQUENCE failed against the existing table object and
        // schema creation broke. Suffix space is now reserved before capping.
        await WithSchemaAsync("max_len_table", async schema =>
        {
            var options = ChannelOptions(schema);
            options.MessageTable = new string('m', 128);
            var sql = new SqlServerChannelSql(Options.Create(options));
            await sql.EnsureCreatedAsync();

            var (_, startSeq) = await sql.GetSubscriptionStartAsync(CancellationToken.None);
            Assert.True(startSeq > 0);

            // Every derived index must actually exist in the catalog. Whole-name truncation used
            // to give both messages-table indexes ONE shared name, so the second IF NOT EXISTS
            // guard matched the first index and silently skipped creation — invisible to any test
            // that only drew from the sequence.
            var indexNames = new List<string>();
            await using (var connection = new SqlConnection(Fixture.SqlServerConnectionString))
            {
                await connection.OpenAsync();
                await using var indexes = connection.CreateCommand();
                indexes.CommandText =
                    """
                    SELECT i.name FROM sys.indexes i
                    JOIN sys.tables t ON t.object_id = i.object_id
                    JOIN sys.schemas s ON s.schema_id = t.schema_id
                    WHERE s.name = @schema AND i.name IS NOT NULL;
                    """;
                indexes.Parameters.AddWithValue("@schema", schema);
                await using var reader = await indexes.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                    indexNames.Add(reader.GetString(0));
            }

            // Suffix space is reserved by truncating the table STEM, so at the 128-character cap
            // the two messages-table indexes stay distinct.
            Assert.Contains(new string('m', 116) + "_expires_idx", indexNames);
            Assert.Contains(new string('m', 104) + "_correlation_created_idx", indexNames);
            Assert.Contains("asyncresponse_recovery_state_expires_idx", indexNames);
            Assert.Contains("asyncresponse_channel_subscribers_expires_idx", indexNames);

            var managedOptions = ChannelOptions(schema);
            managedOptions.MessageTable = options.MessageTable;
            managedOptions.AutoCreateSchema = false;
            var managed = new SqlServerChannelSql(Options.Create(managedOptions));
            var (_, managedSeq) = await managed.GetSubscriptionStartAsync(CancellationToken.None);
            Assert.True(managedSeq > startSeq);
        });
    }

    [Fact]
    public async Task SharedSchema_CrossComponentNameCollisions_FailActionablyInBothOrders()
    {
        // IF OBJECT_ID(N'…', N'U') answers only "is there a USER TABLE with this name", so the
        // component that starts second either silently skipped its own CREATE (and failed later on
        // a column that does not exist) or hit raw error 2714. Post-DDL catalog verification must
        // fail it up front, whichever order the two components start in.
        await WithSchemaAsync("sql_collide_a", async schema =>
        {
            var transportOptions = TransportOptions(schema);
            transportOptions.MessageTable = "shared_name";
            await new SqlServerTransportStore(Options.Create(transportOptions)).EnsureCreatedAsync();

            // The channel's message table now lands on the transport's table: same kind, wrong
            // shape. Its CREATE is suppressed and the rest of its batch runs against the wrong
            // table, which must surface as the collision it is rather than a raw SqlException.
            var channelOptions = ChannelOptions(schema);
            channelOptions.MessageTable = "shared_name";
            var channel = new SqlServerChannelSql(Options.Create(channelOptions));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => channel.EnsureCreatedAsync());
            Assert.Contains("shared_name", ex.Message, StringComparison.Ordinal);
            Assert.Contains("missing the column", ex.Message, StringComparison.Ordinal);
            Assert.IsType<SqlException>(ex.InnerException);
        });

        await WithSchemaAsync("sql_collide_b", async schema =>
        {
            var channelOptions = ChannelOptions(schema);
            channelOptions.MessageTable = "shared_name";
            await new SqlServerChannelSql(Options.Create(channelOptions)).EnsureCreatedAsync();

            var transportOptions = TransportOptions(schema);
            transportOptions.MessageTable = "shared_name";
            var transport = new SqlServerTransportStore(Options.Create(transportOptions));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => transport.EnsureCreatedAsync());
            Assert.Contains("shared_name", ex.Message, StringComparison.Ordinal);
            Assert.Contains("missing the column", ex.Message, StringComparison.Ordinal);
            Assert.IsType<SqlException>(ex.InnerException);
        });
    }

    [Fact]
    public async Task SharedSchema_CaseVariantNameCollision_IsDiagnosedNotMisreportedAsAbsent()
    {
        // Regression (r23): the post-DDL verification keyed its catalog dictionary ORDINALLY while
        // the server matched names under its case-insensitive catalog collation. A foreign table
        // whose name differs from the configured one only in case was found by the server but
        // missed by every dictionary lookup — the kind/column/key checks silently skipped the
        // collision and the operator was told the table "does not exist", the one diagnosis that
        // is provably false. The dictionary now folds case exactly like the catalog.
        await WithSchemaAsync("sql_collide_case", async schema =>
        {
            var transportOptions = TransportOptions(schema);
            transportOptions.MessageTable = "SHARED_CASE_NAME";
            await new SqlServerTransportStore(Options.Create(transportOptions)).EnsureCreatedAsync();

            var channelOptions = ChannelOptions(schema);
            channelOptions.MessageTable = "shared_case_name";
            var channel = new SqlServerChannelSql(Options.Create(channelOptions));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => channel.EnsureCreatedAsync());
            Assert.Contains("missing the column", ex.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("to exist after schema creation, but it does not", ex.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task PreDdlFailure_SurfacesTheOriginalError_NotAPhantomAbsenceCollision()
    {
        // A DDL batch that dies before creating anything (here: CREATE SCHEMA/TABLE permission
        // denied) used to be re-diagnosed against an EMPTY catalog, where the unconditional
        // absence check reported "expected ... to exist after schema creation" — wrapping the
        // real SqlException in a phantom collision about a table nobody expected to be there. A
        // diagnosis that finds nothing present and wrong must let the original error stand.
        await WithSchemaAsync("sql_pre_ddl", async schema =>
        {
            var login = $"{schema}_login";
            const string password = "Pre-Ddl-Failure-1!";
            await ExecuteAsync($"CREATE LOGIN [{login}] WITH PASSWORD = N'{password}', CHECK_POLICY = OFF;");
            await ExecuteAsync($"CREATE USER [{login}] FOR LOGIN [{login}];");
            try
            {
                // CONNECT only: the login can open connections and read what little metadata it
                // owns (nothing), but every CREATE in the DDL batch is denied.
                var options = ChannelOptions(schema);
                options.ConnectionString = new SqlConnectionStringBuilder(Fixture.SqlServerConnectionString)
                {
                    UserID = login,
                    Password = password,
                    Pooling = false
                }.ConnectionString;
                var channel = new SqlServerChannelSql(Options.Create(options));

                var ex = await Assert.ThrowsAsync<SqlException>(() => channel.EnsureCreatedAsync());
                Assert.Contains("permission denied", ex.Message, StringComparison.OrdinalIgnoreCase);
            }
            finally
            {
                await ExecuteAsync($"DROP USER IF EXISTS [{login}];");
                await ExecuteAsync($"IF SUSER_ID(N'{login}') IS NOT NULL DROP LOGIN [{login}];");
            }
        });
    }

    [Fact]
    public async Task ManagedTransportSchema_VerifiesProvisionedTables_AndDefersWhileAbsent()
    {
        // AutoCreateSchema=false used to return before ANY network call: an operator-provisioned
        // queue table's shape was never checked — the transport ran the verifier only on the DDL
        // path, i.e. only where it was least needed. Managed mode now runs the same catalog
        // verification with the flow store's semantics: absent object = silent, re-check later
        // (never latch); present = verify (wrong shape fails actionably) and latch.
        await WithSchemaAsync("sql_managed_transport", async schema =>
        {
            var managedOptions = TransportOptions(schema);
            managedOptions.AutoCreateSchema = false;
            var managed = new SqlServerTransportStore(Options.Create(managedOptions));

            await managed.EnsureCreatedAsync();
            Assert.False(CreatedLatch(managed));

            await new SqlServerTransportStore(Options.Create(TransportOptions(schema))).EnsureCreatedAsync();
            await managed.EnsureCreatedAsync();
            Assert.True(CreatedLatch(managed));
        });

        // What "the same verification" means on a table this build did not create: every column
        // the store's own reads and writes depend on is held to its shape — but NOT the queue
        // column, whose type and collation ExactQueueMatch overrides in the query itself (proved
        // end to end by AssertLegacyQueueColumnClaimsExactlyAsync). Constraining it here would
        // reject the legacy schemas that predicate exists to keep working.
        await WithSchemaAsync("sql_managed_shape", async schema =>
        {
            await ExecuteAsync($"IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');");
            await ExecuteAsync(
                $"""
                CREATE TABLE [{schema}].[jobs] (
                    id uniqueidentifier NOT NULL PRIMARY KEY NONCLUSTERED,
                    queue nvarchar(200) COLLATE Latin1_General_100_CI_AS NOT NULL,
                    payload_json nvarchar(200) NOT NULL,
                    headers_json nvarchar(max) NOT NULL DEFAULT N'{EmptyJsonObject}',
                    created_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    available_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    locked_until datetime2 NULL,
                    lock_id uniqueidentifier NULL,
                    attempts int NOT NULL DEFAULT 0,
                    dead_letter_reason nvarchar(max) NULL
                );
                """);

            var options = TransportOptions(schema);
            options.AutoCreateSchema = false;
            options.MessageTable = "jobs";
            var store = new SqlServerTransportStore(Options.Create(options));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync());
            Assert.Contains("payload_json", ex.Message, StringComparison.Ordinal);
            Assert.Contains("expected nvarchar(max) NOT NULL", ex.Message, StringComparison.Ordinal);
            Assert.Contains("found nvarchar(200) NOT NULL", ex.Message, StringComparison.Ordinal);
            Assert.False(CreatedLatch(store));

            // Repairing only that column is enough: the case-folding queue column stays exactly as
            // the operator left it, and the same unlatched store re-verifies and accepts.
            await ExecuteAsync($"ALTER TABLE [{schema}].[jobs] ALTER COLUMN payload_json nvarchar(max) NOT NULL;");
            await store.EnsureCreatedAsync();
            Assert.True(CreatedLatch(store));
        });
    }

    // Interpolated into crafted DDL: literal braces cannot appear directly inside the
    // interpolated raw string.
    private const string EmptyJsonObject = "{}";

    private static bool CreatedLatch(object store)
        => (bool)store.GetType().GetField("_created", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(store)!;

    [Fact]
    public async Task NameHeldByAnotherObjectKind_FailsVerificationInsteadOfRawError2714()
    {
        // A view (or synonym, or procedure) occupying the name slips past an existence guard that
        // only looks for user tables: the CREATE then failed with a bare "There is already an
        // object named …" and no hint about which component or what to do.
        await WithSchemaAsync("sql_kind_clash", async schema =>
        {
            // Separate batches: CREATE SCHEMA has to commit before the view can reference it, and
            // CREATE VIEW must be the first statement in its own batch.
            await ExecuteAsync($"IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');");
            await ExecuteAsync($"CREATE VIEW [{schema}].[occupied] AS SELECT 1 AS one;");

            var options = ChannelOptions(schema);
            options.MessageTable = "occupied";
            var channel = new SqlServerChannelSql(Options.Create(options));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => channel.EnsureCreatedAsync());
            Assert.Contains("occupied", ex.Message, StringComparison.Ordinal);
            Assert.Contains("a view", ex.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task MaximumLengthFlowTableName_CreatesWithoutBreachingTheIdentifierCap()
    {
        // The revision column's default used to be NAMED "DF_{table}_revision", deriving a
        // 129-character identifier from a 117-character table name the store otherwise accepts —
        // over SQL Server's 128-character cap, so CREATE TABLE failed with error 103. The default
        // is unnamed now; the table must simply create and round-trip.
        await WithSchemaAsync("sql_flow_longname", async schema =>
        {
            var options = new AsyncResponse.DurableFlows.SqlServer.SqlServerDurableFlowOptions
            {
                ConnectionString = Fixture.SqlServerConnectionString,
                SchemaName = schema,
                TableName = new string('f', 117)
            };
            var store = new AsyncResponse.DurableFlows.SqlServer.SqlServerFlowStateStore(Options.Create(options));

            Assert.True(await store.TryCreateAsync(
                "long-table-flow",
                new FlowState { FlowId = "long-table-flow", Status = FlowRunStatus.Running },
                TimeSpan.FromMinutes(5)));
            var loaded = await store.LoadAsync("long-table-flow");
            Assert.Equal(0, loaded!.Revision);
        });
    }

    [Fact]
    public async Task IdColumns_ArePinnedToABinaryCollation_SoCaseVariantIdsStayDistinct()
    {
        // The database collation is case-INSENSITIVE by default, which made 'x' and 'X' the same
        // key: the channel's correlation lookup cross-matched, and the flow store's primary key
        // rejected the second id. Every column holding an id the library compares ordinally must
        // therefore carry its own binary collation, whatever the database is set to.
        await WithSchemaAsync("sql_collation", async schema =>
        {
            var channelOptions = ChannelOptions(schema);
            await new SqlServerChannelSql(Options.Create(channelOptions)).EnsureCreatedAsync();

            var transportOptions = TransportOptions(schema);
            await new SqlServerTransportStore(Options.Create(transportOptions)).EnsureCreatedAsync();

            var flowOptions = new AsyncResponse.DurableFlows.SqlServer.SqlServerDurableFlowOptions
            {
                ConnectionString = Fixture.SqlServerConnectionString,
                SchemaName = schema
            };
            var flowStore = new AsyncResponse.DurableFlows.SqlServer.SqlServerFlowStateStore(Options.Create(flowOptions));
            await flowStore.TryCreateAsync(
                "collation-probe",
                new FlowState { FlowId = "collation-probe", Status = FlowRunStatus.Running },
                TimeSpan.FromMinutes(5));

            var collations = new Dictionary<string, string>(StringComparer.Ordinal);
            await using (var connection = new SqlConnection(Fixture.SqlServerConnectionString))
            {
                await connection.OpenAsync();
                await using var command = connection.CreateCommand();
                command.CommandText =
                    """
                    SELECT t.name + N'.' + c.name, ISNULL(c.collation_name, N'')
                    FROM sys.columns c
                    JOIN sys.tables t ON t.object_id = c.object_id
                    JOIN sys.schemas s ON s.schema_id = t.schema_id
                    WHERE s.name = @schema AND c.name IN (N'correlation_id', N'flow_id', N'queue');
                    """;
                command.Parameters.AddWithValue("@schema", schema);
                await using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                    collations[reader.GetString(0)] = reader.GetString(1);
            }

            // Channel (3 tables) + transport queue + flow id: every one of them, not just the ones
            // a given test happens to exercise.
            Assert.Equal(5, collations.Count);
            Assert.All(collations, entry => Assert.Contains("_BIN", entry.Value, StringComparison.OrdinalIgnoreCase));

            // And the end-to-end consequence: two flow ids differing only in case are two flows.
            Assert.True(await flowStore.TryCreateAsync(
                "CASE-FLOW",
                new FlowState { FlowId = "CASE-FLOW", Status = FlowRunStatus.Running },
                TimeSpan.FromMinutes(5)));
            Assert.True(await flowStore.TryCreateAsync(
                "case-flow",
                new FlowState { FlowId = "case-flow", Status = FlowRunStatus.Running },
                TimeSpan.FromMinutes(5)));
            Assert.Equal("CASE-FLOW", (await flowStore.LoadAsync("CASE-FLOW"))!.FlowId);
            Assert.Equal("case-flow", (await flowStore.LoadAsync("case-flow"))!.FlowId);
        });
    }

    [Fact]
    public async Task LegacyCaseInsensitiveTables_AreRejectedAtFirstUse_InsteadOfCrossDelivering()
    {
        // The collation above protects tables this build creates. Tables created by an EARLIER
        // build inherit the database's case-insensitive collation, and re-collating a primary-key
        // column is not something an upgrade can do silently. The dispatch loop re-checks the
        // returned correlation id ordinally as the last line of defence — but with
        // AutoCreateSchema = false the channel now verifies the relations it did not create
        // (round 31), and a non-binary identity column fails that verification at first use with
        // the actionable error, rather than the sweep ever cross-matching 'LEGACY-CI' and
        // 'legacy-ci'. Here the tables are deliberately created the old way.
        var schema = NewSchema("sql_legacy_ci");
        ServiceProvider? provider = null;
        try
        {
            await CreateLegacyChannelSchemaAsync(schema, "Latin1_General_100_CI_AS");
            provider = BuildProvider(schema, options => options.AutoCreateSchema = false);
            var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();

            var rejected = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await using var upper = await subscriber.CreateResponseWaiter<OperationResult>("LEGACY-CI", timeout: TimeSpan.FromSeconds(5));
            });

            Assert.Contains("Latin1_General_100_CI_AS", rejected.Message, StringComparison.Ordinal);
            Assert.Contains("_BIN2", rejected.Message, StringComparison.Ordinal);
        }
        finally
        {
            if (provider is not null)
                await provider.DisposeAsync();
            await DropSchemaAsync(schema);
        }
    }

    /// <summary>
    /// The channel schema as an EARLIER build created it: no COLLATE clause, so the columns
    /// inherit the database collation (case-insensitive on a default SQL Server).
    /// </summary>
    private async Task CreateLegacyChannelSchemaAsync(string schema, string collation)
    {
        var options = ChannelOptions(schema);
        await ExecuteAsync(
            $"""
            IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');

            CREATE TABLE [{schema}].[{options.RecoveryStateTable}] (
                correlation_id nvarchar(400) COLLATE {collation} NOT NULL,
                registration_id uniqueidentifier NOT NULL,
                state_json nvarchar(max) NOT NULL,
                expires_at datetime2 NOT NULL,
                registered_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                PRIMARY KEY (correlation_id, registration_id)
            );

            CREATE TABLE [{schema}].[{options.MessageTable}] (
                id uniqueidentifier NOT NULL PRIMARY KEY NONCLUSTERED,
                correlation_id nvarchar(400) COLLATE {collation} NOT NULL,
                envelope_json nvarchar(max) NOT NULL,
                created_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                expires_at datetime2 NOT NULL,
                acked_at datetime2 NULL,
                acked_seq bigint NULL,
                recovery_claimed bit NOT NULL DEFAULT 0
            );
            CREATE SEQUENCE [{schema}].[{options.MessageTable}_ack_seq] AS bigint START WITH 1;

            CREATE TABLE [{schema}].[{options.SubscriberTable}] (
                correlation_id nvarchar(400) COLLATE {collation} NOT NULL,
                registration_id uniqueidentifier NOT NULL,
                instance_id nvarchar(200) NOT NULL,
                expires_at datetime2 NOT NULL,
                PRIMARY KEY (correlation_id, registration_id)
            );
            """);
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task TransportStore_DeadLetterOnAStaleClaim_NoOpsInsteadOfBuryingALiveMessage()
    {
        // Regression (round 29): the DLQ row was written unconditionally and the fenced delete's
        // result ignored, so a claim whose lease had lapsed (a peer re-claimed the row) still
        // buried a full copy of a message that is still live and may yet succeed under its new
        // owner — the DLQ showed a poison entry for work that completed, and an operator replaying
        // it duplicated its side effects. The fenced ack and NAK already no-op in that window.
        await WithSchemaAsync("dlqfence", async schema =>
        {
            var options = TransportOptions(schema);
            var store = new SqlServerTransportStore(Options.Create(options));
            await store.EnsureCreatedAsync();

            var id = Guid.NewGuid();
            await store.PublishAsync(id, options.WorkerQueue, """{"kind":"fenced"}""", null, CancellationToken.None);
            var claimed = (await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None))!;

            // The peer takeover: a different lock_id now owns the row, so this claim's fence is dead.
            await ExecuteAsync(
                $"UPDATE {store.MessageTable} SET lock_id = NEWID(), locked_until = DATEADD(minute, 5, SYSUTCDATETIME()) WHERE id = '{id}';");

            Assert.False(await claimed.DeadLetterAsync(new InvalidOperationException("stale"), true, CancellationToken.None));

            // No DLQ copy was written, and the live row is untouched.
            Assert.Equal(0, await ScalarIntAsync(
                $"SELECT COUNT(*) FROM {store.MessageTable} WHERE queue = '{options.DeadLetterQueue}';"));
            Assert.Equal(1, await ScalarIntAsync($"SELECT COUNT(*) FROM {store.MessageTable} WHERE id = '{id}';"));
        });
    }

    private async Task<int> ScalarIntAsync(string sql)
    {
        await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return (int)(await command.ExecuteScalarAsync())!;
    }

    [Fact]
    public async Task ASpacePaddedQueueRow_IsNeverClaimed_AndDoesNotStarveTheRowsBehindIt()
    {
        // Startup validation now rejects queue names with surrounding spaces, so this row can only
        // come from an EARLIER build (or another writer). SQL Server's `=` pads the shorter operand
        // under every collation, binary ones included, so `queue = N'worker'` matches this 'worker '
        // row — and it is the OLDEST, so `ORDER BY created_at` puts it first in line on every poll.
        // Rejecting it after the claim is not enough: the claim is what puts it back at the head of
        // the queue, and the poll that follows selects it again, forever. The exclusion has to
        // happen in the SELECT, so the valid row behind it is claimed on the very first try.
        await WithSchemaAsync("sql_padded_queue", async schema =>
        {
            var options = TransportOptions(schema);
            var store = new SqlServerTransportStore(Options.Create(options));
            await store.EnsureCreatedAsync();

            // Insert the padded row directly: the store's own publish path would reject the name.
            // Literal braces cannot appear directly inside a single-$ interpolated raw string.
            var payloadJson = """{"kind":"other-queue"}""";
            var emptyJson = "{}";
            var paddedId = Guid.NewGuid();
            await ExecuteAsync(
                $"""
                INSERT INTO [{schema}].[{options.MessageTable}] (id, queue, payload_json, headers_json, created_at)
                VALUES ('{paddedId}', N'{options.WorkerQueue} ', N'{payloadJson}', N'{emptyJson}', DATEADD(minute, -5, SYSUTCDATETIME()));
                """);

            var firstId = Guid.NewGuid();
            var secondId = Guid.NewGuid();
            await store.PublishAsync(firstId, options.WorkerQueue, """{"kind":"mine"}""", null, CancellationToken.None);
            await store.PublishAsync(secondId, options.WorkerQueue, """{"kind":"mine-too"}""", null, CancellationToken.None);

            // The queue is not blocked: the first claim returns a valid row, not null.
            var delivery = await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None);
            Assert.NotNull(delivery);
            Assert.Equal(firstId, delivery.Id);
            Assert.Equal("""{"kind":"mine"}""", delivery.Payload);
            await delivery.AckAsync();

            // And the BATCH path drains the rest rather than stopping at the head. This is where
            // the starvation actually bit: ClaimBatchAsync yields until a claim returns null, so a
            // single unclaimable row at the front of the ordering ended every batch at zero.
            var batch = new List<SqlServerTransportDelivery>();
            await foreach (var claimed in store.ClaimBatchAsync(options.WorkerQueue, 5, options.LockTimeout, CancellationToken.None))
                batch.Add(claimed);
            Assert.Equal([secondId], batch.Select(claimed => claimed.Id));
            foreach (var claimed in batch)
                await claimed.AckAsync();

            // And the padded row is not treated as a fallback either — it stays invisible.
            Assert.Null(await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None));

            // Untouched, because it was never claimed: no attempt charged, no lease taken. A row
            // that is claimed and then released would have to have its attempt refunded, or
            // repeated polls would walk somebody else's message to its delivery limit.
            await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
            await connection.OpenAsync();
            await using var check = connection.CreateCommand();
            check.CommandText =
                $"SELECT attempts, CASE WHEN lock_id IS NULL THEN 1 ELSE 0 END FROM [{schema}].[{options.MessageTable}] WHERE id = @id;";
            check.Parameters.AddWithValue("@id", paddedId);
            await using var reader = await check.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(0, reader.GetInt32(0));
            Assert.Equal(1, reader.GetInt32(1));
        });
    }

    [Fact]
    public async Task DeadLetterPrune_DeletesOnlyItsOwnQueuesRows()
    {
        // The retention prune is a DELETE keyed on the same shared queue column, so it inherits the
        // same padding rule — and here the consequence is not a mis-delivery but data loss: an
        // unqualified `queue = @queue` would delete a NEIGHBOURING queue's rows whose name differs
        // only in trailing blanks, silently, on a timer.
        await WithSchemaAsync("sql_dlq_prune", async schema =>
        {
            var options = TransportOptions(schema);
            options.DeadLetterRetention = TimeSpan.FromSeconds(1);
            var store = new SqlServerTransportStore(Options.Create(options));
            await store.EnsureCreatedAsync();

            var payloadJson = """{"kind":"old"}""";
            var emptyJson = "{}";
            var expiredId = Guid.NewGuid();
            var paddedNeighbourId = Guid.NewGuid();
            await ExecuteAsync(
                $"""
                INSERT INTO [{schema}].[{options.MessageTable}] (id, queue, payload_json, headers_json, created_at)
                VALUES ('{expiredId}', N'{options.DeadLetterQueue}', N'{payloadJson}', N'{emptyJson}', DATEADD(minute, -5, SYSUTCDATETIME())),
                       ('{paddedNeighbourId}', N'{options.DeadLetterQueue} ', N'{payloadJson}', N'{emptyJson}', DATEADD(minute, -5, SYSUTCDATETIME()));
                """);

            // A publish is what triggers the throttled prune, and a store this fresh has never run
            // one, so the first publish is due immediately.
            await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, """{"kind":"trigger"}""", null, CancellationToken.None);

            await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
            await connection.OpenAsync();
            await using var check = connection.CreateCommand();
            check.CommandText = $"SELECT id FROM [{schema}].[{options.MessageTable}] WHERE id IN (@expired, @neighbour);";
            check.Parameters.AddWithValue("@expired", expiredId);
            check.Parameters.AddWithValue("@neighbour", paddedNeighbourId);
            var surviving = new List<Guid>();
            await using (var reader = await check.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                    surviving.Add(reader.GetGuid(0));
            }

            Assert.Equal([paddedNeighbourId], surviving);
        });
    }

    [Fact]
    public async Task ACaseFoldedQueueRow_IsNeverClaimed_OnALegacyCaseInsensitiveTable()
        => await AssertLegacyQueueColumnClaimsExactlyAsync("sql_ci_queue", "nvarchar(200) COLLATE Latin1_General_100_CI_AS");

    [Fact]
    public async Task ExactQueueMatching_SurvivesANonUnicodeQueueColumn()
        => await AssertLegacyQueueColumnClaimsExactlyAsync("sql_varchar_queue", "varchar(200)");

    /// <summary>
    /// The exact-match predicate has to survive a table this build did NOT create: with
    /// AutoCreateSchema off there is no COLLATE clause to lean on, so the column carries the server
    /// default (which folds case, making <c>queue = N'worker'</c> match 'WORKER'), and its type is
    /// whatever the migration chose. Both are covered by the same claim, from opposite directions:
    /// the case-folding column must NOT over-match, and the varchar column must still match at all
    /// — the intuitive byte-count formulation (<c>DATALENGTH(queue) = DATALENGTH(@queue)</c>)
    /// compares 1-byte characters against 2-byte ones and silently claims nothing, forever.
    /// </summary>
    private async Task AssertLegacyQueueColumnClaimsExactlyAsync(string prefix, string queueColumnType)
    {
        await WithSchemaAsync(prefix, async schema =>
        {
            var options = TransportOptions(schema);
            options.AutoCreateSchema = false;
            var emptyJson = "{}";
            await ExecuteAsync(
                $"""
                IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');

                CREATE TABLE [{schema}].[{options.MessageTable}] (
                    id uniqueidentifier NOT NULL PRIMARY KEY NONCLUSTERED,
                    queue {queueColumnType} NOT NULL,
                    payload_json nvarchar(max) NOT NULL,
                    headers_json nvarchar(max) NOT NULL DEFAULT N'{emptyJson}',
                    created_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    available_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    locked_until datetime2 NULL,
                    lock_id uniqueidentifier NULL,
                    attempts int NOT NULL DEFAULT 0,
                    dead_letter_reason nvarchar(max) NULL
                );
                """);

            var store = new SqlServerTransportStore(Options.Create(options));
            var payloadJson = """{"kind":"other-queue"}""";
            // Two decoys, both older than the valid row so they sort ahead of it: one differing only
            // in case, one only in a trailing space.
            await ExecuteAsync(
                $"""
                INSERT INTO [{schema}].[{options.MessageTable}] (id, queue, payload_json, headers_json, created_at)
                VALUES ('{Guid.NewGuid()}', N'{options.WorkerQueue.ToUpperInvariant()}', N'{payloadJson}', N'{emptyJson}', DATEADD(minute, -5, SYSUTCDATETIME())),
                       ('{Guid.NewGuid()}', N'{options.WorkerQueue} ', N'{payloadJson}', N'{emptyJson}', DATEADD(minute, -4, SYSUTCDATETIME()));
                """);

            var validId = Guid.NewGuid();
            await store.PublishAsync(validId, options.WorkerQueue, """{"kind":"mine"}""", null, CancellationToken.None);

            var delivery = await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None);
            Assert.NotNull(delivery);
            Assert.Equal(validId, delivery.Id);
            await delivery.AckAsync();
            Assert.Null(await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None));
        });
    }

    [Fact]
    public async Task CaseSensitiveButNonBinaryCollation_IsRejected()
    {
        // A merely case-SENSITIVE collation is not ordinal: probed on SQL Server 2022,
        // Latin1_General_100_CS_AS still folds full-width forms ('ab' = 'ａｂ'), and _CS_AI folds
        // accents. Only a binary collation compares by code point, which is what the id contract
        // promises — so accepting any _CS_ collation (as an earlier build did) left ids colliding.
        await WithSchemaAsync("sql_cs_as", async schema =>
        {
            var options = ChannelOptions(schema);
            options.AutoCreateSchema = false;
            await CreateLegacyChannelSchemaAsync(schema, "Latin1_General_100_CS_AS");

            var managed = ChannelOptions(schema);
            var channel = new SqlServerChannelSql(Options.Create(managed));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => channel.EnsureCreatedAsync());
            Assert.Contains("is not binary", ex.Message, StringComparison.Ordinal);
            Assert.Contains("Latin1_General_100_CS_AS", ex.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ReducedScaleTimestampColumns_AreRejected_AndFullScaleOnesAccepted()
    {
        // datetime2(0) is not a coarser VIEW of the same instant: SQL Server ROUNDS on store, so a
        // lease or ack timestamp can land BELOW an already-observed full-precision watermark and
        // reorder the events the stores compare. The catalog query read only max_length, so the
        // reduced-scale column rendered as a bare "datetime2" and matched the expectation exactly.
        await WithSchemaAsync("sql_scale", async schema =>
        {
            var options = TransportOptions(schema);
            options.MessageTable = "jobs";
            options.AutoCreateSchema = false;
            var emptyJsonDefault = "{}";
            await ExecuteAsync($"IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');");
            await ExecuteAsync(
                $"""
                CREATE TABLE [{schema}].[jobs] (
                    id uniqueidentifier NOT NULL PRIMARY KEY NONCLUSTERED,
                    queue nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                    payload_json nvarchar(max) NOT NULL,
                    headers_json nvarchar(max) NOT NULL DEFAULT N'{emptyJsonDefault}',
                    created_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    available_at datetime2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
                    locked_until datetime2 NULL,
                    lock_id uniqueidentifier NULL,
                    attempts int NOT NULL DEFAULT 0,
                    dead_letter_reason nvarchar(max) NULL
                );
                """);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => new SqlServerTransportStore(Options.Create(options)).EnsureCreatedAsync());
            Assert.Contains("available_at", ex.Message, StringComparison.Ordinal);
            Assert.Contains("expected datetime2(7)", ex.Message, StringComparison.Ordinal);
            Assert.Contains("found datetime2(0)", ex.Message, StringComparison.Ordinal);
        });

        await WithSchemaAsync("sql_full_scale", async schema =>
        {
            // A bare datetime2 declaration IS datetime2(7): the operator-provisioned twin of the
            // DDL path's own table must keep passing.
            var options = TransportOptions(schema);
            options.MessageTable = "jobs";
            await new SqlServerTransportStore(Options.Create(options)).EnsureCreatedAsync();

            var managed = TransportOptions(schema);
            managed.MessageTable = "jobs";
            managed.AutoCreateSchema = false;
            await new SqlServerTransportStore(Options.Create(managed)).EnsureCreatedAsync();
        });
    }

    [Fact]
    public async Task TableMissingARequiredDefault_OrItsPrimaryKey_IsRejected()
    {
        // The store never names these columns on insert, so a table without their defaults fails
        // every insert with error 515 — and one without the primary key silently accepts the
        // duplicates the idempotent publish relies on it to reject. Both used to pass verification.
        await WithSchemaAsync("sql_no_default", async schema =>
        {
            var options = TransportOptions(schema);
            options.MessageTable = "jobs";
            var emptyJsonDefault = "{}";
            await ExecuteAsync($"IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');");
            // Exactly ONE default is missing, so the message must name that column and no other.
            await ExecuteAsync(
                $"""
                CREATE TABLE [{schema}].[jobs] (
                    id uniqueidentifier NOT NULL PRIMARY KEY NONCLUSTERED,
                    queue nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                    payload_json nvarchar(max) NOT NULL,
                    headers_json nvarchar(max) NOT NULL DEFAULT N'{emptyJsonDefault}',
                    created_at datetime2 NOT NULL,
                    available_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    locked_until datetime2 NULL,
                    lock_id uniqueidentifier NULL,
                    attempts int NOT NULL DEFAULT 0,
                    dead_letter_reason nvarchar(max) NULL
                );
                """);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => new SqlServerTransportStore(Options.Create(options)).EnsureCreatedAsync());
            Assert.Contains("created_at", ex.Message, StringComparison.Ordinal);
            Assert.Contains("has no default", ex.Message, StringComparison.Ordinal);
        });

        await WithSchemaAsync("sql_no_pk", async schema =>
        {
            var options = TransportOptions(schema);
            options.MessageTable = "jobs";
            var emptyJsonDefault = "{}";
            await ExecuteAsync($"IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');");
            await ExecuteAsync(
                $"""
                CREATE TABLE [{schema}].[jobs] (
                    id uniqueidentifier NOT NULL,
                    queue nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
                    payload_json nvarchar(max) NOT NULL,
                    headers_json nvarchar(max) NOT NULL DEFAULT N'{emptyJsonDefault}',
                    created_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    available_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    locked_until datetime2 NULL,
                    lock_id uniqueidentifier NULL,
                    attempts int NOT NULL DEFAULT 0,
                    dead_letter_reason nvarchar(max) NULL
                );
                """);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => new SqlServerTransportStore(Options.Create(options)).EnsureCreatedAsync());
            Assert.Contains("has no primary key", ex.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ManagedSchemaValidation_CaseInsensitiveCorrelationCollation_IsRejectedAtStartup()
    {
        // Regression (round 31): AutoCreateSchema = false ran only the acked_seq migration probe
        // and skipped full relation verification, so an operator-provisioned correlation_id
        // without a binary collation was accepted at startup — and SQL Server's `=` then folded
        // case (and padded trailing spaces) at runtime, cross-routing responses silently. The
        // managed path now verifies relations exactly like the DDL path, the transport and the
        // flow store.
        await WithSchemaAsync("managed_collation", async schema =>
        {
            var options = ChannelOptions(schema);
            var creator = new SqlServerChannelSql(Options.Create(options));
            await creator.EnsureCreatedAsync();

            await using (var connection = new SqlConnection(Fixture.SqlServerConnectionString))
            {
                await connection.OpenAsync();
                await using var degrade = connection.CreateCommand();
                // The legacy shape an operator-managed migration can produce: the server's
                // default case-insensitive collation on the identity column. The dependent index
                // must be dropped for the ALTER and is recreated with its exact definition, so
                // only the column's collation diverges from the contract.
                degrade.CommandText =
                    $"""
                    DROP INDEX {options.MessageTable}_correlation_created_idx ON {creator.MessageTable};
                    ALTER TABLE {creator.MessageTable} ALTER COLUMN correlation_id nvarchar(400) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL;
                    CREATE INDEX {options.MessageTable}_correlation_created_idx ON {creator.MessageTable} (correlation_id, created_at);
                    """;
                await degrade.ExecuteNonQueryAsync();
            }

            var managedOptions = ChannelOptions(schema);
            managedOptions.AutoCreateSchema = false;
            var managed = new SqlServerChannelSql(Options.Create(managedOptions));

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => managed.GetSubscriptionStartAsync(CancellationToken.None));
            Assert.Contains("correlation_id", ex.Message, StringComparison.Ordinal);
            Assert.Contains("collation", ex.Message, StringComparison.OrdinalIgnoreCase);
        });
    }

    [Fact]
    public async Task ManagedSchemaValidation_MissingAckSequenceObjects_FailsActionably_AndPassesAfterTheDocumentedMigration()
    {
        // A pre-1.0 manually managed schema (AutoCreateSchema = false) lacks acked_seq and its
        // sequence, which registration and delivery claims now require unconditionally. The
        // channel must fail at first use with an error carrying the exact migration — not a raw
        // "invalid column name" mid-operation — and work immediately once the documented
        // migration has been applied.
        await WithSchemaAsync("managed_upgrade", async schema =>
        {
            var creator = new SqlServerChannelSql(Options.Create(ChannelOptions(schema)));
            await creator.EnsureCreatedAsync();
            await using (var connection = new SqlConnection(Fixture.SqlServerConnectionString))
            {
                await connection.OpenAsync();
                await using var strip = connection.CreateCommand();
                strip.CommandText =
                    $"""
                    ALTER TABLE {creator.MessageTable} DROP COLUMN acked_seq;
                    DROP SEQUENCE {creator.AckSequence};
                    """;
                await strip.ExecuteNonQueryAsync();
            }

            var managedOptions = ChannelOptions(schema);
            managedOptions.AutoCreateSchema = false;
            var managed = new SqlServerChannelSql(Options.Create(managedOptions));

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => managed.GetSubscriptionStartAsync(CancellationToken.None));
            Assert.Contains("acked_seq", ex.Message, StringComparison.Ordinal);
            Assert.Contains("docs/sqlserver.md", ex.Message, StringComparison.Ordinal);

            // The exact migration from docs/sqlserver.md, "Upgrading a manually managed schema".
            await using (var connection = new SqlConnection(Fixture.SqlServerConnectionString))
            {
                await connection.OpenAsync();
                await using var migrate = connection.CreateCommand();
                migrate.CommandText = ex.Message[(ex.Message.IndexOf("IF COL_LENGTH", StringComparison.Ordinal))..ex.Message.IndexOf(" See docs/", StringComparison.Ordinal)];
                await migrate.ExecuteNonQueryAsync();
            }

            var (_, startSeq) = await managed.GetSubscriptionStartAsync(CancellationToken.None);
            Assert.True(startSeq > 0);

            // Rolling-upgrade rule: a row acked by a PRE-sequence build (acked_at set, acked_seq
            // null — simulated here) must stay permanently unsequenced through later fan-out
            // re-claims. Back-filling would pair the old acked_at with a fresh sequence and let a
            // tick-tied waiter replay its predecessor's response.
            var legacyId = Guid.NewGuid();
            await managed.InsertMessageAsync(legacyId, "legacy-ack", SuccessEnvelope("old"), TimeSpan.FromSeconds(30), CancellationToken.None);
            await using (var connection = new SqlConnection(Fixture.SqlServerConnectionString))
            {
                await connection.OpenAsync();
                await using var legacyAck = connection.CreateCommand();
                legacyAck.CommandText = $"UPDATE {creator.MessageTable} SET acked_at = SYSUTCDATETIME() WHERE id = @id;";
                legacyAck.Parameters.AddWithValue("@id", legacyId);
                await legacyAck.ExecuteNonQueryAsync();
            }

            Assert.True(await managed.TryClaimForDeliveryAsync(legacyId, CancellationToken.None));
            var freshId = Guid.NewGuid();
            await managed.InsertMessageAsync(freshId, "fresh-ack", SuccessEnvelope("new"), TimeSpan.FromSeconds(30), CancellationToken.None);
            Assert.True(await managed.TryClaimForDeliveryAsync(freshId, CancellationToken.None));

            await using (var connection = new SqlConnection(Fixture.SqlServerConnectionString))
            {
                await connection.OpenAsync();
                await using var check = connection.CreateCommand();
                check.CommandText =
                    $"""
                    SELECT
                      CASE WHEN (SELECT acked_seq FROM {creator.MessageTable} WHERE id = @legacy) IS NULL THEN 1 ELSE 0 END,
                      CASE WHEN (SELECT acked_seq FROM {creator.MessageTable} WHERE id = @fresh) IS NOT NULL THEN 1 ELSE 0 END;
                    """;
                check.Parameters.AddWithValue("@legacy", legacyId);
                check.Parameters.AddWithValue("@fresh", freshId);
                await using var reader = await check.ExecuteReaderAsync();
                await reader.ReadAsync();
                Assert.Equal(1, reader.GetInt32(0));
                Assert.Equal(1, reader.GetInt32(1));
            }
        });
    }

    [Fact]
    public async Task ChannelSql_RoundTripsRecoverySubscribersMessagesAndClaims()
    {
        await WithSchemaAsync("channel_sql", async schema =>
        {
            var options = ChannelOptions(schema);
            var sql = new SqlServerChannelSql(Options.Create(options));
            var store = new SqlServerRecoveryStateStore(sql, NullLogger<SqlServerRecoveryStateStore>.Instance);
            await sql.EnsureCreatedAsync();

            Assert.Equal(Quote(schema), sql.Schema);
            Assert.Contains(Quote(options.MessageTable), sql.MessageTable, StringComparison.Ordinal);
            Assert.Equal(
                SqlServerTransportStore.SchemaLockResource(schema),
                SqlServerChannelSql.SchemaLockResource(schema));

            var correlationId = NewId("direct-recovery");
            var state = new RecoveryState
            {
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(OperationResult).FullName
            };

            await store.SaveAsync(correlationId, state, TimeSpan.FromSeconds(30));
            Assert.NotEqual(Guid.Empty, state.RegistrationId);

            var stored = Assert.Single(await store.GetAllAsync(correlationId));
            Assert.Equal(correlationId, stored.CorrelationId);
            Assert.Equal(state.RegistrationId, stored.RegistrationId);

            var scanned = new List<RecoveryState>();
            await foreach (var scannedState in store.ScanAsync())
                scanned.Add(scannedState);
            Assert.Contains(scanned, item => item.CorrelationId == correlationId);

            var newerState = new RecoveryState
            {
                CorrelationId = "future-state",
                RegistrationId = Guid.NewGuid(),
                PayloadTypeFullName = typeof(OperationResult).FullName,
                SchemaVersion = RecoveryStateSchema.Current + 1
            };
            await sql.SaveRecoveryStateAsync("future-state", newerState, TimeSpan.FromSeconds(30), CancellationToken.None);
            // Every stored registration for this id is unreadable, so an empty list would be a lie:
            // it reads as "no callback was ever armed", which the dispatcher answers by ACKing the
            // terminal response. Failing the delivery is what keeps the response recoverable.
            await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => store.GetAllAsync("future-state"));

            await InsertUnreadableRecoveryStateAsync(schema, options.RecoveryStateTable, "bad-state");
            await Assert.ThrowsAsync<RecoveryStateUnreadableException>(() => store.GetAllAsync("bad-state"));

            Assert.False(await store.TryDeleteAsync(correlationId, Guid.NewGuid()));
            Assert.True(await store.TryDeleteAsync(correlationId, state.RegistrationId));
            Assert.Empty(await store.GetAllAsync(correlationId));

            var deleteState = new RecoveryState { CorrelationId = correlationId };
            await store.SaveAsync(correlationId, deleteState, TimeSpan.FromSeconds(30));
            Assert.True(await store.TryDeleteAsync(correlationId, deleteState.RegistrationId));
            Assert.Empty(await store.GetAllAsync(correlationId));

            await store.SaveAsync("expired-state", new RecoveryState { CorrelationId = "expired-state" }, TimeSpan.FromMilliseconds(1));
            await Task.Delay(40);
            Assert.Empty(await store.GetAllAsync("expired-state"));

            var subscriberId = Guid.NewGuid();
            await sql.UpsertSubscriberAsync("subscribed", subscriberId, "test-instance", TimeSpan.FromSeconds(30), CancellationToken.None);
            await sql.UpsertSubscriberAsync("expired-subscriber", Guid.NewGuid(), "test-instance", TimeSpan.FromMilliseconds(1), CancellationToken.None);
            await Task.Delay(40);
            Assert.Equal(1, await sql.CountActiveSubscribersAsync("subscribed", CancellationToken.None));
            Assert.Equal(0, await sql.CountActiveSubscribersAsync("expired-subscriber", CancellationToken.None));
            await sql.DeleteSubscriberAsync("subscribed", subscriberId, CancellationToken.None);
            Assert.Equal(0, await sql.CountActiveSubscribersAsync("subscribed", CancellationToken.None));

            var heartbeatA = Guid.NewGuid();
            var heartbeatB = Guid.NewGuid();
            var staleHeartbeat = Guid.NewGuid();
            await sql.UpsertSubscriberAsync("heartbeat-a", heartbeatA, "heartbeat-instance", TimeSpan.FromMilliseconds(100), CancellationToken.None);
            await sql.UpsertSubscriberAsync("heartbeat-b", heartbeatB, "heartbeat-instance", TimeSpan.FromMilliseconds(100), CancellationToken.None);
            await sql.UpsertSubscriberAsync("heartbeat-stale", staleHeartbeat, "heartbeat-instance", TimeSpan.FromMilliseconds(100), CancellationToken.None);
            await Task.Delay(50);
            await sql.HeartbeatSubscribersAsync(
                "heartbeat-instance",
                [("heartbeat-a", heartbeatA), ("heartbeat-b", heartbeatB)],
                TimeSpan.FromSeconds(2),
                CancellationToken.None);
            await Task.Delay(100);
            Assert.Equal(1, await sql.CountActiveSubscribersAsync("heartbeat-a", CancellationToken.None));
            Assert.Equal(1, await sql.CountActiveSubscribersAsync("heartbeat-b", CancellationToken.None));
            Assert.Equal(0, await sql.CountActiveSubscribersAsync("heartbeat-stale", CancellationToken.None));

            // The heartbeat is an UPSERT: a row pruned out from under a live waiter (e.g. after a
            // >timeout stall) is re-created by the next heartbeat cycle instead of staying gone.
            await sql.DeleteSubscriberAsync("heartbeat-a", heartbeatA, CancellationToken.None);
            Assert.Equal(0, await sql.CountActiveSubscribersAsync("heartbeat-a", CancellationToken.None));
            await sql.HeartbeatSubscribersAsync(
                "heartbeat-instance",
                [("heartbeat-a", heartbeatA)],
                TimeSpan.FromSeconds(2),
                CancellationToken.None);
            Assert.Equal(1, await sql.CountActiveSubscribersAsync("heartbeat-a", CancellationToken.None));

            var startedAt = await sql.GetServerTimeUtcAsync(CancellationToken.None);
            var messageId = Guid.NewGuid();
            var firstCreatedAt = (await sql.InsertMessageAsync(messageId, "message-correlation", SuccessEnvelope("first"), TimeSpan.FromSeconds(30), CancellationToken.None)).CreatedAtUtc;
            var duplicateCreatedAt = (await sql.InsertMessageAsync(messageId, "message-correlation", SuccessEnvelope("duplicate"), TimeSpan.FromSeconds(30), CancellationToken.None)).CreatedAtUtc;

            // The insert returns the row's server-stamped created_at — the ORIGINAL row's on a
            // duplicate — so the same-process fast path compares like clocks under app-clock skew.
            Assert.Equal(firstCreatedAt, duplicateCreatedAt);

            var messages = await sql.LoadMessagesAsync("message-correlation", startedAt.AddSeconds(-5), 10, null, null, CancellationToken.None);
            var message = Assert.Single(messages);
            Assert.Equal(messageId, message.Id);
            Assert.Equal("message-correlation", message.CorrelationId);
            Assert.Equal(firstCreatedAt, message.CreatedAtUtc);
            Assert.False(await sql.IsMessageAcknowledgedAsync(messageId, CancellationToken.None));
            Assert.True(await sql.TryClaimForDeliveryAsync(messageId, CancellationToken.None));
            Assert.True(await sql.IsMessageAcknowledgedAsync(messageId, CancellationToken.None));
            Assert.False(await sql.TryClaimForRecoveryAsync(messageId, CancellationToken.None));

            var recoveryMessageId = Guid.NewGuid();
            await sql.InsertMessageAsync(recoveryMessageId, "recovery-correlation", SuccessEnvelope("late"), TimeSpan.FromSeconds(30), CancellationToken.None);
            Assert.True(await sql.TryClaimForRecoveryAsync(recoveryMessageId, CancellationToken.None));
            Assert.False(await sql.TryClaimForDeliveryAsync(recoveryMessageId, CancellationToken.None));

            await sql.InsertMessageAsync(Guid.NewGuid(), "expired-message", SuccessEnvelope("old"), TimeSpan.FromMilliseconds(1), CancellationToken.None);
            await Task.Delay(40);
            await sql.InsertMessageAsync(Guid.NewGuid(), "fresh-message", SuccessEnvelope("new"), TimeSpan.FromSeconds(30), CancellationToken.None);
            Assert.Empty(await sql.LoadMessagesAsync("expired-message", startedAt.AddSeconds(-5), 10, null, null, CancellationToken.None));

            const int pagedCount = 70;
            var pagedCorrelation = NewId("paged-messages");
            for (var index = 0; index < pagedCount; index++)
                await sql.InsertMessageAsync(Guid.NewGuid(), pagedCorrelation, SuccessEnvelope($"page-{index}"), TimeSpan.FromSeconds(30), CancellationToken.None);
            var paged = new List<SqlServerChannelMessage>();
            DateTimeOffset? afterCreatedAtUtc = null;
            Guid? afterId = null;
            while (true)
            {
                var page = await sql.LoadMessagesAsync(pagedCorrelation, startedAt.AddSeconds(-5), 16, afterCreatedAtUtc, afterId, CancellationToken.None);
                paged.AddRange(page);
                if (page.Count < 16)
                    break;
                afterCreatedAtUtc = page[^1].CreatedAtUtc;
                afterId = page[^1].Id;
            }
            Assert.Equal(pagedCount, paged.Count);
            Assert.Equal(pagedCount, paged.Select(item => item.Id).Distinct().Count());
        });
    }

    [Fact]
    public async Task Channel_DeliversLiveResponsesAndLostSubscriberCallbacks()
    {
        var schema = NewSchema("channel");
        ServiceProvider? provider = null;
        try
        {
            provider = BuildProvider(schema);
            var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
            var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
            var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
            var recoverable = provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>();
            var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
            var channel = provider.GetRequiredService<SqlServerAsyncResponseChannel>();
            var flow = provider.GetRequiredService<DirectRecoveryFlow>();

            var correlationId = NewId("live");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
                correlationId,
                payload => new ValueTask<bool>(payload.Status == OperationStatus.Completed),
                TimeSpan.FromSeconds(5)))
            {
                Assert.Equal(1, await probe.CountActiveSubscribersAsync(correlationId));
                await publisher.SetResponse(new OperationResult { Status = OperationStatus.Running, Message = "progress" }, correlationId);
                await Task.Delay(100);
                Assert.False(waiter.ResponseTask.IsCompleted);

                await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "done" }, correlationId);
                var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Equal("done", result.Message);
            }

            var rawCorrelationId = NewId("raw");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(rawCorrelationId, timeout: TimeSpan.FromSeconds(5)))
            {
                await rawPublisher.SetRawResponseJson("""{"Status":2,"Message":"raw"}""", rawCorrelationId);
                var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Equal("raw", result.Message);
            }

            var rawObjectCorrelationId = NewId("raw-object");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(rawObjectCorrelationId, timeout: TimeSpan.FromSeconds(5)))
            {
                await rawPublisher.SetRawResponse(new OperationResult { Status = OperationStatus.Completed, Message = "raw-object" }, rawObjectCorrelationId);
                var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Equal("raw-object", result.Message);
            }

            var exceptionCorrelationId = NewId("exception");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(exceptionCorrelationId, timeout: TimeSpan.FromSeconds(5)))
            {
                await publisher.SetException(new InvalidOperationException("remote boom"), exceptionCorrelationId);
                var ex = await Assert.ThrowsAnyAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
                Assert.Equal("remote boom", ex.Message);
            }

            await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, " ");
            await rawPublisher.SetRawResponseJson("""{"Status":2}""", " ");
            await publisher.SetException(new InvalidOperationException("blank"), " ");
            Assert.Equal(0, await probe.CountActiveSubscribersAsync(" "));

            var resumeCorrelationId = NewId("lost-resume");
            await using (var waiter = await recoverable.CreateRecoverableResponseWaiter<OperationResult>(
                resumeCorrelationId,
                ResumeCallback(),
                FailureCallback(),
                timeout: TimeSpan.FromSeconds(5)))
            {
                await channel.DropLocalSubscriptionsAsync();
                await publisher.SetResponse(
                    new OperationResult { Status = OperationStatus.Completed, Message = "late" },
                    resumeCorrelationId);
                var resumed = await flow.WaitResumeAsync(resumeCorrelationId).WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Equal("late", resumed.Message);
            }

            var rawLostCorrelationId = NewId("lost-raw");
            await using (var waiter = await recoverable.CreateRecoverableResponseWaiter<OperationResult>(
                rawLostCorrelationId,
                ResumeCallback(),
                FailureCallback(),
                timeout: TimeSpan.FromSeconds(5)))
            {
                await channel.DropLocalSubscriptionsAsync();
                await rawPublisher.SetRawResponseJson("""{"Status":2,"Message":"late raw"}""", rawLostCorrelationId);
                var resumed = await flow.WaitResumeAsync(rawLostCorrelationId).WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Equal("late raw", resumed.Message);
            }

            var failedCorrelationId = NewId("lost-failed");
            await using (var waiter = await recoverable.CreateRecoverableResponseWaiter<OperationResult>(
                failedCorrelationId,
                ResumeCallback(),
                FailureCallback(),
                timeout: TimeSpan.FromSeconds(5)))
            {
                await channel.DropLocalSubscriptionsAsync();
                await publisher.SetResponse(
                    new OperationResult { Status = OperationStatus.Failed, Message = "domain failed" },
                    failedCorrelationId);
                var failure = await flow.WaitFailureAsync(failedCorrelationId).WaitAsync(TimeSpan.FromSeconds(5));
                var domainFailure = Assert.IsType<AsyncResponseDomainFailureException>(failure);
                Assert.Contains("domain failed", domainFailure.PayloadJson, StringComparison.Ordinal);
            }

            var lostExceptionCorrelationId = NewId("lost-exception");
            await using (var waiter = await recoverable.CreateRecoverableResponseWaiter<OperationResult>(
                lostExceptionCorrelationId,
                ResumeCallback(),
                FailureCallback(),
                timeout: TimeSpan.FromSeconds(5)))
            {
                await channel.DropLocalSubscriptionsAsync();
                await publisher.SetException(new InvalidOperationException("late exception"), lostExceptionCorrelationId);
                var failure = await flow.WaitFailureAsync(lostExceptionCorrelationId).WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Equal("late exception", failure.Message);
            }

            var callback = ResumeCallback();
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                recoverable.CreateRecoverableResponseWaiter<DefaultRecoveryPayload>(
                    NewId("default-recovery"),
                    callback,
                    timeout: TimeSpan.FromSeconds(5)));
        }
        finally
        {
            if (provider is not null)
                await provider.DisposeAsync();
            await DropSchemaAsync(schema);
        }
    }

    [Fact]
    public async Task Channel_DeliversLocalResponsesWithoutWaitingForSweepBacklog()
    {
        var schema = NewSchema("fast");
        ServiceProvider? provider = null;
        var waiters = new List<IAsyncResponseWaiter<OperationResult>>();
        try
        {
            provider = BuildProvider(schema, options =>
            {
                // A deliberately glacial sweep (30s): local deliveries must complete through the
                // same-process fast path without ever waiting for the polling loop. The assertion
                // window (20s) stays below the sweep so a pass can only mean in-process delivery.
                // DeliveryConfirmationTimeout is generous (5s) on purpose: it is the budget after which
                // the publisher gives a response up to recovery, so it must comfortably exceed a claim
                // round-trip under a loaded CI database. A too-tight value (the previous 20ms is below a
                // CI round-trip) makes the publisher steal a live-but-slow local delivery to recovery,
                // starving the waiter — which is exactly what flaked under the heavier integration fixture.
                options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5);
                options.ActivePollInterval = TimeSpan.FromSeconds(30);
                options.IdlePollInterval = TimeSpan.FromSeconds(30);
            });
            var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
            var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();

            var correlationIds = Enumerable.Range(0, 32)
                .Select(_ => NewId("local-fast"))
                .ToArray();

            foreach (var correlationId in correlationIds)
                waiters.Add(await subscriber.CreateResponseWaiter<OperationResult>(
                    correlationId,
                    timeout: TimeSpan.FromSeconds(20)));

            await Task.WhenAll(correlationIds.Select(correlationId =>
                publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, correlationId)))
                .WaitAsync(TimeSpan.FromSeconds(20));

            var results = await Task.WhenAll(waiters.Select(waiter => waiter.ResponseTask))
                .WaitAsync(TimeSpan.FromSeconds(20));
            Assert.All(results, result => Assert.Equal(OperationStatus.Completed, result.Status));
        }
        finally
        {
            foreach (var waiter in waiters)
                await waiter.DisposeAsync();
            if (provider is not null)
                await provider.DisposeAsync();
            await DropSchemaAsync(schema);
        }
    }

    [Fact]
    public async Task Channel_RegressionEdges_HandleFallbacksFaultedEnvelopesAndSetupFailures()
    {
        var schema = NewSchema("channel_edges");
        ServiceProvider? provider = null;
        try
        {
            provider = BuildProvider(schema);
            var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
            var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
            var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
            var recoverableStore = provider.GetRequiredService<IRecoveryStateStore>();
            var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
            var sql = provider.GetRequiredService<SqlServerChannelSql>();
            var channel = provider.GetRequiredService<SqlServerAsyncResponseChannel>();
            var flow = provider.GetRequiredService<DirectRecoveryFlow>();

            await Assert.ThrowsAsync<ArgumentNullException>(() =>
                subscriber.CreateResponseWaiter<OperationResult>(" "));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                subscriber.CreateResponseWaiter<OperationResult>(NewId("bad-timeout"), timeout: TimeSpan.Zero));

            var recoveryClaimedCorrelationId = NewId("recovery-claimed-before-waiter");
            var recoveryClaimedMessageId = Guid.NewGuid();
            await sql.InsertMessageAsync(
                recoveryClaimedMessageId,
                recoveryClaimedCorrelationId,
                SuccessEnvelope("already-recovered"),
                TimeSpan.FromSeconds(30),
                CancellationToken.None);
            Assert.True(await sql.TryClaimForRecoveryAsync(recoveryClaimedMessageId, CancellationToken.None));
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
                recoveryClaimedCorrelationId,
                timeout: TimeSpan.FromSeconds(1)))
            {
                await Assert.ThrowsAsync<TimeoutException>(() =>
                    waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(3)));
            }

            var malformedCorrelationId = NewId("malformed");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(malformedCorrelationId, timeout: TimeSpan.FromSeconds(5)))
            {
                await sql.InsertMessageAsync(Guid.NewGuid(), malformedCorrelationId, "null", TimeSpan.FromSeconds(30), CancellationToken.None);
                await Assert.ThrowsAsync<JsonException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
            }

            var futureSchemaCorrelationId = NewId("future-envelope");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(futureSchemaCorrelationId, timeout: TimeSpan.FromSeconds(5)))
            {
                await sql.InsertMessageAsync(
                    Guid.NewGuid(),
                    futureSchemaCorrelationId,
                    """{"SchemaVersion":999,"Success":true,"Payload":{"Status":2},"ExceptionMessage":null,"ExceptionStackTrace":null}""",
                    TimeSpan.FromSeconds(30),
                    CancellationToken.None);
                var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
                Assert.Contains("schema version", ex.Message, StringComparison.Ordinal);
            }

            var predicateCorrelationId = NewId("predicate");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
                predicateCorrelationId,
                _ => throw new InvalidOperationException("predicate boom"),
                TimeSpan.FromSeconds(5)))
            {
                await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, predicateCorrelationId);
                var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
                Assert.Equal("predicate boom", ex.Message);
            }

            var stackCorrelationId = NewId("remote-stack");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(stackCorrelationId, timeout: TimeSpan.FromSeconds(5)))
            {
                Exception captured;
                try
                {
                    throw new InvalidOperationException("with stack");
                }
                catch (Exception thrown)
                {
                    captured = thrown;
                }

                await publisher.SetException(captured, stackCorrelationId);
                var remoteFailure = await Assert.ThrowsAnyAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
                Assert.Equal("with stack", remoteFailure.Message);
                Assert.True(remoteFailure.Data.Contains("RemoteStackTrace"));
            }

            var noLocalResponse = NewId("no-local-response");
            await ArmRecoveryStateAsync(recoverableStore, noLocalResponse);
            await sql.UpsertSubscriberAsync(noLocalResponse, Guid.NewGuid(), "remote-instance", TimeSpan.FromSeconds(1), CancellationToken.None);
            await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "confirmed late" }, noLocalResponse);
            var resumed = await flow.WaitResumeAsync(noLocalResponse).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("confirmed late", resumed.Message);

            var noLocalRaw = NewId("no-local-raw");
            await ArmRecoveryStateAsync(recoverableStore, noLocalRaw);
            await sql.UpsertSubscriberAsync(noLocalRaw, Guid.NewGuid(), "remote-instance", TimeSpan.FromSeconds(1), CancellationToken.None);
            await rawPublisher.SetRawResponseJson("""{"Status":2,"Message":"confirmed raw"}""", noLocalRaw);
            var rawResumed = await flow.WaitResumeAsync(noLocalRaw).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("confirmed raw", rawResumed.Message);

            var noLocalException = NewId("no-local-exception");
            await ArmRecoveryStateAsync(recoverableStore, noLocalException);
            await sql.UpsertSubscriberAsync(noLocalException, Guid.NewGuid(), "remote-instance", TimeSpan.FromSeconds(1), CancellationToken.None);
            await publisher.SetException(new InvalidOperationException("confirmed exception"), noLocalException);
            var failure = await flow.WaitFailureAsync(noLocalException).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("confirmed exception", failure.Message);

            Assert.Equal(0, await probe.CountActiveSubscribersAsync(" "));

            var lingering = await subscriber.CreateResponseWaiter<OperationResult>(NewId("dispose-active"), timeout: TimeSpan.FromSeconds(30));
            await using (lingering)
            {
                await channel.DisposeAsync();
                await Assert.ThrowsAsync<ObjectDisposedException>(() =>
                    subscriber.CreateResponseWaiter<OperationResult>(NewId("disposed"), timeout: TimeSpan.FromSeconds(5)));
            }
        }
        finally
        {
            if (provider is not null)
                await provider.DisposeAsync();
            await DropSchemaAsync(schema);
        }

        var badSchema = NewSchema("missing_channel");
        await using var badProvider = BuildProvider(badSchema, options => options.AutoCreateSchema = false);
        var badSubscriber = badProvider.GetRequiredService<IAsyncResponseSubscriber>();
        var badPublisher = badProvider.GetRequiredService<IAsyncResponsePublisher>();
        var badRawPublisher = badProvider.GetRequiredService<IRawAsyncResponsePublisher>();
        var badProbe = badProvider.GetRequiredService<IActiveSubscriberProbe>();

        // Registration failure must throw instead of returning a pre-faulted waiter, so the
        // builder can never run its trigger with no registration behind it.
        await Assert.ThrowsAnyAsync<Exception>(() =>
            badSubscriber.CreateResponseWaiter<OperationResult>(NewId("missing"), timeout: TimeSpan.FromSeconds(5)));

        // -1, not 0: the subscriber table does not exist under AutoCreateSchema=false, so the probe
        // could not be run at all. Reporting 0 would assert "definitively no live waiter" and let
        // the watchdog flag every over-threshold registration stale.
        Assert.Equal(-1, await badProbe.CountActiveSubscribersAsync(NewId("missing-count")));
        await Assert.ThrowsAnyAsync<Exception>(() =>
            badPublisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, NewId("missing-response")));
        await Assert.ThrowsAnyAsync<Exception>(() =>
            badRawPublisher.SetRawResponseJson("""{"Status":2}""", NewId("missing-raw")));
        await Assert.ThrowsAnyAsync<Exception>(() =>
            badPublisher.SetException(new InvalidOperationException("missing"), NewId("missing-exception")));
    }

    [Fact]
    public async Task TransportStore_WorkerTransportSubscribersAndDeliveryStatesRoundTrip()
    {
        await WithSchemaAsync("transport", async schema =>
        {
            var options = TransportOptions(schema);
            var optionsAccessor = Options.Create(options);
            var store = new SqlServerTransportStore(optionsAccessor);
            await store.EnsureCreatedAsync();

            Assert.Equal(Quote(schema), store.Schema);
            Assert.Contains(Quote(options.MessageTable), store.MessageTable, StringComparison.Ordinal);

            // Same-process wake: publishes raise the in-process event that replaces LISTEN/NOTIFY.
            var published = new List<string?>();
            store.MessagePublished += published.Add;

            var id = Guid.NewGuid();
            await store.PublishAsync(
                id,
                options.WorkerQueue,
                """{"kind":"ack"}""",
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["X-Trace"] = "trace-1" },
                CancellationToken.None);
            Assert.Contains(options.WorkerQueue, published);

            // Publishing the same id again is a no-op (idempotent publish).
            await store.PublishAsync(id, options.WorkerQueue, """{"kind":"ack-duplicate"}""", null, CancellationToken.None);

            var delivery = await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None);
            Assert.NotNull(delivery);
            Assert.Equal(id, delivery.Id);
            Assert.Equal(1, delivery.Attempt);
            Assert.Equal("""{"kind":"ack"}""", delivery.Payload);
            Assert.Equal("trace-1", delivery.Headers["x-trace"]);
            await delivery.AckAsync();
            Assert.Null(await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None));

            published.Clear();
            var nakId = Guid.NewGuid();
            await store.PublishAsync(nakId, options.WorkerQueue, """{"kind":"nak"}""", null, CancellationToken.None);
            var retry = (await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None))!;
            // Ten minutes, not thirty milliseconds. The assertion below is that the row is INVISIBLE
            // during its delay, and a delay short enough to expire while the next round trip is in
            // flight turns that assertion into a stopwatch race against the database — which is how
            // it failed in CI. The delay is not what is under test here; the invisibility is.
            await retry.NakAsync(TimeSpan.FromMinutes(10));
            Assert.Contains(null, published); // a NAK release wakes every queue's subscriber
            Assert.Null(await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None));

            // Bring the row forward by hand so redelivery is a fact rather than a wait: the release
            // itself is what the rest of the assertion is about.
            await ExecuteAsync(
                $"UPDATE [{schema}].[{options.MessageTable}] SET available_at = DATEADD(minute, -1, SYSUTCDATETIME()) WHERE id = '{nakId}';");
            var retried = (await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None))!;
            Assert.True(await AckAndMatchAttemptAsync(retried, 2));

            await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, """{"kind":"deadletter"}""", null, CancellationToken.None);
            var poison = (await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None))!;
            Assert.True(await poison.DeadLetterAsync(new InvalidOperationException("line1\nline2"), true, CancellationToken.None));
            Assert.Null(await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None));
            var deadLetter = (await store.TryClaimAsync(options.DeadLetterQueue, options.LockTimeout, CancellationToken.None))!;
            Assert.Equal(options.DeadLetterQueue, deadLetter.Queue);
            Assert.Equal("line1 line2", deadLetter.Headers["AR-DeadLetter-Reason"]);
            Assert.Equal(options.WorkerQueue, deadLetter.Headers["AR-DeadLetter-Source-Queue"]);
            await deadLetter.AckAsync();

            var disabledOptions = TransportOptions(schema);
            disabledOptions.DeadLetterEnabled = false;
            var disabledStore = new SqlServerTransportStore(Options.Create(disabledOptions));
            await disabledStore.PublishAsync(Guid.NewGuid(), disabledOptions.WorkerQueue, """{"kind":"disabled"}""", null, CancellationToken.None);
            var disabled = (await disabledStore.TryClaimAsync(disabledOptions.WorkerQueue, disabledOptions.LockTimeout, CancellationToken.None))!;
            Assert.True(await disabled.DeadLetterAsync(new InvalidOperationException("no dlq"), true, CancellationToken.None));
            Assert.Null(await disabledStore.TryClaimAsync(disabledOptions.WorkerQueue, disabledOptions.LockTimeout, CancellationToken.None));

            for (var i = 0; i < 3; i++)
                await store.PublishAsync(Guid.NewGuid(), "batch", $$"""{"index":{{i}}}""", null, CancellationToken.None);

            var batch = new List<SqlServerTransportDelivery>();
            await foreach (var item in store.ClaimBatchAsync("batch", 2, options.LockTimeout, CancellationToken.None))
                batch.Add(item);
            Assert.Equal(2, batch.Count);
            foreach (var item in batch)
                await item.AckAsync();
            await DrainQueueAsync(store, "batch", options.LockTimeout);

            var transport = new SqlServerWorkerTransport(optionsAccessor, store);
            await transport.PublishAsync(new WorkerJobEnvelope
            {
                CorrelationId = "corr-worker",
                Call = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IDirectSqlServerRecoveryFlow).FullName!,
                    MethodName = nameof(IDirectSqlServerRecoveryFlow.ResumeAsync),
                    Params = []
                },
                ReplyTarget = new AsyncResponseReplyTarget
                {
                    Name = "default",
                    Transport = SqlServerAsyncResponseTransportOptions.TransportName,
                    Address = options.ResponseQueue
                }
            });

            var jobDelivery = (await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None))!;
            Assert.Equal("corr-worker", jobDelivery.Headers[options.CorrelationIdHeader]);
            var job = JsonSerializer.Deserialize<WorkerJobEnvelope>(jobDelivery.Payload);
            Assert.NotNull(job);
            Assert.Equal("corr-worker", job.CorrelationId);
            Assert.Equal(nameof(IDirectSqlServerRecoveryFlow.ResumeAsync), job.Call.MethodName);
            await jobDelivery.AckAsync();

            var ingress = new RecordingIngress();
            var workerSubscriber = new SqlServerWorkerSubscriber(
                optionsAccessor,
                store,
                ingress,
                NullLogger<SqlServerWorkerSubscriber>.Instance);
            var responseSubscriber = new SqlServerResponseIngressSubscriber(
                optionsAccessor,
                store,
                ingress,
                NullLogger<SqlServerResponseIngressSubscriber>.Instance);

            await workerSubscriber.StartAsync(CancellationToken.None);
            await responseSubscriber.StartAsync(CancellationToken.None);
            try
            {
                await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, """{"worker":true}""", null, CancellationToken.None);
                await store.PublishAsync(Guid.NewGuid(), options.ResponseQueue, """{"CorrelationId":"corr-response","Status":2}""", null, CancellationToken.None);

                using (var workerJson = JsonDocument.Parse(await ingress.WorkerReceived.Task.WaitAsync(TimeSpan.FromSeconds(5))))
                    Assert.True(workerJson.RootElement.GetProperty("worker").GetBoolean());

                var response = await ingress.ResponseReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
                using (var responseJson = JsonDocument.Parse(response.Json))
                    Assert.Equal(2, responseJson.RootElement.GetProperty("Status").GetInt32());
                Assert.Equal("corr-response", response.CorrelationId);
            }
            finally
            {
                await workerSubscriber.StopAsync(CancellationToken.None);
                await responseSubscriber.StopAsync(CancellationToken.None);
            }
        });
    }

    private ServiceProvider BuildProvider(
        string schema,
        Action<SqlServerAsyncResponseChannelOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(EnabledLogger<>));
        services.AddSingleton<IDirectSqlServerRecoveryFlow, DirectRecoveryFlow>();
        services.AddSingleton(provider => (DirectRecoveryFlow)provider.GetRequiredService<IDirectSqlServerRecoveryFlow>());
        services.AddAsyncResponse().WithSqlServerChannel(options =>
        {
            ApplyChannelOptions(options, schema);
            configure?.Invoke(options);
        });
        return services.BuildServiceProvider();
    }

    private async Task WithSchemaAsync(string prefix, Func<string, Task> body)
    {
        var schema = NewSchema(prefix);
        try
        {
            await body(schema);
        }
        finally
        {
            await DropSchemaAsync(schema);
        }
    }

    private SqlServerAsyncResponseChannelOptions ChannelOptions(string schema)
    {
        var options = new SqlServerAsyncResponseChannelOptions();
        ApplyChannelOptions(options, schema);
        return options;
    }

    private void ApplyChannelOptions(SqlServerAsyncResponseChannelOptions options, string schema)
    {
        options.ConnectionString = Fixture.SqlServerConnectionString;
        options.SchemaName = schema;
        options.DefaultTimeout = TimeSpan.FromSeconds(5);
        options.RecoveryStateExpiry = TimeSpan.FromSeconds(30);
        options.MessageRetention = TimeSpan.FromSeconds(30);
        options.DeliveryConfirmationTimeout = TimeSpan.FromMilliseconds(250);
        options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(10);
        options.ActivePollInterval = TimeSpan.FromMilliseconds(25);
        options.IdlePollInterval = TimeSpan.FromMilliseconds(100);
        options.PendingMessageBatchSize = 32;
        options.SubscriberHeartbeatInterval = TimeSpan.FromMilliseconds(50);
        options.SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(1);
        options.PruneInterval = TimeSpan.Zero;
    }

    private SqlServerAsyncResponseTransportOptions TransportOptions(string schema)
    {
        var options = new SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = Fixture.SqlServerConnectionString,
            SchemaName = schema,
            WorkerQueue = $"{schema}_worker",
            ResponseQueue = $"{schema}_response",
            DeadLetterQueue = $"{schema}_deadletter",
            LockTimeout = TimeSpan.FromMilliseconds(200),
            DeadLetterRetention = TimeSpan.FromSeconds(30),
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(10),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(50)
        };

        options.WorkerSubscriber.BatchSize = 4;
        options.WorkerSubscriber.EmptyPollDelay = TimeSpan.FromMilliseconds(25);
        options.WorkerSubscriber.RedeliveryDelay = TimeSpan.FromMilliseconds(25);
        options.WorkerSubscriber.MaxDeliveryAttempts = 2;
        options.ResponseSubscriber.BatchSize = 4;
        options.ResponseSubscriber.EmptyPollDelay = TimeSpan.FromMilliseconds(25);
        options.ResponseSubscriber.RedeliveryDelay = TimeSpan.FromMilliseconds(25);
        options.ResponseSubscriber.MaxDeliveryAttempts = 2;
        return options;
    }

    private async Task DropSchemaAsync(string schema)
    {
        // SQL Server has no DROP SCHEMA ... CASCADE: drop the schema's views (a collision test may
        // have planted one), tables, and sequences (the channel's monotonic ack sequence) first,
        // then the schema — which refuses to go while anything still references it.
        await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            DECLARE @drop nvarchar(max) = N'';
            SELECT @drop += N'DROP VIEW ' + QUOTENAME(s.name) + N'.' + QUOTENAME(v.name) + N';'
            FROM sys.views v
            JOIN sys.schemas s ON v.schema_id = s.schema_id
            WHERE s.name = @schema;
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
    }

    private async Task InsertUnreadableRecoveryStateAsync(string schema, string recoveryTable, string correlationId)
    {
        await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {Quote(schema)}.{Quote(recoveryTable)}
                (correlation_id, registration_id, state_json, expires_at, registered_at)
            VALUES (@correlation_id, @registration_id, N'"bad-json-string"', DATEADD(SECOND, 30, SYSUTCDATETIME()), SYSUTCDATETIME());
            """;
        command.Parameters.AddWithValue("@correlation_id", correlationId);
        command.Parameters.AddWithValue("@registration_id", Guid.NewGuid());
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<bool> AckAndMatchAttemptAsync(SqlServerTransportDelivery delivery, int attempt)
    {
        var matched = delivery.Attempt == attempt;
        await delivery.AckAsync();
        return matched;
    }

    private static async Task DrainQueueAsync(SqlServerTransportStore store, string queue, TimeSpan lockTimeout)
    {
        while (await store.TryClaimAsync(queue, lockTimeout, CancellationToken.None) is { } delivery)
            await delivery.AckAsync();
    }

    private static Task ArmRecoveryStateAsync(IRecoveryStateStore store, string correlationId)
        => store.SaveAsync(correlationId, new RecoveryState
        {
            CorrelationId = correlationId,
            PayloadTypeFullName = typeof(OperationResult).FullName,
            ResumeCallback = ResumeCallback(),
            FailureCallback = FailureCallback()
        }, TimeSpan.FromSeconds(30));

    private static async Task EventuallyAsync(Func<Task<bool>> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!await condition())
        {
            await Task.Delay(25, cts.Token);
            cts.Token.ThrowIfCancellationRequested();
        }
    }

    private static ReflectionCallDto ResumeCallback() => new()
    {
        ServiceInterfaceFullName = typeof(IDirectSqlServerRecoveryFlow).FullName!,
        MethodName = nameof(IDirectSqlServerRecoveryFlow.ResumeAsync),
        Params =
        [
            CallbackParam.ForPlaceholder(PlaceholderType.Payload),
            CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId)
        ]
    };

    private static ReflectionCallDto FailureCallback() => new()
    {
        ServiceInterfaceFullName = typeof(IDirectSqlServerRecoveryFlow).FullName!,
        MethodName = nameof(IDirectSqlServerRecoveryFlow.FailAsync),
        Params =
        [
            CallbackParam.ForPlaceholder(PlaceholderType.Exception),
            CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId)
        ]
    };

    private static string NewSchema(string prefix)
        => $"ar_{prefix}_{Guid.NewGuid():N}";

    private static string Quote(string identifier) => "[" + identifier + "]";

    private static string SuccessEnvelope(string message)
        => $$"""{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"{{message}}"},"ExceptionMessage":null,"ExceptionStackTrace":null}""";

    [Fact]
    public async Task SqlServerAsyncResponseChannel_CoverInternalEdgeCases()
    {
        await WithSchemaAsync("channel_edges", async schema =>
        {
            var services = new ServiceCollection();
            services.AddLogging();
            var options = ChannelOptions(schema);
            options.DeliveryConfirmationTimeout = TimeSpan.FromMilliseconds(10);
            options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(5);
            
            services.AddSingleton(Options.Create(options));
            var sql = new SqlServerChannelSql(Options.Create(options));
            services.AddSingleton(sql);
            services.AddSingleton(MockRecoveryStore());
            services.AddSingleton(new AsyncResponseContextPropagation([]));
            services.AddSingleton<SqlServerAsyncResponseChannel>();
            
            await using var provider = services.BuildServiceProvider();
            var channel = provider.GetRequiredService<SqlServerAsyncResponseChannel>();
            
            // Cover EnsureCreatedAsync double call (first/second return)
            await sql.EnsureCreatedAsync();
            await sql.EnsureCreatedAsync();

            var subscription1 = Subscription(typeof(SqlServerAsyncResponseChannel), "DbSubscription`1", channel, _ => new ValueTask<bool>(true));
            var subscription2 = Subscription(typeof(SqlServerAsyncResponseChannel), "DbSubscription`1", channel, _ => new ValueTask<bool>(true));
            var subscription3 = Subscription(typeof(SqlServerAsyncResponseChannel), "DbSubscription`1", channel, _ => new ValueTask<bool>(true));
            
            SetField(subscription1.Instance, "_dropped", true);

            var addSubMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("AddSubscription", BindingFlags.Instance | BindingFlags.NonPublic)!;
            addSubMethod.Invoke(channel, ["corr", subscription1.Instance]);
            addSubMethod.Invoke(channel, ["corr", subscription2.Instance]);
            addSubMethod.Invoke(channel, ["corr", subscription3.Instance]);

            // 1. Cover DispatchPendingMessagesAsync where subscriptions.Count == 0
            var channelClean = provider.GetRequiredService<SqlServerAsyncResponseChannel>();
            var subsField = typeof(SqlServerAsyncResponseChannel).GetField("_subscriptions", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var subsDict = (System.Collections.IDictionary)subsField.GetValue(channelClean)!;
            subsDict.Clear();
            
            addSubMethod.Invoke(channelClean, ["corr-dropped-only", subscription1.Instance]);
            
            var dispatchPendingMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("DispatchPendingMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var scope = new HashSet<string> { "corr-dropped-only" };
            await (Task)dispatchPendingMethod.Invoke(channelClean, [scope, CancellationToken.None])!;

            // 2. Cover WaitForAcknowledgementAsync break branch and pollDelay branches
            var beginConfirmationMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("BeginConfirmation", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var tryConfirmMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("TryConfirmDeliveryAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
            
            var messageId = Guid.NewGuid();
            await sql.InsertMessageAsync(messageId, "corr", "{}", TimeSpan.FromMinutes(1), CancellationToken.None);
            
            var confirmation = beginConfirmationMethod.Invoke(channel, [messageId])!;
            var confirmed = await (Task<bool>)tryConfirmMethod.Invoke(channel, [confirmation, CancellationToken.None])!;
            // The window lapsed with the message never acked: the poll loop must report
            // non-delivery, not merely terminate.
            Assert.False(confirmed);

            // 3. Cover DispatchMessageToSubscribersAsync continue branch (dropped & seen & live)
            var messageId2 = Guid.NewGuid();
            // Cover PK violation unique constraint catch block
            await sql.InsertMessageAsync(messageId2, "corr", "{}", TimeSpan.FromMinutes(1), CancellationToken.None);
            await sql.InsertMessageAsync(messageId2, "corr", "{}", TimeSpan.FromMinutes(1), CancellationToken.None);

            subscription2.Instance.GetType().GetMethod("MarkSeen", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!
                .Invoke(subscription2.Instance, [messageId2]);

            var message2 = new SqlServerChannelMessage(messageId2, "corr", "{}", DateTimeOffset.UtcNow);
            var dispatchMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("DispatchMessageToSubscribersAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
            
            var subInterfaceType = typeof(SqlServerAsyncResponseChannel).BaseType!.GetNestedType("IDbSubscription", BindingFlags.NonPublic)!;
            var subArray = Array.CreateInstance(subInterfaceType, 3);
            subArray.SetValue(subscription1.Instance, 0); // Dropped
            subArray.SetValue(subscription2.Instance, 1); // Already seen
            subArray.SetValue(subscription3.Instance, 2); // Live (covers ProcessUnderCapturedContextAsync)
            
            await (Task)dispatchMethod.Invoke(channel, [message2, subArray, CancellationToken.None])!;

            // The fan-out actually discriminated: the dropped and already-seen waiters were
            // skipped, and only the live one was processed — its waiter settling (with the
            // envelope converter's rejection of this "{}" body, which carries no SchemaVersion)
            // proves ProcessUnderCapturedContextAsync ran end to end.
            Assert.False(subscription1.Completion.Task.IsCompleted);
            Assert.False(subscription2.Completion.Task.IsCompleted);
            var liveError = await Assert.ThrowsAsync<System.Text.Json.JsonException>(
                () => subscription3.Completion.Task.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.Contains("SchemaVersion", liveError.Message);

            // Start dispatcher to cover its background task dispose/cancellation
            var ensureDispatcherStartedMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("EnsureListenerStarted", BindingFlags.Instance | BindingFlags.NonPublic)!;
            ensureDispatcherStartedMethod.Invoke(channel, null);

            await channel.DisposeAsync();
            await channelClean.DisposeAsync();
        });
    }

    /// <summary>
    /// Regression (round 33): a publish RETRY — the same message id landing as an idempotent
    /// duplicate (<c>WHERE NOT EXISTS</c>, or the key-violation race) after another process had already claimed and
    /// acked the first attempt — reached the same-process fast path with a fabricated
    /// <c>AckedAtUtc = null</c>, which bypassed <c>IsWithinWatermark</c>'s acked-history exclusion
    /// and replayed the consumed response to a waiter registered AFTER the ack (the delivery claim
    /// gates only on <c>recovery_claimed</c>, so it won again). The store now returns the ORIGINAL
    /// row's settlement columns for a duplicate and the fast path dispatches that. Driven through
    /// the channel's own publish path with the acked message's id. Pre-fix: the late waiter
    /// completed with the retried response.
    /// </summary>
    [Fact]
    public async Task PublishRetry_OfAnAckedMessage_DoesNotReplayItToAWaiterRegisteredAfterTheAck()
    {
        await WithSchemaAsync("retry_acked", async schema =>
        {
            var options = ChannelOptions(schema);
            var sql = new SqlServerChannelSql(Options.Create(options));
            await using var channel = new SqlServerAsyncResponseChannel(
                new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
                sql,
                MockRecoveryStore(),
                Options.Create(options),
                new AsyncResponseContextPropagation([]),
                NullLogger<SqlServerAsyncResponseChannel>.Instance);
            await sql.EnsureCreatedAsync();

            // First attempt: persisted, then claimed and acked by "another process".
            var messageId = Guid.NewGuid();
            await sql.InsertMessageAsync(messageId, "corr", SuccessEnvelope("consumed"), TimeSpan.FromSeconds(30), CancellationToken.None);
            Assert.True(await sql.TryClaimForDeliveryAsync(messageId, CancellationToken.None));

            // A waiter registering AFTER that ack, stamped on the server clock the watermark
            // compares against — exactly what CreateResponseWaiter draws.
            var (startedAt, startedSeq) = await sql.GetSubscriptionStartAsync(CancellationToken.None);
            var late = Subscription(typeof(SqlServerAsyncResponseChannel), "DbSubscription`1", channel, _ => new ValueTask<bool>(true), startedAt, startedSeq);
            InheritedMethod(typeof(SqlServerAsyncResponseChannel), "AddSubscription").Invoke(channel, ["corr", late.Instance]);

            // The retry lands through the channel's publish path with the SAME message id.
            await (Task)InheritedMethod(typeof(SqlServerAsyncResponseChannel), "PublishMessageAsync")
                .Invoke(channel, [messageId, "corr", SuccessEnvelope("retry"), CancellationToken.None])!;
            await DrainLocalDispatchAsync(typeof(SqlServerAsyncResponseChannel), channel, "corr");

            Assert.False(late.Completion.Task.IsCompleted, "the consumed response was replayed to a waiter registered after its ack");

            // The original settlement is what the fast path saw: acked before the waiter existed,
            // and the row still carries exactly one ack.
            var stored = Assert.Single(await sql.LoadMessagesAsync("corr", startedAt.AddSeconds(-30), 10, null, null, CancellationToken.None));
            Assert.NotNull(stored.AckedAtUtc);
            Assert.True(stored.AckedAtUtc <= startedAt, $"acked_at {stored.AckedAtUtc:O} is not before the waiter's start {startedAt:O}");
        });
    }

    /// <summary>
    /// Regression (round 33), the store contract behind the fast-path fix: an idempotent duplicate
    /// insert returns the ORIGINAL row — its server-stamped <c>created_at</c> AND its settlement
    /// columns (<c>acked_at</c>, <c>acked_seq</c>) exactly as the sweep's <c>LoadMessagesAsync</c>
    /// reads them — so the same-process fast path compares against the watermark like the sweep
    /// does. Pre-fix the insert returned only <c>created_at</c> and the channel fabricated a null
    /// <c>acked_at</c> (this method did not compile: the return type is the fix).
    /// </summary>
    [Fact]
    public async Task InsertMessage_Duplicate_ReturnsTheOriginalRowsSettlementColumns()
    {
        await WithSchemaAsync("dup_settled", async schema =>
        {
            var sql = new SqlServerChannelSql(Options.Create(ChannelOptions(schema)));
            await sql.EnsureCreatedAsync();
            var since = await sql.GetServerTimeUtcAsync(CancellationToken.None);

            var messageId = Guid.NewGuid();
            var first = await sql.InsertMessageAsync(messageId, "settled", SuccessEnvelope("first"), TimeSpan.FromSeconds(30), CancellationToken.None);
            Assert.Null(first.AckedAtUtc);
            Assert.Null(first.AckedSeq);
            Assert.True(await sql.TryClaimForDeliveryAsync(messageId, CancellationToken.None));

            var duplicate = await sql.InsertMessageAsync(messageId, "settled", SuccessEnvelope("retry"), TimeSpan.FromSeconds(30), CancellationToken.None);

            var stored = Assert.Single(await sql.LoadMessagesAsync("settled", since.AddSeconds(-5), 10, null, null, CancellationToken.None));
            Assert.Equal(messageId, duplicate.Id);
            Assert.Equal(first.CreatedAtUtc, duplicate.CreatedAtUtc);
            Assert.NotNull(duplicate.AckedAtUtc);
            Assert.NotNull(duplicate.AckedSeq);
            Assert.Equal(stored.AckedAtUtc, duplicate.AckedAtUtc);
            Assert.Equal(stored.AckedSeq, duplicate.AckedSeq);
        });
    }

    /// <summary>
    /// A <c>DbSubscription</c> registered at an explicit server-clock watermark, for the
    /// registered-after-the-ack scenario; the no-argument overload above stamps the app clock.
    /// </summary>
    private static (object Instance, TaskCompletionSource<OperationResult> Completion) Subscription(
        Type channelType,
        string nestedTypeName,
        object channel,
        Func<OperationResult, ValueTask<bool>> predicate,
        DateTimeOffset startedAtUtc,
        long startedSeq)
    {
        var completion = new TaskCompletionSource<OperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var type = channelType.BaseType!.GetNestedType(nestedTypeName, BindingFlags.NonPublic)!
            .MakeGenericType(typeof(OperationResult));
        var instance = Activator.CreateInstance(
            type,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            [channel, "corr", Guid.NewGuid(), startedAtUtc, startedSeq, predicate, completion, null],
            culture: null)!;
        SetField(instance, "_cleanupStarted", 1);
        return (instance, completion);
    }

    /// <summary>Resolves a method declared anywhere up the channel's hierarchy (private base members are invisible to a derived-type lookup).</summary>
    private static MethodInfo InheritedMethod(Type channelType, string name)
    {
        for (var type = channelType; type is not null; type = type.BaseType)
        {
            var method = type.GetMethod(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            if (method is not null)
                return method;
        }

        throw new MissingMethodException(channelType.FullName, name);
    }

    /// <summary>
    /// Queues a marker behind whatever the same-process fast path enqueued on the correlation
    /// id's serial executor and waits for it, so an assertion about that dispatch's outcome never
    /// races the dispatch itself.
    /// </summary>
    private static async Task DrainLocalDispatchAsync(Type channelType, object channel, string correlationId)
    {
        var executors = (SerialExecutorRegistry)channelType.BaseType!
            .GetField("_executors", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(channel)!;
        var channelName = (string)InheritedMethod(channelType, "ChannelName").Invoke(channel, [correlationId])!;
        var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Assert.True(await executors.EnqueueAsync(channelName, () =>
        {
            drained.TrySetResult();
            return Task.CompletedTask;
        }));
        await drained.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    private static (object Instance, TaskCompletionSource<OperationResult> Completion) Subscription(
        Type channelType,
        string nestedTypeName,
        object channel,
        Func<OperationResult, ValueTask<bool>> predicate)
    {
        var completion = new TaskCompletionSource<OperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var type = channelType.BaseType!.GetNestedType(nestedTypeName, BindingFlags.NonPublic)!
            .MakeGenericType(typeof(OperationResult));
        var instance = Activator.CreateInstance(
            type,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            [channel, "corr", Guid.NewGuid(), DateTimeOffset.UtcNow, 0L, predicate, completion, null],
            culture: null)!;
        SetField(instance, "_cleanupStarted", 1);
        return (instance, completion);
    }

    private static void SetField(object target, string name, object value)
        => target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(target, value);

    private static IRecoveryStateStore MockRecoveryStore() => new FakeRecoveryStore();

    private sealed class FakeRecoveryStore : IRecoveryStateStore
    {
        public Task SaveAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default) => Task.FromResult<IReadOnlyList<RecoveryState>>([]);
        public Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default) => Task.FromResult(true);
    }

    private interface IDirectSqlServerRecoveryFlow
    {
        Task ResumeAsync(OperationResult payload, string correlationId);
        Task FailAsync(Exception exception, string correlationId);
    }

    private sealed class DirectRecoveryFlow : IDirectSqlServerRecoveryFlow
    {
        private readonly ConcurrentDictionary<string, TaskCompletionSource<OperationResult>> _resumes = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, TaskCompletionSource<Exception>> _failures = new(StringComparer.Ordinal);

        public Task<OperationResult> WaitResumeAsync(string correlationId)
            => _resumes.GetOrAdd(correlationId, _ => NewSource<OperationResult>()).Task;

        public Task<Exception> WaitFailureAsync(string correlationId)
            => _failures.GetOrAdd(correlationId, _ => NewSource<Exception>()).Task;

        public Task ResumeAsync(OperationResult payload, string correlationId)
        {
            _resumes.GetOrAdd(correlationId, _ => NewSource<OperationResult>()).TrySetResult(payload);
            return Task.CompletedTask;
        }

        public Task FailAsync(Exception exception, string correlationId)
        {
            _failures.GetOrAdd(correlationId, _ => NewSource<Exception>()).TrySetResult(exception);
            return Task.CompletedTask;
        }

        private static TaskCompletionSource<T> NewSource<T>()
            => new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class RecordingIngress : IAsyncResponseIngress
    {
        public TaskCompletionSource<string> WorkerReceived { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<(string Json, string? CorrelationId)> ResponseReceived { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task HandleResponseMessageAsync(string messageJson, string? correlationId = null)
        {
            ResponseReceived.TrySetResult((messageJson, correlationId));
            return Task.CompletedTask;
        }

        public Task HandleWorkerMessageAsync(string messageJson)
        {
            WorkerReceived.TrySetResult(messageJson);
            return Task.CompletedTask;
        }
    }

    private sealed class DefaultRecoveryPayload : IAsyncResponsePayload
    {
    }

    private sealed class EnabledLogger<T> : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
        }
    }
}
