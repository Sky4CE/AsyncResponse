using System.Reflection;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.DurableFlows.PostgreSQL;
using AsyncResponse.Sample;
using AsyncResponse.Transports.PostgreSQL;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Npgsql;
using System.Collections.Concurrent;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

[Collection(DataCollection.Name)]
[Trait(Batches.Trait, Batches.Data)]
public sealed class PostgreSqlDirectIntegrationTests(DataBatchFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task ChannelListen_IsEstablishedOnceItsDeliveryProbeComesBack()
    {
        // Fixpoint r2 S5#6: a LISTEN counts as established only once a NOTIFY the listen
        // connection sends itself has come back through it (behind a transaction-mode pooler none
        // ever does). The unit tests drive that against a fake server; this pins that a real
        // PostgreSQL delivers a listening session its own notification, so the channel's push wake
        // (and the full-sweep throttle it earns) is not silently lost everywhere.
        await WithDataSourceAsync("listen_probe", async (schema, dataSource) =>
        {
            var sql = new PostgreSqlChannelSql(dataSource, Options.Create(ChannelOptions(schema)));
            await sql.EnsureCreatedAsync();
            var established = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var cts = new CancellationTokenSource();
            var listen = Task.Run(() => sql.ExecuteListenAsync(_ => Task.CompletedTask, cts.Token, () => established.TrySetResult()));
            try
            {
                await Task.WhenAny(established.Task, listen).WaitAsync(TimeSpan.FromSeconds(30));
                Assert.True(established.Task.IsCompleted, $"the delivery probe did not come back: {listen.Exception?.GetBaseException().Message}");
            }
            finally
            {
                await cts.CancelAsync();
                await IgnoreCancellationAsync(listen);
            }
        });
    }

    [Fact]
    public async Task MaximumLengthMessageTableName_CreatesADistinctSequence_AndDrawsFromIt()
    {
        // A 63-character table name used to collide with its own generated sequence name
        // (whole-name truncation): CREATE SEQUENCE IF NOT EXISTS silently matched the TABLE,
        // managed-schema validation was fooled by to_regclass, and the first nextval failed.
        await WithDataSourceAsync("max_len_table", async (schema, dataSource) =>
        {
            var options = ChannelOptions(schema);
            options.MessageTable = new string('m', 63);
            var sql = new PostgreSqlChannelSql(dataSource, Options.Create(options));
            await sql.EnsureCreatedAsync();

            var (_, startSeq) = await sql.GetSubscriptionStartAsync(CancellationToken.None);
            Assert.True(startSeq > 0);

            // Every derived index must actually exist in the catalog. Whole-name truncation used
            // to make both messages-table index names equal the table's own name, so CREATE INDEX
            // IF NOT EXISTS matched the table relation and created ZERO indexes — invisible to
            // any test that only drew from the sequence.
            var indexNames = new List<string>();
            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var indexes = connection.CreateCommand())
            {
                indexes.CommandText = "SELECT indexname FROM pg_indexes WHERE schemaname = @schema;";
                indexes.Parameters.AddWithValue("schema", schema);
                await using var reader = await indexes.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                    indexNames.Add(reader.GetString(0));
            }

            // Suffix space is reserved by truncating the table STEM, so at the 63-character cap
            // the two messages-table indexes stay distinct from each other and from the table.
            Assert.Contains(new string('m', 51) + "_expires_idx", indexNames);
            Assert.Contains(new string('m', 39) + "_correlation_created_idx", indexNames);
            Assert.Contains("asyncresponse_recovery_state_expires_idx", indexNames);
            Assert.Contains("asyncresponse_channel_subscribers_expires_idx", indexNames);

            // The relkind-precise managed-schema validation passes against the real sequence.
            var managedOptions = ChannelOptions(schema);
            managedOptions.MessageTable = options.MessageTable;
            managedOptions.AutoCreateSchema = false;
            var managed = new PostgreSqlChannelSql(dataSource, Options.Create(managedOptions));
            var (_, managedSeq) = await managed.GetSubscriptionStartAsync(CancellationToken.None);
            Assert.True(managedSeq > startSeq);
        });
    }

    [Fact]
    public async Task SharedSchema_CrossComponentNameCollisions_FailActionablyInsteadOfSilentlySkippingDdl()
    {
        // Per-component ValidateNamePlan cannot see the OTHER packages sharing a schema: the
        // channel's recovery table below occupies the transport's derived dequeue-index name.
        // Tables, indexes, and sequences share one relation namespace, and CREATE ... IF NOT
        // EXISTS matches ANY relation — previously the loser silently skipped its DDL (a missing
        // dequeue index, or a channel "table" that is actually someone else's index). The post-DDL
        // catalog verification must fail whichever component starts second, in both orders.
        await WithDataSourceAsync("cross_collide_a", async (schema, dataSource) =>
        {
            var channelOptions = ChannelOptions(schema);
            channelOptions.RecoveryStateTable = "jobs_ready_idx";
            var channel = new PostgreSqlChannelSql(dataSource, Options.Create(channelOptions));
            await channel.EnsureCreatedAsync();

            var transportOptions = TransportOptions(schema);
            transportOptions.MessageTable = "jobs";
            var transport = new PostgreSqlTransportStore(dataSource, Options.Create(transportOptions));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => transport.EnsureCreatedAsync());
            Assert.Contains("jobs_ready_idx", ex.Message, StringComparison.Ordinal);
            Assert.Contains("occupied by a table", ex.Message, StringComparison.Ordinal);
        });

        await WithDataSourceAsync("cross_collide_b", async (schema, dataSource) =>
        {
            var transportOptions = TransportOptions(schema);
            transportOptions.MessageTable = "jobs";
            var transport = new PostgreSqlTransportStore(dataSource, Options.Create(transportOptions));
            await transport.EnsureCreatedAsync();

            var channelOptions = ChannelOptions(schema);
            channelOptions.RecoveryStateTable = "jobs_ready_idx";
            var channel = new PostgreSqlChannelSql(dataSource, Options.Create(channelOptions));
            // In this direction the collision breaks the DDL batch itself (CREATE INDEX ... ON a
            // relation that is really the transport's index → wrong object type), which the store
            // wraps with the same namespace-collision guidance.
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => channel.EnsureCreatedAsync());
            Assert.Contains("share one namespace", ex.Message, StringComparison.Ordinal);
            Assert.IsType<PostgresException>(ex.InnerException);
        });
    }

    [Fact]
    public async Task PreexistingObjectsWithWrongDefinitions_FailVerificationActionably()
    {
        // CREATE INDEX IF NOT EXISTS accepts ANY existing index with the name and guarantees
        // nothing about its shape: a same-name index over the WRONG columns silently starved the
        // claim query of its dequeue index. The verifier must compare definitions, not names.
        await WithDataSourceAsync("wrong_index_def", async (schema, dataSource) =>
        {
            var transportOptions = TransportOptions(schema);
            transportOptions.MessageTable = "jobs";
            var creator = new PostgreSqlTransportStore(dataSource, Options.Create(transportOptions));
            await creator.EnsureCreatedAsync();

            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var reshape = connection.CreateCommand())
            {
                reshape.CommandText =
                    $"""
                    DROP INDEX "{schema}"."jobs_ready_idx";
                    CREATE INDEX "jobs_ready_idx" ON "{schema}"."jobs" (created_at);
                    """;
                await reshape.ExecuteNonQueryAsync();
            }

            var second = new PostgreSqlTransportStore(dataSource, Options.Create(transportOptions));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => second.EnsureCreatedAsync());
            Assert.Contains("does not match the expected definition", ex.Message, StringComparison.Ordinal);
            Assert.Contains("queue, available_at, created_at", ex.Message, StringComparison.Ordinal);
        });

        // Same family for sequences: an existing integer sequence would overflow at 2^31 draws;
        // CREATE SEQUENCE IF NOT EXISTS accepts it silently.
        await WithDataSourceAsync("wrong_seq_type", async (schema, dataSource) =>
        {
            var channelOptions = ChannelOptions(schema);
            var creator = new PostgreSqlChannelSql(dataSource, Options.Create(channelOptions));
            await creator.EnsureCreatedAsync();

            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var reshape = connection.CreateCommand())
            {
                reshape.CommandText =
                    $"""
                    DROP SEQUENCE {creator.AckSequence};
                    CREATE SEQUENCE {creator.AckSequence} AS integer;
                    """;
                await reshape.ExecuteNonQueryAsync();
            }

            var second = new PostgreSqlChannelSql(dataSource, Options.Create(ChannelOptions(schema)));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => second.EnsureCreatedAsync());
            Assert.Contains("expected bigint", ex.Message, StringComparison.Ordinal);
        });

        // A bigint sequence is still not necessarily a monotonic cross-process clock: a
        // descending increment counts down, CYCLE wraps, and CACHE > 1 hands sessions private
        // blocks. All were accepted before; the property check pins them.
        await WithDataSourceAsync("wrong_seq_props", async (schema, dataSource) =>
        {
            var channelOptions = ChannelOptions(schema);
            var creator = new PostgreSqlChannelSql(dataSource, Options.Create(channelOptions));
            await creator.EnsureCreatedAsync();

            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var reshape = connection.CreateCommand())
            {
                reshape.CommandText =
                    $"""
                    DROP SEQUENCE {creator.AckSequence};
                    CREATE SEQUENCE {creator.AckSequence} AS bigint INCREMENT -1 START -1;
                    """;
                await reshape.ExecuteNonQueryAsync();
            }

            var second = new PostgreSqlChannelSql(dataSource, Options.Create(ChannelOptions(schema)));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => second.EnsureCreatedAsync());
            Assert.Contains("INCREMENT -1", ex.Message, StringComparison.Ordinal);
        });

        // A crafted same-kind table that satisfies the index DDL (indexed columns present) but
        // lacks operational columns previously passed verification and failed at first insert.
        await WithDataSourceAsync("crafted_table", async (schema, dataSource) =>
        {
            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var craft = connection.CreateCommand())
            {
                craft.CommandText =
                    $"""
                    CREATE SCHEMA IF NOT EXISTS "{schema}";
                    CREATE TABLE "{schema}"."jobs" (
                        id uuid PRIMARY KEY,
                        queue text NOT NULL,
                        available_at timestamptz NOT NULL DEFAULT now(),
                        locked_until timestamptz NULL,
                        created_at timestamptz NOT NULL DEFAULT now()
                    );
                    """;
                await craft.ExecuteNonQueryAsync();
            }

            var transportOptions = TransportOptions(schema);
            transportOptions.MessageTable = "jobs";
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(transportOptions));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync());
            Assert.Contains("missing the column 'payload_json'", ex.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task SharedSchema_SameKindTableCollisions_FailActionablyInBothOrders()
    {
        // A channel recovery table occupying the transport's table name is the same RELATION
        // KIND, so the kind check alone accepted it; the dependent index DDL then failed with a
        // raw "column does not exist". Both directions must produce the actionable collision
        // error instead.
        await WithDataSourceAsync("same_kind_a", async (schema, dataSource) =>
        {
            var channelOptions = ChannelOptions(schema);
            var channel = new PostgreSqlChannelSql(dataSource, Options.Create(channelOptions));
            await channel.EnsureCreatedAsync();

            var transportOptions = TransportOptions(schema);
            transportOptions.MessageTable = channelOptions.RecoveryStateTable;
            var transport = new PostgreSqlTransportStore(dataSource, Options.Create(transportOptions));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => transport.EnsureCreatedAsync());
            Assert.Contains("occupied by an object of a different kind or shape", ex.Message, StringComparison.Ordinal);
        });

        await WithDataSourceAsync("same_kind_b", async (schema, dataSource) =>
        {
            var transportOptions = TransportOptions(schema);
            transportOptions.MessageTable = "jobs";
            var transport = new PostgreSqlTransportStore(dataSource, Options.Create(transportOptions));
            await transport.EnsureCreatedAsync();

            var channelOptions = ChannelOptions(schema);
            channelOptions.RecoveryStateTable = "jobs";
            var channel = new PostgreSqlChannelSql(dataSource, Options.Create(channelOptions));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => channel.EnsureCreatedAsync());
            Assert.Contains("occupied by an object of a different kind or shape", ex.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ExtraSelfPopulatingColumns_PassVerification_AndInsertsStillSucceed()
    {
        // Identity columns carry no pg_attrdef row, so the extra-column writability check used to
        // read GENERATED ALWAYS AS IDENTITY as "NOT NULL without a default" and fail startup —
        // for a column PostgreSQL populates on every insert. Stored generated columns are the
        // same self-populating class.
        await WithDataSourceAsync("extra_identity", async (schema, dataSource) =>
        {
            var transportOptions = TransportOptions(schema);
            transportOptions.MessageTable = "jobs";
            await new PostgreSqlTransportStore(dataSource, Options.Create(transportOptions)).EnsureCreatedAsync();

            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var reshape = connection.CreateCommand())
            {
                reshape.CommandText =
                    $"""
                    ALTER TABLE "{schema}"."jobs" ADD COLUMN audit_seq bigint GENERATED ALWAYS AS IDENTITY;
                    ALTER TABLE "{schema}"."jobs" ADD COLUMN attempts_snapshot integer GENERATED ALWAYS AS (attempts) STORED NOT NULL;
                    """;
                await reshape.ExecuteNonQueryAsync();
            }

            var store = new PostgreSqlTransportStore(dataSource, Options.Create(transportOptions));
            await store.EnsureCreatedAsync();

            // The columns really are self-populating: a publish that names neither succeeds.
            await store.PublishAsync(Guid.NewGuid(), transportOptions.WorkerQueue, SuccessEnvelope("extra"), headers: null, CancellationToken.None);
        });
    }

    [Fact]
    public async Task CoveringPrimaryKey_WithIncludePayloadColumns_PassesVerification()
    {
        // PostgreSQL 11+ covering keys: PRIMARY KEY (id) INCLUDE (queue) enforces exactly the
        // uniqueness the store relies on, with the INCLUDE columns riding along in the index
        // without being key columns. The verifier used to read the WHOLE indkey — key and
        // payload alike — and reject the table with "primary key is (id, queue) instead of (id)".
        await WithDataSourceAsync("covering_pk", async (schema, dataSource) =>
        {
            var transportOptions = TransportOptions(schema);
            transportOptions.MessageTable = "jobs";
            await new PostgreSqlTransportStore(dataSource, Options.Create(transportOptions)).EnsureCreatedAsync();

            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var reshape = connection.CreateCommand())
            {
                reshape.CommandText =
                    $"""
                    ALTER TABLE "{schema}"."jobs" DROP CONSTRAINT "jobs_pkey";
                    ALTER TABLE "{schema}"."jobs" ADD PRIMARY KEY (id) INCLUDE (queue);
                    """;
                await reshape.ExecuteNonQueryAsync();
            }

            await new PostgreSqlTransportStore(dataSource, Options.Create(transportOptions)).EnsureCreatedAsync();
        });
    }

    [Fact]
    public async Task OperatorProvisionedFlowSchema_WrongColumnShape_IsReported_AndAMissingIndexIsNotAnError()
    {
        // A misprovisioned schema is usually wrong in several ways at once. The verifier used to
        // report the index the operator forgot ("does not exist") while the wrong column type on
        // the table they DID provide — the actionable finding — went unreported. The wrong shape
        // seeded here is state_json jsonb — the pre-round-29 column type, i.e. exactly what a
        // manually-migrated deployment presents after upgrading (the expected type is now text;
        // see the store DDL). The expiry index itself is performance-only: on an operator-managed
        // schema it is verified when present and only warned about when absent, so once the column
        // is fixed the store runs — and a revision column without the DDL's DEFAULT 0 is accepted
        // too, since every insert names it.
        await WithDataSourceAsync("flow_cause_order", async (schema, dataSource) =>
        {
            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var provision = connection.CreateCommand())
            {
                provision.CommandText =
                    $"""
                    CREATE SCHEMA IF NOT EXISTS "{schema}";
                    CREATE TABLE "{schema}"."flow_state" (
                        flow_id text NOT NULL PRIMARY KEY,
                        state_json jsonb NOT NULL,
                        expires_at_utc timestamptz NOT NULL,
                        updated_at_utc timestamptz NOT NULL,
                        revision bigint NOT NULL,
                        lease_id text NULL,
                        lease_expires_at_utc timestamptz NULL
                    );
                    """;
                await provision.ExecuteNonQueryAsync();
            }

            var store = new PostgreSqlFlowStateStore(
                dataSource,
                Options.Create(new PostgreSqlDurableFlowOptions
                {
                    SchemaName = schema,
                    TableName = "flow_state",
                    AutoCreateSchema = false
                }));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.LoadAsync("missing"));
            Assert.Contains("column 'state_json'", ex.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("to exist after schema creation", ex.Message, StringComparison.Ordinal);

            // With the shape fixed, the forgotten index no longer fails every operation.
            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var repair = connection.CreateCommand())
            {
                repair.CommandText =
                    $"""ALTER TABLE "{schema}"."flow_state" ALTER COLUMN state_json TYPE text USING state_json::text;""";
                await repair.ExecuteNonQueryAsync();
            }

            var repaired = new PostgreSqlFlowStateStore(
                dataSource,
                Options.Create(new PostgreSqlDurableFlowOptions
                {
                    SchemaName = schema,
                    TableName = "flow_state",
                    AutoCreateSchema = false
                }));
            Assert.Null(await repaired.LoadAsync("missing"));
            Assert.True(await repaired.TryCreateAsync("flow-a", FlowStoreContract.CreateState("flow-a"), TimeSpan.FromMinutes(5)));

            // Present but mis-shaped, the index is still refused: optional covers absence only.
            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var misshape = connection.CreateCommand())
            {
                misshape.CommandText = $"""CREATE INDEX "flow_state_expires_idx" ON "{schema}"."flow_state" (updated_at_utc);""";
                await misshape.ExecuteNonQueryAsync();
            }

            var misshapen = new PostgreSqlFlowStateStore(
                dataSource,
                Options.Create(new PostgreSqlDurableFlowOptions
                {
                    SchemaName = schema,
                    TableName = "flow_state",
                    AutoCreateSchema = false
                }));
            var wrongIndex = await Assert.ThrowsAsync<InvalidOperationException>(() => misshapen.LoadAsync("missing"));
            Assert.Contains("flow_state_expires_idx", wrongIndex.Message, StringComparison.Ordinal);
            Assert.Contains("does not match the expected definition", wrongIndex.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ManagedTransportSchema_VerifiesProvisionedTables_AndDefersWhileAbsent()
    {
        // AutoCreateSchema=false used to return before ANY network call: an operator-provisioned
        // queue table's shape was never checked — the transport ran the full verifier only on the
        // DDL path, i.e. only where it was least needed. Managed mode now runs the same catalog
        // verification with the flow stores' semantics: absent table = silent, re-check later
        // (never latch); present = verify (wrong shape fails actionably) and latch.
        await WithDataSourceAsync("managed_ok", async (schema, dataSource) =>
        {
            var managedOptions = TransportOptions(schema);
            managedOptions.AutoCreateSchema = false;
            var managed = new PostgreSqlTransportStore(dataSource, Options.Create(managedOptions));

            await managed.EnsureCreatedAsync();
            Assert.False(CreatedLatch(managed));

            // Provision through the DDL path (the exact expected shape, indexes included); the
            // managed store then verifies against the catalog and latches.
            await new PostgreSqlTransportStore(dataSource, Options.Create(TransportOptions(schema))).EnsureCreatedAsync();
            await managed.EnsureCreatedAsync();
            Assert.True(CreatedLatch(managed));
        });

        await WithDataSourceAsync("managed_wrong", async (schema, dataSource) =>
        {
            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var craft = connection.CreateCommand())
            {
                craft.CommandText =
                    $"""
                    CREATE SCHEMA IF NOT EXISTS "{schema}";
                    CREATE TABLE "{schema}"."jobs" (
                        id uuid PRIMARY KEY,
                        queue text NOT NULL,
                        available_at timestamptz NOT NULL DEFAULT now(),
                        locked_until timestamptz NULL,
                        created_at timestamptz NOT NULL DEFAULT now()
                    );
                    """;
                await craft.ExecuteNonQueryAsync();
            }

            var options = TransportOptions(schema);
            options.AutoCreateSchema = false;
            options.MessageTable = "jobs";
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync());
            Assert.Contains("missing the column 'payload_json'", ex.Message, StringComparison.Ordinal);
            Assert.False(CreatedLatch(store));
        });
    }

    private static bool CreatedLatch(object store)
        => (bool)store.GetType().GetField("_created", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(store)!;

    [Fact]
    public async Task NonDeterministicCollationOnAnIdentityColumn_IsRejected()
    {
        // PostgreSQL's own DDL emits no COLLATE, so this needs an operator-provisioned table — but
        // a non-deterministic ICU collation is exactly what the SQL Server sibling has always
        // rejected on the same columns: the database folds strings its rules call equal into ONE
        // key, so two distinct queue names collide and the second is rejected on insert. Nothing
        // downstream re-checks the queue column's storage, and the verifier checked no collation.
        await WithDataSourceAsync("nondet_coll", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            options.MessageTable = "jobs";
            options.AutoCreateSchema = false;

            var claimIndex = PostgreSqlTransportStore.IndexName("jobs", "claim");
            var createdIndex = PostgreSqlTransportStore.IndexName("jobs", "created");
            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var craft = connection.CreateCommand())
            {
                craft.CommandText =
                    $"""
                    CREATE SCHEMA IF NOT EXISTS "{schema}";
                    CREATE COLLATION "{schema}".case_insensitive (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
                    CREATE TABLE "{schema}"."jobs" (
                        id uuid PRIMARY KEY,
                        queue text COLLATE "{schema}".case_insensitive NOT NULL,
                        payload_json text NOT NULL,
                        headers_json text NOT NULL DEFAULT '{EmptyJson}',
                        created_at timestamptz NOT NULL DEFAULT now(),
                        available_at timestamptz NOT NULL DEFAULT now(),
                        locked_until timestamptz NULL,
                        lock_id uuid NULL,
                        attempts integer NOT NULL DEFAULT 0,
                        dead_letter_reason text NULL
                    );
                    CREATE INDEX "{claimIndex}" ON "{schema}"."jobs" (queue, available_at, locked_until, created_at);
                    CREATE INDEX "{createdIndex}" ON "{schema}"."jobs" (created_at);
                    """;
                await craft.ExecuteNonQueryAsync();
            }

            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync());
            Assert.Contains("jobs.queue", ex.Message, StringComparison.Ordinal);
            Assert.Contains("case_insensitive", ex.Message, StringComparison.Ordinal);
            Assert.Contains("non-deterministic", ex.Message, StringComparison.Ordinal);
            Assert.False(CreatedLatch(store));
        });

        await WithDataSourceAsync("default_coll", async (schema, dataSource) =>
        {
            // The default collation reports collisdeterministic = true, and uuid/jsonb columns
            // carry no collation at all (attcollation 0, no joined row) — both must pass.
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(TransportOptions(schema)));
            await store.EnsureCreatedAsync();
            Assert.True(CreatedLatch(store));
        });
    }

    [Fact]
    public async Task ManagedSchemaValidation_NonDeterministicCorrelationCollation_IsRejectedAtStartup()
    {
        // Regression (round 31): AutoCreateSchema = false ran only the acked_seq migration probe
        // and skipped full relation verification, so an operator-provisioned correlation_id with
        // a non-deterministic collation was accepted at startup — and then folded distinct
        // ordinal ids into one key at runtime, cross-routing responses silently. The managed path
        // now verifies relations exactly like the DDL path, the transport and the flow store.
        await WithDataSourceAsync("managed_collation", async (schema, dataSource) =>
        {
            var creator = new PostgreSqlChannelSql(dataSource, Options.Create(ChannelOptions(schema)));
            await creator.EnsureCreatedAsync();
            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var degrade = connection.CreateCommand())
            {
                // The legacy shape an operator-managed migration can produce: an ICU
                // case-folding collation on the identity column. ALTER TYPE rebuilds the
                // dependent index, so only the column's collation diverges from the contract.
                degrade.CommandText =
                    $"""
                    CREATE COLLATION "{schema}".folding (provider = icu, locale = 'und-u-ks-level2', deterministic = false);
                    ALTER TABLE {creator.MessageTable} ALTER COLUMN correlation_id TYPE text COLLATE "{schema}".folding;
                    """;
                await degrade.ExecuteNonQueryAsync();
            }

            var managedOptions = ChannelOptions(schema);
            managedOptions.AutoCreateSchema = false;
            var managed = new PostgreSqlChannelSql(dataSource, Options.Create(managedOptions));

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => managed.GetSubscriptionStartAsync(CancellationToken.None));
            Assert.Contains("correlation_id", ex.Message, StringComparison.Ordinal);
            Assert.Contains("non-deterministic", ex.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ManagedSchemaValidation_MissingAckSequenceObjects_FailsActionably_AndPassesAfterTheDocumentedMigration()
    {
        // A pre-1.0 manually managed schema (AutoCreateSchema = false) lacks acked_seq and its
        // sequence, which registration and delivery claims now require unconditionally. The
        // channel must fail at first use with an error carrying the exact migration — not a raw
        // "column does not exist" mid-operation — and work immediately once the documented
        // migration has been applied.
        await WithDataSourceAsync("managed_upgrade", async (schema, dataSource) =>
        {
            var creator = new PostgreSqlChannelSql(dataSource, Options.Create(ChannelOptions(schema)));
            await creator.EnsureCreatedAsync();
            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var strip = connection.CreateCommand())
            {
                strip.CommandText =
                    $"""
                    ALTER TABLE {creator.MessageTable} DROP COLUMN acked_seq;
                    DROP SEQUENCE {creator.AckSequence};
                    """;
                await strip.ExecuteNonQueryAsync();
            }

            var managedOptions = ChannelOptions(schema);
            managedOptions.AutoCreateSchema = false;
            var managed = new PostgreSqlChannelSql(dataSource, Options.Create(managedOptions));

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => managed.GetSubscriptionStartAsync(CancellationToken.None));
            Assert.Contains("acked_seq", ex.Message, StringComparison.Ordinal);
            Assert.Contains("docs/postgresql.md", ex.Message, StringComparison.Ordinal);

            // The exact migration from docs/postgresql.md, "Upgrading a manually managed schema".
            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var migrate = connection.CreateCommand())
            {
                migrate.CommandText =
                    $"""
                    ALTER TABLE {creator.MessageTable} ADD COLUMN IF NOT EXISTS acked_seq bigint NULL;
                    CREATE SEQUENCE IF NOT EXISTS {creator.AckSequence} AS bigint;
                    """;
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
            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var legacyAck = connection.CreateCommand())
            {
                legacyAck.CommandText = $"UPDATE {creator.MessageTable} SET acked_at = now() WHERE id = @id;";
                legacyAck.Parameters.AddWithValue("id", legacyId);
                await legacyAck.ExecuteNonQueryAsync();
            }

            Assert.True(await managed.TryClaimForDeliveryAsync(legacyId, CancellationToken.None));
            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var check = connection.CreateCommand())
            {
                check.CommandText = $"SELECT acked_seq IS NULL FROM {creator.MessageTable} WHERE id = @id;";
                check.Parameters.AddWithValue("id", legacyId);
                Assert.True((bool)(await check.ExecuteScalarAsync())!);
            }

            // A fresh (unacked) row still gets its stamp on the first claim.
            var freshId = Guid.NewGuid();
            await managed.InsertMessageAsync(freshId, "fresh-ack", SuccessEnvelope("new"), TimeSpan.FromSeconds(30), CancellationToken.None);
            Assert.True(await managed.TryClaimForDeliveryAsync(freshId, CancellationToken.None));
            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var check = connection.CreateCommand())
            {
                check.CommandText = $"SELECT acked_seq IS NOT NULL FROM {creator.MessageTable} WHERE id = @id;";
                check.Parameters.AddWithValue("id", freshId);
                Assert.True((bool)(await check.ExecuteScalarAsync())!);
            }
        });
    }

    [Fact]
    public async Task InsertMessage_LosingAnUncommittedConflict_ResolvesAfterCommitViaFreshRead()
    {
        // Deterministically forces the stale-snapshot NULL path the fresh-statement re-read
        // exists for: the winner's insert is held UNCOMMITTED, so the competing statement blocks
        // on the speculative conflict; on commit, ON CONFLICT suppresses the competing insert
        // while its same-statement fallback still reads the pre-commit snapshot — only the
        // fresh-statement re-read can resolve the timestamp. The previous implementation threw.
        await WithDataSourceAsync("conflict_commit", async (schema, dataSource) =>
        {
            var options = ChannelOptions(schema);
            var sql = new PostgreSqlChannelSql(dataSource, Options.Create(options));
            await sql.EnsureCreatedAsync();

            var id = Guid.NewGuid();
            await using var winnerConnection = await dataSource.OpenConnectionAsync();
            await using var winnerTransaction = await winnerConnection.BeginTransactionAsync();
            await using var winnerInsert = winnerConnection.CreateCommand();
            winnerInsert.Transaction = winnerTransaction;
            winnerInsert.CommandText =
                $"""
                INSERT INTO {sql.MessageTable} (id, correlation_id, envelope_json, expires_at)
                VALUES (@id, @correlation_id, @envelope_json, now() + interval '30 seconds')
                RETURNING created_at;
                """;
            winnerInsert.Parameters.AddWithValue("id", id);
            winnerInsert.Parameters.AddWithValue("correlation_id", "conflict-corr");
            winnerInsert.Parameters.Add("envelope_json", NpgsqlTypes.NpgsqlDbType.Jsonb).Value = SuccessEnvelope("winner");
            var winnerStamp = Assert.IsType<DateTime>(await winnerInsert.ExecuteScalarAsync());

            await using var winnerPidQuery = winnerConnection.CreateCommand();
            winnerPidQuery.Transaction = winnerTransaction;
            winnerPidQuery.CommandText = "SELECT pg_backend_pid();";
            var winnerPid = Assert.IsType<int>(await winnerPidQuery.ExecuteScalarAsync());

            var competing = sql.InsertMessageAsync(
                id, "conflict-corr", SuccessEnvelope("loser"), TimeSpan.FromSeconds(30), CancellationToken.None);

            // Server-observed block barrier: commit only after PostgreSQL itself reports a backend
            // blocked on the winner's transaction. A timing sleep could elapse before the competing
            // statement even started — and then the pre-fix implementation would pass too.
            await using (var monitor = await dataSource.OpenConnectionAsync())
            {
                await using var blocked = monitor.CreateCommand();
                blocked.CommandText = "SELECT count(*) FROM pg_stat_activity WHERE @winner_pid = ANY (pg_blocking_pids(pid));";
                blocked.Parameters.AddWithValue("winner_pid", winnerPid);
                var deadline = DateTime.UtcNow.AddSeconds(15);
                while (Assert.IsType<long>(await blocked.ExecuteScalarAsync()) == 0)
                {
                    Assert.True(DateTime.UtcNow < deadline, "the competing insert never blocked on the winner's transaction");
                    await Task.Delay(25);
                }
            }

            Assert.False(competing.IsCompleted); // provably parked on the winner's uncommitted conflict

            await winnerTransaction.CommitAsync();
            var resolved = await competing.WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(new DateTimeOffset(winnerStamp, TimeSpan.Zero), resolved.CreatedAtUtc);
        });
    }

    [Fact]
    public async Task ChannelSql_RoundTripsRecoverySubscribersMessagesClaimsAndListen()
    {
        await WithDataSourceAsync("channel_sql", async (schema, dataSource) =>
        {
            var options = ChannelOptions(schema);
            var sql = new PostgreSqlChannelSql(dataSource, Options.Create(options));
            var store = new PostgreSqlRecoveryStateStore(sql, NullLogger<PostgreSqlRecoveryStateStore>.Instance);
            await sql.EnsureCreatedAsync();

            Assert.Equal(Quote(schema), sql.Schema);
            Assert.Contains(Quote(options.MessageTable), sql.MessageTable, StringComparison.Ordinal);
            Assert.Equal(options.NotificationChannel, sql.NotificationChannel);
            Assert.Equal(
                PostgreSqlTransportStore.SchemaAdvisoryLockKey(schema),
                PostgreSqlChannelSql.SchemaAdvisoryLockKey(schema));

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

            await InsertUnreadableRecoveryStateAsync(dataSource, schema, "bad-state");
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
            var paged = new List<PostgreSqlChannelMessage>();
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

            await AssertListenReceivesAsync(dataSource, options.NotificationChannel, (payload, token) =>
                sql.ExecuteListenAsync(payload, token));
        });
    }

    [Fact]
    public async Task Channel_DeliversLiveResponsesAndLostSubscriberCallbacks()
    {
        var schema = NewSchema("channel");
        await using var cleanupDataSource = NpgsqlDataSource.Create(Fixture.PostgreSqlConnectionString);
        ServiceProvider? provider = null;
        try
        {
            provider = BuildProvider(schema);
            var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
            var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
            var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
            var recoverable = provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>();
            var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
            var channel = provider.GetRequiredService<PostgreSqlAsyncResponseChannel>();
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
            await DropSchemaAsync(cleanupDataSource, schema);
        }
    }

    [Fact]
    public async Task Channel_DeliversLocalResponsesWithoutWaitingForListenerBacklog()
    {
        var schema = NewSchema("fast");
        await using var cleanupDataSource = NpgsqlDataSource.Create(Fixture.PostgreSqlConnectionString);
        ServiceProvider? provider = null;
        var waiters = new List<IAsyncResponseWaiter<OperationResult>>();
        try
        {
            // Local (same-process) responses must reach their waiters through the in-process fast path,
            // never the LISTEN/NOTIFY poll. The poll interval (30s) is kept well above the assertion
            // window (20s) so a pass can only mean in-process delivery beat the listener — if the fast
            // path regressed, the 30s poll could not deliver within the window and the test would fail.
            // DeliveryConfirmationTimeout is generous (5s) on purpose: it is the budget after which the
            // publisher gives a response up to recovery, so it must comfortably exceed a claim round-trip
            // under a loaded CI database. A too-tight value (the previous 20ms is below a CI round-trip)
            // makes the publisher steal a live-but-slow local delivery to the recovery path, starving the
            // waiter — which is exactly what flaked here under the heavier integration fixture.
            provider = BuildProvider(schema, options =>
            {
                options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5);
                options.ListenerPollInterval = TimeSpan.FromSeconds(30);
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
            await DropSchemaAsync(cleanupDataSource, schema);
        }
    }

    [Fact]
    public async Task Channel_RegressionEdges_HandleFallbacksFaultedEnvelopesAndSetupFailures()
    {
        var schema = NewSchema("channel_edges");
        await using var cleanupDataSource = NpgsqlDataSource.Create(Fixture.PostgreSqlConnectionString);
        ServiceProvider? provider = null;
        try
        {
            provider = BuildProvider(schema);
            var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
            var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
            var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
            var recoverableStore = provider.GetRequiredService<IRecoveryStateStore>();
            var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
            var sql = provider.GetRequiredService<PostgreSqlChannelSql>();
            var channel = provider.GetRequiredService<PostgreSqlAsyncResponseChannel>();
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
            await DropSchemaAsync(cleanupDataSource, schema);
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
    public async Task TransportStore_DeadLetterOnAStaleClaim_NoOpsInsteadOfBuryingALiveMessage()
    {
        // Regression (round 29): the DLQ row was written unconditionally and the fenced delete's
        // result ignored, so a claim whose lease had lapsed (a peer re-claimed the row) still
        // buried a full copy of a message that is still live and may yet succeed under its new
        // owner — the DLQ showed a poison entry for work that completed, and an operator replaying
        // it duplicated its side effects. The fenced ack and NAK already no-op in that window.
        await WithDataSourceAsync("dlqfence", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options));
            await store.EnsureCreatedAsync();

            var id = Guid.NewGuid();
            await store.PublishAsync(id, options.WorkerQueue, """{"kind":"fenced"}""", null, CancellationToken.None);
            var claimed = (await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None))!;

            // The peer takeover: a different lock_id now owns the row, so this claim's fence is dead.
            await using (var steal = dataSource.CreateCommand(
                $"UPDATE {store.MessageTable} SET lock_id = @peer, locked_until = now() + interval '5 minutes' WHERE id = @id;"))
            {
                steal.Parameters.AddWithValue("@peer", Guid.NewGuid());
                steal.Parameters.AddWithValue("@id", id);
                await steal.ExecuteNonQueryAsync();
            }

            Assert.False(await claimed.DeadLetterAsync(new InvalidOperationException("stale"), true, CancellationToken.None));

            // No DLQ copy was written...
            await using (var count = dataSource.CreateCommand(
                $"SELECT count(*) FROM {store.MessageTable} WHERE queue = @queue;"))
            {
                count.Parameters.AddWithValue("@queue", options.DeadLetterQueue);
                Assert.Equal(0L, (long)(await count.ExecuteScalarAsync())!);
            }

            // ...and the live row is untouched, still owned by its new claimant.
            await using (var survives = dataSource.CreateCommand(
                $"SELECT count(*) FROM {store.MessageTable} WHERE id = @id;"))
            {
                survives.Parameters.AddWithValue("@id", id);
                Assert.Equal(1L, (long)(await survives.ExecuteScalarAsync())!);
            }
        });
    }

    [Fact]
    public async Task TransportStore_WorkerTransportSubscribersAndDeliveryStatesRoundTrip()
    {
        await WithDataSourceAsync("transport", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            var optionsAccessor = Options.Create(options);
            var store = new PostgreSqlTransportStore(dataSource, optionsAccessor);
            await store.EnsureCreatedAsync();

            Assert.Equal(Quote(schema), store.Schema);
            Assert.Contains(Quote(options.MessageTable), store.MessageTable, StringComparison.Ordinal);

            var id = Guid.NewGuid();
            await store.PublishAsync(
                id,
                options.WorkerQueue,
                """{"kind":"ack"}""",
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["X-Trace"] = "trace-1" },
                CancellationToken.None);

            var delivery = await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None);
            Assert.NotNull(delivery);
            Assert.Equal(id, delivery.Id);
            Assert.Equal(1, delivery.Attempt);
            Assert.Equal("trace-1", delivery.Headers["x-trace"]);
            await delivery.AckAsync();
            Assert.Null(await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None));

            var nakId = Guid.NewGuid();
            await store.PublishAsync(nakId, options.WorkerQueue, """{"kind":"nak"}""", null, CancellationToken.None);
            var retry = (await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None))!;
            // Ten minutes, not thirty milliseconds — same reasoning as the SQL Server twin of this
            // test: the assertion below is that the row is INVISIBLE during its delay, and a delay
            // that can expire while the next round trip is in flight makes it a race with the
            // database rather than a statement about the store.
            await retry.NakAsync(TimeSpan.FromMinutes(10));
            Assert.Null(await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None));

            // Bring the row forward by hand so redelivery is a fact rather than a wait.
            await using (var release = dataSource.CreateCommand(
                $"UPDATE {store.MessageTable} SET available_at = now() - interval '1 minute' WHERE id = @id;"))
            {
                release.Parameters.AddWithValue("@id", nakId);
                await release.ExecuteNonQueryAsync();
            }

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
            var disabledStore = new PostgreSqlTransportStore(dataSource, Options.Create(disabledOptions));
            await disabledStore.PublishAsync(Guid.NewGuid(), disabledOptions.WorkerQueue, """{"kind":"disabled"}""", null, CancellationToken.None);
            var disabled = (await disabledStore.TryClaimAsync(disabledOptions.WorkerQueue, disabledOptions.LockTimeout, CancellationToken.None))!;
            Assert.True(await disabled.DeadLetterAsync(new InvalidOperationException("no dlq"), true, CancellationToken.None));
            Assert.Null(await disabledStore.TryClaimAsync(disabledOptions.WorkerQueue, disabledOptions.LockTimeout, CancellationToken.None));

            for (var i = 0; i < 3; i++)
                await store.PublishAsync(Guid.NewGuid(), "batch", $$"""{"index":{{i}}}""", null, CancellationToken.None);

            var batch = new List<PostgreSqlTransportDelivery>();
            await foreach (var item in store.ClaimBatchAsync("batch", 2, options.LockTimeout, CancellationToken.None))
                batch.Add(item);
            Assert.Equal(2, batch.Count);
            foreach (var item in batch)
                await item.AckAsync();

            await AssertTransportListenReceivesAsync(store, async () =>
            {
                await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, """{"kind":"listen"}""", null, CancellationToken.None);
            });
            await DrainQueueAsync(store, options.WorkerQueue, options.LockTimeout);

            var transport = new PostgreSqlWorkerTransport(optionsAccessor, store);
            await transport.PublishAsync(new WorkerJobEnvelope
            {
                CorrelationId = "corr-worker",
                Call = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IDirectPostgreSqlRecoveryFlow).FullName!,
                    MethodName = nameof(IDirectPostgreSqlRecoveryFlow.ResumeAsync),
                    Params = []
                },
                ReplyTarget = new AsyncResponseReplyTarget
                {
                    Name = "default",
                    Transport = PostgreSqlAsyncResponseTransportOptions.TransportName,
                    Address = options.ResponseQueue
                }
            });

            var jobDelivery = (await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None))!;
            Assert.Equal("corr-worker", jobDelivery.Headers[options.CorrelationIdHeader]);
            var job = JsonSerializer.Deserialize<WorkerJobEnvelope>(jobDelivery.Payload);
            Assert.NotNull(job);
            Assert.Equal("corr-worker", job.CorrelationId);
            Assert.Equal(nameof(IDirectPostgreSqlRecoveryFlow.ResumeAsync), job.Call.MethodName);
            await jobDelivery.AckAsync();

            var ingress = new RecordingIngress();
            var workerSubscriber = new PostgreSqlWorkerSubscriber(
                optionsAccessor,
                store,
                ingress,
                NullLogger<PostgreSqlWorkerSubscriber>.Instance);
            var responseSubscriber = new PostgreSqlResponseIngressSubscriber(
                optionsAccessor,
                store,
                ingress,
                NullLogger<PostgreSqlResponseIngressSubscriber>.Instance);

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
        Action<PostgreSqlAsyncResponseChannelOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(EnabledLogger<>));
        services.AddSingleton<IDirectPostgreSqlRecoveryFlow, DirectRecoveryFlow>();
        services.AddSingleton(provider => (DirectRecoveryFlow)provider.GetRequiredService<IDirectPostgreSqlRecoveryFlow>());
        services.AddSingleton<NpgsqlDataSource>(_ => NpgsqlDataSource.Create(Fixture.PostgreSqlConnectionString));
        services.AddAsyncResponse().WithPostgreSqlChannel(options =>
        {
            ApplyChannelOptions(options, schema);
            configure?.Invoke(options);
        });
        return services.BuildServiceProvider();
    }

    /// <summary>
    /// Round 39: the sweep's page carries the envelope only for rows nobody has acknowledged; an
    /// acknowledged row comes back header-only and is hydrated by id when a live subscription
    /// still has to receive it. Pre-fix both reads returned the body for every row (and the
    /// by-id read did not exist).
    /// </summary>
    [Fact]
    public async Task LoadMessages_ShipsTheEnvelopeOnlyForUnacknowledgedRows_AndHydratesById()
    {
        await WithDataSourceAsync("sweep_header_only", async (schema, dataSource) =>
        {
            var sql = new PostgreSqlChannelSql(dataSource, Options.Create(ChannelOptions(schema)));
            await sql.EnsureCreatedAsync();
            var correlationId = $"header-only-{Guid.NewGuid():N}";
            var since = (await sql.GetServerTimeUtcAsync(CancellationToken.None)).AddSeconds(-1);
            var pending = Guid.NewGuid();
            var acked = Guid.NewGuid();
            await sql.InsertMessageAsync(pending, correlationId, """{"Success":true,"Payload":"pending"}""", TimeSpan.FromMinutes(5), CancellationToken.None);
            await sql.InsertMessageAsync(acked, correlationId, """{"Success":true,"Payload":"acked"}""", TimeSpan.FromMinutes(5), CancellationToken.None);
            Assert.True(await sql.TryClaimForDeliveryAsync(acked, CancellationToken.None));

            var page = await sql.LoadMessagesAsync(correlationId, since, 16, null, null, CancellationToken.None);
            Assert.Equal(2, page.Count);
            Assert.Contains("\"pending\"", Assert.Single(page, m => m.Id == pending).EnvelopeJson, StringComparison.Ordinal);
            var ackedRow = Assert.Single(page, m => m.Id == acked);
            Assert.Null(ackedRow.EnvelopeJson);
            Assert.NotNull(ackedRow.AckedAtUtc);
            Assert.NotNull(ackedRow.AckedSeq);

            var hydrated = await sql.LoadMessagesByIdAsync(correlationId, [acked, Guid.NewGuid()], CancellationToken.None);
            var full = Assert.Single(hydrated);
            Assert.Equal(acked, full.Id);
            Assert.Contains("\"acked\"", full.EnvelopeJson, StringComparison.Ordinal);
            Assert.Equal(ackedRow.AckedSeq, full.AckedSeq);
            Assert.Empty(await sql.LoadMessagesByIdAsync("some-other-correlation", [acked], CancellationToken.None));
        });
    }

    /// <summary>
    /// Fixpoint r1 S5#19 / GS3#6: the channel's and the transport's LISTEN connections come from
    /// the shared pool, and Npgsql keeps a connector usable after a cancelled wait. With
    /// <c>No Reset On Close=true</c> (the setting docs/postgresql.md recommends) nothing ever ran
    /// <c>DISCARD ALL</c> on it, so a disposed channel — and every transport subscriber restart —
    /// returned a still-LISTENing connection to the pool. Both now <c>UNLISTEN *</c> before the
    /// connection goes back. A one-connection pool hands the listener's connector straight to the
    /// probe. Pre-fix: <c>pg_listening_channels()</c> still listed the channel.
    /// <para>
    /// The probe must also be that SAME backend (pre-commit fix, fixpoint r1 pass 2): an UNLISTEN
    /// that fails on a still-open connection makes the release clear the pool instead, and a fresh
    /// backend lists no channels either — so without the pid check this passed whether or not
    /// UNLISTEN works after a cancelled wait, which is the premise of the release, and a failing
    /// one clears the application's shared pool on every listener teardown.
    /// </para>
    /// </summary>
    [Fact]
    public async Task ListenConnections_GoBackToThePoolWithoutTheirSubscription()
    {
        var schema = NewSchema("unlisten");
        var pooled = new NpgsqlConnectionStringBuilder(Fixture.PostgreSqlConnectionString)
        {
            NoResetOnClose = true,
            MaxPoolSize = 1
        }.ConnectionString;
        await using var dataSource = NpgsqlDataSource.Create(pooled);
        await using var notifier = NpgsqlDataSource.Create(Fixture.PostgreSqlConnectionString);
        try
        {
            var channelOptions = ChannelOptions(schema);
            var channel = new PostgreSqlChannelSql(dataSource, Options.Create(channelOptions));
            await channel.EnsureCreatedAsync();
            await AssertListenReleasesItsSubscriptionAsync(
                dataSource,
                notifier,
                channelOptions.NotificationChannel,
                (wake, token) => channel.ExecuteListenAsync(_ => wake(), token));

            var transportOptions = TransportOptions(schema);
            var transport = new PostgreSqlTransportStore(dataSource, Options.Create(transportOptions));
            await transport.EnsureCreatedAsync();
            await AssertListenReleasesItsSubscriptionAsync(
                dataSource,
                notifier,
                transportOptions.NotificationChannel,
                (wake, token) => transport.ExecuteListenAsync(wake, token));
        }
        finally
        {
            await DropSchemaAsync(notifier, schema);
        }
    }

    private static async Task AssertListenReleasesItsSubscriptionAsync(
        NpgsqlDataSource dataSource,
        NpgsqlDataSource notifier,
        string notificationChannel,
        Func<Func<Task>, CancellationToken, Task> listen)
    {
        // The pool's one backend, which the listener is about to take.
        int listenerBackend;
        await using (var before = await dataSource.OpenConnectionAsync())
        await using (var pid = before.CreateCommand())
        {
            pid.CommandText = "SELECT pg_backend_pid();";
            listenerBackend = (int)(await pid.ExecuteScalarAsync())!;
        }

        using var cts = new CancellationTokenSource();
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var listenTask = Task.Run(() => listen(() =>
        {
            received.TrySetResult();
            return Task.CompletedTask;
        }, cts.Token));

        // LISTEN is established once a NOTIFY from another connection reaches it.
        await EventuallyAsync(async () =>
        {
            await NotifyAsync(notifier, notificationChannel, "unlisten-probe");
            return received.Task.IsCompleted;
        });
        await cts.CancelAsync();
        await IgnoreCancellationAsync(listenTask);

        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT pg_backend_pid(), (SELECT count(*) FROM pg_listening_channels());";
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(listenerBackend, reader.GetInt32(0));
        Assert.Equal(0L, reader.GetInt64(1));
    }

    [Fact]
    public async Task Transport_PublishesAndClaimsAPayloadCarryingANul_ByteIdentical()
    {
        // Regression (round-29 parity): payload_json/headers_json were jsonb, which REJECTS the
        // \u0000 escape System.Text.Json emits for U+0000 (22P05) — a job whose string argument or
        // context value carried a NUL was unpublishable on PostgreSQL alone.
        await WithDataSourceAsync("nul_payload", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options));
            var payload = JsonSerializer.Serialize(new { Note = "before\u0000after", Keys = new { b = 1, a = 2 } });
            var headers = new Dictionary<string, string> { ["X-Note"] = "nul\u0000header" };

            await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, payload, headers, CancellationToken.None);
            var delivery = await store.TryClaimAsync(options.WorkerQueue, TimeSpan.FromSeconds(30), CancellationToken.None);

            Assert.NotNull(delivery);
            Assert.Equal(payload, delivery!.Payload);
            Assert.Equal("nul\u0000header", delivery.Headers["X-Note"]);
        });
    }

    [Fact]
    public async Task Transport_AutoCreate_ConvertsAnExistingJsonbQueueTableToText_InPlace()
    {
        // A table created by an older build is converted once, in place, on the auto-create path —
        // rows survive, headers_json keeps a (text) default, and a NUL payload then publishes. And in
        // ONE rewrite: a statement per column rewrote the table (and rebuilt every index) twice under
        // ACCESS EXCLUSIVE. The table_rewrite event trigger counts them (database-wide, so it filters
        // to this test's table and is dropped with the schema — and explicitly, below).
        await WithDataSourceAsync("jsonb_upgrade", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            options.MessageTable = "jobs";
            var existing = Guid.NewGuid();
            await CreatePreTextQueueTableAsync(dataSource, schema, options, existing);
            await ExecuteAsync(dataSource, $"""
                CREATE TABLE "{schema}".rewrites (n integer NOT NULL);
                INSERT INTO "{schema}".rewrites VALUES (0);
                CREATE FUNCTION "{schema}".count_rewrite() RETURNS event_trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.oid = pg_event_trigger_table_rewrite_oid() AND n.nspname = '{schema}' AND c.relname = 'jobs')
                    THEN
                        UPDATE "{schema}".rewrites SET n = n + 1;
                    END IF;
                END $$;
                CREATE EVENT TRIGGER "{schema}_rewrites" ON table_rewrite EXECUTE FUNCTION "{schema}".count_rewrite();
                """);

            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options));
            try
            {
                await store.EnsureCreatedAsync();
            }
            finally
            {
                await ExecuteAsync(dataSource, $"""DROP EVENT TRIGGER IF EXISTS "{schema}_rewrites";""");
            }

            Assert.Equal(1L, await CountAsync(dataSource, $"""SELECT n FROM "{schema}".rewrites;"""));

            await using (var connection = await dataSource.OpenConnectionAsync())
            await using (var command = connection.CreateCommand())
            {
                command.CommandText =
                    $"""
                    SELECT string_agg(column_name || ':' || data_type || ':' || coalesce(column_default, ''), ',' ORDER BY column_name)
                    FROM information_schema.columns
                    WHERE table_schema = '{schema}' AND table_name = 'jobs' AND column_name IN ('payload_json', 'headers_json');
                    """;
                Assert.Equal("headers_json:text:'{}'::text,payload_json:text:", (string)(await command.ExecuteScalarAsync())!);
            }

            var old = await store.TryClaimAsync(options.WorkerQueue, TimeSpan.FromSeconds(30), CancellationToken.None);
            Assert.Equal(existing, old!.Id);
            await old.AckAsync();

            var nul = JsonSerializer.Serialize(new { Note = "a\u0000b" });
            await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, nul, null, CancellationToken.None);
            Assert.Equal(nul, (await store.TryClaimAsync(options.WorkerQueue, TimeSpan.FromSeconds(30), CancellationToken.None))!.Payload);
        });
    }

    [Fact]
    public async Task Transport_AutoCreate_TheJsonbConversionFailsFastOnABusyTable_AndConvertsOnceItIsFree()
    {
        // Regression: the conversion waited for its ACCESS EXCLUSIVE lock with no lock bound — every
        // later statement on the queue queued behind the waiting request — until the data source's
        // 30 s command timeout rolled it back, and every later operation then retried it the same
        // way. The DDL transaction's lock_timeout now fails it fast (55P03), changing nothing, and
        // the store waits out a retry-after window before its next attempt instead of queueing
        // another lock request behind the same holder at once.
        await WithDataSourceAsync("jsonb_busy", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            options.MessageTable = "jobs";
            await CreatePreTextQueueTableAsync(dataSource, schema, options, Guid.NewGuid());
            var clock = new ManualClock();
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options)) { Clock = clock };

            await using (var holder = await dataSource.OpenConnectionAsync())
            await using (var transaction = await holder.BeginTransactionAsync())
            {
                await using (var read = holder.CreateCommand())
                {
                    read.Transaction = transaction;
                    read.CommandText = $"""SELECT count(*) FROM "{schema}"."jobs";""";
                    await read.ExecuteScalarAsync();
                }

                var busy = await Assert.ThrowsAsync<PostgresException>(() => store.EnsureCreatedAsync());
                Assert.Equal(PostgresErrorCodes.LockNotAvailable, busy.SqlState);

                // Inside the window the next operation fails at once, without a second lock request
                // (which would have answered 55P03 again: the holder is still open).
                var latched = await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync());
                Assert.Same(busy, latched.InnerException);
                await transaction.CommitAsync();
            }

            Assert.Equal(2L, await CountAsync(dataSource, $"""
                SELECT count(*) FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = 'jobs' AND column_name IN ('payload_json', 'headers_json') AND data_type = 'jsonb';
                """));

            await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync());
            clock.Advance(2 * PostgreSqlTransportStore.DdlLockTimeoutBackoff);
            await store.EnsureCreatedAsync();

            Assert.Equal(0L, await CountAsync(dataSource, $"""
                SELECT count(*) FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = 'jobs' AND column_name IN ('payload_json', 'headers_json') AND data_type = 'jsonb';
                """));
        });
    }

    [Fact]
    public async Task Channel_AutoCreate_OnAnExistingSchema_LocksNoTable_SoAWriterDoesNotStallIt()
    {
        // Regression (fixpoint r2 S5#2): the channel ran ALTER TABLE … ADD COLUMN IF NOT EXISTS and
        // CREATE INDEX IF NOT EXISTS on every start, and PostgreSQL takes their ACCESS EXCLUSIVE /
        // SHARE lock BEFORE the IF NOT EXISTS check — so behind a session holding a conflicting lock
        // (here: an open writer's ROW EXCLUSIVE) a fresh host's EnsureCreated queued until the 30 s
        // command timeout, with every channel statement of every host queued behind its request. It
        // now alters and builds only what the catalog shows missing, and on an existing schema that
        // is nothing, so it takes no table lock at all.
        await WithDataSourceAsync("chan_busy", async (schema, dataSource) =>
        {
            var options = ChannelOptions(schema);
            await new PostgreSqlChannelSql(dataSource, Options.Create(options)).EnsureCreatedAsync();

            await using var holder = await dataSource.OpenConnectionAsync();
            await using var transaction = await holder.BeginTransactionAsync();
            await using (var writer = holder.CreateCommand())
            {
                writer.Transaction = transaction;
                writer.CommandText =
                    $"""
                    LOCK TABLE "{schema}"."{options.MessageTable}", "{schema}"."{options.RecoveryStateTable}", "{schema}"."{options.SubscriberTable}"
                        IN ROW EXCLUSIVE MODE;
                    """;
                await writer.ExecuteNonQueryAsync();
            }

            await new PostgreSqlChannelSql(dataSource, Options.Create(ChannelOptions(schema))).EnsureCreatedAsync().WaitAsync(TimeSpan.FromSeconds(10));
            await transaction.RollbackAsync();
        });
    }

    [Fact]
    public async Task Channel_AutoCreate_TheJsonbConversionFailsFastOnABusyTable_AndBacksOff()
    {
        // Regression (fixpoint r2 S5#2): the channel's jsonb → text rewrite ran with no lock bound
        // and under the 30 s default command timeout, retried by every operation. Its lock wait is now
        // bounded by the DDL transaction's lock_timeout (55P03, nothing changed), the retry-after
        // window latches, and once the table is free the conversion runs as long-running DDL.
        await WithDataSourceAsync("chan_jsonb", async (schema, dataSource) =>
        {
            var options = ChannelOptions(schema);
            var creator = new PostgreSqlChannelSql(dataSource, Options.Create(options));
            await creator.EnsureCreatedAsync();
            await ExecuteAsync(dataSource, $"""ALTER TABLE {creator.MessageTable} ALTER COLUMN envelope_json TYPE jsonb USING envelope_json::jsonb;""");

            var clock = new ManualClock();
            var store = new PostgreSqlChannelSql(dataSource, Options.Create(ChannelOptions(schema))) { Clock = clock };
            await using (var holder = await dataSource.OpenConnectionAsync())
            await using (var transaction = await holder.BeginTransactionAsync())
            {
                await using (var read = holder.CreateCommand())
                {
                    read.Transaction = transaction;
                    read.CommandText = $"SELECT count(*) FROM {creator.MessageTable};";
                    await read.ExecuteScalarAsync();
                }

                var busy = await Assert.ThrowsAsync<PostgresException>(() => store.EnsureCreatedAsync().WaitAsync(TimeSpan.FromSeconds(20)));
                Assert.Equal(PostgresErrorCodes.LockNotAvailable, busy.SqlState);
                var latched = await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync());
                Assert.Same(busy, latched.InnerException);
                await transaction.CommitAsync();
            }

            clock.Advance(TimeSpan.FromMinutes(2));
            await store.EnsureCreatedAsync();
            Assert.Equal(0L, await CountAsync(dataSource, $"""
                SELECT count(*) FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = '{options.MessageTable}' AND column_name = 'envelope_json' AND data_type = 'jsonb';
                """));
        });
    }

    [Fact]
    public async Task Channel_AutoCreate_TheSchemaKeyHeldElsewhere_FailsFastAndBacksOff()
    {
        // Regression (fixpoint r2 S5#11): the channel waited for the schema's advisory DDL key with no
        // bound but the 30 s command timeout — the key the transport held through its hour-bounded
        // rewrite — and the next operation queued for it again. lock_timeout now comes first in the
        // transaction and bounds the advisory wait too: the attempt fails fast (55P03) and latches.
        await WithDataSourceAsync("chan_key", async (schema, dataSource) =>
        {
            await new PostgreSqlChannelSql(dataSource, Options.Create(ChannelOptions(schema))).EnsureCreatedAsync();

            await using var holder = await dataSource.OpenConnectionAsync();
            await using var transaction = await holder.BeginTransactionAsync();
            await using (var key = holder.CreateCommand())
            {
                key.Transaction = transaction;
                key.CommandText = "SELECT pg_advisory_xact_lock(@key);";
                key.Parameters.AddWithValue("key", PostgreSqlChannelSql.SchemaAdvisoryLockKey(schema));
                await key.ExecuteNonQueryAsync();
            }

            var store = new PostgreSqlChannelSql(dataSource, Options.Create(ChannelOptions(schema)));
            var busy = await Assert.ThrowsAsync<PostgresException>(() => store.EnsureCreatedAsync().WaitAsync(TimeSpan.FromSeconds(20)));
            Assert.Equal(PostgresErrorCodes.LockNotAvailable, busy.SqlState);
            Assert.Same(busy, (await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync())).InnerException);
            await transaction.RollbackAsync();
        });
    }

    [Fact]
    public async Task FlowStore_AutoCreate_OnAnExistingTable_LocksNoTable_SoAWriterDoesNotStallIt()
    {
        // Regression (fixpoint r2 S10#2): CREATE INDEX IF NOT EXISTS {table}_expires_idx ran on every
        // start and took its SHARE lock before finding the name taken — behind any open checkpoint
        // writer (ROW EXCLUSIVE), until the 30 s command timeout, with every checkpoint, lease acquire
        // and renewal of every host queued behind it. Only an absent index is built now.
        await WithDataSourceAsync("flow_busy", async (schema, dataSource) =>
        {
            var options = new PostgreSqlDurableFlowOptions { SchemaName = schema, TableName = "flow_state" };
            await new PostgreSqlFlowStateStore(dataSource, Options.Create(options)).LoadAsync("warm-up");

            await using var holder = await dataSource.OpenConnectionAsync();
            await using var transaction = await holder.BeginTransactionAsync();
            await using (var writer = holder.CreateCommand())
            {
                writer.Transaction = transaction;
                writer.CommandText =
                    $"""
                    INSERT INTO "{schema}"."flow_state" (flow_id, state_json, expires_at_utc, updated_at_utc, revision)
                    VALUES ('open-writer', '{"{}"}', now() + interval '1 hour', now(), 1);
                    """;
                await writer.ExecuteNonQueryAsync();
            }

            var fresh = new PostgreSqlFlowStateStore(dataSource, Options.Create(new PostgreSqlDurableFlowOptions { SchemaName = schema, TableName = "flow_state" }));
            Assert.Null(await fresh.LoadAsync("missing").WaitAsync(TimeSpan.FromSeconds(10)));
            await transaction.RollbackAsync();
        });
    }

    [Fact]
    public async Task FlowStore_AutoCreate_TheJsonbConversionFailsFastOnABusyTable_AndBacksOff()
    {
        // Regression (fixpoint r2 S10#2): the flow store's jsonb → text rewrite ran with no lock bound
        // and under the 30 s default command timeout, and a failed attempt was retried by every flow
        // operation. The wait is bounded (55P03, nothing changed), the window latches, and once the
        // table is free the conversion runs as long-running DDL.
        await WithDataSourceAsync("flow_jsonb", async (schema, dataSource) =>
        {
            await ExecuteAsync(dataSource, $"""
                CREATE SCHEMA IF NOT EXISTS "{schema}";
                CREATE TABLE "{schema}"."flow_state" (
                    flow_id text NOT NULL PRIMARY KEY,
                    state_json jsonb NOT NULL,
                    expires_at_utc timestamptz NOT NULL,
                    updated_at_utc timestamptz NOT NULL,
                    revision bigint NOT NULL DEFAULT 0,
                    lease_id text NULL,
                    lease_expires_at_utc timestamptz NULL
                );
                CREATE INDEX "flow_state_expires_idx" ON "{schema}"."flow_state" (expires_at_utc);
                """);
            var clock = new ManualClock();
            var store = new PostgreSqlFlowStateStore(
                dataSource,
                Options.Create(new PostgreSqlDurableFlowOptions { SchemaName = schema, TableName = "flow_state" })) { Clock = clock };

            await using (var holder = await dataSource.OpenConnectionAsync())
            await using (var transaction = await holder.BeginTransactionAsync())
            {
                await using (var read = holder.CreateCommand())
                {
                    read.Transaction = transaction;
                    read.CommandText = $"""SELECT count(*) FROM "{schema}"."flow_state";""";
                    await read.ExecuteScalarAsync();
                }

                var busy = await Assert.ThrowsAsync<PostgresException>(() => store.LoadAsync("flow").WaitAsync(TimeSpan.FromSeconds(20)));
                Assert.Equal(PostgresErrorCodes.LockNotAvailable, busy.SqlState);
                Assert.Same(busy, (await Assert.ThrowsAsync<InvalidOperationException>(() => store.LoadAsync("flow"))).InnerException);
                await transaction.CommitAsync();
            }

            clock.Advance(TimeSpan.FromMinutes(2));
            Assert.Null(await store.LoadAsync("flow"));
            Assert.Equal(0L, await CountAsync(dataSource, $"""
                SELECT count(*) FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = 'flow_state' AND column_name = 'state_json' AND data_type = 'jsonb';
                """));
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FlowStore_AnExpiryIndexNotValidAndReady_WarnsInsteadOfFailing(bool autoCreateSchema)
    {
        // Regression (fixpoint r2 S4#6): with AutoCreateSchema = false the expiry index is Optional,
        // but an Optional index that EXISTS and is not valid and ready — an operator's CREATE INDEX
        // CONCURRENTLY in progress, or one that failed — threw "invalid or not ready", failing every
        // flow operation on every host that started meanwhile. It is prune performance, like an absent
        // index: a warning, on the auto-create path too (which builds only an ABSENT index).
        await WithDataSourceAsync("flow_invalid", async (schema, dataSource) =>
        {
            await new PostgreSqlFlowStateStore(
                dataSource,
                Options.Create(new PostgreSqlDurableFlowOptions { SchemaName = schema, TableName = "flow_state" })).LoadAsync("warm-up");
            await ExecuteAsync(dataSource, $"""UPDATE pg_index SET indisvalid = false WHERE indexrelid = '"{schema}"."flow_state_expires_idx"'::regclass;""");

            var logger = new RecordingLogger<PostgreSqlFlowStateStore>();
            var store = new PostgreSqlFlowStateStore(
                dataSource,
                Options.Create(new PostgreSqlDurableFlowOptions { SchemaName = schema, TableName = "flow_state", AutoCreateSchema = autoCreateSchema }),
                logger: logger);

            Assert.Null(await store.LoadAsync("missing"));
            Assert.Contains(logger.Warnings, warning => warning.Contains("not valid and ready", StringComparison.Ordinal));
        });
    }

    [Fact]
    public async Task Transport_AutoCreate_AConversionFailureOtherThanALockTimeout_BacksOffToo()
    {
        // Regression (fixpoint r2 S9#2, S11#2): only a lock timeout latched the retry-after window, so
        // a conversion that failed any other way — here a view over payload_json (0A000); equally a
        // statement_timeout it outran (57014), or a full disk — re-ran its ACCESS EXCLUSIVE rewrite on
        // every later publish and claim of every host. Any failure of the long-running step latches now.
        await WithDataSourceAsync("jsonb_view", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            options.MessageTable = "jobs";
            await CreatePreTextQueueTableAsync(dataSource, schema, options, Guid.NewGuid());
            await ExecuteAsync(dataSource, $"""CREATE VIEW "{schema}"."jobs_payloads" AS SELECT payload_json FROM "{schema}"."jobs";""");
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options)) { Clock = new ManualClock() };

            var failed = await Assert.ThrowsAsync<PostgresException>(() => store.EnsureCreatedAsync());
            Assert.Equal(PostgresErrorCodes.FeatureNotSupported, failed.SqlState);

            var latched = await Assert.ThrowsAsync<InvalidOperationException>(() => store.EnsureCreatedAsync());
            Assert.Same(failed, latched.InnerException);
            Assert.Contains("failed and was rolled back", latched.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task Transport_TheOneTimeTableWork_HoldsTheTableKey_NotTheSchemaKey()
    {
        // Regression (fixpoint r2 S9#6): the hour-bounded rewrite ran inside the transaction holding
        // the schema-wide advisory key the channel and flow store also take, so for the whole rewrite
        // no host starting meanwhile could initialize either of them. The schema-shared DDL now
        // commits first and the rewrite runs under a key scoped to the table: while it waits for its
        // lock here, the schema key is free and the table key is held.
        await WithDataSourceAsync("table_key", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            options.MessageTable = "jobs";
            await CreatePreTextQueueTableAsync(dataSource, schema, options, Guid.NewGuid());
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options));

            await using var holder = await dataSource.OpenConnectionAsync();
            await using var transaction = await holder.BeginTransactionAsync();
            await using (var read = holder.CreateCommand())
            {
                read.Transaction = transaction;
                read.CommandText = $"""SELECT count(*) FROM "{schema}"."jobs";""";
                await read.ExecuteScalarAsync();
            }

            var ensure = store.EnsureCreatedAsync();
            await EventuallyAsync(async () => await CountAsync(dataSource, $"""
                SELECT count(*) FROM pg_locks WHERE relation = '"{schema}"."jobs"'::regclass AND NOT granted;
                """) > 0);

            Assert.True(await TryAdvisoryKeyAsync(dataSource, PostgreSqlTransportStore.SchemaAdvisoryLockKey(schema)));
            Assert.False(await TryAdvisoryKeyAsync(dataSource, PostgreSqlTransportStore.TableAdvisoryLockKey(schema, "jobs")));

            await transaction.CommitAsync();
            try
            {
                await ensure;
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.LockNotAvailable)
            {
                // A slow runner outwaited the 5 s lock_timeout; the keys were the assertion.
            }
        });
    }

    [Fact]
    public async Task Transport_AutoCreate_ACancelledCaller_DoesNotRollBackTheConversion()
    {
        // Regression (fixpoint r2 S9#9): the startup DDL ran under the first caller's token, so a
        // publish whose request token fired partway cancelled the hour-bounded rewrite and rolled it
        // back, after the table had been locked all that time. The attempt now runs under the store's
        // lifetime: the cancelled caller leaves, and the rewrite finishes once its lock is free.
        await WithDataSourceAsync("jsonb_cancel", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            options.MessageTable = "jobs";
            await CreatePreTextQueueTableAsync(dataSource, schema, options, Guid.NewGuid());
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options));
            using var caller = new CancellationTokenSource();

            await using (var holder = await dataSource.OpenConnectionAsync())
            await using (var transaction = await holder.BeginTransactionAsync())
            {
                await using (var read = holder.CreateCommand())
                {
                    read.Transaction = transaction;
                    read.CommandText = $"""SELECT count(*) FROM "{schema}"."jobs";""";
                    await read.ExecuteScalarAsync();
                }

                var publish = store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, "{}", null, caller.Token);
                await EventuallyAsync(async () => await CountAsync(dataSource, $"""
                    SELECT count(*) FROM pg_locks WHERE relation = '"{schema}"."jobs"'::regclass AND NOT granted;
                    """) > 0);
                await caller.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => publish);
                await transaction.CommitAsync();
            }

            await EventuallyAsync(async () => await CountAsync(dataSource, $"""
                SELECT count(*) FROM information_schema.columns
                WHERE table_schema = '{schema}' AND table_name = 'jobs' AND column_name IN ('payload_json', 'headers_json') AND data_type = 'jsonb';
                """) == 0);
        });
    }

    [Fact]
    public async Task Transport_ADelayedPublish_SendsNoNotify()
    {
        // Regression (fixpoint r2 GS3#4): a delayed publish NOTIFYed the queue for a row nobody can
        // claim until its delay has passed, waking every idle subscriber of the queue in every process
        // into a claim that found nothing. One session's notifications arrive in commit order, so the
        // first one received after a delayed publish to one queue and an immediate publish to another
        // is the immediate one's — had the delayed publish notified, its queue would arrive first.
        await WithDataSourceAsync("delay_notify", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options));
            await store.EnsureCreatedAsync();

            await using var listener = await dataSource.OpenConnectionAsync();
            var received = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            listener.Notification += (_, e) => received.TrySetResult(e.Payload);
            await using (var listen = listener.CreateCommand())
            {
                listen.CommandText = $"""LISTEN "{options.NotificationChannel}";""";
                await listen.ExecuteNonQueryAsync();
            }

            await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, "{}", null, CancellationToken.None, delay: TimeSpan.FromMinutes(5));
            await store.PublishAsync(Guid.NewGuid(), options.ResponseQueue, "{}", null, CancellationToken.None);

            using var wait = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            while (!received.Task.IsCompleted)
                await listener.WaitAsync(wait.Token);
            Assert.Equal(options.ResponseQueue, await received.Task);
        });
    }

    private static async Task<bool> TryAdvisoryKeyAsync(NpgsqlDataSource dataSource, long key)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT pg_try_advisory_xact_lock(@key);";
        command.Parameters.AddWithValue("key", key);
        var taken = (bool)(await command.ExecuteScalarAsync())!;
        await transaction.RollbackAsync();
        return taken;
    }

    [Fact]
    public async Task Transport_DeadLettersAFailureWhoseMessageCarriesANul()
    {
        // dead_letter_reason is text, and PostgreSQL text rejects a NUL character outright (22021):
        // a failure whose exception message carried one could never be dead-lettered.
        await WithDataSourceAsync("nul_reason", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options));
            await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, "{}", null, CancellationToken.None);
            var delivery = (await store.TryClaimAsync(options.WorkerQueue, TimeSpan.FromSeconds(30), CancellationToken.None))!;

            Assert.True(await delivery.DeadLetterAsync(new InvalidOperationException("bad\u0000byte"), true, CancellationToken.None));

            await using var connection = await dataSource.OpenConnectionAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"""SELECT dead_letter_reason FROM "{schema}"."{options.MessageTable}" WHERE queue = '{options.DeadLetterQueue}';""";
            Assert.Equal("bad\uFFFDbyte", (string)(await command.ExecuteScalarAsync())!);
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Transport_AnInvalidDequeueIndex_WarnsInsteadOfFailingStartup(bool autoCreateSchema)
    {
        // A CREATE INDEX CONCURRENTLY still running (or one that failed) leaves the index in
        // pg_class with indisvalid = false. Verified as present, it failed EnsureCreated — and so
        // every publish — on every host that started during the build, and forever after a failed
        // one. It is claim performance, like an absent index: a warning — on the auto-create path
        // too, where docs/postgresql.md has large tables pre-build it CONCURRENTLY.
        await WithDataSourceAsync("invalid_ready", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            options.MessageTable = "jobs";
            await new PostgreSqlTransportStore(dataSource, Options.Create(options)).EnsureCreatedAsync();
            var readyIndex = PostgreSqlTransportStore.IndexName("jobs", "ready");
            await ExecuteAsync(dataSource, $"""UPDATE pg_index SET indisvalid = false WHERE indexrelid = '"{schema}"."{readyIndex}"'::regclass;""");

            options.AutoCreateSchema = autoCreateSchema;
            var logger = new RecordingLogger<PostgreSqlTransportStore>();
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options), logger);
            await store.EnsureCreatedAsync();

            Assert.Contains(logger.Warnings, warning => warning.Contains("not valid and ready", StringComparison.Ordinal));
            await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, "{}", null, CancellationToken.None);
        });
    }

    [Fact]
    public async Task Transport_OperatorSchema_WithoutTheDequeueIndex_Warns()
    {
        // Round 42 (S9#15): an operator table carrying no ready index still starts, with a warning.
        await WithDataSourceAsync("absent_ready", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            options.MessageTable = "jobs";
            await new PostgreSqlTransportStore(dataSource, Options.Create(options)).EnsureCreatedAsync();
            await ExecuteAsync(dataSource, $"""DROP INDEX "{schema}"."{PostgreSqlTransportStore.IndexName("jobs", "ready")}";""");

            options.AutoCreateSchema = false;
            var logger = new RecordingLogger<PostgreSqlTransportStore>();
            await new PostgreSqlTransportStore(dataSource, Options.Create(options), logger).EnsureCreatedAsync();

            Assert.Contains(logger.Warnings, warning => warning.Contains("has no dequeue index", StringComparison.Ordinal));
        });
    }

    [Fact]
    public async Task Transport_OperatorSchema_StillOnJsonb_FailsWithTheMigration()
    {
        await WithDataSourceAsync("managed_jsonb", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            options.MessageTable = "jobs";
            await ExecuteAsync(dataSource, $"""
                CREATE SCHEMA IF NOT EXISTS "{schema}";
                CREATE TABLE "{schema}"."jobs" (
                    id uuid PRIMARY KEY,
                    queue text NOT NULL,
                    payload_json jsonb NOT NULL,
                    headers_json jsonb NOT NULL DEFAULT jsonb_build_object(),
                    created_at timestamptz NOT NULL DEFAULT now(),
                    available_at timestamptz NOT NULL DEFAULT now(),
                    locked_until timestamptz NULL,
                    lock_id uuid NULL,
                    attempts integer NOT NULL DEFAULT 0,
                    dead_letter_reason text NULL
                );
                """);

            options.AutoCreateSchema = false;
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new PostgreSqlTransportStore(dataSource, Options.Create(options)).EnsureCreatedAsync());
            Assert.Contains("TYPE text USING payload_json::text", ex.Message, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task Transport_DeadLetterPrune_DrainsABacklogBeyondOneBatch()
    {
        // Regression: the prune was ONE unbounded DELETE. The statement trigger below stands in for
        // a command timeout on a big backlog: any single statement deleting more than 1,000 rows
        // fails — so the old prune failed (swallowed) and nothing shrank, while the bounded drain
        // removes the whole backlog in batches within its budget.
        await WithDataSourceAsync("dlq_drain", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options));
            await store.EnsureCreatedAsync();
            var table = $"\"{schema}\".\"{options.MessageTable}\"";
            await ExecuteAsync(dataSource, $"""
                INSERT INTO {table} (id, queue, payload_json, created_at)
                SELECT gen_random_uuid(), '{options.DeadLetterQueue}', '{EmptyJson}', now() - interval '1 hour'
                FROM generate_series(1, 2500);
                CREATE FUNCTION "{schema}".cap_delete() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF (SELECT count(*) FROM old_rows) > 1000 THEN
                        RAISE EXCEPTION 'unbounded delete';
                    END IF;
                    RETURN NULL;
                END $$;
                CREATE TRIGGER cap_delete AFTER DELETE ON {table}
                    REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".cap_delete();
                """);

            await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, "{}", null, CancellationToken.None);

            Assert.Equal(0L, await CountAsync(dataSource, $"SELECT count(*) FROM {table} WHERE queue = '{options.DeadLetterQueue}';"));
        });
    }

    [Fact]
    public async Task Transport_ClaimOrder_IsAvailabilityOrder()
    {
        // Round 42 (S9#15): a delayed or redelivered row queues by when it became due.
        await WithDataSourceAsync("claim_order", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options));
            var first = Guid.NewGuid();
            var second = Guid.NewGuid();
            await store.PublishAsync(first, options.WorkerQueue, "{}", null, CancellationToken.None);
            await store.PublishAsync(second, options.WorkerQueue, "{}", null, CancellationToken.None);
            await ExecuteAsync(dataSource, $"""
                UPDATE "{schema}"."{options.MessageTable}" SET available_at = now() - interval '1 second' WHERE id = '{first}';
                UPDATE "{schema}"."{options.MessageTable}" SET available_at = now() - interval '1 minute' WHERE id = '{second}';
                """);

            var claimed = await store.TryClaimAsync(options.WorkerQueue, TimeSpan.FromSeconds(30), CancellationToken.None);
            Assert.Equal(second, claimed!.Id);
        });
    }

    [Fact]
    public async Task WorkerSubscriber_IsWokenOnlyByItsOwnQueuesNotify()
    {
        // Regression: the subscriber's LISTEN never named its queue, so every publish to ANY queue
        // on the shared notification channel woke it. A worker row inserted without a NOTIFY is
        // picked up only by the poll (5 min here) or by a wake — a NOTIFY carrying the RESPONSE
        // queue must not claim it; one carrying the worker queue must.
        await WithDataSourceAsync("scoped_wake", async (schema, dataSource) =>
        {
            var options = TransportOptions(schema);
            options.WorkerSubscriber.EmptyPollDelay = TimeSpan.FromMinutes(5);
            var store = new PostgreSqlTransportStore(dataSource, Options.Create(options));
            await store.EnsureCreatedAsync();

            // Evidence the subscriber's first (empty) claim has run, instead of a fixed sleep: its
            // ExecuteAsync is queued to the thread pool (Hosting 10.0.10+), so on a loaded runner a
            // late first claim picked up the row inserted below — no wake involved — and failed the
            // test as if a response-queue NOTIFY had woken it. A statement-level trigger fires once
            // per UPDATE statement even when the claim updates nothing.
            await ExecuteAsync(dataSource, $"""
                CREATE TABLE "{schema}".claim_statements (n bigint NOT NULL);
                INSERT INTO "{schema}".claim_statements VALUES (0);
                CREATE FUNCTION "{schema}".count_claim() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    UPDATE "{schema}".claim_statements SET n = n + 1;
                    RETURN NULL;
                END $$;
                CREATE TRIGGER count_claim AFTER UPDATE ON "{schema}"."{options.MessageTable}"
                    FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".count_claim();
                """);

            var ingress = new RecordingIngress();
            var handled = ingress.WorkerReceived;
            var subscriber = new PostgreSqlWorkerSubscriber(Options.Create(options), store, ingress, NullLogger<PostgreSqlWorkerSubscriber>.Instance);
            await subscriber.StartAsync(CancellationToken.None);
            try
            {
                // Wait (hang guard only) for the first empty claim to have committed and for the
                // LISTEN session to exist — the subscriber then sits in its 5-minute idle wait.
                using (var ready = new CancellationTokenSource(TimeSpan.FromSeconds(60)))
                {
                    while (await CountAsync(dataSource, $"""SELECT n FROM "{schema}".claim_statements;""") == 0
                           || await CountAsync(dataSource, $"""SELECT count(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND query LIKE 'LISTEN "{options.NotificationChannel}"%';""") == 0)
                    {
                        await Task.Delay(50, ready.Token);
                    }
                }

                await ExecuteAsync(dataSource, $"""INSERT INTO "{schema}"."{options.MessageTable}" (id, queue, payload_json) VALUES ('{Guid.NewGuid()}', '{options.WorkerQueue}', '{EmptyJson}');""");

                for (var i = 0; i < 5; i++)
                {
                    await ExecuteAsync(dataSource, $"SELECT pg_notify('{options.NotificationChannel}', '{options.ResponseQueue}');");
                    await Task.Delay(200);
                }

                Assert.False(handled.Task.IsCompleted, "a NOTIFY for the response queue woke the worker subscriber");

                using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(15));
                while (!handled.Task.IsCompleted && !deadline.IsCancellationRequested)
                {
                    await ExecuteAsync(dataSource, $"SELECT pg_notify('{options.NotificationChannel}', '{options.WorkerQueue}');");
                    await Task.WhenAny(handled.Task, Task.Delay(200));
                }

                Assert.True(handled.Task.IsCompleted, "a NOTIFY for the worker queue did not wake the worker subscriber");
            }
            finally
            {
                await subscriber.StopAsync(CancellationToken.None);
            }
        });
    }

    [Fact]
    public async Task Channel_MessagePrune_DrainsABacklogBeyondOneBatch_AndSubscriberOrphansArePrunedTableWide()
    {
        // S5#18: the publish-path message prune was one unbounded DELETE (the trigger below stands
        // in for a command timeout); S5#9: subscriber rows were only ever pruned for the CALLING
        // correlation id, so rows orphaned by a crashed process stayed forever.
        await WithDataSourceAsync("chan_drain", async (schema, dataSource) =>
        {
            var sql = new PostgreSqlChannelSql(dataSource, Options.Create(ChannelOptions(schema)));
            await sql.EnsureCreatedAsync();
            await ExecuteAsync(dataSource, $"""
                INSERT INTO {sql.MessageTable} (id, correlation_id, envelope_json, expires_at)
                SELECT gen_random_uuid(), 'expired-' || g, '{EmptyJson}', now() - interval '1 minute'
                FROM generate_series(1, 2500) AS g;
                INSERT INTO {sql.SubscriberTable} (correlation_id, registration_id, instance_id, expires_at)
                VALUES ('crashed-waiter', gen_random_uuid(), 'dead-instance', now() - interval '1 minute');
                CREATE FUNCTION "{schema}".cap_delete() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF (SELECT count(*) FROM old_rows) > 1000 THEN
                        RAISE EXCEPTION 'unbounded delete';
                    END IF;
                    RETURN NULL;
                END $$;
                CREATE TRIGGER cap_delete AFTER DELETE ON {sql.MessageTable}
                    REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION "{schema}".cap_delete();
                """);

            await sql.InsertMessageAsync(Guid.NewGuid(), "fresh", "{}", TimeSpan.FromMinutes(1), CancellationToken.None);
            Assert.Equal(0L, await CountAsync(dataSource, $"SELECT count(*) FROM {sql.MessageTable} WHERE expires_at <= now();"));

            await sql.CountActiveSubscribersAsync("some-other-id", CancellationToken.None);
            Assert.Equal(0L, await CountAsync(dataSource, $"SELECT count(*) FROM {sql.SubscriberTable} WHERE correlation_id = 'crashed-waiter';"));
        });
    }

    private const string EmptyJson = "{}";
    private const string OldJson = "{\"old\":1}";

    /// <summary>The queue table as a pre-text build created it (jsonb documents), with both indexes.</summary>
    private static Task CreatePreTextQueueTableAsync(NpgsqlDataSource dataSource, string schema, PostgreSqlAsyncResponseTransportOptions options, Guid existing)
        => ExecuteAsync(dataSource, $"""
            CREATE SCHEMA IF NOT EXISTS "{schema}";
            CREATE TABLE "{schema}"."{options.MessageTable}" (
                id uuid PRIMARY KEY,
                queue text NOT NULL,
                payload_json jsonb NOT NULL,
                headers_json jsonb NOT NULL DEFAULT jsonb_build_object(),
                created_at timestamptz NOT NULL DEFAULT now(),
                available_at timestamptz NOT NULL DEFAULT now(),
                locked_until timestamptz NULL,
                lock_id uuid NULL,
                attempts integer NOT NULL DEFAULT 0,
                dead_letter_reason text NULL
            );
            CREATE INDEX "{PostgreSqlTransportStore.IndexName(options.MessageTable, "ready")}" ON "{schema}"."{options.MessageTable}" (queue, available_at, created_at);
            CREATE INDEX "{PostgreSqlTransportStore.IndexName(options.MessageTable, "created")}" ON "{schema}"."{options.MessageTable}" (created_at);
            INSERT INTO "{schema}"."{options.MessageTable}" (id, queue, payload_json) VALUES ('{existing}', '{options.WorkerQueue}', '{OldJson}');
            """);

    /// <summary>A clock that moves only when told to (the transport store's retry-after window).</summary>
    private sealed class ManualClock : TimeProvider
    {
        private long _ticks = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero).UtcTicks;

        public void Advance(TimeSpan delta) => Interlocked.Add(ref _ticks, delta.Ticks);

        public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => Interlocked.Read(ref _ticks);
    }

    private static async Task ExecuteAsync(NpgsqlDataSource dataSource, string sql)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<long> CountAsync(NpgsqlDataSource dataSource, string sql)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(), System.Globalization.CultureInfo.InvariantCulture);
    }

    private sealed class RecordingLogger<T> : ILogger<T>
    {
        private readonly ConcurrentQueue<string> _warnings = new();

        public IReadOnlyCollection<string> Warnings => _warnings.ToArray();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel >= LogLevel.Warning)
                _warnings.Enqueue(formatter(state, exception));
        }
    }

    private async Task WithDataSourceAsync(string prefix, Func<string, NpgsqlDataSource, Task> body)
    {
        var schema = NewSchema(prefix);
        await using var dataSource = NpgsqlDataSource.Create(Fixture.PostgreSqlConnectionString);
        try
        {
            await body(schema, dataSource);
        }
        finally
        {
            await DropSchemaAsync(dataSource, schema);
        }
    }

    private static PostgreSqlAsyncResponseChannelOptions ChannelOptions(string schema)
    {
        var options = new PostgreSqlAsyncResponseChannelOptions();
        ApplyChannelOptions(options, schema);
        return options;
    }

    private static void ApplyChannelOptions(PostgreSqlAsyncResponseChannelOptions options, string schema)
    {
        options.SchemaName = schema;
        options.NotificationChannel = $"{schema}_channel";
        options.DefaultTimeout = TimeSpan.FromSeconds(5);
        options.RecoveryStateExpiry = TimeSpan.FromSeconds(30);
        options.MessageRetention = TimeSpan.FromSeconds(30);
        options.DeliveryConfirmationTimeout = TimeSpan.FromMilliseconds(250);
        options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(10);
        options.ListenerPollInterval = TimeSpan.FromMilliseconds(25);
        options.PendingMessageBatchSize = 32;
        options.SubscriberHeartbeatInterval = TimeSpan.FromMilliseconds(50);
        options.SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(1);
        options.PruneInterval = TimeSpan.Zero;
    }

    private static PostgreSqlAsyncResponseTransportOptions TransportOptions(string schema)
    {
        var options = new PostgreSqlAsyncResponseTransportOptions
        {
            SchemaName = schema,
            NotificationChannel = $"{schema}_transport",
            WorkerQueue = $"{schema}_worker",
            ResponseQueue = $"{schema}_response",
            DeadLetterQueue = $"{schema}_deadletter",
            LockTimeout = TimeSpan.FromMilliseconds(200),
            DeadLetterRetention = TimeSpan.FromSeconds(30),
            ShutdownTimeout = TimeSpan.FromSeconds(2),
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
        await using var dataSource = NpgsqlDataSource.Create(Fixture.PostgreSqlConnectionString);
        await DropSchemaAsync(dataSource, schema);
    }

    private static async Task DropSchemaAsync(NpgsqlDataSource dataSource, string schema)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"DROP SCHEMA IF EXISTS {Quote(schema)} CASCADE;";
        await command.ExecuteNonQueryAsync();
    }

    private static async Task InsertUnreadableRecoveryStateAsync(NpgsqlDataSource dataSource, string schema, string correlationId)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {Quote(schema)}.{Quote("asyncresponse_recovery_state")}
                (correlation_id, registration_id, state_json, expires_at, registered_at)
            VALUES (@correlation_id, @registration_id, '"bad-json-string"'::jsonb, now() + interval '30 seconds', now());
            """;
        command.Parameters.AddWithValue("correlation_id", correlationId);
        command.Parameters.AddWithValue("registration_id", Guid.NewGuid());
        await command.ExecuteNonQueryAsync();
    }

    private static async Task AssertListenReceivesAsync(
        NpgsqlDataSource dataSource,
        string notificationChannel,
        Func<Func<string?, Task>, CancellationToken, Task> listen)
    {
        using var cts = new CancellationTokenSource();
        var received = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var listenTask = Task.Run(() => listen(payload =>
        {
            received.TrySetResult(payload);
            return Task.CompletedTask;
        }, cts.Token));

        await EventuallyAsync(async () =>
        {
            await NotifyAsync(dataSource, notificationChannel, "manual-payload");
            return received.Task.IsCompleted;
        });

        Assert.Equal("manual-payload", await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        await cts.CancelAsync();
        await IgnoreCancellationAsync(listenTask);
    }

    private static async Task AssertTransportListenReceivesAsync(
        PostgreSqlTransportStore store,
        Func<Task> publish)
    {
        using var cts = new CancellationTokenSource();
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var listenTask = Task.Run(() => store.ExecuteListenAsync(() =>
        {
            received.TrySetResult();
            return Task.CompletedTask;
        }, cts.Token));

        await EventuallyAsync(async () =>
        {
            await publish();
            return received.Task.IsCompleted;
        });
        await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await cts.CancelAsync();
        await IgnoreCancellationAsync(listenTask);
    }

    private static async Task NotifyAsync(NpgsqlDataSource dataSource, string channel, string payload)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT pg_notify(@channel, @payload);";
        command.Parameters.AddWithValue("channel", channel);
        command.Parameters.AddWithValue("payload", payload);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<bool> AckAndMatchAttemptAsync(PostgreSqlTransportDelivery delivery, int attempt)
    {
        var matched = delivery.Attempt == attempt;
        await delivery.AckAsync();
        return matched;
    }

    private static async Task DrainQueueAsync(PostgreSqlTransportStore store, string queue, TimeSpan lockTimeout)
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

    private static async Task IgnoreCancellationAsync(Task task)
    {
        try
        {
            await task.WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static ReflectionCallDto ResumeCallback() => new()
    {
        ServiceInterfaceFullName = typeof(IDirectPostgreSqlRecoveryFlow).FullName!,
        MethodName = nameof(IDirectPostgreSqlRecoveryFlow.ResumeAsync),
        Params =
        [
            CallbackParam.ForPlaceholder(PlaceholderType.Payload),
            CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId)
        ]
    };

    private static ReflectionCallDto FailureCallback() => new()
    {
        ServiceInterfaceFullName = typeof(IDirectPostgreSqlRecoveryFlow).FullName!,
        MethodName = nameof(IDirectPostgreSqlRecoveryFlow.FailAsync),
        Params =
        [
            CallbackParam.ForPlaceholder(PlaceholderType.Exception),
            CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId)
        ]
    };

    // The longest suffix any derived identifier appends to a schema name below ("_transport").
    private const int LongestDerivedSuffix = 10;

    private static string NewSchema(string prefix)
    {
        // PostgreSQL truncates identifiers past 63 characters, so the options validators reject an
        // overlong derived name at construction — asserting here names the too-long PREFIX instead
        // of failing the test with a validator message about a name it never chose.
        var schema = $"ar_{prefix}_{Guid.NewGuid():N}";
        Assert.True(
            schema.Length + LongestDerivedSuffix <= 63,
            $"Schema prefix '{prefix}' is too long: '{schema}' plus a derived suffix exceeds PostgreSQL's 63-character identifier limit.");
        return schema;
    }

    private static string Quote(string identifier) => "\"" + identifier + "\"";

    private static string SuccessEnvelope(string message)
        => $$"""{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"{{message}}"},"ExceptionMessage":null,"ExceptionStackTrace":null}""";

    [Fact]
    public async Task PostgreSqlAsyncResponseChannel_CoverInternalEdgeCases()
    {
        await WithDataSourceAsync("channel_edges", async (schema, dataSource) =>
        {
            var services = new ServiceCollection();
            services.AddLogging();
            var options = ChannelOptions(schema);
            options.DeliveryConfirmationTimeout = TimeSpan.FromMilliseconds(10);
            options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(5);
            
            services.AddSingleton(Options.Create(options));
            var sql = new PostgreSqlChannelSql(dataSource, Options.Create(options));
            services.AddSingleton(sql);
            services.AddSingleton(MockRecoveryStore());
            services.AddSingleton(new AsyncResponseContextPropagation([]));
            services.AddSingleton<PostgreSqlAsyncResponseChannel>();
            
            await using var provider = services.BuildServiceProvider();
            var channel = provider.GetRequiredService<PostgreSqlAsyncResponseChannel>();
            
            // Cover EnsureCreatedAsync double call (first/second return)
            await sql.EnsureCreatedAsync();
            await sql.EnsureCreatedAsync();

            // Cover HeartbeatSubscribersAsync empty collection fast return
            await sql.HeartbeatSubscribersAsync("instance", [], TimeSpan.FromMinutes(1), CancellationToken.None);

            var subscription1 = Subscription(typeof(PostgreSqlAsyncResponseChannel), "DbSubscription`1", channel, _ => new ValueTask<bool>(true));
            var subscription2 = Subscription(typeof(PostgreSqlAsyncResponseChannel), "DbSubscription`1", channel, _ => new ValueTask<bool>(true));
            var subscription3 = Subscription(typeof(PostgreSqlAsyncResponseChannel), "DbSubscription`1", channel, _ => new ValueTask<bool>(true));
            
            SetField(subscription1.Instance, "_dropped", true);

            var addSubMethod = typeof(PostgreSqlAsyncResponseChannel).GetMethod("AddSubscription", BindingFlags.Instance | BindingFlags.NonPublic)!;
            addSubMethod.Invoke(channel, ["corr", subscription1.Instance]);
            addSubMethod.Invoke(channel, ["corr", subscription2.Instance]);
            addSubMethod.Invoke(channel, ["corr", subscription3.Instance]);

            // 1. Cover DispatchPendingMessagesAsync where subscriptions.Count == 0
            var channelClean = provider.GetRequiredService<PostgreSqlAsyncResponseChannel>();
            var subsField = typeof(PostgreSqlAsyncResponseChannel).GetField("_subscriptions", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var subsDict = (System.Collections.IDictionary)subsField.GetValue(channelClean)!;
            subsDict.Clear();
            
            addSubMethod.Invoke(channelClean, ["corr-dropped-only", subscription1.Instance]);
            
            var dispatchPendingMethod = typeof(PostgreSqlAsyncResponseChannel).GetMethod("DispatchPendingMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var scope = new HashSet<string> { "corr-dropped-only" };
            await (Task)dispatchPendingMethod.Invoke(channelClean, [scope, CancellationToken.None])!;

            // 2. Cover WaitForAcknowledgementAsync break branch and pollDelay branches
            var beginConfirmationMethod = typeof(PostgreSqlAsyncResponseChannel).GetMethod("BeginConfirmation", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var tryConfirmMethod = typeof(PostgreSqlAsyncResponseChannel).GetMethod("TryConfirmDeliveryAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
            
            var messageId = Guid.NewGuid();
            await sql.InsertMessageAsync(messageId, "corr", "{}", TimeSpan.FromMinutes(1), CancellationToken.None);
            
            var confirmation = beginConfirmationMethod.Invoke(channel, [messageId])!;
            var confirmed = await (Task<bool>)tryConfirmMethod.Invoke(channel, [confirmation, CancellationToken.None])!;
            // The window lapsed with the message never acked: the poll loop must report
            // non-delivery, not merely terminate.
            Assert.False(confirmed);

            // 3. Cover DispatchMessageToSubscribersAsync continue branch (dropped & seen & live)
            var messageId2 = Guid.NewGuid();
            await sql.InsertMessageAsync(messageId2, "corr", "{}", TimeSpan.FromMinutes(1), CancellationToken.None);

            subscription2.Instance.GetType().GetMethod("MarkSeen", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!
                .Invoke(subscription2.Instance, [messageId2]);

            var message2 = new PostgreSqlChannelMessage(messageId2, "corr", "{}", DateTimeOffset.UtcNow);
            var dispatchMethod = typeof(PostgreSqlAsyncResponseChannel).GetMethod("DispatchMessageToSubscribersAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
            
            var subInterfaceType = typeof(PostgreSqlAsyncResponseChannel).BaseType!.GetNestedType("IDbSubscription", BindingFlags.NonPublic)!;
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
            // The converter's own contract violations stay a plain JsonException AND keep their
            // message: JsonSafety's body-free scrub (round 36) skips the failures the library
            // authored, which name only the contract's own properties, and replaces only the
            // reader's own — whose messages quote the inbound body.
            var liveError = await Assert.ThrowsAsync<System.Text.Json.JsonException>(
                () => subscription3.Completion.Task.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.Contains("SchemaVersion", liveError.Message);

            // Start listener to cover background task dispose/cancellation
            var ensureListenerStartedMethod = typeof(PostgreSqlAsyncResponseChannel).GetMethod("EnsureListenerStarted", BindingFlags.Instance | BindingFlags.NonPublic)!;
            ensureListenerStartedMethod.Invoke(channel, null);

            await channel.DisposeAsync();
            await channelClean.DisposeAsync();
        });
    }

    /// <summary>
    /// Regression (round 33): a publish RETRY — the same message id landing as an idempotent
    /// duplicate (<c>ON CONFLICT DO NOTHING</c>) after another process had already claimed and
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
        await WithDataSourceAsync("retry_acked", async (schema, dataSource) =>
        {
            var options = ChannelOptions(schema);
            var sql = new PostgreSqlChannelSql(dataSource, Options.Create(options));
            await using var channel = new PostgreSqlAsyncResponseChannel(
                new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(),
                sql,
                MockRecoveryStore(),
                Options.Create(options),
                new AsyncResponseContextPropagation([]),
                NullLogger<PostgreSqlAsyncResponseChannel>.Instance);
            await sql.EnsureCreatedAsync();

            // First attempt: persisted, then claimed and acked by "another process".
            var messageId = Guid.NewGuid();
            await sql.InsertMessageAsync(messageId, "corr", SuccessEnvelope("consumed"), TimeSpan.FromSeconds(30), CancellationToken.None);
            Assert.True(await sql.TryClaimForDeliveryAsync(messageId, CancellationToken.None));

            // A waiter registering AFTER that ack, stamped on the server clock the watermark
            // compares against — exactly what CreateResponseWaiter draws.
            var (startedAt, startedSeq) = await sql.GetSubscriptionStartAsync(CancellationToken.None);
            var late = Subscription(typeof(PostgreSqlAsyncResponseChannel), "DbSubscription`1", channel, _ => new ValueTask<bool>(true), startedAt, startedSeq);
            InheritedMethod(typeof(PostgreSqlAsyncResponseChannel), "AddSubscription").Invoke(channel, ["corr", late.Instance]);

            // The retry lands through the channel's publish path with the SAME message id.
            await (Task)InheritedMethod(typeof(PostgreSqlAsyncResponseChannel), "PublishMessageAsync")
                .Invoke(channel, [messageId, "corr", SuccessEnvelope("retry"), CancellationToken.None])!;
            await DrainLocalDispatchAsync(typeof(PostgreSqlAsyncResponseChannel), channel, "corr");

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
        await WithDataSourceAsync("dup_settled", async (schema, dataSource) =>
        {
            var sql = new PostgreSqlChannelSql(dataSource, Options.Create(ChannelOptions(schema)));
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

    private interface IDirectPostgreSqlRecoveryFlow
    {
        Task ResumeAsync(OperationResult payload, string correlationId);
        Task FailAsync(Exception exception, string correlationId);
    }

    private sealed class DirectRecoveryFlow : IDirectPostgreSqlRecoveryFlow
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
