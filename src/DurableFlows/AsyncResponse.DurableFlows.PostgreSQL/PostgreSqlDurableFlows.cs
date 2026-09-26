using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.PostgreSQL;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using PostgreSqlDdlGuard = AsyncResponse.Internal.PostgreSqlDdlGuard;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>DI registration for the PostgreSQL durable-flow state store.</summary>
    public static class PostgreSqlDurableFlowServiceCollectionExtensions
    {
        /// <summary>
        /// Stores durable-flow state in PostgreSQL. Hosts may either register an
        /// <see cref="NpgsqlDataSource"/> singleton or set
        /// <see cref="PostgreSqlDurableFlowOptions.ConnectionString"/>.
        /// </summary>
        public static AsyncResponseRegistrationBuilder WithPostgreSqlDurableFlows(
            this AsyncResponseRegistrationBuilder builder,
            Action<PostgreSqlDurableFlowOptions>? configure = null)
        {
            // Singleton on purpose: schema provisioning is cached per store instance, and the
            // executor resolves the store from a fresh scope per flow execution — a scoped store
            // would re-run EnsureCreated's DDL round-trip on every run. All dependencies are
            // singletons, so the singleton is safe.
            builder.Services.TryAddSingleton(provider =>
            {
                var options = provider.GetRequiredService<IOptions<PostgreSqlDurableFlowOptions>>();

                // Reuse a host-registered NpgsqlDataSource when present; otherwise create one from
                // ConnectionString, owned (and disposed) by the store. Nothing is registered as a
                // bare NpgsqlDataSource service, so unrelated resolutions of that type are never
                // answered — or broken — by this package.
                var shared = provider.GetService<NpgsqlDataSource>();
                var logger = provider.GetService<ILogger<PostgreSqlFlowStateStore>>();
                if (shared is not null)
                    return new PostgreSqlFlowStateStore(shared, options, logger: logger);

                if (string.IsNullOrWhiteSpace(options.Value.ConnectionString))
                    throw new InvalidOperationException($"{nameof(PostgreSqlDurableFlowOptions)}.{nameof(PostgreSqlDurableFlowOptions.ConnectionString)} must be configured when no NpgsqlDataSource is registered.");
                return new PostgreSqlFlowStateStore(NpgsqlDataSource.Create(options.Value.ConnectionString), options, ownsDataSource: true, logger: logger);
            });
            return builder.WithDurableFlows<PostgreSqlFlowStateStore, PostgreSqlDurableFlowOptions>(configure);
        }
    }
}

namespace AsyncResponse.DurableFlows.PostgreSQL
{
/// <summary>Options for the PostgreSQL durable-flow state store.</summary>
public sealed class PostgreSqlDurableFlowOptions : DurableFlowOptions
{
    /// <summary>Optional PostgreSQL connection string used when no <see cref="NpgsqlDataSource"/> is registered.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Database schema that contains the durable-flow table. Default: <c>public</c>.</summary>
    public string SchemaName { get; set; } = "public";

    /// <summary>Table storing one durable-flow ledger row per flow id.</summary>
    public string TableName { get; set; } = "asyncresponse_flow_state";

    /// <summary>Creates the schema, table, and expiry index on first use.</summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// How often <see cref="PostgreSqlFlowStateStore.TryCreateAsync"/> opportunistically runs a
    /// budgeted prune of expired rows: batches of 1000 until one comes back short or
    /// <see cref="PruneBudget"/> lapses (loads already treat expired state as absent; pruning
    /// bounds table growth). Zero or negative prunes on every save. Default: 5 minutes.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Wall-clock budget one opportunistic prune may spend draining expired rows in batches of
    /// 1000 after its first batch (the first always runs). A single batch per interval capped
    /// cleanup at ~3 rows/second, which any busier instance outgrew forever; the prune now drains
    /// batches until one comes back short or this budget lapses, and reports the outcome on the
    /// <c>AsyncResponse</c> meter (<c>asyncresponse.flow_state.pruned_rows</c>,
    /// <c>prune_failures</c>, <c>prune_budget_exhausted</c>) and the store's logger. The create
    /// that triggers the prune waits for it, so this bounds that create's added latency. Zero
    /// keeps the historical single batch. Default: 2 seconds.
    /// </summary>
    public TimeSpan PruneBudget { get; set; } = DurableFlowStoreShared.DefaultPruneBudget;

    /// <summary>
    /// Maximum serialized flow-state size in bytes accepted by writes; oversized ledgers fail fast
    /// with an actionable error instead of an opaque provider error. Default: <c>null</c>
    /// (unlimited — PostgreSQL <c>jsonb</c> is effectively unbounded), settable as an operator budget.
    /// </summary>
    public long? MaxStateBytes { get; set; }

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        DurableFlowStoreShared.ValidateIdentifier(SchemaName, $"{nameof(PostgreSqlDurableFlowOptions)}.{nameof(SchemaName)}", "PostgreSQL", identifierCap: 63);
        DurableFlowStoreShared.ValidateIdentifier(TableName, $"{nameof(PostgreSqlDurableFlowOptions)}.{nameof(TableName)}", "PostgreSQL", identifierCap: 63);

        // Indexes share PostgreSQL's relation namespace with tables: a table whose name ends
        // exactly where the reserved "_expires_idx" stem truncates derives its own name, and
        // CREATE INDEX IF NOT EXISTS would silently match the table and skip the index.
        if (string.Equals(DurableFlowStoreShared.DerivedName(TableName, "_expires_idx", 63), TableName, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                $"{nameof(PostgreSqlDurableFlowOptions)}.{nameof(TableName)} '{TableName}' collides with its derived expiry-index name; rename the table.");
        DurableFlowStoreShared.ValidateMaxStateBytes(MaxStateBytes, nameof(PostgreSqlDurableFlowOptions));
        DurableFlowStoreShared.ValidatePruneBudget(PruneBudget, nameof(PostgreSqlDurableFlowOptions));
    }
}

/// <summary>PostgreSQL implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class PostgreSqlFlowStateStore : IFlowStateStore, IDisposable, IAsyncDisposable
{
    private readonly ILogger<PostgreSqlFlowStateStore>? _logger;

    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgreSqlDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly bool _ownsDataSource;
    private readonly long _schemaLockKey;
    private readonly long _tableLockKey;
    private readonly PostgreSqlDdlGuard _ddl;
    private long _lastPruneTicks;
    private volatile bool _created;

    public PostgreSqlFlowStateStore(NpgsqlDataSource dataSource, IOptions<PostgreSqlDurableFlowOptions> options, bool ownsDataSource = false, ILogger<PostgreSqlFlowStateStore>? logger = null)
    {
        _dataSource = dataSource;
        _logger = logger;
        _options = options.Value;
        _options.Validate();
        _ownsDataSource = ownsDataSource;
        _schemaLockKey = DurableFlowStoreShared.SchemaLockKey(_options.SchemaName);
        _tableLockKey = PostgreSqlDdlGuard.TableLockKey(_options.SchemaName, _options.TableName);
        _ddl = new PostgreSqlDdlGuard("durable-flow", $"'{_options.SchemaName}.{_options.TableName}'", "docs/durable-flow-state-stores.md", logger);
    }

    /// <summary>The clock of the startup-DDL retry-after window (test seam; see <see cref="PostgreSqlDdlGuard.RetryAfter"/>).</summary>
    internal TimeProvider Clock
    {
        get => _ddl.Clock;
        set => _ddl.Clock = value;
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        // All expiry/lease time math in this store runs on the database clock (now()), never an
        // app-computed timestamp: with multiple workers, app clock skew beyond the lease window
        // would let two nodes both consider a lease expired and double-run a flow.
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT state_json::text, revision FROM {Table} WHERE flow_id = @flow_id AND expires_at_utc > now();";
        command.Parameters.AddWithValue("flow_id", flowId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        return DurableFlowStoreShared.ReadState(flowId, reader.GetString(0), reader.GetInt64(1));
    }

    /// <inheritdoc />
    public void ValidateCreate(string flowId, FlowState state, TimeSpan ttl)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        if (_options.MaxStateBytes is not null)
            _ = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "PostgreSQL");
    }

    public async Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "PostgreSQL");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await DurableFlowStoreShared.PruneQuietlyAsync(() => PruneExpiredAsync(cancellationToken), _options.PruneBudget, "PostgreSQL", _logger, cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {Table} (flow_id, state_json, expires_at_utc, updated_at_utc, revision)
            VALUES (@flow_id, @state_json, now() + @ttl, now(), @revision)
            ON CONFLICT (flow_id) DO UPDATE
            SET state_json = EXCLUDED.state_json,
                expires_at_utc = EXCLUDED.expires_at_utc,
                updated_at_utc = EXCLUDED.updated_at_utc,
                revision = EXCLUDED.revision,
                lease_id = NULL,
                lease_expires_at_utc = NULL
            WHERE {Table}.expires_at_utc <= now();
            """;
        command.Parameters.AddWithValue("flow_id", flowId);
        command.Parameters.Add("state_json", NpgsqlDbType.Text).Value = stateJson;
        command.Parameters.Add("ttl", NpgsqlDbType.Interval).Value = DurableFlowStoreShared.ServerClockTtl(ttl);
        command.Parameters.AddWithValue("revision", state.Revision);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async Task<bool> TryUpdateAsync(
        string flowId,
        FlowState state,
        long expectedRevision,
        TimeSpan ttl,
        string? leaseId = null,
        CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateUpdate(flowId, state, expectedRevision, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "PostgreSQL");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            UPDATE {Table}
            SET state_json = @state_json,
                expires_at_utc = now() + @ttl,
                updated_at_utc = now(),
                revision = @new_revision
            WHERE flow_id = @flow_id
              AND revision = @expected_revision
              AND expires_at_utc > now()
              AND (@lease_id IS NULL OR (lease_id = @lease_id AND lease_expires_at_utc > now()));
            """;
        command.Parameters.AddWithValue("flow_id", flowId);
        command.Parameters.Add("state_json", NpgsqlDbType.Text).Value = stateJson;
        command.Parameters.Add("ttl", NpgsqlDbType.Interval).Value = DurableFlowStoreShared.ServerClockTtl(ttl);
        command.Parameters.AddWithValue("expected_revision", expectedRevision);
        command.Parameters.AddWithValue("new_revision", state.Revision);
        command.Parameters.AddWithValue("lease_id", NpgsqlDbType.Text, (object?)leaseId ?? DBNull.Value);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: true, cancellationToken);

    public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: false, cancellationToken);

    public async Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"UPDATE {Table} SET lease_id = NULL, lease_expires_at_utc = NULL WHERE flow_id = @flow_id AND lease_id = @lease_id;";
        command.Parameters.AddWithValue("flow_id", flowId);
        command.Parameters.AddWithValue("lease_id", leaseId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<FlowLeaseObservation?> ObserveLeaseAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        // The two lease columns exactly as stored — deliberately no now() predicate, unlike every
        // other statement in this store: an expired lease nobody has taken over must keep reading
        // as the same lease, because the engine's proof of a live holder is that two observations
        // DIFFER. Whether it has lapsed stays UpdateLeaseAsync's call, on the database clock.
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT lease_id, lease_expires_at_utc FROM {Table} WHERE flow_id = @flow_id;";
        command.Parameters.AddWithValue("flow_id", flowId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return FlowLeaseObservation.Unheld;

        return DurableFlowStoreShared.LeaseObservation(
            reader.IsDBNull(0) ? null : reader.GetString(0),
            reader.IsDBNull(1) ? null : reader.GetDateTime(1));
    }

    public async Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {Table} WHERE flow_id = @flow_id;";
        command.Parameters.AddWithValue("flow_id", flowId);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    private async Task<int> PruneExpiredAsync(CancellationToken cancellationToken)
    {
        // One bounded batch per call; DurableFlowStoreShared.PruneQuietlyAsync repeats it under
        // the PruneBudget while batches come back full (policy shared by all relational stores): an
        // unbatched DELETE over a large expired backlog holds row locks and bloats one
        // transaction for the unlucky create that triggered the prune. Loads already filter on
        // expiry, so any backlog beyond the batch just waits for the next interval.
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            DELETE FROM {Table}
            WHERE ctid IN (SELECT ctid FROM {Table} WHERE expires_at_utc <= now() LIMIT {DurableFlowStoreShared.PruneBatchSize});
            """;
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task EnsureCreatedAsync(CancellationToken cancellationToken)
    {
        if (_created)
            return;

        _ddl.ThrowIfBackingOff();
        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            // Again under the gate: a caller that queued behind the attempt that just failed —
            // and latched the retry-after window — fails here instead of starting the next one.
            _ddl.ThrowIfBackingOff();

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

            if (!_options.AutoCreateSchema)
            {
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                if (!await TableExistsAsync(connection, transaction, cancellationToken).ConfigureAwait(false))
                {
                    // Operator-managed schema and the migration has not run yet: the first query
                    // surfaces a clear PostgreSQL error (the documented "create it yourself, later"
                    // workflow), and _created stays unlatched so a later operation re-verifies once
                    // the migration lands. When the relation DOES exist it flows into the same catalog
                    // verification the DDL path uses — operator-provisioned schemas are exactly what
                    // that check exists for.
                    return;
                }

                await VerifyRelationsAsync(connection, transaction, PostgreSqlDdlGuard.IndexState.Usable, cancellationToken).ConfigureAwait(false);
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                _created = true;
                return;
            }

            // The transport's round-43 hardening (PostgreSqlDdlGuard), applied here. Every DDL
            // transaction bounds its lock waits — the advisory key's first — with a lock_timeout:
            // CREATE INDEX IF NOT EXISTS takes its SHARE lock BEFORE it finds the name taken, so on
            // every process start it queued behind any conflicting holder (an operator's CREATE
            // INDEX CONCURRENTLY, an anti-wraparound vacuum of this update-heavy table, an
            // idle-in-transaction checkpoint writer), with every checkpoint, lease acquire and
            // renewal of every host queued behind it until the 30 s command timeout — and the next
            // operation did it again. Only a missing index is built now, and the one-time rewrite
            // runs under the long-running command timeout, in its own transaction.
            try
            {
                // 1. The schema-shared DDL, under the schema's advisory key (shared with the
                //    channel and transport stores): creates only, none of which locks a table that
                //    already exists. Serializes creation across processes — CREATE ... IF NOT EXISTS
                //    is not atomic against a concurrent create of the same object: two instances
                //    starting together both pass the existence check and collide on the system
                //    catalog ("duplicate key ... pg_type_typname_nsp_index").
                await using (var transaction = await PostgreSqlDdlGuard.BeginLockedTransactionAsync(connection, _schemaLockKey, cancellationToken).ConfigureAwait(false))
                {
                    await using (var command = connection.CreateCommand())
                    {
                        command.Transaction = transaction;
                        command.CommandText =
                            $"""
                            CREATE SCHEMA IF NOT EXISTS {Schema};
                            CREATE TABLE IF NOT EXISTS {Table} (
                                flow_id text NOT NULL PRIMARY KEY,
                                state_json text NOT NULL,
                                expires_at_utc timestamptz NOT NULL,
                                updated_at_utc timestamptz NOT NULL,
                                revision bigint NOT NULL DEFAULT 0,
                                lease_id text NULL,
                                lease_expires_at_utc timestamptz NULL
                            );
                            """;
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }

                    var (stateJsonb, index) = await ReadTableWorkAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
                    if (!stateJsonb && index is not PostgreSqlDdlGuard.IndexState.Absent)
                    {
                        await VerifyRelationsAsync(connection, transaction, index, cancellationToken).ConfigureAwait(false);
                        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                        _created = true;
                        return;
                    }

                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                }

                // 2. The table work — the one-time jsonb rewrite, the index build on an existing
                //    table — in a transaction of its own under a key scoped to this table: a rewrite
                //    held for up to an hour under the schema-wide key would stop every host starting
                //    meanwhile from initializing ANY AsyncResponse store on the schema. Re-read under
                //    the key: another host may have done the work while this one waited for it.
                await using (var transaction = await PostgreSqlDdlGuard.BeginLockedTransactionAsync(connection, _tableLockKey, cancellationToken).ConfigureAwait(false))
                {
                    var (stateJsonb, index) = await ReadTableWorkAsync(connection, transaction, cancellationToken).ConfigureAwait(false);

                    // jsonb REJECTS the \u0000 escape System.Text.Json emits for U+0000 (SQLSTATE
                    // 22P05), so a ledger every other store accepts failed every write here: the
                    // flow could not start, or its checkpoint failed on every attempt until the
                    // job dead-lettered. Nothing queries INSIDE the ledger (it is read back with
                    // ::text), so text costs nothing. The rewrite runs once, under ACCESS EXCLUSIVE.
                    if (stateJsonb)
                    {
                        await _ddl.ExecuteLongRunningAsync(
                            $"ALTER TABLE {Table} ALTER COLUMN state_json TYPE text USING state_json::text;", connection, transaction, cancellationToken).ConfigureAwait(false);
                    }

                    if (index is PostgreSqlDdlGuard.IndexState.Absent)
                    {
                        await _ddl.ExecuteLongRunningAsync(
                            $"CREATE INDEX IF NOT EXISTS {IndexName} ON {Table} (expires_at_utc);", connection, transaction, cancellationToken).ConfigureAwait(false);
                        index = PostgreSqlDdlGuard.IndexState.Usable;
                    }

                    await VerifyRelationsAsync(connection, transaction, index, cancellationToken).ConfigureAwait(false);
                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                    _created = true;
                }
            }
            catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.WrongObjectType or PostgresErrorCodes.UndefinedColumn)
            {
                // E.g. CREATE INDEX ... ON a name that is really another component's index:
                // IF NOT EXISTS skipped the table create, and the dependent statement then hits
                // the wrong relation kind — surface the namespace collision instead of the raw
                // "cannot open relation".
                throw new InvalidOperationException(AsyncResponse.Internal.PostgreSqlRelationVerifier.DdlCollisionMessage("durable-flow", _options.SchemaName), ex);
            }
            catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.LockNotAvailable)
            {
                // A lock wait lost anywhere in the DDL — the advisory key's included — latches the
                // retry-after window (a failed long-running step latched it already).
                _ddl.BackOff(ex);
                throw;
            }
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    /// <summary>
    /// The table work the auto-create DDL still owes: whether <c>state_json</c> still carries the
    /// pre-text <c>jsonb</c> type, and the expiry index's state (only an absent one is built).
    /// </summary>
    private async Task<(bool StateJsonb, PostgreSqlDdlGuard.IndexState Index)> ReadTableWorkAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        CancellationToken cancellationToken)
    {
        bool stateJsonb;
        await using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText =
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = @schema AND table_name = @table AND column_name = 'state_json' AND data_type = 'jsonb');
                """;
            command.Parameters.AddWithValue("schema", _options.SchemaName);
            command.Parameters.AddWithValue("table", _options.TableName);
            stateJsonb = (bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }

        var index = await PostgreSqlDdlGuard.GetIndexStateAsync(
            connection,
            transaction,
            _options.SchemaName,
            DurableFlowStoreShared.DerivedName(_options.TableName, "_expires_idx", 63),
            cancellationToken).ConfigureAwait(false);
        return (stateJsonb, index);
    }

    /// <summary>
    /// The flow store can share a schema with the channel and transport stores (and unrelated
    /// objects), whose derived names its own validation cannot see — and IF NOT EXISTS also accepts
    /// a same-name index with the WRONG definition, exactly as an operator-provisioned table can
    /// carry the wrong shape. Verify against the catalog that both relations actually ARE what this
    /// store reads and writes, definitions included (in the DDL transaction, under its advisory key,
    /// when this build just ran the DDL).
    /// </summary>
    /// <remarks>
    /// An expiry index that exists but is not valid and ready — an operator's
    /// <c>CREATE INDEX CONCURRENTLY</c> still running, or one that failed — is prune performance
    /// lost, exactly like an absent one, on either kind of schema (the DDL builds only an absent
    /// index, so it leaves such a one alone): it is reported and warned about, never verified as
    /// required. Verified as present, it failed every flow operation on every host that started
    /// during the build, and forever after a failed one.
    /// </remarks>
    private async Task VerifyRelationsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        PostgreSqlDdlGuard.IndexState index,
        CancellationToken cancellationToken)
    {
        var expected = ExpectedRelations(_options);
        if (index is PostgreSqlDdlGuard.IndexState.NotReady)
            expected[^1] = expected[^1] with { Optional = true };

        var (absent, notReady) = await AsyncResponse.Internal.PostgreSqlRelationVerifier.VerifyAsync(
            connection,
            transaction,
            _options.SchemaName,
            "durable-flow",
            expected,
            cancellationToken).ConfigureAwait(false);
        if (_logger is not { } logger)
            return;

        // The verifier matches the index by NAME, so all this knows is that {table}_expires_idx
        // is absent — an operator's migration may carry the same index under another name.
        foreach (var name in absent)
        {
            SafeLog.Try(() => logger.LogWarning(
                "PostgreSQL durable-flow table {Schema}.{Table} has no index named {Index} and AutoCreateSchema is disabled. " +
                "If an index on (expires_at_utc) exists under another name, prunes use it and nothing needs doing; otherwise " +
                "each prune batch scans for expired rows — performance only; create one as described in docs/durable-flow-state-stores.md.",
                _options.SchemaName,
                _options.TableName,
                name));
        }

        foreach (var name in notReady)
        {
            SafeLog.Try(() => logger.LogWarning(
                "PostgreSQL durable-flow index {Schema}.{Index} exists but is not valid and ready (a CREATE INDEX CONCURRENTLY " +
                "still running, or one that failed). Flows still run, but each prune batch scans for expired rows until it is " +
                "usable — performance only; if its build failed, drop it and create it again as described in " +
                "docs/durable-flow-state-stores.md.",
                _options.SchemaName,
                name));
        }
    }

    /// <summary>
    /// The catalog shape this store needs. The expiry index is REQUIRED only where this build's DDL
    /// just guaranteed it (<see cref="PostgreSqlDurableFlowOptions.AutoCreateSchema"/>); on an
    /// operator-managed schema it is verified when present and only warned about when absent —
    /// it is prune performance, not correctness (loads filter on expiry either way), and a table
    /// whose migration tool named or omitted it differently must not fail every operation.
    /// <c>revision</c> declares no expected default: every insert names the column, so no default
    /// is load-bearing, and requiring the DDL's <c>DEFAULT 0</c> refused an operator table that
    /// declares <c>bigint NOT NULL</c> without one.
    /// </summary>
    internal static AsyncResponse.Internal.PostgreSqlRelationVerifier.ExpectedRelation[] ExpectedRelations(PostgreSqlDurableFlowOptions options)
        =>
        [
            new(options.TableName, 'r', Columns:
                [
                    new("flow_id", "text", Nullable: false, RequiresDeterministicCollation: true),
                    new("state_json", "text", Nullable: false),
                    new("expires_at_utc", "timestamp with time zone", Nullable: false),
                    new("updated_at_utc", "timestamp with time zone", Nullable: false),
                    new("revision", "bigint", Nullable: false),
                    new("lease_id", "text", Nullable: true),
                    new("lease_expires_at_utc", "timestamp with time zone", Nullable: true),
                ], PrimaryKey: ["flow_id"]),
            new(DurableFlowStoreShared.DerivedName(options.TableName, "_expires_idx", 63), 'i', options.TableName, ["expires_at_utc"])
            {
                Optional = !options.AutoCreateSchema
            },
        ];

    /// <summary>
    /// Reports whether ANY relation occupies the configured name (any relkind: a view or foreign
    /// component's object must reach verification, which names the precise wrong-kind reason
    /// instead of skipping the checks).
    /// </summary>
    private async Task<bool> TableExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = @schema AND c.relname = @table);
            """;
        command.Parameters.AddWithValue("schema", _options.SchemaName);
        command.Parameters.AddWithValue("table", _options.TableName);
        return (bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
    }


    private async Task<bool> UpdateLeaseAsync(
        string flowId,
        string leaseId,
        TimeSpan leaseDuration,
        bool acquire,
        CancellationToken cancellationToken)
    {
        DurableFlowStoreShared.ValidateLeaseArgs(flowId, leaseId, leaseDuration);

        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // Lease fencing runs entirely on the database clock: acquire steals only leases the
        // database considers expired, and renew/extend stays relative to now(), so worker clock
        // skew can never make two nodes hold the same lease.
        command.CommandText =
            $"""
            UPDATE {Table}
            SET lease_id = @lease_id, lease_expires_at_utc = now() + @lease_duration
            WHERE flow_id = @flow_id
              AND expires_at_utc > now()
              AND {(acquire ? "(lease_id IS NULL OR lease_expires_at_utc <= now() OR lease_id = @lease_id)" : "lease_id = @lease_id AND lease_expires_at_utc > now()")};
            """;
        command.Parameters.AddWithValue("flow_id", flowId);
        command.Parameters.AddWithValue("lease_id", leaseId);
        command.Parameters.Add("lease_duration", NpgsqlDbType.Interval).Value = DurableFlowStoreShared.ServerClockTtl(leaseDuration);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    /// <summary>Disposes the data source when the store created (and therefore owns) it.</summary>
    public void Dispose()
    {
        _ensureGate.Dispose();
        if (_ownsDataSource)
            _dataSource.Dispose();
    }

    /// <inheritdoc cref="Dispose" />
    public async ValueTask DisposeAsync()
    {
        _ensureGate.Dispose();
        if (_ownsDataSource)
            await _dataSource.DisposeAsync().ConfigureAwait(false);
    }

    private string Schema => Quote(_options.SchemaName);
    private string Table => $"{Schema}.{Quote(_options.TableName)}";
    private string IndexName => Quote(DurableFlowStoreShared.DerivedName(_options.TableName, "_expires_idx", 63));
    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
}
