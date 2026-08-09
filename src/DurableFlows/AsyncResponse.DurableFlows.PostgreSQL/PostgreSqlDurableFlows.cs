using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.PostgreSQL;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;

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
                if (shared is not null)
                    return new PostgreSqlFlowStateStore(shared, options);

                if (string.IsNullOrWhiteSpace(options.Value.ConnectionString))
                    throw new InvalidOperationException($"{nameof(PostgreSqlDurableFlowOptions)}.{nameof(PostgreSqlDurableFlowOptions.ConnectionString)} must be configured when no NpgsqlDataSource is registered.");
                return new PostgreSqlFlowStateStore(NpgsqlDataSource.Create(options.Value.ConnectionString), options, ownsDataSource: true);
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
    /// How often <see cref="PostgreSqlFlowStateStore.TryCreateAsync"/> opportunistically deletes one
    /// bounded batch (1000 rows) of expired rows (loads already treat expired state as absent;
    /// pruning bounds table growth). Zero or negative prunes on every save. Default: 5 minutes.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromMinutes(5);

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
        if (MaxStateBytes is <= 0)
            throw new InvalidOperationException($"{nameof(PostgreSqlDurableFlowOptions)}.{nameof(MaxStateBytes)} must be positive when configured.");
    }
}

/// <summary>PostgreSQL implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class PostgreSqlFlowStateStore : IFlowStateStore, IDisposable, IAsyncDisposable
{
    private const int PruneBatchSize = 1000;

    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgreSqlDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly bool _ownsDataSource;
    private readonly long _schemaLockKey;
    private long _lastPruneTicks;
    private bool _created;

    public PostgreSqlFlowStateStore(NpgsqlDataSource dataSource, IOptions<PostgreSqlDurableFlowOptions> options, bool ownsDataSource = false)
    {
        _dataSource = dataSource;
        _options = options.Value;
        _options.Validate();
        _ownsDataSource = ownsDataSource;
        _schemaLockKey = DurableFlowStoreShared.SchemaLockKey(_options.SchemaName);
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

    public async Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "PostgreSQL");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await PruneExpiredAsync(cancellationToken).ConfigureAwait(false);

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
        command.Parameters.Add("state_json", NpgsqlDbType.Jsonb).Value = stateJson;
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
        command.Parameters.Add("state_json", NpgsqlDbType.Jsonb).Value = stateJson;
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

    private async Task PruneExpiredAsync(CancellationToken cancellationToken)
    {
        // One bounded batch per prune interval (policy shared by all relational stores): an
        // unbatched DELETE over a large expired backlog holds row locks and bloats one
        // transaction for the unlucky create that triggered the prune. Loads already filter on
        // expiry, so any backlog beyond the batch just waits for the next interval.
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            DELETE FROM {Table}
            WHERE ctid IN (SELECT ctid FROM {Table} WHERE expires_at_utc <= now() LIMIT {PruneBatchSize});
            """;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task EnsureCreatedAsync(CancellationToken cancellationToken)
    {
        if (_created || !_options.AutoCreateSchema)
            return;

        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            // Serialize schema creation across processes. CREATE ... IF NOT EXISTS is not atomic
            // against a concurrent create of the same object: two instances starting together both
            // pass the existence check and collide on the system catalog ("duplicate key ...
            // pg_type_typname_nsp_index"). The transaction-scoped advisory lock (keyed by schema,
            // shared with the channel/transport packages) lets one instance build the schema while
            // the rest wait and then find it already present.
            await using (var lockCommand = connection.CreateCommand())
            {
                lockCommand.Transaction = transaction;
                lockCommand.CommandText = "SELECT pg_advisory_xact_lock(@lock_key);";
                lockCommand.Parameters.AddWithValue("lock_key", _schemaLockKey);
                await lockCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText =
                $"""
                CREATE SCHEMA IF NOT EXISTS {Schema};
                CREATE TABLE IF NOT EXISTS {Table} (
                    flow_id text NOT NULL PRIMARY KEY,
                    state_json jsonb NOT NULL,
                    expires_at_utc timestamptz NOT NULL,
                    updated_at_utc timestamptz NOT NULL,
                    revision bigint NOT NULL DEFAULT 0,
                    lease_id text NULL,
                    lease_expires_at_utc timestamptz NULL
                );
                CREATE INDEX IF NOT EXISTS {IndexName} ON {Table} (expires_at_utc);
                """;
            try
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.WrongObjectType)
            {
                // E.g. CREATE INDEX ... ON a name that is really another component's index:
                // IF NOT EXISTS skipped the table create, and the dependent statement then hits
                // the wrong relation kind mid-batch — surface the namespace collision instead of
                // the raw "cannot open relation".
                throw new InvalidOperationException(
                    $"The PostgreSQL durable-flow store could not create its schema objects in '{_options.SchemaName}' because a configured or " +
                    "derived name is occupied by a different kind of relation. Tables, indexes, and sequences share one namespace per " +
                    "schema — across the channel, transport, and durable-flow stores and any unrelated objects in it. Rename the " +
                    "configured table so every configured and derived name stays unique within the schema.", ex);
            }

            // The flow store can share a schema with the channel and transport stores (and
            // unrelated objects), whose derived names its own validation cannot see. Verify
            // against the catalog, in-transaction under the shared DDL lock, that both relations
            // actually ARE what the DDL above intended — CREATE ... IF NOT EXISTS matches ANY
            // relation with the name and silently skips creation otherwise.
            await VerifyCreatedRelationsAsync(
                connection,
                transaction,
                _options.SchemaName,
                [
                    (_options.TableName, 'r', null),
                    (DurableFlowStoreShared.DerivedName(_options.TableName, "_expires_idx", 63), 'i', _options.TableName),
                ],
                cancellationToken).ConfigureAwait(false);

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    /// <summary>
    /// In-transaction catalog check that each expected relation exists with the expected kind
    /// (and, for the index, on the expected table). Runs under the schema-keyed advisory DDL
    /// lock shared by every AsyncResponse PostgreSQL store. Mirrors the channel/transport
    /// stores' check (separate packages cannot share the code).
    /// </summary>
    private static async Task VerifyCreatedRelationsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string schemaName,
        (string Name, char Kind, string? OwningTable)[] expected,
        CancellationToken cancellationToken)
    {
        await using var verify = connection.CreateCommand();
        verify.Transaction = transaction;
        verify.CommandText =
            """
            SELECT c.relname, c.relkind::text, COALESCE(t.relname, '')
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_index i ON i.indexrelid = c.oid
            LEFT JOIN pg_class t ON t.oid = i.indrelid
            WHERE n.nspname = @schema AND c.relname = ANY(@names);
            """;
        verify.Parameters.AddWithValue("schema", schemaName);
        verify.Parameters.AddWithValue("names", expected.Select(e => e.Name).ToArray());

        var actual = new Dictionary<string, (string Kind, string OwningTable)>(StringComparer.Ordinal);
        await using (var reader = await verify.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                actual[reader.GetString(0)] = (reader.GetString(1), reader.GetString(2));
        }

        foreach (var (name, kind, owningTable) in expected)
        {
            if (!actual.TryGetValue(name, out var relation))
                throw new InvalidOperationException(
                    $"The PostgreSQL durable-flow store expected '{schemaName}.{name}' to exist after schema creation, but it does not.");

            if (relation.Kind != kind.ToString()
                || (owningTable is not null && !string.Equals(relation.OwningTable, owningTable, StringComparison.Ordinal)))
            {
                var expectedDescription = owningTable is null ? "a table" : $"an index on '{owningTable}'";
                var actualDescription = relation.OwningTable.Length == 0
                    ? relation.Kind switch { "r" => "a table", "i" => "an index", "S" => "a sequence", _ => $"a relation of kind '{relation.Kind}'" }
                    : $"an index on '{relation.OwningTable}'";
                throw new InvalidOperationException(
                    $"The PostgreSQL durable-flow store expected '{schemaName}.{name}' to be {expectedDescription}, " +
                    $"but the name is occupied by {actualDescription}, so CREATE ... IF NOT EXISTS silently skipped creating it. " +
                    "Tables, indexes, and sequences share one namespace per schema — across the channel, transport, and " +
                    "durable-flow stores and any unrelated objects in it. Rename the configured table so every configured " +
                    "and derived name stays unique within the schema.");
            }
        }
    }

    private async Task<bool> UpdateLeaseAsync(
        string flowId,
        string leaseId,
        TimeSpan leaseDuration,
        bool acquire,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseId);
        if (leaseDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(leaseDuration));

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
