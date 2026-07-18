using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>DI registration for the SQLite durable-flow state store.</summary>
    public static class SqliteDurableFlowServiceCollectionExtensions
    {
        /// <summary>Stores durable-flow state in SQLite.</summary>
        public static AsyncResponseRegistrationBuilder WithSqliteDurableFlows(
            this AsyncResponseRegistrationBuilder builder,
            Action<SqliteDurableFlowOptions>? configure = null)
        {
            // Singleton on purpose: schema provisioning is cached per store instance, and the
            // executor resolves the store from a fresh scope per flow execution — a scoped store
            // would re-run EnsureCreated's DDL round-trip on every run.
            builder.Services.TryAddSingleton<SqliteFlowStateStore>();
            return builder.WithDurableFlows<SqliteFlowStateStore, SqliteDurableFlowOptions>(configure);
        }
    }
}

namespace AsyncResponse.DurableFlows.Sqlite
{
/// <summary>Options for the SQLite durable-flow state store.</summary>
public sealed class SqliteDurableFlowOptions : DurableFlowOptions
{
    /// <summary>SQLite connection string. Default: <c>Data Source=asyncresponse-flow-state.db</c>.</summary>
    public string ConnectionString { get; set; } = "Data Source=asyncresponse-flow-state.db";

    /// <summary>Table storing one durable-flow ledger row per flow id.</summary>
    public string TableName { get; set; } = "asyncresponse_flow_state";

    /// <summary>Creates the table and expiry index on first use.</summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// How often <see cref="SqliteFlowStateStore.TryCreateAsync"/> opportunistically deletes expired rows
    /// (loads already treat expired state as absent; pruning bounds table growth). Zero or negative
    /// prunes on every save. Default: 5 minutes.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new InvalidOperationException($"{nameof(SqliteDurableFlowOptions)}.{nameof(ConnectionString)} must be configured.");

        DurableFlowStoreShared.ValidateIdentifier(TableName, $"{nameof(SqliteDurableFlowOptions)}.{nameof(TableName)}", "SQLite");
    }
}

/// <summary>SQLite implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class SqliteFlowStateStore : IFlowStateStore
{
    private readonly SqliteDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private long _lastPruneTicks;
    private bool _created;

    public SqliteFlowStateStore(IOptions<SqliteDurableFlowOptions> options)
    {
        _options = options.Value;
        _options.Validate();
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT state_json, revision
            FROM {Table}
            WHERE flow_id = $flow_id AND expires_at_utc > $now_utc;
            """;
        command.Parameters.AddWithValue("$flow_id", flowId);
        command.Parameters.AddWithValue("$now_utc", DateTime.UtcNow);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        var state = DurableFlowStoreShared.Deserialize(reader.GetString(0));
        return state?.Revision == reader.GetInt64(1) ? state : null;
    }

    public async Task<bool> TryCreateAsync(
        string flowId,
        FlowState state,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await PruneExpiredAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {Table} (flow_id, state_json, expires_at_utc, updated_at_utc, revision)
            VALUES ($flow_id, $state_json, $expires_at_utc, $now_utc, $revision)
            ON CONFLICT(flow_id) DO UPDATE SET
                state_json = excluded.state_json,
                expires_at_utc = excluded.expires_at_utc,
                updated_at_utc = excluded.updated_at_utc,
                revision = excluded.revision,
                lease_id = NULL,
                lease_expires_at_utc = NULL
            WHERE {Table}.expires_at_utc <= $now_utc;
            """;
        var now = DateTime.UtcNow;
        command.Parameters.AddWithValue("$flow_id", flowId);
        command.Parameters.AddWithValue("$state_json", DurableFlowStoreShared.Serialize(state));
        command.Parameters.AddWithValue("$expires_at_utc", now.Add(ttl));
        command.Parameters.AddWithValue("$now_utc", now);
        command.Parameters.AddWithValue("$revision", state.Revision);
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
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        var now = DateTime.UtcNow;
        command.CommandText =
            $"""
            UPDATE {Table}
            SET state_json = $state_json,
                expires_at_utc = $expires_at_utc,
                updated_at_utc = $updated_at_utc,
                revision = $new_revision
            WHERE flow_id = $flow_id
              AND revision = $expected_revision
              AND expires_at_utc > $now_utc
              AND ($lease_id IS NULL OR (lease_id = $lease_id AND lease_expires_at_utc > $now_utc));
            """;
        command.Parameters.AddWithValue("$flow_id", flowId);
        command.Parameters.AddWithValue("$state_json", DurableFlowStoreShared.Serialize(state));
        command.Parameters.AddWithValue("$expires_at_utc", now.Add(ttl));
        command.Parameters.AddWithValue("$updated_at_utc", now);
        command.Parameters.AddWithValue("$new_revision", state.Revision);
        command.Parameters.AddWithValue("$expected_revision", expectedRevision);
        command.Parameters.AddWithValue("$now_utc", now);
        command.Parameters.AddWithValue("$lease_id", (object?)leaseId ?? DBNull.Value);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, renew: false, cancellationToken);

    public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, renew: true, cancellationToken);

    public async Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"UPDATE {Table} SET lease_id = NULL, lease_expires_at_utc = NULL WHERE flow_id = $flow_id AND lease_id = $lease_id;";
        command.Parameters.AddWithValue("$flow_id", flowId);
        command.Parameters.AddWithValue("$lease_id", leaseId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {Table} WHERE flow_id = $flow_id;";
        command.Parameters.AddWithValue("$flow_id", flowId);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    private async Task PruneExpiredAsync(CancellationToken cancellationToken)
    {
        // Timestamps are stored as ISO-8601 TEXT, which compares correctly lexicographically.
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {Table} WHERE expires_at_utc <= $now_utc;";
        command.Parameters.AddWithValue("$now_utc", DateTime.UtcNow);
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

            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"""
                -- WAL is the right journal mode for this store's use case (concurrent flow
                -- executors on one node): readers never block behind a writer, which rollback
                -- journal mode does not guarantee — concurrent load/save storms on slow disks
                -- surface as SQLITE_BUSY 'database is locked' there. The mode is persistent in
                -- the database file, so setting it alongside the schema costs nothing per
                -- operation. Manually-provisioned databases (AutoCreateSchema=false) should set
                -- it themselves — see docs/durable-flow-state-stores.md.
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS {Table} (
                    flow_id TEXT NOT NULL PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    expires_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 0,
                    lease_id TEXT NULL,
                    lease_expires_at_utc TEXT NULL
                );
                CREATE INDEX IF NOT EXISTS {IndexName} ON {Table} (expires_at_utc);
                """;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    private async Task<bool> UpdateLeaseAsync(
        string flowId,
        string leaseId,
        TimeSpan leaseDuration,
        bool renew,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseId);
        if (leaseDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(leaseDuration));

        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        var now = DateTime.UtcNow;
        command.CommandText =
            $"""
            UPDATE {Table}
            SET lease_id = $lease_id, lease_expires_at_utc = $lease_expires_at_utc
            WHERE flow_id = $flow_id
              AND expires_at_utc > $now_utc
              AND {(renew ? "lease_id = $lease_id AND lease_expires_at_utc > $now_utc" : "(lease_id IS NULL OR lease_expires_at_utc <= $now_utc OR lease_id = $lease_id)")};
            """;
        command.Parameters.AddWithValue("$flow_id", flowId);
        command.Parameters.AddWithValue("$lease_id", leaseId);
        command.Parameters.AddWithValue("$lease_expires_at_utc", now.Add(leaseDuration));
        command.Parameters.AddWithValue("$now_utc", now);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    private async Task<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqliteConnection(_options.ConnectionString);
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private string Table => Quote(_options.TableName);
    private string IndexName => Quote($"{_options.TableName}_expires_idx");
    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
}
