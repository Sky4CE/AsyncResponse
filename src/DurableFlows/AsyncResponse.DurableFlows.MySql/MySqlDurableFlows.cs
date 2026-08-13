using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.MySql;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using MySqlConnector;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>DI registration for the MySQL/MariaDB durable-flow state store.</summary>
    public static class MySqlDurableFlowServiceCollectionExtensions
    {
        /// <summary>Stores durable-flow state in MySQL or MariaDB.</summary>
        public static AsyncResponseRegistrationBuilder WithMySqlDurableFlows(
            this AsyncResponseRegistrationBuilder builder,
            Action<MySqlDurableFlowOptions>? configure = null)
        {
            // Singleton on purpose: schema provisioning is cached per store instance, and the
            // executor resolves the store from a fresh scope per flow execution — a scoped store
            // would re-run EnsureCreated's DDL round-trip on every run.
            builder.Services.TryAddSingleton<MySqlFlowStateStore>();
            return builder.WithDurableFlows<MySqlFlowStateStore, MySqlDurableFlowOptions>(configure);
        }
    }
}

namespace AsyncResponse.DurableFlows.MySql
{
/// <summary>Options for the MySQL/MariaDB durable-flow state store.</summary>
public sealed class MySqlDurableFlowOptions : DurableFlowOptions
{
    /// <summary>MySQL or MariaDB connection string. Required.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Table storing one durable-flow ledger row per flow id.</summary>
    public string TableName { get; set; } = "asyncresponse_flow_state";

    /// <summary>Creates the table and expiry index on first use.</summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// How often <see cref="MySqlFlowStateStore.TryCreateAsync"/> opportunistically deletes one bounded
    /// batch (1000 rows) of expired rows (loads already treat expired state as absent; pruning
    /// bounds table growth). Zero or negative prunes on every save. Default: 5 minutes.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum serialized flow-state size in bytes accepted by writes; oversized ledgers fail fast
    /// with an actionable error instead of an opaque provider error. Default: <c>null</c>
    /// (unlimited — <c>longtext</c> holds up to 4 GB), settable as an operator budget.
    /// </summary>
    public long? MaxStateBytes { get; set; }

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new InvalidOperationException($"{nameof(MySqlDurableFlowOptions)}.{nameof(ConnectionString)} must be configured.");

        DurableFlowStoreShared.ValidateIdentifier(TableName, $"{nameof(MySqlDurableFlowOptions)}.{nameof(TableName)}", "MySQL", identifierCap: 64);
        if (MaxStateBytes is <= 0)
            throw new InvalidOperationException($"{nameof(MySqlDurableFlowOptions)}.{nameof(MaxStateBytes)} must be positive when configured.");
    }
}

/// <summary>MySQL/MariaDB implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class MySqlFlowStateStore : IFlowStateStore
{
    private const int PruneBatchSize = 1000;

    /// <summary>
    /// SQL expression adding a millisecond bigint parameter to the database clock. All expiry and
    /// lease math runs on <c>UTC_TIMESTAMP(6)</c> (statement-stable, like <c>NOW()</c>) so app
    /// clock skew can never fence a lease in or out; microsecond arithmetic keeps
    /// <c>datetime(6)</c> precision.
    /// </summary>
    private static string AddMilliseconds(string parameterName)
        => $"TIMESTAMPADD(MICROSECOND, {parameterName} * 1000, UTC_TIMESTAMP(6))";

    private readonly MySqlDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private long _lastPruneTicks;
    private bool _created;

    public MySqlFlowStateStore(IOptions<MySqlDurableFlowOptions> options)
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
        command.CommandText = $"SELECT state_json, revision FROM {Table} WHERE flow_id = @flow_id AND expires_at_utc > UTC_TIMESTAMP(6);";
        command.Parameters.AddWithValue("@flow_id", flowId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        return DurableFlowStoreShared.ReadState(flowId, reader.GetString(0), reader.GetInt64(1));
    }

    public async Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "MySQL");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await PruneExpiredAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {Table} (flow_id, state_json, expires_at_utc, updated_at_utc, revision)
            VALUES (@flow_id, @state_json, {AddMilliseconds("@ttl_ms")}, UTC_TIMESTAMP(6), @revision);
            """;
        command.Parameters.AddWithValue("@flow_id", flowId);
        command.Parameters.AddWithValue("@state_json", stateJson);
        command.Parameters.AddWithValue("@ttl_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(ttl));
        command.Parameters.AddWithValue("@revision", state.Revision);
        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (MySqlException exception) when (exception.Number == 1062)
        {
            // The id already exists. Only an expired row may be replaced below; do not use
            // INSERT IGNORE here because it also suppresses truncation and other data errors.
        }

        // Exactly one caller can replace an expired ledger: after its conditional update, every
        // competing caller sees the new future expiry and returns false. This avoids relying on
        // MySQL's configurable "changed rows" versus "matched rows" result semantics.
        command.CommandText =
            $"""
            UPDATE {Table}
            SET state_json = @state_json,
                revision = @revision,
                lease_id = NULL,
                lease_expires_at_utc = NULL,
                updated_at_utc = UTC_TIMESTAMP(6),
                expires_at_utc = {AddMilliseconds("@ttl_ms")}
            WHERE flow_id = @flow_id AND expires_at_utc <= UTC_TIMESTAMP(6);
            """;
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
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "MySQL");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            UPDATE {Table}
            SET state_json = @state_json,
                expires_at_utc = {AddMilliseconds("@ttl_ms")},
                updated_at_utc = UTC_TIMESTAMP(6),
                revision = @new_revision
            WHERE flow_id = @flow_id
              AND revision = @expected_revision
              AND expires_at_utc > UTC_TIMESTAMP(6)
              AND (@lease_id IS NULL OR (lease_id = @lease_id AND lease_expires_at_utc > UTC_TIMESTAMP(6)));
            """;
        command.Parameters.AddWithValue("@flow_id", flowId);
        command.Parameters.AddWithValue("@state_json", stateJson);
        command.Parameters.AddWithValue("@ttl_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(ttl));
        command.Parameters.AddWithValue("@expected_revision", expectedRevision);
        command.Parameters.AddWithValue("@new_revision", state.Revision);
        command.Parameters.AddWithValue("@lease_id", (object?)leaseId ?? DBNull.Value);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: true, cancellationToken);

    public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: false, cancellationToken);

    public async Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"UPDATE {Table} SET lease_id = NULL, lease_expires_at_utc = NULL WHERE flow_id = @flow_id AND lease_id = @lease_id;";
        command.Parameters.AddWithValue("@flow_id", flowId);
        command.Parameters.AddWithValue("@lease_id", leaseId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {Table} WHERE flow_id = @flow_id;";
        command.Parameters.AddWithValue("@flow_id", flowId);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    private async Task PruneExpiredAsync(CancellationToken cancellationToken)
    {
        // One bounded batch per prune interval (policy shared by all relational stores): an
        // unbatched DELETE over a large expired backlog holds row locks and bloats one
        // transaction for the unlucky create that triggered the prune. Loads already filter on
        // expiry, so any backlog beyond the batch just waits for the next interval.
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {Table} WHERE expires_at_utc <= UTC_TIMESTAMP(6) LIMIT {PruneBatchSize};";
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task EnsureCreatedAsync(CancellationToken cancellationToken)
    {
        if (_created)
            return;

        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            if (_options.AutoCreateSchema)
            {
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    CREATE TABLE IF NOT EXISTS {Table} (
                        flow_id varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
                        state_json longtext NOT NULL,
                        expires_at_utc datetime(6) NOT NULL,
                        updated_at_utc datetime(6) NOT NULL,
                        revision bigint NOT NULL DEFAULT 0,
                        lease_id varchar(64) NULL,
                        lease_expires_at_utc datetime(6) NULL,
                        INDEX {IndexName} (expires_at_utc)
                    );
                    """;
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await VerifyFlowTableAsync(connection, cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    /// <summary>The columns this store reads and writes; a table missing any of them cannot serve it.</summary>
    private static readonly string[] RequiredColumns =
        ["flow_id", "state_json", "expires_at_utc", "updated_at_utc", "revision", "lease_id", "lease_expires_at_utc"];

    /// <summary>
    /// Checks the table this store will actually use, independently of who created it.
    /// <c>CREATE TABLE IF NOT EXISTS</c> leaves a table made by an earlier build (or by hand)
    /// exactly as it was, and <c>AutoCreateSchema = false</c> issues no DDL at all — so the DDL
    /// above only ever protects a table this build created. Two properties of that table are
    /// load-bearing and both fail SILENTLY when absent, which is why they are checked at startup
    /// rather than left to the first query:
    /// <list type="bullet">
    /// <item><description>
    /// A UNIQUE key on flow_id alone. <see cref="TryCreateAsync"/> is the engine's insert-if-absent
    /// primitive and detects "already exists" from MySQL's duplicate-key error 1062 — with no such
    /// key nothing raises 1062, so two concurrent starts of ONE flow id both report success and the
    /// ledger gets two rows.
    /// </description></item>
    /// <item><description>
    /// A binary collation on flow_id. MySQL's default is case-insensitive, which makes two ids
    /// differing only in case (or accent, or width) one key: the second start fails as a duplicate
    /// and a load returns the other run's state.
    /// </description></item>
    /// </list>
    /// </summary>
    private async Task VerifyFlowTableAsync(MySqlConnection connection, CancellationToken cancellationToken)
    {
        var columns = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        await using (var command = connection.CreateCommand())
        {
            command.CommandText =
                """
                SELECT COLUMN_NAME, COLLATION_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @table;
                """;
            command.Parameters.AddWithValue("@table", _options.TableName);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                columns[reader.GetString(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);
        }

        if (columns.Count == 0)
        {
            // The table does not exist: AutoCreateSchema = false and the migration has not run yet.
            // That surfaces at the first query with a clear MySQL error, and failing here would
            // break the documented "create it yourself, later" workflow.
            return;
        }

        foreach (var required in RequiredColumns)
        {
            if (!columns.ContainsKey(required))
            {
                throw new InvalidOperationException(
                    $"The MySQL durable-flow table '{_options.TableName}' has no '{required}' column. It was created by an earlier " +
                    "build or by hand and does not match the shape this store reads and writes " +
                    $"({string.Join(", ", RequiredColumns)}). Re-create it, or add the missing columns — the DDL is in " +
                    "docs/durable-flow-state-stores.md.");
            }
        }

        var collation = columns["flow_id"];
        if (collation is null || !collation.EndsWith("_bin", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"The MySQL durable-flow table '{_options.TableName}' stores flow_id with the collation '{collation ?? "(none)"}', " +
                "which is not binary. Flow ids are compared ordinally by the engine, so ids differing only in case (or accent, or " +
                "width) collide on the primary key: the second flow fails to start and a load returns the other run's state. Fix it " +
                $"with ALTER TABLE `{_options.TableName}` MODIFY flow_id varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT " +
                "NULL; (tables this build creates get that collation automatically).");
        }

        await VerifyFlowIdIsUniqueAsync(connection, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Requires a unique index keyed on flow_id and nothing else. The PRIMARY KEY this store's DDL
    /// declares is the usual one, but any single-column unique index raises the 1062 that
    /// <see cref="TryCreateAsync"/> reads as "already exists", so all of them are accepted. A
    /// COMPOSITE unique key is not: it permits two rows with the same flow_id.
    /// </summary>
    private async Task VerifyFlowIdIsUniqueAsync(MySqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT 1
            FROM information_schema.STATISTICS s
            WHERE s.TABLE_SCHEMA = DATABASE() AND s.TABLE_NAME = @table
              AND s.NON_UNIQUE = 0 AND s.COLUMN_NAME = 'flow_id' AND s.SEQ_IN_INDEX = 1
              AND NOT EXISTS (
                  SELECT 1 FROM information_schema.STATISTICS o
                  WHERE o.TABLE_SCHEMA = s.TABLE_SCHEMA AND o.TABLE_NAME = s.TABLE_NAME
                    AND o.INDEX_NAME = s.INDEX_NAME AND o.SEQ_IN_INDEX > 1)
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("@table", _options.TableName);

        if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not null)
            return;

        throw new InvalidOperationException(
            $"The MySQL durable-flow table '{_options.TableName}' has no unique key on flow_id alone. Starting a flow is an " +
            "insert-if-absent, and this store learns that a ledger already exists from MySQL's duplicate-key error — without that " +
            "key nothing reports the duplicate, so two concurrent starts of the same flow id both succeed and the flow runs twice. " +
            $"Fix it with ALTER TABLE `{_options.TableName}` ADD PRIMARY KEY (flow_id); (tables this build creates declare it " +
            "automatically).");
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
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // Lease fencing runs entirely on the database clock: acquire steals only leases the
        // database considers expired, and renew/extend stays relative to UTC_TIMESTAMP(6), so
        // worker clock skew can never make two nodes hold the same lease.
        command.CommandText =
            $"""
            UPDATE {Table}
            SET lease_id = @lease_id, lease_expires_at_utc = {AddMilliseconds("@lease_ms")}
            WHERE flow_id = @flow_id
              AND expires_at_utc > UTC_TIMESTAMP(6)
              AND {(acquire ? "(lease_id IS NULL OR lease_expires_at_utc <= UTC_TIMESTAMP(6) OR lease_id = @lease_id)" : "lease_id = @lease_id AND lease_expires_at_utc > UTC_TIMESTAMP(6)")};
            """;
        command.Parameters.AddWithValue("@flow_id", flowId);
        command.Parameters.AddWithValue("@lease_id", leaseId);
        command.Parameters.AddWithValue("@lease_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(leaseDuration));
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    private async Task<MySqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        // Row-count semantics guard: this store's lease renewal (and update fencing) treats
        // ExecuteNonQuery's result as ROWS MATCHED, which is MySqlConnector's default
        // (UseAffectedRows=false). A connection string with UseAffectedRows=true switches the
        // result to ROWS CHANGED, and a renewal that lands in the same microsecond as the current
        // lease expiry would report 0 and abort a healthy execution. Do not set
        // UseAffectedRows=true on this store's connection string.
        var connection = new MySqlConnection(_options.ConnectionString);
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
    private string IndexName => Quote(DurableFlowStoreShared.DerivedName(_options.TableName, "_expires_idx", 64));
    private static string Quote(string identifier) => "`" + identifier.Replace("`", "``", StringComparison.Ordinal) + "`";
}
}
