using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.Oracle;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>DI registration for the Oracle durable-flow state store.</summary>
    public static class OracleDurableFlowServiceCollectionExtensions
    {
        /// <summary>Stores durable-flow state in Oracle Database.</summary>
        public static AsyncResponseRegistrationBuilder WithOracleDurableFlows(
            this AsyncResponseRegistrationBuilder builder,
            Action<OracleDurableFlowOptions>? configure = null)
        {
            // Singleton on purpose: schema provisioning is cached per store instance, and the
            // executor resolves the store from a fresh scope per flow execution — a scoped store
            // would re-run EnsureCreated's DDL round-trip on every run.
            builder.Services.TryAddSingleton<OracleFlowStateStore>();
            return builder.WithDurableFlows<OracleFlowStateStore, OracleDurableFlowOptions>(configure);
        }
    }
}

namespace AsyncResponse.DurableFlows.Oracle
{
/// <summary>Options for the Oracle durable-flow state store.</summary>
public sealed class OracleDurableFlowOptions : DurableFlowOptions
{
    /// <summary>Oracle connection string. Required.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Table storing one durable-flow ledger row per flow id.</summary>
    public string TableName { get; set; } = "ASYNCRESPONSE_FLOW_STATE";

    /// <summary>Creates the table and expiry index on first use.</summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// How often <see cref="OracleFlowStateStore.TryCreateAsync"/> opportunistically deletes one bounded
    /// batch (1000 rows) of expired rows (loads already treat expired state as absent; pruning
    /// bounds table growth). Zero or negative prunes on every save. Default: 5 minutes.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum serialized flow-state size in bytes accepted by writes; oversized ledgers fail fast
    /// with an actionable error instead of an opaque provider error. Default: <c>null</c>
    /// (unlimited — <c>NCLOB</c> is effectively unbounded), settable as an operator budget.
    /// </summary>
    public long? MaxStateBytes { get; set; }

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new InvalidOperationException($"{nameof(OracleDurableFlowOptions)}.{nameof(ConnectionString)} must be configured.");

        DurableFlowStoreShared.ValidateIdentifier(TableName, $"{nameof(OracleDurableFlowOptions)}.{nameof(TableName)}", "Oracle", identifierCap: 128);
        if (MaxStateBytes is <= 0)
            throw new InvalidOperationException($"{nameof(OracleDurableFlowOptions)}.{nameof(MaxStateBytes)} must be positive when configured.");
    }
}

/// <summary>Oracle implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class OracleFlowStateStore : IFlowStateStore
{
    private const int ObjectAlreadyExists = 955;
    private const int ColumnListAlreadyIndexed = 1408;
    private const int UniqueConstraintViolated = 1;
    private const int PruneBatchSize = 1000;

    /// <summary>
    /// SQL expression adding a millisecond bind parameter to the database clock. All expiry and
    /// lease math runs on <c>SYS_EXTRACT_UTC(SYSTIMESTAMP)</c> so app clock skew can never fence a
    /// lease in or out; Oracle NUMBER division keeps fractional seconds, so <c>datetime</c>
    /// precision survives the millisecond parameter.
    /// </summary>
    private static string AddMilliseconds(string parameterName)
        => $"SYS_EXTRACT_UTC(SYSTIMESTAMP) + NUMTODSINTERVAL({parameterName} / 1000, 'SECOND')";

    private const string UtcNowSql = "SYS_EXTRACT_UTC(SYSTIMESTAMP)";

    private readonly OracleDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private long _lastPruneTicks;
    private bool _created;

    public OracleFlowStateStore(IOptions<OracleDurableFlowOptions> options)
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
        command.BindByName = true;
        command.CommandText = $"SELECT state_json, revision FROM {Table} WHERE flow_id = :flow_id AND expires_at_utc > {UtcNowSql}";
        command.Parameters.Add(new OracleParameter("flow_id", flowId));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        return DurableFlowStoreShared.ReadState(flowId, reader.GetString(0), reader.GetInt64(1));
    }

    public async Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "Oracle");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await PruneExpiredAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await TryCreateCoreAsync(connection, flowId, stateJson, state.Revision, ttl, cancellationToken).ConfigureAwait(false);
        }
        catch (OracleException ex) when (ex.Number == UniqueConstraintViolated)
        {
            return await TryCreateCoreAsync(connection, flowId, stateJson, state.Revision, ttl, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<bool> TryCreateCoreAsync(
        OracleConnection connection,
        string flowId,
        string stateJson,
        long revision,
        TimeSpan ttl,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.BindByName = true;
        command.CommandText =
            $"""
            MERGE INTO {Table} target
            USING (SELECT :flow_id AS flow_id FROM dual) source ON (target.flow_id = source.flow_id)
            WHEN MATCHED THEN UPDATE SET
                target.state_json = :state_json,
                target.expires_at_utc = {AddMilliseconds(":ttl_ms")},
                target.updated_at_utc = {UtcNowSql},
                target.revision = :revision,
                target.lease_id = NULL,
                target.lease_expires_at_utc = NULL
                WHERE target.expires_at_utc <= {UtcNowSql}
            WHEN NOT MATCHED THEN
                INSERT (flow_id, state_json, expires_at_utc, updated_at_utc, revision)
                VALUES (:flow_id, :state_json, {AddMilliseconds(":ttl_ms")}, {UtcNowSql}, :revision)
            """;
        command.Parameters.Add(new OracleParameter("flow_id", flowId));
        command.Parameters.Add(new OracleParameter("state_json", OracleDbType.NClob) { Value = stateJson });
        command.Parameters.Add(new OracleParameter("ttl_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(ttl)));
        command.Parameters.Add(new OracleParameter("revision", revision));
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
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "Oracle");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.BindByName = true;
        command.CommandText =
            $"""
            UPDATE {Table}
            SET state_json = :state_json,
                expires_at_utc = {AddMilliseconds(":ttl_ms")},
                updated_at_utc = {UtcNowSql},
                revision = :new_revision
            WHERE flow_id = :flow_id
              AND revision = :expected_revision
              AND expires_at_utc > {UtcNowSql}
              AND (:lease_id IS NULL OR (lease_id = :lease_id AND lease_expires_at_utc > {UtcNowSql}))
            """;
        command.Parameters.Add(new OracleParameter("flow_id", flowId));
        command.Parameters.Add(new OracleParameter("state_json", OracleDbType.NClob) { Value = stateJson });
        command.Parameters.Add(new OracleParameter("ttl_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(ttl)));
        command.Parameters.Add(new OracleParameter("expected_revision", expectedRevision));
        command.Parameters.Add(new OracleParameter("new_revision", state.Revision));
        command.Parameters.Add(new OracleParameter("lease_id", OracleDbType.NVarchar2) { Value = (object?)leaseId ?? DBNull.Value });
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
        command.BindByName = true;
        command.CommandText = $"UPDATE {Table} SET lease_id = NULL, lease_expires_at_utc = NULL WHERE flow_id = :flow_id AND lease_id = :lease_id";
        command.Parameters.Add(new OracleParameter("flow_id", flowId));
        command.Parameters.Add(new OracleParameter("lease_id", leaseId));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.BindByName = true;
        command.CommandText = $"DELETE FROM {Table} WHERE flow_id = :flow_id";
        command.Parameters.Add(new OracleParameter("flow_id", flowId));
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
        command.BindByName = true;
        command.CommandText = $"DELETE FROM {Table} WHERE expires_at_utc <= {UtcNowSql} AND ROWNUM <= {PruneBatchSize}";
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
            await ExecuteIgnoringExistsAsync(
                connection,
                $"""
                CREATE TABLE {Table} (
                    flow_id NVARCHAR2(400) NOT NULL PRIMARY KEY,
                    state_json NCLOB NOT NULL,
                    expires_at_utc TIMESTAMP(6) NOT NULL,
                    updated_at_utc TIMESTAMP(6) NOT NULL,
                    revision NUMBER(19) DEFAULT 0 NOT NULL,
                    lease_id NVARCHAR2(64) NULL,
                    lease_expires_at_utc TIMESTAMP(6) NULL
                )
                """,
                cancellationToken).ConfigureAwait(false);
            await ExecuteIgnoringExistsAsync(
                connection,
                $"CREATE INDEX {IndexName} ON {Table} (expires_at_utc)",
                cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    private static async Task ExecuteIgnoringExistsAsync(OracleConnection connection, string commandText, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = commandText;
        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OracleException ex) when (ex.Number is ObjectAlreadyExists or ColumnListAlreadyIndexed)
        {
            // ORA-00955: the object (table/index name) already exists. ORA-01408: the column list
            // is already indexed — raised instead of ORA-00955 when an operator pre-created the
            // expiry index under a different name; the index we want exists in substance.
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
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.BindByName = true;
        // Lease fencing runs entirely on the database clock: acquire steals only leases the
        // database considers expired, and renew/extend stays relative to the server's UTC time,
        // so worker clock skew can never make two nodes hold the same lease.
        command.CommandText =
            $"""
            UPDATE {Table}
            SET lease_id = :lease_id, lease_expires_at_utc = {AddMilliseconds(":lease_ms")}
            WHERE flow_id = :flow_id
              AND expires_at_utc > {UtcNowSql}
              AND {(acquire ? $"(lease_id IS NULL OR lease_expires_at_utc <= {UtcNowSql} OR lease_id = :lease_id)" : $"lease_id = :lease_id AND lease_expires_at_utc > {UtcNowSql}")}
            """;
        command.Parameters.Add(new OracleParameter("flow_id", flowId));
        command.Parameters.Add(new OracleParameter("lease_id", leaseId));
        command.Parameters.Add(new OracleParameter("lease_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(leaseDuration)));
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    private async Task<OracleConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new OracleConnection(_options.ConnectionString);
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

    private string Table => _options.TableName;
    private string IndexName => DurableFlowStoreShared.DerivedName(_options.TableName, "_EXPIRES_IDX", 128);
}
}
