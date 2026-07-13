using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.Oracle;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

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
    /// How often <see cref="OracleFlowStateStore.TryCreateAsync"/> opportunistically deletes expired rows
    /// (loads already treat expired state as absent; pruning bounds table growth). Zero or negative
    /// prunes on every save. Default: 5 minutes.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new InvalidOperationException($"{nameof(OracleDurableFlowOptions)}.{nameof(ConnectionString)} must be configured.");

        DurableFlowStoreShared.ValidateIdentifier(TableName, $"{nameof(OracleDurableFlowOptions)}.{nameof(TableName)}", "Oracle");
    }
}

/// <summary>Oracle implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class OracleFlowStateStore : IFlowStateStore
{
    private const int ObjectAlreadyExists = 955;
    private const int UniqueConstraintViolated = 1;

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
        command.CommandText = $"SELECT state_json, revision FROM {Table} WHERE flow_id = :flow_id AND expires_at_utc > :now_utc";
        command.Parameters.Add(new OracleParameter("flow_id", flowId));
        command.Parameters.Add(new OracleParameter("now_utc", DateTime.UtcNow));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        FlowState? state;
        switch (reader.GetValue(0))
        {
            case string json:
                state = DurableFlowStoreShared.Deserialize(json);
                break;
            case OracleClob clob:
                using (clob)
                    state = DurableFlowStoreShared.Deserialize(clob.Value);
                break;
            default:
                return null;
        }

        return state?.Revision == reader.GetInt64(1) ? state : null;
    }

    public async Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await PruneExpiredAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await TryCreateCoreAsync(connection, flowId, state, ttl, cancellationToken).ConfigureAwait(false);
        }
        catch (OracleException ex) when (ex.Number == UniqueConstraintViolated)
        {
            return await TryCreateCoreAsync(connection, flowId, state, ttl, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<bool> TryCreateCoreAsync(
        OracleConnection connection,
        string flowId,
        FlowState state,
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
                target.expires_at_utc = :expires_at_utc,
                target.updated_at_utc = :now_utc,
                target.revision = :revision,
                target.lease_id = NULL,
                target.lease_expires_at_utc = NULL
                WHERE target.expires_at_utc <= :now_utc
            WHEN NOT MATCHED THEN
                INSERT (flow_id, state_json, expires_at_utc, updated_at_utc, revision)
                VALUES (:flow_id, :state_json, :expires_at_utc, :now_utc, :revision)
            """;
        var now = DateTime.UtcNow;
        command.Parameters.Add(new OracleParameter("flow_id", flowId));
        command.Parameters.Add(new OracleParameter("state_json", OracleDbType.NClob) { Value = DurableFlowStoreShared.Serialize(state) });
        command.Parameters.Add(new OracleParameter("expires_at_utc", now.Add(ttl)));
        command.Parameters.Add(new OracleParameter("now_utc", now));
        command.Parameters.Add(new OracleParameter("revision", state.Revision));
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
        command.BindByName = true;
        command.CommandText =
            $"""
            UPDATE {Table}
            SET state_json = :state_json,
                expires_at_utc = :expires_at_utc,
                updated_at_utc = :now_utc,
                revision = :new_revision
            WHERE flow_id = :flow_id
              AND revision = :expected_revision
              AND expires_at_utc > :now_utc
              AND (:lease_id IS NULL OR (lease_id = :lease_id AND lease_expires_at_utc > :now_utc))
            """;
        var now = DateTime.UtcNow;
        command.Parameters.Add(new OracleParameter("flow_id", flowId));
        command.Parameters.Add(new OracleParameter("state_json", OracleDbType.NClob) { Value = DurableFlowStoreShared.Serialize(state) });
        command.Parameters.Add(new OracleParameter("expires_at_utc", now.Add(ttl)));
        command.Parameters.Add(new OracleParameter("now_utc", now));
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
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.BindByName = true;
        command.CommandText = $"DELETE FROM {Table} WHERE expires_at_utc <= :now_utc";
        command.Parameters.Add(new OracleParameter("now_utc", DateTime.UtcNow));
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
        catch (OracleException ex) when (ex.Number == ObjectAlreadyExists)
        {
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
        var now = DateTime.UtcNow;
        command.CommandText =
            $"""
            UPDATE {Table}
            SET lease_id = :lease_id, lease_expires_at_utc = :lease_expires_at_utc
            WHERE flow_id = :flow_id
              AND expires_at_utc > :now_utc
              AND {(acquire ? "(lease_id IS NULL OR lease_expires_at_utc <= :now_utc OR lease_id = :lease_id)" : "lease_id = :lease_id AND lease_expires_at_utc > :now_utc")}
            """;
        command.Parameters.Add(new OracleParameter("flow_id", flowId));
        command.Parameters.Add(new OracleParameter("lease_id", leaseId));
        command.Parameters.Add(new OracleParameter("now_utc", now));
        command.Parameters.Add(new OracleParameter("lease_expires_at_utc", now.Add(leaseDuration)));
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
    private string IndexName => $"{_options.TableName}_EXPIRES_IDX";
}
}
