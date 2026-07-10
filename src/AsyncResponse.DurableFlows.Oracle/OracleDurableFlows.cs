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
            builder.Services.AddOptions();
            if (configure is not null)
                builder.Services.Configure(configure);

            // Singleton on purpose: schema provisioning is cached per store instance, and the
            // executor resolves the store from a fresh scope per flow execution — a scoped store
            // would re-run EnsureCreated's DDL round-trip on every run.
            builder.Services.TryAddSingleton<OracleFlowStateStore>();
            return builder.WithCustomDurableFlows<OracleFlowStateStore>();
        }
    }
}

namespace AsyncResponse.DurableFlows.Oracle
{
/// <summary>Options for the Oracle durable-flow state store.</summary>
public sealed class OracleDurableFlowOptions
{
    /// <summary>Oracle connection string. Required.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Table storing one durable-flow ledger row per flow id.</summary>
    public string TableName { get; set; } = "ASYNCRESPONSE_FLOW_STATE";

    /// <summary>Creates the table and expiry index on first use.</summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// How often <see cref="OracleFlowStateStore.SaveAsync"/> opportunistically deletes expired rows
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

    public async Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateSave(flowId, state, ttl);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await PruneExpiredAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await MergeAsync(connection, flowId, state, ttl, cancellationToken).ConfigureAwait(false);
        }
        catch (OracleException ex) when (ex.Number == UniqueConstraintViolated)
        {
            // Oracle MERGE has no HOLDLOCK equivalent: two concurrent saves for the same NEW flow
            // id can both take WHEN NOT MATCHED, and the loser dies with ORA-00001. The row exists
            // now, so one retry takes the MATCHED branch — ordinary last-writer-wins, matching the
            // other stores' atomic upserts.
            await MergeAsync(connection, flowId, state, ttl, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task MergeAsync(OracleConnection connection, string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.BindByName = true;
        command.CommandText =
            $"""
            MERGE INTO {Table} target
            USING (SELECT :flow_id AS flow_id FROM dual) source
                ON (target.flow_id = source.flow_id)
            WHEN MATCHED THEN
                UPDATE SET target.state_json = :state_json,
                           target.expires_at_utc = :expires_at_utc,
                           target.updated_at_utc = :updated_at_utc
            WHEN NOT MATCHED THEN
                INSERT (flow_id, state_json, expires_at_utc, updated_at_utc)
                VALUES (:flow_id, :state_json, :expires_at_utc, :updated_at_utc)
            """;
        var now = DateTime.UtcNow;
        command.Parameters.Add(new OracleParameter("flow_id", flowId));
        command.Parameters.Add(new OracleParameter("state_json", OracleDbType.NClob) { Value = DurableFlowStoreShared.Serialize(state) });
        command.Parameters.Add(new OracleParameter("expires_at_utc", now.Add(ttl)));
        command.Parameters.Add(new OracleParameter("updated_at_utc", now));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.BindByName = true;
        command.CommandText = $"SELECT state_json FROM {Table} WHERE flow_id = :flow_id AND expires_at_utc > :now_utc";
        command.Parameters.Add(new OracleParameter("flow_id", flowId));
        command.Parameters.Add(new OracleParameter("now_utc", DateTime.UtcNow));

        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        switch (result)
        {
            case string json:
                return DurableFlowStoreShared.Deserialize(json);
            case OracleClob clob:
                using (clob)
                    return DurableFlowStoreShared.Deserialize(clob.Value);
            default:
                return null;
        }
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
                    updated_at_utc TIMESTAMP(6) NOT NULL
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
