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
            builder.Services.AddOptions();
            if (configure is not null)
                builder.Services.Configure(configure);

            builder.Services.TryAddScoped<MySqlFlowStateStore>();
            return builder.WithCustomDurableFlows<MySqlFlowStateStore>();
        }
    }
}

namespace AsyncResponse.DurableFlows.MySql
{
/// <summary>Options for the MySQL/MariaDB durable-flow state store.</summary>
public sealed class MySqlDurableFlowOptions
{
    /// <summary>MySQL or MariaDB connection string. Required.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Table storing one durable-flow ledger row per flow id.</summary>
    public string TableName { get; set; } = "asyncresponse_flow_state";

    /// <summary>Creates the table and expiry index on first use.</summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new InvalidOperationException($"{nameof(MySqlDurableFlowOptions)}.{nameof(ConnectionString)} must be configured.");

        DurableFlowStoreShared.ValidateIdentifier(TableName, $"{nameof(MySqlDurableFlowOptions)}.{nameof(TableName)}", "MySQL");
    }
}

/// <summary>MySQL/MariaDB implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class MySqlFlowStateStore : IFlowStateStore
{
    private readonly MySqlDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private bool _created;

    public MySqlFlowStateStore(IOptions<MySqlDurableFlowOptions> options)
    {
        _options = options.Value;
        _options.Validate();
    }

    public async Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateSave(flowId, state, ttl);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {Table} (flow_id, state_json, expires_at_utc, updated_at_utc)
            VALUES (@flow_id, @state_json, @expires_at_utc, @updated_at_utc)
            ON DUPLICATE KEY UPDATE
                state_json = VALUES(state_json),
                expires_at_utc = VALUES(expires_at_utc),
                updated_at_utc = VALUES(updated_at_utc);
            """;
        var now = DateTime.UtcNow;
        command.Parameters.AddWithValue("@flow_id", flowId);
        command.Parameters.AddWithValue("@state_json", DurableFlowStoreShared.Serialize(state));
        command.Parameters.AddWithValue("@expires_at_utc", now.Add(ttl));
        command.Parameters.AddWithValue("@updated_at_utc", now);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT state_json FROM {Table} WHERE flow_id = @flow_id AND expires_at_utc > @now_utc;";
        command.Parameters.AddWithValue("@flow_id", flowId);
        command.Parameters.AddWithValue("@now_utc", DateTime.UtcNow);

        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is string json ? DurableFlowStoreShared.Deserialize(json) : null;
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
                CREATE TABLE IF NOT EXISTS {Table} (
                    flow_id varchar(400) NOT NULL PRIMARY KEY,
                    state_json longtext NOT NULL,
                    expires_at_utc datetime(6) NOT NULL,
                    updated_at_utc datetime(6) NOT NULL,
                    INDEX {IndexName} (expires_at_utc)
                );
                """;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    private async Task<MySqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
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
    private string IndexName => Quote($"{_options.TableName}_expires_idx");
    private static string Quote(string identifier) => "`" + identifier.Replace("`", "``", StringComparison.Ordinal) + "`";
}
}
