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
            builder.Services.AddOptions();
            if (configure is not null)
                builder.Services.Configure(configure);

            builder.Services.TryAddSingleton(provider =>
            {
                var options = provider.GetRequiredService<IOptions<PostgreSqlDurableFlowOptions>>().Value;
                if (string.IsNullOrWhiteSpace(options.ConnectionString))
                    throw new InvalidOperationException($"{nameof(PostgreSqlDurableFlowOptions)}.{nameof(PostgreSqlDurableFlowOptions.ConnectionString)} must be configured when no NpgsqlDataSource is registered.");
                return NpgsqlDataSource.Create(options.ConnectionString);
            });

            builder.Services.TryAddScoped<PostgreSqlFlowStateStore>();
            return builder.WithCustomDurableFlows<PostgreSqlFlowStateStore>();
        }
    }
}

namespace AsyncResponse.DurableFlows.PostgreSQL
{
/// <summary>Options for the PostgreSQL durable-flow state store.</summary>
public sealed class PostgreSqlDurableFlowOptions
{
    /// <summary>Optional PostgreSQL connection string used when no <see cref="NpgsqlDataSource"/> is registered.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Database schema that contains the durable-flow table. Default: <c>public</c>.</summary>
    public string SchemaName { get; set; } = "public";

    /// <summary>Table storing one durable-flow ledger row per flow id.</summary>
    public string TableName { get; set; } = "asyncresponse_flow_state";

    /// <summary>Creates the schema, table, and expiry index on first use.</summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        DurableFlowStoreShared.ValidateIdentifier(SchemaName, $"{nameof(PostgreSqlDurableFlowOptions)}.{nameof(SchemaName)}", "PostgreSQL");
        DurableFlowStoreShared.ValidateIdentifier(TableName, $"{nameof(PostgreSqlDurableFlowOptions)}.{nameof(TableName)}", "PostgreSQL");
    }
}

/// <summary>PostgreSQL implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class PostgreSqlFlowStateStore : IFlowStateStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgreSqlDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private bool _created;

    public PostgreSqlFlowStateStore(NpgsqlDataSource dataSource, IOptions<PostgreSqlDurableFlowOptions> options)
    {
        _dataSource = dataSource;
        _options = options.Value;
        _options.Validate();
    }

    public async Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateSave(flowId, state, ttl);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {Table} (flow_id, state_json, expires_at_utc, updated_at_utc)
            VALUES (@flow_id, @state_json, @expires_at_utc, @updated_at_utc)
            ON CONFLICT (flow_id)
            DO UPDATE SET state_json = EXCLUDED.state_json,
                          expires_at_utc = EXCLUDED.expires_at_utc,
                          updated_at_utc = EXCLUDED.updated_at_utc;
            """;
        var now = DateTime.UtcNow;
        command.Parameters.AddWithValue("flow_id", flowId);
        command.Parameters.Add("state_json", NpgsqlDbType.Jsonb).Value = DurableFlowStoreShared.Serialize(state);
        command.Parameters.AddWithValue("expires_at_utc", now.Add(ttl));
        command.Parameters.AddWithValue("updated_at_utc", now);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT state_json::text FROM {Table} WHERE flow_id = @flow_id AND expires_at_utc > @now_utc;";
        command.Parameters.AddWithValue("flow_id", flowId);
        command.Parameters.AddWithValue("now_utc", DateTime.UtcNow);

        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is string json ? DurableFlowStoreShared.Deserialize(json) : null;
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
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"""
                CREATE SCHEMA IF NOT EXISTS {Schema};
                CREATE TABLE IF NOT EXISTS {Table} (
                    flow_id text NOT NULL PRIMARY KEY,
                    state_json jsonb NOT NULL,
                    expires_at_utc timestamptz NOT NULL,
                    updated_at_utc timestamptz NOT NULL
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

    private string Schema => Quote(_options.SchemaName);
    private string Table => $"{Schema}.{Quote(_options.TableName)}";
    private string IndexName => Quote($"{_options.TableName}_expires_idx");
    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
}
