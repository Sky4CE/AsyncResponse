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

    /// <summary>
    /// How often <see cref="PostgreSqlFlowStateStore.SaveAsync"/> opportunistically deletes expired
    /// rows (loads already treat expired state as absent; pruning bounds table growth). Zero or
    /// negative prunes on every save. Default: 5 minutes.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        DurableFlowStoreShared.ValidateIdentifier(SchemaName, $"{nameof(PostgreSqlDurableFlowOptions)}.{nameof(SchemaName)}", "PostgreSQL");
        DurableFlowStoreShared.ValidateIdentifier(TableName, $"{nameof(PostgreSqlDurableFlowOptions)}.{nameof(TableName)}", "PostgreSQL");
    }
}

/// <summary>PostgreSQL implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class PostgreSqlFlowStateStore : IFlowStateStore, IDisposable, IAsyncDisposable
{
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

    public async Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateSave(flowId, state, ttl);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await PruneExpiredAsync(cancellationToken).ConfigureAwait(false);

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

    private async Task PruneExpiredAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {Table} WHERE expires_at_utc <= @now_utc;";
        command.Parameters.AddWithValue("now_utc", DateTime.UtcNow);
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
                    updated_at_utc timestamptz NOT NULL
                );
                CREATE INDEX IF NOT EXISTS {IndexName} ON {Table} (expires_at_utc);
                """;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
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
    private string IndexName => Quote($"{_options.TableName}_expires_idx");
    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
}
