using Npgsql;
using NpgsqlTypes;
using System.Text.Json;

namespace AsyncResponse.Channels.PostgreSQL;

internal readonly record struct PostgreSqlChannelMessage(Guid Id, string CorrelationId, string EnvelopeJson);

/// <summary>SQL helper for the PostgreSQL channel tables and notification channel.</summary>
internal sealed class PostgreSqlChannelSql
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgreSqlAsyncResponseChannelOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private bool _created;

    public PostgreSqlChannelSql(NpgsqlDataSource dataSource, Microsoft.Extensions.Options.IOptions<PostgreSqlAsyncResponseChannelOptions> options)
    {
        _dataSource = dataSource;
        _options = options.Value;
        _options.Validate();

        Schema = Quote(_options.SchemaName);
        RecoveryTable = $"{Schema}.{Quote(_options.RecoveryStateTable)}";
        MessageTable = $"{Schema}.{Quote(_options.MessageTable)}";
        SubscriberTable = $"{Schema}.{Quote(_options.SubscriberTable)}";
    }

    public string Schema { get; }
    public string RecoveryTable { get; }
    public string MessageTable { get; }
    public string SubscriberTable { get; }
    public string NotificationChannel => _options.NotificationChannel;

    public async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
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

                CREATE TABLE IF NOT EXISTS {RecoveryTable} (
                    correlation_id text NOT NULL,
                    registration_id uuid NOT NULL,
                    state_json jsonb NOT NULL,
                    expires_at timestamptz NOT NULL,
                    registered_at timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (correlation_id, registration_id)
                );
                CREATE INDEX IF NOT EXISTS {Quote(IndexName(_options.RecoveryStateTable, "expires"))}
                    ON {RecoveryTable} (expires_at);

                CREATE TABLE IF NOT EXISTS {MessageTable} (
                    id uuid PRIMARY KEY,
                    correlation_id text NOT NULL,
                    envelope_json jsonb NOT NULL,
                    created_at timestamptz NOT NULL DEFAULT now(),
                    expires_at timestamptz NOT NULL,
                    acked_at timestamptz NULL
                );
                CREATE INDEX IF NOT EXISTS {Quote(IndexName(_options.MessageTable, "correlation_created"))}
                    ON {MessageTable} (correlation_id, created_at);
                CREATE INDEX IF NOT EXISTS {Quote(IndexName(_options.MessageTable, "expires"))}
                    ON {MessageTable} (expires_at);

                CREATE TABLE IF NOT EXISTS {SubscriberTable} (
                    correlation_id text NOT NULL,
                    registration_id uuid NOT NULL,
                    instance_id text NOT NULL,
                    expires_at timestamptz NOT NULL,
                    PRIMARY KEY (correlation_id, registration_id)
                );
                CREATE INDEX IF NOT EXISTS {Quote(IndexName(_options.SubscriberTable, "expires"))}
                    ON {SubscriberTable} (expires_at);
                """;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    public async Task SaveRecoveryStateAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {RecoveryTable} (correlation_id, registration_id, state_json, expires_at, registered_at)
            VALUES (@correlation_id, @registration_id, @state_json, now() + @ttl, now())
            ON CONFLICT (correlation_id, registration_id)
            DO UPDATE SET state_json = EXCLUDED.state_json,
                          expires_at = EXCLUDED.expires_at,
                          registered_at = EXCLUDED.registered_at;
            """;
        command.Parameters.AddWithValue("correlation_id", correlationId);
        command.Parameters.AddWithValue("registration_id", state.RegistrationId);
        command.Parameters.Add("state_json", NpgsqlDbType.Jsonb).Value = JsonSerializer.Serialize(state);
        command.Parameters.AddWithValue("ttl", ttl);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<string>> LoadRecoveryStatesAsync(string correlationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await PruneExpiredRecoveryAsync(correlationId, cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT state_json::text
            FROM {RecoveryTable}
            WHERE correlation_id = @correlation_id AND expires_at > now()
            ORDER BY registered_at;
            """;
        command.Parameters.AddWithValue("correlation_id", correlationId);

        var states = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            states.Add(reader.GetString(0));
        return states;
    }

    public async Task<bool> DeleteRecoveryStateAsync(string correlationId, Guid? registrationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = registrationId is null
            ? $"DELETE FROM {RecoveryTable} WHERE correlation_id = @correlation_id;"
            : $"DELETE FROM {RecoveryTable} WHERE correlation_id = @correlation_id AND registration_id = @registration_id;";
        command.Parameters.AddWithValue("correlation_id", correlationId);
        if (registrationId is not null)
            command.Parameters.AddWithValue("registration_id", registrationId.Value);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async IAsyncEnumerable<string> ScanRecoveryStateJsonAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await PruneExpiredRecoveryAsync(null, cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT state_json::text
            FROM {RecoveryTable}
            WHERE expires_at > now()
            ORDER BY registered_at;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            yield return reader.GetString(0);
    }

    public async Task<Guid> InsertMessageAsync(string correlationId, string envelopeJson, TimeSpan retention, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await PruneExpiredMessagesAsync(cancellationToken).ConfigureAwait(false);

        var id = Guid.NewGuid();
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {MessageTable} (id, correlation_id, envelope_json, expires_at)
            VALUES (@id, @correlation_id, @envelope_json, now() + @retention);
            SELECT pg_notify(@channel, @payload);
            """;
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("correlation_id", correlationId);
        command.Parameters.Add("envelope_json", NpgsqlDbType.Jsonb).Value = envelopeJson;
        command.Parameters.AddWithValue("retention", retention);
        command.Parameters.AddWithValue("channel", NotificationChannel);
        command.Parameters.AddWithValue("payload", JsonSerializer.Serialize(new PostgreSqlNotification(id, correlationId)));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        return id;
    }

    public async Task<IReadOnlyList<PostgreSqlChannelMessage>> LoadMessagesAsync(
        string correlationId,
        DateTimeOffset sinceUtc,
        int batchSize,
        CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT id, correlation_id, envelope_json::text
            FROM {MessageTable}
            WHERE correlation_id = @correlation_id
              AND created_at >= @since
              AND expires_at > now()
            ORDER BY created_at
            LIMIT @limit;
            """;
        command.Parameters.AddWithValue("correlation_id", correlationId);
        command.Parameters.AddWithValue("since", sinceUtc);
        command.Parameters.AddWithValue("limit", batchSize);

        var messages = new List<PostgreSqlChannelMessage>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            messages.Add(new PostgreSqlChannelMessage(reader.GetGuid(0), reader.GetString(1), reader.GetString(2)));
        return messages;
    }

    public async Task AcknowledgeMessageAsync(Guid messageId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"UPDATE {MessageTable} SET acked_at = COALESCE(acked_at, now()) WHERE id = @id;";
        command.Parameters.AddWithValue("id", messageId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> IsMessageAcknowledgedAsync(Guid messageId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT acked_at IS NOT NULL FROM {MessageTable} WHERE id = @id AND expires_at > now();";
        command.Parameters.AddWithValue("id", messageId);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is bool acknowledged && acknowledged;
    }

    public async Task UpsertSubscriberAsync(string correlationId, Guid registrationId, string instanceId, TimeSpan ttl, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await PruneExpiredSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {SubscriberTable} (correlation_id, registration_id, instance_id, expires_at)
            VALUES (@correlation_id, @registration_id, @instance_id, now() + @ttl)
            ON CONFLICT (correlation_id, registration_id)
            DO UPDATE SET instance_id = EXCLUDED.instance_id,
                          expires_at = EXCLUDED.expires_at;
            """;
        command.Parameters.AddWithValue("correlation_id", correlationId);
        command.Parameters.AddWithValue("registration_id", registrationId);
        command.Parameters.AddWithValue("instance_id", instanceId);
        command.Parameters.AddWithValue("ttl", ttl);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteSubscriberAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {SubscriberTable} WHERE correlation_id = @correlation_id AND registration_id = @registration_id;";
        command.Parameters.AddWithValue("correlation_id", correlationId);
        command.Parameters.AddWithValue("registration_id", registrationId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await PruneExpiredSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT count(*)::bigint
            FROM {SubscriberTable}
            WHERE correlation_id = @correlation_id AND expires_at > now();
            """;
        command.Parameters.AddWithValue("correlation_id", correlationId);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is long count ? count : 0L;
    }

    public async Task ExecuteListenAsync(Func<Task> onNotification, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        connection.Notification += (_, _) => _ = onNotification();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = $"LISTEN {Quote(NotificationChannel)};";
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        while (!cancellationToken.IsCancellationRequested)
            await connection.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task PruneExpiredRecoveryAsync(string? correlationId, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = correlationId is null
            ? $"DELETE FROM {RecoveryTable} WHERE expires_at <= now();"
            : $"DELETE FROM {RecoveryTable} WHERE correlation_id = @correlation_id AND expires_at <= now();";
        if (correlationId is not null)
            command.Parameters.AddWithValue("correlation_id", correlationId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task PruneExpiredMessagesAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {MessageTable} WHERE expires_at <= now();";
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task PruneExpiredSubscribersAsync(string? correlationId, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = correlationId is null
            ? $"DELETE FROM {SubscriberTable} WHERE expires_at <= now();"
            : $"DELETE FROM {SubscriberTable} WHERE correlation_id = @correlation_id AND expires_at <= now();";
        if (correlationId is not null)
            command.Parameters.AddWithValue("correlation_id", correlationId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public static void ValidateIdentifier(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{nameof(PostgreSqlAsyncResponseChannelOptions)}.{name} must be configured.");
        if (!IsIdentifier(value))
            throw new InvalidOperationException(
                $"{nameof(PostgreSqlAsyncResponseChannelOptions)}.{name} '{value}' must be a simple PostgreSQL identifier (letters, digits, and underscores; not starting with a digit).");
    }

    private static bool IsIdentifier(string value)
    {
        if (value.Length == 0 || !(char.IsAsciiLetter(value[0]) || value[0] == '_'))
            return false;

        foreach (var c in value)
        {
            if (!(char.IsAsciiLetterOrDigit(c) || c == '_'))
                return false;
        }

        return true;
    }

    private static string Quote(string identifier) => "\"" + identifier + "\"";

    private static string IndexName(string table, string suffix)
    {
        var name = $"{table}_{suffix}_idx";
        return name.Length <= 63 ? name : name[..63];
    }

    private sealed record PostgreSqlNotification(Guid Id, string CorrelationId);
}
