using Microsoft.Extensions.Options;
using Npgsql;
using NpgsqlTypes;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace AsyncResponse.Transports.PostgreSQL;

internal enum PostgreSqlSubscriberRole
{
    Worker,
    ResponseIngress
}

/// <summary>A claimed PostgreSQL transport row, decoupled from Npgsql types for dispatch tests.</summary>
internal sealed record PostgreSqlTransportDelivery(
    Guid Id,
    string Queue,
    string Payload,
    IReadOnlyDictionary<string, string> Headers,
    int Attempt,
    Func<ValueTask> AckAsync,
    Func<TimeSpan, ValueTask> NakAsync,
    Func<Exception, bool, CancellationToken, ValueTask<bool>> DeadLetterAsync);

/// <summary>Small SQL adapter for the PostgreSQL transport queue table.</summary>
internal sealed class PostgreSqlTransportStore
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgreSqlAsyncResponseTransportOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private bool _created;
    private readonly long _schemaLockKey;
    private long _lastDeadLetterPruneTicks;

    public PostgreSqlTransportStore(NpgsqlDataSource dataSource, IOptions<PostgreSqlAsyncResponseTransportOptions> options)
    {
        _dataSource = dataSource;
        _options = options.Value;
        PostgreSqlTransportOptionsValidator.ValidateCommon(_options);
        Schema = Quote(_options.SchemaName);
        MessageTable = $"{Schema}.{Quote(_options.MessageTable)}";
        _schemaLockKey = SchemaAdvisoryLockKey(_options.SchemaName);
    }

    public string Schema { get; }
    public string MessageTable { get; }

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
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            // Serialize schema creation across processes. CREATE ... IF NOT EXISTS is not atomic against a
            // concurrent create of the same object: two instances starting together both pass the existence
            // check and collide on the system catalog ("duplicate key ... pg_type_typname_nsp_index"). A
            // transaction-scoped advisory lock (keyed by schema, shared with the channel store) lets one
            // instance build the schema while the rest wait and then find it already present.
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

                CREATE TABLE IF NOT EXISTS {MessageTable} (
                    id uuid PRIMARY KEY,
                    queue text NOT NULL,
                    payload_json jsonb NOT NULL,
                    headers_json jsonb NOT NULL DEFAULT jsonb_build_object(),
                    created_at timestamptz NOT NULL DEFAULT now(),
                    available_at timestamptz NOT NULL DEFAULT now(),
                    locked_until timestamptz NULL,
                    lock_id uuid NULL,
                    attempts integer NOT NULL DEFAULT 0,
                    dead_letter_reason text NULL
                );
                CREATE INDEX IF NOT EXISTS {Quote(IndexName(_options.MessageTable, "claim"))}
                    ON {MessageTable} (queue, available_at, locked_until, created_at);
                CREATE INDEX IF NOT EXISTS {Quote(IndexName(_options.MessageTable, "created"))}
                    ON {MessageTable} (created_at);
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

    /// <summary>
    /// Publishes a queue row. The caller supplies the id so a retried publish is idempotent
    /// (<c>ON CONFLICT DO NOTHING</c>) rather than inserting a duplicate job.
    /// </summary>
    public async Task PublishAsync(
        Guid id,
        string queue,
        string payload,
        IReadOnlyDictionary<string, string>? headers,
        CancellationToken cancellationToken)
    {
        await InsertAsync(id, queue, payload, headers, deadLetterReason: null, notify: true, cancellationToken).ConfigureAwait(false);
        await PruneDeadLettersIfDueAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<PostgreSqlTransportDelivery?> TryClaimAsync(string queue, TimeSpan lockTimeout, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var lockId = Guid.NewGuid();

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            WITH next AS (
                SELECT id
                FROM {MessageTable}
                WHERE queue = @queue
                  AND available_at <= now()
                  AND (locked_until IS NULL OR locked_until <= now())
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE {MessageTable} AS message
            SET attempts = message.attempts + 1,
                locked_until = now() + @lock_timeout,
                lock_id = @lock_id
            FROM next
            WHERE message.id = next.id
            RETURNING message.id, message.queue, message.payload_json::text, message.headers_json::text, message.attempts;
            """;
        command.Parameters.AddWithValue("queue", queue);
        command.Parameters.AddWithValue("lock_timeout", lockTimeout);
        command.Parameters.AddWithValue("lock_id", lockId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        var id = reader.GetGuid(0);
        var payload = reader.GetString(2);
        var headerJson = reader.GetString(3);
        var attempt = reader.GetInt32(4);
        var headers = DeserializeHeaders(headerJson);

        return new PostgreSqlTransportDelivery(
            id,
            reader.GetString(1),
            payload,
            headers,
            attempt,
            () => AckAsync(id, lockId),
            delay => NakAsync(id, lockId, delay),
            (exception, deleteOriginal, token) => DeadLetterAsync(id, lockId, queue, payload, headers, exception, deleteOriginal, token));
    }

    public async IAsyncEnumerable<PostgreSqlTransportDelivery> ClaimBatchAsync(
        string queue,
        int batchSize,
        TimeSpan lockTimeout,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (var i = 0; i < batchSize; i++)
        {
            var delivery = await TryClaimAsync(queue, lockTimeout, cancellationToken).ConfigureAwait(false);
            if (delivery is null)
                yield break;
            yield return delivery;
        }
    }

    private async Task InsertAsync(
        Guid id,
        string queue,
        string payload,
        IReadOnlyDictionary<string, string>? headers,
        string? deadLetterReason,
        bool notify,
        CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {MessageTable} (id, queue, payload_json, headers_json, dead_letter_reason)
            VALUES (@id, @queue, @payload_json, @headers_json, @dead_letter_reason)
            ON CONFLICT (id) DO NOTHING;
            """;
        if (notify)
            command.CommandText += "SELECT pg_notify(@channel, @payload);";

        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("queue", queue);
        command.Parameters.Add("payload_json", NpgsqlDbType.Jsonb).Value = payload;
        command.Parameters.Add("headers_json", NpgsqlDbType.Jsonb).Value = AsyncResponseJson.Serialize(headers ?? EmptyHeaders);
        command.Parameters.AddWithValue("dead_letter_reason", (object?)deadLetterReason ?? DBNull.Value);
        if (notify)
        {
            command.Parameters.AddWithValue("channel", _options.NotificationChannel);
            command.Parameters.AddWithValue("payload", queue);
        }

        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask AckAsync(Guid id, Guid lockId)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {MessageTable} WHERE id = @id AND lock_id = @lock_id;";
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("lock_id", lockId);
        await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
    }

    private async ValueTask NakAsync(Guid id, Guid lockId, TimeSpan delay)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            UPDATE {MessageTable}
            SET available_at = now() + @delay,
                locked_until = NULL,
                lock_id = NULL
            WHERE id = @id AND lock_id = @lock_id;
            SELECT pg_notify(@channel, @payload);
            """;
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("lock_id", lockId);
        command.Parameters.AddWithValue("delay", delay);
        command.Parameters.AddWithValue("channel", _options.NotificationChannel);
        command.Parameters.AddWithValue("payload", "retry");
        await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
    }

    private async ValueTask<bool> DeadLetterAsync(
        Guid id,
        Guid lockId,
        string sourceQueue,
        string payload,
        IReadOnlyDictionary<string, string> headers,
        Exception exception,
        bool deleteOriginal,
        CancellationToken cancellationToken)
    {
        if (!_options.DeadLetterEnabled)
        {
            if (deleteOriginal)
                await AckAsync(id, lockId).ConfigureAwait(false);
            return true;
        }

        var deadHeaders = new Dictionary<string, string>(headers, StringComparer.OrdinalIgnoreCase)
        {
            ["AR-DeadLetter-Reason"] = Sanitize(exception.Message),
            ["AR-DeadLetter-Source-Queue"] = sourceQueue
        };

        try
        {
            if (!deleteOriginal)
            {
                await InsertAsync(Guid.NewGuid(), _options.DeadLetterQueue, payload, deadHeaders, exception.Message, notify: false, cancellationToken).ConfigureAwait(false);
                return true;
            }

            // The DLQ insert and the original-row delete must commit atomically: split across two
            // connections, a crash between them leaves the original row to be redelivered and
            // dead-lettered again, duplicating the DLQ entry.
            await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText =
                $"""
                INSERT INTO {MessageTable} (id, queue, payload_json, headers_json, dead_letter_reason)
                VALUES (@id, @queue, @payload_json, @headers_json, @dead_letter_reason)
                ON CONFLICT (id) DO NOTHING;
                DELETE FROM {MessageTable} WHERE id = @source_id AND lock_id = @lock_id;
                """;
            command.Parameters.AddWithValue("id", Guid.NewGuid());
            command.Parameters.AddWithValue("queue", _options.DeadLetterQueue);
            command.Parameters.Add("payload_json", NpgsqlDbType.Jsonb).Value = payload;
            command.Parameters.Add("headers_json", NpgsqlDbType.Jsonb).Value = AsyncResponseJson.Serialize(deadHeaders);
            command.Parameters.AddWithValue("dead_letter_reason", exception.Message);
            command.Parameters.AddWithValue("source_id", id);
            command.Parameters.AddWithValue("lock_id", lockId);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
        }
    }

    public async Task ExecuteListenAsync(Func<Task> onNotification, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        connection.Notification += (_, _) => _ = onNotification();
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = $"LISTEN {Quote(_options.NotificationChannel)};";
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        while (!cancellationToken.IsCancellationRequested)
            await connection.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Opportunistically deletes dead-letter rows older than the configured retention. No-op unless
    /// <see cref="PostgreSqlAsyncResponseTransportOptions.DeadLetterRetention"/> is set, and throttled
    /// so the DELETE runs at most once per minute regardless of publish rate.
    /// </summary>
    private async Task PruneDeadLettersIfDueAsync(CancellationToken cancellationToken)
    {
        if (_options.DeadLetterRetention is not { } retention || !ShouldPruneDeadLetters())
            return;

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {MessageTable} WHERE queue = @queue AND created_at < now() - @retention;";
        command.Parameters.AddWithValue("queue", _options.DeadLetterQueue);
        command.Parameters.AddWithValue("retention", retention);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private bool ShouldPruneDeadLetters()
    {
        var now = DateTime.UtcNow.Ticks;
        var last = Interlocked.Read(ref _lastDeadLetterPruneTicks);
        return now - last >= DeadLetterPruneThrottle.Ticks
            && Interlocked.CompareExchange(ref _lastDeadLetterPruneTicks, now, last) == last;
    }

    private static IReadOnlyDictionary<string, string> DeserializeHeaders(string json)
    {
        var parsed = AsyncResponseJson.Deserialize<Dictionary<string, string>>(json);
        return parsed is null
            ? EmptyHeaders
            : new Dictionary<string, string>(parsed, StringComparer.OrdinalIgnoreCase);
    }

    private static string Sanitize(string value) => value.Replace('\r', ' ').Replace('\n', ' ');

    /// <summary>
    /// Stable 64-bit advisory-lock key for serializing schema creation. Must be byte-for-byte identical
    /// to the channel store's algorithm/discriminator so that, for a shared schema, the channel and
    /// transport take the same lock and never race each other on CREATE SCHEMA.
    /// </summary>
    internal static long SchemaAdvisoryLockKey(string schemaName)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;
        var hash = offset;
        foreach (var b in Encoding.UTF8.GetBytes($"asyncresponse:ddl:{schemaName}"))
        {
            hash ^= b;
            hash *= prime;
        }

        return unchecked((long)hash);
    }

    private static string Quote(string identifier) => "\"" + identifier + "\"";

    private static string IndexName(string table, string suffix)
    {
        var name = $"{table}_{suffix}_idx";
        return name.Length <= 63 ? name : name[..63];
    }

    private static readonly TimeSpan DeadLetterPruneThrottle = TimeSpan.FromMinutes(1);

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);
}
