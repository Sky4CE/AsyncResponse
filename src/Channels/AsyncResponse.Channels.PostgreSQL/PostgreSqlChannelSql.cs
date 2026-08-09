using Npgsql;
using NpgsqlTypes;
using System.Text;

namespace AsyncResponse.Channels.PostgreSQL;

internal readonly record struct PostgreSqlChannelMessage(
    Guid Id,
    string CorrelationId,
    string EnvelopeJson,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset? AckedAtUtc = null,
    long? AckedSeq = null);

/// <summary>SQL helper for the PostgreSQL channel tables and notification channel.</summary>
internal sealed class PostgreSqlChannelSql
{
    // PostgreSQL rejects a NOTIFY payload of 8000 bytes or more; stay well under it. A correlation
    // id longer than this is sent as an empty payload, which the listener treats as "scan all".
    private const int MaxNotifyPayloadBytes = 7000;

    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgreSqlAsyncResponseChannelOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private bool _created;
    private readonly long _schemaLockKey;
    private long _lastRecoveryPruneTicks;
    private long _lastMessagePruneTicks;
    private long _lastSubscriberPruneTicks;

    public PostgreSqlChannelSql(NpgsqlDataSource dataSource, Microsoft.Extensions.Options.IOptions<PostgreSqlAsyncResponseChannelOptions> options)
    {
        _dataSource = dataSource;
        _options = options.Value;
        _options.Validate();

        Schema = Quote(_options.SchemaName);
        RecoveryTable = $"{Schema}.{Quote(_options.RecoveryStateTable)}";
        MessageTable = $"{Schema}.{Quote(_options.MessageTable)}";
        SubscriberTable = $"{Schema}.{Quote(_options.SubscriberTable)}";
        AckSequence = $"{Schema}.{Quote(SequenceName(_options.MessageTable))}";
        _schemaLockKey = SchemaAdvisoryLockKey(_options.SchemaName);
    }

    public string Schema { get; }
    public string RecoveryTable { get; }
    public string MessageTable { get; }
    public string SubscriberTable { get; }

    /// <summary>
    /// Qualified name of the monotonic ack sequence. Delivery claims and subscription
    /// registrations draw from this ONE sequence, giving <c>acked_seq</c> and a subscription's
    /// start position a total order no pair of same-tick timestamps has.
    /// </summary>
    public string AckSequence { get; }
    public string NotificationChannel => _options.NotificationChannel;

    public async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        if (_created)
            return;

        if (!_options.AutoCreateSchema)
        {
            // Manually managed schemas get a one-time validation instead of DDL: 1.0.0 added
            // acked_seq and its sequence, which waiter registration and delivery claims require
            // unconditionally — without this check an un-migrated schema fails later with a raw
            // "column does not exist" mid-operation instead of an actionable startup error
            // carrying the exact migration.
            await ValidateManagedSchemaAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

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
            // transaction-scoped advisory lock (keyed by schema, shared with the transport store) lets one
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
                    acked_at timestamptz NULL,
                    acked_seq bigint NULL,
                    recovery_claimed boolean NOT NULL DEFAULT false
                );
                ALTER TABLE {MessageTable} ADD COLUMN IF NOT EXISTS recovery_claimed boolean NOT NULL DEFAULT false;
                ALTER TABLE {MessageTable} ADD COLUMN IF NOT EXISTS acked_seq bigint NULL;
                CREATE SEQUENCE IF NOT EXISTS {AckSequence} AS bigint;
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
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    private async Task ValidateManagedSchemaAsync(CancellationToken cancellationToken)
    {
        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"""
                SELECT
                  EXISTS (SELECT 1 FROM information_schema.columns
                          WHERE table_schema = @schema AND table_name = @table AND column_name = 'acked_seq'),
                  to_regclass('{AckSequence}') IS NOT NULL;
                """;
            command.Parameters.AddWithValue("schema", _options.SchemaName);
            command.Parameters.AddWithValue("table", _options.MessageTable);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            var hasColumn = reader.GetBoolean(0);
            var hasSequence = reader.GetBoolean(1);
            if (!hasColumn || !hasSequence)
            {
                throw new InvalidOperationException(
                    $"The PostgreSQL channel schema is managed manually (AutoCreateSchema = false) but is missing " +
                    $"objects this version requires: " +
                    $"{(hasColumn ? "" : $"column {MessageTable}.acked_seq")}{(!hasColumn && !hasSequence ? " and " : "")}{(hasSequence ? "" : $"sequence {AckSequence}")}. " +
                    $"Apply the migration and restart: " +
                    $"ALTER TABLE {MessageTable} ADD COLUMN IF NOT EXISTS acked_seq bigint NULL; " +
                    $"CREATE SEQUENCE IF NOT EXISTS {AckSequence} AS bigint; " +
                    "See docs/postgresql.md, section 'Upgrading a manually managed schema'.");
            }

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
        command.Parameters.Add("state_json", NpgsqlDbType.Jsonb).Value = AsyncResponseJson.Serialize(state);
        command.Parameters.AddWithValue("ttl", ttl);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<string>> LoadRecoveryStatesAsync(string correlationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (ShouldPrune(ref _lastRecoveryPruneTicks))
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

    public async Task<bool> DeleteRecoveryStateAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {RecoveryTable} WHERE correlation_id = @correlation_id AND registration_id = @registration_id;";
        command.Parameters.AddWithValue("correlation_id", correlationId);
        command.Parameters.AddWithValue("registration_id", registrationId);
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

    /// <summary>
    /// Inserts a response envelope row and notifies listeners. The caller supplies the message id so
    /// the insert is idempotent under retry (<c>ON CONFLICT DO NOTHING</c>); the NOTIFY still fires so
    /// a retried publish never strands an active waiter. Returns the row's server-stamped
    /// <c>created_at</c> (the original row's on a duplicate) so the same-process fast path compares
    /// against subscription watermarks on the server clock rather than the app clock.
    /// </summary>
    public Task<DateTimeOffset> InsertMessageAsync(Guid id, string correlationId, string envelopeJson, TimeSpan retention, CancellationToken cancellationToken)
        => AsyncResponseRetry.ExecuteAsync(
            token => InsertMessageOnceAsync(id, correlationId, envelopeJson, retention, token),
            IsTransient,
            _options.PublishMaxAttempts,
            _options.PublishRetryBaseDelay,
            _options.PublishRetryMaxDelay,
            cancellationToken);

    private async Task<DateTimeOffset> InsertMessageOnceAsync(Guid id, string correlationId, string envelopeJson, TimeSpan retention, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (ShouldPrune(ref _lastMessagePruneTicks))
            await PruneExpiredMessagesAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // Single statement: the final SELECT both fires the NOTIFY exactly once and returns the
        // server-stamped created_at — the fresh row's via RETURNING, or the original row's when
        // the idempotent insert hit a duplicate.
        command.CommandText =
            $"""
            WITH inserted AS (
                INSERT INTO {MessageTable} (id, correlation_id, envelope_json, expires_at)
                VALUES (@id, @correlation_id, @envelope_json, now() + @retention)
                ON CONFLICT (id) DO NOTHING
                RETURNING created_at
            )
            SELECT COALESCE(
                       (SELECT created_at FROM inserted),
                       (SELECT created_at FROM {MessageTable} WHERE id = @id)) AS created_at,
                   pg_notify(@channel, @payload);
            """;
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("correlation_id", correlationId);
        command.Parameters.Add("envelope_json", NpgsqlDbType.Jsonb).Value = envelopeJson;
        command.Parameters.AddWithValue("retention", retention);
        command.Parameters.AddWithValue("channel", NotificationChannel);
        command.Parameters.AddWithValue("payload", NotifyPayload(correlationId));
        DateTimeOffset? createdAt;
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            createdAt = reader.IsDBNull(0) ? null : reader.GetFieldValue<DateTimeOffset>(0);
        }

        if (createdAt is { } stamped)
            return stamped;

        // NULL is (almost always) a CONCURRENT idempotent publish, not a missing row: ON CONFLICT
        // detects the other transaction's row against latest data, but the same-statement fallback
        // subquery reads under this statement's snapshot, which predates that commit — so the row
        // exists and is invisible here (reproduced on PostgreSQL 16). A fresh statement gets a
        // fresh read-committed snapshot and resolves it deterministically; no retry loop needed.
        await using var lookup = connection.CreateCommand();
        lookup.CommandText = $"SELECT created_at FROM {MessageTable} WHERE id = @id;";
        lookup.Parameters.AddWithValue("id", id);
        var existing = await lookup.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

        return existing switch
        {
            DateTimeOffset offset => offset,
            DateTime dateTime => new DateTimeOffset(dateTime, TimeSpan.Zero),

            // Only reachable when the duplicate's original row is genuinely gone (pruned
            // mid-publish): the message is not persisted, and reporting success with a fabricated
            // app-clock timestamp would both lie about persistence and feed a client clock into
            // the server-clock watermark.
            _ => throw new InvalidOperationException(
                $"PostgreSQL response insert for message {id} found no row after a duplicate: the original no longer exists (pruned). The response is not persisted.")
        };
    }

    public async Task<IReadOnlyList<PostgreSqlChannelMessage>> LoadMessagesAsync(
        string correlationId,
        DateTimeOffset sinceUtc,
        int batchSize,
        DateTimeOffset? afterCreatedAtUtc,
        Guid? afterId,
        CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT id, correlation_id, envelope_json::text, created_at, acked_at, acked_seq
            FROM {MessageTable}
            WHERE correlation_id = @correlation_id
              AND created_at >= @since
              AND expires_at > now()
              {(afterCreatedAtUtc is null ? "" : "AND (created_at > @after_created_at OR (created_at = @after_created_at AND id > @after_id))")}
            ORDER BY created_at, id
            LIMIT @limit;
            """;
        command.Parameters.AddWithValue("correlation_id", correlationId);
        command.Parameters.AddWithValue("since", sinceUtc);
        command.Parameters.AddWithValue("limit", batchSize);
        if (afterCreatedAtUtc is not null)
        {
            command.Parameters.AddWithValue("after_created_at", afterCreatedAtUtc.Value);
            command.Parameters.AddWithValue("after_id", afterId ?? throw new ArgumentNullException(nameof(afterId)));
        }

        var messages = new List<PostgreSqlChannelMessage>(batchSize);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            messages.Add(new PostgreSqlChannelMessage(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetFieldValue<DateTimeOffset>(3),
                reader.IsDBNull(4) ? null : reader.GetFieldValue<DateTimeOffset>(4),
                reader.IsDBNull(5) ? null : reader.GetInt64(5)));
        return messages;
    }

    /// <summary>
    /// Atomically claims a message for live delivery: sets <c>acked_at</c> unless the publisher has
    /// already routed it to the lost-subscriber path (<c>recovery_claimed</c>). Returns <c>false</c>
    /// when recovery owns the message, so a slow-but-live waiter does not deliver a response the
    /// recovery callback already handled. Multiple processes may each win this claim, preserving
    /// cross-process fan-out, because it gates only on <c>recovery_claimed</c>, not on <c>acked_at</c>.
    /// </summary>
    public async Task<bool> TryClaimForDeliveryAsync(Guid messageId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            UPDATE {MessageTable}
            SET acked_at = COALESCE(acked_at, now()),
                acked_seq = COALESCE(acked_seq, nextval('{AckSequence}'))
            WHERE id = @id AND NOT recovery_claimed AND expires_at > now()
            RETURNING id;
            """;
        command.Parameters.AddWithValue("id", messageId);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is not null and not DBNull;
    }

    /// <summary>
    /// Atomically claims a message for the lost-subscriber path: sets <c>recovery_claimed</c> only
    /// while no waiter has delivered (<c>acked_at IS NULL</c>). Returns <c>true</c> when recovery wins;
    /// <c>false</c> means a live waiter already took the message, so the publisher must not also fire
    /// the recovery callback. Row-level locking serializes this against <see cref="TryClaimForDeliveryAsync"/>.
    /// </summary>
    public async Task<bool> TryClaimForRecoveryAsync(Guid messageId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            UPDATE {MessageTable}
            SET recovery_claimed = true
            WHERE id = @id AND acked_at IS NULL
            RETURNING id;
            """;
        command.Parameters.AddWithValue("id", messageId);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is not null and not DBNull;
    }

    /// <summary>
    /// One round trip for a subscription's registration watermark: the server's UTC clock (for
    /// the created-at bound) and a fresh position in the monotonic ack sequence (for the exact
    /// acked-history bound — see the watermark in the shared channel base).
    /// </summary>
    public async Task<(DateTimeOffset ServerTimeUtc, long StartSeq)> GetSubscriptionStartAsync(CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT now(), nextval('{AckSequence}');";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        return (reader.GetFieldValue<DateTimeOffset>(0).ToUniversalTime(), reader.GetInt64(1));
    }

    /// <summary>Returns the database server's current UTC time, used as a clock-safe delivery watermark.</summary>
    public async Task<DateTimeOffset> GetServerTimeUtcAsync(CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT now();";
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result switch
        {
            DateTimeOffset dto => dto.ToUniversalTime(),
            DateTime dt => new DateTimeOffset(DateTime.SpecifyKind(dt, DateTimeKind.Utc), TimeSpan.Zero),
            _ => DateTimeOffset.UtcNow
        };
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
        if (ShouldPrune(ref _lastSubscriberPruneTicks))
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

    public async Task HeartbeatSubscribersAsync(
        string instanceId,
        IReadOnlyCollection<(string CorrelationId, Guid RegistrationId)> registrations,
        TimeSpan ttl,
        CancellationToken cancellationToken)
    {
        if (registrations.Count == 0)
            return;

        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();

        // UPSERT rather than a bare UPDATE: the caller only heartbeats registrations that are live
        // in this process, so a missing row means the pruner deleted it (e.g. after a >timeout
        // stall) — re-creating it here is what brings the waiter back from "permanently invisible".
        var correlationIds = new string[registrations.Count];
        var registrationIds = new Guid[registrations.Count];
        var index = 0;
        foreach (var (correlationId, registrationId) in registrations)
        {
            correlationIds[index] = correlationId;
            registrationIds[index] = registrationId;
            index++;
        }

        command.CommandText =
            $"""
            INSERT INTO {SubscriberTable} (correlation_id, registration_id, instance_id, expires_at)
            SELECT correlation_id, registration_id, @instance_id, now() + @ttl
            FROM unnest(@correlation_ids, @registration_ids) AS live (correlation_id, registration_id)
            ON CONFLICT (correlation_id, registration_id)
            DO UPDATE SET instance_id = EXCLUDED.instance_id,
                          expires_at = EXCLUDED.expires_at;
            """;
        command.Parameters.AddWithValue("instance_id", instanceId);
        command.Parameters.AddWithValue("correlation_ids", NpgsqlDbType.Array | NpgsqlDbType.Text, correlationIds);
        command.Parameters.AddWithValue("registration_ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid, registrationIds);
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
        if (ShouldPrune(ref _lastSubscriberPruneTicks))
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

    public async Task ExecuteListenAsync(Func<string?, Task> onNotification, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        connection.Notification += (_, args) => _ = onNotification(args.Payload);
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

    private static string SequenceName(string table)
    {
        var name = $"{table}_ack_seq";
        return name.Length <= 63 ? name : name[..63];
    }

    /// <summary>NOTIFY payload for a publish: the correlation id, or empty when it is too long to carry.</summary>
    private static string NotifyPayload(string correlationId)
        => Encoding.UTF8.GetByteCount(correlationId) <= MaxNotifyPayloadBytes ? correlationId : string.Empty;

    internal static bool IsTransient(Exception exception)
        => exception is not OperationCanceledException
           && (exception is NpgsqlException { IsTransient: true } || exception is TimeoutException);

    /// <summary>
    /// Stable 64-bit advisory-lock key for serializing schema creation. Uses FNV-1a over a
    /// schema-scoped discriminator: it must be deterministic across processes (so
    /// <see cref="string.GetHashCode()"/>, which is per-process randomized, is unusable) and identical
    /// to the transport store's key for the same schema so both serialize their shared CREATE SCHEMA.
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

    /// <summary>
    /// Time-gates opportunistic pruning so the housekeeping DELETE runs at most once per
    /// <see cref="PostgreSqlAsyncResponseChannelOptions.PruneInterval"/> instead of on every operation.
    /// Read queries already filter on <c>expires_at</c>, so throttling pruning never affects correctness.
    /// </summary>
    private bool ShouldPrune(ref long lastTicks)
    {
        var interval = _options.PruneInterval;
        if (interval <= TimeSpan.Zero)
            return true;

        var now = DateTime.UtcNow.Ticks;
        var last = Interlocked.Read(ref lastTicks);
        return now - last >= interval.Ticks
            && Interlocked.CompareExchange(ref lastTicks, now, last) == last;
    }
}
