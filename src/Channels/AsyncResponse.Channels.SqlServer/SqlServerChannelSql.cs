using Microsoft.Data.SqlClient;
using System.Data;

namespace AsyncResponse.Channels.SqlServer;

internal readonly record struct SqlServerChannelMessage(
    Guid Id,
    string CorrelationId,
    string EnvelopeJson,
    DateTimeOffset CreatedAtUtc);

/// <summary>SQL helper for the SQL Server channel tables.</summary>
internal sealed class SqlServerChannelSql
{
    // SQL Server duplicate-key error numbers: 2627 = PRIMARY KEY/UNIQUE constraint violation,
    // 2601 = unique index violation. Retried idempotent inserts treat them as success.
    private const int PrimaryKeyViolation = 2627;
    private const int UniqueIndexViolation = 2601;

    private readonly string _connectionString;
    private readonly SqlServerAsyncResponseChannelOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private bool _created;
    private long _lastRecoveryPruneTicks;
    private long _lastMessagePruneTicks;
    private long _lastSubscriberPruneTicks;

    public SqlServerChannelSql(Microsoft.Extensions.Options.IOptions<SqlServerAsyncResponseChannelOptions> options)
    {
        _options = options.Value;
        _options.Validate();
        _connectionString = _options.ConnectionString!;

        Schema = Quote(_options.SchemaName);
        RecoveryTable = $"{Schema}.{Quote(_options.RecoveryStateTable)}";
        MessageTable = $"{Schema}.{Quote(_options.MessageTable)}";
        SubscriberTable = $"{Schema}.{Quote(_options.SubscriberTable)}";
    }

    public string Schema { get; }
    public string RecoveryTable { get; }
    public string MessageTable { get; }
    public string SubscriberTable { get; }

    public async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        if (_created || !_options.AutoCreateSchema)
            return;

        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            // Serialize schema creation across processes. The IF-NOT-EXISTS guards are not atomic
            // against a concurrent create of the same object: two instances starting together both
            // pass the existence check and collide on the catalog (error 2714/2627). A
            // transaction-scoped application lock (keyed by schema, shared with the transport store)
            // lets one instance build the schema while the rest wait and then find it already present.
            await using (var lockCommand = connection.CreateCommand())
            {
                lockCommand.Transaction = transaction;
                lockCommand.CommandText =
                    """
                    DECLARE @lock_result int;
                    EXEC @lock_result = sp_getapplock
                        @Resource = @lock_resource,
                        @LockMode = 'Exclusive',
                        @LockOwner = 'Transaction',
                        @LockTimeout = 60000;
                    IF @lock_result < 0
                        THROW 51000, N'Failed to acquire the AsyncResponse DDL application lock.', 1;
                    """;
                lockCommand.Parameters.AddWithValue("@lock_resource", SchemaLockResource(_options.SchemaName));
                await lockCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText =
                $"""
                IF SCHEMA_ID(N'{_options.SchemaName}') IS NULL
                    EXEC(N'CREATE SCHEMA {Schema}');

                IF OBJECT_ID(N'{RecoveryTable}', N'U') IS NULL
                CREATE TABLE {RecoveryTable} (
                    correlation_id nvarchar(400) NOT NULL,
                    registration_id uniqueidentifier NOT NULL,
                    state_json nvarchar(max) NOT NULL,
                    expires_at datetime2 NOT NULL,
                    registered_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    PRIMARY KEY (correlation_id, registration_id)
                );
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{IndexName(_options.RecoveryStateTable, "expires")}' AND object_id = OBJECT_ID(N'{RecoveryTable}'))
                    CREATE INDEX {Quote(IndexName(_options.RecoveryStateTable, "expires"))}
                        ON {RecoveryTable} (expires_at);

                IF OBJECT_ID(N'{MessageTable}', N'U') IS NULL
                CREATE TABLE {MessageTable} (
                    id uniqueidentifier NOT NULL PRIMARY KEY NONCLUSTERED,
                    correlation_id nvarchar(400) NOT NULL,
                    envelope_json nvarchar(max) NOT NULL,
                    created_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    expires_at datetime2 NOT NULL,
                    acked_at datetime2 NULL,
                    recovery_claimed bit NOT NULL DEFAULT 0
                );
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{IndexName(_options.MessageTable, "correlation_created")}' AND object_id = OBJECT_ID(N'{MessageTable}'))
                    CREATE INDEX {Quote(IndexName(_options.MessageTable, "correlation_created"))}
                        ON {MessageTable} (correlation_id, created_at);
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{IndexName(_options.MessageTable, "expires")}' AND object_id = OBJECT_ID(N'{MessageTable}'))
                    CREATE INDEX {Quote(IndexName(_options.MessageTable, "expires"))}
                        ON {MessageTable} (expires_at);

                IF OBJECT_ID(N'{SubscriberTable}', N'U') IS NULL
                CREATE TABLE {SubscriberTable} (
                    correlation_id nvarchar(400) NOT NULL,
                    registration_id uniqueidentifier NOT NULL,
                    instance_id nvarchar(200) NOT NULL,
                    expires_at datetime2 NOT NULL,
                    PRIMARY KEY (correlation_id, registration_id)
                );
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{IndexName(_options.SubscriberTable, "expires")}' AND object_id = OBJECT_ID(N'{SubscriberTable}'))
                    CREATE INDEX {Quote(IndexName(_options.SubscriberTable, "expires"))}
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

    public async Task SaveRecoveryStateAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // MERGE WITH (HOLDLOCK) makes the match check and insert atomic — the SQL Server equivalent
        // of PostgreSQL's INSERT ... ON CONFLICT DO UPDATE for the (correlation_id, registration_id) key.
        command.CommandText =
            $"""
            MERGE {RecoveryTable} WITH (HOLDLOCK) AS target
            USING (SELECT @correlation_id AS correlation_id, @registration_id AS registration_id) AS source
                ON target.correlation_id = source.correlation_id AND target.registration_id = source.registration_id
            WHEN MATCHED THEN
                UPDATE SET state_json = @state_json,
                           expires_at = {AddMilliseconds("@ttl_ms")},
                           registered_at = SYSUTCDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (correlation_id, registration_id, state_json, expires_at, registered_at)
                VALUES (@correlation_id, @registration_id, @state_json, {AddMilliseconds("@ttl_ms")}, SYSUTCDATETIME());
            """;
        command.Parameters.AddWithValue("@correlation_id", correlationId);
        command.Parameters.AddWithValue("@registration_id", state.RegistrationId);
        command.Parameters.AddWithValue("@state_json", AsyncResponseJson.Serialize(state));
        command.Parameters.AddWithValue("@ttl_ms", (long)ttl.TotalMilliseconds);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<string>> LoadRecoveryStatesAsync(string correlationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (ShouldPrune(ref _lastRecoveryPruneTicks))
            await PruneExpiredRecoveryAsync(correlationId, cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT state_json
            FROM {RecoveryTable}
            WHERE correlation_id = @correlation_id AND expires_at > SYSUTCDATETIME()
            ORDER BY registered_at;
            """;
        command.Parameters.AddWithValue("@correlation_id", correlationId);

        var states = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            states.Add(reader.GetString(0));
        return states;
    }

    public async Task<bool> DeleteRecoveryStateAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {RecoveryTable} WHERE correlation_id = @correlation_id AND registration_id = @registration_id;";
        command.Parameters.AddWithValue("@correlation_id", correlationId);
        command.Parameters.AddWithValue("@registration_id", registrationId);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public async IAsyncEnumerable<string> ScanRecoveryStateJsonAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await PruneExpiredRecoveryAsync(null, cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT state_json
            FROM {RecoveryTable}
            WHERE expires_at > SYSUTCDATETIME()
            ORDER BY registered_at;
            """;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            yield return reader.GetString(0);
    }

    /// <summary>
    /// Inserts a response envelope row. The caller supplies the message id so the insert is
    /// idempotent under retry — a duplicate insert (lost WHERE NOT EXISTS race or an outer retry)
    /// is treated as success, so a retried publish never duplicates a response.
    /// </summary>
    public Task InsertMessageAsync(Guid id, string correlationId, string envelopeJson, TimeSpan retention, CancellationToken cancellationToken)
        => AsyncResponseRetry.ExecuteAsync(
            token => InsertMessageOnceAsync(id, correlationId, envelopeJson, retention, token),
            IsTransient,
            _options.PublishMaxAttempts,
            _options.PublishRetryBaseDelay,
            _options.PublishRetryMaxDelay,
            cancellationToken);

    private async Task<bool> InsertMessageOnceAsync(Guid id, string correlationId, string envelopeJson, TimeSpan retention, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (ShouldPrune(ref _lastMessagePruneTicks))
            await PruneExpiredMessagesAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {MessageTable} (id, correlation_id, envelope_json, expires_at)
            SELECT @id, @correlation_id, @envelope_json, {AddMilliseconds("@retention_ms")}
            WHERE NOT EXISTS (SELECT 1 FROM {MessageTable} WITH (UPDLOCK, HOLDLOCK) WHERE id = @id);
            """;
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@correlation_id", correlationId);
        command.Parameters.AddWithValue("@envelope_json", envelopeJson);
        command.Parameters.AddWithValue("@retention_ms", (long)retention.TotalMilliseconds);

        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (SqlException ex) when (ex.Number is PrimaryKeyViolation or UniqueIndexViolation)
        {
        }

        return true;
    }

    public async Task<IReadOnlyList<SqlServerChannelMessage>> LoadMessagesAsync(
        string correlationId,
        DateTimeOffset sinceUtc,
        int batchSize,
        DateTimeOffset? afterCreatedAtUtc,
        Guid? afterId,
        CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT id, correlation_id, envelope_json, created_at
            FROM {MessageTable}
            WHERE correlation_id = @correlation_id
              AND created_at >= @since
              AND expires_at > SYSUTCDATETIME()
              {(afterCreatedAtUtc is null ? "" : "AND (created_at > @after_created_at OR (created_at = @after_created_at AND id > @after_id))")}
            ORDER BY created_at, id
            OFFSET 0 ROWS FETCH NEXT @limit ROWS ONLY;
            """;
        command.Parameters.AddWithValue("@correlation_id", correlationId);
        var sinceParameter = command.Parameters.Add("@since", SqlDbType.DateTime2);
        sinceParameter.Scale = 7;
        sinceParameter.Value = sinceUtc.UtcDateTime;
        command.Parameters.AddWithValue("@limit", batchSize);
        if (afterCreatedAtUtc is not null)
        {
            var cursorParameter = command.Parameters.Add("@after_created_at", SqlDbType.DateTime2);
            cursorParameter.Scale = 7;
            cursorParameter.Value = afterCreatedAtUtc.Value.UtcDateTime;
            command.Parameters.AddWithValue("@after_id", afterId ?? throw new ArgumentNullException(nameof(afterId)));
        }

        var messages = new List<SqlServerChannelMessage>(batchSize);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            messages.Add(new SqlServerChannelMessage(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), new DateTimeOffset(reader.GetDateTime(3), TimeSpan.Zero)));
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
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            UPDATE {MessageTable}
            SET acked_at = COALESCE(acked_at, SYSUTCDATETIME())
            OUTPUT inserted.id
            WHERE id = @id AND recovery_claimed = 0 AND expires_at > SYSUTCDATETIME();
            """;
        command.Parameters.AddWithValue("@id", messageId);
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
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            UPDATE {MessageTable}
            SET recovery_claimed = 1
            OUTPUT inserted.id
            WHERE id = @id AND acked_at IS NULL;
            """;
        command.Parameters.AddWithValue("@id", messageId);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is not null and not DBNull;
    }

    /// <summary>Returns the database server's current UTC time, used as a clock-safe delivery watermark.</summary>
    public async Task<DateTimeOffset> GetServerTimeUtcAsync(CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT SYSUTCDATETIME();";
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
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT CAST(CASE WHEN acked_at IS NOT NULL THEN 1 ELSE 0 END AS bit)
            FROM {MessageTable}
            WHERE id = @id AND expires_at > SYSUTCDATETIME();
            """;
        command.Parameters.AddWithValue("@id", messageId);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is bool acknowledged && acknowledged;
    }

    public async Task UpsertSubscriberAsync(string correlationId, Guid registrationId, string instanceId, TimeSpan ttl, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (ShouldPrune(ref _lastSubscriberPruneTicks))
            await PruneExpiredSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            MERGE {SubscriberTable} WITH (HOLDLOCK) AS target
            USING (SELECT @correlation_id AS correlation_id, @registration_id AS registration_id) AS source
                ON target.correlation_id = source.correlation_id AND target.registration_id = source.registration_id
            WHEN MATCHED THEN
                UPDATE SET instance_id = @instance_id,
                           expires_at = {AddMilliseconds("@ttl_ms")}
            WHEN NOT MATCHED THEN
                INSERT (correlation_id, registration_id, instance_id, expires_at)
                VALUES (@correlation_id, @registration_id, @instance_id, {AddMilliseconds("@ttl_ms")});
            """;
        command.Parameters.AddWithValue("@correlation_id", correlationId);
        command.Parameters.AddWithValue("@registration_id", registrationId);
        command.Parameters.AddWithValue("@instance_id", instanceId);
        command.Parameters.AddWithValue("@ttl_ms", (long)ttl.TotalMilliseconds);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task HeartbeatSubscribersAsync(
        string instanceId,
        IReadOnlyList<(string CorrelationId, Guid RegistrationId)> registrations,
        TimeSpan ttl,
        CancellationToken cancellationToken)
    {
        if (registrations.Count == 0)
            return;

        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        // Two parameters per row plus instance/ttl stays under SQL Server's 2100-parameter cap.
        const int batchSize = 1000;
        for (var offset = 0; offset < registrations.Count; offset += batchSize)
        {
            var count = Math.Min(batchSize, registrations.Count - offset);
            await using var command = connection.CreateCommand();
            var sourceRows = new string[count];
            for (var index = 0; index < count; index++)
            {
                var (correlationId, registrationId) = registrations[offset + index];
                sourceRows[index] = $"(@correlation_id_{index}, @registration_id_{index})";
                command.Parameters.AddWithValue($"@correlation_id_{index}", correlationId);
                command.Parameters.AddWithValue($"@registration_id_{index}", registrationId);
            }

            // MERGE upsert rather than a bare UPDATE, in the same WITH (HOLDLOCK) style as
            // UpsertSubscriberAsync: the caller only heartbeats registrations that are live in this
            // process, so a missing row means the pruner deleted it (e.g. after a >timeout stall)
            // — re-creating it here is what brings the waiter back from "permanently invisible".
            command.CommandText =
                $"""
                MERGE {SubscriberTable} WITH (HOLDLOCK) AS target
                USING (VALUES {string.Join(", ", sourceRows)}) AS source (correlation_id, registration_id)
                    ON target.correlation_id = source.correlation_id AND target.registration_id = source.registration_id
                WHEN MATCHED THEN
                    UPDATE SET instance_id = @instance_id,
                               expires_at = {AddMilliseconds("@ttl_ms")}
                WHEN NOT MATCHED THEN
                    INSERT (correlation_id, registration_id, instance_id, expires_at)
                    VALUES (source.correlation_id, source.registration_id, @instance_id, {AddMilliseconds("@ttl_ms")});
                """;
            command.Parameters.AddWithValue("@instance_id", instanceId);
            command.Parameters.AddWithValue("@ttl_ms", (long)ttl.TotalMilliseconds);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task DeleteSubscriberAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {SubscriberTable} WHERE correlation_id = @correlation_id AND registration_id = @registration_id;";
        command.Parameters.AddWithValue("@correlation_id", correlationId);
        command.Parameters.AddWithValue("@registration_id", registrationId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (ShouldPrune(ref _lastSubscriberPruneTicks))
            await PruneExpiredSubscribersAsync(correlationId, cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT COUNT_BIG(*)
            FROM {SubscriberTable}
            WHERE correlation_id = @correlation_id AND expires_at > SYSUTCDATETIME();
            """;
        command.Parameters.AddWithValue("@correlation_id", correlationId);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is long count ? count : 0L;
    }

    private async Task PruneExpiredRecoveryAsync(string? correlationId, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = correlationId is null
            ? $"DELETE FROM {RecoveryTable} WHERE expires_at <= SYSUTCDATETIME();"
            : $"DELETE FROM {RecoveryTable} WHERE correlation_id = @correlation_id AND expires_at <= SYSUTCDATETIME();";
        if (correlationId is not null)
            command.Parameters.AddWithValue("@correlation_id", correlationId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task PruneExpiredMessagesAsync(CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {MessageTable} WHERE expires_at <= SYSUTCDATETIME();";
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task PruneExpiredSubscribersAsync(string? correlationId, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = correlationId is null
            ? $"DELETE FROM {SubscriberTable} WHERE expires_at <= SYSUTCDATETIME();"
            : $"DELETE FROM {SubscriberTable} WHERE correlation_id = @correlation_id AND expires_at <= SYSUTCDATETIME();";
        if (correlationId is not null)
            command.Parameters.AddWithValue("@correlation_id", correlationId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
    {
        var connection = new SqlConnection(_connectionString);
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

    public static void ValidateIdentifier(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{nameof(SqlServerAsyncResponseChannelOptions)}.{name} must be configured.");
        if (!IsIdentifier(value))
            throw new InvalidOperationException(
                $"{nameof(SqlServerAsyncResponseChannelOptions)}.{name} '{value}' must be a simple SQL Server identifier (letters, digits, and underscores; not starting with a digit).");
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

    private static string Quote(string identifier) => "[" + identifier + "]";

    private static string IndexName(string table, string suffix)
    {
        var name = $"{table}_{suffix}_idx";
        return name.Length <= 128 ? name : name[..128];
    }

    /// <summary>
    /// SQL expression adding a millisecond bigint parameter to the database clock. DATEADD only takes
    /// int arguments, so the value is split into whole seconds and a sub-second remainder — TTLs and
    /// retentions stay on the database clock, immune to app-side clock skew, without overflowing on
    /// long spans such as the 7-day recovery expiry.
    /// </summary>
    internal static string AddMilliseconds(string parameterName)
        => $"DATEADD(SECOND, CAST({parameterName} / 1000 AS int), DATEADD(MILLISECOND, CAST({parameterName} % 1000 AS int), SYSUTCDATETIME()))";

    internal static bool IsTransient(Exception exception)
        => exception is not OperationCanceledException
           && (exception is SqlException sqlException && SqlServerTransientFaults.IsTransient(sqlException)
               || exception is TimeoutException);

    /// <summary>
    /// Stable application-lock resource for serializing schema creation. It must be deterministic
    /// across processes and identical to the transport store's resource for the same schema so both
    /// serialize their shared CREATE SCHEMA.
    /// </summary>
    internal static string SchemaLockResource(string schemaName)
        => $"asyncresponse:ddl:{schemaName}";

    /// <summary>
    /// Time-gates opportunistic pruning so the housekeeping DELETE runs at most once per
    /// <see cref="SqlServerAsyncResponseChannelOptions.PruneInterval"/> instead of on every operation.
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

/// <summary>
/// Classifies SQL Server errors worth retrying. <see cref="SqlException"/> exposes no public
/// transient flag, so this mirrors the error numbers Microsoft's own retry guidance and the
/// SqlClient configurable-retry defaults treat as transient, plus severity ≥ 20 (broken connection).
/// </summary>
internal static class SqlServerTransientFaults
{
    private static readonly HashSet<int> TransientErrorNumbers =
    [
        -2,    // client-side command timeout
        20,    // instance does not support encryption
        64,    // connection lost during login
        121,   // transport semaphore timeout
        233,   // no process on the other end of the pipe
        997,   // overlapped I/O in progress
        1204,  // lock resources exhausted
        1205,  // deadlock victim
        1222,  // lock request timeout
        4060,  // database unavailable
        4221,  // readable secondary timeout
        10053, // transport-level connection abort
        10054, // transport-level connection reset
        10060, // network unreachable / connect timeout
        10928, // Azure SQL resource limit reached
        10929, // Azure SQL minimum guarantee exceeded
        40143, // Azure SQL connection failure
        40197, // Azure SQL service processing error
        40501, // Azure SQL service busy
        40540, // Azure SQL service unavailable
        40613, // Azure SQL database unavailable
        49918, // cannot process request, not enough resources
        49919, // cannot process create/update request
        49920  // cannot process request, too many operations
    ];

    public static bool IsTransient(SqlException exception)
    {
        if (exception.Class >= 20)
            return true;

        foreach (SqlError error in exception.Errors)
        {
            if (TransientErrorNumbers.Contains(error.Number))
                return true;
        }

        return TransientErrorNumbers.Contains(exception.Number);
    }
}
