using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace AsyncResponse.Transports.SqlServer;

internal enum SqlServerSubscriberRole
{
    Worker,
    ResponseIngress
}

/// <summary>A claimed SQL Server transport row, decoupled from SqlClient types for dispatch tests.</summary>
/// <remarks>
/// <c>RenewAsync</c> extends the claim's lease (<c>locked_until</c>) by the original lock timeout,
/// fenced on the claim's <c>lock_id</c>; it returns <c>false</c> when the fence no longer matches
/// (the lease lapsed and another subscriber re-claimed the row).
/// </remarks>
internal sealed record SqlServerTransportDelivery(
    Guid Id,
    string Queue,
    string Payload,
    IReadOnlyDictionary<string, string> Headers,
    int Attempt,
    Func<ValueTask> AckAsync,
    Func<TimeSpan, ValueTask> NakAsync,
    Func<Exception, bool, CancellationToken, ValueTask<bool>> DeadLetterAsync,
    Func<ValueTask<bool>> RenewAsync);

/// <summary>Small SQL adapter for the SQL Server transport queue table.</summary>
internal sealed class SqlServerTransportStore
{
    // SQL Server duplicate-key error numbers: 2627 = PRIMARY KEY/UNIQUE constraint violation,
    // 2601 = unique index violation. A concurrent retry of the same idempotent publish can lose the
    // WHERE NOT EXISTS race; the duplicate is the outcome the caller asked for, not a failure.
    private const int PrimaryKeyViolation = 2627;
    private const int UniqueIndexViolation = 2601;

    // Interpolated into DDL: literal braces cannot appear directly inside the interpolated raw string.
    private const string EmptyJsonObject = "{}";

    private readonly string _connectionString;
    private readonly SqlServerAsyncResponseTransportOptions _options;
    private readonly ILogger<SqlServerTransportStore>? _logger;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private bool _created;
    private long _lastDeadLetterPruneTicks;

    public SqlServerTransportStore(
        IOptions<SqlServerAsyncResponseTransportOptions> options,
        ILogger<SqlServerTransportStore>? logger = null)
    {
        _options = options.Value;
        _logger = logger;
        SqlServerTransportOptionsValidator.ValidateCommon(_options);
        _connectionString = _options.ConnectionString!;
        Schema = Quote(_options.SchemaName);
        MessageTable = $"{Schema}.{Quote(_options.MessageTable)}";
    }

    public string Schema { get; }
    public string MessageTable { get; }

    /// <summary>
    /// Raised after a row is inserted (with the logical queue name) or released for retry
    /// (<c>null</c>). Same-process subscribers use it to wake immediately instead of waiting out
    /// their empty-poll delay; SQL Server has no LISTEN/NOTIFY, so cross-process wakes rely on polling.
    /// </summary>
    public event Action<string?>? MessagePublished;

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
            // transaction-scoped application lock (keyed by schema, shared with the channel store)
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

                IF OBJECT_ID(N'{MessageTable}', N'U') IS NULL
                CREATE TABLE {MessageTable} (
                    id uniqueidentifier NOT NULL PRIMARY KEY NONCLUSTERED,
                    queue nvarchar(200) NOT NULL,
                    payload_json nvarchar(max) NOT NULL,
                    headers_json nvarchar(max) NOT NULL DEFAULT N'{EmptyJsonObject}',
                    created_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    available_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    locked_until datetime2 NULL,
                    lock_id uniqueidentifier NULL,
                    attempts int NOT NULL DEFAULT 0,
                    dead_letter_reason nvarchar(max) NULL
                );

                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{IndexName(_options.MessageTable, "claim")}' AND object_id = OBJECT_ID(N'{MessageTable}'))
                    CREATE INDEX {Quote(IndexName(_options.MessageTable, "claim"))}
                        ON {MessageTable} (queue, available_at, locked_until, created_at);
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{IndexName(_options.MessageTable, "created")}' AND object_id = OBJECT_ID(N'{MessageTable}'))
                    CREATE INDEX {Quote(IndexName(_options.MessageTable, "created"))}
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
    /// (insert-if-absent) rather than inserting a duplicate job.
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

    public async Task<SqlServerTransportDelivery?> TryClaimAsync(string queue, TimeSpan lockTimeout, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        var lockId = Guid.NewGuid();

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // READPAST skips rows other subscribers hold UPDLOCK on — SQL Server's equivalent of
        // PostgreSQL's FOR UPDATE SKIP LOCKED — so competing consumers never block on each other.
        command.CommandText =
            $"""
            WITH next AS (
                SELECT TOP (1) id, queue, payload_json, headers_json, attempts, locked_until, lock_id
                FROM {MessageTable} WITH (UPDLOCK, ROWLOCK, READPAST)
                WHERE queue = @queue
                  AND available_at <= SYSUTCDATETIME()
                  AND (locked_until IS NULL OR locked_until <= SYSUTCDATETIME())
                ORDER BY created_at
            )
            UPDATE next
            SET attempts = attempts + 1,
                locked_until = {AddMilliseconds("@lock_timeout_ms")},
                lock_id = @lock_id
            OUTPUT inserted.id, inserted.queue, inserted.payload_json, inserted.headers_json, inserted.attempts;
            """;
        command.Parameters.AddWithValue("@queue", queue);
        command.Parameters.AddWithValue("@lock_timeout_ms", (long)lockTimeout.TotalMilliseconds);
        command.Parameters.AddWithValue("@lock_id", lockId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        var id = reader.GetGuid(0);
        var payload = reader.GetString(2);
        var headerJson = reader.GetString(3);
        var attempt = reader.GetInt32(4);
        var headers = DeserializeHeaders(headerJson);

        return new SqlServerTransportDelivery(
            id,
            reader.GetString(1),
            payload,
            headers,
            attempt,
            () => AckAsync(id, lockId),
            delay => NakAsync(id, lockId, delay),
            (exception, deleteOriginal, token) => DeadLetterAsync(id, lockId, queue, payload, headers, exception, deleteOriginal, token),
            () => RenewLeaseAsync(id, lockId, lockTimeout));
    }

    public async IAsyncEnumerable<SqlServerTransportDelivery> ClaimBatchAsync(
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
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // Insert-if-absent keeps a retried publish idempotent. The UPDLOCK/HOLDLOCK hints make the
        // existence check and the insert atomic; a concurrent same-id insert that still slips through
        // surfaces as a duplicate-key error, which is treated as success below.
        command.CommandText =
            $"""
            INSERT INTO {MessageTable} (id, queue, payload_json, headers_json, dead_letter_reason)
            SELECT @id, @queue, @payload_json, @headers_json, @dead_letter_reason
            WHERE NOT EXISTS (SELECT 1 FROM {MessageTable} WITH (UPDLOCK, HOLDLOCK) WHERE id = @id);
            """;
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@queue", queue);
        command.Parameters.AddWithValue("@payload_json", payload);
        command.Parameters.AddWithValue("@headers_json", AsyncResponseJson.Serialize(headers ?? EmptyHeaders));
        command.Parameters.AddWithValue("@dead_letter_reason", (object?)deadLetterReason ?? DBNull.Value);

        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (SqlException ex) when (ex.Number is PrimaryKeyViolation or UniqueIndexViolation)
        {
        }

        if (notify)
            MessagePublished?.Invoke(queue);
    }

    private async ValueTask AckAsync(Guid id, Guid lockId)
    {
        await using var connection = await OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {MessageTable} WHERE id = @id AND lock_id = @lock_id;";
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@lock_id", lockId);
        await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
    }

    private async ValueTask<bool> RenewLeaseAsync(Guid id, Guid lockId, TimeSpan lockTimeout)
    {
        await using var connection = await OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            UPDATE {MessageTable}
            SET locked_until = {AddMilliseconds("@lock_timeout_ms")}
            WHERE id = @id AND lock_id = @lock_id;
            """;
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@lock_id", lockId);
        command.Parameters.AddWithValue("@lock_timeout_ms", (long)lockTimeout.TotalMilliseconds);
        return await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false) > 0;
    }

    private async ValueTask NakAsync(Guid id, Guid lockId, TimeSpan delay)
    {
        await using var connection = await OpenConnectionAsync(CancellationToken.None).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            UPDATE {MessageTable}
            SET available_at = {AddMilliseconds("@delay_ms")},
                locked_until = NULL,
                lock_id = NULL
            WHERE id = @id AND lock_id = @lock_id;
            """;
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@lock_id", lockId);
        command.Parameters.AddWithValue("@delay_ms", (long)delay.TotalMilliseconds);
        await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        MessagePublished?.Invoke(null);
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
            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText =
                $"""
                INSERT INTO {MessageTable} (id, queue, payload_json, headers_json, dead_letter_reason)
                VALUES (@id, @queue, @payload_json, @headers_json, @dead_letter_reason);
                DELETE FROM {MessageTable} WHERE id = @source_id AND lock_id = @lock_id;
                """;
            command.Parameters.AddWithValue("@id", Guid.NewGuid());
            command.Parameters.AddWithValue("@queue", _options.DeadLetterQueue);
            command.Parameters.AddWithValue("@payload_json", payload);
            command.Parameters.AddWithValue("@headers_json", AsyncResponseJson.Serialize(deadHeaders));
            command.Parameters.AddWithValue("@dead_letter_reason", exception.Message);
            command.Parameters.AddWithValue("@source_id", id);
            command.Parameters.AddWithValue("@lock_id", lockId);
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex)
        {
            // Callers decide the redelivery consequence from the false return; log the cause here so
            // a failing dead-letter write is never silent.
            _logger?.LogError(
                ex,
                "Failed to write SQL Server dead-letter row for message {MessageId} from queue {SourceQueue}.",
                id,
                sourceQueue);
            return false;
        }
    }

    /// <summary>
    /// Opportunistically deletes dead-letter rows older than the configured retention. No-op unless
    /// <see cref="SqlServerAsyncResponseTransportOptions.DeadLetterRetention"/> is set, and throttled
    /// so the DELETE runs at most once per minute regardless of publish rate.
    /// </summary>
    private async Task PruneDeadLettersIfDueAsync(CancellationToken cancellationToken)
    {
        if (_options.DeadLetterRetention is not { } retention || !ShouldPruneDeadLetters())
            return;

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {MessageTable} WHERE queue = @queue AND created_at < {AddMilliseconds("@negative_retention_ms")};";
        command.Parameters.AddWithValue("@queue", _options.DeadLetterQueue);
        command.Parameters.AddWithValue("@negative_retention_ms", -(long)retention.TotalMilliseconds);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private bool ShouldPruneDeadLetters()
    {
        var now = DateTime.UtcNow.Ticks;
        var last = Interlocked.Read(ref _lastDeadLetterPruneTicks);
        return now - last >= DeadLetterPruneThrottle.Ticks
            && Interlocked.CompareExchange(ref _lastDeadLetterPruneTicks, now, last) == last;
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

    private static IReadOnlyDictionary<string, string> DeserializeHeaders(string json)
    {
        var parsed = AsyncResponseJson.Deserialize<Dictionary<string, string>>(json);
        return parsed is null
            ? EmptyHeaders
            : new Dictionary<string, string>(parsed, StringComparer.OrdinalIgnoreCase);
    }

    private static string Sanitize(string value) => value.Replace('\r', ' ').Replace('\n', ' ');

    /// <summary>
    /// SQL expression adding a millisecond bigint parameter to the database clock. DATEADD only takes
    /// int arguments, so the value is split into whole seconds and a sub-second remainder — intervals
    /// (lock timeouts, redelivery delays, retentions) stay on the database clock, immune to app-side
    /// clock skew, without overflowing on long spans.
    /// </summary>
    internal static string AddMilliseconds(string parameterName)
        => $"DATEADD(SECOND, CAST({parameterName} / 1000 AS int), DATEADD(MILLISECOND, CAST({parameterName} % 1000 AS int), SYSUTCDATETIME()))";

    /// <summary>
    /// Stable application-lock resource for serializing schema creation. Must be byte-for-byte
    /// identical to the channel store's resource so that, for a shared schema, the channel and
    /// transport take the same lock and never race each other on CREATE SCHEMA.
    /// </summary>
    internal static string SchemaLockResource(string schemaName)
        => $"asyncresponse:ddl:{schemaName}";

    private static string Quote(string identifier) => "[" + identifier + "]";

    private static string IndexName(string table, string suffix)
    {
        var name = $"{table}_{suffix}_idx";
        return name.Length <= 128 ? name : name[..128];
    }

    private static readonly TimeSpan DeadLetterPruneThrottle = TimeSpan.FromMinutes(1);

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);
}
