using AsyncResponse.Internal;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Runtime.CompilerServices;

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

    /// <summary>
    /// An EXACT queue-name predicate — the only kind this table can be filtered by safely, because
    /// its three logical queues share one table and are told apart by nothing but this column.
    /// <c>queue = @queue</c> alone is not exact in two independent ways: SQL Server pads the shorter
    /// operand of an equality comparison with spaces (under EVERY collation, binary ones included),
    /// so <c>'worker '</c> answers a query for <c>'worker'</c>; and on a table an older build or a
    /// hand-written migration left with the server's default collation, the comparison also folds
    /// case, accent, and width.
    /// <para>
    /// The second comparison closes both. Appending a non-blank sentinel to each side makes the
    /// last character non-blank, so the padding SQL Server may add can no longer bridge two
    /// different strings — <c>'worker .'</c> versus <c>'worker. '</c> differ at the seventh
    /// character — and the explicit collation makes the comparison ordinal whatever the column's
    /// own collation is. The first comparison is kept as the seekable driver, so the claim index is
    /// still used and this only filters the rows it returns.
    /// </para>
    /// <para>
    /// Verified on SQL Server 2022, which is also why the shape is this one and not the more
    /// obvious <c>DATALENGTH(queue) = DATALENGTH(@queue)</c>: byte counts are meaningless across
    /// types, so that form silently matches NOTHING on a <c>varchar</c> column, and pushing the
    /// explicit collation onto the driver comparison costs the index seek on a case-folding column.
    /// This form keeps an Index Seek on both, and was measured exact against <c>nvarchar</c>
    /// binary, <c>nvarchar</c> case-insensitive, and <c>varchar</c> columns alike.
    /// </para>
    /// <para>
    /// Exactness belongs HERE rather than in a post-claim re-check: a row the query returns has
    /// already been claimed, and releasing it leaves it first in line for the very next poll, which
    /// starves every valid row behind it.
    /// </para>
    /// </summary>
    private const string ExactQueueMatch =
        "queue = @queue AND queue + N'.' = @queue + N'.' COLLATE Latin1_General_100_BIN2";

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
        if (_created)
            return;

        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

            if (!_options.AutoCreateSchema)
            {
                // Operator-managed schema: no DDL and no DDL lock, but catalog verification all the
                // same — an operator-provisioned queue table whose payload_json, headers_json, or
                // timestamp columns have the wrong shape breaks every insert or silently reorders
                // the timestamps this store compares, which is exactly what verification exists to
                // catch. An absent object is fine: the migration has not run yet, the first query
                // surfaces a clear SQL Server error (the documented "create it yourself, later"
                // workflow), and _created stays unlatched so a later operation re-verifies once the
                // migration lands.
                if (!await ObjectExistsAsync(connection, cancellationToken).ConfigureAwait(false))
                    return;

                await VerifyRelationsAsync(connection, transaction: null, selfCreated: false, cancellationToken).ConfigureAwait(false);
                _created = true;
                return;
            }

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
                    queue nvarchar(200) COLLATE Latin1_General_100_BIN2 NOT NULL,
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
            try
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (SqlException ex)
            {
                // The batch can break BEFORE the verification below runs: a name held by another
                // component's table suppresses the guarded CREATE and the index that follows hits
                // the wrong table, and a name held by a view fails outright with error 2714. Run
                // the same catalog checks now, on a fresh connection (the objects in question are
                // somebody else's and already committed), so the operator gets the precise reason.
                await SqlServerRelationVerifier.ThrowDiagnosedCollisionAsync(
                    OpenConnectionAsync,
                    ex,
                    _options.SchemaName,
                    "transport",
                    ExpectedObjects(selfCreated: true),
                    cancellationToken).ConfigureAwait(false);
                throw;
            }

            // Post-DDL catalog verification inside the DDL transaction (and therefore under the
            // shared application lock): the existence guard above only asks "is there a user table
            // with this name", so another component's table silently suppresses creation and a
            // view or synonym makes the CREATE fail with raw error 2714.
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

            // Verified AFTER the commit, on the same connection but outside the transaction. The
            // checks read the catalog, and a transaction that has just run DDL still holds
            // schema-modification locks — catalog reads under those deadlock (error 1205) against
            // this store's own live traffic, which is already polling by the time a later
            // EnsureCreated re-runs. Correctness does not need the transaction: the application
            // lock serialized the DDL, and what these checks look for is somebody ELSE'S committed
            // object occupying a name, never our own uncommitted work.
            await VerifyRelationsAsync(connection, transaction: null, selfCreated: true, cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    private Task VerifyRelationsAsync(SqlConnection connection, SqlTransaction? transaction, bool selfCreated, CancellationToken cancellationToken)
        => SqlServerRelationVerifier.VerifyAsync(
            connection,
            transaction,
            _options.SchemaName,
            "transport",
            ExpectedObjects(selfCreated),
            cancellationToken);

    /// <summary>
    /// Reports whether ANY object occupies the configured queue-table name (any kind: a view or
    /// foreign component's object must reach verification, which names the precise wrong-kind
    /// reason instead of skipping the checks). The catalog's own collation decides case matching,
    /// exactly as the server resolves the runtime identifier.
    /// </summary>
    private async Task<bool> ObjectExistsAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT CASE WHEN EXISTS (
                SELECT 1
                FROM sys.objects o
                JOIN sys.schemas s ON s.schema_id = o.schema_id
                WHERE s.name = @schema AND o.name = @table) THEN 1 ELSE 0 END;
            """;
        command.Parameters.AddWithValue("@schema", _options.SchemaName);
        command.Parameters.AddWithValue("@table", _options.MessageTable);
        return (int)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))! == 1;
    }

    /// <summary>The catalog shape this store expects — the single source for the post-DDL
    /// verification, the failed-batch diagnosis, and the operator-provisioned check.</summary>
    /// <remarks>
    /// A bare <c>datetime2</c> declaration is <c>datetime2(7)</c>; the expected types state the
    /// scale, because a reduced-scale column rounds <c>available_at</c>/<c>locked_until</c> on
    /// store — a claim lease that rounds backwards is already expired when it is written.
    /// <para>
    /// <paramref name="selfCreated"/> distinguishes a table this build's DDL created — where any
    /// drift means somebody ALTERed it, so the queue column is held to the exact declared shape —
    /// from an operator-provisioned one, where the queue column's type and collation are
    /// deliberately unconstrained: <see cref="ExactQueueMatch"/> supplies the binary collation in
    /// the query itself and its sentinel concat defeats trailing-space padding, so every logical
    /// queue is told apart exactly whatever string type the migration chose and whatever collation
    /// the column carries. Every other column keeps its expectation on both paths: a wrong
    /// <c>payload_json</c>, <c>headers_json</c>, or timestamp shape breaks inserts or reorders the
    /// timestamps this store compares no matter who created the table.
    /// </para>
    /// </remarks>
    private SqlServerRelationVerifier.ExpectedObject[] ExpectedObjects(bool selfCreated) =>
            [
                new(_options.MessageTable, SqlServerObjectKind.Table,
                [
                    new("id", "uniqueidentifier", Nullable: false),
                    selfCreated
                        ? new("queue", "nvarchar(200)", Nullable: false, RequiresBinaryCollation: true)
                        : new("queue", Type: null, Nullable: false),
                    new("payload_json", "nvarchar(max)", Nullable: false),
                    new("headers_json", "nvarchar(max)", Nullable: false, DefaultExpression: "(N'{}')"),
                    new("created_at", "datetime2(7)", Nullable: false, DefaultExpression: "(sysutcdatetime())"),
                    new("available_at", "datetime2(7)", Nullable: false, DefaultExpression: "(sysutcdatetime())"),
                    new("locked_until", "datetime2(7)", Nullable: true),
                    new("lock_id", "uniqueidentifier", Nullable: true),
                    new("attempts", "int", Nullable: false, DefaultExpression: "((0))"),
                    new("dead_letter_reason", "nvarchar(max)", Nullable: true)
                ],
                PrimaryKey: ["id"]),
                // Only on the table this build created (PostgreSQL-sibling parity): the DDL's
                // index guard is name-only, so a pre-existing same-name index with the WRONG
                // definition silently suppressed the CREATE and cost the claim its seek. An
                // operator-owned table keeps its own indexing strategy — the same philosophy as
                // the unconstrained queue column — because indexes are claim performance, not
                // correctness.
                .. selfCreated
                    ? (SqlServerRelationVerifier.ExpectedObject[])
                    [
                        new(IndexName(_options.MessageTable, "claim"), SqlServerObjectKind.Index,
                            OwningTable: _options.MessageTable, KeyColumns: ["queue", "available_at", "locked_until", "created_at"]),
                        new(IndexName(_options.MessageTable, "created"), SqlServerObjectKind.Index,
                            OwningTable: _options.MessageTable, KeyColumns: ["created_at"])
                    ]
                    : []
            ];

    /// <summary>
    /// Publishes a queue row. The caller supplies the id so a retried publish is idempotent
    /// (insert-if-absent) rather than inserting a duplicate job.
    /// </summary>
    public async Task PublishAsync(
        Guid id,
        string queue,
        string payload,
        IReadOnlyDictionary<string, string>? headers,
        CancellationToken cancellationToken,
        TimeSpan? delay = null)
    {
        await InsertAsync(id, queue, payload, headers, deadLetterReason: null, notify: true, cancellationToken, delay).ConfigureAwait(false);
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
                SELECT TOP (1) id, payload_json, headers_json, attempts, locked_until, lock_id
                FROM {MessageTable} WITH (UPDLOCK, ROWLOCK, READPAST)
                WHERE {ExactQueueMatch}
                  AND available_at <= SYSUTCDATETIME()
                  AND (locked_until IS NULL OR locked_until <= SYSUTCDATETIME())
                ORDER BY created_at
            )
            UPDATE next
            SET attempts = attempts + 1,
                locked_until = {AddMilliseconds("@lock_timeout_ms")},
                lock_id = @lock_id
            OUTPUT inserted.id, inserted.payload_json, inserted.headers_json, inserted.attempts;
            """;
        command.Parameters.AddWithValue("@queue", queue);
        command.Parameters.AddWithValue("@lock_timeout_ms", (long)lockTimeout.TotalMilliseconds);
        command.Parameters.AddWithValue("@lock_id", lockId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        var id = reader.GetGuid(0);
        var payload = reader.GetString(1);
        var headerJson = reader.GetString(2);
        var attempt = reader.GetInt32(3);
        var headers = DeserializeHeaders(headerJson);

        // The claim predicate matches the queue exactly (see ExactQueueMatch), so the claimed row's
        // queue IS the requested one — no post-claim re-check, and therefore no row that gets
        // claimed, rejected, and released back to the head of the same ordering on every poll.
        return new SqlServerTransportDelivery(
            id,
            queue,
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
        CancellationToken cancellationToken,
        TimeSpan? delay = null)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // Insert-if-absent keeps a retried publish idempotent. The UPDLOCK/HOLDLOCK hints make the
        // existence check and the insert atomic; a concurrent same-id insert that still slips through
        // surfaces as a duplicate-key error, which is treated as success below.
        // Native delayed delivery: available_at gates the claim query, computed on the DATABASE
        // clock (SYSUTCDATETIME + delay) so client clock skew cannot shift the due time.
        command.CommandText =
            delay is null
                ? $"""
                  INSERT INTO {MessageTable} (id, queue, payload_json, headers_json, dead_letter_reason)
                  SELECT @id, @queue, @payload_json, @headers_json, @dead_letter_reason
                  WHERE NOT EXISTS (SELECT 1 FROM {MessageTable} WITH (UPDLOCK, HOLDLOCK) WHERE id = @id);
                  """
                : $"""
                  INSERT INTO {MessageTable} (id, queue, payload_json, headers_json, dead_letter_reason, available_at)
                  SELECT @id, @queue, @payload_json, @headers_json, @dead_letter_reason, {AddMilliseconds("@available_delay_ms")}
                  WHERE NOT EXISTS (SELECT 1 FROM {MessageTable} WITH (UPDLOCK, HOLDLOCK) WHERE id = @id);
                  """;
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@queue", queue);
        command.Parameters.AddWithValue("@payload_json", payload);
        command.Parameters.AddWithValue("@headers_json", AsyncResponseJson.Serialize(headers ?? EmptyHeaders));
        command.Parameters.AddWithValue("@dead_letter_reason", (object?)deadLetterReason ?? DBNull.Value);
        if (delay is { } pending)
            command.Parameters.AddWithValue("@available_delay_ms", (long)pending.TotalMilliseconds);

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
            // Delete FIRST and write the DLQ row only if the fence matched. A stale claim (the lease
            // lapsed and a peer re-claimed the row) must no-op here exactly as the fenced ack and
            // NAK do; writing the row unconditionally buried a full copy of a message that is still
            // live and may yet succeed under its new owner, so the DLQ showed a poison entry for
            // work that completed — and an operator replaying it duplicated its side effects.
            command.CommandText =
                $"""
                SET NOCOUNT ON;
                DELETE FROM {MessageTable} WHERE id = @source_id AND lock_id = @lock_id;
                IF @@ROWCOUNT = 1
                BEGIN
                    INSERT INTO {MessageTable} (id, queue, payload_json, headers_json, dead_letter_reason)
                    VALUES (@id, @queue, @payload_json, @headers_json, @dead_letter_reason);
                    SELECT 1;
                END
                ELSE
                    SELECT 0;
                """;
            command.Parameters.AddWithValue("@id", Guid.NewGuid());
            command.Parameters.AddWithValue("@queue", _options.DeadLetterQueue);
            command.Parameters.AddWithValue("@payload_json", payload);
            command.Parameters.AddWithValue("@headers_json", AsyncResponseJson.Serialize(deadHeaders));
            command.Parameters.AddWithValue("@dead_letter_reason", exception.Message);
            command.Parameters.AddWithValue("@source_id", id);
            command.Parameters.AddWithValue("@lock_id", lockId);
            var buried = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is int and 1;
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

            // Zero means the fence was lost, not that the write failed. Report it as a
            // non-dead-letter so the caller does not log a burial that did not happen; its NAK
            // fallback is fenced too, so the new owner keeps the row untouched.
            if (!buried)
            {
                _logger?.LogWarning(
                    "SQL Server dead-letter for message {MessageId} from queue {SourceQueue} no-opped: the claim's lease had lapsed and the row was re-claimed.",
                    id,
                    sourceQueue);
                return false;
            }

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
        command.CommandText = $"DELETE FROM {MessageTable} WHERE {ExactQueueMatch} AND created_at < {AddMilliseconds("@negative_retention_ms")};";
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

    // Lenient by contract (see DbTransportHeaders): this runs after the claim already committed
    // attempts+1/lock_id, so rejecting any content the nvarchar column legally holds would create
    // an unkillable poison row.
    private static IReadOnlyDictionary<string, string> DeserializeHeaders(string json)
        => DbTransportHeaders.Materialize(json);

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

    // Suffix space is RESERVED before capping; see RelationalNamePlan.DerivedName for why and for
    // the single implementation this and the PostgreSQL / channel stores all share.
    internal static string IndexName(string table, string suffix)
        => RelationalNamePlan.DerivedName(table, $"_{suffix}_idx", identifierCap: 128);

    private static readonly TimeSpan DeadLetterPruneThrottle = TimeSpan.FromMinutes(1);

    private static readonly IReadOnlyDictionary<string, string> EmptyHeaders =
        new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);
}
