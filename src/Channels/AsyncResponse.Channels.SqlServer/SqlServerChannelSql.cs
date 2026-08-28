using AsyncResponse.Internal;
using Microsoft.Data.SqlClient;
using System.Data;

namespace AsyncResponse.Channels.SqlServer;

internal readonly record struct SqlServerChannelMessage(
    Guid Id,
    string CorrelationId,
    string EnvelopeJson,
    DateTimeOffset CreatedAtUtc,
    DateTimeOffset? AckedAtUtc = null,
    long? AckedSeq = null);

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
        AckSequenceName = SequenceName(_options.MessageTable);
        AckSequence = $"{Schema}.{Quote(AckSequenceName)}";
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

    private string AckSequenceName { get; }

    public async Task EnsureCreatedAsync(CancellationToken cancellationToken = default)
    {
        if (_created)
            return;

        if (!_options.AutoCreateSchema)
        {
            // Manually managed schemas get a one-time validation instead of DDL: 1.0.0 added
            // acked_seq and its sequence, which waiter registration and delivery claims require
            // unconditionally — without this check an un-migrated schema fails later with a raw
            // "invalid column name" mid-operation instead of an actionable startup error carrying
            // the exact migration.
            await ValidateManagedSchemaAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

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
                    correlation_id nvarchar(400) COLLATE Latin1_General_100_BIN2 NOT NULL,
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
                    correlation_id nvarchar(400) COLLATE Latin1_General_100_BIN2 NOT NULL,
                    envelope_json nvarchar(max) NOT NULL,
                    created_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
                    expires_at datetime2 NOT NULL,
                    acked_at datetime2 NULL,
                    acked_seq bigint NULL,
                    recovery_claimed bit NOT NULL DEFAULT 0
                );
                IF COL_LENGTH(N'{MessageTable}', N'acked_seq') IS NULL
                    ALTER TABLE {MessageTable} ADD acked_seq bigint NULL;
                IF NOT EXISTS (SELECT 1 FROM sys.sequences WHERE name = N'{AckSequenceName}' AND schema_id = SCHEMA_ID(N'{_options.SchemaName}'))
                    CREATE SEQUENCE {AckSequence} AS bigint START WITH 1;
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{IndexName(_options.MessageTable, "correlation_created")}' AND object_id = OBJECT_ID(N'{MessageTable}'))
                    CREATE INDEX {Quote(IndexName(_options.MessageTable, "correlation_created"))}
                        ON {MessageTable} (correlation_id, created_at);
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{IndexName(_options.MessageTable, "expires")}' AND object_id = OBJECT_ID(N'{MessageTable}'))
                    CREATE INDEX {Quote(IndexName(_options.MessageTable, "expires"))}
                        ON {MessageTable} (expires_at);

                IF OBJECT_ID(N'{SubscriberTable}', N'U') IS NULL
                CREATE TABLE {SubscriberTable} (
                    correlation_id nvarchar(400) COLLATE Latin1_General_100_BIN2 NOT NULL,
                    registration_id uniqueidentifier NOT NULL,
                    instance_id nvarchar(200) NOT NULL,
                    expires_at datetime2 NOT NULL,
                    PRIMARY KEY (correlation_id, registration_id)
                );
                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{IndexName(_options.SubscriberTable, "expires")}' AND object_id = OBJECT_ID(N'{SubscriberTable}'))
                    CREATE INDEX {Quote(IndexName(_options.SubscriberTable, "expires"))}
                        ON {SubscriberTable} (expires_at);
                """;
            try
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (SqlException ex)
            {
                // The batch can break BEFORE the verification below ever runs: a name held by
                // another component's table suppresses the guarded CREATE and the statements that
                // follow (an index over columns that table lacks, the acked_seq ALTER) hit the
                // wrong table, and a name held by a view fails outright with error 2714. Run the
                // very same catalog checks now — on a fresh connection, since the objects in
                // question are somebody else's and already committed — so the operator gets the
                // precise reason instead of a raw provider error.
                await SqlServerRelationVerifier.ThrowDiagnosedCollisionAsync(
                    OpenConnectionAsync,
                    ex,
                    _options.SchemaName,
                    "channel",
                    ExpectedObjects(),
                    cancellationToken).ConfigureAwait(false);
                throw;
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

            // Verified AFTER the commit, on the same connection but outside the transaction. The
            // checks read the catalog, and a transaction that has just run DDL still holds
            // schema-modification locks — catalog reads under those deadlock (error 1205) against
            // this store's own live traffic, which is already polling by the time a later
            // EnsureCreated re-runs. Correctness does not need the transaction: the application
            // lock serialized the DDL, and what these checks look for is somebody ELSE'S committed
            // object occupying a name, never our own uncommitted work.
            await VerifyRelationsAsync(connection, transaction: null, cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }


    /// <summary>
    /// Post-DDL catalog verification, inside the DDL transaction (and therefore under the shared
    /// application lock). The existence guards above only ask "is there a user table with this
    /// name": a name held by another AsyncResponse component's table makes them skip creation
    /// silently, and a name held by a view or synonym makes the CREATE fail with raw error 2714.
    /// </summary>
    private Task VerifyRelationsAsync(SqlConnection connection, SqlTransaction? transaction, CancellationToken cancellationToken)
        => SqlServerRelationVerifier.VerifyAsync(
            connection,
            transaction,
            _options.SchemaName,
            "channel",
            ExpectedObjects(),
            cancellationToken);

    /// <summary>The catalog shape this store's DDL intends — the single source for both the
    /// post-DDL verification and the failed-batch diagnosis.</summary>
    /// <remarks>A bare <c>datetime2</c> declaration is <c>datetime2(7)</c>; the expected types
    /// state the scale, because a reduced-scale column rounds the timestamps on store rather than
    /// merely displaying them coarsely.</remarks>
    private SqlServerRelationVerifier.ExpectedObject[] ExpectedObjects() =>
            [
                new(_options.RecoveryStateTable, SqlServerObjectKind.Table,
                [
                    new("correlation_id", "nvarchar(400)", Nullable: false, RequiresBinaryCollation: true),
                    new("registration_id", "uniqueidentifier", Nullable: false),
                    new("state_json", "nvarchar(max)", Nullable: false),
                    new("expires_at", "datetime2(7)", Nullable: false),
                    new("registered_at", "datetime2(7)", Nullable: false, DefaultExpression: "(sysutcdatetime())")
                ],
                PrimaryKey: ["correlation_id", "registration_id"]),
                new(_options.MessageTable, SqlServerObjectKind.Table,
                [
                    new("id", "uniqueidentifier", Nullable: false),
                    new("correlation_id", "nvarchar(400)", Nullable: false, RequiresBinaryCollation: true),
                    new("envelope_json", "nvarchar(max)", Nullable: false),
                    new("created_at", "datetime2(7)", Nullable: false, DefaultExpression: "(sysutcdatetime())"),
                    new("expires_at", "datetime2(7)", Nullable: false),
                    new("acked_at", "datetime2(7)", Nullable: true),
                    new("acked_seq", "bigint", Nullable: true),
                    new("recovery_claimed", "bit", Nullable: false, DefaultExpression: "((0))")
                ],
                PrimaryKey: ["id"]),
                new(_options.SubscriberTable, SqlServerObjectKind.Table,
                [
                    new("correlation_id", "nvarchar(400)", Nullable: false, RequiresBinaryCollation: true),
                    new("registration_id", "uniqueidentifier", Nullable: false),
                    new("instance_id", "nvarchar(200)", Nullable: false),
                    new("expires_at", "datetime2(7)", Nullable: false)
                ],
                PrimaryKey: ["correlation_id", "registration_id"]),
                new(AckSequenceName, SqlServerObjectKind.Sequence),
                new(IndexName(_options.RecoveryStateTable, "expires"), SqlServerObjectKind.Index,
                    OwningTable: _options.RecoveryStateTable, KeyColumns: ["expires_at"]),
                new(IndexName(_options.MessageTable, "correlation_created"), SqlServerObjectKind.Index,
                    OwningTable: _options.MessageTable, KeyColumns: ["correlation_id", "created_at"]),
                new(IndexName(_options.MessageTable, "expires"), SqlServerObjectKind.Index,
                    OwningTable: _options.MessageTable, KeyColumns: ["expires_at"]),
                new(IndexName(_options.SubscriberTable, "expires"), SqlServerObjectKind.Index,
                    OwningTable: _options.SubscriberTable, KeyColumns: ["expires_at"])
            ];

    private async Task ValidateManagedSchemaAsync(CancellationToken cancellationToken)
    {
        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = connection.CreateCommand();
            command.CommandText =
                $"""
                SELECT
                  CASE WHEN COL_LENGTH(N'{MessageTable}', N'acked_seq') IS NOT NULL THEN 1 ELSE 0 END,
                  CASE WHEN EXISTS (SELECT 1 FROM sys.sequences WHERE name = N'{AckSequenceName}' AND schema_id = SCHEMA_ID(N'{_options.SchemaName}')) THEN 1 ELSE 0 END;
                """;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            var hasColumn = reader.GetInt32(0) == 1;
            var hasSequence = reader.GetInt32(1) == 1;
            if (!hasColumn || !hasSequence)
            {
                throw new InvalidOperationException(
                    $"The SQL Server channel schema is managed manually (AutoCreateSchema = false) but is missing " +
                    $"objects this version requires: " +
                    $"{(hasColumn ? "" : $"column {MessageTable}.acked_seq")}{(!hasColumn && !hasSequence ? " and " : "")}{(hasSequence ? "" : $"sequence {AckSequence}")}. " +
                    $"Apply the migration and restart: " +
                    $"IF COL_LENGTH(N'{MessageTable}', N'acked_seq') IS NULL ALTER TABLE {MessageTable} ADD acked_seq bigint NULL; " +
                    $"IF NOT EXISTS (SELECT 1 FROM sys.sequences WHERE name = N'{AckSequenceName}' AND schema_id = SCHEMA_ID(N'{_options.SchemaName}')) CREATE SEQUENCE {AckSequence} AS bigint START WITH 1; " +
                    "See docs/sqlserver.md, section 'Upgrading a manually managed schema'.");
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
    /// is treated as success, so a retried publish never duplicates a response. Returns the row's
    /// server-stamped <c>created_at</c> (the original row's on a duplicate) so the same-process
    /// fast path compares against subscription watermarks on the server clock rather than the
    /// app clock.
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

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {MessageTable} (id, correlation_id, envelope_json, expires_at)
            OUTPUT inserted.created_at
            SELECT @id, @correlation_id, @envelope_json, {AddMilliseconds("@retention_ms")}
            WHERE NOT EXISTS (SELECT 1 FROM {MessageTable} WITH (UPDLOCK, HOLDLOCK) WHERE id = @id);
            """;
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@correlation_id", correlationId);
        command.Parameters.AddWithValue("@envelope_json", envelopeJson);
        command.Parameters.AddWithValue("@retention_ms", (long)retention.TotalMilliseconds);

        object? createdAt = null;
        try
        {
            createdAt = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (SqlException ex) when (ex.Number is PrimaryKeyViolation or UniqueIndexViolation)
        {
        }

        if (createdAt is DateTime insertedCreatedAt)
            return new DateTimeOffset(insertedCreatedAt, TimeSpan.Zero);

        // Duplicate insert (WHERE NOT EXISTS suppressed it, or the key-violation race lost):
        // return the original row's server-stamped created_at. Unlike PostgreSQL's single-statement
        // CTE, this fallback is already a SEPARATE statement, so a concurrent same-id publish is
        // resolved here deterministically: the HOLDLOCK range lock on the first statement
        // serializes against the competing insert, and this second statement reads its own fresh
        // snapshot/locks and sees the committed row.
        await using var lookup = connection.CreateCommand();
        lookup.CommandText = $"SELECT created_at FROM {MessageTable} WHERE id = @id;";
        lookup.Parameters.AddWithValue("@id", id);
        var existing = await lookup.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

        // A missing row means the idempotent duplicate's original is already gone (pruned
        // mid-publish): the message is not persisted, so reporting success with a fabricated
        // app-clock timestamp would both lie about persistence and feed a client clock into the
        // server-clock watermark. Fail instead, so the publisher's error handling runs.
        return existing is DateTime existingCreatedAt
            ? new DateTimeOffset(existingCreatedAt, TimeSpan.Zero)
            : throw new InvalidOperationException(
                $"SQL Server response insert for message {id} found no row after a duplicate: the original no longer exists (pruned). The response is not persisted.");
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
            SELECT id, correlation_id, envelope_json, created_at, acked_at, acked_seq
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
            messages.Add(new SqlServerChannelMessage(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.GetString(2),
                new DateTimeOffset(reader.GetDateTime(3), TimeSpan.Zero),
                reader.IsDBNull(4) ? null : new DateTimeOffset(reader.GetDateTime(4), TimeSpan.Zero),
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
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // NEXT VALUE FOR is not allowed inside CASE/COALESCE, so the sequence value is drawn into
        // a variable first — one batch, one round trip; the unused draw on an already-acked row
        // just leaves a harmless sequence gap. The sequence is stamped ONLY when this same update
        // transitions acked_at from null (SET expressions read the pre-update row): a row acked by
        // a pre-sequence build must stay permanently unsequenced — back-filling it on a later
        // fan-out re-claim would pair an OLD acked_at with a FRESH sequence value, and a waiter
        // that registered in the original ack's tick would then read the tie as post-registration
        // fan-out, replaying a response its predecessor consumed.
        command.CommandText =
            $"""
            DECLARE @seq bigint = NEXT VALUE FOR {AckSequence};
            UPDATE {MessageTable}
            SET acked_at = COALESCE(acked_at, SYSUTCDATETIME()),
                acked_seq = CASE WHEN acked_at IS NULL THEN @seq ELSE acked_seq END
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

    /// <summary>
    /// One round trip for a subscription's registration watermark: the server's UTC clock (for
    /// the created-at bound) and a fresh position in the monotonic ack sequence (for the exact
    /// acked-history bound — see the watermark in the shared channel base).
    /// </summary>
    public async Task<(DateTimeOffset ServerTimeUtc, long StartSeq)> GetSubscriptionStartAsync(CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT SYSUTCDATETIME(), NEXT VALUE FOR {AckSequence};";
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        return (new DateTimeOffset(reader.GetDateTime(0), TimeSpan.Zero), reader.GetInt64(1));
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
        // sysname caps identifiers at 128; an over-limit name fails at DDL time with a raw
        // "identifier too long" error instead of an actionable configuration error.
        if (value.Length > IdentifierCap)
            throw new InvalidOperationException(
                $"{nameof(SqlServerAsyncResponseChannelOptions)}.{name} '{value}' is {value.Length} characters; SQL Server identifiers are limited to {IdentifierCap}.");
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

    /// <summary>SQL Server's identifier length cap (sysname); longer names error at DDL time.</summary>
    internal const int IdentifierCap = 128;

    // Suffix space is RESERVED before capping in BOTH derived-name helpers: truncating the whole
    // "{table}{suffix}" let a maximum-length table name derive exactly its own name (the sequence
    // collided with the table in the schema-object namespace and CREATE SEQUENCE failed) or let
    // the table's two indexes derive one shared name (the second IF NOT EXISTS guard matched the
    // first index and silently skipped creation).
    private static string SequenceName(string table)
        => RelationalNamePlan.DerivedName(table, "_ack_seq", IdentifierCap);

    private static string IndexName(string table, string suffix)
        => RelationalNamePlan.DerivedName(table, $"_{suffix}_idx", IdentifierCap);

    /// <summary>
    /// Validates the effective schema-object name plan: the three configured tables plus the
    /// derived ack sequence must be pairwise distinct (they share SQL Server's schema-scoped
    /// object namespace, and a table whose name ends exactly where the reserved "_ack_seq" stem
    /// truncates derives its own name). Index names live in per-table namespaces and carry
    /// distinct reserved suffixes, so they cannot collide once the tables are distinct.
    /// Comparison is case-insensitive to match SQL Server's default catalog collations.
    /// </summary>
    public static void ValidateNamePlan(SqlServerAsyncResponseChannelOptions options)
    {
        (string Role, string Name)[] plan =
        [
            ($"{nameof(options.RecoveryStateTable)} table", options.RecoveryStateTable),
            ($"{nameof(options.MessageTable)} table", options.MessageTable),
            ($"{nameof(options.SubscriberTable)} table", options.SubscriberTable),
            ("ack sequence (derived from MessageTable)", SequenceName(options.MessageTable)),
        ];
        RelationalNamePlan.RequireDistinct(
            plan,
            nameof(SqlServerAsyncResponseChannelOptions),
            ". Tables and the sequence derived from MessageTable share one schema-object namespace and must be distinct " +
            "(long names reserve suffix space by truncating the table stem). Shorten or de-overlap the configured table names.");
    }

    /// <summary>
    /// SQL expression adding a millisecond bigint parameter to the database clock. DATEADD only takes
    /// int arguments, so the value is split into whole seconds and a sub-second remainder — TTLs and
    /// retentions stay on the database clock, immune to app-side clock skew, without overflowing on
    /// long spans such as the 7-day recovery expiry.
    /// </summary>
    internal static string AddMilliseconds(string parameterName)
        => $"DATEADD(SECOND, CAST({parameterName} / 1000 AS int), DATEADD(MILLISECOND, CAST({parameterName} % 1000 AS int), SYSUTCDATETIME()))";

    internal static bool IsTransient(Exception exception) => SqlServerTransientFaults.IsTransient(exception);

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
