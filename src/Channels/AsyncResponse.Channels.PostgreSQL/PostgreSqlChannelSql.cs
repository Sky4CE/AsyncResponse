using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using System.Text;

using AsyncResponse.Internal;

namespace AsyncResponse.Channels.PostgreSQL;

/// <summary>One stored response envelope row/document as the channel store returns it.</summary>
/// <remarks>
/// <c>EnvelopeJson</c> is the stored envelope, or <c>null</c> for a row the dispatch sweep loaded header-only (an
/// already-acknowledged row — see <see cref="PostgreSqlChannelSql.LoadMessagesAsync"/>); the
/// sweep hydrates the few such rows it still has to deliver through
/// <see cref="PostgreSqlChannelSql.LoadMessagesByIdAsync"/> before handing them to a waiter.
/// </remarks>
internal readonly record struct PostgreSqlChannelMessage(
    Guid Id,
    string CorrelationId,
    string? EnvelopeJson,
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
    private readonly ILogger<PostgreSqlChannelSql>? _logger;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly PostgreSqlDdlGuard _ddl;
    private bool _created;
    private readonly long _schemaLockKey;
    private readonly long _tableLockKey;
    private long _lastRecoveryPruneTicks;
    private long _lastMessagePruneTicks;
    private long _lastSubscriberPruneTicks;

    public PostgreSqlChannelSql(
        NpgsqlDataSource dataSource,
        Microsoft.Extensions.Options.IOptions<PostgreSqlAsyncResponseChannelOptions> options,
        ILogger<PostgreSqlChannelSql>? logger = null)
    {
        _dataSource = dataSource;
        _options = options.Value;
        _logger = logger;
        _options.Validate();

        Schema = Quote(_options.SchemaName);
        RecoveryTable = $"{Schema}.{Quote(_options.RecoveryStateTable)}";
        MessageTable = $"{Schema}.{Quote(_options.MessageTable)}";
        SubscriberTable = $"{Schema}.{Quote(_options.SubscriberTable)}";
        AckSequenceName = SequenceName(_options.MessageTable);
        AckSequence = $"{Schema}.{Quote(AckSequenceName)}";
        _schemaLockKey = SchemaAdvisoryLockKey(_options.SchemaName);
        _tableLockKey = PostgreSqlDdlGuard.TableLockKey(_options.SchemaName, _options.MessageTable);
        _ddl = new PostgreSqlDdlGuard("channel", $"the channel tables in '{_options.SchemaName}'", "docs/postgresql.md", logger);
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

    /// <summary>Unquoted sequence identifier, for catalog queries.</summary>
    public string AckSequenceName { get; }
    public string NotificationChannel => _options.NotificationChannel;

    /// <summary>The clock of the startup-DDL retry-after window (test seam; see <see cref="PostgreSqlDdlGuard.RetryAfter"/>).</summary>
    internal TimeProvider Clock
    {
        get => _ddl.Clock;
        set => _ddl.Clock = value;
    }

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

        _ddl.ThrowIfBackingOff();
        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            // Again under the gate: a caller that queued behind the attempt that just failed —
            // and latched the retry-after window — fails here instead of starting the next one.
            _ddl.ThrowIfBackingOff();

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

            // The transport's round-43 hardening (PostgreSqlDdlGuard), applied here. Every DDL
            // transaction bounds its lock waits — the advisory key's first — with a lock_timeout:
            // ALTER TABLE … ADD COLUMN IF NOT EXISTS takes its ACCESS EXCLUSIVE lock, and CREATE
            // INDEX IF NOT EXISTS its SHARE lock, BEFORE finding the object present, so on every
            // process start they queued behind any conflicting holder (pg_dump, an idle-in-
            // transaction reader, an anti-wraparound vacuum) with every statement on the table,
            // from every host, queued behind them until the 30 s command timeout — and the next
            // operation did it again. Only what the catalog shows missing is altered or built now.
            try
            {
                // 1. The schema-shared DDL, under the schema's advisory key (shared with the
                //    transport and durable-flow stores): creates only, none of which locks a table
                //    that already exists. Serializes creation across processes — CREATE ... IF NOT
                //    EXISTS is not atomic against a concurrent create of the same object: two
                //    instances starting together both pass the existence check and collide on the
                //    system catalog ("duplicate key ... pg_type_typname_nsp_index").
                await using (var transaction = await PostgreSqlDdlGuard.BeginLockedTransactionAsync(connection, _schemaLockKey, cancellationToken).ConfigureAwait(false))
                {
                    await using (var command = connection.CreateCommand())
                    {
                        command.Transaction = transaction;
                        command.CommandText =
                            $"""
                            CREATE SCHEMA IF NOT EXISTS {Schema};

                            CREATE TABLE IF NOT EXISTS {RecoveryTable} (
                                correlation_id text NOT NULL,
                                registration_id uuid NOT NULL,
                                state_json text NOT NULL,
                                expires_at timestamptz NOT NULL,
                                registered_at timestamptz NOT NULL DEFAULT now(),
                                PRIMARY KEY (correlation_id, registration_id)
                            );

                            CREATE TABLE IF NOT EXISTS {MessageTable} (
                                id uuid PRIMARY KEY,
                                correlation_id text NOT NULL,
                                envelope_json text NOT NULL,
                                created_at timestamptz NOT NULL DEFAULT now(),
                                expires_at timestamptz NOT NULL,
                                acked_at timestamptz NULL,
                                acked_seq bigint NULL,
                                recovery_claimed boolean NOT NULL DEFAULT false
                            );
                            CREATE SEQUENCE IF NOT EXISTS {AckSequence} AS bigint;

                            CREATE TABLE IF NOT EXISTS {SubscriberTable} (
                                correlation_id text NOT NULL,
                                registration_id uuid NOT NULL,
                                instance_id text NOT NULL,
                                expires_at timestamptz NOT NULL,
                                PRIMARY KEY (correlation_id, registration_id)
                            );
                            """;
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }

                    var work = await ReadTableWorkAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
                    if (work.Length == 0)
                    {
                        await VerifyRelationsAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
                        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                        _created = true;
                        return;
                    }

                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                }

                // 2. The table work — columns an older build's table lacks, the one-time jsonb
                //    rewrite, index builds on existing tables — in a transaction of its own under a
                //    key scoped to this channel's tables: a rewrite held for up to an hour under the
                //    schema-wide key would stop every host starting meanwhile from initializing ANY
                //    AsyncResponse store on the schema. Re-read under the key: another host may have
                //    done the work while this one waited for it.
                await using (var transaction = await PostgreSqlDdlGuard.BeginLockedTransactionAsync(connection, _tableLockKey, cancellationToken).ConfigureAwait(false))
                {
                    var work = await ReadTableWorkAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
                    if (work.Length > 0)
                        await _ddl.ExecuteLongRunningAsync(work, connection, transaction, cancellationToken).ConfigureAwait(false);

                    // Options-level ValidateNamePlan keeps THIS component's names distinct, but the
                    // channel can share a schema with the transport and durable-flow stores (and
                    // unrelated objects), whose derived names it cannot see — and IF NOT EXISTS also
                    // accepts a same-name index with the WRONG definition. Verify against the
                    // catalog, in the DDL transaction, that every relation actually IS what the DDL
                    // intended, definitions included.
                    await VerifyRelationsAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                    _created = true;
                }
            }
            catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.WrongObjectType or PostgresErrorCodes.UndefinedColumn)
            {
                // E.g. CREATE INDEX ... ON a name that is really another component's index:
                // IF NOT EXISTS skipped the table create, and the dependent statement then hits
                // the wrong relation kind — surface the namespace collision instead of the raw
                // "cannot open relation".
                throw new InvalidOperationException(PostgreSqlRelationVerifier.DdlCollisionMessage("channel", _options.SchemaName), ex);
            }
            catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.LockNotAvailable)
            {
                // A lock wait lost anywhere in the DDL — the advisory key's included — latches the
                // retry-after window (a failed table-work step latched it already).
                _ddl.BackOff(ex);
                throw;
            }
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    /// <summary>
    /// The table work the auto-create DDL still owes, as one script (empty when there is none):
    /// the two message-table columns an older build's table lacks, the one-time jsonb → text
    /// conversion of the two document columns, and the indexes that do not exist yet. Each is
    /// included only when the catalog shows it missing — the statements take their table lock
    /// before any IF NOT EXISTS check, so the old unconditional script locked every table on every
    /// process start.
    /// </summary>
    /// <remarks>
    /// jsonb REJECTS the \u0000 escape that System.Text.Json emits for U+0000 (SQLSTATE 22P05), so
    /// any payload, exception message, propagated context value or callback argument containing a
    /// NUL was unpublishable on PostgreSQL alone while every other channel delivered it. Nothing here
    /// ever queries INSIDE the document — both columns are read back with ::text — so text costs
    /// nothing and accepts the whole contract.
    /// </remarks>
    private async Task<string> ReadTableWorkAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
        bool hasRecoveryClaimed, hasAckedSeq, envelopeJsonb, stateJsonb;
        await using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText =
                """
                SELECT
                  EXISTS (SELECT 1 FROM information_schema.columns
                          WHERE table_schema = @schema AND table_name = @messages AND column_name = 'recovery_claimed'),
                  EXISTS (SELECT 1 FROM information_schema.columns
                          WHERE table_schema = @schema AND table_name = @messages AND column_name = 'acked_seq'),
                  EXISTS (SELECT 1 FROM information_schema.columns
                          WHERE table_schema = @schema AND table_name = @messages AND column_name = 'envelope_json' AND data_type = 'jsonb'),
                  EXISTS (SELECT 1 FROM information_schema.columns
                          WHERE table_schema = @schema AND table_name = @recovery AND column_name = 'state_json' AND data_type = 'jsonb');
                """;
            command.Parameters.AddWithValue("schema", _options.SchemaName);
            command.Parameters.AddWithValue("messages", _options.MessageTable);
            command.Parameters.AddWithValue("recovery", _options.RecoveryStateTable);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            hasRecoveryClaimed = reader.GetBoolean(0);
            hasAckedSeq = reader.GetBoolean(1);
            envelopeJsonb = reader.GetBoolean(2);
            stateJsonb = reader.GetBoolean(3);
        }

        var work = new StringBuilder();
        if (!hasRecoveryClaimed)
            work.Append($"ALTER TABLE {MessageTable} ADD COLUMN IF NOT EXISTS recovery_claimed boolean NOT NULL DEFAULT false;\n");
        if (!hasAckedSeq)
            work.Append($"ALTER TABLE {MessageTable} ADD COLUMN IF NOT EXISTS acked_seq bigint NULL;\n");
        if (envelopeJsonb)
            work.Append($"ALTER TABLE {MessageTable} ALTER COLUMN envelope_json TYPE text USING envelope_json::text;\n");
        if (stateJsonb)
            work.Append($"ALTER TABLE {RecoveryTable} ALTER COLUMN state_json TYPE text USING state_json::text;\n");

        foreach (var (index, table, keys) in Indexes())
        {
            if (await PostgreSqlDdlGuard.GetIndexStateAsync(connection, transaction, _options.SchemaName, index, cancellationToken).ConfigureAwait(false)
                is PostgreSqlDdlGuard.IndexState.Absent)
            {
                work.Append($"CREATE INDEX IF NOT EXISTS {Quote(index)} ON {Schema}.{Quote(table)} ({keys});\n");
            }
        }

        return work.ToString();
    }

    /// <summary>The channel's indexes: name, owning table, key columns.</summary>
    private (string Index, string Table, string Keys)[] Indexes() =>
    [
        (IndexName(_options.RecoveryStateTable, "expires"), _options.RecoveryStateTable, "expires_at"),
        (IndexName(_options.MessageTable, "correlation_created"), _options.MessageTable, "correlation_id, created_at"),
        (IndexName(_options.MessageTable, "expires"), _options.MessageTable, "expires_at"),
        (IndexName(_options.SubscriberTable, "expires"), _options.SubscriberTable, "expires_at"),
    ];

    private Task VerifyRelationsAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
        => PostgreSqlRelationVerifier.VerifyAsync(
            connection,
            transaction,
            _options.SchemaName,
            "channel",
            [
                new(_options.RecoveryStateTable, 'r', Columns:
                    [
                        new("correlation_id", "text", Nullable: false, RequiresDeterministicCollation: true),
                        new("registration_id", "uuid", Nullable: false, RequiresDeterministicCollation: true),
                        new("state_json", "text", Nullable: false),
                        new("expires_at", "timestamp with time zone", Nullable: false),
                        new("registered_at", "timestamp with time zone", Nullable: false, DefaultExpression: "now()"),
                    ], PrimaryKey: ["correlation_id", "registration_id"]),
                new(_options.MessageTable, 'r', Columns:
                    [
                        new("id", "uuid", Nullable: false),
                        new("correlation_id", "text", Nullable: false, RequiresDeterministicCollation: true),
                        new("envelope_json", "text", Nullable: false),
                        new("created_at", "timestamp with time zone", Nullable: false, DefaultExpression: "now()"),
                        new("expires_at", "timestamp with time zone", Nullable: false),
                        new("acked_at", "timestamp with time zone", Nullable: true),
                        new("acked_seq", "bigint", Nullable: true),
                        new("recovery_claimed", "boolean", Nullable: false, DefaultExpression: "false"),
                    ], PrimaryKey: ["id"]),
                new(_options.SubscriberTable, 'r', Columns:
                    [
                        new("correlation_id", "text", Nullable: false, RequiresDeterministicCollation: true),
                        new("registration_id", "uuid", Nullable: false, RequiresDeterministicCollation: true),
                        new("instance_id", "text", Nullable: false),
                        new("expires_at", "timestamp with time zone", Nullable: false),
                    ], PrimaryKey: ["correlation_id", "registration_id"]),
                new(AckSequenceName, 'S'),
                new(IndexName(_options.RecoveryStateTable, "expires"), 'i', _options.RecoveryStateTable, ["expires_at"]),
                new(IndexName(_options.MessageTable, "correlation_created"), 'i', _options.MessageTable, ["correlation_id", "created_at"]),
                new(IndexName(_options.MessageTable, "expires"), 'i', _options.MessageTable, ["expires_at"]),
                new(IndexName(_options.SubscriberTable, "expires"), 'i', _options.SubscriberTable, ["expires_at"]),
            ],
            cancellationToken);


    private async Task ValidateManagedSchemaAsync(CancellationToken cancellationToken)
    {
        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            bool hasColumn;
            bool hasSequence;
            // The probe's command and reader are scoped so they are disposed before the relation
            // verification below reuses this connection — Npgsql allows one command in progress.
            await using (var command = connection.CreateCommand())
            {
                // relkind = 'S' precisely: to_regclass matches ANY relation, so a table sharing the
                // sequence's name (the pre-fix truncation collision) passed validation and failed at
                // the first nextval instead.
                command.CommandText =
                    """
                    SELECT
                      EXISTS (SELECT 1 FROM information_schema.columns
                              WHERE table_schema = @schema AND table_name = @table AND column_name = 'acked_seq'),
                      EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                              WHERE n.nspname = @schema AND c.relname = @sequence AND c.relkind = 'S');
                    """;
                command.Parameters.AddWithValue("schema", _options.SchemaName);
                command.Parameters.AddWithValue("table", _options.MessageTable);
                command.Parameters.AddWithValue("sequence", AckSequenceName);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                hasColumn = reader.GetBoolean(0);
                hasSequence = reader.GetBoolean(1);
            }
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

            // Full relation verification on the managed path too (transport/flow-store parity):
            // an operator-provisioned table with the wrong shape — a nondeterministic
            // correlation_id collation above all — previously passed startup here and
            // misrouted silently at runtime, which is exactly what verification exists to catch.
            await VerifyRelationsAsync(connection, transaction: null, cancellationToken).ConfigureAwait(false);

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
        command.Parameters.Add("state_json", NpgsqlDbType.Text).Value = AsyncResponseJson.Serialize(state);
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
    /// a retried publish never strands an active waiter. Returns the same-process fast-path
    /// message carrying the row's server-stamped <c>created_at</c> — and, on a duplicate, the
    /// ORIGINAL row's settlement columns, so the fast path compares against subscription
    /// watermarks exactly as the sweep does (a fabricated null <c>acked_at</c> replayed an
    /// already-consumed response to a waiter registered after the ack).
    /// </summary>
    public Task<PostgreSqlChannelMessage> InsertMessageAsync(Guid id, string correlationId, string envelopeJson, TimeSpan retention, CancellationToken cancellationToken)
        => AsyncResponseRetry.ExecuteAsync(
            token => InsertMessageOnceAsync(id, correlationId, envelopeJson, retention, token),
            IsTransient,
            _options.PublishMaxAttempts,
            _options.PublishRetryBaseDelay,
            _options.PublishRetryMaxDelay,
            cancellationToken);

    private async Task<PostgreSqlChannelMessage> InsertMessageOnceAsync(Guid id, string correlationId, string envelopeJson, TimeSpan retention, CancellationToken cancellationToken)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (ShouldPrune(ref _lastMessagePruneTicks))
            await PruneExpiredMessagesAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // Single statement: the final SELECT both fires the NOTIFY exactly once and returns the
        // fresh row's server-stamped created_at via RETURNING — NULL when the idempotent insert
        // hit a duplicate, which the separate lookup below resolves.
        command.CommandText =
            $"""
            WITH inserted AS (
                INSERT INTO {MessageTable} (id, correlation_id, envelope_json, expires_at)
                VALUES (@id, @correlation_id, @envelope_json, now() + @retention)
                ON CONFLICT (id) DO NOTHING
                RETURNING created_at
            )
            SELECT (SELECT created_at FROM inserted) AS created_at,
                   pg_notify(@channel, @payload);
            """;
        command.Parameters.AddWithValue("id", id);
        command.Parameters.AddWithValue("correlation_id", correlationId);
        command.Parameters.Add("envelope_json", NpgsqlDbType.Text).Value = envelopeJson;
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
            return new PostgreSqlChannelMessage(id, correlationId, envelopeJson, stamped);

        // Duplicate: a publish retry, or a CONCURRENT idempotent publish (ON CONFLICT detects the
        // other transaction's row against latest data, while a same-statement subquery would read
        // under this statement's older snapshot — reproduced on PostgreSQL 16). A fresh statement
        // gets a fresh read-committed snapshot and resolves both deterministically, and it reads
        // the original row's settlement columns for the fast-path watermark.
        await using var lookup = connection.CreateCommand();
        lookup.CommandText = $"SELECT created_at, acked_at, acked_seq FROM {MessageTable} WHERE id = @id;";
        lookup.Parameters.AddWithValue("id", id);
        await using var existing = await lookup.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (await existing.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return new PostgreSqlChannelMessage(
                id,
                correlationId,
                envelopeJson,
                existing.GetFieldValue<DateTimeOffset>(0),
                existing.IsDBNull(1) ? null : existing.GetFieldValue<DateTimeOffset>(1),
                existing.IsDBNull(2) ? null : existing.GetInt64(2));
        }

        // Only reachable when the duplicate's original row is genuinely gone (pruned
        // mid-publish): the message is not persisted, and reporting success with a fabricated
        // app-clock timestamp would both lie about persistence and feed a client clock into
        // the server-clock watermark.
        throw new InvalidOperationException(
            $"PostgreSQL response insert for message {id} found no row after a duplicate: the original no longer exists (pruned). The response is not persisted.");
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
        // The envelope travels only for rows nobody has acknowledged yet. Acknowledged rows are
        // the consumed history the sweep re-reads on every tick (they stay in the result set so a
        // fan-out waiter in ANOTHER process still receives them): shipping their bodies with each
        // sweep made a long-lived progress subscription's cost grow with its whole retained
        // history. The shared sweep fetches the envelope by id for the rare acknowledged row a
        // live subscription has not seen.
        command.CommandText =
            $"""
            SELECT id, correlation_id, CASE WHEN acked_at IS NULL THEN envelope_json::text END, created_at, acked_at, acked_seq
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

        return await ReadMessagesAsync(command, batchSize, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The full rows (envelope included) for <paramref name="ids"/> under
    /// <paramref name="correlationId"/>, in sweep order — how the dispatch sweep hydrates the
    /// header-only acknowledged rows it still has to deliver. A row pruned between the sweep's
    /// page and this read is simply absent.
    /// </summary>
    public async Task<IReadOnlyList<PostgreSqlChannelMessage>> LoadMessagesByIdAsync(
        string correlationId,
        IReadOnlyList<Guid> ids,
        CancellationToken cancellationToken)
    {
        if (ids.Count == 0)
            return [];

        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            SELECT id, correlation_id, envelope_json::text, created_at, acked_at, acked_seq
            FROM {MessageTable}
            WHERE correlation_id = @correlation_id
              AND id = ANY(@ids)
              AND expires_at > now()
            ORDER BY created_at, id;
            """;
        command.Parameters.AddWithValue("correlation_id", correlationId);
        command.Parameters.AddWithValue("ids", ids is Guid[] array ? array : [.. ids]);
        return await ReadMessagesAsync(command, ids.Count, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<IReadOnlyList<PostgreSqlChannelMessage>> ReadMessagesAsync(NpgsqlCommand command, int capacity, CancellationToken cancellationToken)
    {
        var messages = new List<PostgreSqlChannelMessage>(capacity);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            messages.Add(new PostgreSqlChannelMessage(
                reader.GetGuid(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
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
        // The sequence is stamped ONLY when this same update transitions acked_at from null (SET
        // expressions read the pre-update row): a row acked by a pre-sequence build must stay
        // permanently unsequenced. Back-filling it on a later fan-out re-claim would pair an OLD
        // acked_at with a FRESH sequence value, and a waiter that registered in the original ack's
        // tick would then read the tie as post-registration fan-out — replaying a response its
        // predecessor consumed.
        command.CommandText =
            $"""
            UPDATE {MessageTable}
            SET acked_at = COALESCE(acked_at, now()),
                acked_seq = CASE WHEN acked_at IS NULL THEN nextval('{AckSequence}') ELSE acked_seq END
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
            await PruneExpiredSubscribersAsync(cancellationToken).ConfigureAwait(false);

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
            await PruneExpiredSubscribersAsync(cancellationToken).ConfigureAwait(false);

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

    /// <summary>
    /// LISTENs on the notification channel and invokes <paramref name="onNotification"/> with every
    /// NOTIFY payload until cancellation or a connection failure; <paramref name="onListening"/>
    /// runs once the LISTEN is established and a delivery probe has come back through it.
    /// </summary>
    public async Task ExecuteListenAsync(Func<string?, Task> onNotification, CancellationToken cancellationToken, Action? onListening = null)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        // Not `await using`: the release below owns disposal, and may finish it after this returns.
        var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var listening = false;
        var probe = new ListenProbe();
        try
        {
            // The probe's own notification is consumed here, never handed on as a wake. Other
            // processes' probes are handed on like any payload and dropped by the channel, which
            // holds no waiter under a probe payload.
            connection.Notification += (_, args) =>
            {
                if (!probe.TryComplete(args.Payload))
                    _ = onNotification(args.Payload);
            };
            await using (var command = connection.CreateCommand())
            {
                command.CommandText = $"LISTEN {Quote(NotificationChannel)};";
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            listening = true;

            // A LISTEN that succeeded proves the command ran, not that notifications will arrive
            // on THIS connection: behind a transaction-mode pooler (PgBouncer pool_mode=transaction
            // or statement) the LISTEN ran on a server connection that went straight back to the
            // pool, and nothing published afterwards reaches it. The channel then trusted a push
            // wake that never came and kept the full-sweep throttle — whose 5 s default equals the
            // publisher's confirmation budget — for the life of the process, so cross-process
            // responses routinely lost the race to lost-subscriber recovery under live waiters.
            // Only a notification this connection sends itself and receives back proves delivery.
            await ProbeListenDeliveryAsync(connection, probe, cancellationToken).ConfigureAwait(false);
            onListening?.Invoke();

            // A BOUNDED wait with a probe on every quiet interval. An unbounded WaitAsync on a
            // half-open socket (a NAT or load balancer silently dropping an idle LISTEN
            // connection; Npgsql's keepalive is off by default) blocked forever while the channel
            // kept trusting a push wake that could no longer deliver. A failed, timed-out or
            // undelivered probe throws into the listen loop's reconnect path. (It used to be a
            // SELECT 1, which proves the socket, not delivery.)
            while (!cancellationToken.IsCancellationRequested)
            {
                if (!await connection.WaitAsync(ListenLivenessInterval, cancellationToken).ConfigureAwait(false))
                    await ProbeListenDeliveryAsync(connection, probe, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            // UNLISTEN before the pool gets it back, bounded so a half-open socket cannot hold
            // disposal (see PostgreSqlListenConnection).
            await PostgreSqlListenConnection.ReleaseAsync(_dataSource, connection, listening, _logger).ConfigureAwait(false);
        }
    }

    /// <summary>How long the LISTEN connection may stay silent before it is probed. Settable only so a test can shorten it.</summary>
    internal TimeSpan ListenLivenessInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// How long a delivery probe — the <c>NOTIFY</c> and its notification coming back — may take
    /// before the listen connection is treated as not delivering. Settable only so a test can
    /// shorten it.
    /// </summary>
    internal TimeSpan ListenProbeTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Sends a <c>NOTIFY</c> with a fresh payload on the listen connection itself and waits for
    /// PostgreSQL to deliver it back (a session listening on a channel receives its own
    /// notifications too). Throws when the command fails or the notification does not arrive
    /// within <see cref="ListenProbeTimeout"/>.
    /// </summary>
    private async Task ProbeListenDeliveryAsync(NpgsqlConnection connection, ListenProbe probe, CancellationToken cancellationToken)
    {
        var startedAt = System.Diagnostics.Stopwatch.GetTimestamp();
        var payload = probe.Arm();
        await using (var notify = connection.CreateCommand())
        {
            // A literal, not a parameter: NOTIFY is a utility statement. The payload is a fixed
            // prefix and a hex GUID (no quote can occur in it), the channel a validated identifier.
            notify.CommandText = $"NOTIFY {Quote(NotificationChannel)}, '{payload}';";
            notify.CommandTimeout = Math.Max(1, (int)Math.Ceiling(ListenProbeTimeout.TotalSeconds));
            await notify.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // Usually already delivered with the command's reply (PostgreSQL sends a session its own
        // notification at commit); otherwise it arrives on the connection shortly after.
        while (!probe.Delivered)
        {
            var remaining = ListenProbeTimeout - System.Diagnostics.Stopwatch.GetElapsedTime(startedAt);
            if (ProbeBudgetSpent(remaining) || !await connection.WaitAsync(remaining, cancellationToken).ConfigureAwait(false))
            {
                if (probe.Delivered)
                    break;
                throw new TimeoutException(
                    $"The PostgreSQL LISTEN connection did not receive its own delivery-probe notification on channel '{NotificationChannel}' within {ListenProbeTimeout}: " +
                    "notifications are not reaching it. The channel needs a session-pooled or direct connection for LISTEN — a transaction- or statement-mode pooler " +
                    "(PgBouncer pool_mode=transaction) never delivers them — or the connection is dead. Reconnecting; meanwhile the sweep is the only cross-process wake.");
            }
        }
    }

    /// <summary>
    /// Whether the probe's <paramref name="remaining"/> budget is spent. Under a millisecond counts:
    /// Npgsql's <c>WaitAsync(TimeSpan)</c> truncates to whole milliseconds, and a 0 ms timeout means
    /// wait FOREVER — the probe's last fraction of a millisecond became the unbounded wait on a
    /// half-open socket that the probe exists to catch.
    /// </summary>
    internal static bool ProbeBudgetSpent(TimeSpan remaining) => remaining < TimeSpan.FromMilliseconds(1);

    /// <summary>
    /// The pending delivery probe of one listen connection: the payload it waits for and whether
    /// it came back. Completed from Npgsql's notification callback, armed and read by the listen
    /// loop.
    /// </summary>
    private sealed class ListenProbe
    {
        private const string PayloadPrefix = "asyncresponse:listen-probe:";

        // One object per probe, swapped whole: a callback completing an earlier probe can never
        // mark a later one delivered.
        private Pending? _pending;

        public bool Delivered => Volatile.Read(ref _pending)?.Delivered == true;

        public string Arm()
        {
            var pending = new Pending(PayloadPrefix + Guid.NewGuid().ToString("N"));
            Volatile.Write(ref _pending, pending);
            return pending.Payload;
        }

        /// <summary>Whether <paramref name="payload"/> is the pending probe's; completes it if so.</summary>
        public bool TryComplete(string? payload)
        {
            if (Volatile.Read(ref _pending) is not { } pending || !string.Equals(payload, pending.Payload, StringComparison.Ordinal))
                return false;

            pending.Delivered = true;
            return true;
        }

        private sealed class Pending(string payload)
        {
            public string Payload { get; } = payload;
            public volatile bool Delivered;
        }
    }

    // Every prune below is opportunistic housekeeping riding on a publish, a waiter registration,
    // a liveness probe, or a recovery load/scan, and goes through OpportunisticPrune: bounded
    // batches drained under a budget, and every failure logged and swallowed. Awaited bare, a prune
    // that lost a lock wait or a connection failed the operation it rode on (a waiter could not
    // register, a publish failed) after ShouldPrune had already consumed the interval; and the
    // unbounded table-wide DELETE on the publish path held that publish for the whole command
    // timeout over a large expired backlog, rolled back, and never shrank it. Reads filter on
    // expires_at, so a skipped prune costs only disk until the next window.

    private Task PruneExpiredRecoveryAsync(string? correlationId, CancellationToken cancellationToken)
        => OpportunisticPrune.DrainQuietlyAsync(
            token => PruneExpiredRecoveryBatchAsync(correlationId, token),
            _logger,
            "PostgreSQL channel recovery-state prune",
            cancellationToken);

    private async Task<int> PruneExpiredRecoveryBatchAsync(string? correlationId, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = correlationId is null
            ? ExpiredPruneSql(RecoveryTable)
            : $"DELETE FROM {RecoveryTable} WHERE correlation_id = @correlation_id AND expires_at <= now();";
        if (correlationId is not null)
            command.Parameters.AddWithValue("correlation_id", correlationId);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private Task PruneExpiredMessagesAsync(CancellationToken cancellationToken)
        => OpportunisticPrune.DrainQuietlyAsync(
            token => PruneExpiredBatchAsync(MessageTable, token),
            _logger,
            "PostgreSQL channel message prune",
            cancellationToken);

    /// <summary>
    /// Table-wide, not scoped to the calling waiter's correlation id: a scoped prune never reached
    /// the rows of a process that crashed with waiters in flight (correlation ids are rarely
    /// reused), so those orphans stayed forever and the expires index built for this delete went
    /// unused. Bounded, and throttled by <see cref="PostgreSqlAsyncResponseChannelOptions.PruneInterval"/>.
    /// </summary>
    private Task PruneExpiredSubscribersAsync(CancellationToken cancellationToken)
        => OpportunisticPrune.DrainQuietlyAsync(
            token => PruneExpiredBatchAsync(SubscriberTable, token),
            _logger,
            "PostgreSQL channel subscriber prune",
            cancellationToken);

    private async Task<int> PruneExpiredBatchAsync(string table, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = ExpiredPruneSql(table);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// One bounded batch of a table-wide expiry prune for <paramref name="table"/> (the durable-flow
    /// stores' <c>ctid … LIMIT</c> shape, SQL Server channel <c>TOP (1000)</c> parity).
    /// </summary>
    internal static string ExpiredPruneSql(string table)
        => $"DELETE FROM {table} WHERE ctid IN (SELECT ctid FROM {table} WHERE expires_at <= now() LIMIT {OpportunisticPrune.BatchSize});";

    public static void ValidateIdentifier(string? value, string name)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new InvalidOperationException($"{nameof(PostgreSqlAsyncResponseChannelOptions)}.{name} must be configured.");
        if (!IsIdentifier(value))
            throw new InvalidOperationException(
                $"{nameof(PostgreSqlAsyncResponseChannelOptions)}.{name} '{value}' must be a simple PostgreSQL identifier (letters, digits, and underscores; not starting with a digit).");
        // PostgreSQL TRUNCATES over-limit identifiers silently (a NOTICE, not an error), so an
        // over-limit configured name would create/address an object under a different name.
        if (value.Length > IdentifierCap)
            throw new InvalidOperationException(
                $"{nameof(PostgreSqlAsyncResponseChannelOptions)}.{name} '{value}' is {value.Length} characters; PostgreSQL identifiers are limited to {IdentifierCap} and longer names are silently truncated.");
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

    /// <summary>PostgreSQL's identifier length cap (NAMEDATALEN - 1); longer names are silently truncated server-side.</summary>
    internal const int IdentifierCap = 63;

    // Suffix space is RESERVED before capping in BOTH derived-name helpers (see
    // RelationalNamePlan.DerivedName, the shared implementation): truncating the whole
    // "{table}{suffix}" let a maximum-length table name produce exactly the table's own name — the
    // derived object then collided with the table (indexes, sequences, and tables share one
    // relation namespace), CREATE ... IF NOT EXISTS silently skipped it, and the store ran with a
    // missing sequence (runtime failure) or missing indexes (silent full scans).
    private static string IndexName(string table, string suffix)
        => RelationalNamePlan.DerivedName(table, $"_{suffix}_idx", IdentifierCap);

    private static string SequenceName(string table)
        => RelationalNamePlan.DerivedName(table, "_ack_seq", IdentifierCap);

    /// <summary>
    /// Validates the complete effective object-name plan — configured tables plus every derived
    /// index and sequence name — for pairwise distinctness. Suffix reservation makes one table's
    /// derived names collision-free, but two long tables whose reserved stems truncate identically
    /// still derive the same index name, and a configured table can occupy a derived name outright;
    /// either way <c>CREATE ... IF NOT EXISTS</c> silently skips the object. Comparison is
    /// case-insensitive: the DDL quotes identifiers (case-sensitive to PostgreSQL), but a plan
    /// distinct only by letter case is a misconfiguration magnet and is rejected for parity with
    /// SQL Server's case-insensitive catalogs.
    /// </summary>
    public static void ValidateNamePlan(PostgreSqlAsyncResponseChannelOptions options)
    {
        (string Role, string Name)[] plan =
        [
            ($"{nameof(options.RecoveryStateTable)} table", options.RecoveryStateTable),
            ($"{nameof(options.MessageTable)} table", options.MessageTable),
            ($"{nameof(options.SubscriberTable)} table", options.SubscriberTable),
            ("ack sequence (derived from MessageTable)", SequenceName(options.MessageTable)),
            ("RecoveryStateTable expiry index", IndexName(options.RecoveryStateTable, "expires")),
            ("MessageTable correlation index", IndexName(options.MessageTable, "correlation_created")),
            ("MessageTable expiry index", IndexName(options.MessageTable, "expires")),
            ("SubscriberTable expiry index", IndexName(options.SubscriberTable, "expires")),
        ];
        RelationalNamePlan.RequireDistinct(
            plan,
            nameof(PostgreSqlAsyncResponseChannelOptions),
            ". All tables and the index/sequence names derived from them share one namespace and must be distinct " +
            "(long names reserve suffix space by truncating the table stem, which can make distinct tables derive " +
            "the same name). Shorten or de-overlap the configured table names.");
    }

    /// <summary>NOTIFY payload for a publish: the correlation id, or empty when it is too long to carry.</summary>
    private static string NotifyPayload(string correlationId)
        => Encoding.UTF8.GetByteCount(correlationId) <= MaxNotifyPayloadBytes ? correlationId : string.Empty;

    internal static bool IsTransient(Exception exception) => PostgreSqlTransientFaults.IsTransient(exception);

    /// <summary>
    /// Stable 64-bit advisory-lock key for serializing schema creation. Uses FNV-1a over a
    /// schema-scoped discriminator: it must be deterministic across processes (so
    /// <see cref="string.GetHashCode()"/>, which is per-process randomized, is unusable) and identical
    /// to the transport store's key for the same schema so both serialize their shared CREATE SCHEMA.
    /// </summary>
    internal static long SchemaAdvisoryLockKey(string schemaName) => PostgreSqlDdlGuard.SchemaLockKey(schemaName);

    /// <summary>
    /// Time-gates opportunistic pruning so the housekeeping DELETE runs at most once per
    /// <see cref="PostgreSqlAsyncResponseChannelOptions.PruneInterval"/> instead of on every operation.
    /// Read queries already filter on <c>expires_at</c>, so throttling pruning never affects correctness.
    /// Monotonic (see <see cref="OpportunisticPrune.ShouldRun"/>): a backward wall-clock step no
    /// longer suspends the housekeeping for the size of the step.
    /// </summary>
    private bool ShouldPrune(ref long lastTicks)
        => OpportunisticPrune.ShouldRun(ref lastTicks, _options.PruneInterval);
}
