using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.MySql;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using MySqlConnector;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>DI registration for the MySQL/MariaDB durable-flow state store.</summary>
    public static class MySqlDurableFlowServiceCollectionExtensions
    {
        /// <summary>Stores durable-flow state in MySQL or MariaDB.</summary>
        public static AsyncResponseRegistrationBuilder WithMySqlDurableFlows(
            this AsyncResponseRegistrationBuilder builder,
            Action<MySqlDurableFlowOptions>? configure = null)
        {
            // Singleton on purpose: schema provisioning is cached per store instance, and the
            // executor resolves the store from a fresh scope per flow execution — a scoped store
            // would re-run EnsureCreated's DDL round-trip on every run.
            builder.Services.TryAddSingleton<MySqlFlowStateStore>();
            return builder.WithDurableFlows<MySqlFlowStateStore, MySqlDurableFlowOptions>(configure);
        }
    }
}

namespace AsyncResponse.DurableFlows.MySql
{
/// <summary>Options for the MySQL/MariaDB durable-flow state store.</summary>
public sealed class MySqlDurableFlowOptions : DurableFlowOptions
{
    /// <summary>MySQL or MariaDB connection string. Required.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Table storing one durable-flow ledger row per flow id.</summary>
    public string TableName { get; set; } = "asyncresponse_flow_state";

    /// <summary>Creates the table and expiry index on first use.</summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// How often <see cref="MySqlFlowStateStore.TryCreateAsync"/> opportunistically deletes one bounded
    /// batch (1000 rows) of expired rows (loads already treat expired state as absent; pruning
    /// bounds table growth). Zero or negative prunes on every save. Default: 5 minutes.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum serialized flow-state size in bytes accepted by writes; oversized ledgers fail fast
    /// with an actionable error instead of an opaque provider error. Default: <c>null</c>
    /// (unlimited — <c>longtext</c> holds up to 4 GB), settable as an operator budget.
    /// </summary>
    public long? MaxStateBytes { get; set; }

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        DurableFlowStoreShared.ValidateConnectionString(ConnectionString, nameof(MySqlDurableFlowOptions));
        DurableFlowStoreShared.ValidateIdentifier(TableName, $"{nameof(MySqlDurableFlowOptions)}.{nameof(TableName)}", "MySQL", identifierCap: 64);
        DurableFlowStoreShared.ValidateMaxStateBytes(MaxStateBytes, nameof(MySqlDurableFlowOptions));
    }
}

/// <summary>MySQL/MariaDB implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class MySqlFlowStateStore : IFlowStateStore
{
    private const int PruneBatchSize = 1000;

    /// <summary>
    /// SQL expression adding a millisecond bigint parameter to the database clock. All expiry and
    /// lease math runs on <c>UTC_TIMESTAMP(6)</c> (statement-stable, like <c>NOW()</c>) so app
    /// clock skew can never fence a lease in or out; microsecond arithmetic keeps
    /// <c>datetime(6)</c> precision.
    /// </summary>
    private static string AddMilliseconds(string parameterName)
        => $"TIMESTAMPADD(MICROSECOND, {parameterName} * 1000, UTC_TIMESTAMP(6))";

    private readonly MySqlDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private long _lastPruneTicks;
    private volatile bool _created;

    public MySqlFlowStateStore(IOptions<MySqlDurableFlowOptions> options)
    {
        _options = options.Value;
        _options.Validate();
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT state_json, revision FROM {Table} WHERE flow_id = @flow_id AND expires_at_utc > UTC_TIMESTAMP(6);";
        command.Parameters.AddWithValue("@flow_id", flowId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        return DurableFlowStoreShared.ReadState(flowId, reader.GetString(0), reader.GetInt64(1));
    }

    public async Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "MySQL");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await DurableFlowStoreShared.PruneQuietlyAsync(() => PruneExpiredAsync(cancellationToken)).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {Table} (flow_id, state_json, expires_at_utc, updated_at_utc, revision)
            VALUES (@flow_id, @state_json, {AddMilliseconds("@ttl_ms")}, UTC_TIMESTAMP(6), @revision);
            """;
        command.Parameters.AddWithValue("@flow_id", flowId);
        command.Parameters.AddWithValue("@state_json", stateJson);
        command.Parameters.AddWithValue("@ttl_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(ttl));
        command.Parameters.AddWithValue("@revision", state.Revision);
        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (MySqlException exception) when (exception.Number == 1062)
        {
            // 1062 says "some unique constraint rejected this row" — NOT "this flow id exists".
            // The distinction is load-bearing on a table this build did not create: a legacy
            // PREFIX key alongside the required one (UNIQUE (flow_id(100))) raises 1062 for a
            // DIFFERENT id that happens to share a prefix, and reading that as "already exists"
            // would report a successful start for a flow with no row and no run. Startup
            // verification refuses such tables, but this store also runs against schemas it did not
            // get to inspect first (AutoCreateSchema off, table created later), so confirm the row
            // is actually there before believing the error.
            if (!await ExistsAsync(flowId, cancellationToken).ConfigureAwait(false))
                throw;

            // The id already exists. Only an expired row may be replaced below; do not use
            // INSERT IGNORE here because it also suppresses truncation and other data errors.
        }

        // Exactly one caller can replace an expired ledger: after its conditional update, every
        // competing caller sees the new future expiry and returns false. This avoids relying on
        // MySQL's configurable "changed rows" versus "matched rows" result semantics.
        command.CommandText =
            $"""
            UPDATE {Table}
            SET state_json = @state_json,
                revision = @revision,
                lease_id = NULL,
                lease_expires_at_utc = NULL,
                updated_at_utc = UTC_TIMESTAMP(6),
                expires_at_utc = {AddMilliseconds("@ttl_ms")}
            WHERE flow_id = @flow_id AND expires_at_utc <= UTC_TIMESTAMP(6);
            """;
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    /// <summary>
    /// Whether a row with EXACTLY this flow id exists, expired or not — the question a 1062 does
    /// not answer on its own. Opens its own connection so it is safe to call mid-operation.
    /// </summary>
    private async Task<bool> ExistsAsync(string flowId, CancellationToken cancellationToken)
    {
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT 1 FROM {Table} WHERE flow_id = @flow_id LIMIT 1;";
        command.Parameters.AddWithValue("@flow_id", flowId);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not null;
    }

    public async Task<bool> TryUpdateAsync(
        string flowId,
        FlowState state,
        long expectedRevision,
        TimeSpan ttl,
        string? leaseId = null,
        CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateUpdate(flowId, state, expectedRevision, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "MySQL");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            UPDATE {Table}
            SET state_json = @state_json,
                expires_at_utc = {AddMilliseconds("@ttl_ms")},
                updated_at_utc = UTC_TIMESTAMP(6),
                revision = @new_revision
            WHERE flow_id = @flow_id
              AND revision = @expected_revision
              AND expires_at_utc > UTC_TIMESTAMP(6)
              AND (@lease_id IS NULL OR (lease_id = @lease_id AND lease_expires_at_utc > UTC_TIMESTAMP(6)));
            """;
        command.Parameters.AddWithValue("@flow_id", flowId);
        command.Parameters.AddWithValue("@state_json", stateJson);
        command.Parameters.AddWithValue("@ttl_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(ttl));
        command.Parameters.AddWithValue("@expected_revision", expectedRevision);
        command.Parameters.AddWithValue("@new_revision", state.Revision);
        command.Parameters.AddWithValue("@lease_id", (object?)leaseId ?? DBNull.Value);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: true, cancellationToken);

    public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: false, cancellationToken);

    public async Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
    {
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"UPDATE {Table} SET lease_id = NULL, lease_expires_at_utc = NULL WHERE flow_id = @flow_id AND lease_id = @lease_id;";
        command.Parameters.AddWithValue("@flow_id", flowId);
        command.Parameters.AddWithValue("@lease_id", leaseId);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {Table} WHERE flow_id = @flow_id;";
        command.Parameters.AddWithValue("@flow_id", flowId);
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    private async Task PruneExpiredAsync(CancellationToken cancellationToken)
    {
        // One bounded batch per prune interval (policy shared by all relational stores): an
        // unbatched DELETE over a large expired backlog holds row locks and bloats one
        // transaction for the unlucky create that triggered the prune. Loads already filter on
        // expiry, so any backlog beyond the batch just waits for the next interval.
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {Table} WHERE expires_at_utc <= UTC_TIMESTAMP(6) LIMIT {PruneBatchSize};";
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task EnsureCreatedAsync(CancellationToken cancellationToken)
    {
        if (_created)
            return;

        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            // Row-count semantics guard: lease renewal and update fencing treat ExecuteNonQuery's
            // result as ROWS MATCHED, MySqlConnector's default (UseAffectedRows=false). With
            // UseAffectedRows=true the result becomes rows CHANGED, so an UPDATE that rewrites
            // identical values (a renewal landing in the same microsecond as the stored expiry, a
            // stalled clock) reports 0 and a healthy execution aborts as "lease lost" — sporadic
            // and unattributable from logs. Every other silently-breaking property (charset,
            // collation, column shape, keys) fails startup in VerifyFlowTableAsync below; the
            // connection string gets the same treatment.
            if (new MySqlConnectionStringBuilder(_options.ConnectionString!).UseAffectedRows)
            {
                throw new InvalidOperationException(
                    $"{nameof(MySqlDurableFlowOptions)}.{nameof(MySqlDurableFlowOptions.ConnectionString)} sets UseAffectedRows=true, " +
                    "which switches ExecuteNonQuery from rows-MATCHED to rows-CHANGED semantics and silently breaks this store's " +
                    "lease renewal and update fencing. Remove UseAffectedRows from the connection string; the MySqlConnector " +
                    "default (false) is required.");
            }

            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            if (_options.AutoCreateSchema)
            {
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    CREATE TABLE IF NOT EXISTS {Table} (
                        flow_id varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
                        state_json longtext CHARACTER SET utf8mb4 NOT NULL,
                        expires_at_utc datetime(6) NOT NULL,
                        updated_at_utc datetime(6) NOT NULL,
                        revision bigint NOT NULL DEFAULT 0,
                        lease_id varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
                        lease_expires_at_utc datetime(6) NULL,
                        INDEX {IndexName} (expires_at_utc)
                    );
                    """;
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            // Latch only when the table was actually verified (Oracle parity): an absent table
            // under AutoCreateSchema = false must keep re-verifying, or a migration that lands
            // AFTER the first operation would never have its unique-key/collation/charset checks
            // run for the process lifetime.
            _created = await VerifyFlowTableAsync(connection, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    /// <summary>
    /// Checks the table this store will actually use, independently of who created it.
    /// <c>CREATE TABLE IF NOT EXISTS</c> leaves a table made by an earlier build (or by hand)
    /// exactly as it was, and <c>AutoCreateSchema = false</c> issues no DDL at all — so the DDL
    /// above only ever protects a table this build created. Two properties of that table are
    /// load-bearing and both fail SILENTLY when absent, which is why they are checked at startup
    /// rather than left to the first query:
    /// <list type="bullet">
    /// <item><description>
    /// A UNIQUE key on flow_id alone. <see cref="TryCreateAsync"/> is the engine's insert-if-absent
    /// primitive and detects "already exists" from MySQL's duplicate-key error 1062 — with no such
    /// key nothing raises 1062, so two concurrent starts of ONE flow id both report success and the
    /// ledger gets two rows.
    /// </description></item>
    /// <item><description>
    /// A binary collation on flow_id. MySQL's default is case-insensitive, which makes two ids
    /// differing only in case (or accent, or width) one key: the second start fails as a duplicate
    /// and a load returns the other run's state.
    /// </description></item>
    /// </list>
    /// </summary>
    private async Task<bool> VerifyFlowTableAsync(MySqlConnection connection, CancellationToken cancellationToken)
    {
        var columns = new Dictionary<string, ActualColumn>(StringComparer.OrdinalIgnoreCase);
        await using (var command = connection.CreateCommand())
        {
            command.CommandText =
                """
                SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, COLLATION_NAME, IS_NULLABLE,
                       CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION, CHARACTER_SET_NAME,
                       COLUMN_DEFAULT, EXTRA
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = @table;
                """;
            command.Parameters.AddWithValue("@table", _options.TableName);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                columns[reader.GetString(0)] = new ActualColumn(
                    DataType: reader.GetString(1),
                    ColumnType: reader.GetString(2),
                    Collation: reader.IsDBNull(3) ? null : reader.GetString(3),
                    Nullable: string.Equals(reader.GetString(4), "YES", StringComparison.OrdinalIgnoreCase),
                    MaxLength: reader.IsDBNull(5) ? null : reader.GetInt64(5),
                    DateTimePrecision: reader.IsDBNull(6) ? null : reader.GetInt64(6),
                    CharacterSet: reader.IsDBNull(7) ? null : reader.GetString(7),
                    HasDefault: !reader.IsDBNull(8),
                    Extra: reader.IsDBNull(9) ? string.Empty : reader.GetString(9));
            }
        }

        if (columns.Count == 0)
        {
            // The table does not exist: AutoCreateSchema = false and the migration has not run yet.
            // That surfaces at the first query with a clear MySQL error, and failing here would
            // break the documented "create it yourself, later" workflow. Returning false keeps
            // _created unlatched so the next operation re-verifies once the migration has run.
            return false;
        }

        foreach (var expected in ExpectedColumns)
        {
            if (!columns.TryGetValue(expected.Name, out var actual))
            {
                throw new InvalidOperationException(
                    $"The MySQL durable-flow table '{_options.TableName}' has no '{expected.Name}' column. It was created by an " +
                    "earlier build or by hand and does not match the shape this store reads and writes " +
                    $"({string.Join(", ", ExpectedColumns.Select(column => $"{column.Name} {column.Declaration}"))}). Re-create it, " +
                    "or add the missing columns — the DDL is in docs/durable-flow-state-stores.md.");
            }

            if (expected.Mismatch(actual) is { } mismatch)
            {
                throw new InvalidOperationException(
                    $"The MySQL durable-flow table '{_options.TableName}' declares {expected.Name} as '{actual.ColumnType}" +
                    $"{(actual.Nullable ? " NULL" : " NOT NULL")}', which {mismatch}. This store needs " +
                    $"{expected.Name} {expected.Declaration}. Fix it with " +
                    $"ALTER TABLE `{_options.TableName}` MODIFY {expected.Name} {expected.Declaration}; " +
                    "(tables this build creates get that shape automatically).");
            }
        }

        // Columns this store never names in an INSERT. One that the database cannot fill in for
        // itself makes EVERY create fail — the shape is otherwise perfect, so the failure arrives
        // at the first flow rather than at startup, which is the wrong end of the deployment.
        // Generated and auto-increment columns are fine; so is anything nullable or defaulted.
        foreach (var (name, actual) in columns)
        {
            if (ExpectedColumns.Any(expected => string.Equals(expected.Name, name, StringComparison.OrdinalIgnoreCase))
                || actual.IsWritableWithoutValue)
            {
                continue;
            }

            throw new InvalidOperationException(
                $"The MySQL durable-flow table '{_options.TableName}' has an extra column '{name}' ({actual.ColumnType} NOT NULL) " +
                "with no default. This store writes only its own columns, so every flow creation would fail on that column. Give " +
                "it a default, make it nullable or generated, or move it to a table of your own.");
        }

        var flowIdColumn = columns["flow_id"];
        // utf8mb4 or nothing: MySQL's older `utf8` is three bytes and holds no supplementary
        // character, and a single-byte set like latin1 holds almost nothing. Either one turns a
        // perfectly legal flow id — an emoji, a Han character, most non-Latin text — into an insert
        // error or a mangled key, and the collation check above cannot see it because latin1_bin
        // ends in _bin just like utf8mb4_bin does.
        if (flowIdColumn.CharacterSet is not { } characterSet
            || !characterSet.Equals("utf8mb4", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"The MySQL durable-flow table '{_options.TableName}' stores flow_id in the character set " +
                $"'{flowIdColumn.CharacterSet ?? "(none)"}', which cannot hold every id the engine accepts. Flow ids are arbitrary " +
                "text — this store's contract bounds their length, not their alphabet — so a narrower set rejects or mangles ids " +
                $"that are perfectly valid. Fix it with ALTER TABLE `{_options.TableName}` MODIFY flow_id varchar(400) CHARACTER " +
                "SET utf8mb4 COLLATE utf8mb4_bin NOT NULL; (tables this build creates get that character set automatically).");
        }

        var collation = flowIdColumn.Collation;
        if (collation is null || !collation.EndsWith("_bin", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"The MySQL durable-flow table '{_options.TableName}' stores flow_id with the collation '{collation ?? "(none)"}', " +
                "which is not binary. Flow ids are compared ordinally by the engine, so ids differing only in case (or accent, or " +
                "width) collide on the primary key: the second flow fails to start and a load returns the other run's state. Fix it " +
                $"with ALTER TABLE `{_options.TableName}` MODIFY flow_id varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT " +
                "NULL; (tables this build creates get that collation automatically).");
        }

        // The ledger JSON needs the same alphabet as the ids it embeds: on a latin1-default
        // server a table that inherited the database charset stores state_json in latin1, so any
        // non-Latin-1 state (a name, an emoji in a step result) hard-fails every update under
        // strict mode or is silently truncated at the first bad byte otherwise — malformed JSON
        // that deserializes to null, a flow that can neither load nor be re-created.
        var stateJsonColumn = columns["state_json"];
        if (stateJsonColumn.CharacterSet is not { } stateJsonCharacterSet
            || !stateJsonCharacterSet.Equals("utf8mb4", StringComparison.OrdinalIgnoreCase))
        {
            // Tables created by builds before this charset was pinned declared state_json with no
            // CHARACTER SET and inherited the server default, so an unconditional throw would
            // hard-fail every pre-existing deployment on a latin1/utf8mb3-default server with no
            // way back: CREATE TABLE IF NOT EXISTS cannot alter an existing table. Under
            // AutoCreateSchema this store owns the DDL, so it repairs the column in place — MODIFY
            // converts the stored text to utf8mb4, lossless for everything the old charset could
            // actually represent. Operator-managed schemas keep the throw, with the exact ALTER.
            if (_options.AutoCreateSchema)
            {
                await using var repair = connection.CreateCommand();
                repair.CommandText = $"ALTER TABLE {Table} MODIFY state_json longtext CHARACTER SET utf8mb4 NOT NULL;";
                await repair.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                throw new InvalidOperationException(
                    $"The MySQL durable-flow table '{_options.TableName}' stores state_json in the character set " +
                    $"'{stateJsonColumn.CharacterSet ?? "(none)"}', which cannot hold every flow state the engine accepts. The ledger " +
                    "JSON is arbitrary text, so a narrower set rejects updates under strict mode or silently truncates the stored " +
                    $"state otherwise. Fix it with ALTER TABLE `{_options.TableName}` MODIFY state_json longtext CHARACTER SET utf8mb4 " +
                    "NOT NULL; (tables this build creates get that character set automatically).");
            }
        }

        await VerifyFlowIdIsUniqueAsync(connection, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>One column as <c>information_schema</c> reports it.</summary>
    private readonly record struct ActualColumn(
        string DataType,
        string ColumnType,
        string? Collation,
        bool Nullable,
        long? MaxLength,
        long? DateTimePrecision,
        string? CharacterSet,
        bool HasDefault,
        string Extra)
    {
        /// <summary>
        /// Whether this store could insert a row without naming this column. True when the column
        /// is nullable, carries a default, auto-increments, or is computed by the database.
        /// </summary>
        internal bool IsWritableWithoutValue
            => Nullable
                || HasDefault
                || Extra.Contains("auto_increment", StringComparison.OrdinalIgnoreCase)
                || Extra.Contains("GENERATED", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// What this store needs from one column, and the check that says so. Widths and precisions are
    /// MINIMA rather than exact matches: a wider flow_id or a higher-precision timestamp still
    /// satisfies every promise the store makes, and rejecting a more generous schema would be a
    /// false alarm. Too NARROW is not — <c>varchar(10)</c> passes a name-only check and then
    /// truncates or errors on the first 400-character id the public contract permits.
    /// </summary>
    private sealed record ExpectedColumn(string Name, string Declaration, string DataType, bool Nullable, long? Minimum = null)
    {
        internal string? Mismatch(ActualColumn actual)
        {
            if (!string.Equals(actual.DataType, DataType, StringComparison.OrdinalIgnoreCase))
                return $"is a '{actual.DataType}'";
            if (actual.Nullable != Nullable)
                return Nullable ? "is NOT NULL (this store writes NULL to it)" : "is nullable";
            if (Minimum is not { } minimum)
                return null;

            // CHARACTER_MAXIMUM_LENGTH for the string columns, DATETIME_PRECISION for the
            // timestamps: a datetime(0) column silently rounds the sub-second lease arithmetic this
            // store runs on UTC_TIMESTAMP(6), which is how two workers end up holding one lease.
            var actualSize = actual.MaxLength ?? actual.DateTimePrecision;
            return actualSize is { } size && size >= minimum
                ? null
                : $"holds {actualSize?.ToString() ?? "an unknown size"} where at least {minimum} is required";
        }
    }

    /// <summary>
    /// The shape this store reads and writes. No default expressions are verified because none are
    /// load-bearing: every write names every column except the two lease fields, whose absence
    /// means NULL.
    /// </summary>
    private static readonly ExpectedColumn[] ExpectedColumns =
    [
        new("flow_id", "varchar(400) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL", "varchar", Nullable: false, Minimum: 400),
        new("state_json", "longtext NOT NULL", "longtext", Nullable: false),
        new("expires_at_utc", "datetime(6) NOT NULL", "datetime", Nullable: false, Minimum: 6),
        new("updated_at_utc", "datetime(6) NOT NULL", "datetime", Nullable: false, Minimum: 6),
        new("revision", "bigint NOT NULL DEFAULT 0", "bigint", Nullable: false),
        new("lease_id", "varchar(64) NULL", "varchar", Nullable: true, Minimum: 64),
        new("lease_expires_at_utc", "datetime(6) NULL", "datetime", Nullable: true, Minimum: 6)
    ];

    /// <summary>
    /// Requires a unique index keyed on the WHOLE of flow_id and nothing else. The PRIMARY KEY this
    /// store's DDL declares is the usual one, but any single-column unique index raises the 1062
    /// that <see cref="TryCreateAsync"/> reads as "already exists", so all of them are accepted.
    /// Two shapes are not:
    /// <list type="bullet">
    /// <item><description>
    /// A COMPOSITE unique key — it permits two rows with the same flow_id.
    /// </description></item>
    /// <item><description>
    /// A PREFIX key (<c>UNIQUE (flow_id(100))</c>, <c>SUB_PART = 100</c>) — the opposite failure,
    /// and the reason this asks about SUB_PART. It constrains only the first N characters, so two
    /// ids the library treats as distinct collide on 1062 and the second flow never starts. Ids run
    /// to 400 characters by contract, and prefix keys are a common way to fit an index under
    /// MySQL's key-length limit, so this is a plausible hand-written schema, not a hypothetical.
    /// </description></item>
    /// </list>
    /// </summary>
    private async Task VerifyFlowIdIsUniqueAsync(MySqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            SELECT 1
            FROM information_schema.STATISTICS s
            WHERE s.TABLE_SCHEMA = DATABASE() AND s.TABLE_NAME = @table
              AND s.NON_UNIQUE = 0 AND s.COLUMN_NAME = 'flow_id' AND s.SEQ_IN_INDEX = 1
              AND s.SUB_PART IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM information_schema.STATISTICS o
                  WHERE o.TABLE_SCHEMA = s.TABLE_SCHEMA AND o.TABLE_NAME = s.TABLE_NAME
                    AND o.INDEX_NAME = s.INDEX_NAME AND o.SEQ_IN_INDEX > 1)
            LIMIT 1;
            """;
        command.Parameters.AddWithValue("@table", _options.TableName);

        if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not null)
            return;

        throw new InvalidOperationException(
            $"The MySQL durable-flow table '{_options.TableName}' has no unique key on the whole of flow_id. Starting a flow is an " +
            "insert-if-absent, and this store learns that a ledger already exists from MySQL's duplicate-key error. Without such a " +
            "key nothing reports the duplicate, so two concurrent starts of one flow id both succeed and the flow runs twice; with " +
            "a PREFIX key (flow_id(n)) the opposite happens, and two distinct ids sharing their first n characters collide so the " +
            $"second never starts. Fix it with ALTER TABLE `{_options.TableName}` ADD PRIMARY KEY (flow_id); (tables this build " +
            "creates declare it automatically).");
    }

    private async Task<bool> UpdateLeaseAsync(
        string flowId,
        string leaseId,
        TimeSpan leaseDuration,
        bool acquire,
        CancellationToken cancellationToken)
    {
        DurableFlowStoreShared.ValidateLeaseArgs(flowId, leaseId, leaseDuration);

        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        // Lease fencing runs entirely on the database clock: acquire steals only leases the
        // database considers expired, and renew/extend stays relative to UTC_TIMESTAMP(6), so
        // worker clock skew can never make two nodes hold the same lease.
        command.CommandText =
            $"""
            UPDATE {Table}
            SET lease_id = @lease_id, lease_expires_at_utc = {AddMilliseconds("@lease_ms")}
            WHERE flow_id = @flow_id
              AND expires_at_utc > UTC_TIMESTAMP(6)
              AND {(acquire ? "(lease_id IS NULL OR lease_expires_at_utc <= UTC_TIMESTAMP(6) OR lease_id = @lease_id)" : "lease_id = @lease_id AND lease_expires_at_utc > UTC_TIMESTAMP(6)")};
            """;
        command.Parameters.AddWithValue("@flow_id", flowId);
        command.Parameters.AddWithValue("@lease_id", leaseId);
        command.Parameters.AddWithValue("@lease_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(leaseDuration));
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    // Row-count semantics: this store's lease renewal (and update fencing) treats
    // ExecuteNonQuery's result as ROWS MATCHED, which is MySqlConnector's default
    // (UseAffectedRows=false). EnsureCreatedAsync rejects a connection string that sets
    // UseAffectedRows=true before any of those UPDATEs can run.
    private Task<MySqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
        => DurableFlowStoreShared.OpenConnectionAsync<MySqlConnection>(_options.ConnectionString, cancellationToken);

    private string Table => Quote(_options.TableName);
    private string IndexName => Quote(DurableFlowStoreShared.DerivedName(_options.TableName, "_expires_idx", 64));
    private static string Quote(string identifier) => "`" + identifier.Replace("`", "``", StringComparison.Ordinal) + "`";
}
}
