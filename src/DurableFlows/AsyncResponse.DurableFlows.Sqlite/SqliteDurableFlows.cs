using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>DI registration for the SQLite durable-flow state store.</summary>
    public static class SqliteDurableFlowServiceCollectionExtensions
    {
        /// <summary>Stores durable-flow state in SQLite.</summary>
        public static AsyncResponseRegistrationBuilder WithSqliteDurableFlows(
            this AsyncResponseRegistrationBuilder builder,
            Action<SqliteDurableFlowOptions>? configure = null)
        {
            // Singleton on purpose: schema provisioning is cached per store instance, and the
            // executor resolves the store from a fresh scope per flow execution — a scoped store
            // would re-run EnsureCreated's DDL round-trip on every run.
            builder.Services.TryAddSingleton<SqliteFlowStateStore>();
            return builder.WithDurableFlows<SqliteFlowStateStore, SqliteDurableFlowOptions>(configure);
        }
    }
}

namespace AsyncResponse.DurableFlows.Sqlite
{
/// <summary>Options for the SQLite durable-flow state store.</summary>
public sealed class SqliteDurableFlowOptions : DurableFlowOptions
{
    /// <summary>SQLite connection string. Default: <c>Data Source=asyncresponse-flow-state.db</c>.</summary>
    public string ConnectionString { get; set; } = "Data Source=asyncresponse-flow-state.db";

    /// <summary>Table storing one durable-flow ledger row per flow id.</summary>
    public string TableName { get; set; } = "asyncresponse_flow_state";

    /// <summary>Creates the table and expiry index on first use.</summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// How often <see cref="SqliteFlowStateStore.TryCreateAsync"/> opportunistically deletes one bounded
    /// batch (1000 rows) of expired rows (loads already treat expired state as absent; pruning
    /// bounds table growth). Zero or negative prunes on every save. Default: 5 minutes.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum serialized flow-state size in bytes accepted by writes; oversized ledgers fail fast
    /// with an actionable error instead of an opaque provider error. Default: <c>null</c>
    /// (unlimited — SQLite <c>TEXT</c> holds up to ~1 GB), settable as an operator budget.
    /// </summary>
    public long? MaxStateBytes { get; set; }

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        DurableFlowStoreShared.ValidateConnectionString(ConnectionString, nameof(SqliteDurableFlowOptions));
        DurableFlowStoreShared.ValidateIdentifier(TableName, $"{nameof(SqliteDurableFlowOptions)}.{nameof(TableName)}", "SQLite");
        DurableFlowStoreShared.ValidateMaxStateBytes(MaxStateBytes, nameof(SqliteDurableFlowOptions));
    }
}

/// <summary>SQLite implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class SqliteFlowStateStore : IFlowStateStore
{
    private const int PruneBatchSize = 1000;

    // Time authority: this store deliberately keeps the app clock (DateTime.UtcNow) for expiry
    // and lease comparisons. A SQLite database file lives on a single machine, and every writer
    // is a process on that machine sharing the same clock — the multi-node clock-skew hazard the
    // server-clock stores guard against cannot occur, and SQLite has no server clock to ask.
    private readonly SqliteDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);

    // SQLite allows exactly one writer at a time, and its cross-connection busy handler is a
    // poll loop, not a queue: under heavy concurrency on a slow machine an unlucky writer can
    // lose every poll until the busy timeout expires ('database is locked' storms on 2-core CI
    // runners). Serializing this process's writers through a real FIFO gate costs no throughput
    // (they would serialize inside SQLite anyway) and makes in-process contention
    // starvation-free; the busy timeout then only covers cross-process writers. Reads stay
    // concurrent (WAL).
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    private long _lastPruneTicks;
    private volatile bool _created;

    public SqliteFlowStateStore(IOptions<SqliteDurableFlowOptions> options)
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
        command.CommandText =
            $"""
            SELECT state_json, revision
            FROM {Table}
            WHERE flow_id = $flow_id AND expires_at_utc > $now_utc;
            """;
        command.Parameters.AddWithValue("$flow_id", flowId);
        command.Parameters.AddWithValue("$now_utc", DateTime.UtcNow);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        return DurableFlowStoreShared.ReadState(flowId, reader.GetString(0), reader.GetInt64(1));
    }

    public async Task<bool> TryCreateAsync(
        string flowId,
        FlowState state,
        TimeSpan ttl,
        CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "SQLite");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await DurableFlowStoreShared.PruneQuietlyAsync(() => PruneExpiredAsync(cancellationToken)).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {Table} (flow_id, state_json, expires_at_utc, updated_at_utc, revision)
            VALUES ($flow_id, $state_json, $expires_at_utc, $now_utc, $revision)
            ON CONFLICT(flow_id) DO UPDATE SET
                state_json = excluded.state_json,
                expires_at_utc = excluded.expires_at_utc,
                updated_at_utc = excluded.updated_at_utc,
                revision = excluded.revision,
                lease_id = NULL,
                lease_expires_at_utc = NULL
            WHERE {Table}.expires_at_utc <= $now_utc;
            """;
        var now = DateTime.UtcNow;
        command.Parameters.AddWithValue("$flow_id", flowId);
        command.Parameters.AddWithValue("$state_json", stateJson);
        command.Parameters.AddWithValue("$expires_at_utc", DurableFlowStoreShared.AddSaturating(now, ttl));
        command.Parameters.AddWithValue("$now_utc", now);
        command.Parameters.AddWithValue("$revision", state.Revision);
        return await ExecuteWriteAsync(command, cancellationToken).ConfigureAwait(false) > 0;
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
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "SQLite");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        var now = DateTime.UtcNow;
        command.CommandText =
            $"""
            UPDATE {Table}
            SET state_json = $state_json,
                expires_at_utc = $expires_at_utc,
                updated_at_utc = $updated_at_utc,
                revision = $new_revision
            WHERE flow_id = $flow_id
              AND revision = $expected_revision
              AND expires_at_utc > $now_utc
              AND ($lease_id IS NULL OR (lease_id = $lease_id AND lease_expires_at_utc > $now_utc));
            """;
        command.Parameters.AddWithValue("$flow_id", flowId);
        command.Parameters.AddWithValue("$state_json", stateJson);
        command.Parameters.AddWithValue("$expires_at_utc", DurableFlowStoreShared.AddSaturating(now, ttl));
        command.Parameters.AddWithValue("$updated_at_utc", now);
        command.Parameters.AddWithValue("$new_revision", state.Revision);
        command.Parameters.AddWithValue("$expected_revision", expectedRevision);
        command.Parameters.AddWithValue("$now_utc", now);
        command.Parameters.AddWithValue("$lease_id", (object?)leaseId ?? DBNull.Value);
        return await ExecuteWriteAsync(command, cancellationToken).ConfigureAwait(false) > 0;
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
        command.CommandText = $"UPDATE {Table} SET lease_id = NULL, lease_expires_at_utc = NULL WHERE flow_id = $flow_id AND lease_id = $lease_id;";
        command.Parameters.AddWithValue("$flow_id", flowId);
        command.Parameters.AddWithValue("$lease_id", leaseId);
        await ExecuteWriteAsync(command, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {Table} WHERE flow_id = $flow_id;";
        command.Parameters.AddWithValue("$flow_id", flowId);
        return await ExecuteWriteAsync(command, cancellationToken).ConfigureAwait(false) > 0;
    }

    private async Task PruneExpiredAsync(CancellationToken cancellationToken)
    {
        // Timestamps are stored as ISO-8601 TEXT, which compares correctly lexicographically.
        // One bounded batch per prune interval (policy shared by all relational stores): an
        // unbatched DELETE over a large expired backlog holds the single SQLite write lock for
        // the whole sweep. Loads already filter on expiry, so any backlog beyond the batch just
        // waits for the next interval. Id-subquery form because DELETE ... LIMIT needs a
        // non-default SQLite compile flag.
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            DELETE FROM {Table}
            WHERE flow_id IN (SELECT flow_id FROM {Table} WHERE expires_at_utc <= $now_utc LIMIT {PruneBatchSize});
            """;
        command.Parameters.AddWithValue("$now_utc", DateTime.UtcNow);
        await ExecuteWriteAsync(command, cancellationToken).ConfigureAwait(false);
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

            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            if (_options.AutoCreateSchema)
            {
                await using var command = connection.CreateCommand();
                command.CommandText =
                    $"""
                    -- WAL is the right journal mode for this store's use case (concurrent flow
                    -- executors on one node): readers never block behind a writer, which rollback
                    -- journal mode does not guarantee — concurrent load/save storms on slow disks
                    -- surface as SQLITE_BUSY 'database is locked' there. The mode is persistent in
                    -- the database file, so setting it alongside the schema costs nothing per
                    -- operation. Manually-provisioned databases (AutoCreateSchema=false) should set
                    -- it themselves — see docs/durable-flow-state-stores.md.
                    PRAGMA journal_mode=WAL;
                    CREATE TABLE IF NOT EXISTS {Table} (
                        flow_id TEXT NOT NULL PRIMARY KEY,
                        state_json TEXT NOT NULL,
                        expires_at_utc TEXT NOT NULL,
                        updated_at_utc TEXT NOT NULL,
                        revision INTEGER NOT NULL DEFAULT 0,
                        lease_id TEXT NULL,
                        lease_expires_at_utc TEXT NULL
                    );
                    CREATE INDEX IF NOT EXISTS {IndexName} ON {Table} (expires_at_utc);
                    """;
                await ExecuteWriteAsync(command, cancellationToken).ConfigureAwait(false);

                // Fall THROUGH to verification instead of latching here. CREATE TABLE IF NOT EXISTS
                // is a no-op against a table an earlier build or a hand-run migration left behind,
                // so latching on the DDL trusted its shape for the process lifetime — none of the
                // load-bearing checks below (a single-column NOT NULL primary key, ISO-8601 text
                // affinity on the expiry and lease columns, no extra NOT NULL column, a BINARY
                // flow_id collation) ever ran on the DEFAULT path. MySQL, PostgreSQL and SQL Server
                // all verify after their DDL for exactly this reason.
                _created = await VerifyFlowTableAsync(connection, cancellationToken).ConfigureAwait(false);
                return;
            }

            // Operator-provisioned schema: nothing on this path issues DDL (not even the WAL
            // pragma — see the note above), but the table's shape is still verified before the
            // store trusts it. Latch only when the table was actually verified (MySQL/Oracle
            // parity): an absent table must keep re-verifying, or a migration that lands AFTER
            // the first operation would never have its shape checked for the process lifetime.
            _created = await VerifyFlowTableAsync(connection, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    /// <summary>
    /// Checks the operator-provisioned table against the shape this store reads and writes.
    /// Declared types are compared by SQLite AFFINITY, not spelling, so any declaration that
    /// behaves like the documented DDL passes. Two properties are load-bearing and misfire
    /// SILENTLY or at the first flow — the wrong end of the deployment — when absent:
    /// <list type="bullet">
    /// <item><description>
    /// A single-column PRIMARY KEY on flow_id. <see cref="TryCreateAsync"/>'s upsert targets
    /// <c>ON CONFLICT(flow_id)</c>, which requires a uniqueness constraint on exactly that
    /// column; and SQLite's historical quirk admits NULL keys when the PRIMARY KEY column is
    /// not also declared NOT NULL.
    /// </description></item>
    /// <item><description>
    /// TEXT affinity on the timestamp columns. Expiry and lease fencing compare ISO-8601
    /// strings lexicographically; a numeric affinity coerces digit-only values and breaks that
    /// ordering.
    /// </description></item>
    /// </list>
    /// </summary>
    private async Task<bool> VerifyFlowTableAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        var columns = new Dictionary<string, ActualColumn>(StringComparer.OrdinalIgnoreCase);
        await using (var command = connection.CreateCommand())
        {
            // Read-only: table_info reports name, declared type, NOT NULL, default, and the
            // primary-key ordinal. Generated columns are not listed — exactly right for the
            // extra-column check below, because they fill themselves in on insert.
            command.CommandText = $"PRAGMA table_info({Table});";
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                columns[reader.GetString(1)] = new ActualColumn(
                    DeclaredType: reader.GetString(2),
                    NotNull: reader.GetInt64(3) != 0,
                    HasDefault: !reader.IsDBNull(4),
                    PrimaryKeyOrdinal: reader.GetInt64(5));
            }
        }

        if (columns.Count == 0)
        {
            // The table does not exist: AutoCreateSchema = false and the migration has not run
            // yet. That surfaces at the first query with a clear SQLite error, and failing here
            // would break the documented "create it yourself, later" workflow. Returning false
            // keeps _created unlatched so the next operation re-verifies once the migration has
            // run.
            return false;
        }

        foreach (var expected in ExpectedColumns)
        {
            if (!columns.TryGetValue(expected.Name, out var actual))
            {
                throw new InvalidOperationException(
                    $"The SQLite durable-flow table '{_options.TableName}' has no '{expected.Name}' column. It was created by an " +
                    "earlier build or by hand and does not match the shape this store reads and writes " +
                    $"({string.Join(", ", ExpectedColumns.Select(column => $"{column.Name} {column.Declaration}"))}). Re-create it " +
                    "with the DDL in docs/durable-flow-state-stores.md (tables this build creates get that shape automatically).");
            }

            if (expected.Mismatch(actual) is { } mismatch)
            {
                throw new InvalidOperationException(
                    $"The SQLite durable-flow table '{_options.TableName}' declares {expected.Name} as " +
                    $"'{(actual.DeclaredType.Length == 0 ? "(no type)" : actual.DeclaredType)}{(actual.NotNull ? " NOT NULL" : " NULL")}', " +
                    $"which {mismatch}. This store needs {expected.Name} {expected.Declaration}. SQLite cannot alter a column in " +
                    "place — re-create the table with the DDL in docs/durable-flow-state-stores.md (tables this build creates get " +
                    "that shape automatically).");
            }
        }

        // Exactly PRIMARY KEY (flow_id): the create upsert's ON CONFLICT(flow_id) needs a
        // uniqueness constraint on that column alone — a composite key constrains a different
        // tuple, so SQLite rejects the upsert at the first flow rather than at startup.
        if (columns["flow_id"].PrimaryKeyOrdinal != 1 || columns.Values.Count(column => column.PrimaryKeyOrdinal != 0) != 1)
        {
            throw new InvalidOperationException(
                $"The SQLite durable-flow table '{_options.TableName}' does not declare PRIMARY KEY (flow_id) on that column alone. " +
                "Starting a flow is an insert-if-absent targeting ON CONFLICT(flow_id), which requires a uniqueness constraint on " +
                "exactly flow_id — without it every flow creation fails, and a composite key admits duplicate ids. Re-create the " +
                "table with the DDL in docs/durable-flow-state-stores.md (tables this build creates declare it automatically).");
        }

        // Columns this store never names in an INSERT. One that the database cannot fill in for
        // itself makes EVERY create fail — the shape is otherwise perfect, so the failure arrives
        // at the first flow rather than at startup. Generated columns never reach this loop; so
        // is anything nullable or defaulted.
        foreach (var (name, actual) in columns)
        {
            if (ExpectedColumns.Any(expected => string.Equals(expected.Name, name, StringComparison.OrdinalIgnoreCase))
                || !actual.NotNull
                || actual.HasDefault)
            {
                continue;
            }

            throw new InvalidOperationException(
                $"The SQLite durable-flow table '{_options.TableName}' has an extra column '{name}' ({actual.DeclaredType} NOT NULL) " +
                "with no default. This store writes only its own columns, so every flow creation would fail on that column. Give " +
                "it a default, make it nullable or generated, or move it to a table of your own.");
        }

        await VerifyFlowIdCollationAsync(connection, cancellationToken).ConfigureAwait(false);

        return true;
    }

    /// <summary>
    /// flow_id must compare ordinally. PRAGMA table_info does not report a column's collation, so
    /// the declaration is read from sqlite_master instead — the one property of this table that a
    /// shape check cannot see and that fails SILENTLY: under COLLATE NOCASE the primary key and
    /// every <c>WHERE flow_id = $flow_id</c> fold case, so "Order-A1" and "order-a1" become one
    /// key. The second flow's insert-if-absent then reports "already running" and a load for one
    /// id returns the other run's ledger. SQLite is the only one of the six relational stores that
    /// was not checking this; MySQL, PostgreSQL, SQL Server, Oracle and EF Core all do.
    /// <para>
    /// The lookup matches the stored table name case-insensitively — <c>sqlite_master.name</c>
    /// compares BINARY while SQLite resolves identifiers case-insensitively everywhere else, so a
    /// case-variant table silently no-opped this whole check. And EVERY identifier-boundary
    /// occurrence of <c>flow_id</c> is inspected, not the first substring hit: an earlier column
    /// ending in flow_id (<c>parent_flow_id</c>) captured the match and hid the real column's
    /// collation, and a table-level <c>PRIMARY KEY (flow_id COLLATE NOCASE)</c> — which the docs
    /// promise is caught — was never reached.
    /// </para>
    /// </summary>
    private async Task VerifyFlowIdCollationAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = $name COLLATE NOCASE;";
        command.Parameters.AddWithValue("$name", _options.TableName);

        var ddl = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string;
        if (string.IsNullOrEmpty(ddl))
            return;

        // Each flow_id occurrence's clause, up to the next column separator: the column
        // declaration carries a column-level COLLATE, the table-level PRIMARY KEY clause a
        // key-level one. Occurrences without a COLLATE (an FK column list, a plain PK clause)
        // are skipped.
        for (var start = IndexOfFlowIdIdentifier(ddl, 0); start >= 0; start = IndexOfFlowIdIdentifier(ddl, start + 1))
        {
            var end = ddl.IndexOf(',', start);
            var declaration = end < 0 ? ddl[start..] : ddl[start..end];

            var collate = declaration.IndexOf("COLLATE", StringComparison.OrdinalIgnoreCase);
            if (collate < 0)
                continue;

            var tail = declaration[(collate + "COLLATE".Length)..].TrimStart();
            var tokenEnd = 0;
            while (tokenEnd < tail.Length && !char.IsWhiteSpace(tail[tokenEnd]) && tail[tokenEnd] is not (')' or ','))
                tokenEnd++;

            var collation = tail[..tokenEnd].Trim('"');
            if (collation.Length == 0 || string.Equals(collation, "BINARY", StringComparison.OrdinalIgnoreCase))
                continue;

            throw new InvalidOperationException(
                $"The SQLite durable-flow table '{_options.TableName}' declares flow_id with COLLATE {collation}. Flow ids are compared " +
                "ordinally by this library, so a case- or accent-insensitive collation makes ids differing only in case collide on the " +
                "primary key: the second flow fails to start and a load returns the other run's state. Re-create the table with flow_id " +
                "using the default BINARY collation (the DDL in docs/durable-flow-state-stores.md, which tables this build creates use).");
        }
    }

    /// <summary>
    /// Finds the next occurrence of <c>flow_id</c> that is a whole identifier — not the tail of
    /// <c>parent_flow_id</c> or the head of <c>flow_id_shadow</c>. Quoted forms ("flow_id",
    /// [flow_id], `flow_id`) satisfy the boundary test through their quote characters.
    /// </summary>
    private static int IndexOfFlowIdIdentifier(string ddl, int startIndex)
    {
        for (var index = ddl.IndexOf("flow_id", startIndex, StringComparison.OrdinalIgnoreCase);
             index >= 0;
             index = index + 1 < ddl.Length ? ddl.IndexOf("flow_id", index + 1, StringComparison.OrdinalIgnoreCase) : -1)
        {
            var beforeIsIdentifierChar = index > 0 && (char.IsAsciiLetterOrDigit(ddl[index - 1]) || ddl[index - 1] == '_');
            var afterIndex = index + "flow_id".Length;
            var afterIsIdentifierChar = afterIndex < ddl.Length && (char.IsAsciiLetterOrDigit(ddl[afterIndex]) || ddl[afterIndex] == '_');
            if (!beforeIsIdentifierChar && !afterIsIdentifierChar)
                return index;
        }

        return -1;
    }

    /// <summary>SQLite column-affinity rules (in the documented precedence order).</summary>
    private static string Affinity(string declaredType)
    {
        if (declaredType.Contains("INT", StringComparison.OrdinalIgnoreCase))
            return "INTEGER";
        if (declaredType.Contains("CHAR", StringComparison.OrdinalIgnoreCase)
            || declaredType.Contains("CLOB", StringComparison.OrdinalIgnoreCase)
            || declaredType.Contains("TEXT", StringComparison.OrdinalIgnoreCase))
        {
            return "TEXT";
        }

        if (declaredType.Length == 0 || declaredType.Contains("BLOB", StringComparison.OrdinalIgnoreCase))
            return "BLOB";
        if (declaredType.Contains("REAL", StringComparison.OrdinalIgnoreCase)
            || declaredType.Contains("FLOA", StringComparison.OrdinalIgnoreCase)
            || declaredType.Contains("DOUB", StringComparison.OrdinalIgnoreCase))
        {
            return "REAL";
        }

        return "NUMERIC";
    }

    private sealed record ActualColumn(string DeclaredType, bool NotNull, bool HasDefault, long PrimaryKeyOrdinal);

    private sealed record ExpectedColumn(string Name, string Declaration, string RequiredAffinity, bool NotNull)
    {
        public string? Mismatch(ActualColumn actual)
        {
            if (!string.Equals(Affinity(actual.DeclaredType), RequiredAffinity, StringComparison.Ordinal))
                return $"resolves to {Affinity(actual.DeclaredType)} affinity where {RequiredAffinity} is required";
            if (actual.NotNull != NotNull)
                return NotNull ? "must be NOT NULL" : "must be nullable (this store writes and clears NULL there)";
            return null;
        }
    }

    private static readonly ExpectedColumn[] ExpectedColumns =
    [
        new("flow_id", "TEXT NOT NULL PRIMARY KEY", "TEXT", NotNull: true),
        new("state_json", "TEXT NOT NULL", "TEXT", NotNull: true),
        new("expires_at_utc", "TEXT NOT NULL", "TEXT", NotNull: true),
        new("updated_at_utc", "TEXT NOT NULL", "TEXT", NotNull: true),
        new("revision", "INTEGER NOT NULL DEFAULT 0", "INTEGER", NotNull: true),
        new("lease_id", "TEXT NULL", "TEXT", NotNull: false),
        new("lease_expires_at_utc", "TEXT NULL", "TEXT", NotNull: false)
    ];

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
        var now = DateTime.UtcNow;
        command.CommandText =
            $"""
            UPDATE {Table}
            SET lease_id = $lease_id, lease_expires_at_utc = $lease_expires_at_utc
            WHERE flow_id = $flow_id
              AND expires_at_utc > $now_utc
              AND {(acquire ? "(lease_id IS NULL OR lease_expires_at_utc <= $now_utc OR lease_id = $lease_id)" : "lease_id = $lease_id AND lease_expires_at_utc > $now_utc")};
            """;
        command.Parameters.AddWithValue("$flow_id", flowId);
        command.Parameters.AddWithValue("$lease_id", leaseId);
        command.Parameters.AddWithValue("$lease_expires_at_utc", DurableFlowStoreShared.AddSaturating(now, leaseDuration));
        command.Parameters.AddWithValue("$now_utc", now);
        return await ExecuteWriteAsync(command, cancellationToken).ConfigureAwait(false) > 0;
    }

    private async Task<int> ExecuteWriteAsync(SqliteCommand command, CancellationToken cancellationToken)
    {
        await _writeGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeGate.Release();
        }
    }

    private Task<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken)
        => DurableFlowStoreShared.OpenConnectionAsync<SqliteConnection>(_options.ConnectionString, cancellationToken);

    private string Table => Quote(_options.TableName);
    private string IndexName => Quote($"{_options.TableName}_expires_idx");
    private static string Quote(string identifier) => "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
}
