using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.Oracle;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>DI registration for the Oracle durable-flow state store.</summary>
    public static class OracleDurableFlowServiceCollectionExtensions
    {
        /// <summary>Stores durable-flow state in Oracle Database.</summary>
        public static AsyncResponseRegistrationBuilder WithOracleDurableFlows(
            this AsyncResponseRegistrationBuilder builder,
            Action<OracleDurableFlowOptions>? configure = null)
        {
            // Singleton on purpose: schema provisioning is cached per store instance, and the
            // executor resolves the store from a fresh scope per flow execution — a scoped store
            // would re-run EnsureCreated's DDL round-trip on every run.
            builder.Services.TryAddSingleton<OracleFlowStateStore>();
            return builder.WithDurableFlows<OracleFlowStateStore, OracleDurableFlowOptions>(configure);
        }
    }
}

namespace AsyncResponse.DurableFlows.Oracle
{
/// <summary>Options for the Oracle durable-flow state store.</summary>
public sealed class OracleDurableFlowOptions : DurableFlowOptions
{
    /// <summary>Oracle connection string. Required.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Table storing one durable-flow ledger row per flow id.</summary>
    public string TableName { get; set; } = "ASYNCRESPONSE_FLOW_STATE";

    /// <summary>Creates the table and expiry index on first use.</summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// How often <see cref="OracleFlowStateStore.TryCreateAsync"/> opportunistically deletes one bounded
    /// batch (1000 rows) of expired rows (loads already treat expired state as absent; pruning
    /// bounds table growth). Zero or negative prunes on every save. Default: 5 minutes.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum serialized flow-state size in bytes accepted by writes; oversized ledgers fail fast
    /// with an actionable error instead of an opaque provider error. Default: <c>null</c>
    /// (unlimited — <c>NCLOB</c> is effectively unbounded), settable as an operator budget.
    /// </summary>
    public long? MaxStateBytes { get; set; }

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        DurableFlowStoreShared.ValidateConnectionString(ConnectionString, nameof(OracleDurableFlowOptions));
        DurableFlowStoreShared.ValidateIdentifier(TableName, $"{nameof(OracleDurableFlowOptions)}.{nameof(TableName)}", "Oracle", identifierCap: 128);

        // Indexes share Oracle's schema-object namespace with tables: a table whose name ends
        // exactly where the reserved "_EXPIRES_IDX" stem truncates derives its own name, and the
        // CREATE INDEX then raises ORA-00955 — indistinguishable from the benign already-exists
        // race the DDL path deliberately swallows — so the expiry index would silently never
        // exist and every prune would full-scan. Unquoted identifiers are case-insensitive.
        if (string.Equals(DurableFlowStoreShared.DerivedName(TableName, "_EXPIRES_IDX", 128), TableName, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                $"{nameof(OracleDurableFlowOptions)}.{nameof(TableName)} '{TableName}' collides with its derived expiry-index name; rename the table.");
        DurableFlowStoreShared.ValidateMaxStateBytes(MaxStateBytes, nameof(OracleDurableFlowOptions));
    }
}

/// <summary>Oracle implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class OracleFlowStateStore : IFlowStateStore
{
    private const int ObjectAlreadyExists = 955;
    private const int ColumnListAlreadyIndexed = 1408;
    private const int UniqueConstraintViolated = 1;
    private const int PruneBatchSize = 1000;

    /// <summary>
    /// SQL expression adding a millisecond bind parameter to the database clock. All expiry and
    /// lease math runs on <c>SYS_EXTRACT_UTC(SYSTIMESTAMP)</c> so app clock skew can never fence a
    /// lease in or out; Oracle NUMBER division keeps fractional seconds, so <c>datetime</c>
    /// precision survives the millisecond parameter.
    /// </summary>
    private static string AddMilliseconds(string parameterName)
        => $"SYS_EXTRACT_UTC(SYSTIMESTAMP) + NUMTODSINTERVAL({parameterName} / 1000, 'SECOND')";

    private const string UtcNowSql = "SYS_EXTRACT_UTC(SYSTIMESTAMP)";

    private readonly OracleDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private long _lastPruneTicks;
    private volatile bool _created;

    public OracleFlowStateStore(IOptions<OracleDurableFlowOptions> options)
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
        command.BindByName = true;
        command.CommandText = $"SELECT state_json, revision FROM {Table} WHERE flow_id = :flow_id AND expires_at_utc > {UtcNowSql}";
        command.Parameters.Add(NationalId("flow_id", flowId));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        return DurableFlowStoreShared.ReadState(flowId, reader.GetString(0), reader.GetInt64(1));
    }

    public async Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "Oracle");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await DurableFlowStoreShared.PruneQuietlyAsync(() => PruneExpiredAsync(cancellationToken)).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await TryCreateCoreAsync(connection, flowId, stateJson, state.Revision, ttl, cancellationToken).ConfigureAwait(false);
        }
        catch (OracleException ex) when (ex.Number == UniqueConstraintViolated)
        {
            return await TryCreateCoreAsync(connection, flowId, stateJson, state.Revision, ttl, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<bool> TryCreateCoreAsync(
        OracleConnection connection,
        string flowId,
        string stateJson,
        long revision,
        TimeSpan ttl,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.BindByName = true;
        command.CommandText =
            $"""
            MERGE INTO {Table} target
            USING (SELECT :flow_id AS flow_id FROM dual) source ON (target.flow_id = source.flow_id)
            WHEN MATCHED THEN UPDATE SET
                target.state_json = :state_json,
                target.expires_at_utc = {AddMilliseconds(":ttl_ms")},
                target.updated_at_utc = {UtcNowSql},
                target.revision = :revision,
                target.lease_id = NULL,
                target.lease_expires_at_utc = NULL
                WHERE target.expires_at_utc <= {UtcNowSql}
            WHEN NOT MATCHED THEN
                INSERT (flow_id, state_json, expires_at_utc, updated_at_utc, revision)
                VALUES (:flow_id, :state_json, {AddMilliseconds(":ttl_ms")}, {UtcNowSql}, :revision)
            """;
        command.Parameters.Add(NationalId("flow_id", flowId));
        command.Parameters.Add(new OracleParameter("state_json", OracleDbType.NClob) { Value = stateJson });
        command.Parameters.Add(new OracleParameter("ttl_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(ttl)));
        command.Parameters.Add(new OracleParameter("revision", revision));
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
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
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "Oracle");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.BindByName = true;
        command.CommandText =
            $"""
            UPDATE {Table}
            SET state_json = :state_json,
                expires_at_utc = {AddMilliseconds(":ttl_ms")},
                updated_at_utc = {UtcNowSql},
                revision = :new_revision
            WHERE flow_id = :flow_id
              AND revision = :expected_revision
              AND expires_at_utc > {UtcNowSql}
              AND (:lease_id IS NULL OR (lease_id = :lease_id AND lease_expires_at_utc > {UtcNowSql}))
            """;
        command.Parameters.Add(NationalId("flow_id", flowId));
        command.Parameters.Add(new OracleParameter("state_json", OracleDbType.NClob) { Value = stateJson });
        command.Parameters.Add(new OracleParameter("ttl_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(ttl)));
        command.Parameters.Add(new OracleParameter("expected_revision", expectedRevision));
        command.Parameters.Add(new OracleParameter("new_revision", state.Revision));
        command.Parameters.Add(NationalId("lease_id", leaseId));
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
        command.BindByName = true;
        command.CommandText = $"UPDATE {Table} SET lease_id = NULL, lease_expires_at_utc = NULL WHERE flow_id = :flow_id AND lease_id = :lease_id";
        command.Parameters.Add(NationalId("flow_id", flowId));
        command.Parameters.Add(NationalId("lease_id", leaseId));
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.BindByName = true;
        command.CommandText = $"DELETE FROM {Table} WHERE flow_id = :flow_id";
        command.Parameters.Add(NationalId("flow_id", flowId));
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
        command.BindByName = true;
        command.CommandText = $"DELETE FROM {Table} WHERE expires_at_utc <= {UtcNowSql} AND ROWNUM <= {PruneBatchSize}";
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

            await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            if (_options.AutoCreateSchema)
            {
                await ExecuteIgnoringExistsAsync(
                    connection,
                    $"""
                    CREATE TABLE {Table} (
                        flow_id NVARCHAR2(400) NOT NULL PRIMARY KEY,
                        state_json NCLOB NOT NULL,
                        expires_at_utc TIMESTAMP(6) NOT NULL,
                        updated_at_utc TIMESTAMP(6) NOT NULL,
                        revision NUMBER(19) DEFAULT 0 NOT NULL,
                        lease_id NVARCHAR2(64) NULL,
                        lease_expires_at_utc TIMESTAMP(6) NULL
                    )
                    """,
                    cancellationToken).ConfigureAwait(false);
                await ExecuteIgnoringExistsAsync(
                    connection,
                    $"CREATE INDEX {IndexName} ON {Table} (expires_at_utc)",
                    cancellationToken).ConfigureAwait(false);
            }

            // Oracle DDL commits implicitly, so everything above is already committed and the
            // checks below run outside any transaction: they read the catalog for OTHER sessions'
            // committed objects and must never sit on DDL locks of their own. _created latches
            // only on a VERIFIED table: when the table does not exist yet (AutoCreateSchema =
            // false, migration not run), verification re-runs on the next operation instead of
            // being silently skipped for the process lifetime.
            _created = await VerifyFlowTableAsync(connection, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    // NVarchar2, never the inferred Varchar2: ids travel to NVARCHAR2 columns, and a Varchar2
    // bind converts through the DATABASE character set on the wire — on a non-Unicode
    // NLS_CHARACTERSET (e.g. WE8MSWIN1252) two distinct Unicode ids collapse to one '???' key,
    // so one flow's row answers another flow's create/load. The verifier enforces NVARCHAR2
    // columns for exactly this reason; the binds must match it. Internal (not private) so the
    // unit suite can pin the bind type without an Oracle server.
    internal static OracleParameter NationalId(string name, string? value)
        => new(name, OracleDbType.NVarchar2) { Value = (object?)value ?? DBNull.Value };

    private static async Task ExecuteIgnoringExistsAsync(OracleConnection connection, string commandText, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = commandText;
        try
        {
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OracleException ex) when (ex.Number is ObjectAlreadyExists or ColumnListAlreadyIndexed)
        {
            // ORA-00955: the object (table/index name) already exists. ORA-01408: the column list
            // is already indexed — raised instead of ORA-00955 when an operator pre-created the
            // expiry index under a different name; the index we want exists in substance.
        }
    }

    /// <summary>
    /// Checks the objects this store will actually use, independently of who created them.
    /// <see cref="ExecuteIgnoringExistsAsync"/> swallows ORA-00955, so a pre-existing object under
    /// the table's name — an earlier build's table, a hand-written one, even a VIEW — is left
    /// exactly as it was, and <c>AutoCreateSchema = false</c> issues no DDL at all: the CREATE
    /// above only ever protects a table this build created. Three properties of what the name
    /// resolves to are load-bearing and all fail SILENTLY (or mid-operation) when absent, which is
    /// why they are checked once per store instance rather than left to the first query:
    /// <list type="bullet">
    /// <item><description>
    /// Ordinal comparison in this session. NLS_COMP=LINGUISTIC plus a case-insensitive NLS_SORT
    /// folds every <c>flow_id = :flow_id</c> predicate this store runs while the primary-key index
    /// stays binary and admits both casings — loads and leases silently cross two flows. Checked
    /// first and always, even before the table exists: it poisons every future query no matter who
    /// creates the table.
    /// </description></item>
    /// <item><description>
    /// A real TABLE under the name. The column views below happily describe a VIEW, which has no
    /// key to raise ORA-00001, so it would pass every shape check and then lose duplicate
    /// detection (or fail outright) at the first MERGE.
    /// </description></item>
    /// <item><description>
    /// A unique key on flow_id alone. <see cref="TryCreateAsync"/> is the engine's insert-if-absent
    /// primitive: its MERGE detects "already exists" from ORA-00001 when two creates race — with no
    /// such key nothing raises it, both MERGEs insert, and one flow id gets two rows and two
    /// executions.
    /// </description></item>
    /// </list>
    /// The expiry index is deliberately not verified: it is performance-only (loads and pruning
    /// filter on expiry either way), the same standard the sibling stores apply to theirs.
    /// </summary>
    private async Task<bool> VerifyFlowTableAsync(OracleConnection connection, CancellationToken cancellationToken)
    {
        await VerifyComparisonSemanticsAsync(connection, cancellationToken).ConfigureAwait(false);

        if (await ResolveBaseTableAsync(connection, cancellationToken).ConfigureAwait(false) is not { } table)
        {
            // The table does not exist: AutoCreateSchema = false and the migration has not run yet.
            // That surfaces at the first query with a clear ORA-00942, and failing here would break
            // the documented "create it yourself, later" workflow. Returning false leaves _created
            // unlatched, so a table created later is still verified before it is trusted.
            return false;
        }

        var columns = new Dictionary<string, ActualColumn>(StringComparer.OrdinalIgnoreCase);
        await using (var command = connection.CreateCommand())
        {
            command.BindByName = true;
            // ALL_TAB_COLS rather than ALL_TAB_COLUMNS: it carries VIRTUAL_COLUMN and
            // IDENTITY_COLUMN, and HIDDEN_COLUMN = 'NO' keeps user columns — including INVISIBLE
            // ones, which still break INSERTs that do not name them — while dropping the
            // system-generated ones function-based indexes add. DEFAULT_LENGTH stands in for
            // DATA_DEFAULT, which is a LONG and cannot be filtered or fetched cheaply; only its
            // presence matters here.
            command.CommandText =
                """
                SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, CHAR_LENGTH, DATA_PRECISION, DATA_SCALE,
                       DEFAULT_LENGTH, VIRTUAL_COLUMN, IDENTITY_COLUMN
                FROM ALL_TAB_COLS
                WHERE OWNER = :owner AND TABLE_NAME = :table_name AND HIDDEN_COLUMN = 'NO'
                """;
            command.Parameters.Add(new OracleParameter("owner", table.Owner));
            command.Parameters.Add(new OracleParameter("table_name", table.Name));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                columns[reader.GetString(0)] = new ActualColumn(
                    DataType: reader.GetString(1),
                    Nullable: string.Equals(reader.GetString(2), "Y", StringComparison.OrdinalIgnoreCase),
                    CharLength: reader.IsDBNull(3) ? null : reader.GetInt64(3),
                    Precision: reader.IsDBNull(4) ? null : reader.GetInt64(4),
                    Scale: reader.IsDBNull(5) ? null : reader.GetInt64(5),
                    HasDefault: !reader.IsDBNull(6),
                    Virtual: !reader.IsDBNull(7) && string.Equals(reader.GetString(7), "YES", StringComparison.OrdinalIgnoreCase),
                    Identity: !reader.IsDBNull(8) && string.Equals(reader.GetString(8), "YES", StringComparison.OrdinalIgnoreCase));
            }
        }

        foreach (var expected in ExpectedColumns)
        {
            if (!columns.TryGetValue(expected.Name, out var actual))
            {
                throw new InvalidOperationException(
                    $"The Oracle durable-flow table '{_options.TableName}' has no '{expected.Name}' column. It was created by an " +
                    "earlier build or by hand and does not match the shape this store reads and writes " +
                    $"({string.Join(", ", ExpectedColumns.Select(column => $"{column.Name} {column.Declaration}"))}). Re-create it, " +
                    "or add the missing columns — the DDL is in docs/durable-flow-state-stores.md.");
            }

            if (expected.Mismatch(actual) is { } mismatch)
            {
                throw new InvalidOperationException(
                    $"The Oracle durable-flow table '{_options.TableName}' declares {expected.Name} as '{actual.DataType}" +
                    $"{(actual.Nullable ? " NULL" : " NOT NULL")}', which {mismatch}. This store needs " +
                    $"{expected.Name} {expected.Declaration}. Fix it with " +
                    $"ALTER TABLE {_options.TableName} MODIFY ({expected.Name} {expected.Declaration}); " +
                    "(tables this build creates get that shape automatically).");
            }
        }

        // Columns this store never names in an INSERT. One that the database cannot fill in for
        // itself makes EVERY create fail — the shape is otherwise perfect, so the failure arrives
        // at the first flow rather than at startup, which is the wrong end of the deployment.
        // Virtual and identity columns are fine; so is anything nullable or defaulted.
        foreach (var (name, actual) in columns)
        {
            if (ExpectedColumns.Any(expected => string.Equals(expected.Name, name, StringComparison.OrdinalIgnoreCase))
                || actual.IsWritableWithoutValue)
            {
                continue;
            }

            throw new InvalidOperationException(
                $"The Oracle durable-flow table '{_options.TableName}' has an extra column '{name}' ({actual.DataType} NOT NULL) " +
                "with no default. This store writes only its own columns, so every flow creation would fail on that column. Give " +
                "it a default, make it nullable, virtual, or identity, or move it to a table of your own.");
        }

        await VerifyFlowIdIsUniqueAsync(connection, table.Owner, table.Name, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Resolves what the store's unqualified table name actually reaches — the same path Oracle's
    /// own name resolution takes: an object in CURRENT_SCHEMA, else a private synonym there, else
    /// a PUBLIC synonym, following synonym chains to the base object. USER_* views describe the
    /// CONNECTING user, which is the wrong scope whenever a logon trigger sets CURRENT_SCHEMA or
    /// the table is reached through a synonym: they either go blank (silently skipping every
    /// check) or report the synonym itself as a disqualifying non-TABLE. Returns null when nothing
    /// resolves (table absent) or the chain leaves this database (a DB link, unverifiable here);
    /// throws when the name resolves to a non-TABLE object.
    /// </summary>
    private async Task<(string Owner, string Name)?> ResolveBaseTableAsync(OracleConnection connection, CancellationToken cancellationToken)
    {
        string? owner;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') FROM dual";
            owner = (string?)await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }

        if (string.IsNullOrEmpty(owner))
            return null;

        var name = CatalogName;
        // Oracle itself raises ORA-01775 on a looping synonym chain; the bound only keeps a
        // broken catalog from spinning this check.
        for (var hop = 0; hop < 10; hop++)
        {
            var objectTypes = new List<string>();
            await using (var command = connection.CreateCommand())
            {
                command.BindByName = true;
                // Only the namespaces that can collide with a table: tables, views, materialized
                // views, synonyms, and sequences share one namespace (indexes and triggers do
                // not). A materialized view registers BOTH a TABLE and a MATERIALIZED VIEW row for
                // its name, so any non-TABLE row disqualifies it.
                command.CommandText =
                    """
                    SELECT OBJECT_TYPE FROM ALL_OBJECTS
                    WHERE OWNER = :owner AND OBJECT_NAME = :object_name
                      AND OBJECT_TYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'SYNONYM', 'SEQUENCE')
                    """;
                command.Parameters.Add(new OracleParameter("owner", owner));
                command.Parameters.Add(new OracleParameter("object_name", name));
                await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    objectTypes.Add(reader.GetString(0));
            }

            if (objectTypes.Count == 0)
            {
                // Nothing in this schema: an unqualified name falls through to a PUBLIC synonym.
                if (await ResolveSynonymAsync(connection, "PUBLIC", name, cancellationToken).ConfigureAwait(false) is not { } publicTarget)
                    return null;

                (owner, name) = publicTarget;
                continue;
            }

            // Synonyms share the object namespace within a schema, so a SYNONYM row is the only
            // row: follow it.
            if (objectTypes.Contains("SYNONYM"))
            {
                if (await ResolveSynonymAsync(connection, owner, name, cancellationToken).ConfigureAwait(false) is not { } target)
                    return null;

                (owner, name) = target;
                continue;
            }

            if (objectTypes.Find(type => !type.Equals("TABLE", StringComparison.OrdinalIgnoreCase)) is { } notATable)
            {
                throw new InvalidOperationException(
                    $"The Oracle durable-flow table name '{_options.TableName}' resolves to a {notATable} ({owner}.{name}), not a table. Reads against " +
                    "it may even work, but this store's MERGE-based create needs a real table with a unique key on flow_id to detect " +
                    "duplicate flows, so writes would fail — or double-run flows — mid-operation instead of at startup. Point " +
                    $"{nameof(OracleDurableFlowOptions)}.{nameof(OracleDurableFlowOptions.TableName)} at a table, or drop the " +
                    $"{notATable} and let the store create the table.");
            }

            return (owner, name);
        }

        throw new InvalidOperationException(
            $"The Oracle durable-flow table name '{_options.TableName}' did not resolve to a base table within 10 synonym hops; " +
            "the synonym chain is looping or degenerate.");
    }

    private static async Task<(string Owner, string Name)?> ResolveSynonymAsync(
        OracleConnection connection,
        string owner,
        string name,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.BindByName = true;
        command.CommandText =
            """
            SELECT TABLE_OWNER, TABLE_NAME, DB_LINK FROM ALL_SYNONYMS
            WHERE OWNER = :owner AND SYNONYM_NAME = :synonym_name
            """;
        command.Parameters.Add(new OracleParameter("owner", owner));
        command.Parameters.Add(new OracleParameter("synonym_name", name));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        // A DB-link synonym points outside this database; the local catalog cannot describe it.
        if (!await reader.IsDBNullAsync(2, cancellationToken).ConfigureAwait(false))
            return null;

        if (await reader.IsDBNullAsync(0, cancellationToken).ConfigureAwait(false))
            return null;

        return (reader.GetString(0), reader.GetString(1));
    }

    /// <summary>
    /// Requires ordinal NVARCHAR2 comparison in this session. NLS_COMP=BINARY (Oracle's default)
    /// compares bytes regardless of NLS_SORT; NLS_COMP=LINGUISTIC (or the deprecated ANSI) routes
    /// every comparison through NLS_SORT instead, where anything but BINARY — BINARY_CI,
    /// BINARY_AI, a language sort — folds case or accents. Sessions inherit these from instance
    /// parameters, client NLS configuration, and logon triggers, uniformly for every connection
    /// this store opens, so one check on one pooled session stands for all of them. Internal (not
    /// private) so the integration suite can run it against a deliberately mis-set session without
    /// installing a logon trigger.
    /// </summary>
    internal static async Task VerifyComparisonSemanticsAsync(OracleConnection connection, CancellationToken cancellationToken)
    {
        string? comp = null;
        string? sort = null;
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT PARAMETER, VALUE FROM NLS_SESSION_PARAMETERS WHERE PARAMETER IN ('NLS_COMP', 'NLS_SORT')";
            await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (string.Equals(reader.GetString(0), "NLS_COMP", StringComparison.OrdinalIgnoreCase))
                    comp = reader.GetString(1);
                else
                    sort = reader.GetString(1);
            }
        }

        if (DiagnoseComparisonSemantics(comp, sort) is { } linguisticSession)
            throw new InvalidOperationException(linguisticSession);
    }

    /// <summary>
    /// The comparison-semantics decision on its own catalog inputs: the rejection message for a
    /// session whose NLS settings fold flow-id equality, or null for an ordinal one. Folded
    /// equality is the silent kind of wrong — every <c>WHERE flow_id = :flow_id</c> this store
    /// runs matches BOTH casings, so a load returns another flow's state and a lease update fences
    /// the other flow's execution, while the primary-key index (always binary) keeps admitting
    /// both rows. Nothing errors; flows cross-route. A session whose parameters could not be read
    /// is not known to be ordinal, so silence does not pass.
    /// </summary>
    internal static string? DiagnoseComparisonSemantics(string? nlsComp, string? nlsSort)
    {
        static bool IsBinary(string? value) => string.Equals(value, "BINARY", StringComparison.OrdinalIgnoreCase);
        if (IsBinary(nlsComp) || IsBinary(nlsSort))
            return null;

        return
            $"This Oracle session compares text linguistically (NLS_COMP='{nlsComp ?? "(unknown)"}', " +
            $"NLS_SORT='{nlsSort ?? "(unknown)"}'), so flow ids differing only in case (or accent) match each " +
            "other's rows: a load can return the other flow's state and a lease can fence the other flow's " +
            "execution, while the binary primary-key index still admits both ids. Flow ids are compared " +
            "ordinally by the engine. Restore binary comparison with ALTER SESSION SET NLS_COMP = BINARY (or " +
            "NLS_SORT = BINARY) — typically by removing the logon trigger or client NLS configuration that " +
            "changed them.";
    }

    /// <summary>One column as <c>USER_TAB_COLS</c> reports it. Internal (with the expected shape
    /// below) so the accept/reject decision table is unit-testable without an Oracle server — the
    /// integration suite proves the same decisions against a real catalog, but only where the
    /// Oracle container runs.</summary>
    internal readonly record struct ActualColumn(
        string DataType,
        bool Nullable,
        long? CharLength,
        long? Precision,
        long? Scale,
        bool HasDefault,
        bool Virtual,
        bool Identity)
    {
        /// <summary>
        /// Whether this store could insert a row without naming this column. True when the column
        /// is nullable, carries a default, is computed by the database, or is an identity.
        /// </summary>
        internal bool IsWritableWithoutValue => Nullable || HasDefault || Virtual || Identity;
    }

    /// <summary>
    /// What this store needs from one column, and the check that says so. Widths and precisions
    /// are MINIMA rather than exact matches: a wider flow_id or a higher-precision timestamp still
    /// satisfies every promise the store makes, and rejecting a more generous schema would be a
    /// false alarm. Too NARROW is not — NVARCHAR2(10) passes a name-only check and then errors on
    /// the first 400-character id the public contract permits.
    /// </summary>
    internal sealed record ExpectedColumn(string Name, string Declaration, string DataType, bool Nullable, long? Minimum = null)
    {
        internal string? Mismatch(ActualColumn actual)
        {
            if (string.Equals(DataType, "TIMESTAMP", StringComparison.Ordinal))
            {
                // USER_TAB_COLS embeds the fractional precision in the type name ('TIMESTAMP(6)'),
                // so the family is a prefix match. The WITH [LOCAL] TIME ZONE variants are
                // rejected as different types: their values shift with the session time zone, and
                // expiry/lease math must stay on the plain UTC timestamps this store writes.
                if (!actual.DataType.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase)
                    || actual.DataType.Contains("TIME ZONE", StringComparison.OrdinalIgnoreCase))
                {
                    return $"is a '{actual.DataType}'";
                }
            }
            else if (!string.Equals(actual.DataType, DataType, StringComparison.OrdinalIgnoreCase))
            {
                return $"is a '{actual.DataType}'";
            }

            if (actual.Nullable != Nullable)
                return Nullable ? "is NOT NULL (this store writes NULL to it)" : "is nullable";
            if (Minimum is not { } minimum)
                return null;

            // CHAR_LENGTH for the character columns, DATA_SCALE (fractional-second digits) for the
            // timestamps, DATA_PRECISION for NUMBER: a TIMESTAMP(0) column silently rounds the
            // sub-second lease arithmetic this store runs on SYS_EXTRACT_UTC(SYSTIMESTAMP), which
            // is how two workers end up holding one lease, and a NUMBER(9) revision overflows
            // without a word. An unconstrained NUMBER (null precision) holds 38 digits and passes.
            var actualSize = DataType switch
            {
                "TIMESTAMP" => actual.Scale,
                "NUMBER" => actual.Precision ?? 38,
                _ => actual.CharLength
            };
            return actualSize is { } size && size >= minimum
                ? null
                : $"holds {actualSize?.ToString() ?? "an unknown size"} where at least {minimum} is required";
        }
    }

    /// <summary>
    /// The shape this store reads and writes. flow_id, lease_id, and state_json are the NATIONAL
    /// character types on purpose: NVARCHAR2/NCLOB store the national character set, which is
    /// always Unicode, while VARCHAR2/CLOB inherit the database character set — on a non-AL32UTF8
    /// database a perfectly legal flow id (an emoji, most non-Latin text) mangles or fails on
    /// insert, so those types are rejected rather than trusted. No default expressions are
    /// verified because none are load-bearing: every write names every column except the two lease
    /// fields, whose absence means NULL.
    /// </summary>
    internal static readonly ExpectedColumn[] ExpectedColumns =
    [
        new("flow_id", "NVARCHAR2(400) NOT NULL", "NVARCHAR2", Nullable: false, Minimum: 400),
        new("state_json", "NCLOB NOT NULL", "NCLOB", Nullable: false),
        new("expires_at_utc", "TIMESTAMP(6) NOT NULL", "TIMESTAMP", Nullable: false, Minimum: 6),
        new("updated_at_utc", "TIMESTAMP(6) NOT NULL", "TIMESTAMP", Nullable: false, Minimum: 6),
        new("revision", "NUMBER(19) DEFAULT 0 NOT NULL", "NUMBER", Nullable: false, Minimum: 19),
        new("lease_id", "NVARCHAR2(64) NULL", "NVARCHAR2", Nullable: true, Minimum: 64),
        new("lease_expires_at_utc", "TIMESTAMP(6) NULL", "TIMESTAMP", Nullable: true, Minimum: 6)
    ];

    /// <summary>
    /// Requires a unique key on the WHOLE of flow_id and nothing else. The PRIMARY KEY this
    /// store's DDL declares is the usual shape, but any enabled single-column UNIQUE constraint —
    /// or a bare unique INDEX, which raises ORA-00001 without a constraint row — serves the
    /// MERGE's duplicate detection, so all of them are accepted. A COMPOSITE key is not: it
    /// permits two rows with one flow_id. A DISABLED constraint is not either: it sits in the
    /// catalog and enforces nothing. (Deferrable constraints use a NONUNIQUE index, which is why
    /// the constraint and index arms are both asked.)
    /// </summary>
    private async Task VerifyFlowIdIsUniqueAsync(OracleConnection connection, string owner, string tableName, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.BindByName = true;
        command.CommandText =
            """
            SELECT 1 FROM dual
            WHERE EXISTS (
                SELECT 1 FROM ALL_CONSTRAINTS c
                WHERE c.OWNER = :owner AND c.TABLE_NAME = :table_name AND c.CONSTRAINT_TYPE IN ('P', 'U') AND c.STATUS = 'ENABLED'
                  AND EXISTS (SELECT 1 FROM ALL_CONS_COLUMNS k
                              WHERE k.OWNER = c.OWNER AND k.CONSTRAINT_NAME = c.CONSTRAINT_NAME AND k.COLUMN_NAME = 'FLOW_ID')
                  AND NOT EXISTS (SELECT 1 FROM ALL_CONS_COLUMNS o
                                  WHERE o.OWNER = c.OWNER AND o.CONSTRAINT_NAME = c.CONSTRAINT_NAME AND o.COLUMN_NAME <> 'FLOW_ID'))
               OR EXISTS (
                SELECT 1 FROM ALL_INDEXES i
                WHERE i.TABLE_OWNER = :owner AND i.TABLE_NAME = :table_name AND i.UNIQUENESS = 'UNIQUE'
                  AND EXISTS (SELECT 1 FROM ALL_IND_COLUMNS k
                              WHERE k.INDEX_OWNER = i.OWNER AND k.INDEX_NAME = i.INDEX_NAME AND k.COLUMN_NAME = 'FLOW_ID')
                  AND NOT EXISTS (SELECT 1 FROM ALL_IND_COLUMNS o
                                  WHERE o.INDEX_OWNER = i.OWNER AND o.INDEX_NAME = i.INDEX_NAME AND o.COLUMN_NAME <> 'FLOW_ID'))
            """;
        command.Parameters.Add(new OracleParameter("owner", owner));
        command.Parameters.Add(new OracleParameter("table_name", tableName));

        if (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not null)
            return;

        throw new InvalidOperationException(
            $"The Oracle durable-flow table '{_options.TableName}' has no enabled unique key on the whole of flow_id. Starting a " +
            "flow is an insert-if-absent, and this store's MERGE learns that a ledger already exists from the duplicate-key error " +
            "ORA-00001 when two starts race. Without such a key nothing reports the duplicate, so two concurrent starts of one " +
            $"flow id both insert and the flow runs twice. Fix it with ALTER TABLE {_options.TableName} ADD PRIMARY KEY (flow_id); " +
            "(tables this build creates declare it automatically).");
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
        command.BindByName = true;
        // Lease fencing runs entirely on the database clock: acquire steals only leases the
        // database considers expired, and renew/extend stays relative to the server's UTC time,
        // so worker clock skew can never make two nodes hold the same lease.
        command.CommandText =
            $"""
            UPDATE {Table}
            SET lease_id = :lease_id, lease_expires_at_utc = {AddMilliseconds(":lease_ms")}
            WHERE flow_id = :flow_id
              AND expires_at_utc > {UtcNowSql}
              AND {(acquire ? $"(lease_id IS NULL OR lease_expires_at_utc <= {UtcNowSql} OR lease_id = :lease_id)" : $"lease_id = :lease_id AND lease_expires_at_utc > {UtcNowSql}")}
            """;
        command.Parameters.Add(NationalId("flow_id", flowId));
        command.Parameters.Add(NationalId("lease_id", leaseId));
        command.Parameters.Add(new OracleParameter("lease_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(leaseDuration)));
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    private Task<OracleConnection> OpenConnectionAsync(CancellationToken cancellationToken)
        => DurableFlowStoreShared.OpenConnectionAsync<OracleConnection>(_options.ConnectionString, cancellationToken);

    private string Table => _options.TableName;
    private string IndexName => DurableFlowStoreShared.DerivedName(_options.TableName, "_EXPIRES_IDX", 128);

    /// <summary>
    /// The name the data dictionary stores. This store interpolates <see cref="Table"/> unquoted,
    /// which Oracle resolves to the upper-cased catalog entry; the validated identifier alphabet
    /// (ASCII letters, digits, underscore) makes the invariant upper-casing exact, so catalog
    /// lookups on this name describe precisely the object the store's SQL touches.
    /// </summary>
    private string CatalogName => _options.TableName.ToUpperInvariant();
}
}
