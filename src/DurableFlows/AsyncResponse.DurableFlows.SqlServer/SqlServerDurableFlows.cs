using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.SqlServer;
using AsyncResponse.Internal;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>DI registration for the SQL Server durable-flow state store.</summary>
    public static class SqlServerDurableFlowServiceCollectionExtensions
    {
        /// <summary>Stores durable-flow state in SQL Server.</summary>
        public static AsyncResponseRegistrationBuilder WithSqlServerDurableFlows(
            this AsyncResponseRegistrationBuilder builder,
            Action<SqlServerDurableFlowOptions>? configure = null)
        {
            // Singleton on purpose: schema provisioning is cached per store instance, and the
            // executor resolves the store from a fresh scope per flow execution — a scoped store
            // would re-run EnsureCreated's DDL round-trip on every run.
            builder.Services.TryAddSingleton<SqlServerFlowStateStore>();
            return builder.WithDurableFlows<SqlServerFlowStateStore, SqlServerDurableFlowOptions>(configure);
        }
    }
}

namespace AsyncResponse.DurableFlows.SqlServer
{
/// <summary>Options for the SQL Server durable-flow state store.</summary>
public sealed class SqlServerDurableFlowOptions : DurableFlowOptions
{
    /// <summary>SQL Server connection string. Required.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Database schema that contains the durable-flow table. Default: <c>dbo</c>.</summary>
    public string SchemaName { get; set; } = "dbo";

    /// <summary>Table storing one durable-flow ledger row per flow id.</summary>
    public string TableName { get; set; } = "asyncresponse_flow_state";

    /// <summary>Creates the schema, table, and expiry index on first use.</summary>
    public bool AutoCreateSchema { get; set; } = true;

    /// <summary>
    /// How often <see cref="SqlServerFlowStateStore.TryCreateAsync"/> opportunistically deletes one
    /// bounded batch (1000 rows) of expired rows (loads already treat expired state as absent;
    /// pruning bounds table growth). Zero or negative prunes on every save. Default: 5 minutes.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum serialized flow-state size in bytes accepted by writes; oversized ledgers fail fast
    /// with an actionable error instead of an opaque provider error. Default: <c>null</c>
    /// (unlimited — <c>nvarchar(max)</c> is effectively unbounded), settable as an operator budget.
    /// </summary>
    public long? MaxStateBytes { get; set; }

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        DurableFlowStoreShared.ValidateConnectionString(ConnectionString, nameof(SqlServerDurableFlowOptions));
        DurableFlowStoreShared.ValidateIdentifier(SchemaName, $"{nameof(SqlServerDurableFlowOptions)}.{nameof(SchemaName)}", "SQL Server", identifierCap: 128);
        DurableFlowStoreShared.ValidateIdentifier(TableName, $"{nameof(SqlServerDurableFlowOptions)}.{nameof(TableName)}", "SQL Server", identifierCap: 128);
        DurableFlowStoreShared.ValidateMaxStateBytes(MaxStateBytes, nameof(SqlServerDurableFlowOptions));
    }
}

/// <summary>SQL Server implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class SqlServerFlowStateStore : IFlowStateStore
{
    private const int PruneBatchSize = 1000;

    private readonly SqlServerDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private long _lastPruneTicks;
    private volatile bool _created;

    public SqlServerFlowStateStore(IOptions<SqlServerDurableFlowOptions> options)
    {
        _options = options.Value;
        _options.Validate();
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        // All expiry/lease time math in this store runs on the database clock (SYSUTCDATETIME()),
        // never an app-computed timestamp: with multiple workers, app clock skew beyond the lease
        // window would let two nodes both consider a lease expired and double-run a flow.
        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT state_json, revision FROM {Table} WHERE flow_id = @flow_id AND expires_at_utc > SYSUTCDATETIME();";
        command.Parameters.AddWithValue("@flow_id", flowId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        return DurableFlowStoreShared.ReadState(flowId, reader.GetString(0), reader.GetInt64(1));
    }

    public async Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "SQL Server");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);
        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await DurableFlowStoreShared.PruneQuietlyAsync(() => PruneExpiredAsync(cancellationToken)).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            MERGE {Table} WITH (HOLDLOCK) AS target
            USING (SELECT @flow_id AS flow_id) AS source ON target.flow_id = source.flow_id
            WHEN MATCHED AND target.expires_at_utc <= SYSUTCDATETIME() THEN
                UPDATE SET state_json = @state_json,
                           expires_at_utc = {AddMilliseconds("@ttl_ms")},
                           updated_at_utc = SYSUTCDATETIME(),
                           revision = @revision,
                           lease_id = NULL,
                           lease_expires_at_utc = NULL
            WHEN NOT MATCHED THEN
                INSERT (flow_id, state_json, expires_at_utc, updated_at_utc, revision)
                VALUES (@flow_id, @state_json, {AddMilliseconds("@ttl_ms")}, SYSUTCDATETIME(), @revision);
            """;
        command.Parameters.AddWithValue("@flow_id", flowId);
        command.Parameters.AddWithValue("@state_json", stateJson);
        command.Parameters.AddWithValue("@ttl_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(ttl));
        command.Parameters.AddWithValue("@revision", state.Revision);
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
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "SQL Server");
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        await using var connection = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            UPDATE {Table}
            SET state_json = @state_json,
                expires_at_utc = {AddMilliseconds("@ttl_ms")},
                updated_at_utc = SYSUTCDATETIME(),
                revision = @new_revision
            WHERE flow_id = @flow_id
              AND revision = @expected_revision
              AND expires_at_utc > SYSUTCDATETIME()
              AND (@lease_id IS NULL OR (lease_id = @lease_id AND lease_expires_at_utc > SYSUTCDATETIME()));
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
        command.CommandText = $"DELETE TOP ({PruneBatchSize}) FROM {Table} WHERE expires_at_utc <= SYSUTCDATETIME();";
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

            if (!_options.AutoCreateSchema)
            {
                // Operator-managed schema: no DDL and no DDL lock, but the SAME catalog
                // verification the DDL path runs — an operator-provisioned table with the wrong
                // shape (a case-insensitive flow_id collation above all) fails silently at
                // runtime, which is exactly what verification exists to catch. An absent object is
                // fine: the migration has not run yet, the first query surfaces a clear SQL Server
                // error (the documented "create it yourself, later" workflow), and _created stays
                // unlatched so a later operation re-verifies once the migration lands.
                if (!await ObjectExistsAsync(connection, cancellationToken).ConfigureAwait(false))
                    return;

                await VerifyRelationsAsync(connection, transaction: null, cancellationToken).ConfigureAwait(false);
                _created = true;
                return;
            }

            await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            // Serialize schema creation across processes. The IF-NOT-EXISTS guards are not atomic
            // against a concurrent create of the same object (catalog errors 2714/2627). The
            // transaction-scoped application lock (keyed by schema, shared with the channel/transport
            // packages) lets one instance build the schema while the rest wait and then find it
            // already present.
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
                lockCommand.Parameters.AddWithValue("@lock_resource", DurableFlowStoreShared.SchemaLockResource(_options.SchemaName));
                await lockCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText =
                $"""
                IF SCHEMA_ID(N'{_options.SchemaName}') IS NULL
                    EXEC(N'CREATE SCHEMA {Quote(_options.SchemaName)}');

                IF OBJECT_ID(N'{_options.SchemaName}.{_options.TableName}', N'U') IS NULL
                CREATE TABLE {Table} (
                    flow_id nvarchar(400) COLLATE Latin1_General_100_BIN2 NOT NULL PRIMARY KEY,
                    state_json nvarchar(max) NOT NULL,
                    expires_at_utc datetime2 NOT NULL,
                    updated_at_utc datetime2 NOT NULL,
                    -- Unnamed DEFAULT deliberately: the previous derived name prefixed "DF_" and
                    -- suffixed "_revision" onto the full table name, so a table name this store
                    -- otherwise accepts (117 characters) produced a 129-character constraint name
                    -- and the CREATE failed with error 103 — SQL Server's identifier cap is 128.
                    -- Nothing reads the constraint by name; the store never drops or alters it.
                    revision bigint NOT NULL DEFAULT 0,
                    lease_id nvarchar(64) NULL,
                    lease_expires_at_utc datetime2 NULL
                );

                IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{IndexName}' AND object_id = OBJECT_ID(N'{_options.SchemaName}.{_options.TableName}'))
                    CREATE INDEX {Quote(IndexName)} ON {Table} (expires_at_utc);
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
                    "durable-flow",
                    ExpectedObjects(),
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
            await VerifyRelationsAsync(connection, transaction: null, cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    private Task VerifyRelationsAsync(SqlConnection connection, SqlTransaction? transaction, CancellationToken cancellationToken)
        => SqlServerRelationVerifier.VerifyAsync(
            connection,
            transaction,
            _options.SchemaName,
            "durable-flow",
            ExpectedObjects(),
            cancellationToken);

    /// <summary>
    /// Reports whether ANY object occupies the configured name (any kind: a view or foreign
    /// component's object must reach verification, which names the precise wrong-kind reason
    /// instead of skipping the checks). The catalog's own collation decides case matching, exactly
    /// as the server resolves the runtime identifier.
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
        command.Parameters.AddWithValue("@table", _options.TableName);
        return (int)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))! == 1;
    }

    /// <summary>The catalog shape this store's DDL intends — the single source for both the
    /// post-DDL verification and the failed-batch diagnosis.</summary>
    /// <remarks>A bare <c>datetime2</c> declaration is <c>datetime2(7)</c>; the expected types
    /// state the scale, because a reduced-scale <c>lease_expires_at_utc</c> rounds the fence on
    /// store — two nodes can then both read the lease as expired and run one flow at once.</remarks>
    private SqlServerRelationVerifier.ExpectedObject[] ExpectedObjects() =>
            [
                new(_options.TableName, SqlServerObjectKind.Table,
                [
                    new("flow_id", "nvarchar(400)", Nullable: false, RequiresBinaryCollation: true),
                    new("state_json", "nvarchar(max)", Nullable: false),
                    new("expires_at_utc", "datetime2(7)", Nullable: false),
                    new("updated_at_utc", "datetime2(7)", Nullable: false),
                    new("revision", "bigint", Nullable: false),
                    new("lease_id", "nvarchar(64)", Nullable: true),
                    new("lease_expires_at_utc", "datetime2(7)", Nullable: true)
                ],
                PrimaryKey: ["flow_id"])
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
        // Lease fencing runs entirely on the database clock: acquire steals only leases the
        // database considers expired, and renew/extend stays relative to SYSUTCDATETIME(), so
        // worker clock skew can never make two nodes hold the same lease.
        command.CommandText =
            $"""
            UPDATE {Table}
            SET lease_id = @lease_id, lease_expires_at_utc = {AddMilliseconds("@lease_ms")}
            WHERE flow_id = @flow_id
              AND expires_at_utc > SYSUTCDATETIME()
              AND {(acquire ? "(lease_id IS NULL OR lease_expires_at_utc <= SYSUTCDATETIME() OR lease_id = @lease_id)" : "lease_id = @lease_id AND lease_expires_at_utc > SYSUTCDATETIME()")};
            """;
        command.Parameters.AddWithValue("@flow_id", flowId);
        command.Parameters.AddWithValue("@lease_id", leaseId);
        command.Parameters.AddWithValue("@lease_ms", DurableFlowStoreShared.ServerClockTtlMilliseconds(leaseDuration));
        return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0;
    }

    private Task<SqlConnection> OpenConnectionAsync(CancellationToken cancellationToken)
        => DurableFlowStoreShared.OpenConnectionAsync<SqlConnection>(_options.ConnectionString, cancellationToken);

    /// <summary>
    /// SQL expression adding a millisecond bigint parameter to the database clock (the same
    /// pattern as the SQL Server channel package). DATEADD only takes int arguments, so the value
    /// is split into whole seconds and a sub-second remainder — TTLs and lease durations stay on
    /// the database clock, immune to app-side clock skew, without overflowing on multi-day spans
    /// such as the 7-day default state expiry.
    /// </summary>
    private static string AddMilliseconds(string parameterName)
        => $"DATEADD(SECOND, CAST({parameterName} / 1000 AS int), DATEADD(MILLISECOND, CAST({parameterName} % 1000 AS int), SYSUTCDATETIME()))";

    private string Table => $"{Quote(_options.SchemaName)}.{Quote(_options.TableName)}";
    private string IndexName => DurableFlowStoreShared.DerivedName(_options.TableName, "_expires_idx", 128);
    private static string Quote(string identifier) => "[" + identifier + "]";
}
}
