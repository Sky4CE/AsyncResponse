using AsyncResponse.DurableFlows.Sqlite;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Under <c>AutoCreateSchema = false</c> the SQLite store issues no DDL, but it must still verify
/// the operator-provisioned table before trusting it — the same contract as the MySQL/Oracle/
/// PostgreSQL/SQL Server flow stores. Shape mismatches otherwise surface as the provider's opaque
/// error at the first flow (or, worse, misbehave silently), which is the wrong end of the
/// deployment. An absent table is NOT an error: the documented workflow provisions it later, so
/// the store keeps re-checking without latching.
/// </summary>
public sealed class SqliteOperatorSchemaVerificationTests
{
    [Fact]
    public async Task OperatorTable_WithoutSingleColumnPrimaryKey_FailsWithActionableError()
    {
        await using var database = new TempSqliteDatabase();
        await ProvisionAsync(database,
            """
            CREATE TABLE asyncresponse_flow_state (
                flow_id TEXT NOT NULL,
                state_json TEXT NOT NULL,
                expires_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 0,
                lease_id TEXT NULL,
                lease_expires_at_utc TEXT NULL
            );
            """);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateStore(database).TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        Assert.Contains("PRIMARY KEY (flow_id)", error.Message);
    }

    [Fact]
    public async Task OperatorTable_WithNumericTimestampAffinity_FailsWithActionableError()
    {
        // Expiry and lease fencing compare ISO-8601 strings lexicographically; DATETIME resolves
        // to NUMERIC affinity, which coerces digit-only values and breaks that ordering. Before
        // verification existed this shape was silently accepted.
        await using var database = new TempSqliteDatabase();
        await ProvisionAsync(database,
            """
            CREATE TABLE asyncresponse_flow_state (
                flow_id TEXT NOT NULL PRIMARY KEY,
                state_json TEXT NOT NULL,
                expires_at_utc DATETIME NOT NULL,
                updated_at_utc TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 0,
                lease_id TEXT NULL,
                lease_expires_at_utc TEXT NULL
            );
            """);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateStore(database).LoadAsync("flow"));
        Assert.Contains("expires_at_utc", error.Message);
        Assert.Contains("affinity", error.Message);
    }

    [Fact]
    public async Task OperatorTable_WithNotNullLeaseColumn_FailsWithActionableError()
    {
        // The store's INSERT never names lease_id and ReleaseLeaseAsync writes NULL there, so a
        // NOT NULL lease column breaks both; without verification the failure was an opaque
        // constraint error at the first create.
        await using var database = new TempSqliteDatabase();
        await ProvisionAsync(database,
            """
            CREATE TABLE asyncresponse_flow_state (
                flow_id TEXT NOT NULL PRIMARY KEY,
                state_json TEXT NOT NULL,
                expires_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 0,
                lease_id TEXT NOT NULL DEFAULT '',
                lease_expires_at_utc TEXT NULL
            );
            """);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateStore(database).TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        Assert.Contains("lease_id", error.Message);
    }

    [Fact]
    public async Task OperatorTable_MissingColumn_FailsWithActionableError()
    {
        await using var database = new TempSqliteDatabase();
        await ProvisionAsync(database,
            """
            CREATE TABLE asyncresponse_flow_state (
                flow_id TEXT NOT NULL PRIMARY KEY,
                state_json TEXT NOT NULL,
                expires_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            );
            """);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateStore(database).TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        Assert.Contains("no 'revision' column", error.Message);
    }

    [Fact]
    public async Task OperatorTable_WithExtraNotNullColumnWithoutDefault_FailsWithActionableError()
    {
        // The store writes only its own columns, so a required extra column the database cannot
        // fill in makes EVERY create fail — at the first flow rather than at startup.
        await using var database = new TempSqliteDatabase();
        await ProvisionAsync(database,
            """
            CREATE TABLE asyncresponse_flow_state (
                flow_id TEXT NOT NULL PRIMARY KEY,
                state_json TEXT NOT NULL,
                expires_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 0,
                lease_id TEXT NULL,
                lease_expires_at_utc TEXT NULL,
                audit_owner TEXT NOT NULL
            );
            """);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateStore(database).TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        Assert.Contains("extra column 'audit_owner'", error.Message);
    }

    [Fact]
    public async Task OperatorTable_EquivalentAffinities_AndFillableExtras_Verify()
    {
        // Declared types are compared by SQLite AFFINITY, not spelling: varchar/clob resolve to
        // TEXT and bigint to INTEGER, so any declaration that behaves like the documented DDL
        // passes. Nullable, defaulted, and generated extra columns all fill themselves in.
        await using var database = new TempSqliteDatabase();
        await ProvisionAsync(database,
            """
            CREATE TABLE asyncresponse_flow_state (
                flow_id varchar(400) NOT NULL PRIMARY KEY,
                state_json clob NOT NULL,
                expires_at_utc varchar(64) NOT NULL,
                updated_at_utc varchar(64) NOT NULL,
                revision bigint NOT NULL DEFAULT 0,
                lease_id varchar(64) NULL,
                lease_expires_at_utc varchar(64) NULL,
                audit_note TEXT NULL,
                audit_stamp TEXT NOT NULL DEFAULT 'n/a',
                audit_len INTEGER GENERATED ALWAYS AS (length(flow_id)) VIRTUAL
            );
            """);

        var store = CreateStore(database);
        Assert.True(await store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        Assert.Equal("flow", (await store.LoadAsync("flow"))!.FlowId);
    }

    [Fact]
    public async Task OperatorTable_Absent_IsNotLatched_AndVerifiesOnceProvisioned()
    {
        // The documented "create it yourself, later" workflow: before the migration runs, the
        // operation fails with the provider's clear no-such-table error (verification must not
        // turn that into a startup failure) — and the store must NOT latch, so the shape checks
        // still run once the table appears.
        await using var database = new TempSqliteDatabase();
        var store = CreateStore(database);

        await Assert.ThrowsAsync<SqliteException>(
            () => store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));

        // The migration lands with a WRONG shape: the same store instance must reject it, which
        // proves the absent-table probe did not latch verification off.
        await ProvisionAsync(database,
            """
            CREATE TABLE asyncresponse_flow_state (
                flow_id TEXT NOT NULL,
                state_json TEXT NOT NULL,
                expires_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 0,
                lease_id TEXT NULL,
                lease_expires_at_utc TEXT NULL
            );
            """);
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));

        // Corrected in place: the store verifies, latches, and works.
        await ProvisionAsync(database, "DROP TABLE asyncresponse_flow_state;");
        await ProvisionAsync(database,
            """
            CREATE TABLE asyncresponse_flow_state (
                flow_id TEXT NOT NULL PRIMARY KEY,
                state_json TEXT NOT NULL,
                expires_at_utc TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL,
                revision INTEGER NOT NULL DEFAULT 0,
                lease_id TEXT NULL,
                lease_expires_at_utc TEXT NULL
            );
            """);
        Assert.True(await store.TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));
        Assert.Equal("flow", (await store.LoadAsync("flow"))!.FlowId);
    }

    [Fact]
    public async Task OperatorPath_IssuesNoDdl()
    {
        // Verification is read-only: no table, no index, and no WAL pragma appear as side
        // effects (manually-provisioned databases own their journal mode — see the docs note).
        await using var database = new TempSqliteDatabase();
        await Assert.ThrowsAsync<SqliteException>(
            () => CreateStore(database).TryCreateAsync("flow", CreateState("flow"), TimeSpan.FromMinutes(5)));

        await using var connection = new SqliteConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using (var tables = connection.CreateCommand())
        {
            tables.CommandText = "SELECT COUNT(*) FROM sqlite_master;";
            Assert.Equal(0L, (long)(await tables.ExecuteScalarAsync())!);
        }

        await using var journal = connection.CreateCommand();
        journal.CommandText = "PRAGMA journal_mode;";
        Assert.Equal("delete", (string)(await journal.ExecuteScalarAsync())!, ignoreCase: true);
    }

    private static SqliteFlowStateStore CreateStore(TempSqliteDatabase database)
        => new(Options.Create(new SqliteDurableFlowOptions
        {
            ConnectionString = database.ConnectionString,
            AutoCreateSchema = false
        }));

    private static async Task ProvisionAsync(TempSqliteDatabase database, string ddl)
    {
        await using var connection = new SqliteConnection(database.ConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = ddl;
        await command.ExecuteNonQueryAsync();
    }

    private static FlowState CreateState(string flowId) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(TestOnboardingFlow).FullName,
        InputTypeName = typeof(TestFlowInput).FullName,
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    private sealed class TempSqliteDatabase : IAsyncDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"ar-flow-verify-{Guid.NewGuid():N}.db");

        public string ConnectionString => $"Data Source={_path}";

        public ValueTask DisposeAsync()
        {
            // Release this database's pooled handles (targeted — other tests' pools are not
            // touched), then best-effort delete the file and its WAL sidecars.
            SqliteConnection.ClearPool(new SqliteConnection(ConnectionString));
            foreach (var file in new[] { _path, _path + "-wal", _path + "-shm" })
            {
                try
                {
                    File.Delete(file);
                }
                catch (IOException)
                {
                }
            }

            return ValueTask.CompletedTask;
        }
    }
}
