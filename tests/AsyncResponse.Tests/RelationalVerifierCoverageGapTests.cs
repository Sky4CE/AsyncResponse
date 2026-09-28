using System.Collections;
using System.Reflection;
using System.Runtime.CompilerServices;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.DurableFlows.PostgreSQL;
using AsyncResponse.DurableFlows.SqlServer;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Server-free coverage of the source-linked relational schema helpers
/// (<c>src/Shared/SqlServerRelationVerifier.cs</c>, <c>PostgreSqlRelationVerifier.cs</c>,
/// <c>PostgreSqlDdlGuard.cs</c>, <c>RelationalNamePlan.cs</c>, <c>SqlServerTransientFaults.cs</c>).
/// Each file compiles into several provider assemblies under one full type name, so every fact
/// runs against each compiled copy by reflection over one anchor type per assembly. The pure
/// decision helpers take crafted catalog rows; the catalog-reading paths are reached only where
/// they return before touching the connection, or through <see cref="FakePostgresWireServer"/>.
/// </summary>
public sealed class RelationalVerifierCoverageGapTests
{
    public static TheoryData<Type> SqlServerVerifierAnchors =>
    [
        typeof(SqlServerAsyncResponseChannelOptions),
        typeof(SqlServerAsyncResponseTransportOptions),
        typeof(SqlServerDurableFlowOptions)
    ];

    public static TheoryData<Type> PostgreSqlAnchors =>
    [
        typeof(PostgreSqlAsyncResponseChannelOptions),
        typeof(PostgreSqlAsyncResponseTransportOptions),
        typeof(PostgreSqlDurableFlowOptions)
    ];

    /// <summary>The packages RelationalNamePlan is linked into.</summary>
    public static TheoryData<Type> NamePlanAnchors =>
    [
        typeof(PostgreSqlAsyncResponseChannelOptions),
        typeof(PostgreSqlAsyncResponseTransportOptions),
        typeof(SqlServerAsyncResponseChannelOptions),
        typeof(SqlServerAsyncResponseTransportOptions)
    ];

    /// <summary>The packages SqlServerTransientFaults is linked into.</summary>
    public static TheoryData<Type> SqlServerTransientAnchors =>
    [
        typeof(SqlServerAsyncResponseChannelOptions),
        typeof(SqlServerAsyncResponseTransportOptions)
    ];

    // ---------------------------------------------------------------------------------------
    // SqlServerRelationVerifier
    // ---------------------------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(SqlServerVerifierAnchors))]
    public void SqlServer_Describe_NamesEveryObjectKindAnOccupyingNameCanHave(Type anchor)
    {
        var verifier = new SqlVerifier(anchor);

        // The wrong-kind collision message names both kinds, so each sys.objects.type code the
        // guard can meet must read as the object an operator would look for.
        Assert.Equal("a user table", verifier.Describe("U"));
        Assert.Equal("a sequence", verifier.Describe("SO"));
        Assert.Equal("a view", verifier.Describe("V"));
        Assert.Equal("a synonym", verifier.Describe("SN"));
        Assert.Equal("a stored procedure", verifier.Describe("P"));
        Assert.Equal("a function", verifier.Describe("IF"));
        Assert.Equal("a function", verifier.Describe("TF"));
        Assert.Equal("a function", verifier.Describe("FN"));
        Assert.Equal("an object of type 'TR'", verifier.Describe("TR"));
    }

    [Theory]
    [MemberData(nameof(SqlServerVerifierAnchors))]
    public void SqlServer_EvaluateIndexes_NamesTheFoundIndexType(Type anchor)
    {
        var verifier = new SqlVerifier(anchor);
        var expected = verifier.Array(verifier.ObjectType, verifier.Index("jobs_expires_idx", "jobs", "expires_at"));

        // A clustered index is an accepted type, so only its wrong key columns reject it.
        var clustered = verifier.EvaluateIndexes(expected, ("jobs_expires_idx", "jobs"), verifier.IndexRow(type: 1, "created_at"));
        Assert.NotNull(clustered);
        Assert.Contains("found a clustered index over (created_at)", clustered.Message, StringComparison.Ordinal);

        var hash = verifier.EvaluateIndexes(expected, ("jobs_expires_idx", "jobs"), verifier.IndexRow(type: 7, "expires_at"));
        Assert.NotNull(hash);
        Assert.Contains("found a hash index over (expires_at)", hash.Message, StringComparison.Ordinal);

        // A type code the verifier has no name for is still reported, by its number.
        var spatial = verifier.EvaluateIndexes(expected, ("jobs_expires_idx", "jobs"), verifier.IndexRow(type: 4, "expires_at"));
        Assert.NotNull(spatial);
        Assert.Contains("found a type-4 index over (expires_at)", spatial.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(SqlServerVerifierAnchors))]
    public void SqlServer_EvaluateTableColumns_RejectsANonBinaryCollationOnAnIdentityColumn(Type anchor)
    {
        var verifier = new SqlVerifier(anchor);
        var expected = verifier.Array(verifier.ObjectType, verifier.Table("jobs", verifier.Column("id", "nvarchar(400)", nullable: false, binary: true)));

        var rejected = verifier.EvaluateColumns(expected, (("jobs", "id"), verifier.ColumnRow("nvarchar(400)", nullable: false, collation: "SQL_Latin1_General_CP1_CS_AS")));

        Assert.NotNull(rejected);
        Assert.Contains("uses the collation 'SQL_Latin1_General_CP1_CS_AS', which is not binary", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("Latin1_General_100_BIN2", rejected.Message, StringComparison.Ordinal);

        // Any _BIN / _BIN2 collation compares by code point and passes.
        Assert.Null(verifier.EvaluateColumns(expected, (("jobs", "id"), verifier.ColumnRow("nvarchar(400)", nullable: false, collation: "Latin1_General_BIN"))));
        Assert.Null(verifier.EvaluateColumns(expected, (("jobs", "id"), verifier.ColumnRow("nvarchar(400)", nullable: false, collation: "Latin1_General_100_BIN2"))));
    }

    [Theory]
    [MemberData(nameof(SqlServerVerifierAnchors))]
    public void SqlServer_EvaluateTableColumns_RequiresTheExactDefaultTheStoreDependsOn(Type anchor)
    {
        var verifier = new SqlVerifier(anchor);
        var expected = verifier.Array(
            verifier.ObjectType,
            verifier.Table("jobs", verifier.Column("created_at", "datetime2(7)", nullable: false, defaultExpression: "(sysutcdatetime())")));

        // No default at all: every insert (which never names the column) fails with 515.
        var missing = verifier.EvaluateColumns(expected, (("jobs", "created_at"), verifier.ColumnRow("datetime2(7)", nullable: false, writable: false)));
        Assert.NotNull(missing);
        Assert.Contains("has no default", missing.Message, StringComparison.Ordinal);
        Assert.Contains("error 515", missing.Message, StringComparison.Ordinal);

        // A different default: rows would carry timestamps the store's logic does not expect.
        var shifted = verifier.EvaluateColumns(expected, (("jobs", "created_at"), verifier.ColumnRow("datetime2(7)", nullable: false, writable: true, @default: "(getdate())")));
        Assert.NotNull(shifted);
        Assert.Contains("defaults to (getdate())", shifted.Message, StringComparison.Ordinal);
        Assert.Contains("depends on the default (sysutcdatetime())", shifted.Message, StringComparison.Ordinal);
        Assert.Contains("time and visibility logic", shifted.Message, StringComparison.Ordinal);

        // The catalog's rendering matches case-insensitively.
        Assert.Null(verifier.EvaluateColumns(expected, (("jobs", "created_at"), verifier.ColumnRow("datetime2(7)", nullable: false, writable: true, @default: "(SYSUTCDATETIME())"))));
    }

    [Theory]
    [MemberData(nameof(SqlServerVerifierAnchors))]
    public void SqlServer_EvaluateTableColumns_AllowsAnExtraColumnOnlyWhenInsertsCanStillSucceed(Type anchor)
    {
        var verifier = new SqlVerifier(anchor);
        var expected = verifier.Array(verifier.ObjectType, verifier.Table("jobs", verifier.Column("id", "uniqueidentifier", nullable: false)));
        var id = (("jobs", "id"), verifier.ColumnRow("uniqueidentifier", nullable: false));

        var blocking = verifier.EvaluateColumns(expected, id, (("jobs", "tenant"), verifier.ColumnRow("int", nullable: false, writable: false)));
        Assert.NotNull(blocking);
        Assert.Contains("has an extra column 'tenant' that is NOT NULL without a default", blocking.Message, StringComparison.Ordinal);

        // Nullable, or filled in by the server (default / identity / computed), or on another table: fine.
        Assert.Null(verifier.EvaluateColumns(expected, id, (("jobs", "tenant"), verifier.ColumnRow("int", nullable: true))));
        Assert.Null(verifier.EvaluateColumns(expected, id, (("jobs", "tenant"), verifier.ColumnRow("int", nullable: false, writable: true))));
        Assert.Null(verifier.EvaluateColumns(expected, id, (("other", "tenant"), verifier.ColumnRow("int", nullable: false, writable: false))));
    }

    [Theory]
    [MemberData(nameof(SqlServerVerifierAnchors))]
    public void SqlServer_MinimumWidth_RejectsAFoundTypeThatHasNoVariableWidth(Type anchor)
    {
        var verifier = new SqlVerifier(anchor);
        var column = verifier.Column("flow_id", "nvarchar(400)", nullable: false);
        verifier.ObjectColumnType.GetProperty("MinimumWidth")!.SetValue(column, true);
        var expected = verifier.Array(verifier.ObjectType, verifier.Table("flows", column));

        // A width-less type (ntext) or an unparsable width is not "the same base type, wider".
        var widthless = verifier.EvaluateColumns(expected, (("flows", "flow_id"), verifier.ColumnRow("ntext", nullable: false)));
        Assert.NotNull(widthless);
        Assert.Contains("expected nvarchar(400) NOT NULL; found ntext NOT NULL", widthless.Message, StringComparison.Ordinal);
        Assert.NotNull(verifier.EvaluateColumns(expected, (("flows", "flow_id"), verifier.ColumnRow("nvarchar(-)", nullable: false))));
        Assert.Null(verifier.EvaluateColumns(expected, (("flows", "flow_id"), verifier.ColumnRow("nvarchar(4000)", nullable: false))));
    }

    [Theory]
    [MemberData(nameof(SqlServerVerifierAnchors))]
    public void SqlServer_NameParameters_BindsEveryNameAsItsOwnParameter(Type anchor)
    {
        var verifier = new SqlVerifier(anchor);
        using var command = new SqlCommand();

        var list = verifier.NameParameters(command, ["jobs", "jobs_dlq", "Jobs'); DROP TABLE x;--"]);

        Assert.Equal("@name0, @name1, @name2", list);
        Assert.Equal(["@name0", "@name1", "@name2"], command.Parameters.Cast<SqlParameter>().Select(p => p.ParameterName));
        Assert.Equal("Jobs'); DROP TABLE x;--", command.Parameters["@name2"].Value);

        // No names: an IN list that matches nothing, rather than a syntax error.
        using var empty = new SqlCommand();
        Assert.Equal("NULL", verifier.NameParameters(empty, []));
        Assert.Empty(empty.Parameters);
    }

    [Theory]
    [MemberData(nameof(SqlServerVerifierAnchors))]
    public async Task SqlServer_CatalogChecks_WithNothingOfTheirKind_IssueNoQuery(Type anchor)
    {
        var verifier = new SqlVerifier(anchor);

        // Only a sequence expected: no table columns, primary keys or indexes to verify. A null
        // connection proves each check returns before creating a command.
        var sequenceOnly = verifier.Array(verifier.ObjectType, verifier.Sequence("ack_seq"));
        await verifier.InvokeCheckAsync("VerifyTableColumnsAsync", sequenceOnly);
        await verifier.InvokeCheckAsync("VerifyPrimaryKeysAsync", sequenceOnly);
        await verifier.InvokeCheckAsync("VerifyIndexesAsync", sequenceOnly, reportAbsence: true);

        // A table WITHOUT a column list is not column-verified either.
        var bareTable = verifier.Array(verifier.ObjectType, verifier.Table("jobs"));
        await verifier.InvokeCheckAsync("VerifyTableColumnsAsync", bareTable);
    }

    /// <summary>
    /// The failed-batch diagnosis rolls the failed transaction back first, best effort — a
    /// rollback that throws (a doomed or zombied transaction, a dead connection) must not replace
    /// the diagnosis — and a finding of the verifier's own replaces the original DDL error while
    /// keeping it as the inner exception. A never-initialized <see cref="SqlTransaction"/> stands in
    /// for the dead transaction: its rollback throws, which is exactly the case under test.
    /// </summary>
    [Theory]
    [MemberData(nameof(SqlServerVerifierAnchors))]
    public async Task SqlServer_Diagnosis_SurvivesAFailingRollback_AndReplacesTheErrorOnlyWithItsOwnFinding(Type anchor)
    {
        var verifier = new SqlVerifier(anchor);
        var failure = RelationalSharedHelperTests.SqlExceptionWith(2714);
        var deadTransaction = (SqlTransaction)RuntimeHelpers.GetUninitializedObject(typeof(SqlTransaction));
        var expected = verifier.Array(verifier.ObjectType, verifier.Table("jobs"));
        const string finding = "The SQL Server channel store expected 'dbo.jobs' to be a user table, but the name is occupied by a view.";

        var diagnosed = await Assert.ThrowsAsync<InvalidOperationException>(
            () => verifier.DiagnoseAsync(_ => throw new InvalidOperationException(finding), failure, deadTransaction, expected));
        Assert.Equal(finding, diagnosed.Message);
        Assert.Same(failure, diagnosed.InnerException);

        // Another component's wording is not this verifier's finding: the original error stands.
        await verifier.DiagnoseAsync(
            _ => throw new InvalidOperationException("The SQL Server transport store expected something else."),
            failure,
            deadTransaction,
            expected);
    }

    // ---------------------------------------------------------------------------------------
    // PostgreSqlRelationVerifier
    // ---------------------------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(PostgreSqlAnchors))]
    public void PostgreSql_Evaluate_RejectsAnUnloggedOrTemporaryRelation(Type anchor)
    {
        var verifier = new PgVerifier(anchor);
        var expected = verifier.Array(verifier.RelationType, verifier.Relation("jobs", 'r'));

        foreach (var persistence in new[] { "u", "t" })
        {
            var rejected = verifier.Evaluate(expected, [("jobs", verifier.Row("r", persistence: persistence))]);
            Assert.NotNull(rejected);
            Assert.Contains("is not a permanent relation (UNLOGGED or temporary)", rejected.Message, StringComparison.Ordinal);
        }

        Assert.Null(verifier.Evaluate(expected, [("jobs", verifier.Row("r"))]));
    }

    [Theory]
    [MemberData(nameof(PostgreSqlAnchors))]
    public void PostgreSql_Evaluate_RequiresAMonotonicBigintSequence(Type anchor)
    {
        var verifier = new PgVerifier(anchor);
        var expected = verifier.Array(verifier.RelationType, verifier.Relation("ack_seq", 'S'));

        Assert.Null(verifier.Evaluate(expected, [("ack_seq", verifier.SequenceRow())]));

        // Each property that lets drawn values go backwards, repeat, or run out is rejected alone.
        var cases = new (object Row, string Found)[]
        {
            (verifier.SequenceRow(type: "integer"), "found integer, INCREMENT 1, CACHE 1, NO CYCLE"),
            (verifier.SequenceRow(increment: -1), "INCREMENT -1,"),
            (verifier.SequenceRow(cache: 20), "CACHE 20,"),
            (verifier.SequenceRow(cycles: true), "CACHE 1, CYCLE,"),
            (verifier.SequenceRow(max: 1_000_000), "MAXVALUE 1000000."),
        };
        foreach (var (row, found) in cases)
        {
            var rejected = verifier.Evaluate(expected, [("ack_seq", row)]);
            Assert.NotNull(rejected);
            Assert.Contains("does not behave as the required cross-process monotonic clock", rejected.Message, StringComparison.Ordinal);
            Assert.Contains(found, rejected.Message, StringComparison.Ordinal);
            Assert.Contains("ALTER SEQUENCE \"catalog_test\".\"ack_seq\" AS bigint INCREMENT 1 CACHE 1 NO CYCLE NO MAXVALUE", rejected.Message, StringComparison.Ordinal);
        }
    }

    [Theory]
    [MemberData(nameof(PostgreSqlAnchors))]
    public void PostgreSql_Evaluate_ReportsAMissingColumnAndAWrongShape(Type anchor)
    {
        var verifier = new PgVerifier(anchor);
        var expected = verifier.Array(
            verifier.RelationType,
            verifier.Relation("jobs", 'r', columns: verifier.Array(
                verifier.ColumnType,
                verifier.Column("id", "uuid", nullable: false),
                verifier.Column("created_at", "timestamp with time zone", nullable: false, defaultExpression: "now()"))));
        var healthyId = (("jobs", "id"), verifier.ColumnRow("uuid", notNull: true));
        var healthyCreated = (("jobs", "created_at"), verifier.ColumnRow("timestamp with time zone", notNull: true, @default: "now()", writable: true));
        (string, object)[] table = [("jobs", verifier.Row("r"))];

        Assert.Null(verifier.Evaluate(expected, table, healthyId, healthyCreated));

        var missing = verifier.Evaluate(expected, table, healthyCreated);
        Assert.NotNull(missing);
        Assert.Contains("is missing the column 'id' (uuid)", missing.Message, StringComparison.Ordinal);

        var wrongType = verifier.Evaluate(expected, table, (("jobs", "id"), verifier.ColumnRow("text", notNull: false)), healthyCreated);
        Assert.NotNull(wrongType);
        Assert.Contains("expected uuid NOT NULL; found text NULL without a default", wrongType.Message, StringComparison.Ordinal);

        var wrongDefault = verifier.Evaluate(
            expected,
            table,
            healthyId,
            (("jobs", "created_at"), verifier.ColumnRow("timestamp with time zone", notNull: true, @default: "(now() + '1 year'::interval)", writable: true)));
        Assert.NotNull(wrongDefault);
        Assert.Contains("expected timestamp with time zone NOT NULL DEFAULT now()", wrongDefault.Message, StringComparison.Ordinal);
        Assert.Contains("NOT NULL DEFAULT (now() + '1 year'::interval)", wrongDefault.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(PostgreSqlAnchors))]
    public void PostgreSql_Evaluate_NamesTheKindThatOccupiesTheName(Type anchor)
    {
        var verifier = new PgVerifier(anchor);
        var table = verifier.Array(verifier.RelationType, verifier.Relation("jobs", 'r'));

        foreach (var (kind, description) in new[] { ("v", "a view"), ("m", "a materialized view"), ("f", "a relation of kind 'f'"), ("i", "an index") })
        {
            var rejected = verifier.Evaluate(table, [("jobs", verifier.Row(kind))]);
            Assert.NotNull(rejected);
            Assert.Contains($"to be a table, but the name is occupied by {description},", rejected.Message, StringComparison.Ordinal);
        }

        // An index expectation without an owning table describes itself by kind alone.
        var index = verifier.Array(verifier.RelationType, verifier.Relation("jobs_idx", 'i'));
        var occupied = verifier.Evaluate(index, [("jobs_idx", verifier.Row("r"))]);
        Assert.NotNull(occupied);
        Assert.Contains("to be an index, but the name is occupied by a table,", occupied.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(PostgreSqlAnchors))]
    public async Task PostgreSql_LoadTableColumns_WithNoColumnVerifiedTable_IssuesNoQuery(Type anchor)
    {
        var verifier = new PgVerifier(anchor);
        var method = verifier.Type.GetMethod("LoadTableColumnsAsync", BindingFlags.NonPublic | BindingFlags.Static)!;
        var expected = verifier.Array(verifier.RelationType, verifier.Relation("ack_seq", 'S'), verifier.Relation("jobs", 'r'));

        // A null connection proves no command is created when nothing needs column verification.
        var task = (Task)method.Invoke(null, [null, null, "catalog_test", expected, CancellationToken.None])!;
        await task;

        var columns = (IDictionary)task.GetType().GetProperty("Result")!.GetValue(task)!;
        Assert.Empty(columns);
    }

    [Theory]
    [MemberData(nameof(PostgreSqlAnchors))]
    public void PostgreSql_DdlCollisionMessage_NamesTheComponentAndTheSchema(Type anchor)
    {
        var verifier = new PgVerifier(anchor);

        var message = (string)verifier.Type.GetMethod("DdlCollisionMessage", BindingFlags.Public | BindingFlags.Static)!
            .Invoke(null, ["durable-flow", "tenant_a"])!;

        Assert.StartsWith("The PostgreSQL durable-flow store could not create its schema objects in 'tenant_a'", message, StringComparison.Ordinal);
        Assert.Contains("share one namespace per schema", message, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------------------------------
    // PostgreSqlDdlGuard
    // ---------------------------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(PostgreSqlAnchors))]
    public void PostgreSqlDdlGuard_BackOff_LogsTheCauseOnceWithItsWindow(Type anchor)
    {
        var logger = new CapturingLogger();
        var guardType = anchor.Assembly.GetType("AsyncResponse.Internal.PostgreSqlDdlGuard", throwOnError: true)!;
        var guard = Activator.CreateInstance(guardType, "transport", "'s.jobs'", "docs/postgresql.md", logger)!;
        var backOff = guardType.GetMethod("BackOff", BindingFlags.Public | BindingFlags.Instance)!;

        var lockTimeout = new PostgresException("canceling statement due to lock timeout", "ERROR", "ERROR", PostgresErrorCodes.LockNotAvailable);
        var lockWindow = (TimeSpan)backOff.Invoke(guard, [lockTimeout])!;
        var locked = Assert.Single(logger.Entries);
        Assert.Equal(LogLevel.Warning, locked.Level);
        Assert.Same(lockTimeout, locked.Exception);
        Assert.Contains("could not take a lock within 5s", locked.Message, StringComparison.Ordinal);
        Assert.Contains("'s.jobs'", locked.Message, StringComparison.Ordinal);
        Assert.Contains(lockWindow.ToString(), locked.Message, StringComparison.Ordinal);

        // The same failure latched again keeps its window and logs nothing new.
        Assert.Equal(lockWindow, (TimeSpan)backOff.Invoke(guard, [lockTimeout])!);
        Assert.Single(logger.Entries);

        var diskFull = new PostgresException("could not extend file", "ERROR", "ERROR", PostgresErrorCodes.DiskFull);
        var diskWindow = (TimeSpan)backOff.Invoke(guard, [diskFull])!;
        Assert.Equal(2, logger.Entries.Count);
        var failed = logger.Entries[1];
        Assert.Same(diskFull, failed.Exception);
        Assert.Contains("failed and was rolled back", failed.Message, StringComparison.Ordinal);
        Assert.Contains(diskWindow.ToString(), failed.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(PostgreSqlAnchors))]
    public void PostgreSqlDdlGuard_LockKeys_AreStablePerNameAndDistinctBetweenScopes(Type anchor)
    {
        var guardType = anchor.Assembly.GetType("AsyncResponse.Internal.PostgreSqlDdlGuard", throwOnError: true)!;
        long SchemaKey(string schema) => (long)guardType.GetMethod("SchemaLockKey")!.Invoke(null, [schema])!;
        long TableKey(string schema, string table) => (long)guardType.GetMethod("TableLockKey")!.Invoke(null, [schema, table])!;

        // Every package computes the identical key (FNV-1a), or two stores would not serialize.
        var reference = typeof(PostgreSqlDurableFlowOptions).Assembly.GetType("AsyncResponse.Internal.PostgreSqlDdlGuard", throwOnError: true)!;
        Assert.Equal((long)reference.GetMethod("SchemaLockKey")!.Invoke(null, ["public"])!, SchemaKey("public"));

        Assert.Equal(SchemaKey("public"), SchemaKey("public"));
        Assert.NotEqual(SchemaKey("public"), SchemaKey("tenant_a"));
        Assert.NotEqual(SchemaKey("public"), TableKey("public", "jobs"));
    }

    /// <summary>
    /// A DDL transaction whose lock bound fails is disposed and the server's error rethrown
    /// unchanged — the caller (the store's lock-timeout arm) classifies it by SQLSTATE — and the
    /// advisory DDL key is never requested, so a host that could not bound its wait never queues
    /// for the key unbounded. (The fake server cannot answer the lazily prepended BEGIN of a
    /// transaction whose first statement succeeds, so only the first statement's failure is driven.)
    /// </summary>
    [Theory]
    [MemberData(nameof(PostgreSqlAnchors))]
    public async Task PostgreSqlDdlGuard_BeginLockedTransaction_ALockBoundFailure_RethrowsWithoutTakingTheKey(Type anchor)
    {
        var guardType = anchor.Assembly.GetType("AsyncResponse.Internal.PostgreSqlDdlGuard", throwOnError: true)!;
        var begin = guardType.GetMethod("BeginLockedTransactionAsync", BindingFlags.Public | BindingFlags.Static)!;
        await using var server = new FakePostgresWireServer
        {
            Respond = (_, sql) => Task.FromResult(sql.StartsWith("SET LOCAL lock_timeout", StringComparison.Ordinal)
                ? FakePostgresWireServer.Reply.Error("permission denied to set parameter")
                : FakePostgresWireServer.Reply.Complete(sql))
        };
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        await using var connection = await dataSource.OpenConnectionAsync();

        var thrown = await Assert.ThrowsAsync<PostgresException>(
            () => (Task)begin.Invoke(null, [connection, 42L, CancellationToken.None])!);

        Assert.Equal("permission denied to set parameter", thrown.MessageText);
        var statement = Assert.Single(server.Statements);
        Assert.StartsWith("SET LOCAL lock_timeout = '5s'", statement.Sql, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------------------------------
    // RelationalNamePlan / SqlServerTransientFaults
    // ---------------------------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(NamePlanAnchors))]
    public void NamePlan_RequireDistinct_ReportsBothSpellingsOfACaseFoldedCollision(Type anchor)
    {
        var plan = anchor.Assembly.GetType("AsyncResponse.Internal.RelationalNamePlan", throwOnError: true)!;
        var requireDistinct = plan.GetMethod("RequireDistinct", BindingFlags.Public | BindingFlags.Static)!;

        requireDistinct.Invoke(null, [new[] { ("table", "jobs"), ("dead-letter table", "jobs_dlq"), ("index", "jobs_idx") }, "TestOptions", "."]);

        var thrown = Assert.IsType<InvalidOperationException>(Assert.Throws<TargetInvocationException>(
            () => requireDistinct.Invoke(null, [new[] { ("table", "Jobs"), ("index", "jobs_idx"), ("dead-letter table", "jobs") }, "TestOptions", " Rename one."])).InnerException);
        Assert.Equal(
            "TestOptions: the table ('Jobs') and the dead-letter table ('jobs') resolve to the same name — object names are compared case-insensitively Rename one.",
            thrown.Message);
    }

    [Theory]
    [MemberData(nameof(NamePlanAnchors))]
    public void NamePlan_DerivedName_RefusesASuffixThatLeavesNoStem(Type anchor)
    {
        var plan = anchor.Assembly.GetType("AsyncResponse.Internal.RelationalNamePlan", throwOnError: true)!;
        var derivedName = plan.GetMethod("DerivedName", BindingFlags.Public | BindingFlags.Static)!;

        var thrown = Assert.IsType<ArgumentOutOfRangeException>(Assert.Throws<TargetInvocationException>(
            () => derivedName.Invoke(null, [new string('t', 70), new string('x', 63), 63])).InnerException);
        Assert.Equal("suffix", thrown.ParamName);
        Assert.Contains("is 63 characters, which leaves no room for a stem inside the 63-character identifier limit", thrown.Message, StringComparison.Ordinal);

        // Room for one stem character still derives a name at the cap.
        Assert.Equal("t" + new string('x', 62), derivedName.Invoke(null, [new string('t', 70), new string('x', 62), 63]));
        Assert.Equal("jobs_idx", derivedName.Invoke(null, ["jobs", "_idx", 0]));
    }

    [Theory]
    [MemberData(nameof(SqlServerTransientAnchors))]
    public void SqlServerTransientFaults_AnUnparsableConnectionString_IsNoPoolTimeout(Type anchor)
    {
        var faults = anchor.Assembly.GetType("AsyncResponse.Internal.SqlServerTransientFaults", throwOnError: true)!;
        var isPoolTimeout = faults.GetMethod("IsPoolTimeout", BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Static)!;
        var poolTimeout = new InvalidOperationException("Timeout expired.");

        // The builder rejects an unknown keyword; the classifier must answer "no", not throw from
        // inside the open helper's exception filter.
        Assert.False((bool)isPoolTimeout.Invoke(null, [poolTimeout, "Server=unused;Not A Keyword=1", TimeSpan.FromMinutes(5)])!);
        Assert.True((bool)isPoolTimeout.Invoke(null, [poolTimeout, "Server=unused;Connect Timeout=1", TimeSpan.FromMinutes(5)])!);
    }

    // ---------------------------------------------------------------------------------------
    // Reflection facades (the internal types share one full name across assemblies)
    // ---------------------------------------------------------------------------------------

    private sealed class SqlVerifier
    {
        private readonly Type _type;
        private readonly Type _actualColumn;
        private readonly Type _actualIndex;
        private readonly Type _objectKind;
        private readonly object _comparer;

        public SqlVerifier(Type anchor)
        {
            _type = anchor.Assembly.GetType("AsyncResponse.Internal.SqlServerRelationVerifier", throwOnError: true)!;
            ObjectType = _type.GetNestedType("ExpectedObject", BindingFlags.NonPublic)!;
            ObjectColumnType = _type.GetNestedType("ExpectedColumn", BindingFlags.NonPublic)!;
            _actualColumn = _type.GetNestedType("ActualColumn", BindingFlags.NonPublic)!;
            _actualIndex = _type.GetNestedType("ActualIndex", BindingFlags.NonPublic)!;
            _objectKind = anchor.Assembly.GetType("AsyncResponse.Internal.SqlServerObjectKind", throwOnError: true)!;
            _comparer = _type.GetNestedType("TableColumnComparer", BindingFlags.NonPublic)!
                .GetField("Instance", BindingFlags.Public | BindingFlags.Static)!
                .GetValue(null)!;
        }

        public Type ObjectType { get; }

        public Type ObjectColumnType { get; }

        public object Column(string name, string? type, bool nullable, bool binary = false, string? defaultExpression = null)
            => Activator.CreateInstance(ObjectColumnType, name, type, nullable, binary, defaultExpression)!;

        public object Table(string name, params object[] columns)
            => Activator.CreateInstance(ObjectType, name, Kind(0), columns.Length == 0 ? null : Array(ObjectColumnType, columns), null, null, null)!;

        public object Sequence(string name)
            => Activator.CreateInstance(ObjectType, name, Kind(1), null, null, null, null)!;

        public object Index(string name, string owningTable, params string[] keyColumns)
            => Activator.CreateInstance(ObjectType, name, Kind(2), null, null, owningTable, keyColumns)!;

        public object ColumnRow(string type, bool nullable, string collation = "", bool writable = false, string @default = "")
            => Activator.CreateInstance(_actualColumn, type, nullable, collation, writable, @default)!;

        public object IndexRow(byte type, params string[] keyColumns)
        {
            var keys = new List<(byte Ordinal, string Column)>();
            for (var i = 0; i < keyColumns.Length; i++)
                keys.Add(((byte)(i + 1), keyColumns[i]));
            return Activator.CreateInstance(_actualIndex, type, false, false, false, keys)!;
        }

        public string Describe(string type)
            => (string)Method("Describe").Invoke(null, [type])!;

        public string NameParameters(SqlCommand command, IEnumerable<string> names)
            => (string)Method("NameParameters").Invoke(null, [command, names])!;

        public InvalidOperationException? EvaluateColumns(Array tables, params ((string, string) Key, object Row)[] rows)
        {
            var actual = Dictionary(_actualColumn);
            foreach (var (key, row) in rows)
                actual.Add(key, row);
            return Capture(() => Method("EvaluateTableColumns").Invoke(null, ["catalog_test", "channel", tables, actual]));
        }

        public InvalidOperationException? EvaluateIndexes(Array indexes, (string, string) key, object row)
        {
            var actual = Dictionary(_actualIndex);
            actual.Add(key, row);
            return Capture(() => Method("EvaluateIndexes").Invoke(null, ["catalog_test", "channel", indexes, actual, true]));
        }

        public Task InvokeCheckAsync(string name, Array expected, bool? reportAbsence = null)
            => (Task)Method(name).Invoke(
                null,
                reportAbsence is { } report
                    ? [null, null, "catalog_test", "channel", expected, report, CancellationToken.None]
                    : [null, null, "catalog_test", "channel", expected, CancellationToken.None])!;

        public Task DiagnoseAsync(Func<CancellationToken, Task<SqlConnection>> open, SqlException failure, SqlTransaction? transaction, Array expected)
            => (Task)Method("ThrowDiagnosedCollisionAsync").Invoke(null, [open, failure, transaction, "catalog_test", "channel", expected, CancellationToken.None])!;

        public Array Array(Type element, params object[] items)
        {
            var array = System.Array.CreateInstance(element, items.Length);
            for (var i = 0; i < items.Length; i++)
                array.SetValue(items[i], i);
            return array;
        }

        private object Kind(int value) => Enum.ToObject(_objectKind, value);

        private IDictionary Dictionary(Type value)
            => (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(typeof((string, string)), value), _comparer)!;

        private MethodInfo Method(string name)
            => _type.GetMethod(name, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)!;
    }

    private sealed class PgVerifier
    {
        private readonly Type _actualRelation;
        private readonly Type _actualColumn;

        public PgVerifier(Type anchor)
        {
            Type = anchor.Assembly.GetType("AsyncResponse.Internal.PostgreSqlRelationVerifier", throwOnError: true)!;
            RelationType = Type.GetNestedType("ExpectedRelation", BindingFlags.NonPublic)!;
            ColumnType = Type.GetNestedType("ExpectedColumn", BindingFlags.NonPublic)!;
            _actualRelation = Type.GetNestedType("ActualRelation", BindingFlags.NonPublic)!;
            _actualColumn = Type.GetNestedType("ActualColumn", BindingFlags.NonPublic)!;
        }

        public Type Type { get; }

        public Type RelationType { get; }

        public Type ColumnType { get; }

        public object Relation(string name, char kind, Array? columns = null)
            => Activator.CreateInstance(RelationType, name, kind, null, null, columns, null)!;

        public object Column(string name, string type, bool nullable, string? defaultExpression = null)
            => Activator.CreateInstance(ColumnType, name, type, nullable, false, defaultExpression)!;

        public object Row(string kind, string persistence = "p")
            => Activator.CreateInstance(
                _actualRelation,
                kind, persistence, "", "", false, false, true,
                System.Array.Empty<string>(), "", 1L, 1L, false, long.MaxValue,
                System.Array.Empty<string>())!;

        public object SequenceRow(string type = "bigint", long increment = 1, long cache = 1, bool cycles = false, long max = long.MaxValue)
            => Activator.CreateInstance(
                _actualRelation,
                "S", "p", "", "", false, false, true,
                System.Array.Empty<string>(), type, increment, cache, cycles, max,
                System.Array.Empty<string>())!;

        public object ColumnRow(string type, bool notNull, string @default = "", bool writable = false)
            => Activator.CreateInstance(_actualColumn, type, notNull, @default, writable, "", true)!;

        public Array Array(Type element, params object[] items)
        {
            var array = System.Array.CreateInstance(element, items.Length);
            for (var i = 0; i < items.Length; i++)
                array.SetValue(items[i], i);
            return array;
        }

        public InvalidOperationException? Evaluate(Array expected, (string Name, object Row)[] relations, params ((string, string) Key, object Row)[] columns)
        {
            var relationRows = (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(typeof(string), _actualRelation))!;
            foreach (var (name, row) in relations)
                relationRows.Add(name, row);
            var columnRows = (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(typeof((string, string)), _actualColumn))!;
            foreach (var (key, row) in columns)
                columnRows.Add(key, row);

            return Capture(() => Type.GetMethod("Evaluate", BindingFlags.NonPublic | BindingFlags.Static)!
                .Invoke(null, ["catalog_test", "channel", expected, relationRows, columnRows]));
        }
    }

    private static InvalidOperationException? Capture(Action invoke)
    {
        try
        {
            invoke();
            return null;
        }
        catch (TargetInvocationException wrapped)
        {
            return Assert.IsType<InvalidOperationException>(wrapped.InnerException);
        }
    }

    private sealed class CapturingLogger : ILogger
    {
        private readonly List<(LogLevel Level, Exception? Exception, string Message)> _entries = [];

        public IReadOnlyList<(LogLevel Level, Exception? Exception, string Message)> Entries
        {
            get
            {
                lock (_entries)
                    return [.. _entries];
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_entries)
                _entries.Add((logLevel, exception, formatter(state, exception)));
        }
    }
}
