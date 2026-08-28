using System.Collections;
using System.Reflection;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.DurableFlows.SqlServer;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The SQL Server catalog-verification decision table, unit-tested against the verifier's internal
/// catalog-row model so the accept/reject logic runs without a server (the integration suite proves
/// the same decisions against a real catalog). <c>src/Shared/SqlServerRelationVerifier.cs</c> is
/// source-linked into the channel, transport, and durable-flow packages, so every fact runs against
/// all three compilations by reflection, like <see cref="PostgreSqlRelationVerifierTests"/>.
/// </summary>
public sealed class SqlServerRelationVerifierTests
{
    private const string ConnectionString = "Server=(local);Database=asyncresponse;Integrated Security=true;TrustServerCertificate=true";

    public static TheoryData<Type> AnchorTypes =>
    [
        typeof(SqlServerAsyncResponseChannelOptions),
        typeof(SqlServerAsyncResponseTransportOptions),
        typeof(SqlServerDurableFlowOptions)
    ];

    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void RenderType_StatesFractionalSecondsScale_AndLeavesStringWidthsAlone(Type anchor)
    {
        var verifier = new Verifier(anchor);

        // The rendering IS the comparison, so a scale-less rendering made datetime2(0) — which
        // ROUNDS every stored timestamp to the second — indistinguishable from full precision.
        Assert.Equal("datetime2(0)", verifier.RenderType("datetime2", 6, 0));
        Assert.Equal("datetime2(3)", verifier.RenderType("datetime2", 7, 3));
        Assert.Equal("datetime2(7)", verifier.RenderType("datetime2", 8, 7));
        Assert.Equal("datetimeoffset(0)", verifier.RenderType("datetimeoffset", 8, 0));
        Assert.Equal("time(2)", verifier.RenderType("time", 4, 2));

        // Widths are still byte counts (halved for the Unicode types, -1 = the (max) sentinel),
        // and a type with neither width nor scale still renders bare.
        Assert.Equal("nvarchar(400)", verifier.RenderType("nvarchar", 800, 0));
        Assert.Equal("nvarchar(max)", verifier.RenderType("nvarchar", -1, 0));
        Assert.Equal("nchar(64)", verifier.RenderType("nchar", 128, 0));
        Assert.Equal("varchar(200)", verifier.RenderType("varchar", 200, 0));
        Assert.Equal("varbinary(max)", verifier.RenderType("varbinary", -1, 0));
        Assert.Equal("uniqueidentifier", verifier.RenderType("uniqueidentifier", 16, 0));
        Assert.Equal("bigint", verifier.RenderType("bigint", 8, 0));
    }

    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void EvaluateTableColumns_RejectsAReducedScaleTimestamp_NamingBothRenderings(Type anchor)
    {
        var verifier = new Verifier(anchor);
        var expected = verifier.Tables(verifier.Table("jobs", verifier.Column("available_at", "datetime2(7)", nullable: false)));

        var rejected = verifier.Evaluate(expected, [(("jobs", "available_at"), verifier.ColumnRow("datetime2(0)", nullable: false))]);

        Assert.NotNull(rejected);
        Assert.Contains("expected datetime2(7) NOT NULL", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("found datetime2(0) NOT NULL", rejected.Message, StringComparison.Ordinal);

        // Full scale is the shape the DDL creates and must still pass.
        Assert.Null(verifier.Evaluate(expected, [(("jobs", "available_at"), verifier.ColumnRow("datetime2(7)", nullable: false))]));
    }

    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void EvaluateTableColumns_KeepsMatchingCatalogNamesCaseInsensitively(Type anchor)
    {
        var verifier = new Verifier(anchor);
        var expected = verifier.Tables(verifier.Table("jobs", verifier.Column("available_at", "datetime2(7)", nullable: false)));

        // The catalog IN-list matched case-insensitively, so a case-variant row must reach the
        // shape checks instead of being misreported as a missing column (and the extra-column
        // sweep must not read the same row as foreign either).
        Assert.Null(verifier.Evaluate(expected, [(("JOBS", "AVAILABLE_AT"), verifier.ColumnRow("DATETIME2(7)", nullable: false))]));
    }

    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void EvaluateIndexes_RejectsWrongKeyColumns_AndAcceptsTheDeclaredShape(Type anchor)
    {
        // Regression: the DDL's index guard is name-only (sys.indexes WHERE name = ... AND
        // object_id = ...), so an operator-provisioned index keyed on other columns was silently
        // accepted and every poll tick table-scanned; ExpectedObjects could not even DECLARE an
        // index. The verifier now checks the shape the way the PostgreSQL sibling does.
        var verifier = new Verifier(anchor);
        var expected = verifier.Tables(verifier.Index("jobs_correlation_idx", "jobs", "correlation_id", "created_at"));

        var rejected = verifier.EvaluateIndexRows(expected, [(("jobs_correlation_idx", "jobs"), verifier.IndexRow(keyColumns: ["expires_at"]))]);
        Assert.NotNull(rejected);
        Assert.Contains("correlation_id, created_at", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("expires_at", rejected.Message, StringComparison.Ordinal);

        // The declared shape passes, as does a clustered variant (both serve the seek), and
        // included columns are extra capacity rather than a shape change (key_ordinal 0 rows
        // never reach the loaded key list).
        Assert.Null(verifier.EvaluateIndexRows(expected, [(("jobs_correlation_idx", "jobs"), verifier.IndexRow(keyColumns: ["correlation_id", "created_at"]))]));
        Assert.Null(verifier.EvaluateIndexRows(expected, [(("jobs_correlation_idx", "jobs"), verifier.IndexRow(type: 1, keyColumns: ["correlation_id", "created_at"]))]));

        // UNIQUE (would reject duplicate values), filtered (misses rows), disabled (unusable) and
        // columnstore (no seek) are all the wrong index despite the right columns.
        Assert.NotNull(verifier.EvaluateIndexRows(expected, [(("jobs_correlation_idx", "jobs"), verifier.IndexRow(unique: true, keyColumns: ["correlation_id", "created_at"]))]));
        Assert.NotNull(verifier.EvaluateIndexRows(expected, [(("jobs_correlation_idx", "jobs"), verifier.IndexRow(filtered: true, keyColumns: ["correlation_id", "created_at"]))]));
        Assert.NotNull(verifier.EvaluateIndexRows(expected, [(("jobs_correlation_idx", "jobs"), verifier.IndexRow(disabled: true, keyColumns: ["correlation_id", "created_at"]))]));
        Assert.NotNull(verifier.EvaluateIndexRows(expected, [(("jobs_correlation_idx", "jobs"), verifier.IndexRow(type: 5, keyColumns: ["correlation_id", "created_at"]))]));
    }

    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void EvaluateIndexes_TreatsASameNameIndexOnAnotherTableAsAbsent(Type anchor)
    {
        // SQL Server index names are per-table, not schema-wide: a same-name index on an
        // unrelated table is neither the expected index nor a collision — the expected one is
        // simply missing (reported only when absence is reportable; diagnosis mode stays quiet).
        var verifier = new Verifier(anchor);
        var expected = verifier.Tables(verifier.Index("jobs_expires_idx", "jobs", "expires_at"));
        var foreign = (("jobs_expires_idx", "other_table"), verifier.IndexRow(keyColumns: ["whatever"]));

        var missing = verifier.EvaluateIndexRows(expected, [foreign]);
        Assert.NotNull(missing);
        Assert.Contains("to exist", missing.Message, StringComparison.Ordinal);

        Assert.Null(verifier.EvaluateIndexRows(expected, [foreign], reportAbsence: false));
    }

    [Fact]
    public void EveryExpectedFractionalSecondsColumn_StatesTheScaleItsDdlCreates()
    {
        // A bare `datetime2` declaration is datetime2(7). Expecting the bare spelling matched ANY
        // scale, so an operator-provisioned or restored table with datetime2(0) passed verification
        // and then rounded every timestamp the watermark, visibility, and lease logic compare.
        var offenders = new List<string>();
        foreach (var (label, store) in Stores())
        {
            foreach (var (table, column, type) in ExpectedColumns(store))
            {
                if (type is "datetime2" or "datetimeoffset" or "time")
                    offenders.Add($"{label}: {table}.{column} expects a scale-less '{type}'");
                else if (type?.StartsWith("datetime2", StringComparison.Ordinal) == true && type != "datetime2(7)")
                    offenders.Add($"{label}: {table}.{column} expects '{type}', but its DDL declares a bare datetime2 (scale 7)");
            }
        }

        Assert.Empty(offenders);
    }

    [Fact]
    public void TransportQueueColumn_IsUnconstrained_OnlyOnTheOperatorProvisionedPath()
    {
        // ExactQueueMatch forces Latin1_General_100_BIN2 in the query and defeats trailing-space
        // padding with its sentinel concat, so an operator's queue column claims exactly whatever
        // its type and collation are — legacy nvarchar CI and varchar tables included. Constraining
        // them there rejected schemas this transport reads correctly. On the path where this build
        // ran the DDL itself, any drift means somebody ALTERed the column, so both stay pinned.
        var transport = new SqlServerTransportStore(Options.Create(new SqlServerAsyncResponseTransportOptions { ConnectionString = ConnectionString }));

        Assert.Equal("nvarchar(200)", QueueColumn(transport, selfCreated: true).Type);
        Assert.True(QueueColumn(transport, selfCreated: true).RequiresBinaryCollation);
        Assert.Null(QueueColumn(transport, selfCreated: false).Type);
        Assert.False(QueueColumn(transport, selfCreated: false).RequiresBinaryCollation);

        // Unconstrained is not unchecked: the column must still exist and be NOT NULL, and every
        // OTHER column — a wrong payload_json or timestamp shape breaks inserts or reorders the
        // timestamps this store compares — keeps the identical expectation on both paths.
        Assert.False(QueueColumn(transport, selfCreated: false).Nullable);
        Assert.Equal(
            ExpectedColumns(transport, selfCreated: true).Where(c => c.Column != "queue"),
            ExpectedColumns(transport, selfCreated: false).Where(c => c.Column != "queue"));

        static (string? Type, bool Nullable, bool RequiresBinaryCollation) QueueColumn(object store, bool selfCreated)
        {
            var method = store.GetType().GetMethod("ExpectedObjects", BindingFlags.Instance | BindingFlags.NonPublic)!;
            // The self-created path also declares the derived indexes (Columns: null); the queue
            // column lives on the one object that carries columns.
            var columns = (IEnumerable)((IEnumerable)method.Invoke(store, [selfCreated])!)
                .Cast<object>()
                .Select(expectedObject => Read(expectedObject, "Columns"))
                .Single(objectColumns => objectColumns is not null)!;
            var queue = columns.Cast<object>().Single(c => (string)Read(c, "Name")! == "queue");
            return ((string?)Read(queue, "Type"), (bool)Read(queue, "Nullable")!, (bool)Read(queue, "RequiresBinaryCollation")!);
        }
    }

    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void EvaluateTableColumns_UnconstrainedType_AcceptsAnyType_ButStillEnforcesNullability(Type anchor)
    {
        var verifier = new Verifier(anchor);
        var expected = verifier.Tables(verifier.Table("jobs", verifier.Column("queue", type: null, nullable: false)));

        // Any string type claims correctly when the query supplies the collation itself.
        Assert.Null(verifier.Evaluate(expected, [(("jobs", "queue"), verifier.ColumnRow("varchar(200)", nullable: false, collation: "SQL_Latin1_General_CP1_CI_AS"))]));
        Assert.Null(verifier.Evaluate(expected, [(("jobs", "queue"), verifier.ColumnRow("nvarchar(400)", nullable: false, collation: "Latin1_General_100_BIN2"))]));

        // A nullable column is a different contract, and the report names only what is compared.
        var rejected = verifier.Evaluate(expected, [(("jobs", "queue"), verifier.ColumnRow("varchar(200)", nullable: true))]);
        Assert.NotNull(rejected);
        Assert.Contains("expected NOT NULL; found NULL", rejected.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("varchar(200)", rejected.Message, StringComparison.Ordinal);

        // An absent column is still an absent column.
        var missing = verifier.Evaluate(expected, []);
        Assert.NotNull(missing);
        Assert.Contains("missing the column 'queue';", missing.Message, StringComparison.Ordinal);
    }

    private static IEnumerable<(string Label, object Store)> Stores()
    {
        yield return ("channel", new SqlServerChannelSql(Options.Create(new SqlServerAsyncResponseChannelOptions { ConnectionString = ConnectionString })));
        yield return ("transport", new SqlServerTransportStore(Options.Create(new SqlServerAsyncResponseTransportOptions { ConnectionString = ConnectionString })));
        yield return ("durable-flow", new SqlServerFlowStateStore(Options.Create(new SqlServerDurableFlowOptions { ConnectionString = ConnectionString })));
    }

    /// <summary>
    /// Every declared column of every expected table, across one store's contract. Stores whose
    /// expectation differs between the shape their own DDL created and an operator-provisioned one
    /// take a <c>selfCreated</c> flag; the rest declare one shape for both.
    /// </summary>
    private static IEnumerable<(string Table, string Column, string? Type)> ExpectedColumns(object store, bool selfCreated = true)
    {
        var method = store.GetType().GetMethod("ExpectedObjects", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var expectedObjects = (IEnumerable)method.Invoke(store, method.GetParameters().Length == 0 ? null : [selfCreated])!;

        foreach (var expectedObject in expectedObjects)
        {
            var name = (string)Read(expectedObject, "Name")!;
            if (Read(expectedObject, "Columns") is not IEnumerable columns)
                continue;

            foreach (var column in columns)
                yield return (name, (string)Read(column, "Name")!, (string?)Read(column, "Type"));
        }
    }

    private static object? Read(object instance, string property)
        => instance.GetType().GetProperty(property, BindingFlags.Instance | BindingFlags.Public)!.GetValue(instance);

    /// <summary>
    /// Reflection facade over one package's compilation of the verifier. The type name exists in
    /// three assemblies at once, so the C# name is ambiguous and every member is reached through
    /// the anchor assembly instead.
    /// </summary>
    private sealed class Verifier
    {
        private readonly Type _expectedObject;
        private readonly Type _expectedColumn;
        private readonly Type _actualColumn;
        private readonly Type _actualIndex;
        private readonly object _tableKind;
        private readonly object _indexKind;
        private readonly object _columnComparer;
        private readonly MethodInfo _renderType;
        private readonly MethodInfo _evaluate;
        private readonly MethodInfo _evaluateIndexes;

        public Verifier(Type anchor)
        {
            var type = anchor.Assembly.GetType("AsyncResponse.Internal.SqlServerRelationVerifier", throwOnError: true)!;
            _expectedObject = type.GetNestedType("ExpectedObject", BindingFlags.NonPublic)!;
            _expectedColumn = type.GetNestedType("ExpectedColumn", BindingFlags.NonPublic)!;
            _actualColumn = type.GetNestedType("ActualColumn", BindingFlags.NonPublic)!;
            _actualIndex = type.GetNestedType("ActualIndex", BindingFlags.NonPublic)!;
            var objectKind = anchor.Assembly.GetType("AsyncResponse.Internal.SqlServerObjectKind", throwOnError: true)!;
            _tableKind = Enum.ToObject(objectKind, 0);
            _indexKind = Enum.ToObject(objectKind, 2);
            _columnComparer = type.GetNestedType("TableColumnComparer", BindingFlags.NonPublic)!
                .GetField("Instance", BindingFlags.Public | BindingFlags.Static)!
                .GetValue(null)!;
            _renderType = type.GetMethod("RenderType", BindingFlags.NonPublic | BindingFlags.Static)!;
            _evaluate = type.GetMethod("EvaluateTableColumns", BindingFlags.NonPublic | BindingFlags.Static)!;
            _evaluateIndexes = type.GetMethod("EvaluateIndexes", BindingFlags.NonPublic | BindingFlags.Static)!;
        }

        public string RenderType(string typeName, short maxLength, byte scale)
            => (string)_renderType.Invoke(null, [typeName, maxLength, scale])!;

        public object Column(string name, string? type, bool nullable, bool requiresBinaryCollation = false, string? defaultExpression = null)
            => Activator.CreateInstance(_expectedColumn, name, type, nullable, requiresBinaryCollation, defaultExpression)!;

        public object Table(string name, params object[] columns)
            // Positional args must cover the whole ctor: ExpectedObject also carries the index
            // members (OwningTable, KeyColumns), null here — tables do not use them.
            => Activator.CreateInstance(_expectedObject, name, _tableKind, TypedArray(_expectedColumn, columns), null, null, null)!;

        public Array Tables(params object[] tables) => TypedArray(_expectedObject, tables);

        /// <summary>A healthy catalog row; facts override only what they bend.</summary>
        public object ColumnRow(string type, bool nullable, string collation = "", bool writable = false, string @default = "")
            => Activator.CreateInstance(_actualColumn, type, nullable, collation, writable, @default)!;

        public object Index(string name, string owningTable, params string[] keyColumns)
            => Activator.CreateInstance(_expectedObject, name, _indexKind, null, null, owningTable, keyColumns)!;

        /// <summary>A healthy catalog index row (nonclustered rowstore); facts override only what they bend.</summary>
        public object IndexRow(byte type = 2, bool unique = false, bool filtered = false, bool disabled = false, params string[] keyColumns)
        {
            var keys = (System.Collections.IList)Activator.CreateInstance(
                typeof(List<>).MakeGenericType(typeof((byte Ordinal, string Column))))!;
            for (byte ordinal = 0; ordinal < keyColumns.Length; ordinal++)
                keys.Add(((byte)(ordinal + 1), keyColumns[ordinal]));
            return Activator.CreateInstance(_actualIndex, type, unique, filtered, disabled, keys)!;
        }

        /// <summary>Runs the pure index decision over fabricated catalog rows; null means "accepted".</summary>
        public InvalidOperationException? EvaluateIndexRows(
            Array indexes,
            ((string Index, string Table) Key, object Row)[] rows,
            bool reportAbsence = true)
        {
            var actual = (System.Collections.IDictionary)Activator.CreateInstance(
                typeof(Dictionary<,>).MakeGenericType(typeof((string, string)), _actualIndex),
                _columnComparer)!;
            foreach (var (key, row) in rows)
                actual.Add(key, row);

            try
            {
                _evaluateIndexes.Invoke(null, ["catalog_test", "channel", indexes, actual, reportAbsence]);
                return null;
            }
            catch (TargetInvocationException wrapped)
            {
                return Assert.IsType<InvalidOperationException>(wrapped.InnerException);
            }
        }

        /// <summary>Runs the pure decision over fabricated catalog rows; null means "accepted".</summary>
        public InvalidOperationException? Evaluate(Array tables, ((string Table, string Column) Key, object Row)[] columns)
        {
            // Built with the verifier's own comparer: the catalog matched names case-insensitively,
            // so an ordinally keyed dictionary would not be the map the loader hands over.
            var columnRows = (IDictionary)Activator.CreateInstance(
                typeof(Dictionary<,>).MakeGenericType(typeof((string, string)), _actualColumn),
                _columnComparer)!;
            foreach (var (key, row) in columns)
                columnRows.Add(key, row);

            try
            {
                _evaluate.Invoke(null, ["catalog_test", "channel", tables, columnRows]);
                return null;
            }
            catch (TargetInvocationException wrapped)
            {
                return Assert.IsType<InvalidOperationException>(wrapped.InnerException);
            }
        }

        private static Array TypedArray(Type element, object[] items)
        {
            var array = Array.CreateInstance(element, items.Length);
            for (var i = 0; i < items.Length; i++)
                array.SetValue(items[i], i);
            return array;
        }
    }
}
