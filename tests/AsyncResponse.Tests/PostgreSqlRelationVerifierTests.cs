using System.Collections;
using System.Reflection;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.DurableFlows.PostgreSQL;
using AsyncResponse.Transports.PostgreSQL;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The PostgreSQL catalog-verification decision table, unit-tested against the verifier's
/// internal catalog-row model so the accept/reject logic runs on every platform (the Oracle
/// store's verification tests set the precedent; the integration suite proves the same decisions
/// against a real server's catalogs). <c>src/Shared/PostgreSqlRelationVerifier.cs</c> is
/// source-linked into the channel, transport, and durable-flow packages, so every fact runs
/// against all three compilations by reflection, like <see cref="DurableFlowStoreSharedTests"/>.
/// </summary>
public sealed class PostgreSqlRelationVerifierTests
{
    public static TheoryData<Type> AnchorTypes =>
    [
        typeof(PostgreSqlAsyncResponseChannelOptions),
        typeof(PostgreSqlAsyncResponseTransportOptions),
        typeof(PostgreSqlDurableFlowOptions)
    ];

    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void CatalogQueries_SliceKeyColumns_AndSelectSelfPopulatingColumns(Type anchor)
    {
        var verifier = new Verifier(anchor);

        // indkey lists key columns FOLLOWED by INCLUDE payload columns; only the
        // indnkeyatts-sliced prefix is the key. Reading the whole vector rejected a covering
        // PRIMARY KEY (id) INCLUDE (queue) as "(id, queue) instead of (id)" — for an index that
        // enforces exactly the uniqueness the stores rely on. Both subqueries must slice.
        Assert.Contains("unnest(i.indkey[0:i.indnkeyatts-1])", verifier.RelationQuery, StringComparison.Ordinal);
        Assert.Contains("unnest(pi.indkey[0:pi.indnkeyatts-1])", verifier.RelationQuery, StringComparison.Ordinal);

        // Identity columns carry no pg_attrdef row: without attidentity/attgenerated in the
        // select, an extra GENERATED ... AS IDENTITY column — which PostgreSQL populates on
        // every insert — read as "NOT NULL without a default" and blocked startup.
        Assert.Contains("a.attidentity <> ''", verifier.TableColumnQuery, StringComparison.Ordinal);
        Assert.Contains("a.attgenerated <> ''", verifier.TableColumnQuery, StringComparison.Ordinal);

        // format_type renders no collation, so the determinism flag has to be joined in from
        // pg_collation — and through a LEFT JOIN, since attcollation is 0 on a type that carries
        // no collation at all (uuid, bigint), which must pass rather than read as missing.
        Assert.Contains("LEFT JOIN pg_collation co ON co.oid = a.attcollation", verifier.TableColumnQuery, StringComparison.Ordinal);
        Assert.Contains("COALESCE(co.collisdeterministic, true)", verifier.TableColumnQuery, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void Evaluate_RejectsANonDeterministicCollationOnAnIdentityColumn(Type anchor)
    {
        var verifier = new Verifier(anchor);
        var expected = verifier.Relations(verifier.Table(
            "jobs",
            columns: [verifier.Column("queue", "text", nullable: false, deterministicCollation: true)]));
        var relations = new[] { ("jobs", verifier.Row()) };

        // A non-deterministic ICU collation makes the database treat strings its own rules call
        // equal as ONE key: two distinct queue names/ids collide, so lookups cross-match and the
        // primary key rejects the second. Nothing downstream re-checks this column's storage.
        var rejected = verifier.Evaluate(
            expected,
            relations,
            [(("jobs", "queue"), verifier.ColumnRow("text", notNull: true, @default: "", writable: false, collation: "ci_icu", deterministicCollation: false))]);

        Assert.NotNull(rejected);
        Assert.Contains("'catalog_test.jobs.queue'", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("collation 'ci_icu'", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("non-deterministic", rejected.Message, StringComparison.Ordinal);

        // The default collation and an uncollatable type (attcollation 0 → no joined row) pass.
        foreach (var accepted in new[]
        {
            verifier.ColumnRow("text", notNull: true, @default: "", writable: false, collation: "default", deterministicCollation: true),
            verifier.ColumnRow("text", notNull: true, @default: "", writable: false),
        })
        {
            Assert.Null(verifier.Evaluate(expected, relations, [(("jobs", "queue"), accepted)]));
        }

        // An unflagged column is free to carry one: only the ids the library compares ordinally
        // are constrained.
        var unflagged = verifier.Relations(verifier.Table(
            "jobs",
            columns: [verifier.Column("queue", "text", nullable: false)]));
        Assert.Null(verifier.Evaluate(
            unflagged,
            relations,
            [(("jobs", "queue"), verifier.ColumnRow("text", notNull: true, @default: "", writable: false, collation: "ci_icu", deterministicCollation: false))]));
    }

    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void Evaluate_AllowsExtraColumnsTheDatabaseFillsIn_AndRejectsBareNotNull(Type anchor)
    {
        var verifier = new Verifier(anchor);
        var expected = verifier.Relations(verifier.Table("jobs", columns: [verifier.Column("id", "uuid", nullable: false)]));
        var relations = new[] { ("jobs", verifier.Row()) };
        var declaredColumn = (("jobs", "id"), verifier.ColumnRow("uuid", notNull: true, @default: "", writable: false));

        // Anything the database can fill in for itself is harmless bookkeeping: nullable,
        // defaulted, and identity/generated (writable without a rendered default) extras.
        foreach (var harmless in new[]
        {
            verifier.ColumnRow("bigint", notNull: false, @default: "", writable: false),
            verifier.ColumnRow("bigint", notNull: true, @default: "nextval('jobs_extra_seq'::regclass)", writable: true),
            verifier.ColumnRow("bigint", notNull: true, @default: "", writable: true),
        })
        {
            Assert.Null(verifier.Evaluate(expected, relations, [declaredColumn, (("jobs", "extra"), harmless)]));
        }

        // An extra NOT NULL column nothing populates fails every insert the store issues.
        var rejected = verifier.Evaluate(
            expected,
            relations,
            [declaredColumn, (("jobs", "extra"), verifier.ColumnRow("bigint", notNull: true, @default: "", writable: false))]);
        Assert.NotNull(rejected);
        Assert.Contains("extra column 'extra'", rejected.Message, StringComparison.Ordinal);
        Assert.Contains("not_null_violation", rejected.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void Evaluate_NamesThePresentCulprit_NotTheAbsentVictim(Type anchor)
    {
        var verifier = new Verifier(anchor);

        // The absent relation is declared FIRST: a declaration-order walk would report the
        // victim ("does not exist") and hide the wrong-kind occupation that explains it.
        var expected = verifier.Relations(verifier.Table("victim"), verifier.Sequence("culprit"));
        var diagnosed = verifier.Evaluate(expected, [("culprit", verifier.Row(kind: "r"))]);

        Assert.NotNull(diagnosed);
        Assert.Contains("'catalog_test.culprit' to be a sequence", diagnosed.Message, StringComparison.Ordinal);
        Assert.Contains("occupied by a table", diagnosed.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("to exist after schema creation", diagnosed.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void Evaluate_ReportsAbsenceLast_WithTheSharedNamespaceGuidance(Type anchor)
    {
        var verifier = new Verifier(anchor);

        // Nothing present is wrong, so absence IS the finding — reported with the namespace
        // guidance every other throw in the verifier already carries.
        var expected = verifier.Relations(verifier.Table("present_ok"), verifier.Table("victim"));
        var diagnosed = verifier.Evaluate(expected, [("present_ok", verifier.Row())]);

        Assert.NotNull(diagnosed);
        Assert.Contains("'catalog_test.victim' to exist after schema creation", diagnosed.Message, StringComparison.Ordinal);
        Assert.Contains("share one namespace", diagnosed.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(AnchorTypes))]
    public void Evaluate_StillRejectsARealPrimaryKeyMismatch(Type anchor)
    {
        var verifier = new Verifier(anchor);

        // Guards the covering-key acceptance from overcorrecting: a key over genuinely different
        // KEY columns (not INCLUDE payload) must still be rejected.
        var expected = verifier.Relations(verifier.Table("jobs", primaryKey: ["id"]));
        var diagnosed = verifier.Evaluate(expected, [("jobs", verifier.Row(primaryKey: ["id", "queue"]))]);

        Assert.NotNull(diagnosed);
        Assert.Contains("primary key is (id, queue) instead of (id)", diagnosed.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Reflection facade over one package's compilation of the verifier. The type name exists in
    /// three assemblies at once, so the C# name is ambiguous and every member is reached through
    /// the anchor assembly instead.
    /// </summary>
    private sealed class Verifier
    {
        private readonly Type _type;
        private readonly Type _expectedRelation;
        private readonly Type _expectedColumn;
        private readonly Type _actualRelation;
        private readonly Type _actualColumn;
        private readonly MethodInfo _evaluate;

        public Verifier(Type anchor)
        {
            _type = anchor.Assembly.GetType("AsyncResponse.Internal.PostgreSqlRelationVerifier", throwOnError: true)!;
            _expectedRelation = Nested("ExpectedRelation");
            _expectedColumn = Nested("ExpectedColumn");
            _actualRelation = Nested("ActualRelation");
            _actualColumn = Nested("ActualColumn");
            _evaluate = _type.GetMethod("Evaluate", BindingFlags.NonPublic | BindingFlags.Static)!;
        }

        public string RelationQuery => Constant("RelationQuery");
        public string TableColumnQuery => Constant("TableColumnQuery");

        public object Column(string name, string type, bool nullable, string? defaultExpression = null, bool deterministicCollation = false)
            => Activator.CreateInstance(_expectedColumn, name, type, nullable, deterministicCollation, defaultExpression)!;

        public object Table(string name, object[]? columns = null, string[]? primaryKey = null)
            => Activator.CreateInstance(
                _expectedRelation, name, 'r', null, null, columns is null ? null : TypedArray(_expectedColumn, columns), primaryKey)!;

        public object Sequence(string name)
            => Activator.CreateInstance(_expectedRelation, name, 'S', null, null, null, null)!;

        public Array Relations(params object[] relations) => TypedArray(_expectedRelation, relations);

        /// <summary>A healthy permanent-table catalog row; facts override only what they bend.</summary>
        public object Row(string kind = "r", string persistence = "p", string[]? primaryKey = null)
            => Activator.CreateInstance(
                _actualRelation,
                kind, persistence, "", "", false, false, true,
                Array.Empty<string>(), "", 1L, 1L, false, long.MaxValue,
                primaryKey ?? Array.Empty<string>())!;

        public object ColumnRow(
            string type,
            bool notNull,
            string @default,
            bool writable,
            string collation = "",
            bool deterministicCollation = true)
            => Activator.CreateInstance(_actualColumn, type, notNull, @default, writable, collation, deterministicCollation)!;

        /// <summary>Runs the pure decision over fabricated catalog rows; null means "accepted".</summary>
        public InvalidOperationException? Evaluate(
            Array expected,
            (string Name, object Row)[] relations,
            ((string Table, string Column) Key, object Row)[]? columns = null)
        {
            var relationRows = (IDictionary)Activator.CreateInstance(
                typeof(Dictionary<,>).MakeGenericType(typeof(string), _actualRelation))!;
            foreach (var (name, row) in relations)
                relationRows.Add(name, row);

            var columnRows = (IDictionary)Activator.CreateInstance(
                typeof(Dictionary<,>).MakeGenericType(typeof((string, string)), _actualColumn))!;
            foreach (var (key, row) in columns ?? [])
                columnRows.Add(key, row);

            try
            {
                _evaluate.Invoke(null, ["catalog_test", "channel", expected, relationRows, columnRows]);
                return null;
            }
            catch (TargetInvocationException wrapped)
            {
                return Assert.IsType<InvalidOperationException>(wrapped.InnerException);
            }
        }

        private Type Nested(string name) => _type.GetNestedType(name, BindingFlags.NonPublic)!;

        private string Constant(string name)
            => (string)_type.GetField(name, BindingFlags.NonPublic | BindingFlags.Static)!.GetRawConstantValue()!;

        private static Array TypedArray(Type element, object[] items)
        {
            var array = Array.CreateInstance(element, items.Length);
            for (var i = 0; i < items.Length; i++)
                array.SetValue(items[i], i);
            return array;
        }
    }
}
