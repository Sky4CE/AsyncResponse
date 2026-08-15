using Microsoft.Data.SqlClient;
using System.Globalization;
using System.Text;

namespace AsyncResponse.Internal;

/// <summary>
/// In-transaction catalog verification that every object a SQL Server store just ensured actually
/// IS what its DDL intended — the SQL Server counterpart of <c>PostgreSqlRelationVerifier</c>.
/// <para>
/// The stores guard their DDL with <c>IF OBJECT_ID(N'…', N'U') IS NULL</c>, which answers only
/// "is there a user table with this name". That leaves two silent failure modes. A name occupied
/// by a DIFFERENT object kind (a view, a synonym, a procedure) makes the guard fall through and
/// the CREATE fail with raw error 2714. A name occupied by ANOTHER component's user table makes
/// the guard skip creation entirely, and the store then fails at its first query on a column that
/// does not exist. Both are caught here with an actionable message instead.
/// </para>
/// <para>
/// Runs under the schema-keyed <c>sp_getapplock</c> the AsyncResponse SQL Server stores share, so
/// a sibling component's objects are either committed and visible or serialized behind this
/// transaction. Source-linked into the channel, transport, and durable-flow packages (separate
/// packages cannot share compiled code).
/// </para>
/// </summary>
internal static class SqlServerRelationVerifier
{
    /// <summary>
    /// One expected column: name, rendered type (<c>nvarchar(400)</c>, <c>nvarchar(max)</c>,
    /// <c>datetime2</c>, …), and nullability. <paramref name="RequiresBinaryCollation"/> marks the
    /// columns that store an identity the library compares ORDINALLY — correlation ids, flow ids,
    /// queue names. Under a case-insensitive column collation (the default in a great many SQL
    /// Server deployments) the database treats <c>foo</c> and <c>FOO</c> as the same key, so two
    /// distinct ids collide: lookups cross-match and primary keys reject the second id.
    /// </summary>
    /// <remarks>
    /// <c>DefaultExpression</c> is the exact <c>sys.default_constraints.definition</c> rendering,
    /// given for the columns the store never names on insert and therefore DEPENDS on:
    /// <c>(sysutcdatetime())</c>, <c>((0))</c>, <c>(N'{}')</c>. <c>null</c> means the store always
    /// supplies the value itself.
    /// </remarks>
    internal readonly record struct ExpectedColumn(
        string Name,
        string Type,
        bool Nullable,
        bool RequiresBinaryCollation = false,
        string? DefaultExpression = null);

    /// <summary>
    /// One expected object: a user table (verified against <paramref name="Columns"/> when given)
    /// or a sequence (verified <c>bigint</c>, increment 1, no cycle).
    /// </summary>
    /// <remarks>
    /// <c>PrimaryKey</c> lists the key columns in order, when the store's correctness depends on
    /// them: the idempotent-publish insert and the per-id upsert rely on the primary key REJECTING
    /// a duplicate. A same-name table without it accepts every duplicate silently.
    /// </remarks>
    internal readonly record struct ExpectedObject(
        string Name,
        SqlServerObjectKind Kind,
        ExpectedColumn[]? Columns = null,
        string[]? PrimaryKey = null);

    /// <summary>
    /// Verifies every expected object. Pass the DDL transaction so the checks see uncommitted
    /// work; pass <c>null</c> when diagnosing a FAILED batch on a fresh connection, where the
    /// objects of interest are somebody else's and already committed.
    /// </summary>
    public static async Task VerifyAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        string schemaName,
        string componentName,
        IReadOnlyList<ExpectedObject> expected,
        CancellationToken cancellationToken)
    {
        var actual = await LoadObjectKindsAsync(connection, transaction, schemaName, expected, cancellationToken).ConfigureAwait(false);

        // Diagnose in CAUSE order, not declaration order. When this runs after a failed DDL batch,
        // several expected objects are missing precisely BECAUSE one name was already taken — so
        // reporting "does not exist" first would name a victim and hide the culprit. Anything that
        // is present and wrong is checked first; absence is only reported once nothing present
        // explains it.
        foreach (var expectedObject in expected)
        {
            if (!actual.TryGetValue(expectedObject.Name, out var foundType))
                continue;

            var expectedType = expectedObject.Kind == SqlServerObjectKind.Table ? "U" : "SO";
            if (!string.Equals(foundType, expectedType, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"The SQL Server {componentName} store expected '{schemaName}.{expectedObject.Name}' to be {Describe(expectedType)}, " +
                    $"but the name is occupied by {Describe(foundType)}. The store's existence guard only looks for its own object kind, " +
                    "so it either skipped creation or failed with error 2714. " + CollisionGuidance);
            }
        }

        var present = expected.Where(e => actual.ContainsKey(e.Name)).ToArray();
        await VerifySequencesAsync(connection, transaction, schemaName, componentName, present, cancellationToken).ConfigureAwait(false);
        await VerifyTableColumnsAsync(connection, transaction, schemaName, componentName, present, cancellationToken).ConfigureAwait(false);
        await VerifyPrimaryKeysAsync(connection, transaction, schemaName, componentName, present, cancellationToken).ConfigureAwait(false);

        foreach (var expectedObject in expected)
        {
            if (!actual.ContainsKey(expectedObject.Name))
                throw new InvalidOperationException(
                    $"The SQL Server {componentName} store expected '{schemaName}.{expectedObject.Name}' to exist after schema creation, but it does not.");
        }
    }

    /// <summary>
    /// Turns a FAILED DDL batch into the actionable diagnosis, or lets the original error stand.
    /// The batch can die before <see cref="VerifyAsync"/> ever runs — a name held by another
    /// component's table suppresses the guarded CREATE and the statements after it hit the wrong
    /// table; a name held by a view fails outright with error 2714. Re-running the checks on a
    /// FRESH connection (the colliding objects are somebody else's and already committed) recovers
    /// the precise reason. When they find nothing, the caller rethrows: a failure from permissions,
    /// a full disk, or a dropped connection is not a collision and must not be reported as one.
    /// </summary>
    public static async Task ThrowDiagnosedCollisionAsync(
        Func<CancellationToken, Task<SqlConnection>> openConnectionAsync,
        SqlException failure,
        string schemaName,
        string componentName,
        IReadOnlyList<ExpectedObject> expected,
        CancellationToken cancellationToken)
    {
        SqlConnection diagnosis;
        try
        {
            diagnosis = await openConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (SqlException)
        {
            return; // The server is the problem, not the schema; the original error says so.
        }

        await using (diagnosis)
        {
            try
            {
                await VerifyAsync(diagnosis, transaction: null, schemaName, componentName, expected, cancellationToken).ConfigureAwait(false);
            }
            catch (InvalidOperationException diagnosed)
            {
                throw new InvalidOperationException(diagnosed.Message, failure);
            }
        }
    }

    private static async Task<Dictionary<string, string>> LoadObjectKindsAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        string schemaName,
        IReadOnlyList<ExpectedObject> expected,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            SELECT o.name, RTRIM(o.type)
            FROM sys.objects o
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND o.name IN ({NameParameters(command, expected.Select(e => e.Name))});
            """;
        command.Parameters.AddWithValue("@schema", schemaName);

        // OrdinalIgnoreCase, matching how the server itself matched: under a case-insensitive
        // catalog collation (the common default) the IN list above returns a foreign object whose
        // name differs from the configured spelling only in case — keyed ordinally, every
        // downstream lookup by the CONFIGURED spelling missed it, so the kind/column/PK checks
        // silently skipped the collision and the final absence loop misreported the object as
        // "does not exist".
        var actual = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            actual[reader.GetString(0)] = reader.GetString(1);
        return actual;
    }

    /// <summary>
    /// The ack sequence is a cross-process monotonic clock: a descending increment or a wrapping
    /// CYCLE would hand out values whose ORDER contradicts the ordering the watermark relies on.
    /// </summary>
    private static async Task VerifySequencesAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        string schemaName,
        string componentName,
        IReadOnlyList<ExpectedObject> expected,
        CancellationToken cancellationToken)
    {
        var sequences = expected.Where(e => e.Kind == SqlServerObjectKind.Sequence).ToArray();
        if (sequences.Length == 0)
            return;

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            SELECT sq.name, t.name, CAST(sq.increment AS bigint), sq.is_cycling
            FROM sys.sequences sq
            JOIN sys.schemas s ON s.schema_id = sq.schema_id
            JOIN sys.types t ON t.user_type_id = sq.user_type_id
            WHERE s.name = @schema AND sq.name IN ({NameParameters(command, sequences.Select(e => e.Name))});
            """;
        command.Parameters.AddWithValue("@schema", schemaName);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var name = reader.GetString(0);
            var type = reader.GetString(1);
            var increment = reader.GetInt64(2);
            var cycles = reader.GetBoolean(3);
            if (!string.Equals(type, "bigint", StringComparison.Ordinal) || increment != 1 || cycles)
            {
                throw new InvalidOperationException(
                    $"The SQL Server {componentName} store's sequence '{schemaName}.{name}' exists but is not a monotonic counter: " +
                    $"expected bigint INCREMENT BY 1 NO CYCLE; found {type} INCREMENT BY {increment.ToString(CultureInfo.InvariantCulture)}" +
                    $"{(cycles ? " CYCLE" : " NO CYCLE")}. Acknowledgement ordering is derived from this sequence, so a descending or " +
                    "wrapping sequence silently reorders delivery. Fix it with " +
                    $"ALTER SEQUENCE {schemaName}.{name} INCREMENT BY 1 NO CYCLE; (recreate it if the type is wrong).");
            }
        }
    }

    /// <summary>
    /// Column-level verification: a same-kind table occupying the name passes the object-kind
    /// check and fails only at the first query. Every DDL-declared column must exist with the
    /// declared type and nullability; identity columns must carry a case-sensitive collation; and
    /// extra columns are allowed only when an insert that does not name them can still succeed.
    /// </summary>
    private static async Task VerifyTableColumnsAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        string schemaName,
        string componentName,
        IReadOnlyList<ExpectedObject> expected,
        CancellationToken cancellationToken)
    {
        var tables = expected.Where(e => e.Kind == SqlServerObjectKind.Table && e.Columns is not null).ToArray();
        if (tables.Length == 0)
            return;

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            SELECT o.name, c.name, t.name, c.max_length, c.is_nullable, ISNULL(c.collation_name, N''),
                   CASE WHEN c.default_object_id <> 0 OR c.is_identity = 1 OR c.is_computed = 1 THEN 1 ELSE 0 END,
                   ISNULL(dc.definition, N'')
            FROM sys.columns c
            JOIN sys.objects o ON o.object_id = c.object_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            JOIN sys.types t ON t.user_type_id = c.user_type_id
            LEFT JOIN sys.default_constraints dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
            WHERE s.name = @schema AND o.name IN ({NameParameters(command, tables.Select(e => e.Name))});
            """;
        command.Parameters.AddWithValue("@schema", schemaName);

        // OrdinalIgnoreCase throughout, matching how the server itself matched (see the note on
        // LoadObjectKindsAsync): under a case-insensitive catalog collation the IN list above
        // returns rows whose table/column names may differ from the configured spelling only in
        // case — keyed ordinally, every lookup by the configured spelling missed them, so a
        // correctly-shaped case-variant table was rejected as "missing the column" while the
        // real shape checks were silently skipped.
        var actual = new Dictionary<(string Table, string Column), ActualColumn>(TableColumnComparer.Instance);
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                actual[(reader.GetString(0), reader.GetString(1))] = new ActualColumn(
                    Type: RenderType(reader.GetString(2), reader.GetInt16(3)),
                    Nullable: reader.GetBoolean(4),
                    Collation: reader.GetString(5),
                    Writable: reader.GetInt32(6) == 1,
                    Default: reader.GetString(7));
            }
        }

        foreach (var table in tables)
        {
            foreach (var column in table.Columns!)
            {
                if (!actual.TryGetValue((table.Name, column.Name), out var found))
                    throw new InvalidOperationException(
                        $"The SQL Server {componentName} store's table '{schemaName}.{table.Name}' exists but is missing the column " +
                        $"'{column.Name}' ({column.Type}); a same-name table from another component or a partial manual creation " +
                        "occupies the name. " + CollisionGuidance);

                if (!string.Equals(found.Type, column.Type, StringComparison.OrdinalIgnoreCase) || found.Nullable != column.Nullable)
                    throw new InvalidOperationException(
                        $"The SQL Server {componentName} store's table '{schemaName}.{table.Name}' exists but column '{column.Name}' " +
                        $"does not match the expected shape: expected {column.Type}{(column.Nullable ? " NULL" : " NOT NULL")}; " +
                        $"found {found.Type}{(found.Nullable ? " NULL" : " NOT NULL")}. " + CollisionGuidance);

                if (column.RequiresBinaryCollation && !IsOrdinalCollation(found.Collation))
                    throw new InvalidOperationException(
                        $"The SQL Server {componentName} store's column '{schemaName}.{table.Name}.{column.Name}' uses the " +
                        $"collation '{found.Collation}', which is not binary. That column stores an identity the library compares " +
                        "ORDINALLY, and any non-binary collation folds something the library treats as distinct — case under a _CI_ " +
                        "collation, accents under _AI, full-width forms under any collation without _WS. Distinct ids would then " +
                        "collide on one key: lookups cross-match and the second id is rejected on insert. Recreate the table (new " +
                        "deployments get COLLATE Latin1_General_100_BIN2 automatically), or ALTER the column to a _BIN2 collation " +
                        "after dropping the keys and indexes that reference it.");

                // A default the store RELIES on (it never names the column on insert) must exist
                // and must compute what the store expects: a missing one fails every insert with
                // error 515, and a different one silently changes behavior — a shifted created_at
                // moves every watermark comparison, a future available_at strands the job forever.
                if (column.DefaultExpression is { } expectedDefault
                    && !string.Equals(found.Default, expectedDefault, StringComparison.OrdinalIgnoreCase))
                {
                    throw new InvalidOperationException(
                        $"The SQL Server {componentName} store's column '{schemaName}.{table.Name}.{column.Name}' " +
                        $"{(found.Default.Length == 0 ? "has no default" : $"defaults to {found.Default}")}, but the store never names " +
                        $"it on insert and depends on the default {expectedDefault}. " +
                        (found.Default.Length == 0
                            ? "Every insert would fail with error 515. "
                            : "The rows would carry values the store's own time and visibility logic does not expect. ") +
                        CollisionGuidance);
                }
            }

            // Extra columns are fine only when inserts that do not name them can still succeed.
            var expectedNames = table.Columns!.Select(c => c.Name).ToHashSet(StringComparer.OrdinalIgnoreCase);
            foreach (var ((tableName, columnName), found) in actual)
            {
                if (!string.Equals(tableName, table.Name, StringComparison.OrdinalIgnoreCase) || expectedNames.Contains(columnName))
                    continue;

                if (!found.Nullable && !found.Writable)
                    throw new InvalidOperationException(
                        $"The SQL Server {componentName} store's table '{schemaName}.{table.Name}' has an extra column '{columnName}' " +
                        "that is NOT NULL without a default: every insert the store issues would fail with error 515, because the " +
                        "store cannot know to supply a value for it. " + CollisionGuidance);
            }
        }
    }

    /// <summary>
    /// The primary key is load-bearing, not decoration: the transport's insert-if-absent publish
    /// and the flow store's per-id upsert both rely on it to reject a duplicate. A same-name table
    /// carrying the right columns but no key (or a key over different columns) passes every other
    /// check here and then silently accepts duplicate rows.
    /// </summary>
    private static async Task VerifyPrimaryKeysAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        string schemaName,
        string componentName,
        IReadOnlyList<ExpectedObject> expected,
        CancellationToken cancellationToken)
    {
        var keyed = expected.Where(e => e.PrimaryKey is not null).ToArray();
        if (keyed.Length == 0)
            return;

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText =
            $"""
            SELECT o.name, col.name, ic.key_ordinal
            FROM sys.indexes i
            JOIN sys.objects o ON o.object_id = i.object_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns col ON col.object_id = i.object_id AND col.column_id = ic.column_id
            WHERE i.is_primary_key = 1 AND s.name = @schema
              AND o.name IN ({NameParameters(command, keyed.Select(e => e.Name))});
            """;
        command.Parameters.AddWithValue("@schema", schemaName);

        // OrdinalIgnoreCase for the same reason as the column dictionary above: the IN list
        // matched case-insensitively, so an ordinal key would misreport a case-variant table as
        // "has no primary key" regardless of its real key.
        var actual = new Dictionary<string, List<(byte Ordinal, string Column)>>(StringComparer.OrdinalIgnoreCase);
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!actual.TryGetValue(reader.GetString(0), out var columns))
                    actual[reader.GetString(0)] = columns = [];
                columns.Add((reader.GetByte(2), reader.GetString(1)));
            }
        }

        foreach (var table in keyed)
        {
            var found = actual.TryGetValue(table.Name, out var columns)
                ? columns.OrderBy(c => c.Ordinal).Select(c => c.Column).ToArray()
                : [];

            if (!found.AsSpan().SequenceEqual(table.PrimaryKey!, StringComparer.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"The SQL Server {componentName} store's table '{schemaName}.{table.Name}' " +
                    $"{(found.Length == 0 ? "has no primary key" : $"has a primary key over ({string.Join(", ", found)})")}, " +
                    $"but the store's idempotent writes rely on a primary key over ({string.Join(", ", table.PrimaryKey!)}) to reject " +
                    "duplicates. Without it a retried publish or a concurrent create is accepted twice instead of deduplicated. " +
                    CollisionGuidance);
            }
        }
    }

    /// <summary>
    /// Renders a <c>sys.types</c> row the way the DDL declares it. <c>max_length</c> is in bytes,
    /// so the Unicode types halve it, and -1 is the <c>(max)</c> sentinel.
    /// </summary>
    private static string RenderType(string typeName, short maxLength) => typeName switch
    {
        "nvarchar" or "nchar" => maxLength < 0 ? $"{typeName}(max)" : $"{typeName}({maxLength / 2})",
        "varchar" or "char" or "varbinary" or "binary" => maxLength < 0 ? $"{typeName}(max)" : $"{typeName}({maxLength})",
        _ => typeName
    };

    // ONLY a binary collation compares by code point, which is what an ordinal contract requires.
    // A merely case-SENSITIVE collation is not enough: _CS_AI still folds accents (probed on SQL
    // Server 2022: 'cafe' = 'café'), and even _CS_AS is width-insensitive unless it also carries
    // _WS ('ab' = the full-width 'ａｂ'). Two ids the engine considers different would still
    // collide on the key.
    private static bool IsOrdinalCollation(string collation)
        => collation.Contains("_BIN", StringComparison.OrdinalIgnoreCase);

    private static string NameParameters(SqlCommand command, IEnumerable<string> names)
    {
        // sys.objects.name is sysname (an identifier), so the names are bound as parameters rather
        // than interpolated — the store's configured table names reach this method verbatim.
        var builder = new StringBuilder();
        var index = 0;
        foreach (var name in names)
        {
            if (index > 0)
                builder.Append(", ");

            var parameter = $"@name{index.ToString(CultureInfo.InvariantCulture)}";
            builder.Append(parameter);
            command.Parameters.AddWithValue(parameter, name);
            index++;
        }

        return index == 0 ? "NULL" : builder.ToString();
    }

    private static string Describe(string type) => type switch
    {
        "U" => "a user table",
        "SO" => "a sequence",
        "V" => "a view",
        "SN" => "a synonym",
        "P" => "a stored procedure",
        "IF" or "TF" or "FN" => "a function",
        _ => $"an object of type '{type}'"
    };

    private const string CollisionGuidance =
        "Give this component its own object names (or its own schema) so two AsyncResponse components cannot share one name.";

    private readonly record struct ActualColumn(string Type, bool Nullable, string Collation, bool Writable, string Default);

    /// <summary>Case-insensitive (table, column) tuple comparer — catalog name matching is
    /// case-insensitive under the common catalog collations, so the keys must be too.</summary>
    private sealed class TableColumnComparer : IEqualityComparer<(string Table, string Column)>
    {
        public static readonly TableColumnComparer Instance = new();

        public bool Equals((string Table, string Column) x, (string Table, string Column) y)
            => string.Equals(x.Table, y.Table, StringComparison.OrdinalIgnoreCase)
                && string.Equals(x.Column, y.Column, StringComparison.OrdinalIgnoreCase);

        public int GetHashCode((string Table, string Column) value)
            => HashCode.Combine(
                StringComparer.OrdinalIgnoreCase.GetHashCode(value.Table),
                StringComparer.OrdinalIgnoreCase.GetHashCode(value.Column));
    }
}

/// <summary>The SQL Server object kinds an AsyncResponse store creates.</summary>
internal enum SqlServerObjectKind
{
    /// <summary>A user table (<c>sys.objects.type = 'U'</c>).</summary>
    Table = 0,

    /// <summary>A sequence object (<c>sys.objects.type = 'SO'</c>).</summary>
    Sequence = 1
}
