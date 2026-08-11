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
    internal readonly record struct ExpectedColumn(string Name, string Type, bool Nullable, bool RequiresBinaryCollation = false);

    /// <summary>
    /// One expected object: a user table (verified against <paramref name="Columns"/> when given)
    /// or a sequence (verified <c>bigint</c>, increment 1, no cycle).
    /// </summary>
    internal readonly record struct ExpectedObject(string Name, SqlServerObjectKind Kind, ExpectedColumn[]? Columns = null);

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

        foreach (var expectedObject in expected)
        {
            if (!actual.ContainsKey(expectedObject.Name))
                throw new InvalidOperationException(
                    $"The SQL Server {componentName} store expected '{schemaName}.{expectedObject.Name}' to exist after schema creation, but it does not.");
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

        var actual = new Dictionary<string, string>(StringComparer.Ordinal);
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
                   CASE WHEN c.default_object_id <> 0 OR c.is_identity = 1 OR c.is_computed = 1 THEN 1 ELSE 0 END
            FROM sys.columns c
            JOIN sys.objects o ON o.object_id = c.object_id
            JOIN sys.schemas s ON s.schema_id = o.schema_id
            JOIN sys.types t ON t.user_type_id = c.user_type_id
            WHERE s.name = @schema AND o.name IN ({NameParameters(command, tables.Select(e => e.Name))});
            """;
        command.Parameters.AddWithValue("@schema", schemaName);

        var actual = new Dictionary<(string Table, string Column), ActualColumn>();
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                actual[(reader.GetString(0), reader.GetString(1))] = new ActualColumn(
                    Type: RenderType(reader.GetString(2), reader.GetInt16(3)),
                    Nullable: reader.GetBoolean(4),
                    Collation: reader.GetString(5),
                    Writable: reader.GetInt32(6) == 1);
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

                if (column.RequiresBinaryCollation && !IsCaseSensitive(found.Collation))
                    throw new InvalidOperationException(
                        $"The SQL Server {componentName} store's column '{schemaName}.{table.Name}.{column.Name}' uses the " +
                        $"case-insensitive collation '{found.Collation}'. That column stores an identity the library compares " +
                        "ordinally, so the database would treat distinct ids such as 'id-a' and 'ID-A' as one key — cross-matching " +
                        "lookups and rejecting the second id on insert. Recreate the table (new deployments get " +
                        "COLLATE Latin1_General_100_BIN2 automatically), or ALTER the column to a binary or _CS_ collation after " +
                        "dropping the keys and indexes that reference it.");
            }

            // Extra columns are fine only when inserts that do not name them can still succeed.
            var expectedNames = table.Columns!.Select(c => c.Name).ToHashSet(StringComparer.Ordinal);
            foreach (var ((tableName, columnName), found) in actual)
            {
                if (!string.Equals(tableName, table.Name, StringComparison.Ordinal) || expectedNames.Contains(columnName))
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
    /// Renders a <c>sys.types</c> row the way the DDL declares it. <c>max_length</c> is in bytes,
    /// so the Unicode types halve it, and -1 is the <c>(max)</c> sentinel.
    /// </summary>
    private static string RenderType(string typeName, short maxLength) => typeName switch
    {
        "nvarchar" or "nchar" => maxLength < 0 ? $"{typeName}(max)" : $"{typeName}({maxLength / 2})",
        "varchar" or "char" or "varbinary" or "binary" => maxLength < 0 ? $"{typeName}(max)" : $"{typeName}({maxLength})",
        _ => typeName
    };

    // Binary collations sort and compare by code point, which is what an ordinal comparison needs;
    // an explicitly case-sensitive collation distinguishes the ids just as well, so deployments on
    // a _CS_ database are not forced to rebuild.
    private static bool IsCaseSensitive(string collation)
        => collation.Contains("_BIN", StringComparison.OrdinalIgnoreCase)
            || collation.Contains("_CS_", StringComparison.OrdinalIgnoreCase)
            || collation.EndsWith("_CS", StringComparison.OrdinalIgnoreCase);

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

    private readonly record struct ActualColumn(string Type, bool Nullable, string Collation, bool Writable);
}

/// <summary>The SQL Server object kinds an AsyncResponse store creates.</summary>
internal enum SqlServerObjectKind
{
    /// <summary>A user table (<c>sys.objects.type = 'U'</c>).</summary>
    Table = 0,

    /// <summary>A sequence object (<c>sys.objects.type = 'SO'</c>).</summary>
    Sequence = 1
}
