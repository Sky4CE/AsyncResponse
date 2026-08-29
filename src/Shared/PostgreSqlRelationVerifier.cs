using Npgsql;

namespace AsyncResponse.Internal;

/// <summary>
/// In-transaction catalog verification that every relation a PostgreSQL store just ensured
/// actually IS what its DDL intended. <c>CREATE ... IF NOT EXISTS</c> matches ANY relation with
/// the name (tables, indexes, and sequences share one namespace per schema) and explicitly
/// guarantees nothing about an existing object's shape — a same-name table missing operational
/// columns, an index with different key columns, uniqueness, a predicate or access method, or a
/// sequence with the wrong increment/cache/cycle/bounds is silently accepted by the DDL and
/// fails (or corrupts ordering) only at runtime. Runs under the schema-keyed advisory DDL lock
/// shared by every AsyncResponse PostgreSQL store, so other components' objects are either
/// committed and visible or serialized behind this transaction. Source-linked into the channel,
/// transport, and durable-flow packages (separate packages cannot share compiled code).
/// </summary>
internal static class PostgreSqlRelationVerifier
{
    /// <summary>
    /// One expected table column: name, <c>format_type</c> rendering, nullability, and — for
    /// columns whose DDL declares a default the runtime relies on — the exact
    /// <c>pg_get_expr</c> rendering of that default. A merely EXISTING default is not enough:
    /// <c>created_at DEFAULT now() + interval '1 year'</c> silently shifts every timestamp the
    /// watermark and visibility logic compare, and <c>available_at</c> with a future default
    /// would strand transport jobs.
    /// <para>
    /// <paramref name="RequiresDeterministicCollation"/> marks the columns that store an identity
    /// the library compares ORDINALLY — correlation ids, queue names, flow ids. Under a
    /// non-deterministic ICU collation the database treats strings its own rules call equal as ONE
    /// key, so two distinct ids collide: lookups cross-match and a primary key rejects the second
    /// id. A type that carries no collation at all (<c>uuid</c>, <c>bigint</c>) reports none and
    /// always passes.
    /// </para>
    /// </summary>
    /// <remarks>
    /// The check reads the column's OWN collation. A DATABASE whose default collation is itself
    /// non-deterministic is out of scope: an uncollated declaration records the
    /// <c>pg_catalog."default"</c> entry, which is always marked deterministic whatever the
    /// cluster's locale, so the catalog cannot answer that question here.
    /// </remarks>
    internal readonly record struct ExpectedColumn(
        string Name,
        string Type,
        bool Nullable,
        bool RequiresDeterministicCollation = false,
        string? DefaultExpression = null);

    /// <summary>
    /// One expected relation: kind 'r' (table, verified against <paramref name="Columns"/> and
    /// <paramref name="PrimaryKey"/> when given), 'S' (sequence, verified <c>bigint</c>,
    /// increment 1, cache 1, no cycle, full positive range), or 'i' (index, verified to sit on
    /// <paramref name="OwningTable"/> as a plain — non-unique, non-partial, valid and ready —
    /// btree over exactly <paramref name="KeyColumns"/> in order). All relations must be
    /// permanent (not UNLOGGED or temporary).
    /// </summary>
    internal readonly record struct ExpectedRelation(
        string Name,
        char Kind,
        string? OwningTable = null,
        string[]? KeyColumns = null,
        ExpectedColumn[]? Columns = null,
        string[]? PrimaryKey = null);

    public static async Task VerifyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string schemaName,
        string componentName,
        IReadOnlyList<ExpectedRelation> expected,
        CancellationToken cancellationToken)
    {
        var relations = await LoadRelationsAsync(connection, transaction, schemaName, expected, cancellationToken).ConfigureAwait(false);
        var columns = await LoadTableColumnsAsync(connection, transaction, schemaName, expected, cancellationToken).ConfigureAwait(false);
        Evaluate(schemaName, componentName, expected, relations, columns);
    }

    // Both primary-key columns and index key columns come from indkey sliced to indnkeyatts:
    // indkey lists key columns FOLLOWED by INCLUDE payload columns, and a covering
    // PRIMARY KEY (…) INCLUDE (…) enforces exactly the uniqueness the stores rely on — reading
    // the whole vector would reject it for carrying its payload columns.
    internal const string RelationQuery =
        """
        SELECT c.relname,
               c.relkind::text,
               c.relpersistence::text,
               COALESCE(t.relname, ''),
               COALESCE(am.amname, ''),
               COALESCE(i.indisunique, false),
               i.indpred IS NOT NULL,
               COALESCE(i.indisvalid AND i.indisready, true),
               COALESCE((SELECT array_agg(a.attname ORDER BY k.ord)
                         FROM unnest(i.indkey[0:i.indnkeyatts-1]) WITH ORDINALITY AS k(attnum, ord)
                         JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum), '{}'),
               COALESCE(s.seqtypid::regtype::text, ''),
               COALESCE(s.seqincrement, 1),
               COALESCE(s.seqcache, 1),
               COALESCE(s.seqcycle, false),
               COALESCE(s.seqmax, 9223372036854775807),
               COALESCE((SELECT array_agg(a2.attname ORDER BY k2.ord)
                         FROM pg_index pi
                         CROSS JOIN LATERAL unnest(pi.indkey[0:pi.indnkeyatts-1]) WITH ORDINALITY AS k2(attnum, ord)
                         JOIN pg_attribute a2 ON a2.attrelid = c.oid AND a2.attnum = k2.attnum
                         WHERE pi.indrelid = c.oid AND pi.indisprimary), '{}')
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_index i ON i.indexrelid = c.oid
        LEFT JOIN pg_class t ON t.oid = i.indrelid
        LEFT JOIN pg_am am ON am.oid = c.relam
        LEFT JOIN pg_sequence s ON s.seqrelid = c.oid
        WHERE n.nspname = @schema AND c.relname = ANY(@names);
        """;

    private static async Task<Dictionary<string, ActualRelation>> LoadRelationsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string schemaName,
        IReadOnlyList<ExpectedRelation> expected,
        CancellationToken cancellationToken)
    {
        await using var verify = connection.CreateCommand();
        verify.Transaction = transaction;
        verify.CommandText = RelationQuery;
        verify.Parameters.AddWithValue("schema", schemaName);
        verify.Parameters.AddWithValue("names", expected.Select(e => e.Name).ToArray());

        var actual = new Dictionary<string, ActualRelation>(StringComparer.Ordinal);
        await using var reader = await verify.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            actual[reader.GetString(0)] = new ActualRelation(
                Kind: reader.GetString(1),
                Persistence: reader.GetString(2),
                OwningTable: reader.GetString(3),
                AccessMethod: reader.GetString(4),
                IsUnique: reader.GetBoolean(5),
                HasPredicate: reader.GetBoolean(6),
                IsValidAndReady: reader.GetBoolean(7),
                KeyColumns: reader.GetFieldValue<string[]>(8),
                SequenceType: reader.GetString(9),
                SequenceIncrement: reader.GetInt64(10),
                SequenceCache: reader.GetInt64(11),
                SequenceCycles: reader.GetBoolean(12),
                SequenceMax: reader.GetInt64(13),
                PrimaryKey: reader.GetFieldValue<string[]>(14));
        }

        return actual;
    }

    // The writable column is "writable without being named": a default (pg_attrdef also carries
    // stored generation expressions), an identity, or a generated column. Identity columns have
    // NO pg_attrdef row, so testing the rendered default alone would misread
    // GENERATED ... AS IDENTITY — which PostgreSQL populates on every insert — as unwritable.
    // format_type renders no collation, so the last two columns join it in separately;
    // attcollation is 0 for a type that cannot carry one, which the LEFT JOIN resolves to the
    // deterministic default rather than a missing row that would fail every flagged column.
    internal const string TableColumnQuery =
        """
        SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
               COALESCE(pg_get_expr(ad.adbin, ad.adrelid), ''),
               ad.adrelid IS NOT NULL OR a.attidentity <> '' OR a.attgenerated <> '',
               COALESCE(co.collname, ''),
               COALESCE(co.collisdeterministic, true)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        LEFT JOIN pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
        LEFT JOIN pg_collation co ON co.oid = a.attcollation
        WHERE n.nspname = @schema AND c.relname = ANY(@names);
        """;

    private static async Task<Dictionary<(string Table, string Column), ActualColumn>> LoadTableColumnsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string schemaName,
        IReadOnlyList<ExpectedRelation> expected,
        CancellationToken cancellationToken)
    {
        var actual = new Dictionary<(string Table, string Column), ActualColumn>();
        var tables = expected.Where(e => e.Kind == 'r' && e.Columns is not null).ToArray();
        if (tables.Length == 0)
            return actual;

        await using var verify = connection.CreateCommand();
        verify.Transaction = transaction;
        verify.CommandText = TableColumnQuery;
        verify.Parameters.AddWithValue("schema", schemaName);
        verify.Parameters.AddWithValue("names", tables.Select(t => t.Name).ToArray());

        await using var reader = await verify.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            actual[(reader.GetString(0), reader.GetString(1))] = new ActualColumn(
                Type: reader.GetString(2),
                NotNull: reader.GetBoolean(3),
                Default: reader.GetString(4),
                Writable: reader.GetBoolean(5),
                Collation: reader.GetString(6),
                DeterministicCollation: reader.GetBoolean(7));
        }

        return actual;
    }

    internal static void Evaluate(
        string schemaName,
        string componentName,
        IReadOnlyList<ExpectedRelation> expected,
        Dictionary<string, ActualRelation> relations,
        Dictionary<(string Table, string Column), ActualColumn> columns)
    {
        // Diagnose in CAUSE order, not declaration order. A misprovisioned or colliding schema is
        // usually wrong in several ways at once — a foreign relation occupying one name is WHY a
        // dependent object was never created, and an operator who mis-shaped a table has typically
        // also forgotten an index — so reporting "does not exist" first would name a victim and
        // hide the culprit. Anything that is present and wrong is checked first; absence is only
        // reported once nothing present explains it.
        foreach (var relation in expected)
        {
            if (!relations.TryGetValue(relation.Name, out var found))
                continue;

            if (found.Kind != relation.Kind.ToString()
                || (relation.OwningTable is not null && !string.Equals(found.OwningTable, relation.OwningTable, StringComparison.Ordinal)))
            {
                var expectedDescription = relation.OwningTable is null
                    ? DescribeKind(relation.Kind.ToString())
                    : $"an index on '{relation.OwningTable}'";
                var actualDescription = found.OwningTable.Length == 0
                    ? DescribeKind(found.Kind)
                    : $"an index on '{found.OwningTable}'";
                throw new InvalidOperationException(
                    $"The PostgreSQL {componentName} store expected '{schemaName}.{relation.Name}' to be {expectedDescription}, " +
                    $"but the name is occupied by {actualDescription}, so CREATE ... IF NOT EXISTS silently skipped creating it. " +
                    CollisionGuidance);
            }

            if (found.Persistence != "p")
                throw new InvalidOperationException(
                    $"The PostgreSQL {componentName} store's relation '{schemaName}.{relation.Name}' exists but is not a permanent relation " +
                    "(UNLOGGED or temporary): its content would not survive a crash or session end. Drop or convert it and restart.");

            if (relation.Kind == 'S')
                VerifySequence(schemaName, componentName, relation.Name, found);

            if (relation.Kind == 'i' && relation.KeyColumns is { } keyColumns)
            {
                if (!found.IsValidAndReady)
                    throw new InvalidOperationException(
                        $"The PostgreSQL {componentName} store's index '{schemaName}.{relation.Name}' exists but is invalid or not ready " +
                        "(a failed CREATE INDEX CONCURRENTLY leaves such an index behind). Drop it and restart so the store can recreate it.");

                if (found.IsUnique || found.HasPredicate || found.AccessMethod != "btree" || !found.KeyColumns.AsSpan().SequenceEqual(keyColumns))
                    throw new InvalidOperationException(
                        $"The PostgreSQL {componentName} store's index '{schemaName}.{relation.Name}' exists but does not match the " +
                        $"expected definition: expected a plain btree over ({string.Join(", ", keyColumns)}); found " +
                        $"{(found.IsUnique ? "a UNIQUE " : "a ")}{(found.HasPredicate ? "partial " : "")}{found.AccessMethod} index over " +
                        $"({string.Join(", ", found.KeyColumns)}). CREATE INDEX IF NOT EXISTS accepts ANY existing index with the name and " +
                        "guarantees nothing about its shape — drop or rename the existing index so the store can create the correct one.");
            }

            if (relation.Kind == 'r' && relation.PrimaryKey is { } primaryKey && !found.PrimaryKey.AsSpan().SequenceEqual(primaryKey))
                throw new InvalidOperationException(
                    $"The PostgreSQL {componentName} store's table '{schemaName}.{relation.Name}' exists but its primary key is " +
                    $"({string.Join(", ", found.PrimaryKey)}) instead of ({string.Join(", ", primaryKey)}). " + CollisionGuidance);
        }

        EvaluateTableColumns(schemaName, componentName, expected, relations, columns);

        foreach (var relation in expected)
        {
            if (!relations.ContainsKey(relation.Name))
                throw new InvalidOperationException(
                    $"The PostgreSQL {componentName} store expected '{schemaName}.{relation.Name}' to exist after schema creation, " +
                    "but it does not. " + CollisionGuidance);
        }
    }

    /// <summary>
    /// Column-level table verification: a same-kind table occupying the name — another
    /// component's, or a crafted one that happens to satisfy the index DDL — passes the relation
    /// check and fails only at the first INSERT/SELECT, or worse, silently changes runtime
    /// behavior. Every DDL-declared column must exist with the declared type and nullability;
    /// columns the DDL gives runtime-relied defaults must carry EXACTLY that default expression
    /// (a same-named default computing something else shifts every timestamp the store
    /// compares); identity columns must carry a deterministic collation (the SQL Server sibling's
    /// binary-collation rule, in the form PostgreSQL expresses it); and extra columns are allowed
    /// only when they are writable without being named
    /// (nullable, defaulted, identity, or generated) — an extra NOT NULL column the database
    /// cannot fill in itself fails every normal insert with 23502.
    /// </summary>
    private static void EvaluateTableColumns(
        string schemaName,
        string componentName,
        IReadOnlyList<ExpectedRelation> expected,
        Dictionary<string, ActualRelation> relations,
        Dictionary<(string Table, string Column), ActualColumn> columns)
    {
        foreach (var table in expected)
        {
            if (table.Kind != 'r' || table.Columns is null || !relations.ContainsKey(table.Name))
                continue;

            foreach (var column in table.Columns)
            {
                if (!columns.TryGetValue((table.Name, column.Name), out var found))
                    throw new InvalidOperationException(
                        $"The PostgreSQL {componentName} store's table '{schemaName}.{table.Name}' exists but is missing the column " +
                        $"'{column.Name}' ({column.Type}); a same-name table from another component or a partial manual creation " +
                        "occupies the name. " + CollisionGuidance);

                if (!string.Equals(found.Type, column.Type, StringComparison.Ordinal)
                    || found.NotNull == column.Nullable
                    || (column.DefaultExpression is not null && !string.Equals(found.Default, column.DefaultExpression, StringComparison.Ordinal)))
                {
                    throw new InvalidOperationException(
                        $"The PostgreSQL {componentName} store's table '{schemaName}.{table.Name}' exists but column '{column.Name}' " +
                        $"does not match the expected shape: expected {column.Type}{(column.Nullable ? " NULL" : " NOT NULL")}" +
                        $"{(column.DefaultExpression is null ? "" : $" DEFAULT {column.DefaultExpression}")}; found {found.Type}" +
                        $"{(found.NotNull ? " NOT NULL" : " NULL")}{(found.Default.Length == 0 ? " without a default" : $" DEFAULT {found.Default}")}. " +
                        CollisionGuidance);
                }

                if (column.RequiresDeterministicCollation && !found.DeterministicCollation)
                    throw new InvalidOperationException(
                        $"The PostgreSQL {componentName} store's column '{schemaName}.{table.Name}.{column.Name}' uses the " +
                        $"collation '{found.Collation}', which is non-deterministic (collisdeterministic = false). That column " +
                        "stores an identity the library compares ORDINALLY, and a non-deterministic collation folds whatever its " +
                        "rules call equal — case, accents, or full-width forms, depending on the ICU rule — into one key. Distinct " +
                        "ids would then collide: lookups cross-match and the second id is rejected on insert. ALTER the column to " +
                        "a deterministic collation (\"C\" compares by code point) after dropping the keys and indexes that " +
                        "reference it.");
            }

            // Extra columns are fine only when inserts that do not name them can still succeed.
            var expectedNames = table.Columns.Select(c => c.Name).ToHashSet(StringComparer.Ordinal);
            foreach (var ((tableName, columnName), found) in columns)
            {
                if (!string.Equals(tableName, table.Name, StringComparison.Ordinal) || expectedNames.Contains(columnName))
                    continue;

                if (found.NotNull && !found.Writable)
                    throw new InvalidOperationException(
                        $"The PostgreSQL {componentName} store's table '{schemaName}.{table.Name}' has an extra column " +
                        $"'{columnName}' that is NOT NULL without a default: every insert the store issues would fail with " +
                        "not_null_violation (23502). Make the column nullable, give it a default, or drop it.");
            }
        }
    }

    /// <summary>
    /// The ack sequence is a cross-process monotonic clock: delivery claims and waiter
    /// registrations draw from it and compare positions, so any property that lets drawn values
    /// go backwards or repeat silently corrupts the same-tick tie-breaker — a descending
    /// increment counts down, CYCLE wraps, and CACHE &gt; 1 hands each session a private block
    /// so cross-session draw order no longer matches value order.
    /// </summary>
    private static void VerifySequence(string schemaName, string componentName, string name, ActualRelation found)
    {
        if (found.SequenceType != "bigint"
            || found.SequenceIncrement != 1
            || found.SequenceCache != 1
            || found.SequenceCycles
            || found.SequenceMax != long.MaxValue)
        {
            throw new InvalidOperationException(
                $"The PostgreSQL {componentName} store's sequence '{schemaName}.{name}' exists but does not behave as the required " +
                $"cross-process monotonic clock: expected bigint, INCREMENT 1, CACHE 1, NO CYCLE, MAXVALUE {long.MaxValue}; found " +
                $"{found.SequenceType}, INCREMENT {found.SequenceIncrement}, CACHE {found.SequenceCache}, " +
                $"{(found.SequenceCycles ? "CYCLE" : "NO CYCLE")}, MAXVALUE {found.SequenceMax}. Fix it with " +
                $"ALTER SEQUENCE \"{schemaName}\".\"{name}\" AS bigint INCREMENT 1 CACHE 1 NO CYCLE NO MAXVALUE; and restart.");
        }
    }

    /// <summary>
    /// Guidance appended when the schema-object DDL itself fails on an occupied name
    /// (SQLSTATE 42809 wrong object type, or 42703 undefined column when a same-kind foreign
    /// table made IF NOT EXISTS skip the create and the dependent index DDL then referenced a
    /// column that table does not have).
    /// </summary>
    public static string DdlCollisionMessage(string componentName, string schemaName)
        => $"The PostgreSQL {componentName} store could not create its schema objects in '{schemaName}' because a configured or " +
           "derived name is occupied by an object of a different kind or shape. " + CollisionGuidance;

    private const string CollisionGuidance =
        "Tables, indexes, and sequences share one namespace per schema — across the channel, transport, and durable-flow " +
        "stores and any unrelated objects in it. Rename the configured tables so every configured and derived name stays " +
        "unique within the schema.";

    private static string DescribeKind(string kind) => kind switch
    {
        "r" => "a table",
        "i" => "an index",
        "S" => "a sequence",
        "v" => "a view",
        "m" => "a materialized view",
        _ => $"a relation of kind '{kind}'",
    };

    internal readonly record struct ActualRelation(
        string Kind,
        string Persistence,
        string OwningTable,
        string AccessMethod,
        bool IsUnique,
        bool HasPredicate,
        bool IsValidAndReady,
        string[] KeyColumns,
        string SequenceType,
        long SequenceIncrement,
        long SequenceCache,
        bool SequenceCycles,
        long SequenceMax,
        string[] PrimaryKey);

    internal readonly record struct ActualColumn(
        string Type,
        bool NotNull,
        string Default,
        bool Writable,
        string Collation,
        bool DeterministicCollation);
}
