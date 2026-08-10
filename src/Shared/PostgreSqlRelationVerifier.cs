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
    /// <summary>One expected table column: name, <c>format_type</c> rendering, nullability, and whether the DDL declares a default.</summary>
    internal readonly record struct ExpectedColumn(string Name, string Type, bool Nullable, bool HasDefault = false);

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
        NpgsqlTransaction transaction,
        string schemaName,
        string componentName,
        IReadOnlyList<ExpectedRelation> expected,
        CancellationToken cancellationToken)
    {
        await using var verify = connection.CreateCommand();
        verify.Transaction = transaction;
        verify.CommandText =
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
                             CROSS JOIN LATERAL unnest(pi.indkey) WITH ORDINALITY AS k2(attnum, ord)
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
        verify.Parameters.AddWithValue("schema", schemaName);
        verify.Parameters.AddWithValue("names", expected.Select(e => e.Name).ToArray());

        var actual = new Dictionary<string, ActualRelation>(StringComparer.Ordinal);
        await using (var reader = await verify.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
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
        }

        foreach (var relation in expected)
        {
            if (!actual.TryGetValue(relation.Name, out var found))
                throw new InvalidOperationException(
                    $"The PostgreSQL {componentName} store expected '{schemaName}.{relation.Name}' to exist after schema creation, but it does not.");

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

        await VerifyTableColumnsAsync(connection, transaction, schemaName, componentName, expected, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Column-level table verification: a same-kind table occupying the name — another
    /// component's, or a crafted one that happens to satisfy the index DDL — passes the relation
    /// check and fails only at the first INSERT/SELECT. Every DDL-declared column must exist with
    /// the declared type and nullability, and columns the DDL gives defaults must have one
    /// (inserts rely on them). Extra user-added columns are allowed.
    /// </summary>
    private static async Task VerifyTableColumnsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string schemaName,
        string componentName,
        IReadOnlyList<ExpectedRelation> expected,
        CancellationToken cancellationToken)
    {
        var tables = expected.Where(e => e.Kind == 'r' && e.Columns is not null).ToArray();
        if (tables.Length == 0)
            return;

        await using var verify = connection.CreateCommand();
        verify.Transaction = transaction;
        verify.CommandText =
            """
            SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull, a.atthasdef
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
            WHERE n.nspname = @schema AND c.relname = ANY(@names);
            """;
        verify.Parameters.AddWithValue("schema", schemaName);
        verify.Parameters.AddWithValue("names", tables.Select(t => t.Name).ToArray());

        var actual = new Dictionary<(string Table, string Column), (string Type, bool NotNull, bool HasDefault)>();
        await using (var reader = await verify.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                actual[(reader.GetString(0), reader.GetString(1))] = (reader.GetString(2), reader.GetBoolean(3), reader.GetBoolean(4));
        }

        foreach (var table in tables)
        {
            foreach (var column in table.Columns!)
            {
                if (!actual.TryGetValue((table.Name, column.Name), out var found))
                    throw new InvalidOperationException(
                        $"The PostgreSQL {componentName} store's table '{schemaName}.{table.Name}' exists but is missing the column " +
                        $"'{column.Name}' ({column.Type}); a same-name table from another component or a partial manual creation " +
                        "occupies the name. " + CollisionGuidance);

                if (!string.Equals(found.Type, column.Type, StringComparison.Ordinal)
                    || found.NotNull == column.Nullable
                    || (column.HasDefault && !found.HasDefault))
                {
                    throw new InvalidOperationException(
                        $"The PostgreSQL {componentName} store's table '{schemaName}.{table.Name}' exists but column '{column.Name}' " +
                        $"does not match the expected shape: expected {column.Type}{(column.Nullable ? " NULL" : " NOT NULL")}" +
                        $"{(column.HasDefault ? " with a default" : "")}; found {found.Type}{(found.NotNull ? " NOT NULL" : " NULL")}" +
                        $"{(found.HasDefault ? " with a default" : " without a default")}. " + CollisionGuidance);
                }
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

    private readonly record struct ActualRelation(
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
}
