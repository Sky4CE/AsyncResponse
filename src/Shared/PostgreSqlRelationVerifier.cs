using Npgsql;

namespace AsyncResponse.Internal;

/// <summary>
/// In-transaction catalog verification that every relation a PostgreSQL store just ensured
/// actually IS what its DDL intended. <c>CREATE ... IF NOT EXISTS</c> matches ANY relation with
/// the name (tables, indexes, and sequences share one namespace per schema) and explicitly
/// guarantees nothing about an existing object's shape — a same-name index with different key
/// columns, uniqueness, a predicate, or another access method is silently accepted. Runs under
/// the schema-keyed advisory DDL lock shared by every AsyncResponse PostgreSQL store, so other
/// components' objects are either committed and visible or serialized behind this transaction.
/// Source-linked into the channel, transport, and durable-flow packages (separate packages
/// cannot share compiled code).
/// </summary>
internal static class PostgreSqlRelationVerifier
{
    /// <summary>
    /// One expected relation: kind 'r' (table), 'S' (sequence, verified <c>bigint</c>), or 'i'
    /// (index, verified to sit on <paramref name="OwningTable"/> as a plain — non-unique,
    /// non-partial, valid and ready — btree over exactly <paramref name="KeyColumns"/> in order).
    /// </summary>
    internal readonly record struct ExpectedRelation(string Name, char Kind, string? OwningTable = null, string[]? KeyColumns = null);

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
                   COALESCE(t.relname, ''),
                   COALESCE(am.amname, ''),
                   COALESCE(i.indisunique, false),
                   i.indpred IS NOT NULL,
                   COALESCE(i.indisvalid AND i.indisready, true),
                   COALESCE((SELECT array_agg(a.attname ORDER BY k.ord)
                             FROM unnest(i.indkey[0:i.indnkeyatts-1]) WITH ORDINALITY AS k(attnum, ord)
                             JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum), '{}'),
                   COALESCE(s.seqtypid::regtype::text, '')
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
                    OwningTable: reader.GetString(2),
                    AccessMethod: reader.GetString(3),
                    IsUnique: reader.GetBoolean(4),
                    HasPredicate: reader.GetBoolean(5),
                    IsValidAndReady: reader.GetBoolean(6),
                    KeyColumns: reader.GetFieldValue<string[]>(7),
                    SequenceType: reader.GetString(8));
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

            if (relation.Kind == 'S' && found.SequenceType != "bigint")
                throw new InvalidOperationException(
                    $"The PostgreSQL {componentName} store expected sequence '{schemaName}.{relation.Name}' to be bigint, but it is " +
                    $"'{found.SequenceType}' — a pre-existing sequence of a smaller type would eventually overflow. Drop or migrate it " +
                    "(ALTER SEQUENCE ... AS bigint) and restart.");

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
        }
    }

    /// <summary>
    /// Guidance appended when the schema-object DDL itself fails on a wrong relation kind
    /// (SQLSTATE 42809) — e.g. CREATE INDEX ... ON a name that is really another component's
    /// index, where IF NOT EXISTS skipped the table create and the dependent statement then hit
    /// the wrong relation kind mid-batch.
    /// </summary>
    public static string DdlCollisionMessage(string componentName, string schemaName)
        => $"The PostgreSQL {componentName} store could not create its schema objects in '{schemaName}' because a configured or " +
           "derived name is occupied by a different kind of relation. " + CollisionGuidance;

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
        string OwningTable,
        string AccessMethod,
        bool IsUnique,
        bool HasPredicate,
        bool IsValidAndReady,
        string[] KeyColumns,
        string SequenceType);
}
