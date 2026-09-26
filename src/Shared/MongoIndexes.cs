using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace AsyncResponse.Internal;

/// <summary>
/// Index DDL for the MongoDB channel and transport stores (shared source): an index a store
/// needs that the collection already carries under ANOTHER name is accepted instead of failing
/// startup. MongoDB refuses to build the same key under a second name (85
/// <c>IndexOptionsConflict</c>; 86 <c>IndexKeySpecsConflict</c> for a same-named different key),
/// and exactly such an index is what the stores' own <c>AutoCreateIndexes = false</c> warnings
/// tell an operator to create — <c>createIndex({ expires_at: 1 }, { expireAfterSeconds: 0 })</c>
/// is named <c>expires_at_1</c>, not the store's <c>{collection}_expires_idx</c> — so switching
/// <c>AutoCreateIndexes</c> back on failed every operation with the raw command error (each one
/// runs EnsureCreated first). The durable-flow store accepts its TTL index the same way.
/// </summary>
internal static class MongoIndexes
{
    private const int IndexOptionsConflict = 85;
    private const int IndexKeySpecsConflict = 86;
    private const int IndexNotFound = 27;

    /// <summary>
    /// Creates <paramref name="model"/>'s index. On a conflict it lists the collection's indexes
    /// and accepts one under a different name that serves the same queries: the same key
    /// document (fields, order and direction), the same TTL (<c>expireAfterSeconds</c> equal to
    /// the requested one, or absent when none is requested), and nothing that narrows or
    /// constrains it (no <c>unique</c>, <c>sparse</c> or <c>partialFilterExpression</c>) or keeps the
    /// planner from using it (no <c>hidden</c>: a hidden index silently turns every claim into a
    /// collection scan). Otherwise,
    /// with <paramref name="replaceConflicting"/>, the same-named index is dropped and recreated
    /// (an earlier deployment created it with different options; a peer host dropping it first is
    /// tolerated); without it the conflict propagates.
    /// </summary>
    public static async Task CreateOrAcceptEquivalentAsync<TDocument>(
        IMongoCollection<TDocument> collection,
        CreateIndexModel<TDocument> model,
        bool replaceConflicting,
        CancellationToken cancellationToken)
    {
        var name = model.Options?.Name;
        try
        {
            await collection.Indexes.CreateOneAsync(model, cancellationToken: cancellationToken).ConfigureAwait(false);
            return;
        }
        catch (MongoCommandException ex) when (ex.Code is IndexOptionsConflict or IndexKeySpecsConflict)
        {
            List<BsonDocument> indexes;
            using (var cursor = await collection.Indexes.ListAsync(cancellationToken).ConfigureAwait(false))
                indexes = await cursor.ToListAsync(cancellationToken).ConfigureAwait(false);

            var keys = model.Keys.Render(new RenderArgs<TDocument>(BsonSerializer.LookupSerializer<TDocument>(), BsonSerializer.SerializerRegistry));
            if (indexes.Exists(index => IsEquivalentUnderAnotherName(index, name, keys, model.Options?.ExpireAfter)))
                return;

            if (!replaceConflicting || name is null)
                throw;
        }

        try
        {
            await collection.Indexes.DropOneAsync(name, cancellationToken).ConfigureAwait(false);
        }
        catch (MongoCommandException dropException) when (dropException.Code == IndexNotFound)
        {
            // A peer host in the same rolling deploy took the same branch and dropped it first.
            // Converge on the recreate below (idempotent for an identical spec).
        }

        await collection.Indexes.CreateOneAsync(model, cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    internal static bool IsEquivalentUnderAnotherName(BsonDocument index, string? name, BsonDocument keys, TimeSpan? expireAfter)
        => !(index.TryGetValue("name", out var existingName) && existingName is BsonString { Value: var text } && text == name)
           && index.TryGetValue("key", out var existingKeys)
           && existingKeys is BsonDocument existingKeyDocument
           && KeysEqual(existingKeyDocument, keys)
           && (expireAfter is { } ttl
               ? index.TryGetValue("expireAfterSeconds", out var seconds) && seconds.IsNumeric && seconds.ToDouble() == ttl.TotalSeconds
               : !index.Contains("expireAfterSeconds"))
           && !IsTrue(index, "unique")
           && !IsTrue(index, "sparse")
           && !IsTrue(index, "hidden")
           && !index.Contains("partialFilterExpression");

    /// <summary>
    /// Same fields in the same order with the same direction or type. Numbers compare by value:
    /// a shell-created index stores <c>1</c> as a double, the driver as an int.
    /// </summary>
    private static bool KeysEqual(BsonDocument existing, BsonDocument requested)
    {
        if (existing.ElementCount != requested.ElementCount)
            return false;

        for (var i = 0; i < requested.ElementCount; i++)
        {
            var left = existing.GetElement(i);
            var right = requested.GetElement(i);
            if (!string.Equals(left.Name, right.Name, StringComparison.Ordinal))
                return false;
            if (left.Value.IsNumeric && right.Value.IsNumeric
                    ? left.Value.ToDouble() != right.Value.ToDouble()
                    : !left.Value.Equals(right.Value))
                return false;
        }

        return true;
    }

    private static bool IsTrue(BsonDocument index, string option)
        => index.TryGetValue(option, out var value) && value.ToBoolean();
}
