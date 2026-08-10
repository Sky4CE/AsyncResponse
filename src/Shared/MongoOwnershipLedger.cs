using MongoDB.Bson;
using MongoDB.Driver;

namespace AsyncResponse.Internal;

/// <summary>
/// Persisted cross-host collection-ownership ledger. The in-container
/// <c>MongoNamespaceRegistry</c> fails same-container collisions at construction, but two HOSTS
/// (or a directly constructed store) sharing a database cannot see each other's claims — and
/// MongoDB has no catalog metadata to verify after the fact: a durable-flow store configured
/// onto the channel's derived <c>{MessageCollection}_counters</c> collection happily writes flow
/// documents there, and its TTL index then silently deletes the ack-sequence counter. Each
/// store, at first use (EnsureCreated), atomically upserts one claim document per effective
/// collection into the fixed <c>asyncresponse_ownership</c> collection; a claim already held by
/// a DIFFERENT component (or the same component in a different role) fails startup with an
/// actionable error naming both claimants — whichever process starts second, whatever the
/// order. Restarts re-claim idempotently. Deployments that disable auto-creation own their
/// provisioning and skip the ledger, like the rest of the first-use DDL. Renaming a collection
/// in configuration leaves the old claim behind; the error text covers removing a stale claim
/// document deliberately. Source-linked into the channel, transport, and durable-flow packages.
/// </summary>
internal static class MongoOwnershipLedger
{
    /// <summary>Fixed ledger collection name; rejected as a configurable data collection by every store.</summary>
    public const string CollectionName = "asyncresponse_ownership";

    public static async Task ClaimAsync(
        IMongoDatabase database,
        string componentName,
        IReadOnlyList<(string Collection, string Purpose)> claims,
        CancellationToken cancellationToken)
    {
        var ledger = database.GetCollection<BsonDocument>(CollectionName);
        foreach (var (collection, purpose) in claims)
        {
            // Atomic claim: the upsert inserts our ownership document only when no document
            // exists for the collection; a concurrent claimant loses the insert and reads the
            // winner's document. Documents lacking the component field (foreign writes) are
            // tolerated rather than guessed about.
            var existing = await ledger.FindOneAndUpdateAsync<BsonDocument>(
                new BsonDocument("_id", collection),
                new BsonDocument("$setOnInsert", new BsonDocument
                {
                    { "component", componentName },
                    { "purpose", purpose }
                }),
                new FindOneAndUpdateOptions<BsonDocument, BsonDocument>
                {
                    IsUpsert = true,
                    ReturnDocument = ReturnDocument.Before
                },
                cancellationToken).ConfigureAwait(false);

            if (existing is not null
                && existing.TryGetValue("component", out var owner)
                && existing.TryGetValue("purpose", out var ownerPurpose)
                && !(owner.AsString == componentName && ownerPurpose.AsString == purpose))
            {
                throw new InvalidOperationException(
                    $"MongoDB collection '{database.DatabaseNamespace.DatabaseName}.{collection}' is already claimed by the " +
                    $"{owner.AsString} ({ownerPurpose.AsString}) in the persisted ownership ledger " +
                    $"('{CollectionName}'), and this {componentName} configured it as {purpose}. Components sharing a database " +
                    "must use distinct collections — including derived ones such as the channel's '{MessageCollection}_counters' " +
                    "ack-sequence counter, whose documents another component's TTL index would silently delete. Rename one of the " +
                    "configured collection names, or delete the stale claim document if the other component was deliberately " +
                    "reconfigured away from this collection.");
            }
        }
    }
}
