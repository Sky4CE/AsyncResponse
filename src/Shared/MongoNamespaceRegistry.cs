using MongoDB.Driver;
using System.Text;

namespace AsyncResponse.Internal;

/// <summary>
/// Container-scoped ownership ledger for MongoDB collections. The channel, transport, and
/// durable-flow stores validate their own collection plans, but none can see the others' —
/// and MongoDB has no catalog "relation kind" to verify against after the fact: a durable-flow
/// store configured onto the channel's derived <c>{MessageCollection}_counters</c> collection
/// would happily create flow documents there, and its TTL index would then silently delete the
/// ack-sequence counter. Each store claims its effective collections (derived ones included) at
/// construction, keyed by cluster + database, so whichever component starts second fails with an
/// actionable error naming both claimants — in either startup order. Registered per container
/// (no static state), so independent hosts and test fixtures never see each other.
/// </summary>
/// <remarks>
/// Source-linked into the channel, transport, and durable-flow packages (matching
/// <c>MongoOwnershipLedger</c>), but the <see cref="IMongoNamespaceRegistry"/> seam it implements
/// lives in Core: registering <em>this</em> type directly under <c>TryAddSingleton</c> would key
/// the DI container on a per-package-compiled type, so two MongoDB packages sharing one container
/// would each install their own instance instead of sharing one — silently splitting the registry
/// and defeating cross-component collision detection. Resolving through the Core-defined interface
/// keeps one shared singleton regardless of which package's registration runs first.
/// </remarks>
internal sealed class MongoNamespaceRegistry : IMongoNamespaceRegistry
{
    private readonly object _gate = new();
    private readonly Dictionary<string, (string Component, string Purpose)> _claims = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public void Claim(
        string clusterKey,
        string databaseName,
        string componentName,
        IReadOnlyList<(string Collection, string Purpose)> collections)
    {
        lock (_gate)
        {
            foreach (var (collection, purpose) in collections)
            {
                var key = $"{clusterKey}|{databaseName}|{collection}";
                if (_claims.TryGetValue(key, out var existing)
                    && !string.Equals(existing.Component, componentName, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"MongoDB collection '{databaseName}.{collection}' is used by both the {existing.Component} " +
                        $"({existing.Purpose}) and the {componentName} ({purpose}). Components sharing a database must use " +
                        "distinct collections — including derived ones such as the channel's '{MessageCollection}_counters' " +
                        "ack-sequence counter, whose documents another component's TTL index would silently delete. " +
                        "Rename one of the configured collection names.");
                }

                _claims[key] = (componentName, purpose);
            }
        }
    }

    /// <summary>
    /// Stable identity of the cluster a database handle points at, for cross-component
    /// collection-ownership claims: same servers + same database name = same namespace space.
    /// The one implementation every store's ownership claim calls, so a derivation drift (SRV
    /// seedlist normalization, <c>DirectConnection</c>, host casing) can no longer desync the
    /// keys and silently turn collision detection into a no-op.
    /// </summary>
    internal static string ClusterKey(IMongoDatabase database)
        => string.Join(",", database.Client.Settings.Servers.Select(static s => s.ToString()).OrderBy(static s => s, StringComparer.Ordinal));

    /// <summary>MongoDB's SHARDED namespace byte limit; see <see cref="ValidateEffectiveNamespace"/>.</summary>
    internal const int ShardedNamespaceByteLimit = 235;

    /// <summary>
    /// Validates an effective namespace ("database.collection") against MongoDB's 235-byte
    /// SHARDED namespace limit — tighter than the 255-byte limit on an unsharded namespace, and
    /// enforced here even while unsharded so a later <c>shardCollection</c> cannot strand an
    /// already-created collection whose namespace fit under 255 but not 235. Only the store
    /// constructor knows the actual database name, so this cannot live in options validation.
    /// </summary>
    internal static void ValidateEffectiveNamespace(IMongoDatabase database, string collectionName, string description)
    {
        var ns = $"{database.DatabaseNamespace.DatabaseName}.{collectionName}";
        var byteLength = Encoding.UTF8.GetByteCount(ns);
        if (byteLength > ShardedNamespaceByteLimit)
            throw new InvalidOperationException(
                $"The MongoDB namespace '{ns}' ({description}) is {byteLength} UTF-8 bytes; the store enforces MongoDB's SHARDED " +
                "namespace limit of 235 bytes (unsharded allows 255) so a later shard-enable cannot strand the collection. " +
                "Shorten the database or collection name.");
    }
}
