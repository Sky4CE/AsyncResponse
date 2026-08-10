namespace AsyncResponse;

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
internal sealed class MongoNamespaceRegistry
{
    private readonly object _gate = new();
    private readonly Dictionary<string, (string Component, string Purpose)> _claims = new(StringComparer.Ordinal);

    /// <summary>
    /// Claims <paramref name="collections"/> for <paramref name="componentName"/> within the
    /// database identified by <paramref name="clusterKey"/> + <paramref name="databaseName"/>.
    /// Re-claims by the same component are idempotent (a store constructed twice claims twice);
    /// a claim held by a DIFFERENT component throws.
    /// </summary>
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
}
