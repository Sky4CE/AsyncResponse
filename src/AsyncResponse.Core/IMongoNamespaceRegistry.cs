namespace AsyncResponse.Internal;

/// <summary>
/// Container-scoped ownership-claim seam for MongoDB collections, implemented by the MongoDB
/// packages' shared <c>MongoNamespaceRegistry</c> (no <c>MongoDB.Driver</c> dependency here, so
/// Core stays provider-neutral: <see cref="Claim"/> takes a pre-computed cluster key rather than
/// an <c>IMongoDatabase</c>).
/// </summary>
/// <remarks>
/// Declared in Core — not in a MongoDB package — purely so every MongoDB-package store resolves
/// the SAME singleton instance regardless of which package's DI registration runs first. A
/// per-package concrete type compiles into that package's own assembly; two packages sharing one
/// container would then each register an independent instance under <c>TryAddSingleton</c>
/// (keyed by the concrete type), silently splitting one registry into several and defeating
/// cross-component collision detection. Resolving through this shared interface type instead
/// keeps one instance no matter which package registers first.
/// </remarks>
internal interface IMongoNamespaceRegistry
{
    /// <summary>
    /// Claims <paramref name="collections"/> for <paramref name="componentName"/> within the
    /// database identified by <paramref name="clusterKey"/> + <paramref name="databaseName"/>.
    /// Re-claims by the same component are idempotent (a store constructed twice claims twice);
    /// a claim held by a DIFFERENT component throws.
    /// </summary>
    void Claim(
        string clusterKey,
        string databaseName,
        string componentName,
        IReadOnlyList<(string Collection, string Purpose)> collections);
}
