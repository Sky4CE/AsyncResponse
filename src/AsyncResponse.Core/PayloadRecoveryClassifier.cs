using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace AsyncResponse;

/// <summary>
/// Outcome of classifying a lost-subscriber response payload.
/// </summary>
/// <param name="Action">
/// The recovery route the payload chose (<see cref="IAsyncResponsePayload.OnRecovery"/>),
/// or <c>null</c> when it could not be classified — a <c>null</c> payload, missing/unresolvable
/// type information, or a conversion failure. Callers must treat <c>null</c> conservatively as
/// "do not resume", so a payload that cannot be understood never takes the happy path.
/// </param>
/// <param name="MaterializedPayload">
/// The payload as an <see cref="IAsyncResponsePayload"/> instance of the REGISTERED type,
/// materialized from the payload's wire representation — never the publisher's live instance, so
/// the verdict (and what callbacks see) can never depend on a serialization boundary.
/// <c>null</c> exactly when <paramref name="Action"/> is <c>null</c>. Callbacks must receive THIS
/// instance, never the raw JSON: an <c>object</c>-/interface-/base-typed callback parameter
/// otherwise gets a <see cref="JsonElement"/> and every type guard in the consuming flow silently
/// fails (the 292332 flow-deadlock incident).
/// </param>
internal readonly record struct RecoveryClassification(RecoveryAction? Action, object? MaterializedPayload);

/// <summary>
/// Resolves the lost-subscriber recovery route for a response payload by materializing its wire
/// representation as the registered payload type and asking
/// <see cref="IAsyncResponsePayload.OnRecovery"/>.
/// <para>
/// The input is always a wire representation — raw broker JSON, or a typed publish normalized to
/// its declared-type serialization by the dispatcher — so in-process and broker deliveries of the
/// same response classify identically. The materialized instance is returned so the chosen
/// callback receives it instead of the raw JSON.
/// </para>
/// </summary>
internal static class PayloadRecoveryClassifier
{
    // Entries carry the resolver-registry generation observed before the scan that produced them,
    // mirroring the negative cache's stamp: a plain clear-on-unregister has a race — an in-flight
    // resolution that got its answer from the departing resolver can insert AFTER the clear,
    // permanently re-poisoning the name with the revoked type. A stale stamp makes the entry a
    // non-hit, so the next lookup rescans against the current resolver set.
    private static readonly ConcurrentDictionary<string, (Type Type, int Generation)> PayloadTypes = new(StringComparer.Ordinal);
    private static int _resolvedPayloadTypeGeneration;

    /// <summary>
    /// Drops resolved payload types when a type resolver is unregistered. Counterpart to
    /// <see cref="ReflectionExtensions.InvalidateResolvedServiceTypes"/>: a payload name the
    /// departing resolver already answered would otherwise keep materializing into its old type,
    /// and keep that type's assembly reachable through this cache.
    /// </summary>
    internal static void InvalidateResolvedPayloadTypes()
    {
        // Bump BEFORE clearing: the bump is what fences in-flight scans (their pre-scan stamp goes
        // stale); the clear just reclaims memory.
        Interlocked.Increment(ref _resolvedPayloadTypeGeneration);
        PayloadTypes.Clear();
    }

    /// <summary>
    /// Attempts to classify <paramref name="payload"/> for the lost-subscriber path: which route it
    /// takes, and the materialized instance the route's callback must receive.
    /// </summary>
    /// <param name="payload">
    /// The payload as received by <c>SetResponse</c>: either an already-typed
    /// <see cref="IAsyncResponsePayload"/>, or raw JSON (<see cref="JsonElement"/> / JSON string)
    /// when the response came through a broker ingress.
    /// </param>
    /// <param name="payloadTypeFullName">
    /// Full name of the payload type the waiter subscribed for, from the recovery state.
    /// </param>
    public static RecoveryClassification Classify(object? payload, string? payloadTypeFullName)
    {
        try
        {
            if (payload is null || string.IsNullOrWhiteSpace(payloadTypeFullName))
            {
                return new RecoveryClassification(null, null);
            }

            // Wire-only: classification never consults a live CLR instance. A non-JSON payload
            // here means no wire representation exists for it (the dispatcher's serialization
            // failed) — conservatively unclassifiable rather than letting in-process state that
            // never crosses the wire decide the route.
            if (payload is not (JsonElement or string))
            {
                return new RecoveryClassification(null, null);
            }

            // The REGISTRATION's payload type governs, never the publisher's runtime type:
            // multiple registrations may share one correlation id with different payload types
            // (shared-correlation recovery), and each must be classified as the type IT
            // registered. The payload arrives as its WIRE representation (the dispatcher
            // normalizes typed publishes to their declared-type serialization), so materializing
            // it here yields exactly what a broker delivery would have produced — polymorphic
            // discriminators included, in-process-only ([JsonIgnore]) state excluded.
            var registeredType = ResolvePayloadType(payloadTypeFullName!);
            if (registeredType is null || !typeof(IAsyncResponsePayload).IsAssignableFrom(registeredType))
            {
                return new RecoveryClassification(null, null);
            }

            return payload.ConvertTo(registeredType) is IAsyncResponsePayload materialized
                ? new RecoveryClassification(materialized.OnRecovery(), materialized)
                : new RecoveryClassification(null, null);
        }
        catch
        {
            // A payload that cannot be materialized as the registered type (or whose classifier
            // throws) carries no usable domain state; treat it conservatively as "do not resume".
            // The failure route then carries the raw payload, exactly as before materialization
            // existed, and the diagnostics on the resolution path surface the cause.
            return new RecoveryClassification(null, null);
        }
    }

    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "The persisted payload type name comes from a recoverable-waiter registration whose payload type " +
                        "parameter is statically referenced by the registering app; an unresolvable name is answered with " +
                        "null — the conservative 'do not resume' route — plus a type-resolution-failure diagnostic.")]
    internal static Type? ResolvePayloadType(string payloadTypeFullName)
    {
        // Must precede any cache consult/populate: a miss cached without the invalidation hook
        // active could outlive a later assembly load that makes the name resolvable.
        UnresolvableTypeNames.EnsureAssemblyLoadInvalidation();

        if (PayloadTypes.TryGetValue(payloadTypeFullName, out var cached)
            && cached.Generation == Volatile.Read(ref _resolvedPayloadTypeGeneration))
        {
            return cached.Type;
        }

        // Fail fast on a name that already failed a full scan (shared with the callback service
        // resolver): without this, every redelivery naming an unresolvable payload type (a
        // poisoned recovery row, a renamed class) re-walks every loaded assembly. Only a
        // CURRENT-generation entry counts — a stale stamp means the miss may have raced a
        // resolver registration or assembly load, so it rescans. The diagnostic still fires per
        // attempt, so a poisoned name stays visible to operators while costing a dictionary hit.
        if (UnresolvableTypeNames.IsKnownMiss(payloadTypeFullName))
        {
            AsyncResponseDiagnostics.RecordTypeResolutionFailure("payload");
            return null;
        }

        var generationBeforeScan = UnresolvableTypeNames.GenerationBeforeScan();
        var resolvedGenerationBeforeScan = Volatile.Read(ref _resolvedPayloadTypeGeneration);

        // Loaded assemblies only — every component of the name, generic arguments included (see
        // ResolveLoaded): a persisted name must never make the process load an assembly.
        var resolved = AsyncResponseTypeResolution.ResolveLoaded(payloadTypeFullName);

        // Opt-in fallback for payload types loaded into a non-default AssemblyLoadContext (plugins).
        resolved ??= AsyncResponseTypeResolution.Resolve(payloadTypeFullName);

        if (resolved is not null)
        {
            // Collectible (plugin) payload types stay resolve-per-call: a strong process-wide
            // cache entry would pin the plugin's AssemblyLoadContext after unload. Indexer, not
            // TryAdd, so a stale-stamped survivor of a raced unregister is replaced.
            if (!resolved.Assembly.IsCollectible)
                PayloadTypes[payloadTypeFullName] = (resolved, resolvedGenerationBeforeScan);
        }
        else
        {
            UnresolvableTypeNames.RecordMiss(payloadTypeFullName, generationBeforeScan);

            // Surface the silent "couldn't materialize the payload type" path so operators can
            // correlate a recovery that routed to failure with a missing/ALC-loaded type.
            AsyncResponseDiagnostics.RecordTypeResolutionFailure("payload");
        }

        return resolved;
    }
}
