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

    // Names MayNamePayloadType refused: an array, or an instantiation whose LOADED definition is
    // not a payload type. Both verdicts are stable for the process (a loaded definition never
    // changes; an unloaded one passes instead), so a refusal is remembered here rather than in the
    // shared negative cache (see the call site). Without it, every redelivery of a poisoned row
    // re-resolved the definition — an unqualified name walks every loaded assembly. Count-bounded,
    // cleared when full, like the resolved-type caches.
    private static readonly ConcurrentDictionary<string, byte> RefusedPayloadNames = new(StringComparer.Ordinal);

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
        // Before any cache or the parser, exactly as ResolveServiceType does and for the same
        // reason — and with more at stake here: this name is resolved BEFORE a callback is chosen,
        // so no callback authorizer ever stands between a recovery row and this line.
        if (!AsyncResponseTypeResolution.IsWithinResolutionLimits(payloadTypeFullName))
        {
            AsyncResponseDiagnostics.RecordTypeResolutionFailure("payload");
            return null;
        }

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

        // Before the scan BUILDS anything: a generic instantiation or an array is a new runtime
        // type, and the runtime keeps every constructed type for the life of the process (outside
        // the managed heap, beyond any cache bound here). The marker gate in Classify ran only
        // after the closed type existed, so a recovery-store writer naming distinct
        // instantiations of any loaded generic (Dictionary`2 over pairs of loaded types) grew the
        // process by ~3 KB per message — 40 000 names, +116 MB that no GC returned. Neither
        // shape can be a payload unless its definition is one, and that is decidable on the
        // definition alone. Not recorded as a known miss: the negative cache is shared with the
        // callback service resolver, so a row naming a legitimate generic SERVICE type here would
        // poison that name for the callbacks that use it.
        if (RefusedPayloadNames.ContainsKey(payloadTypeFullName) || !MayNamePayloadType(payloadTypeFullName))
        {
            if (RefusedPayloadNames.Count >= ReflectionExtensions.ResolvedTypeCacheCapacity)
                RefusedPayloadNames.Clear();
            RefusedPayloadNames.TryAdd(payloadTypeFullName, 0);
            AsyncResponseDiagnostics.RecordTypeResolutionFailure("payload");
            return null;
        }

        var generationBeforeScan = UnresolvableTypeNames.GenerationBeforeScan();
        var resolvedGenerationBeforeScan = Volatile.Read(ref _resolvedPayloadTypeGeneration);

        // Loaded assemblies only — every component of the name, generic arguments included (see
        // ResolveLoaded): a persisted name must never make the process load an assembly.
        var resolved = AsyncResponseTypeResolution.ResolveLoaded(payloadTypeFullName);

        // Opt-in fallback for payload types loaded into a non-default AssemblyLoadContext (plugins).
        // A resolver RegisterAssembly installed may load an assembly a generic argument names through
        // that assembly's load context (see RegisterAssembly) — the confinement above is the default
        // scan's, not the registered resolvers'.
        resolved ??= AsyncResponseTypeResolution.Resolve(payloadTypeFullName);

        if (resolved is not null)
        {
            // Collectible (plugin) payload types stay resolve-per-call: a strong process-wide
            // cache entry would pin the plugin's AssemblyLoadContext after unload. The insert
            // policy (count-bounded, clear-when-full, stale stamps replaced) is shared with the
            // callback service cache — this name is cached BEFORE the marker gate in Classify
            // runs, so without the bound every novel spelling a recovery row carried stayed.
            ReflectionExtensions.CacheResolvedType(PayloadTypes, payloadTypeFullName, resolved, resolvedGenerationBeforeScan);
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

    /// <summary>
    /// Whether resolving <paramref name="payloadTypeFullName"/> could yield a payload type —
    /// decided WITHOUT constructing the type it names. A plain name constructs nothing and passes
    /// (the marker gate in <see cref="Classify"/> still judges it). An array never implements
    /// <see cref="IAsyncResponsePayload"/>, so it is refused outright. A generic instantiation
    /// passes only when its loaded definition is a payload type (a closed type implements exactly
    /// its definition's interfaces), or when no loaded assembly defines it — then the default
    /// scan builds nothing either, and only an opt-in registered resolver can answer.
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "Same contract as ResolvePayloadType: the definition of a payload type a registering app names " +
                        "is statically referenced by it; a trimmed-away definition reads as not loaded.")]
    private static bool MayNamePayloadType(string payloadTypeFullName)
    {
        var open = payloadTypeFullName.IndexOf('[');
        if (open < 0)
            return true;

        // `[]`, `[,]`, `[*]`: an array rank directly on the named type.
        if (open + 1 >= payloadTypeFullName.Length || payloadTypeFullName[open + 1] is ']' or ',' or '*')
            return false;

        // The generic argument list: find its matching close (the name is within the resolution
        // limits by now, so this single pass is bounded).
        var depth = 0;
        var close = -1;
        for (var index = open; index < payloadTypeFullName.Length; index++)
        {
            if (payloadTypeFullName[index] == '[')
            {
                depth++;
            }
            else if (payloadTypeFullName[index] == ']' && --depth == 0)
            {
                close = index;
                break;
            }
        }

        if (close < 0)
            return false;

        // An array of the instantiation (`Box`1[[…]][]`) is no payload either; what may follow
        // is only the outer assembly qualifier, kept so the definition resolves from the same
        // assembly the full name names.
        var rest = payloadTypeFullName.AsSpan(close + 1);
        if (rest.TrimStart().StartsWith("[", StringComparison.Ordinal))
            return false;

        var definition = AsyncResponseTypeResolution.ResolveLoaded(string.Concat(payloadTypeFullName.AsSpan(0, open), rest));
        return definition is null || typeof(IAsyncResponsePayload).IsAssignableFrom(definition);
    }
}
