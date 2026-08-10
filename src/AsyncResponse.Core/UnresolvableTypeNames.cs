using System.Collections.Concurrent;

namespace AsyncResponse;

/// <summary>
/// Shared negative cache for persisted type names (callback service interfaces and recovery
/// payload types) that already failed a full resolution pass — the default-context assembly scan
/// plus the <see cref="AsyncResponseTypeResolution"/> resolvers. Without it, every delivery naming
/// an unresolvable type (a poisoned recovery row, a renamed class) re-walks every loaded assembly
/// on every attempt.
/// <para>
/// Capacity-bounded so hostile inputs cannot grow it without limit (at capacity, novel
/// unresolvable names simply fall back to scanning — correctness never depends on this cache),
/// and invalidated on the only events that can turn a miss into a hit: a new assembly loading, or
/// a custom resolver registering.
/// </para>
/// <para>
/// Entries are stamped with the invalidation GENERATION observed before their failed scan, not
/// just stored: a plain clear-on-register has a race — an in-flight miss that started against the
/// old resolver set can insert AFTER the clear, permanently poisoning the name. A stale stamp
/// (generation advanced mid-scan) makes the entry a non-hit, so the next lookup rescans with the
/// new resolvers.
/// </para>
/// </summary>
internal static class UnresolvableTypeNames
{
    private static readonly ConcurrentDictionary<string, int> Misses = new(StringComparer.Ordinal);
    private const int Capacity = 1024;
    private static int _generation;

    // The AssemblyLoad invalidation hook is registered on first use, NOT from a static constructor
    // and NOT from a module initializer: an explicit static ctor forfeits beforefieldinit, adding a
    // class-initialization check to every static access, and [ModuleInitializer] is analyzer-banned
    // in library code (CA2255). First-call registration costs one volatile read per type
    // resolution, off every conversion hot path.
    private static readonly object _assemblyLoadGate = new();
    private static bool _assemblyLoadHooked;

    /// <summary>
    /// Must precede any cache consult/populate: a miss cached without the invalidation hook active
    /// could outlive a later assembly load that makes the name resolvable.
    /// </summary>
    internal static void EnsureAssemblyLoadInvalidation()
    {
        if (Volatile.Read(ref _assemblyLoadHooked))
            return;

        // Attach-then-publish under a gate: publishing the flag before the handler was attached
        // let a concurrent thread proceed past the fast path, cache a miss, and race an assembly
        // load into the unhooked window — a false negative that nothing would ever invalidate.
        // The lock is cold-path only; the steady state is the single volatile read above.
        lock (_assemblyLoadGate)
        {
            if (_assemblyLoadHooked)
                return;

            AppDomain.CurrentDomain.AssemblyLoad += static (_, _) => Invalidate();
            Volatile.Write(ref _assemblyLoadHooked, true);
        }
    }

    /// <summary>
    /// Invalidates the cache (a new resolver or assembly may resolve cached misses). The
    /// generation bump is what guarantees correctness for in-flight scans; the clear just
    /// reclaims memory.
    /// </summary>
    internal static void Invalidate()
    {
        Interlocked.Increment(ref _generation);
        Misses.Clear();
    }

    /// <summary>Reads the generation to stamp on a miss recorded after an upcoming scan.</summary>
    internal static int GenerationBeforeScan() => Volatile.Read(ref _generation);

    /// <summary>
    /// Whether <paramref name="typeFullName"/> already failed a full scan in the CURRENT
    /// generation — a stale stamp means the miss may have raced a resolver registration or
    /// assembly load, so the caller rescans.
    /// </summary>
    internal static bool IsKnownMiss(string typeFullName)
        => Misses.TryGetValue(typeFullName, out var missGeneration)
           && missGeneration == Volatile.Read(ref _generation);

    /// <summary>
    /// Records a failed full scan, stamped with the generation observed BEFORE the scan: if a
    /// resolver registered while the scan ran, the stamp is already stale and the entry never
    /// blocks a re-resolve.
    /// </summary>
    internal static void RecordMiss(string typeFullName, int generationBeforeScan)
    {
        if (Misses.Count < Capacity)
            Misses[typeFullName] = generationBeforeScan;
    }
}
