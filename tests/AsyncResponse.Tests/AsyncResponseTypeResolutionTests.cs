using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Collection-serialized with <c>TypeResolutionTests</c> (CallbackSecurityTests.cs): both classes
/// mutate the process-global resolver registry, and this class's per-test <c>Reset()</c> wiped the
/// other's just-registered resolver under parallel execution.
/// </summary>
[Collection("AsyncResponseTypeResolutionRegistry")]
public class AsyncResponseTypeResolutionTests : IDisposable
{
    public void Dispose() => AsyncResponseTypeResolution.Reset();

    [Fact]
    public void Resolve_SkipsThrowingResolverAndUsesNextMatch()
    {
        AsyncResponseTypeResolution.RegisterResolver(_ => throw new InvalidOperationException("bad resolver"));
        AsyncResponseTypeResolution.RegisterResolver(name => name == typeof(OperationResult).FullName ? typeof(OperationResult) : null);

        Assert.Equal(typeof(OperationResult), AsyncResponseTypeResolution.Resolve(typeof(OperationResult).FullName!));
        Assert.Null(AsyncResponseTypeResolution.Resolve("missing.Type"));
    }

    [Fact]
    public void RegisterAssembly_ResolvesTypesFromAssembly()
    {
        AsyncResponseTypeResolution.RegisterAssembly(Assembly.GetExecutingAssembly());

        Assert.Equal(typeof(OperationResult), AsyncResponseTypeResolution.Resolve(typeof(OperationResult).FullName!));
    }

    [Fact]
    public void Register_RejectsNulls()
    {
        Assert.Throws<ArgumentNullException>(() => AsyncResponseTypeResolution.RegisterResolver(null!));
        Assert.Throws<ArgumentNullException>(() => AsyncResponseTypeResolution.RegisterAssembly(null!));
    }

    [Fact]
    public void ResolveServiceType_CachesUnresolvableNames_ConsultsResolversOnce()
    {
        // Without the negative cache, every attempt on an unresolvable name (a poisoned recovery
        // row, a renamed type) re-walks every loaded assembly and the resolver chain. An AMBIENT
        // assembly load between the two lookups legitimately invalidates the cache (that is the
        // product behavior, and lazy loads do happen mid-run), so assert the steady state: within
        // a few attempts, a scanned miss must be served from the cache on the immediate retry.
        for (var attempt = 0; ; attempt++)
        {
            var name = $"Missing.Namespace.Type{Guid.NewGuid():N}";
            var probes = 0;
            AsyncResponseTypeResolution.RegisterResolver(candidate =>
            {
                if (candidate == name)
                    Interlocked.Increment(ref probes);
                return null;
            });

            Assert.Null(ReflectionExtensions.ResolveServiceType(name));
            Assert.Null(ReflectionExtensions.ResolveServiceType(name));

            if (Volatile.Read(ref probes) == 1)
                return;

            Assert.True(attempt < 4, $"Negative cache never held across two lookups ({probes} probes on final attempt).");
        }
    }

    [Fact]
    public void RegisterResolver_InvalidatesCachedMisses()
    {
        // A plugin registering its resolver after a name already missed must not stay blacklisted.
        var name = $"Missing.Namespace.Type{Guid.NewGuid():N}";
        Assert.Null(ReflectionExtensions.ResolveServiceType(name));

        AsyncResponseTypeResolution.RegisterResolver(candidate => candidate == name ? typeof(OperationResult) : null);

        Assert.Equal(typeof(OperationResult), ReflectionExtensions.ResolveServiceType(name));
    }

    [Fact]
    public async Task RegisterResolver_DuringInFlightMiss_DoesNotPoisonNegativeCache()
    {
        // The race: a lookup starts against the old resolver set and blocks mid-scan; a resolver
        // that CAN resolve the name registers (which invalidates the cache); the in-flight miss
        // then completes and inserts its stale verdict AFTER the invalidation. Generation-stamped
        // entries make that stale insert a non-hit, so the next lookup rescans and resolves.
        var name = $"Missing.Namespace.Type{Guid.NewGuid():N}";
        using var scanEntered = new SemaphoreSlim(0);
        using var releaseScan = new SemaphoreSlim(0);
        AsyncResponseTypeResolution.RegisterResolver(candidate =>
        {
            if (candidate != name)
                return null;
            scanEntered.Release();
            releaseScan.Wait(TimeSpan.FromSeconds(5));
            return null;
        });

        var inFlightMiss = Task.Run(() => ReflectionExtensions.ResolveServiceType(name));
        Assert.True(await scanEntered.WaitAsync(TimeSpan.FromSeconds(5)));

        AsyncResponseTypeResolution.RegisterResolver(candidate => candidate == name ? typeof(OperationResult) : null);

        releaseScan.Release();
        Assert.Null(await inFlightMiss);

        Assert.Equal(typeof(OperationResult), ReflectionExtensions.ResolveServiceType(name));
    }
}
