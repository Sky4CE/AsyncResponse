using Microsoft.Extensions.DependencyInjection;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
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

    [Fact]
    public async Task UnregisterResolver_DuringInFlightResolution_DoesNotRepoisonThePositiveCache()
    {
        // Regression (round 31): the mirror image of the negative-cache race below. A resolution
        // starts against the old resolver set, gets its answer from the DEPARTING resolver, and
        // blocks; the registration is disposed (which clears the positive caches); the in-flight
        // resolution then completes and re-inserts the revoked mapping AFTER the clear — served
        // for the life of the process, which is most of what disposing the handle is for.
        // Generation-stamped positive entries make that stale insert a non-hit.
        var name = $"Missing.Namespace.Type{Guid.NewGuid():N}";
        using var scanEntered = new SemaphoreSlim(0);
        using var releaseScan = new SemaphoreSlim(0);
        var registration = AsyncResponseTypeResolution.RegisterResolver(candidate =>
        {
            if (candidate != name)
                return null;
            scanEntered.Release();
            releaseScan.Wait(TimeSpan.FromSeconds(5));
            return typeof(OperationResult);
        });

        var inFlightHit = Task.Run(() => ReflectionExtensions.ResolveServiceType(name));
        Assert.True(await scanEntered.WaitAsync(TimeSpan.FromSeconds(5)));

        registration.Dispose();

        releaseScan.Release();
        // The in-flight resolution still returns the answer it already had in hand...
        Assert.Equal(typeof(OperationResult), await inFlightHit);

        // ...but its stale insert must not outlive the revocation: the next lookup rescans
        // against the current (empty) resolver set and fails to resolve.
        Assert.Null(ReflectionExtensions.ResolveServiceType(name));
    }

    [Fact]
    public void ResolvePayloadType_CachesUnresolvableNames_ConsultsResolversOnce()
    {
        // The payload classifier shares the service resolver's negative cache: a poisoned
        // recovery row's type name must stop re-walking every loaded assembly on each
        // redelivery. Same steady-state assertion as the service-side test — an ambient
        // assembly load between the lookups legitimately invalidates the cache.
        for (var attempt = 0; ; attempt++)
        {
            var name = $"Missing.Namespace.Payload{Guid.NewGuid():N}";
            var probes = 0;
            AsyncResponseTypeResolution.RegisterResolver(candidate =>
            {
                if (candidate == name)
                    Interlocked.Increment(ref probes);
                return null;
            });

            Assert.Null(PayloadRecoveryClassifier.ResolvePayloadType(name));
            Assert.Null(PayloadRecoveryClassifier.ResolvePayloadType(name));

            if (Volatile.Read(ref probes) == 1)
                return;

            Assert.True(attempt < 4, $"Negative cache never held across two lookups ({probes} probes on final attempt).");
        }
    }

    [Fact]
    public void ResolvePayloadType_RegisterResolver_InvalidatesCachedMisses()
    {
        // A plugin registering its resolver after a payload name already missed must not stay
        // blacklisted — its armed recovery registrations become classifiable immediately.
        var name = $"Missing.Namespace.Payload{Guid.NewGuid():N}";
        Assert.Null(PayloadRecoveryClassifier.ResolvePayloadType(name));

        AsyncResponseTypeResolution.RegisterResolver(candidate => candidate == name ? typeof(OperationResult) : null);

        Assert.Equal(typeof(OperationResult), PayloadRecoveryClassifier.ResolvePayloadType(name));
    }

    [Fact]
    public void AssemblyLoad_InvalidatesCachedMisses()
    {
        // A name that missed before its assembly loaded (lazy loads happen mid-run) must resolve
        // afterwards: defining the dynamic assembly raises AppDomain.AssemblyLoad, which
        // invalidates the negative cache, and the next lookup's rescan finds the new type.
        var name = $"AsyncResponse.Tests.Dynamic.LatePayload{Guid.NewGuid():N}";
        Assert.Null(PayloadRecoveryClassifier.ResolvePayloadType(name));

        var builder = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName($"AsyncResponseLateAssembly{Guid.NewGuid():N}"),
            AssemblyBuilderAccess.Run);
        var module = builder.DefineDynamicModule("main");
        module.DefineType(name, TypeAttributes.Public | TypeAttributes.Class).CreateType();

        Assert.NotNull(PayloadRecoveryClassifier.ResolvePayloadType(name));
    }

    [Fact]
    public void CollectibleContextTypes_AreNotPinnedByResolutionCaches()
    {
        // Every type cache on the resolution paths — override detection, conversion plans,
        // invocation plans, and both string→Type resolver caches — must skip types from a
        // collectible AssemblyLoadContext: one strong cache entry pins the unloaded plugin's
        // assemblies until process exit. The proof is the unload itself: exercise every cache
        // site with collectible twins, then demand the context is actually collected.
        var weakContext = ExerciseEveryCacheSiteWithCollectibleTypes();

        for (var i = 0; i < 10 && weakContext.IsAlive; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        Assert.False(weakContext.IsAlive, "The collectible AssemblyLoadContext was pinned by a resolution cache.");
    }

    [Fact]
    public void CollectibleContextTypes_MaterializeWithoutEnteringTheConversionPlanCache()
    {
        // Behavioral pin for the conversion-plan cache skip: a collectible payload type
        // materializes correctly — twice, proving per-call planning works — without a strong
        // ConversionPlans entry (the unload proof above cannot include this exercise because
        // System.Text.Json itself pins collectible contexts through static runtime caches).
        var context = new AssemblyLoadContext($"asyncresponse-plugin-{Guid.NewGuid():N}", isCollectible: true);
        try
        {
            var assembly = context.LoadFromAssemblyPath(typeof(AsyncResponseTypeResolutionTests).Assembly.Location);
            var payloadType = assembly.GetType(typeof(PluginProbePayload).FullName!)!;
            Assert.True(payloadType.Assembly.IsCollectible);

            foreach (var _ in Enumerable.Range(0, 2))
            {
                var materialized = ((object)"""{"Marker":7}""").ConvertTo(payloadType);
                Assert.Equal(payloadType, materialized!.GetType());
                Assert.Equal(RecoveryAction.Resume, ((IAsyncResponsePayload)materialized).OnRecovery());
            }
        }
        finally
        {
            context.Unload();
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference ExerciseEveryCacheSiteWithCollectibleTypes()
    {
        var context = new AssemblyLoadContext($"asyncresponse-plugin-{Guid.NewGuid():N}", isCollectible: true);
        try
        {
            var assembly = context.LoadFromAssemblyPath(typeof(AsyncResponseTypeResolutionTests).Assembly.Location);
            var payloadType = assembly.GetType(typeof(PluginProbePayload).FullName!)!;
            var serviceType = assembly.GetType(typeof(IPluginProbeService).FullName!)!;
            var implementationType = assembly.GetType(typeof(PluginProbeService).FullName!)!;
            Assert.True(payloadType.Assembly.IsCollectible);
            Assert.NotSame(typeof(PluginProbePayload), payloadType);

            // Type-keyed caches: override detection. (JSON materialization of the collectible
            // twin is deliberately NOT exercised here: System.Text.Json pins a collectible
            // context through runtime-internal static caches regardless of options instance —
            // verified against .NET 10 with a fresh JsonSerializerOptions — so it can never sit
            // inside an unload proof. The supported plugin pattern keeps contract types
            // non-collectible, where that boundary is moot; the library's own conversion-plan
            // cache skip is covered behaviorally in
            // CollectibleContextTypes_MaterializeWithoutEnteringTheConversionPlanCache.)
            Assert.True(AsyncResponsePayloadReflection.OverridesOnRecovery(payloadType));

            // Name-keyed caches, fed the collectible twins through the resolver seam under alias
            // names no default-context scan can satisfy.
            var serviceAlias = $"Plugin.Alias.Service{Guid.NewGuid():N}";
            var payloadAlias = $"Plugin.Alias.Payload{Guid.NewGuid():N}";
            AsyncResponseTypeResolution.RegisterResolver(name =>
                name == serviceAlias ? serviceType : name == payloadAlias ? payloadType : null);
            try
            {
                Assert.Same(serviceType, ReflectionExtensions.ResolveServiceType(serviceAlias));
                Assert.Same(payloadType, PayloadRecoveryClassifier.ResolvePayloadType(payloadAlias));

                // Invocation-plan cache: a real call through the reflection invoker against the
                // collectible service interface.
                var services = new ServiceCollection();
                services.AddSingleton(serviceType, Activator.CreateInstance(implementationType)!);
                using var provider = services.BuildServiceProvider();
                provider.InvokeAsync(new ReflectionInvocationDto
                {
                    ServiceInterfaceFullName = serviceAlias,
                    MethodName = nameof(IPluginProbeService.PingAsync),
                    Params = [7]
                }).GetAwaiter().GetResult();
                Assert.Equal(7, PluginProbeService.LastValue);
            }
            finally
            {
                AsyncResponseTypeResolution.Reset();
            }

            // The string-keyed caches must have skipped the collectible twins entirely: with the
            // resolver gone, both aliases miss again instead of serving a cached pin.
            Assert.Null(ReflectionExtensions.ResolveServiceType(serviceAlias));
            Assert.Null(PayloadRecoveryClassifier.ResolvePayloadType(payloadAlias));

            return new WeakReference(context);
        }
        finally
        {
            context.Unload();
        }
    }

    [Fact]
    public void Reset_DropsPositivelyCachedResolutions()
    {
        // Regression: Reset() cleared the resolver list and the negative cache but not the
        // POSITIVE type caches, so a name a removed resolver had already answered kept resolving
        // to its type for the rest of the process — the exact leak Unregister() closes, reopened
        // through the test seam and bleeding resolved types across test cases.
        var serviceAlias = $"Reset.Alias.Service{Guid.NewGuid():N}";
        var payloadAlias = $"Reset.Alias.Payload{Guid.NewGuid():N}";
        AsyncResponseTypeResolution.RegisterResolver(name =>
            name == serviceAlias ? typeof(IRecoverySpy) : name == payloadAlias ? typeof(OperationResult) : null);
        try
        {
            Assert.Same(typeof(IRecoverySpy), ReflectionExtensions.ResolveServiceType(serviceAlias));
            Assert.Same(typeof(OperationResult), PayloadRecoveryClassifier.ResolvePayloadType(payloadAlias));
        }
        finally
        {
            AsyncResponseTypeResolution.Reset();
        }

        Assert.Null(ReflectionExtensions.ResolveServiceType(serviceAlias));
        Assert.Null(PayloadRecoveryClassifier.ResolvePayloadType(payloadAlias));
    }
}

/// <summary>
/// Loaded a second time into a collectible <see cref="AssemblyLoadContext"/> by
/// <see cref="AsyncResponseTypeResolutionTests.CollectibleContextTypes_AreNotPinnedByResolutionCaches"/>;
/// the collectible twin exercises every type cache on the resolution paths.
/// </summary>
public sealed class PluginProbePayload : IAsyncResponsePayload
{
    public int Marker { get; set; }

    public RecoveryAction OnRecovery() => RecoveryAction.Resume;
}

/// <summary>Collectible-twin service contract for the invocation-plan cache exercise.</summary>
public interface IPluginProbeService
{
    Task PingAsync(int value);
}

/// <summary>Implementation resolved and invoked as its collectible twin.</summary>
public sealed class PluginProbeService : IPluginProbeService
{
    // The collectible twin's own static field would live in its context — the default-context
    // test could never read it. The AppDomain data store is a process-wide singleton both twins
    // share, so the value written by the twin is readable here.
    public static int LastValue
        => AppDomain.CurrentDomain.GetData("AsyncResponse.Tests.PluginProbeService.LastValue") is int value ? value : 0;

    public Task PingAsync(int value)
    {
        AppDomain.CurrentDomain.SetData("AsyncResponse.Tests.PluginProbeService.LastValue", value);
        return Task.CompletedTask;
    }
}
