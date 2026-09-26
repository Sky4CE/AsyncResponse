using Microsoft.Extensions.DependencyInjection;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Runtime.Loader;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Serialized against the WHOLE suite, not only the resolver-registry collection: the bound is
/// asserted from the caches' entry counts, and any resolver unregistration anywhere in the process
/// — <c>Round27RegressionTests</c>' plugin registration disposing, a <c>Reset()</c> — clears both
/// positive caches. One such clear late in the loop brought an unbounded cache's count under the
/// bound and masked exactly the regression the test pins (fixpoint r2 S11#16 i).
/// </summary>
[CollectionDefinition(nameof(ResolvedTypeCacheBoundTests), DisableParallelization = true)]
public sealed class ResolvedTypeCacheBoundCollection;

[Collection(nameof(ResolvedTypeCacheBoundTests))]
public sealed class ResolvedTypeCacheBoundTests
{
    [Fact]
    public void PositiveCaches_StayBounded_UnderEndlessSpellingsOfOneResolvableType()
    {
        // The positive caches are keyed by the persisted SPELLING, and every Version variant of a
        // loaded assembly's name resolves to the same type. Pre-fix each novel spelling a store or
        // stream writer chose became a new permanent entry — before the payload or flow gate even
        // looked at the type — so memory grew with every hostile row. Both caches are bounded now.
        var assemblyName = typeof(OperationResult).Assembly.GetName().Name;
        var capacity = ReflectionExtensions.ResolvedTypeCacheCapacity;
        for (var i = 0; i < capacity + 100; i++)
        {
            var spelling = $"{typeof(OperationResult).FullName}, {assemblyName}, Version=7.{i / 1000}.{i % 1000}.0";
            Assert.Equal(typeof(OperationResult), PayloadRecoveryClassifier.ResolvePayloadType(spelling));
            Assert.Equal(typeof(OperationResult), ReflectionExtensions.ResolveServiceType(spelling));
        }

        // A small slack only for background work an earlier test left behind resolving a name at
        // the exact instant the count crosses the bound; unbounded, both counts sit past
        // capacity + 100.
        Assert.InRange(CacheCount(typeof(PayloadRecoveryClassifier), "PayloadTypes"), 0, capacity + 8);
        Assert.InRange(CacheCount(typeof(ReflectionExtensions), "ServiceTypes"), 0, capacity + 8);

        // Names in real use keep resolving (and re-enter the cache after a clear).
        Assert.Equal(typeof(OperationResult), PayloadRecoveryClassifier.ResolvePayloadType(typeof(OperationResult).FullName!));
        Assert.Equal(typeof(OperationResult), ReflectionExtensions.ResolveServiceType(typeof(OperationResult).FullName!));
    }

    private static int CacheCount(Type owner, string field)
        => ((System.Collections.ICollection)owner
            .GetField(field, BindingFlags.Static | BindingFlags.NonPublic)!
            .GetValue(null)!).Count;
}

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

    /// <summary>
    /// Names the runtime parses and finds the parts of, then refuses to BUILD — each one threw out
    /// of <c>Type.GetType(…, throwOnError: false)</c> rather than answering <c>null</c>.
    /// </summary>
    public static TheoryData<string> NamesTheRuntimeRefusesToBuild => new()
    {
        // A constraint violation: Nullable<T> requires a value type.
        "System.Nullable`1[[System.String, System.Private.CoreLib]]",
        // Arity: List<T> given two arguments.
        "System.Collections.Generic.List`1[[System.Int32, System.Private.CoreLib],[System.Int32, System.Private.CoreLib]]",
        // A type that is never a valid generic argument.
        "System.Collections.Generic.List`1[[System.Void, System.Private.CoreLib]]",
        // Arguments given to a non-generic definition.
        "System.String[[System.Int32, System.Private.CoreLib]]",
        // A malformed assembly name inside the brackets (FileLoadException from AssemblyName, on .NET 8 and 10).
        "System.Collections.Generic.List`1[[System.Int32, System.Private.CoreLib, Version=x]]",
    };

    [Theory]
    [MemberData(nameof(NamesTheRuntimeRefusesToBuild))]
    public void ANameTheRuntimeRefusesToBuild_IsUnresolved_AndRecordedAsAMiss(string name)
    {
        // Fixpoint r2 (S3#4): the exception escaped the default scan, so the resolvers threw
        // instead of answering null, the miss was never cached (every redelivery re-parsed the
        // name), and the lost-subscriber dispatcher read the escaped exception as a TRANSIENT
        // callback fault. Each attempt starts from an empty negative cache so the parser runs; an
        // ambient assembly load between the lookup and the check may clear the recorded miss, so
        // the check gets a few attempts (as the negative-cache tests above do).
        for (var attempt = 0; ; attempt++)
        {
            ReflectionExtensions.InvalidateUnresolvableServiceTypes();

            Assert.Null(ReflectionExtensions.ResolveServiceType(name));

            if (UnresolvableTypeNames.IsKnownMiss(name))
                break;

            Assert.True(attempt < 4, "The unresolvable name was never recorded as a miss.");
        }

        ReflectionExtensions.InvalidateUnresolvableServiceTypes();
        Assert.Null(PayloadRecoveryClassifier.ResolvePayloadType(name));
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

    // ---------------------------------------------------------------------------------------
    // Round 33: the default scan resolved a persisted name with Assembly.GetType, which parses
    // the full CLR type-name grammar — a generic instantiation naming an assembly-qualified
    // argument LOADED that assembly on the way to a verdict, and whoever can write the recovery
    // store or the worker stream chose which. The scan must be confined to what is already
    // loaded, for every component of the name.
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// Shared-framework assemblies nothing in this test process references, as (type, assembly)
    /// pairs. A probe takes the first one NOT loaded when it runs, so the two round-33 probes
    /// below each get a fresh target even on pre-fix code, where the first probe loads its pick.
    /// </summary>
    private static readonly (string TypeName, string AssemblyName)[] UnloadedAssemblyCandidates =
    [
        ("System.Net.Mail.MailAddress", "System.Net.Mail"),
        ("System.Net.NetworkInformation.Ping", "System.Net.Ping"),
        ("System.Net.Dns", "System.Net.NameResolution"),
        ("System.Runtime.Serialization.DataContractSerializer", "System.Runtime.Serialization.Xml"),
        ("System.IO.Pipes.PipeStream", "System.IO.Pipes"),
    ];

    private static bool IsLoaded(string assemblyName)
        => AppDomain.CurrentDomain.GetAssemblies()
            .Any(assembly => string.Equals(assembly.GetName().Name, assemblyName, StringComparison.OrdinalIgnoreCase));

    private static (string TypeName, string AssemblyName) PickUnloadedAssembly()
    {
        var candidate = Array.Find(UnloadedAssemblyCandidates, c => !IsLoaded(c.AssemblyName));
        Assert.True(candidate.AssemblyName is not null, "Every candidate assembly is already loaded; add another shared-framework assembly this process does not reference.");
        return candidate;
    }

    /// <summary>
    /// Round 33, the recovery-store half: a persisted payload type name whose generic argument is
    /// qualified to an assembly this process has not loaded. Pre-fix the scan loaded that
    /// assembly and returned the closed type; the verdict must be "unresolvable" with the
    /// process's assembly set unchanged.
    /// </summary>
    [Fact]
    public void ResolvePayloadType_ArgumentQualifiedToAnUnloadedAssembly_ResolvesNothingAndLoadsNothing()
    {
        var (typeName, assemblyName) = PickUnloadedAssembly();
        var persistedName = $"{typeof(Round33Outer<>).FullName}[[{typeName}, {assemblyName}]]";

        var resolved = PayloadRecoveryClassifier.ResolvePayloadType(persistedName);

        Assert.Null(resolved);
        Assert.False(IsLoaded(assemblyName), $"Resolving '{persistedName}' loaded {assemblyName}.");
    }

    /// <summary>
    /// Round 33, the worker-stream half: the envelope's service name goes through the same scan.
    /// Pre-fix a name aiming its generic argument at an unloaded assembly loaded it on delivery;
    /// it must resolve to nothing and load nothing.
    /// </summary>
    [Fact]
    public void ResolveServiceType_ArgumentQualifiedToAnUnloadedAssembly_ResolvesNothingAndLoadsNothing()
    {
        var (typeName, assemblyName) = PickUnloadedAssembly();
        var wireName = $"{typeof(IRound33Service<>).FullName}[[{typeName}, {assemblyName}]]";

        var resolved = ReflectionExtensions.ResolveServiceType(wireName);

        Assert.Null(resolved);
        Assert.False(IsLoaded(assemblyName), $"Resolving '{wireName}' loaded {assemblyName}.");
    }

    /// <summary>
    /// Round 33 control: the loaded-only scan is not over-broad. What the library actually
    /// persists for a closed generic — <c>typeof(T).FullName</c>, whose argument is
    /// assembly-qualified to an assembly that IS loaded (CoreLib, this test assembly) — still
    /// resolves on both paths, and the classifier still routes on it.
    /// </summary>
    [Fact]
    public void ClosedGeneric_WithLoadedArguments_StillResolvesAndClassifies()
    {
        Assert.Same(typeof(Round33Outer<int>), PayloadRecoveryClassifier.ResolvePayloadType(typeof(Round33Outer<int>).FullName!));
        Assert.Same(typeof(Round33Outer<OperationResult>), PayloadRecoveryClassifier.ResolvePayloadType(typeof(Round33Outer<OperationResult>).FullName!));
        Assert.Same(typeof(IRound33Service<int>), ReflectionExtensions.ResolveServiceType(typeof(IRound33Service<int>).FullName!));

        var wireJson = AsyncResponseJson.Serialize(new Round33Outer<int> { Inner = 7 });
        var classification = PayloadRecoveryClassifier.Classify(wireJson, typeof(Round33Outer<int>).FullName);

        Assert.Equal(RecoveryAction.Resume, classification.Action);
        Assert.Equal(7, Assert.IsType<Round33Outer<int>>(classification.MaterializedPayload).Inner);
    }

    // -----------------------------------------------------------------------------------------
    // Persisted type names are written by whoever can write the recovery store or the worker
    // stream. The CLR's type-name parser recurses per generic argument with no depth limit of its
    // own, and a StackOverflowException cannot be caught: the process exits, the message stays
    // unacknowledged, and every worker it is redelivered to exits the same way. So no persisted
    // name reaches that parser — or the caches in front of it — without passing these limits.
    //
    // The tests below deliberately never hand a hostile name to the runtime: they assert that the
    // guard refuses it FIRST. Feeding one to Type.GetType to "prove" the crash would take the
    // whole test host down with it.
    // -----------------------------------------------------------------------------------------

    [Theory]
    // A name longer than the cap, built from a legitimate prefix so nothing else can reject it.
    [InlineData(4097, 0, 0, false)]
    [InlineData(4096, 0, 0, true)]
    // Nesting: a generic level costs TWO brackets in the assembly-qualified form, so the
    // 16-bracket depth bound is eight levels of nesting.
    [InlineData(0, 9, 0, false)]
    [InlineData(0, 8, 0, true)]
    // A chain of decorations nests only one deep however long it grows, so it needs its own bound.
    [InlineData(0, 0, 65, false)]
    [InlineData(0, 0, 64, true)]
    public void IsWithinResolutionLimits_BoundsLengthNestingAndBracketCount(int length, int nesting, int brackets, bool expected)
    {
        var name = length > 0
            ? "N.T" + new string('x', length - 3)
            : nesting > 0
                ? string.Concat(string.Concat(Enumerable.Repeat("N.T`1[[", nesting)), "N.T", string.Concat(Enumerable.Repeat("]]", nesting)))
                : "N.T" + string.Concat(Enumerable.Repeat("[]", brackets));

        Assert.Equal(expected, AsyncResponseTypeResolution.IsWithinResolutionLimits(name));
    }

    [Theory]
    // By-ref and pointer decorations: no callback service, payload, flow or flow-input type is
    // one, and refusing them wherever they appear keeps the guard a single forward scan.
    [InlineData("N.T&")]
    [InlineData("N.T*")]
    [InlineData("N.T`1[[N.U&, Asm]]")]
    public void IsWithinResolutionLimits_RefusesByRefAndPointerDecorations(string name)
        => Assert.False(AsyncResponseTypeResolution.IsWithinResolutionLimits(name));

    [Fact]
    public void ResolutionPaths_RefuseAnOversizedName_WithoutConsultingResolvers()
    {
        // A resolver stands for the recursive parser behind every path: RegisterAssembly's is
        // Assembly.GetType, and an application resolver is as likely to call Type.GetType itself.
        // Reaching it at all is the defect, so the probe records and the assertion is that it
        // never ran — and that the name never became a cache key either.
        var hostile = HostileNames();
        var reasonable = $"N.Reasonable{Guid.NewGuid():N}";
        var consulted = RegisterAnsweringResolver([.. hostile, reasonable]);

        foreach (var name in hostile)
        {
            Assert.Null(AsyncResponseTypeResolution.Resolve(name));
            Assert.Null(ReflectionExtensions.ResolveServiceType(name));
            // ResolveServiceType's own guard, not only Resolve's backstop: the name is not a
            // negative-cache key either.
            Assert.False(UnresolvableTypeNames.IsKnownMiss(name));
        }

        Assert.Equal(0, consulted.Count);

        // A name within the limits still resolves through the very same resolver.
        Assert.Same(typeof(OperationResult), AsyncResponseTypeResolution.Resolve(reasonable));
        Assert.Equal(1, consulted.Count);
    }

    [Fact]
    public void RecoveryClassification_RefusesAnOversizedPayloadTypeName()
    {
        // The recovery path resolves the persisted payload type name BEFORE any callback is
        // chosen or authorized, so no authorizer configuration stands in front of it: the bound
        // has to be here. Regression for the pin itself: with no resolver registered, a long
        // simple name simply failed to resolve, so the test passed without any guard at all. The
        // answer-everything resolver makes an unguarded path MATERIALIZE the payload, and the
        // negative-cache assertion pins ResolvePayloadType's own guard — the backstops in
        // ResolveLoaded and Resolve would otherwise still answer null after the hostile name had
        // become a cache key.
        var hostile = HostileNames();
        var reasonable = $"N.Reasonable{Guid.NewGuid():N}";
        var consulted = RegisterAnsweringResolver([.. hostile, reasonable]);

        foreach (var name in hostile)
        {
            Assert.Null(PayloadRecoveryClassifier.Classify("""{"Status":2}""", name).MaterializedPayload);
            Assert.False(UnresolvableTypeNames.IsKnownMiss(name));
        }

        Assert.Equal(0, consulted.Count);

        // The same resolver does answer a name within the limits: the refusal above is the guard's.
        Assert.NotNull(PayloadRecoveryClassifier.Classify("""{"Status":2}""", reasonable).MaterializedPayload);
        Assert.Equal(1, consulted.Count);
    }

    /// <summary>
    /// Several distinct over-long names. The negative-cache assertion on each is what pins a
    /// guard in front of the caches, and a single name could hide a missing guard: an assembly
    /// the lookup itself lazily loads invalidates the miss it just recorded, so the first name in
    /// a fresh process may read "not cached" even when the guard is gone. The later ones cannot.
    /// </summary>
    private static string[] HostileNames()
        => [.. Enumerable.Range(0, 3).Select(i => $"N.T{i}" + new string('x', AsyncResponseTypeResolution.MaxTypeNameLength))];

    /// <summary>
    /// Registers a resolver that answers <see cref="OperationResult"/> for exactly the given names
    /// and counts those lookups. Name-scoped, not answer-everything: the registry is process-wide,
    /// so tests in other classes running in parallel consult it too, and an answer-everything
    /// resolver would both inflate the count and hand their lookups a type they never asked for.
    /// </summary>
    private static ConsultationCount RegisterAnsweringResolver(params string[] names)
    {
        var count = new ConsultationCount();
        AsyncResponseTypeResolution.RegisterResolver(name =>
        {
            if (Array.IndexOf(names, name) < 0)
                return null;

            count.Increment();
            return typeof(OperationResult);
        });
        return count;
    }

    private sealed class ConsultationCount
    {
        private int _count;

        public int Count => Volatile.Read(ref _count);

        public void Increment() => Interlocked.Increment(ref _count);
    }

    [Fact]
    public void DescribeForDiagnostics_KeepsOrdinaryNamesWhole_AndExcerptsTheRest()
    {
        // The name is store data: an unresolvable one is logged on every delivery, so it must not
        // carry megabytes of store-written text — or its raw line breaks — into the log.
        Assert.Equal("N.Ordinary", AsyncResponseTypeResolution.DescribeForDiagnostics("N.Ordinary"));

        var hostile = "N.T" + new string('x', AsyncResponseTypeResolution.MaxTypeNameLength) + "\n";
        var described = AsyncResponseTypeResolution.DescribeForDiagnostics(hostile);

        Assert.True(described.Length < 200, $"the excerpt is {described.Length} characters long");
        Assert.DoesNotContain("\n", described, StringComparison.Ordinal);
        Assert.Contains(hostile.Length.ToString(System.Globalization.CultureInfo.InvariantCulture), described, StringComparison.Ordinal);
    }
}

/// <summary>
/// Open generic payload whose closed names are the round-33 assembly-load probe: the argument's
/// assembly qualification is what a persisted name can smuggle in.
/// </summary>
public sealed class Round33Outer<T> : IAsyncResponsePayload
{
    public T? Inner { get; set; }

    public RecoveryAction OnRecovery() => RecoveryAction.Resume;
}

/// <summary>Open generic service contract; the worker-stream twin of <see cref="Round33Outer{T}"/>.</summary>
public interface IRound33Service<T>
{
    Task RunAsync(T value);
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
