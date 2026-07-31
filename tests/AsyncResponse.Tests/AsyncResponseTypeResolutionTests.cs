using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

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
        // row, a renamed type) re-walks every loaded assembly and the resolver chain.
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

        Assert.Equal(1, probes);
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
}
