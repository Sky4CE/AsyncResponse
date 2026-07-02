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
}
