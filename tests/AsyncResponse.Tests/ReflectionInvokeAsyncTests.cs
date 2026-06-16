using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>A reflection-invocation target resolved by full name from the DI container.</summary>
public interface IInvokeTarget
{
    Task RecordAsync(string value);

    // Two overloads with the same arity — persisted callbacks cannot disambiguate these.
    Task TwinAsync(string value);
    Task TwinAsync(int value);
}

public sealed class InvokeTarget : IInvokeTarget
{
    public List<string> Recorded { get; } = [];

    public Task RecordAsync(string value)
    {
        Recorded.Add(value);
        return Task.CompletedTask;
    }

    public Task TwinAsync(string value) => Task.CompletedTask;
    public Task TwinAsync(int value) => Task.CompletedTask;
}

/// <summary>
/// Invocation of a persisted <see cref="ReflectionInvocationDto"/> against the DI container: the
/// service is resolved by full name and the method picked by name + arity, with clear failures when
/// the type is unknown, the service is unregistered, no matching method exists, or an overload is
/// ambiguous.
/// </summary>
public class ReflectionInvokeAsyncTests
{
    [Fact]
    public async Task InvokesResolvedServiceMethod_AndConvertsParams()
    {
        var target = new InvokeTarget();
        var provider = new ServiceCollection().AddSingleton<IInvokeTarget>(target).BuildServiceProvider();

        await provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.RecordAsync),
            Params = ["hello"]
        });

        Assert.Equal("hello", Assert.Single(target.Recorded));
    }

    [Fact]
    public async Task TypeNotFound_Throws()
    {
        var provider = new ServiceCollection().BuildServiceProvider();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = "AsyncResponse.Tests.NoSuchService",
            MethodName = "X",
            Params = []
        }));

        Assert.Contains("not found", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ServiceNotRegistered_Throws()
    {
        var provider = new ServiceCollection().BuildServiceProvider(); // IInvokeTarget not registered

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.RecordAsync),
            Params = ["x"]
        }));

        Assert.Contains("not registered", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NoMethodWithNameAndArity_Throws()
    {
        var provider = new ServiceCollection().AddSingleton<IInvokeTarget>(new InvokeTarget()).BuildServiceProvider();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.RecordAsync),
            Params = ["a", "b"] // RecordAsync has arity 1; no arity-2 overload exists
        }));

        Assert.Contains("No method", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AmbiguousOverload_Throws()
    {
        var provider = new ServiceCollection().AddSingleton<IInvokeTarget>(new InvokeTarget()).BuildServiceProvider();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.TwinAsync), // two arity-1 overloads
            Params = ["only-one-arg"]
        }));

        Assert.Contains("overload", ex.Message, StringComparison.Ordinal);
    }
}
