using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>A reflection-invocation target resolved by full name from the DI container.</summary>
public interface IInvokeTarget
{
    Task RecordAsync(string value);
    Task RecordNullTaskAsync(string value);
    ValueTask RecordValueTaskAsync(string value);
    ValueTask<int> RecordGenericValueTaskAsync(string value);
    void RecordVoid(string value);
    int RecordSync(string value);
    Task RecordNumberAsync(int value);
    Task RecordMultipleAsync(int value, string passThrough);
    Task RecordLongAsync(long value);
    Task ByRefAsync(ref int value);
    Task GenericAsync<T>(T value);

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

    public Task RecordNullTaskAsync(string value)
    {
        Recorded.Add(value);
        return null!;
    }

    public async ValueTask RecordValueTaskAsync(string value)
    {
        await Task.Yield();
        Recorded.Add(value);
    }

    public async ValueTask<int> RecordGenericValueTaskAsync(string value)
    {
        await Task.Yield();
        Recorded.Add(value);
        return Recorded.Count;
    }

    public void RecordVoid(string value)
        => Recorded.Add(value);

    public int RecordSync(string value)
    {
        Recorded.Add(value);
        return Recorded.Count;
    }

    public Task RecordNumberAsync(int value)
    {
        Recorded.Add(value.ToString());
        return Task.CompletedTask;
    }

    public Task RecordMultipleAsync(int value, string passThrough)
    {
        Recorded.Add(value.ToString() + passThrough);
        return Task.CompletedTask;
    }

    public Task RecordLongAsync(long value)
    {
        Recorded.Add(value.ToString());
        return Task.CompletedTask;
    }

    public Task ByRefAsync(ref int value) => Task.CompletedTask;
    public Task GenericAsync<T>(T value) => Task.CompletedTask;

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
    public async Task InvokesValueTaskMethod_AndAwaitsCompletion()
    {
        var target = new InvokeTarget();
        var provider = new ServiceCollection().AddSingleton<IInvokeTarget>(target).BuildServiceProvider();

        await provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.RecordValueTaskAsync),
            Params = ["value-task"]
        });

        Assert.Equal("value-task", Assert.Single(target.Recorded));
    }

    [Fact]
    public async Task InvokesVoidSyncAndNullTaskMethods()
    {
        var target = new InvokeTarget();
        var provider = new ServiceCollection().AddSingleton<IInvokeTarget>(target).BuildServiceProvider();

        await provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.RecordVoid),
            Params = ["void"]
        });
        await provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.RecordSync),
            Params = ["sync"]
        });
        await provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.RecordNullTaskAsync),
            Params = ["null-task"]
        });

        Assert.Equal(["void", "sync", "null-task"], target.Recorded);
    }

    [Fact]
    public async Task ConvertsPrimitiveArgumentsWithChangeType()
    {
        var target = new InvokeTarget();
        var provider = new ServiceCollection().AddSingleton<IInvokeTarget>(target).BuildServiceProvider();

        await provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.RecordNumberAsync),
            Params = ["42"]
        });

        Assert.Equal("42", Assert.Single(target.Recorded));
    }

    [Fact]
    public async Task InvokesGenericValueTaskMethod_AndAwaitsCompletion()
    {
        var target = new InvokeTarget();
        var provider = new ServiceCollection().AddSingleton<IInvokeTarget>(target).BuildServiceProvider();

        await provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.RecordGenericValueTaskAsync),
            Params = ["generic-value-task"]
        });

        Assert.Equal("generic-value-task", Assert.Single(target.Recorded));
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

    [Fact]
    public async Task ByRefParameter_ThrowsNotSupported()
    {
        var provider = new ServiceCollection().AddSingleton<IInvokeTarget>(new InvokeTarget()).BuildServiceProvider();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.ByRefAsync),
            Params = [1]
        }));

        Assert.Contains("by-ref", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UnboundGenericMethod_ThrowsNotSupported()
    {
        var provider = new ServiceCollection().AddSingleton<IInvokeTarget>(new InvokeTarget()).BuildServiceProvider();

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.GenericAsync),
            Params = [1]
        }));

        Assert.Contains("generic", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InvokeAsync_ConvertsIntToLongWithPrimitiveFallback()
    {
        var target = new InvokeTarget();
        using var provider = new ServiceCollection().AddSingleton<IInvokeTarget>(target).BuildServiceProvider();

        await provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.RecordLongAsync),
            Params = [(int)42]
        });

        Assert.Equal("42", Assert.Single(target.Recorded));
    }

    [Fact]
    public async Task InvokeAsync_MultipleArguments_ConvertsAndCopiesCorrectly()
    {
        var target = new InvokeTarget();
        using var provider = new ServiceCollection().AddSingleton<IInvokeTarget>(target).BuildServiceProvider();

        await provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IInvokeTarget).FullName!,
            MethodName = nameof(IInvokeTarget.RecordMultipleAsync),
            Params = ["123", "pass"]
        });

        Assert.Equal("123pass", Assert.Single(target.Recorded));
    }
}
