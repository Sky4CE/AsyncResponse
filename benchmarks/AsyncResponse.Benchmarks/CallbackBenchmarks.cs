using System.Linq.Expressions;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// The reflection-callback machinery behind worker jobs and lost-subscriber recovery: converting a
/// strongly-typed lambda into a serializable descriptor, and resolving + invoking it through DI.
/// </summary>
[MemoryDiagnoser]
public class CallbackBenchmarks
{
    private static readonly Expression<Func<IBenchWorker, Task>> Expression = worker => worker.DoWorkAsync(42);

    private ServiceProvider _provider = null!;
    private ReflectionInvocationDto _invocation = null!;

    [GlobalSetup]
    public void Setup()
    {
        _provider = new ServiceCollection().AddSingleton<IBenchWorker, BenchWorker>().BuildServiceProvider();
        var dto = CallbackExpressionConverter.ToReflectionCall(Expression);
        _invocation = ReflectionExtensions.ResolveCallback(dto, payload: null, exception: null, correlationId: "c1");
    }

    [GlobalCleanup]
    public void Cleanup() => _provider.Dispose();

    [Benchmark]
    public ReflectionCallDto ExpressionToReflectionCall() => CallbackExpressionConverter.ToReflectionCall(Expression);

    [Benchmark]
    public Task ReflectionInvoke() => _provider.InvokeAsync(_invocation);
}
