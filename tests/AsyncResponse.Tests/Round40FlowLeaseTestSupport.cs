using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Linq.Expressions;
using System.Text.Json;

namespace AsyncResponse.Tests;

/// <summary>Executor wiring shared by the round-40 lease regression and new-API tests.</summary>
internal static class Round40FlowLeaseTestSupport
{
    public static FlowState RunnableState(string flowId)
        => new()
        {
            FlowId = flowId,
            FlowTypeName = typeof(CountingFlow).FullName,
            InputTypeName = typeof(TestFlowInput).FullName,
            InputJson = JsonSerializer.Serialize(new TestFlowInput(1)),
            Status = FlowRunStatus.Running,
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow
        };

    public static ExecutorHarness CreateHarness(IFlowStateStore store, DurableFlowOptions options)
    {
        var services = new ServiceCollection();
        services.AddSingleton(store);
        services.AddSingleton<CountingFlow>();
        var provider = services.BuildServiceProvider();
        var builder = new Mock<IAsyncResponseBuilder>();
        builder.Setup(instance => instance.EnqueueWorkerAsync(
                It.IsAny<Expression<Func<IDurableFlowExecutor, Task>>>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var executor = new DurableFlowExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            builder.Object,
            Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            new AsyncResponseContextPropagation([]),
            options,
            NullLogger<DurableFlowExecutor>.Instance);
        return new ExecutorHarness(provider, executor);
    }

    internal sealed class ExecutorHarness(ServiceProvider provider, DurableFlowExecutor executor) : IAsyncDisposable
    {
        public DurableFlowExecutor Executor => executor;

        public CountingFlow Flow => provider.GetRequiredService<CountingFlow>();

        public ValueTask DisposeAsync() => provider.DisposeAsync();
    }

    public sealed class CountingFlow : IDurableFlow<TestFlowInput>
    {
        private int _executions;

        public int Executions => Volatile.Read(ref _executions);

        public Task ExecuteAsync(IDurableFlowContext context, TestFlowInput input)
        {
            Interlocked.Increment(ref _executions);
            return Task.CompletedTask;
        }
    }
}
