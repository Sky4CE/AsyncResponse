using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class RecoverySettlementRegressionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ResumePublishOutage_RetainsRecoveryUntilRedeliveryPublishes(bool permanentSiblingFirst)
    {
        var transport = new RecordingTransport { Unavailable = true };
        var services = Services(transport);
        services.AddAsyncResponse().WithInMemoryChannel().WithInMemoryDurableFlows();
        await using var provider = services.BuildServiceProvider();
        var store = provider.GetRequiredService<IFlowStateStore>();
        var recovery = provider.GetRequiredService<IRecoveryStateStore>();
        const string flowId = "recovery-publish-outage", correlationId = "pending-response";
        await store.TryCreateAsync(flowId, new FlowState
        {
            FlowId = flowId,
            Status = FlowRunStatus.Running,
            Steps = new() { ["remote"] = new() { PendingCorrelationId = correlationId, PendingPayloadTypeFullName = typeof(OperationResult).FullName } }
        }, TimeSpan.FromHours(1));
        if (permanentSiblingFirst)
            await recovery.SaveAsync(correlationId, new RecoveryState
            {
                RegistrationId = Guid.NewGuid(), CorrelationId = correlationId,
                PayloadTypeFullName = typeof(OperationResult).FullName,
                ResumeCallback = new ReflectionCallDto { ServiceInterfaceFullName = "Missing.Service", MethodName = "Resume", Params = [] }
            }, TimeSpan.FromHours(1));
        var registration = new RecoveryState
        {
            RegistrationId = Guid.NewGuid(), CorrelationId = correlationId,
            PayloadTypeFullName = typeof(OperationResult).FullName,
            ResumeCallback = CallbackExpressionConverter.ToReflectionCall<IDurableFlowExecutor>(e => e.RecoverAsync(flowId, Placeholder.Payload<OperationResult>()!, Placeholder.CorrelationId())),
            FailureCallback = CallbackExpressionConverter.ToReflectionCall<IDurableFlowExecutor>(e => e.FailAsync(flowId, Placeholder.Exception(), Placeholder.CorrelationId()))
        };
        await recovery.SaveAsync(correlationId, registration, TimeSpan.FromHours(1));
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();
        var json = AsyncResponseJson.Serialize(new OperationResult { Status = OperationStatus.Completed });

        await Assert.ThrowsAsync<RecoveryCallbackFailedException>(() => ingress.HandleResponseMessageAsync(json, correlationId));
        Assert.Contains(await recovery.GetAllAsync(correlationId), s => s.RegistrationId == registration.RegistrationId);
        var state = (await store.LoadAsync(flowId))!;
        Assert.True(state.Steps!["remote"].Completed);
        Assert.Equal(FlowRunStatus.Running, state.Status);
        Assert.Empty(transport.Published);

        transport.Unavailable = false;
        await ingress.HandleResponseMessageAsync(json, correlationId);
        Assert.Single(transport.Published);
        Assert.DoesNotContain(await recovery.GetAllAsync(correlationId), s => s.RegistrationId == registration.RegistrationId);
    }

    [Fact]
    public async Task OversizedInitialLedger_IsRejectedBeforePublishing()
    {
        var path = Path.Combine(Path.GetTempPath(), $"ar-initial-budget-{Guid.NewGuid():N}.db");
        var transport = new RecordingTransport();
        var services = Services(transport);
        services.AddAsyncResponse().WithInMemoryChannel().WithSqliteDurableFlows(o =>
        {
            o.ConnectionString = $"Data Source={path};Pooling=false";
            o.MaxStateBytes = 1024;
        });
        try
        {
            await using var provider = services.BuildServiceProvider();
            var flows = provider.GetRequiredService<IDurableFlows>();
            await Assert.ThrowsAnyAsync<InvalidOperationException>(() => flows.StartAsync<AwaitedFlow, string>(new string('x', 4096), "oversized"));
            Assert.Empty(transport.Published);
            Assert.Null(await flows.GetStateAsync("oversized"));
            await flows.StartAsync<AwaitedFlow, string>("small", "small");
            Assert.Single(transport.Published);
            Assert.NotNull(await flows.GetStateAsync("small"));
        }
        finally { File.Delete(path); }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AwaitedCompletionObserverFailure_StopsAttemptAfterOneCompletion(bool cancellation)
    {
        var transport = new RecordingTransport();
        var observer = new ThrowOnceOnCompletion(cancellation);
        var services = Services(transport);
        services.AddSingleton<IDurableFlowExecutionObserver>(observer);
        services.AddSingleton<Calls>();
        services.AddAsyncResponse().WithInMemoryChannel().WithInMemoryDurableFlows().WithDurableFlow<AwaitedFlow, string>();
        await using var provider = services.BuildServiceProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();
        var id = await flows.StartAsync<AwaitedFlow, string>("input", "observer-failure");
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var execution = executor.ExecuteAsync(id);
        var cid = await provider.GetRequiredService<Calls>().Awaiting.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await provider.GetRequiredService<IAsyncResponsePublisher>().SetResponse(new OperationResult { Status = OperationStatus.Completed }, cid);

        await Assert.ThrowsAnyAsync<Exception>(() => execution.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(0, provider.GetRequiredService<Calls>().Downstream);
        Assert.Equal(1, observer.Completions);
        Assert.True((await flows.GetStateAsync(id))!.Steps!["awaited"].Completed);
        await executor.ExecuteAsync(id);
        Assert.Equal(1, provider.GetRequiredService<Calls>().Downstream);
        Assert.Equal(2, (await flows.GetStateAsync(id))!.Attempts);
        Assert.Equal(1, observer.Completions);
    }

    private static ServiceCollection Services(RecordingTransport transport)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IWorkerTransport>(transport);
        return services;
    }

    private sealed class RecordingTransport : IWorkerTransport
    {
        public bool Unavailable;
        public List<WorkerJobEnvelope> Published { get; } = [];
        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            if (Unavailable) throw new IOException("worker transport unavailable");
            Published.Add(job);
            return Task.CompletedTask;
        }
    }

    public sealed class Calls
    {
        public TaskCompletionSource<string> Awaiting { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Downstream;
    }

    public sealed class AwaitedFlow(Calls calls) : IDurableFlow<string>
    {
        public async Task ExecuteAsync(IDurableFlowContext flow, string input)
        {
            await flow.AwaitStepAsync<OperationResult>("awaited", cid => { calls.Awaiting.TrySetResult(cid); return Task.CompletedTask; });
            await flow.StepAsync("downstream", () => { calls.Downstream++; return Task.CompletedTask; });
        }
    }

    private sealed class ThrowOnceOnCompletion(bool cancellation) : IDurableFlowExecutionObserver
    {
        public int Completions;
        public ValueTask OnStepCompletedAsync(DurableFlowStepEvent step)
        {
            if (step.StepName == "awaited" && ++Completions == 1)
            {
                if (cancellation) throw new OperationCanceledException("observer stopped");
                throw new SimulatedCrashException(step.StepName, beforeStep: false);
            }
            return default;
        }
    }
}
