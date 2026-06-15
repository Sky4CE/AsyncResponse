using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The fluent builder's safety guarantees: subscribe-before-trigger ordering, waiter teardown on
/// a failing trigger, and worker enqueueing with the ambient correlation id.
/// </summary>
public class AsyncResponseBuilderTests
{
    private readonly Mock<IAsyncResponseSubscriber> _subscriber = new();
    private readonly Mock<IAsyncResponseWaiter<OperationResult>> _waiter = new();

    public AsyncResponseBuilderTests()
    {
        _waiter.SetupGet(w => w.ResponseTask)
            .Returns(Task.FromResult(new OperationResult { Status = OperationStatus.Completed }));
        _subscriber
            .Setup(s => s.CreateResponseWaiter<OperationResult>(
                It.IsAny<string>(),
                It.IsAny<ReflectionCallDto?>(),
                It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(),
                It.IsAny<TimeSpan?>()))
            .ReturnsAsync(_waiter.Object);
    }

    [Fact]
    public async Task TriggeredBy_RunsAfterSubscription()
    {
        var callOrder = new List<string>();
        _subscriber
            .Setup(s => s.CreateResponseWaiter<OperationResult>(
                "corr-1", It.IsAny<ReflectionCallDto?>(), It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(), It.IsAny<TimeSpan?>()))
            .Callback(() => callOrder.Add("subscribe"))
            .ReturnsAsync(_waiter.Object);

        var result = await new AsyncResponseBuilder(_subscriber.Object)
            .For<OperationResult>("corr-1")
            .TriggeredBy(() =>
            {
                callOrder.Add("trigger");
                return Task.CompletedTask;
            })
            .BuildAndWaitAsync();

        Assert.Equal(["subscribe", "trigger"], callOrder);
        Assert.Equal(OperationStatus.Completed, result.Status);
    }

    [Fact]
    public async Task TriggeredBy_WhenTriggerThrows_DisposesWaiterAndPropagates()
    {
        var builder = new AsyncResponseBuilder(_subscriber.Object)
            .For<OperationResult>("corr-1")
            .TriggeredBy(() => throw new InvalidOperationException("send failed"));

        await Assert.ThrowsAsync<InvalidOperationException>(builder.BuildWaiterAsync);

        // The operation never started: subscription and recovery state must be torn down.
        _waiter.Verify(w => w.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task ExpressionCallbacks_AreRegisteredAsReflectionCalls()
    {
        ReflectionCallDto? resume = null;
        ReflectionCallDto? failure = null;
        _subscriber
            .Setup(s => s.CreateResponseWaiter<OperationResult>(
                "corr-1", It.IsAny<ReflectionCallDto?>(), It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(), It.IsAny<TimeSpan?>()))
            .Callback<string, ReflectionCallDto?, ReflectionCallDto?, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (_, r, f, _, _) => { resume = r; failure = f; })
            .ReturnsAsync(_waiter.Object);

        await new AsyncResponseBuilder(_subscriber.Object)
            .For<OperationResult>("corr-1")
            .OnLostSubscriberResume<IRecoverySpy>(spy => spy.OnResume(Placeholder.Payload<OperationResult>()))
            .OnLostSubscriberFailure<IRecoverySpy>(spy => spy.OnFailure(Placeholder.Exception()))
            .BuildWaiterAsync();

        Assert.NotNull(resume);
        Assert.Equal(nameof(IRecoverySpy.OnResume), resume!.MethodName);
        Assert.Equal(PlaceholderType.Payload, Assert.Single(resume.Params).Placeholder);
        Assert.NotNull(failure);
        Assert.Equal(nameof(IRecoverySpy.OnFailure), failure!.MethodName);
        Assert.Equal(PlaceholderType.Exception, Assert.Single(failure.Params).Placeholder);
    }

    [Fact]
    public async Task EnqueueWorker_WithoutTransport_ThrowsWithGuidance()
    {
        var builder = new AsyncResponseBuilder(_subscriber.Object, null);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            builder.EnqueueWorkerAsync<IRecoverySpy>(spy => spy.OnWorkerJob(42)));

        Assert.Contains("IWorkerTransport", ex.Message);
    }

    [Fact]
    public async Task EnqueueWorker_CapturesAmbientCorrelationId()
    {
        var transport = new Mock<IWorkerTransport>();
        WorkerJobEnvelope? published = null;
        transport
            .Setup(t => t.PublishAsync(It.IsAny<WorkerJobEnvelope>(), It.IsAny<CancellationToken>()))
            .Callback<WorkerJobEnvelope, CancellationToken>((job, _) => published = job)
            .Returns(Task.CompletedTask);

        AsyncResponseContext.SetCorrelationId("corr-worker");
        await new AsyncResponseBuilder(_subscriber.Object, transport.Object)
            .EnqueueWorkerAsync<IRecoverySpy>(spy => spy.OnWorkerJob(7));

        Assert.NotNull(published);
        Assert.Equal("corr-worker", published!.CorrelationId);
        Assert.Equal(nameof(IRecoverySpy.OnWorkerJob), published.Call.MethodName);
        Assert.Equal(7, Assert.Single(published.Call.Params).Value);
    }
}
