using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The fluent builder's safety guarantees: subscribe-before-trigger ordering, waiter teardown on
/// a failing trigger, and worker enqueueing with the ambient async-response context.
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
    public async Task WaitAsync_TriggerRunsAfterSubscription()
    {
        var callOrder = new List<string>();
        _subscriber
            .Setup(s => s.CreateResponseWaiter<OperationResult>(
                It.IsAny<string>(), It.IsAny<ReflectionCallDto?>(), It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(), It.IsAny<TimeSpan?>()))
            .Callback(() => callOrder.Add("subscribe"))
            .ReturnsAsync(_waiter.Object);

        var result = await new AsyncResponseBuilder(_subscriber.Object)
            .For<OperationResult>()
            .WaitAsync(() =>
            {
                callOrder.Add("trigger");
                return Task.CompletedTask;
            });

        Assert.Equal(["subscribe", "trigger"], callOrder);
        Assert.Equal(OperationStatus.Completed, result.Status);
    }

    [Fact]
    public async Task WaitAsync_WhenTriggerThrows_DisposesWaiterAndPropagates()
    {
        var builder = new AsyncResponseBuilder(_subscriber.Object).For<OperationResult>();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            builder.WaitAsync(() => throw new InvalidOperationException("send failed")));

        // The operation never started: subscription and recovery state must be torn down.
        _waiter.Verify(w => w.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task AttachedWaitAsync_IsWaitOnly()
    {
        // For<T>(correlationId) attaches to an operation started elsewhere; its terminal takes
        // no trigger — a double-send is not expressible.
        var result = await new AsyncResponseBuilder(_subscriber.Object)
            .For<OperationResult>("corr-1")
            .WaitAsync();

        Assert.Equal(OperationStatus.Completed, result.Status);
        _waiter.Verify(w => w.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task For_WithoutCorrelationId_GeneratesOneAndSharesItWithAmbientContextAndTrigger()
    {
        string? subscribedWith = null;
        _subscriber
            .Setup(s => s.CreateResponseWaiter<OperationResult>(
                It.IsAny<string>(), It.IsAny<ReflectionCallDto?>(), It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(), It.IsAny<TimeSpan?>()))
            .Callback<string, ReflectionCallDto?, ReflectionCallDto?, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (correlationId, _, _, _, _) => subscribedWith = correlationId)
            .ReturnsAsync(_waiter.Object);

        string? triggeredWith = null;
        await new AsyncResponseBuilder(_subscriber.Object)
            .For<OperationResult>()
            .WaitAsync((string correlationId) =>
            {
                triggeredWith = correlationId;
                return Task.CompletedTask;
            });

        Assert.False(string.IsNullOrWhiteSpace(subscribedWith));
        Assert.Equal(subscribedWith, triggeredWith);
        // The generated id is also ambient, so outgoing requests built from context still correlate.
        Assert.Equal(subscribedWith, AsyncResponseContext.CorrelationId);
    }

    [Fact]
    public async Task WaitAsync_WithReplyTarget_PassesRequestContextAndScopesAmbientValues()
    {
        var replyTarget = new AsyncResponseReplyTarget
        {
            Name = "default",
            Transport = "test",
            Address = "test://reply"
        };
        var provider = new Mock<IAsyncResponseReplyTargetProvider>();
        provider.Setup(p => p.GetReplyTarget(null)).Returns(replyTarget);

        string? subscribedWith = null;
        _subscriber
            .Setup(s => s.CreateResponseWaiter<OperationResult>(
                It.IsAny<string>(), It.IsAny<ReflectionCallDto?>(), It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(), It.IsAny<TimeSpan?>()))
            .Callback<string, ReflectionCallDto?, ReflectionCallDto?, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (correlationId, _, _, _, _) => subscribedWith = correlationId)
            .ReturnsAsync(_waiter.Object);

        AsyncResponseRequestContext? observed = null;
        AsyncResponseReplyTarget? ambientInTrigger = null;
        await new AsyncResponseBuilder(_subscriber.Object, null, provider.Object)
            .For<OperationResult>()
            .WithReplyTarget()
            .WaitAsync((AsyncResponseRequestContext context) =>
            {
                observed = context;
                ambientInTrigger = AsyncResponseContext.ReplyTarget;
                return Task.CompletedTask;
            });

        Assert.NotNull(observed);
        Assert.Equal(subscribedWith, observed!.CorrelationId);
        Assert.Same(replyTarget, observed.ReplyTarget);
        Assert.Same(replyTarget, ambientInTrigger);
        Assert.Null(AsyncResponseContext.ReplyTarget);
    }

    [Fact]
    public async Task WaitAsync_WithNamedReplyTarget_ResolvesNamedProviderTarget()
    {
        var replyTarget = new AsyncResponseReplyTarget
        {
            Name = "regional-us",
            Transport = "test",
            Address = "test://regional-us"
        };
        var provider = new Mock<IAsyncResponseReplyTargetProvider>();
        provider.Setup(p => p.GetReplyTarget("regional-us")).Returns(replyTarget);

        AsyncResponseRequestContext? observed = null;
        await new AsyncResponseBuilder(_subscriber.Object, null, provider.Object)
            .For<OperationResult>()
            .WithReplyTarget("regional-us")
            .WaitAsync((AsyncResponseRequestContext context) =>
            {
                observed = context;
                return Task.CompletedTask;
            });

        Assert.Same(replyTarget, observed!.ReplyTarget);
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
            .WaitAsync();

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
    public async Task EnqueueWorker_CapturesAmbientContext()
    {
        var transport = new Mock<IWorkerTransport>();
        WorkerJobEnvelope? published = null;
        transport
            .Setup(t => t.PublishAsync(It.IsAny<WorkerJobEnvelope>(), It.IsAny<CancellationToken>()))
            .Callback<WorkerJobEnvelope, CancellationToken>((job, _) => published = job)
            .Returns(Task.CompletedTask);

        var replyTarget = new AsyncResponseReplyTarget
        {
            Name = "default",
            Transport = "test",
            Address = "test://reply"
        };

        AsyncResponseContext.SetCorrelationId("corr-worker");
        AsyncResponseContext.SetReplyTarget(replyTarget);
        try
        {
            await new AsyncResponseBuilder(_subscriber.Object, transport.Object)
                .EnqueueWorkerAsync<IRecoverySpy>(spy => spy.OnWorkerJob(7));
        }
        finally
        {
            AsyncResponseContext.ClearCorrelationId();
            AsyncResponseContext.ClearReplyTarget();
        }

        Assert.NotNull(published);
        Assert.Equal("corr-worker", published!.CorrelationId);
        Assert.Same(replyTarget, published.ReplyTarget);
        Assert.Equal(nameof(IRecoverySpy.OnWorkerJob), published.Call.MethodName);
        Assert.Equal(7, Assert.Single(published.Call.Params).Value);
    }
}
