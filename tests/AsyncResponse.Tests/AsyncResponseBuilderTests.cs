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
    private readonly Mock<IRecoverableAsyncResponseSubscriber> _recoverableSubscriber = new();
    private readonly Mock<IAsyncResponseWaiter<OperationResult>> _waiter = new();

    public AsyncResponseBuilderTests()
    {
        _waiter.SetupGet(w => w.ResponseTask)
            .Returns(Task.FromResult(new OperationResult { Status = OperationStatus.Completed }));
        _subscriber
            .Setup(s => s.CreateResponseWaiter<OperationResult>(
                It.IsAny<string>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(),
                It.IsAny<TimeSpan?>()))
            .ReturnsAsync(_waiter.Object);
        _recoverableSubscriber
            .Setup(s => s.CreateRecoverableResponseWaiter<OperationResult>(
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
                It.IsAny<string>(), It.IsAny<Func<OperationResult, ValueTask<bool>>?>(), It.IsAny<TimeSpan?>()))
            .Callback(() => callOrder.Add("subscribe"))
            .ReturnsAsync(_waiter.Object);

        var result = await new AsyncResponseBuilder(_subscriber.Object)
            .For<OperationResult>()
            .WaitAsync(_ =>
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
            builder.WaitAsync(_ => throw new InvalidOperationException("send failed")));

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
    public async Task WaitAsync_RejectsBuilderReuse()
    {
        var builder = new AsyncResponseBuilder(_subscriber.Object)
            .For<OperationResult>("corr-1");

        await builder.WaitAsync();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(builder.WaitAsync);

        Assert.Contains("single-use", ex.Message);
    }

    [Fact]
    public async Task For_WithoutCorrelationId_GeneratesOneAndSharesItWithAmbientContextAndTrigger()
    {
        string? subscribedWith = null;
        _subscriber
            .Setup(s => s.CreateResponseWaiter<OperationResult>(
                It.IsAny<string>(), It.IsAny<Func<OperationResult, ValueTask<bool>>?>(), It.IsAny<TimeSpan?>()))
            .Callback<string, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (correlationId, _, _) => subscribedWith = correlationId)
            .ReturnsAsync(_waiter.Object);

        string? triggeredWith = null;
        await new AsyncResponseBuilder(_subscriber.Object)
            .For<OperationResult>()
            .WaitAsync(context =>
            {
                triggeredWith = context.CorrelationId;
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
                It.IsAny<string>(), It.IsAny<Func<OperationResult, ValueTask<bool>>?>(), It.IsAny<TimeSpan?>()))
            .Callback<string, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (correlationId, _, _) => subscribedWith = correlationId)
            .ReturnsAsync(_waiter.Object);

        AsyncResponseRequestContext? observed = null;
        AsyncResponseReplyTarget? ambientInTrigger = null;
        await new AsyncResponseBuilder(_subscriber.Object, null, provider.Object)
            .For<OperationResult>()
            .WithReplyTarget()
            .WaitAsync(context =>
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
            .WaitAsync(context =>
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
        _recoverableSubscriber
            .Setup(s => s.CreateRecoverableResponseWaiter<OperationResult>(
                "corr-1", It.IsAny<ReflectionCallDto?>(), It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(), It.IsAny<TimeSpan?>()))
            .Callback<string, ReflectionCallDto?, ReflectionCallDto?, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (_, r, f, _, _) => { resume = r; failure = f; })
            .ReturnsAsync(_waiter.Object);

        await new RecoverableAsyncResponseBuilder(_recoverableSubscriber.Object)
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
    public async Task RecoverableBuilder_OrdinaryInterfaceDispatchStillUsesRecoverableWaiter()
    {
        var correlationIds = new List<string>();
        _recoverableSubscriber
            .Setup(s => s.CreateRecoverableResponseWaiter<OperationResult>(
                It.IsAny<string>(),
                null,
                null,
                null,
                null))
            .Callback<string, ReflectionCallDto?, ReflectionCallDto?, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (correlationId, _, _, _, _) => correlationIds.Add(correlationId))
            .ReturnsAsync(_waiter.Object);

        IAsyncResponseBuilder builder = new RecoverableAsyncResponseBuilder(_recoverableSubscriber.Object);

        await builder.For<OperationResult>("corr-attached").WaitAsync();
        await builder.For<OperationResult>().WaitAsync(_ => Task.CompletedTask);

        Assert.Collection(
            correlationIds,
            correlationId => Assert.Equal("corr-attached", correlationId),
            correlationId => Assert.False(string.IsNullOrWhiteSpace(correlationId)));
    }

    [Fact]
    public async Task RecoverableAttachedBuilder_InterfaceFluentMethodsPassConfiguredOptions()
    {
        ReflectionCallDto? resume = null;
        ReflectionCallDto? failure = null;
        TimeSpan? timeout = null;
        Func<OperationResult, ValueTask<bool>>? predicate = null;
        _recoverableSubscriber
            .Setup(s => s.CreateRecoverableResponseWaiter<OperationResult>(
                "corr-1",
                It.IsAny<ReflectionCallDto?>(),
                It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(),
                It.IsAny<TimeSpan?>()))
            .Callback<string, ReflectionCallDto?, ReflectionCallDto?, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (_, r, f, p, t) => { resume = r; failure = f; predicate = p; timeout = t; })
            .ReturnsAsync(_waiter.Object);
        var provider = new Mock<IAsyncResponseReplyTargetProvider>();
        var replyTarget = ReplyTarget("default");
        provider.Setup(p => p.GetReplyTarget(null)).Returns(replyTarget);
        var resumeCallback = RecoveryCallback(nameof(IRecoverySpy.OnResume), PlaceholderType.Payload);
        var failureCallback = RecoveryCallback(nameof(IRecoverySpy.OnFailure), PlaceholderType.Exception);

        IRecoverableAsyncResponseAttachedBuilder<OperationResult> builder =
            new RecoverableAsyncResponseBuilder(_recoverableSubscriber.Object, null, provider.Object)
                .For<OperationResult>("corr-1");

        await builder
            .OnLostSubscriberResume(resumeCallback)
            .OnLostSubscriberFailure(failureCallback)
            .WithTimeout(TimeSpan.FromSeconds(3))
            .WithReplyTarget()
            .Until(payload => payload.Status == OperationStatus.Completed)
            .WaitAsync();

        Assert.Same(resumeCallback, resume);
        Assert.Same(failureCallback, failure);
        Assert.Equal(TimeSpan.FromSeconds(3), timeout);
        Assert.NotNull(predicate);
        Assert.True(await predicate!(new OperationResult { Status = OperationStatus.Completed }));
        Assert.False(await predicate(new OperationResult { Status = OperationStatus.Running }));
        provider.Verify(p => p.GetReplyTarget(null), Times.Once);
    }

    [Fact]
    public async Task RecoverableAttachedBuilder_InterfaceReplyTargetVariantsResolve()
    {
        _recoverableSubscriber
            .Setup(s => s.CreateRecoverableResponseWaiter<OperationResult>(
                It.IsAny<string>(),
                It.IsAny<ReflectionCallDto?>(),
                It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(),
                It.IsAny<TimeSpan?>()))
            .ReturnsAsync(_waiter.Object);
        var provider = new Mock<IAsyncResponseReplyTargetProvider>();
        var namedTarget = ReplyTarget("regional");
        provider.Setup(p => p.GetReplyTarget("regional")).Returns(namedTarget);

        IRecoverableAsyncResponseAttachedBuilder<OperationResult> namedBuilder =
            new RecoverableAsyncResponseBuilder(_recoverableSubscriber.Object, null, provider.Object)
                .For<OperationResult>("corr-named");

        await namedBuilder
            .WithReplyTarget("regional")
            .Until(payload => Task.FromResult(payload.Status == OperationStatus.Completed))
            .WaitAsync();

        IRecoverableAsyncResponseAttachedBuilder<OperationResult> explicitBuilder =
            new RecoverableAsyncResponseBuilder(_recoverableSubscriber.Object)
                .For<OperationResult>("corr-explicit");

        await explicitBuilder
            .WithReplyTarget(ReplyTarget("explicit"))
            .WaitAsync();

        provider.Verify(p => p.GetReplyTarget("regional"), Times.Once);
    }

    [Fact]
    public async Task RecoverableTriggeredBuilder_InterfaceReplyTargetVariantsPassRequestContext()
    {
        _recoverableSubscriber
            .Setup(s => s.CreateRecoverableResponseWaiter<OperationResult>(
                It.IsAny<string>(),
                It.IsAny<ReflectionCallDto?>(),
                It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(),
                It.IsAny<TimeSpan?>()))
            .ReturnsAsync(_waiter.Object);
        var provider = new Mock<IAsyncResponseReplyTargetProvider>();
        var defaultTarget = ReplyTarget("default");
        var namedTarget = ReplyTarget("regional");
        provider.Setup(p => p.GetReplyTarget(null)).Returns(defaultTarget);
        provider.Setup(p => p.GetReplyTarget("regional")).Returns(namedTarget);

        IRecoverableAsyncResponseTriggeredBuilder<OperationResult> defaultBuilder =
            new RecoverableAsyncResponseBuilder(_recoverableSubscriber.Object, null, provider.Object)
                .For<OperationResult>();
        AsyncResponseReplyTarget? observedDefault = null;
        await defaultBuilder.WithReplyTarget().WaitAsync(context =>
        {
            observedDefault = context.ReplyTarget;
            return Task.CompletedTask;
        });

        IRecoverableAsyncResponseTriggeredBuilder<OperationResult> namedBuilder =
            new RecoverableAsyncResponseBuilder(_recoverableSubscriber.Object, null, provider.Object)
                .For<OperationResult>();
        AsyncResponseReplyTarget? observedNamed = null;
        await namedBuilder.WithReplyTarget("regional").WaitAsync(context =>
        {
            observedNamed = context.ReplyTarget;
            return Task.CompletedTask;
        });

        IRecoverableAsyncResponseTriggeredBuilder<OperationResult> explicitBuilder =
            new RecoverableAsyncResponseBuilder(_recoverableSubscriber.Object)
                .For<OperationResult>();
        var explicitTarget = ReplyTarget("explicit");
        AsyncResponseReplyTarget? observedExplicit = null;
        await explicitBuilder.WithReplyTarget(explicitTarget).WaitAsync(context =>
        {
            observedExplicit = context.ReplyTarget;
            return Task.CompletedTask;
        });

        Assert.Same(defaultTarget, observedDefault);
        Assert.Same(namedTarget, observedNamed);
        Assert.Same(explicitTarget, observedExplicit);
        provider.Verify(p => p.GetReplyTarget(null), Times.Once);
        provider.Verify(p => p.GetReplyTarget("regional"), Times.Once);
    }

    [Fact]
    public void OrdinaryBuilderInterfaces_DoNotExposeLostSubscriberCallbacks()
    {
        Assert.DoesNotContain(
            typeof(IAsyncResponseAttachedBuilder<OperationResult>).GetMethods(),
            method => method.Name.StartsWith("OnLostSubscriber", StringComparison.Ordinal));
        Assert.DoesNotContain(
            typeof(IAsyncResponseTriggeredBuilder<OperationResult>).GetMethods(),
            method => method.Name.StartsWith("OnLostSubscriber", StringComparison.Ordinal));
        Assert.Contains(
            typeof(IRecoverableAsyncResponseAttachedBuilder<OperationResult>).GetMethods(),
            method => method.Name == nameof(IRecoverableAsyncResponseAttachedBuilder<OperationResult>.OnLostSubscriberResume));
        Assert.Contains(
            typeof(IRecoverableAsyncResponseTriggeredBuilder<OperationResult>).GetMethods(),
            method => method.Name == nameof(IRecoverableAsyncResponseTriggeredBuilder<OperationResult>.OnLostSubscriberFailure));
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

    [Fact]
    public async Task EnqueueWorker_PassesCancellationToTransport()
    {
        var transport = new Mock<IWorkerTransport>();
        CancellationToken observed = default;
        transport
            .Setup(t => t.PublishAsync(It.IsAny<WorkerJobEnvelope>(), It.IsAny<CancellationToken>()))
            .Callback<WorkerJobEnvelope, CancellationToken>((_, token) => observed = token)
            .Returns(Task.CompletedTask);
        using var source = new CancellationTokenSource();
        var builder = new AsyncResponseBuilder(_subscriber.Object, transport.Object);

        await builder.EnqueueWorkerAsync<IRecoverySpy>(spy => spy.OnWorkerJob(7), source.Token);

        Assert.Equal(source.Token, observed);
    }

    [Fact]
    public void WithTimeout_RejectsNonPositiveTimeout()
    {
        var builder = new AsyncResponseBuilder(_subscriber.Object).For<OperationResult>("corr-1");

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithTimeout(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithTimeout(TimeSpan.FromTicks(-1)));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void For_WithBlankCorrelationId_Throws(string? correlationId)
    {
        var builder = new AsyncResponseBuilder(_subscriber.Object);

        Assert.Throws<ArgumentNullException>(() => builder.For<OperationResult>(correlationId!));
    }

    [Fact]
    public async Task WithReplyTarget_WithoutProvider_ThrowsWithGuidance()
    {
        var builder = new AsyncResponseBuilder(_subscriber.Object)
            .For<OperationResult>()
            .WithReplyTarget();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            builder.WaitAsync(_ => Task.CompletedTask));

        Assert.Contains("reply target provider", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void WithReplyTarget_RejectsBlankName()
    {
        var builder = new AsyncResponseBuilder(_subscriber.Object).For<OperationResult>("corr-1");

        Assert.Throws<ArgumentException>(() => builder.WithReplyTarget(" "));
    }

    [Theory]
    [InlineData(null, "transport", "test://reply")]
    [InlineData("default", null, "test://reply")]
    [InlineData("default", "transport", null)]
    public void WithReplyTarget_ValidatesExplicitTarget(string? name, string? transport, string? address)
    {
        var builder = new AsyncResponseBuilder(_subscriber.Object).For<OperationResult>("corr-1");

        Assert.ThrowsAny<ArgumentException>(() => builder.WithReplyTarget(new AsyncResponseReplyTarget
        {
            Name = name!,
            Transport = transport!,
            Address = address!
        }));
    }

    [Fact]
    public void Until_RejectsNullPredicates()
    {
        var builder = new AsyncResponseBuilder(_subscriber.Object).For<OperationResult>("corr-1");

        Assert.Throws<ArgumentNullException>(() => builder.Until((Func<OperationResult, bool>)null!));
        Assert.Throws<ArgumentNullException>(() => builder.Until((Func<OperationResult, Task<bool>>)null!));
    }

    [Fact]
    public async Task TriggeredBuilder_InterfaceFluentMethodsPassConfiguredOptions()
    {
        ReflectionCallDto? resume = null;
        ReflectionCallDto? failure = null;
        TimeSpan? timeout = null;
        Func<OperationResult, ValueTask<bool>>? predicate = null;
        _recoverableSubscriber
            .Setup(s => s.CreateRecoverableResponseWaiter<OperationResult>(
                It.IsAny<string>(), It.IsAny<ReflectionCallDto?>(), It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(), It.IsAny<TimeSpan?>()))
            .Callback<string, ReflectionCallDto?, ReflectionCallDto?, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (_, r, f, p, t) => { resume = r; failure = f; predicate = p; timeout = t; })
            .ReturnsAsync(_waiter.Object);
        var resumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
            MethodName = nameof(IRecoverySpy.OnResume),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
        };
        var failureCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
            MethodName = nameof(IRecoverySpy.OnFailure),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
        };

        IRecoverableAsyncResponseTriggeredBuilder<OperationResult> builder = new RecoverableAsyncResponseBuilder(_recoverableSubscriber.Object)
            .For<OperationResult>();

        await builder
            .OnLostSubscriberResume(resumeCallback)
            .OnLostSubscriberFailure(failureCallback)
            .WithTimeout(TimeSpan.FromSeconds(3))
            .Until(payload => payload.Status == OperationStatus.Completed)
            .WaitAsync(_ => Task.CompletedTask);

        Assert.Same(resumeCallback, resume);
        Assert.Same(failureCallback, failure);
        Assert.Equal(TimeSpan.FromSeconds(3), timeout);
        Assert.NotNull(predicate);
        Assert.True(await predicate!(new OperationResult { Status = OperationStatus.Completed }));
        Assert.False(await predicate(new OperationResult { Status = OperationStatus.Running }));
    }

    [Fact]
    public async Task TriggeredBuilder_TaskPredicateViaOrdinaryInterface_PassesConfiguredOptions()
    {
        Func<OperationResult, ValueTask<bool>>? predicate = null;
        _subscriber
            .Setup(s => s.CreateResponseWaiter<OperationResult>(
                It.IsAny<string>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(),
                It.IsAny<TimeSpan?>()))
            .Callback<string, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (_, p, _) => predicate = p)
            .ReturnsAsync(_waiter.Object);

        IAsyncResponseTriggeredBuilder<OperationResult> builder =
            new AsyncResponseBuilder(_subscriber.Object).For<OperationResult>();

        await builder
            .Until(payload => Task.FromResult(payload.Status == OperationStatus.Completed))
            .WaitAsync(_ => Task.CompletedTask);

        Assert.NotNull(predicate);
        Assert.True(await predicate!(new OperationResult { Status = OperationStatus.Completed }));
        Assert.False(await predicate(new OperationResult { Status = OperationStatus.Running }));
    }

    [Fact]
    public void RecoverableBuilder_RejectsNullReflectionCallbacks()
    {
        IRecoverableAsyncResponseAttachedBuilder<OperationResult> attached =
            new RecoverableAsyncResponseBuilder(_recoverableSubscriber.Object).For<OperationResult>("corr-1");
        IRecoverableAsyncResponseTriggeredBuilder<OperationResult> triggered =
            new RecoverableAsyncResponseBuilder(_recoverableSubscriber.Object).For<OperationResult>();

        Assert.Throws<ArgumentNullException>(() => attached.OnLostSubscriberResume((ReflectionCallDto)null!));
        Assert.Throws<ArgumentNullException>(() => attached.OnLostSubscriberFailure((ReflectionCallDto)null!));
        Assert.Throws<ArgumentNullException>(() => triggered.OnLostSubscriberResume((ReflectionCallDto)null!));
        Assert.Throws<ArgumentNullException>(() => triggered.OnLostSubscriberFailure((ReflectionCallDto)null!));
    }

    [Fact]
    public async Task TriggeredBuilder_ExpressionCallbacksAndTaskPredicate_PassConfiguredOptions()
    {
        ReflectionCallDto? resume = null;
        ReflectionCallDto? failure = null;
        Func<OperationResult, ValueTask<bool>>? predicate = null;
        _recoverableSubscriber
            .Setup(s => s.CreateRecoverableResponseWaiter<OperationResult>(
                It.IsAny<string>(), It.IsAny<ReflectionCallDto?>(), It.IsAny<ReflectionCallDto?>(),
                It.IsAny<Func<OperationResult, ValueTask<bool>>?>(), It.IsAny<TimeSpan?>()))
            .Callback<string, ReflectionCallDto?, ReflectionCallDto?, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (_, r, f, p, _) => { resume = r; failure = f; predicate = p; })
            .ReturnsAsync(_waiter.Object);

        IRecoverableAsyncResponseTriggeredBuilder<OperationResult> builder = new RecoverableAsyncResponseBuilder(_recoverableSubscriber.Object)
            .For<OperationResult>();

        await builder
            .OnLostSubscriberResume<IRecoverySpy>(spy => spy.OnResume(Placeholder.Payload<OperationResult>()))
            .OnLostSubscriberFailure<IRecoverySpy>(spy => spy.OnFailure(Placeholder.Exception()))
            .Until(payload => Task.FromResult(payload.Status == OperationStatus.Completed))
            .WaitAsync(_ => Task.CompletedTask);

        Assert.Equal(nameof(IRecoverySpy.OnResume), resume!.MethodName);
        Assert.Equal(nameof(IRecoverySpy.OnFailure), failure!.MethodName);
        Assert.NotNull(predicate);
        Assert.True(await predicate!(new OperationResult { Status = OperationStatus.Completed }));
    }

    [Fact]
    public async Task AttachedBuilder_ExplicitReplyTargetAndTaskPredicate_ArePassedToSubscriber()
    {
        AsyncResponseReplyTarget? observedReplyTarget = null;
        Func<OperationResult, ValueTask<bool>>? predicate = null;
        _subscriber
            .Setup(s => s.CreateResponseWaiter<OperationResult>(
                "corr-1", It.IsAny<Func<OperationResult, ValueTask<bool>>?>(), It.IsAny<TimeSpan?>()))
            .Callback<string, Func<OperationResult, ValueTask<bool>>?, TimeSpan?>(
                (_, p, _) => predicate = p)
            .ReturnsAsync(_waiter.Object);
        var replyTarget = new AsyncResponseReplyTarget
        {
            Name = "explicit",
            Transport = "test",
            Address = "test://explicit"
        };

        await new AsyncResponseBuilder(_subscriber.Object)
            .For<OperationResult>("corr-1")
            .WithReplyTarget(replyTarget)
            .Until(payload => Task.FromResult(payload.Status == OperationStatus.Completed))
            .WaitAsync();

        Assert.NotNull(predicate);
        Assert.True(await predicate!(new OperationResult { Status = OperationStatus.Completed }));
        Assert.False(await predicate(new OperationResult { Status = OperationStatus.Running }));
        _waiter.Verify(w => w.DisposeAsync(), Times.Once);

        await new AsyncResponseBuilder(_subscriber.Object)
            .For<OperationResult>()
            .WithReplyTarget(replyTarget)
            .WaitAsync(context =>
            {
                observedReplyTarget = context.ReplyTarget;
                return Task.CompletedTask;
            });

        Assert.Same(replyTarget, observedReplyTarget);
    }

    [Fact]
    public async Task EnqueueWorker_ExpressionOverloadsSupportActionAndValueTask()
    {
        var transport = new Mock<IWorkerTransport>();
        var calls = new List<ReflectionCallDto>();
        transport
            .Setup(t => t.PublishAsync(It.IsAny<WorkerJobEnvelope>(), It.IsAny<CancellationToken>()))
            .Callback<WorkerJobEnvelope, CancellationToken>((job, _) => calls.Add(job.Call))
            .Returns(Task.CompletedTask);
        var builder = new AsyncResponseBuilder(_subscriber.Object, transport.Object);

        await builder.EnqueueWorkerAsync<IExpressionWorker>(worker => worker.Run(42));
        await builder.EnqueueWorkerAsync<IExpressionWorker>(worker => worker.RunAsync());

        Assert.Collection(
            calls,
            call =>
            {
                Assert.Equal(nameof(IExpressionWorker.Run), call.MethodName);
                Assert.Equal(42, Assert.Single(call.Params).Value);
            },
            call => Assert.Equal(nameof(IExpressionWorker.RunAsync), call.MethodName));
    }

    private static AsyncResponseReplyTarget ReplyTarget(string name) => new()
    {
        Name = name,
        Transport = "test",
        Address = $"test://{name}"
    };

    private static ReflectionCallDto RecoveryCallback(string methodName, PlaceholderType placeholder) => new()
    {
        ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
        MethodName = methodName,
        Params = [CallbackParam.ForPlaceholder(placeholder)]
    };
}

public interface IExpressionWorker
{
    void Run(int value);
    ValueTask RunAsync();
}
