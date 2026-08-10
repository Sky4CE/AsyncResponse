using AsyncResponse.Channels.NATS;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// NATS channel paths the main suite leaves untouched: the re-publish taken when a waiter subscribes
/// between the failed delivery and the recovery-state read, the cleanup fallbacks when tearing a
/// subscription down fails, and the diagnostics tagging on every publish entry point.
/// </summary>
public sealed class NatsChannelCoverageTests
{
    private readonly FakeNatsResponseChannelClient _client = new();
    private readonly Mock<IRecoveryStateStore> _store = new();
    private readonly ServiceProvider _services;

    public NatsChannelCoverageTests()
    {
        _store.Setup(store => store.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _store.Setup(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<RecoveryState>());
        _store.Setup(store => store.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        _services = services.BuildServiceProvider();
    }

    /// <summary>
    /// "No responders" plus a probe that finds a live subscriber means the snapshot was stale: the
    /// publish is retried live, and a successful retry consumes no recovery registration.
    /// </summary>
    [Theory]
    [InlineData(PublishKind.Response)]
    [InlineData(PublishKind.RawJson)]
    [InlineData(PublishKind.Exception)]
    public async Task Publish_RetriesLiveWhenAWaiterRaced_AndStopsOnceItLands(PublishKind kind)
    {
        // A listener is attached so the delivery/recovery tags inside the retry block are exercised.
        using var activities = new AsyncResponseActivityCollector();
        var channel = CreateChannel();
        // First delivery finds nobody; the probe says a waiter is live; the retry then lands.
        _client.DeliveryOutcomes.Enqueue(NatsDeliveryOutcome.NoResponders);
        _client.DeliveryOutcomes.Enqueue(NatsDeliveryOutcome.Replied);
        _client.OutcomeForProbe = _ => NatsDeliveryOutcome.Replied;

        await PublishAsync(channel, kind, "corr-retry-lands");

        // Two non-probe deliveries: the original and the retry.
        Assert.Equal(2, _client.Requests.Count(request => !request.Probe));
        // The dispatcher reads the recovery state once before its live re-check; because the retry
        // landed, it is never read a second time and the registration is left intact.
        _store.Verify(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    /// <summary>
    /// A retry that also finds no responders means the waiter really is gone: only then is the
    /// recovery registration consumed.
    /// </summary>
    [Theory]
    [InlineData(PublishKind.Response)]
    [InlineData(PublishKind.RawJson)]
    [InlineData(PublishKind.Exception)]
    public async Task Publish_ConsumesRecoveryOnlyAfterASecondNoResponders(PublishKind kind)
    {
        using var activities = new AsyncResponseActivityCollector();
        var channel = CreateChannel();
        _client.NextOutcome = NatsDeliveryOutcome.NoResponders;
        // The probe reports a live subscriber once — enough to force the retry — then agrees the
        // waiter is gone, so the second dispatch consumes the registration.
        var probes = 0;
        _client.OutcomeForProbe = _ => probes++ == 0 ? NatsDeliveryOutcome.Replied : NatsDeliveryOutcome.NoResponders;

        await PublishAsync(channel, kind, "corr-retry-fails");

        Assert.Equal(2, _client.Requests.Count(request => !request.Probe));
        _store.Verify(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
        // The second dispatch consumed the registration only because ITS liveness re-check agreed
        // the waiter is gone — the probe must have been consulted both times.
        Assert.Equal(2, probes);
    }

    /// <summary>
    /// Delivery keeps reporting no responders while the probe keeps reporting a live waiter — a
    /// contradiction (subscription interest not yet visible server-side, or a stale heartbeat).
    /// Consuming recovery registrations on that evidence would strip a live waiter of its recovery
    /// arm, so after the bounded retry the publish leaves all state intact.
    /// </summary>
    [Theory]
    [InlineData(PublishKind.Response)]
    [InlineData(PublishKind.RawJson)]
    [InlineData(PublishKind.Exception)]
    public async Task Publish_LeavesRecoveryIntactWhileProbeKeepsReportingALiveWaiter(PublishKind kind)
    {
        using var activities = new AsyncResponseActivityCollector();
        var channel = CreateChannel();
        _client.NextOutcome = NatsDeliveryOutcome.NoResponders;
        _client.OutcomeForProbe = _ => NatsDeliveryOutcome.Replied;
        _store.Setup(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync([NewRecoveryState("corr-contradiction")]);

        await PublishAsync(channel, kind, "corr-contradiction");

        // Bounded: one retry, then hands off to the intact registration instead of a third publish.
        Assert.Equal(2, _client.Requests.Count(request => !request.Probe));
        Assert.Equal(2, _client.Requests.Count(request => request.Probe));
        _store.Verify(store => store.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private static RecoveryState NewRecoveryState(string correlationId) => new()
    {
        RegistrationId = Guid.NewGuid(),
        CorrelationId = correlationId,
        PayloadTypeFullName = typeof(OperationResult).FullName,
        RegisteredAtUtc = DateTime.UtcNow,
        ResumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
            MethodName = nameof(IRecoverySpy.OnResume),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
        },
        FailureCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
            MethodName = nameof(IRecoverySpy.OnFailure),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
        }
    };

    /// <summary>
    /// Every publish entry point opens an activity and tags the channel, the delivery outcome and
    /// (for exceptions) the exception type.
    /// </summary>
    [Fact]
    public async Task PublishPaths_TagTheirActivityWhenAListenerIsAttached()
    {
        using var activities = new AsyncResponseActivityCollector();
        var channel = CreateChannel();

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr-tags");
        await ((IRawAsyncResponsePublisher)channel).SetRawResponseJson("""{"Status":2}""", "corr-tags");
        await channel.SetException(new InvalidOperationException("boom"), "corr-tags");

        foreach (var name in new[] { "asyncresponse.set_response", "asyncresponse.ingress.raw_response", "asyncresponse.set_exception" })
            activities.Single(name, "asyncresponse.channel", "nats");

        var setException = activities.Single("asyncresponse.set_exception", "asyncresponse.channel", "nats");
        Assert.Equal(
            typeof(InvalidOperationException).FullName,
            AsyncResponseActivityCollector.Tag(setException, "asyncresponse.exception_type"));
    }

    /// <summary>
    /// The waiter's own activity is tagged at registration, including the effective timeout.
    /// </summary>
    [Fact]
    public async Task CreateResponseWaiter_TagsTheWaitActivity()
    {
        using var activities = new AsyncResponseActivityCollector();
        var channel = CreateChannel();

        var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-wait-tags", timeout: TimeSpan.FromSeconds(7));
        // The wait activity is only reported once it stops, which is part of the waiter's cleanup.
        await waiter.DisposeAsync();

        var activity = activities.Single("asyncresponse.wait", "asyncresponse.channel", "nats");
        Assert.Equal(7d, AsyncResponseActivityCollector.Tag(activity, "asyncresponse.timeout_seconds"));
    }

    /// <summary>
    /// A subscription teardown that throws is logged and must not skip the fallback cancel: the
    /// server-side subscription is still pumping, and only cancelling its token ends the consume loop.
    /// </summary>
    [Fact]
    public async Task Cleanup_CancelsTheSubscriptionTokenWhenTeardownThrows()
    {
        var client = new FakeNatsResponseChannelClient
        {
            SubscriptionDisposeOverride = () => throw new InvalidOperationException("teardown failed")
        };
        var logger = new CollectingLogger();
        var channel = CreateChannel(client, logger.For<NatsAsyncResponseChannel>());

        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-teardown");
        await waiter.DisposeAsync();

        Assert.Contains(logger.Messages, message => message.StartsWith("Error during cleanup for subject", StringComparison.Ordinal));
        // The teardown failed, so the consume loop is ended by cancelling its token instead.
        Assert.True(client.SubscriptionLifetime.IsCancellationRequested);
    }

    [Fact]
    public async Task TeardownThrowWithDeliveryInFlight_FaultsIndeterminate_NotCanceled()
    {
        // A teardown failure proves NOTHING about a delivery mid-predicate: the drain must still
        // join the consume loop within the remaining budget, and when that cannot prove
        // settlement it must fault as indeterminate — the generic teardown-throw path used to
        // shortcut straight to the cleanup's cancel, telling a re-attaching flow "nothing was
        // delivered" about a message the stream had already handed over.
        var client = new FakeNatsResponseChannelClient
        {
            SubscriptionDisposeOverride = () => throw new InvalidOperationException("teardown failed")
        };
        var logger = new CollectingLogger();
        var channel = CreateChannel(client, logger.For<NatsAsyncResponseChannel>(), drainTimeout: TimeSpan.FromMilliseconds(200));

        using var predicateEntered = new SemaphoreSlim(0);
        using var releasePredicate = new SemaphoreSlim(0);
        var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-teardown-wedged",
            async _ =>
            {
                predicateEntered.Release();
                return await releasePredicate.WaitAsync(TimeSpan.FromSeconds(30));
            });

        client.Push(TerminalEnvelope("wedged"));
        Assert.True(await predicateEntered.WaitAsync(TimeSpan.FromSeconds(5)), "the delivery never reached the Until predicate");

        await waiter.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        var fault = await Assert.ThrowsAsync<AsyncResponseIndeterminateDeliveryException>(
            () => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal("corr-teardown-wedged", fault.CorrelationId);
        Assert.False(waiter.ResponseTask.IsCanceled);
        // Both halves of the contract: the teardown failure stays loud, AND it did not decide
        // the settlement.
        Assert.Contains(logger.Messages, message => message.StartsWith("Error during cleanup for subject", StringComparison.Ordinal));

        releasePredicate.Release();
    }

    /// <summary>
    /// Terminal delivery starts cleanup first, so a disposing waiter awaits the LATCHED core —
    /// the drain skips itself once <c>cleanupStarted</c> is set. The core's teardown must
    /// therefore carry the same <c>DisposalDrainTimeout</c> bound: a hanging client-library
    /// dispose used to hold <c>waiter.DisposeAsync()</c> pending indefinitely.
    /// </summary>
    [Fact]
    public async Task TerminalCleanup_HangingTeardown_KeepsDisposalBounded_AndLogsTheLateOutcome()
    {
        var hang = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new FakeNatsResponseChannelClient
        {
            SubscriptionDisposeOverride = () => new ValueTask(hang.Task)
        };
        var logger = new CollectingLogger();
        var channel = CreateChannel(client, logger.For<NatsAsyncResponseChannel>(), drainTimeout: TimeSpan.FromMilliseconds(200));

        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-hang");
        client.Push(TerminalEnvelope("done"));

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("done", result.Message);

        // The delivered response settled the task; disposal joins the terminal cleanup, whose
        // hanging teardown must be abandoned at the budget — not hold DisposeAsync hostage.
        await waiter.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        // The abandoned teardown finally FAILS: the latched, never-faulting teardown must log
        // that late outcome — it used to die as a TaskScheduler.UnobservedTaskException that
        // never reached the channel logger. Asserted on the SPECIFIC exception: the core's own
        // budget lapse already logs a TimeoutException-flavoured "Error during cleanup" entry
        // before this point, which a message-only check mistook for proof.
        hang.TrySetException(new InvalidOperationException("late teardown boom"));
        await Eventually(() => logger.Entries.Any(entry =>
            entry.Message.StartsWith("Error during cleanup for subject", StringComparison.Ordinal)
            && entry.Exception is InvalidOperationException { Message: "late teardown boom" }));
    }

    /// <summary>
    /// A waiter disposed while the teardown hangs: the drain spends DisposalDrainTimeout on the
    /// latched teardown task, and the latched cleanup must NOT spend a second full budget on the
    /// same task — 200 ms configured used to cost ~410 ms, and the core's second lapse stamped a
    /// spurious TimeoutException "Error during cleanup" entry.
    /// </summary>
    [Fact]
    public async Task Dispose_HangingTeardown_SpendsTheBudgetOnce()
    {
        var hang = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new FakeNatsResponseChannelClient
        {
            SubscriptionDisposeOverride = () => new ValueTask(hang.Task)
        };
        var logger = new CollectingLogger();
        var channel = CreateChannel(client, logger.For<NatsAsyncResponseChannel>(), drainTimeout: TimeSpan.FromMilliseconds(200));

        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-single-budget");
        await waiter.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        // Settlement was unprovable within the budget (the teardown never finished, so the loop
        // was never joined): indeterminate, per the drain contract.
        await Assert.ThrowsAsync<AsyncResponseIndeterminateDeliveryException>(
            () => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));

        // The core skipped its second wait on the drain-budgeted teardown: no TimeoutException
        // cleanup-error entry may exist (the teardown's own late outcome still logs when it
        // completes — covered by the late-outcome fact above).
        Assert.DoesNotContain(logger.Entries, entry =>
            entry.Message.StartsWith("Error during cleanup for subject", StringComparison.Ordinal));

        hang.TrySetResult();
    }

    private NatsAsyncResponseChannel CreateChannel(
        FakeNatsResponseChannelClient client,
        ILogger<NatsAsyncResponseChannel> logger,
        TimeSpan? drainTimeout = null) => new(
        _services.GetRequiredService<IServiceScopeFactory>(),
        client,
        _store.Object,
        Options.Create(new NatsAsyncResponseChannelOptions
        {
            DefaultTimeout = TimeSpan.FromMinutes(1),
            DisposalDrainTimeout = drainTimeout ?? TimeSpan.FromSeconds(30)
        }),
        new AsyncResponseContextPropagation([]),
        logger);

    private static string TerminalEnvelope(string? message = null)
        => System.Text.Json.JsonSerializer.Serialize(
            new AsyncResponseEnvelope<OperationResult>
            {
                Success = true,
                Payload = new OperationResult { Status = OperationStatus.Completed, Message = message }
            },
            AsyncResponseEnvelopeOptions<OperationResult>.Instance);

    /// <summary>
    /// A subscription that faults after the waiter already completed cannot fault the task twice;
    /// the loser is logged rather than lost.
    /// </summary>
    /// <remarks>
    /// The ordering is carried by the iterator itself — terminal envelope, then throw — rather than
    /// by the test faulting the stream after the fact. Doing it from the outside races the waiter's
    /// own cleanup, which tears the subscription down the moment the response lands, so the fault
    /// would usually arrive with no reader left to observe it.
    /// </remarks>
    [Fact]
    public async Task SubscriptionFailure_AfterCompletion_IsLoggedNotSwallowed()
    {
        var terminal = System.Text.Json.JsonSerializer.Serialize(
            new AsyncResponseEnvelope<OperationResult>
            {
                Success = true,
                Payload = new OperationResult { Status = OperationStatus.Completed }
            },
            AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        var client = new Mock<INatsResponseChannelClient>();
        client.Setup(instance => instance.SubscribeAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new TerminalThenFaultSubscription(terminal));
        client.Setup(instance => instance.FlushAsync(It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        var logger = new CollectingLogger();
        var channel = new NatsAsyncResponseChannel(
            _services.GetRequiredService<IServiceScopeFactory>(),
            client.Object,
            _store.Object,
            Options.Create(new NatsAsyncResponseChannelOptions { DefaultTimeout = TimeSpan.FromSeconds(5) }),
            new AsyncResponseContextPropagation([]),
            logger.For<NatsAsyncResponseChannel>());

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-late-failure");
        Assert.Equal(OperationStatus.Completed, (await waiter.ResponseTask).Status);

        await Eventually(() => logger.Messages.Any(
            message => message.StartsWith("TaskCompletionSource already completed", StringComparison.Ordinal)));
    }

    /// <summary>Registration that fails mid-way rethrows rather than handing back a doomed waiter.</summary>
    [Fact]
    public async Task CreateResponseWaiter_RethrowsWhenTheRecoveryStateCannotBeSaved()
    {
        _store.Setup(store => store.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("kv store down"));
        var channel = CreateChannel();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => channel.CreateResponseWaiter<OperationResult>("corr-save-fails", timeout: TimeSpan.FromSeconds(5)));
    }

    public enum PublishKind
    {
        Response,
        RawJson,
        Exception
    }

    private static Task PublishAsync(NatsAsyncResponseChannel channel, PublishKind kind, string correlationId)
        => kind switch
        {
            PublishKind.Response => channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, correlationId),
            PublishKind.RawJson => ((IRawAsyncResponsePublisher)channel).SetRawResponseJson("""{"Status":2}""", correlationId),
            _ => channel.SetException(new InvalidOperationException("boom"), correlationId)
        };

    private NatsAsyncResponseChannel CreateChannel() => new(
        _services.GetRequiredService<IServiceScopeFactory>(),
        _client,
        _store.Object,
        Options.Create(new NatsAsyncResponseChannelOptions
        {
            DefaultTimeout = TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5)
        }),
        new AsyncResponseContextPropagation([]),
        new TestLogger<NatsAsyncResponseChannel>());

    private static async Task Eventually(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        while (!condition())
            await Task.Delay(10, timeout.Token);
    }

    /// <summary>
    /// Hands over one terminal envelope and then faults, in that order. Yielding before throwing is
    /// what makes the "stream died after the waiter completed" sequence deterministic.
    /// </summary>
    private sealed class TerminalThenFaultSubscription(string terminalEnvelopeJson) : INatsChannelSubscription
    {
        public async IAsyncEnumerable<NatsInboundResponse> ReadAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            yield return new NatsInboundResponse(terminalEnvelopeJson, IsProbe: false, () => ValueTask.CompletedTask);
            await Task.Yield();
            throw new InvalidOperationException("stream died");
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }



}
