using AsyncResponse.Channels.NATS;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public class NatsAsyncResponseChannelTests
{
    private readonly FakeNatsResponseChannelClient _client = new();
    private readonly Mock<IRecoveryStateStore> _store = new();
    private readonly NatsRecoverySpy _spy = new();
    private readonly ServiceProvider _services;

    public NatsAsyncResponseChannelTests()
    {
        _store.Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _store.Setup(s => s.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Array.Empty<RecoveryState>());
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<INatsRecoverySpy>(_spy);
        _services = services.BuildServiceProvider();
    }

    private static RecoveryState ArmedState(string correlationId) => new()
    {
        CorrelationId = correlationId,
        PayloadTypeFullName = typeof(OperationResult).FullName,
        ResumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(INatsRecoverySpy).FullName!,
            MethodName = nameof(INatsRecoverySpy.ResumeAsync),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload), CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId)]
        },
        FailureCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = typeof(INatsRecoverySpy).FullName!,
            MethodName = nameof(INatsRecoverySpy.FailAsync),
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception), CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId)]
        }
    };

    [Fact]
    public async Task CreateResponseWaiter_CompletesFromDeliveredResponse_AndSavesRecoveryState()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5));

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "done" }, "corr-a");

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("done", result.Message);
        Assert.Equal($"asyncresponse.response.{NatsSubjectSchema.Encode("corr-a")}", _client.SubscribedSubjects[0]);
        Assert.True(_client.FlushCount >= 1);
        _store.Verify(s => s.SaveAsync(
            "corr-a",
            It.Is<RecoveryState>(state => state.CorrelationId == "corr-a" && state.PayloadTypeFullName == typeof(OperationResult).FullName),
            It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateResponseWaiter_WithoutExecutionContext_UsesRecoveryExpiryTimeoutFallback()
    {
        Task<IAsyncResponseWaiter<OperationResult>> waiterTask;
        using (ExecutionContext.SuppressFlow())
        {
            waiterTask = CreateChannel(useRecoveryExpiry: true)
                .CreateResponseWaiter<OperationResult>("no-context");
        }

        await using var waiter = await waiterTask;
        _client.Push(JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed }
        }, AsyncResponseEnvelopeOptions<OperationResult>.Instance));
        Assert.Equal(OperationStatus.Completed, (await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Status);
    }

    [Fact]
    public async Task DuplicateTerminalMessages_DoNotReplaceFirstCompletion()
    {
        var channel = CreateChannel();
        await using var success = await channel.CreateResponseWaiter<OperationResult>("duplicate");
        var successJson = JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed, Message = "first" }
        }, AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        _client.Push(successJson);
        _client.Push(successJson);

        Assert.Equal("first", (await success.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("{not-json")]
    [InlineData("{\"SchemaVersion\":999,\"Success\":true,\"Payload\":{\"Status\":2}}")]
    [InlineData("{\"SchemaVersion\":1,\"Success\":false,\"ExceptionMessage\":\"remote\"}")]
    public async Task DuplicateFaultMessages_KeepFirstFailure(string payload)
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("duplicate-fault");

        _client.Push(payload);
        _client.Push(payload);

        await Assert.ThrowsAnyAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task CreateResponseWaiter_CompletionPredicateWaitsForLaterMessages()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-a",
            completionPredicate: payload => new ValueTask<bool>(payload.Status == OperationStatus.Completed),
            timeout: TimeSpan.FromSeconds(5));

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Running }, "corr-a");
        await Task.Delay(50);
        Assert.False(waiter.ResponseTask.IsCompleted);

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "done" }, "corr-a");
        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("done", result.Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_RemoteFailureFaultsWaiter()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5));

        await channel.SetException(new InvalidOperationException("remote failed"), "corr-a");

        var ex = await Assert.ThrowsAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal("remote failed", ex.Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_RemoteFailureCarriesCappedStackTrace()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5));
        Exception failure;
        try
        {
            throw new InvalidOperationException("remote failed");
        }
        catch (Exception ex)
        {
            failure = ex;
        }

        await channel.SetException(failure, "corr-a");

        var caught = await Assert.ThrowsAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal("remote failed", caught.Message);
        Assert.Contains(nameof(CreateResponseWaiter_RemoteFailureCarriesCappedStackTrace), (string)caught.Data["RemoteStackTrace"]!);
    }

    [Fact]
    public async Task CreateResponseWaiter_MalformedMessageFaultsWaiter()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5));

        _client.Push("{not-json");

        await Assert.ThrowsAsync<JsonException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task CreateResponseWaiter_NullEnvelopeFaultsWaiter()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5));

        _client.Push("null");

        await Assert.ThrowsAsync<JsonException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task CreateResponseWaiter_UnsupportedEnvelopeSchemaFaultsWaiter()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5));

        _client.Push("""{"SchemaVersion":999,"Success":true,"Payload":{"Status":2}}""");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Contains("does not support", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateResponseWaiter_SyncPredicateFailureFaultsWaiter()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-a",
            _ => throw new InvalidOperationException("predicate failed"),
            timeout: TimeSpan.FromSeconds(5));

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr-a");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal("predicate failed", ex.Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_IgnoresProbeAndEmptyMessages()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5));

        _client.Push(payload: null, isProbe: true);   // liveness probe — must be ignored
        _client.Push(payload: null, isProbe: false);  // empty body — must be ignored, not fault
        await Task.Delay(50);
        Assert.False(waiter.ResponseTask.IsCompleted);

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "after" }, "corr-a");
        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("after", result.Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_AckFailureDoesNotAbortProcessing()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5));
        var envelope = JsonSerializer.Serialize(
            new AsyncResponseEnvelope<OperationResult>
            {
                Success = true,
                Payload = new OperationResult { Status = OperationStatus.Completed, Message = "after-ack-failure" }
            },
            AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        _client.PushWithReplyFailure(envelope, new InvalidOperationException("ack failed"));

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("after-ack-failure", result.Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_TimesOutAndCleansUp()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-timeout", timeout: TimeSpan.FromMilliseconds(50));

        await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        await Eventually(() => _store.Invocations.Any(i => i.Method.Name == nameof(IRecoveryStateStore.TryDeleteAsync)));
    }

    [Fact]
    public async Task CreateResponseWaiter_SubscribeOrSaveFailureThrowsAndDeletesRecoveryState()
    {
        var failure = new InvalidOperationException("save failed");
        _store.Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(failure);
        var channel = CreateChannel();

        // Must throw rather than return a pre-faulted waiter: the builder's contract is that the
        // trigger only runs once the subscription AND recovery state exist, so a registration
        // failure has to surface before any trigger could fire the remote operation.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5)));
        Assert.Same(failure, ex);
        _store.Verify(s => s.TryDeleteAsync("corr-a", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SetResponse_NoResponders_ConsultsRecoveryStore()
    {
        _client.NextOutcome = NatsDeliveryOutcome.NoResponders;
        var channel = CreateChannel();

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr-lost");

        _store.Verify(s => s.GetAllAsync("corr-lost", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SetResponse_WhenRequestFails_Propagates()
    {
        _client.RequestException = new InvalidOperationException("request failed");
        var channel = CreateChannel();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr-a"));
    }

    [Fact]
    public async Task SetException_NoResponders_ConsultsRecoveryStore()
    {
        _client.NextOutcome = NatsDeliveryOutcome.NoResponders;
        var channel = CreateChannel();

        await channel.SetException(new InvalidOperationException("boom"), "corr-lost");

        _store.Verify(s => s.GetAllAsync("corr-lost", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SetException_WhenRequestFails_Propagates()
    {
        _client.RequestException = new InvalidOperationException("request failed");
        var channel = CreateChannel();

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            channel.SetException(new InvalidOperationException("boom"), "corr-a"));
    }

    [Fact]
    public async Task RawResponseJson_DeliveredToWaiter()
    {
        var channel = CreateChannel();
        var raw = (IRawAsyncResponsePublisher)channel;
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5));

        await raw.SetRawResponseJson("""{"Status":2,"Message":"raw"}""", "corr-a");

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(OperationStatus.Completed, result.Status);
        Assert.Equal("raw", result.Message);
    }

    [Fact]
    public async Task RawResponseJson_WhenRequestFails_Propagates()
    {
        _client.RequestException = new InvalidOperationException("request failed");
        var channel = CreateChannel();
        var raw = (IRawAsyncResponsePublisher)channel;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            raw.SetRawResponseJson("""{"Status":2}""", "corr-a"));
    }

    [Fact]
    public async Task RawObjectResponse_DeliveredToWaiter()
    {
        var channel = CreateChannel();
        var raw = (IRawAsyncResponsePublisher)channel;
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5));

        await raw.SetRawResponse(new OperationResult { Status = OperationStatus.Completed, Message = "typed-raw" }, "corr-a");

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("typed-raw", result.Message);
    }

    [Fact]
    public async Task Publishers_WithBlankCorrelationId_AreNoops()
    {
        var channel = CreateChannel();
        var raw = (IRawAsyncResponsePublisher)channel;

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, " ");
        await raw.SetRawResponseJson("""{"Status":2}""", " ");
        await channel.SetException(new InvalidOperationException("no cid"), " ");

        Assert.Empty(_client.Requests);
    }

    [Fact]
    public async Task CountActiveSubscribers_ReportsPresenceFromProbeOutcome()
    {
        var channel = CreateChannel();

        Assert.Equal(0, await channel.CountActiveSubscribersAsync(" "));

        _client.OutcomeForProbe = _ => NatsDeliveryOutcome.Replied;
        Assert.Equal(1, await channel.CountActiveSubscribersAsync("corr-a"));

        _client.OutcomeForProbe = _ => NatsDeliveryOutcome.NoResponders;
        Assert.Equal(0, await channel.CountActiveSubscribersAsync("corr-a"));

        _client.OutcomeForProbe = _ => NatsDeliveryOutcome.NoReply;
        Assert.Equal(0, await channel.CountActiveSubscribersAsync("corr-a"));
    }

    [Fact]
    public async Task RecoverableWaiter_WithoutShouldResumeOverride_Throws()
    {
        var channel = CreateChannel();
        var callback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "X",
            MethodName = "Y",
            Params = []
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            channel.CreateRecoverableResponseWaiter<UnclassifiedNatsPayload>("corr-a", resumeCallback: callback));
    }

    [Fact]
    public async Task CreateResponseWaiter_RejectsBlankCorrelationId()
    {
        var channel = CreateChannel();
        await Assert.ThrowsAsync<ArgumentNullException>(() => channel.CreateResponseWaiter<OperationResult>(" "));
    }

    [Fact]
    public async Task SetRawResponseJson_NoResponders_ConsultsRecoveryStore()
    {
        _client.NextOutcome = NatsDeliveryOutcome.NoResponders;
        var channel = CreateChannel();
        var raw = (IRawAsyncResponsePublisher)channel;

        await raw.SetRawResponseJson("""{"Status":2,"Message":"late"}""", "corr-lost");

        _store.Verify(s => s.GetAllAsync("corr-lost", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CountActiveSubscribers_ReturnsZeroWhenProbeFails()
    {
        _client.OutcomeForProbe = _ => throw new InvalidOperationException("probe failed");
        var channel = CreateChannel();

        Assert.Equal(0, await channel.CountActiveSubscribersAsync("corr-a"));
    }

    [Fact]
    public async Task CountActiveSubscribers_PropagatesCallerCancellation()
    {
        _client.OutcomeForProbe = _ => throw new OperationCanceledException();
        var channel = CreateChannel();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => channel.CountActiveSubscribersAsync("corr-a", cts.Token).AsTask());
    }

    [Fact]
    public async Task ConsumeLoop_SubscriptionError_FaultsWaiter()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5));

        _client.FailSubscription(new InvalidOperationException("subscription read failed"));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal("subscription read failed", ex.Message);
    }

    [Fact]
    public async Task SetResponse_NoResponders_FiresResumeCallback_AndDeletesState()
    {
        _client.NextOutcome = NatsDeliveryOutcome.NoResponders;
        _store.Setup(s => s.GetAllAsync("corr-x", It.IsAny<CancellationToken>())).ReturnsAsync(new[] { ArmedState("corr-x") });
        var channel = CreateChannel();

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "late" }, "corr-x");

        Assert.NotNull(_spy.Resumed);
        Assert.Equal(OperationStatus.Completed, _spy.Resumed!.Status);
        Assert.Equal("corr-x", _spy.CorrelationId);
        _store.Verify(s => s.TryDeleteAsync("corr-x", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SetException_NoResponders_FiresFailureCallback_AndDeletesState()
    {
        _client.NextOutcome = NatsDeliveryOutcome.NoResponders;
        _store.Setup(s => s.GetAllAsync("corr-x", It.IsAny<CancellationToken>())).ReturnsAsync(new[] { ArmedState("corr-x") });
        var channel = CreateChannel();

        await channel.SetException(new InvalidOperationException("boom"), "corr-x");

        Assert.NotNull(_spy.Failed);
        Assert.Equal("boom", _spy.Failed!.Message);
        _store.Verify(s => s.TryDeleteAsync("corr-x", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Waiter_DisposeAsync_RunsCleanup()
    {
        var channel = CreateChannel();
        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-dispose", timeout: TimeSpan.FromSeconds(5));

        await waiter.DisposeAsync();

        await Eventually(() => _store.Invocations.Any(i => i.Method.Name == nameof(IRecoveryStateStore.TryDeleteAsync)));
    }

    [Fact]
    public async Task Waiter_Cleanup_ToleratesRecoveryStoreDeleteFailure()
    {
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("delete failed"));
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-e", timeout: TimeSpan.FromSeconds(5));

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "ok" }, "corr-e");

        // The waiter still completes even though cleanup's recovery-state delete throws (swallowed).
        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("ok", result.Message);
    }

    [Fact]
    public async Task Waiter_CleanupStillDisposesSubscription_WhenRecoveryDeleteThrows()
    {
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("kv store down"));
        var channel = CreateChannel();
        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-kv-down", timeout: TimeSpan.FromSeconds(5));

        await waiter.DisposeAsync();

        // A failed recovery-state delete is a network fault; it must not skip the subscription
        // teardown, or the consume loop would keep running until the process exits. EXACTLY once:
        // the drain and the latched cleanup both need the stream ended, but they share the
        // once-latched EndStreamOnceAsync — a second teardown path would be a regression.
        Assert.Equal(1, _client.SubscriptionDisposeCount);
    }

    [Fact]
    public async Task Waiter_DisposeWithWedgedDelivery_FaultsIndeterminateAfterDrainBudget()
    {
        // A delivery wedged in the Until predicate past DisposalDrainTimeout: disposal must
        // RETURN (a stuck predicate cannot hold shutdown hostage) but must NOT cancel — the
        // consume loop holds a message already consumed from the stream, and "canceled" tells a
        // re-attaching caller nothing was delivered. The explicit indeterminate fault routes
        // durable flows to a fresh idempotent restart; the late TrySetResult loses and is dropped.
        var channel = CreateChannel(drainTimeout: TimeSpan.FromMilliseconds(200));
        using var predicateEntered = new SemaphoreSlim(0);
        using var releasePredicate = new SemaphoreSlim(0);
        var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-wedged",
            async _ =>
            {
                predicateEntered.Release();
                return await releasePredicate.WaitAsync(TimeSpan.FromSeconds(30));
            },
            timeout: TimeSpan.FromMinutes(1));

        _client.Push(JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed, Message = "wedged" }
        }, AsyncResponseEnvelopeOptions<OperationResult>.Instance));
        Assert.True(await predicateEntered.WaitAsync(TimeSpan.FromSeconds(5)), "the delivery never reached the Until predicate");

        await waiter.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        var fault = await Assert.ThrowsAsync<AsyncResponseIndeterminateDeliveryException>(
            () => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal("corr-wedged", fault.CorrelationId);
        Assert.False(waiter.ResponseTask.IsCanceled);

        releasePredicate.Release();
    }

    private NatsAsyncResponseChannel CreateChannel(bool useRecoveryExpiry = false, TimeSpan? drainTimeout = null) => new(
        _services.GetRequiredService<IServiceScopeFactory>(),
        _client,
        _store.Object,
        Options.Create(new NatsAsyncResponseChannelOptions
        {
            DefaultTimeout = useRecoveryExpiry ? null : TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5),
            DisposalDrainTimeout = drainTimeout ?? TimeSpan.FromSeconds(30)
        }),
        new AsyncResponseContextPropagation([]),
        new TestLogger<NatsAsyncResponseChannel>());

    private static async Task Eventually(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        while (!condition())
            await Task.Delay(10, timeout.Token);
    }

    [Fact]
    public async Task CreateResponseWaiter_SynchronousCompletion_HandlesDisposedCts()
    {
        var mockClient = new Mock<INatsResponseChannelClient>();
        var channel = new NatsAsyncResponseChannel(
            _services.GetRequiredService<IServiceScopeFactory>(),
            mockClient.Object,
            _store.Object,
            Options.Create(new NatsAsyncResponseChannelOptions()),
            new AsyncResponseContextPropagation([]),
            new TestLogger<NatsAsyncResponseChannel>());

        var subscriptionMock = new Mock<INatsChannelSubscription>();
        var messageChannel = System.Threading.Channels.Channel.CreateUnbounded<NatsInboundResponse>();
        subscriptionMock.Setup(s => s.ReadAsync(It.IsAny<CancellationToken>()))
            .Returns(messageChannel.Reader.ReadAllAsync());

        var validEnvelope = new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed }
        };
        var json = JsonSerializer.Serialize(validEnvelope, AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        mockClient.Setup(c => c.SubscribeAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Callback<string, CancellationToken>((sub, token) =>
            {
                messageChannel.Writer.TryWrite(new NatsInboundResponse(json, false, () => ValueTask.CompletedTask));
            })
            .ReturnsAsync(subscriptionMock.Object);

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-sync");
        var result = await waiter.ResponseTask;
        Assert.Equal(OperationStatus.Completed, result.Status);
    }
}
