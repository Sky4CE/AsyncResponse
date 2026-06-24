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
        _store.Setup(s => s.GetAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((RecoveryState?)null);
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
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
    public async Task CreateResponseWaiter_MalformedMessageFaultsWaiter()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5));

        _client.Push("{not-json");

        await Assert.ThrowsAsync<JsonException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
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
    public async Task CreateResponseWaiter_TimesOutAndCleansUp()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-timeout", timeout: TimeSpan.FromMilliseconds(50));

        await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        await Eventually(() => _store.Invocations.Any(i => i.Method.Name == nameof(IRecoveryStateStore.TryDeleteAsync)));
    }

    [Fact]
    public async Task CreateResponseWaiter_SubscribeOrSaveFailureReturnsFaultedWaiter()
    {
        var failure = new InvalidOperationException("save failed");
        _store.Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(failure);
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-a", timeout: TimeSpan.FromSeconds(5));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Same(failure, ex);
    }

    [Fact]
    public async Task SetResponse_NoResponders_ConsultsRecoveryStore()
    {
        _client.NextOutcome = NatsDeliveryOutcome.NoResponders;
        var channel = CreateChannel();

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr-lost");

        _store.Verify(s => s.GetAsync("corr-lost", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SetException_NoResponders_ConsultsRecoveryStore()
    {
        _client.NextOutcome = NatsDeliveryOutcome.NoResponders;
        var channel = CreateChannel();

        await channel.SetException(new InvalidOperationException("boom"), "corr-lost");

        _store.Verify(s => s.GetAsync("corr-lost", It.IsAny<CancellationToken>()), Times.Once);
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

        _store.Verify(s => s.GetAsync("corr-lost", It.IsAny<CancellationToken>()), Times.Once);
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
        _store.Setup(s => s.GetAsync("corr-x", It.IsAny<CancellationToken>())).ReturnsAsync(ArmedState("corr-x"));
        var channel = CreateChannel();

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "late" }, "corr-x");

        Assert.NotNull(_spy.Resumed);
        Assert.Equal(OperationStatus.Completed, _spy.Resumed!.Status);
        Assert.Equal("corr-x", _spy.CorrelationId);
        _store.Verify(s => s.TryDeleteAsync("corr-x", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SetException_NoResponders_FiresFailureCallback_AndDeletesState()
    {
        _client.NextOutcome = NatsDeliveryOutcome.NoResponders;
        _store.Setup(s => s.GetAsync("corr-x", It.IsAny<CancellationToken>())).ReturnsAsync(ArmedState("corr-x"));
        var channel = CreateChannel();

        await channel.SetException(new InvalidOperationException("boom"), "corr-x");

        Assert.NotNull(_spy.Failed);
        Assert.Equal("boom", _spy.Failed!.Message);
        _store.Verify(s => s.TryDeleteAsync("corr-x", It.IsAny<CancellationToken>()), Times.Once);
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
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("delete failed"));
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-e", timeout: TimeSpan.FromSeconds(5));

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "ok" }, "corr-e");

        // The waiter still completes even though cleanup's recovery-state delete throws (swallowed).
        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("ok", result.Message);
    }

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
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        while (!condition())
            await Task.Delay(10, timeout.Token);
    }
}
