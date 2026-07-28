using AsyncResponse.Channels.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using System.Net;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public class RedisAsyncResponseChannelWaiterTests
{
    private readonly Mock<IConnectionMultiplexer> _multiplexer = new();
    private readonly Mock<ISubscriber> _subscriber = new();
    private readonly Mock<IRecoveryStateStore> _store = new();
    private readonly FakeRedisChannelSubscriber _channelSubscriber = new();
    private readonly ServiceProvider _services = new ServiceCollection().BuildServiceProvider();

    public RedisAsyncResponseChannelWaiterTests()
    {
        _multiplexer.Setup(m => m.GetSubscriber(It.IsAny<object?>())).Returns(_subscriber.Object);
        _store
            .Setup(s => s.SaveAsync(
                It.IsAny<string>(),
                It.IsAny<RecoveryState>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        _store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
    }

    /// <summary>
    /// Fake for the channel's async subscribe seam (ChannelMessageQueue is sealed and cannot be
    /// mocked): captures the handler for tests to push messages through, and counts unsubscribes.
    /// </summary>
    private sealed class FakeRedisChannelSubscriber : IRedisChannelSubscriber
    {
        private int _unsubscribeCount;

        public RedisChannel SubscribedChannel { get; private set; }
        public Func<RedisChannel, RedisValue, Task>? Handler { get; private set; }
        public Exception? SubscribeException { get; set; }
        public Exception? UnsubscribeException { get; set; }
        public RedisValue? InvokeOnSubscribe { get; set; }
        public int UnsubscribeCount => Volatile.Read(ref _unsubscribeCount);

        public async Task<IRedisChannelSubscription> SubscribeAsync(RedisChannel channel, Func<RedisChannel, RedisValue, Task> onMessage)
        {
            if (SubscribeException is not null)
                throw SubscribeException;

            SubscribedChannel = channel;
            Handler = onMessage;
            if (InvokeOnSubscribe is { } message)
                await onMessage(channel, message);
            return new Subscription(this);
        }

        private sealed class Subscription(FakeRedisChannelSubscriber owner) : IRedisChannelSubscription
        {
            public ValueTask DisposeAsync()
            {
                Interlocked.Increment(ref owner._unsubscribeCount);
                return owner.UnsubscribeException is null
                    ? ValueTask.CompletedTask
                    : ValueTask.FromException(owner.UnsubscribeException);
            }
        }
    }

    [Fact]
    public async Task CreateResponseWaiter_CompletesFromSubscribedRedisMessage()
    {
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-a",
            timeout: TimeSpan.FromSeconds(5));

        await PublishSuccess(new OperationResult { Status = OperationStatus.Completed, Message = "done" });

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("done", result.Message);
        Assert.Equal("asyncresponse:response:corr-a", _channelSubscriber.SubscribedChannel.ToString());
        _store.Verify(s => s.SaveAsync(
            "corr-a",
            It.Is<RecoveryState>(state => state.CorrelationId == "corr-a"
                && state.PayloadTypeFullName == typeof(OperationResult).FullName),
            It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Once);
        await Eventually(() => _channelSubscriber.UnsubscribeCount == 1);
    }

    [Fact]
    public async Task CreateResponseWaiter_ValidatesCorrelationAndUsesRecoveryExpiryAsTimeoutFallback()
    {
        var channel = CreateChannel(new RedisAsyncResponseOptions
        {
            DefaultTimeout = null,
            RecoveryStateExpiry = TimeSpan.FromMinutes(1)
        });

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            channel.CreateResponseWaiter<OperationResult>(" "));
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("fallback-timeout");

        Assert.False(waiter.ResponseTask.IsCompleted);
    }

    [Fact]
    public async Task DuplicateTerminalMessages_DoNotReplaceFirstCompletion()
    {
        var channel = CreateChannel();

        await using (var success = await channel.CreateResponseWaiter<OperationResult>("duplicate-success"))
        {
            var handler = _channelSubscriber.Handler!;
            var subscribed = _channelSubscriber.SubscribedChannel;
            var json = JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
            {
                Success = true,
                Payload = new OperationResult { Status = OperationStatus.Completed, Message = "first" }
            }, AsyncResponseEnvelopeOptions<OperationResult>.Instance);
            await handler(subscribed, json);
            await handler(subscribed, json);
            Assert.Equal("first", (await success.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
        }

        await DuplicateFaultAsync(channel, "duplicate-null", "null", typeof(JsonException));
        await DuplicateFaultAsync(
            channel,
            "duplicate-schema",
            JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
            {
                SchemaVersion = AsyncResponseEnvelopeSchema.Current + 1,
                Success = true,
                Payload = new OperationResult()
            }, AsyncResponseEnvelopeOptions<OperationResult>.Instance),
            typeof(InvalidOperationException));
        await DuplicateFaultAsync(
            channel,
            "duplicate-remote",
            JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
            {
                Success = false,
                ExceptionMessage = "remote"
            }, AsyncResponseEnvelopeOptions<OperationResult>.Instance),
            typeof(Exception));
        await DuplicateFaultAsync(channel, "duplicate-malformed", "{not-json", typeof(JsonException));
    }

    [Fact]
    public async Task CreateResponseWaiter_CompletionPredicateCanWaitForLaterMessages()
    {
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-a",
            completionPredicate: payload => new ValueTask<bool>(payload.Status == OperationStatus.Completed),
            timeout: TimeSpan.FromSeconds(5));

        await PublishSuccess(new OperationResult { Status = OperationStatus.Running, Message = "still running" });
        await Task.Delay(50);
        Assert.False(waiter.ResponseTask.IsCompleted);

        await PublishSuccess(new OperationResult { Status = OperationStatus.Completed, Message = "done" });

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("done", result.Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_RemoteFailureEnvelopeFaultsWaiter()
    {
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-a",
            timeout: TimeSpan.FromSeconds(5));

        await PublishEnvelope(new AsyncResponseEnvelope<OperationResult>
        {
            Success = false,
            ExceptionMessage = "remote failed",
            ExceptionStackTrace = "remote stack"
        });

        var ex = await Assert.ThrowsAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal("remote failed", ex.Message);
        Assert.Equal("remote stack", ex.Data["RemoteStackTrace"]);
    }

    [Fact]
    public async Task CreateResponseWaiter_MalformedRedisMessageFaultsWaiter()
    {
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-a",
            timeout: TimeSpan.FromSeconds(5));

        await _channelSubscriber.Handler!.Invoke(_channelSubscriber.SubscribedChannel, "{not-json");

        await Assert.ThrowsAsync<JsonException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task CreateResponseWaiter_NullEnvelopeFaultsWaiter()
    {
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-null",
            timeout: TimeSpan.FromSeconds(5));

        await _channelSubscriber.Handler!.Invoke(_channelSubscriber.SubscribedChannel, "null");

        await Assert.ThrowsAsync<JsonException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task CreateResponseWaiter_UnsubscribeFailureDoesNotMaskCompletedResponse()
    {
        _channelSubscriber.UnsubscribeException = new InvalidOperationException("unsubscribe failed");
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-cleanup",
            timeout: TimeSpan.FromSeconds(5));

        await PublishSuccess(new OperationResult { Status = OperationStatus.Completed, Message = "done" });

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("done", result.Message);
        await Eventually(() => _channelSubscriber.UnsubscribeCount >= 1);
    }

    [Fact]
    public async Task CreateResponseWaiter_WhenExecutionContextFlowIsSuppressed_StillProcessesMessage()
    {
        var channel = CreateChannel();
        Task<IAsyncResponseWaiter<OperationResult>> waiterTask;
        using (ExecutionContext.SuppressFlow())
        {
            waiterTask = channel.CreateResponseWaiter<OperationResult>(
                "corr-no-context",
                timeout: TimeSpan.FromSeconds(5));
        }

        await using var waiter = await waiterTask;

        await PublishSuccess(new OperationResult { Status = OperationStatus.Completed, Message = "done" });

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("done", result.Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_TimeoutFaultsAndCleansUp()
    {
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-timeout",
            timeout: TimeSpan.FromMilliseconds(5));

        await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        await Eventually(() => _channelSubscriber.UnsubscribeCount == 1);
        _store.Verify(s => s.TryDeleteAsync("corr-timeout", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateResponseWaiter_SubscribeFailureThrowsAndDeletesRecoveryState()
    {
        var failure = new InvalidOperationException("subscribe failed");
        _channelSubscriber.SubscribeException = failure;
        var channel = CreateChannel();

        // Must throw rather than return a pre-faulted waiter: the builder's contract is that the
        // trigger only runs once the subscription AND recovery state exist, so a registration
        // failure has to surface before any trigger could fire the remote operation.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            channel.CreateResponseWaiter<OperationResult>(
                "corr-a",
                timeout: TimeSpan.FromSeconds(5)));
        Assert.Same(failure, ex);
        _store.Verify(s => s.SaveAsync(
            It.IsAny<string>(),
            It.IsAny<RecoveryState>(),
            It.IsAny<TimeSpan>(),
            It.IsAny<CancellationToken>()), Times.Never);
        _store.Verify(s => s.TryDeleteAsync("corr-a", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CountActiveSubscribersAsync_UsesMaximumConnectedEndpointCount()
    {
        var endpointA = new DnsEndPoint("redis-a", 6379);
        var endpointB = new DnsEndPoint("redis-b", 6379);
        var endpointC = new DnsEndPoint("redis-c", 6379);
        var serverA = new Mock<IServer>();
        var serverB = new Mock<IServer>();
        var serverC = new Mock<IServer>();
        serverA.SetupGet(s => s.IsConnected).Returns(true);
        serverB.SetupGet(s => s.IsConnected).Returns(false);
        serverC.SetupGet(s => s.IsConnected).Returns(true);
        serverA
            .Setup(s => s.SubscriptionSubscriberCount(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .Returns(3);
        serverC
            .Setup(s => s.SubscriptionSubscriberCount(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .Throws(new InvalidOperationException("node unavailable"));

        _multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns([endpointA, endpointB, endpointC]);
        _multiplexer.Setup(m => m.GetServer(endpointA, It.IsAny<object?>())).Returns(serverA.Object);
        _multiplexer.Setup(m => m.GetServer(endpointB, It.IsAny<object?>())).Returns(serverB.Object);
        _multiplexer.Setup(m => m.GetServer(endpointC, It.IsAny<object?>())).Returns(serverC.Object);
        var channel = CreateChannel();

        Assert.Equal(0, await channel.CountActiveSubscribersAsync(" "));
        Assert.Equal(3, await channel.CountActiveSubscribersAsync("corr-a"));
    }

    [Fact]
    public async Task Publishers_WithBlankCorrelationId_AreNoops()
    {
        var channel = CreateChannel();
        var rawPublisher = (IRawAsyncResponsePublisher)channel;

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, " ");
        await rawPublisher.SetRawResponseJson("""{"Status":2}""", " ");
        await channel.SetException(new InvalidOperationException("missing correlation"), " ");

        _subscriber.Verify(s => s.PublishAsync(
            It.IsAny<RedisChannel>(),
            It.IsAny<RedisValue>(),
            It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task RawObjectPublisher_UsesTypedSetResponseCore()
    {
        var channel = CreateChannel();
        var rawPublisher = (IRawAsyncResponsePublisher)channel;
        RedisValue publishedValue = default;
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .Callback<RedisChannel, RedisValue, CommandFlags>((_, value, _) => publishedValue = value)
            .ReturnsAsync(1);

        await rawPublisher.SetRawResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "typed raw" },
            "corr-a");

        using var document = JsonDocument.Parse(publishedValue.ToString());
        Assert.True(document.RootElement.GetProperty("Success").GetBoolean());
        Assert.Equal("typed raw", document.RootElement.GetProperty("Payload").GetProperty("Message").GetString());
    }

    [Fact]
    public async Task Publishers_LogSuccessfulPublishWhenSubscribersArePresent()
    {
        var channel = CreateChannel(new RedisAsyncResponseOptions
        {
            DefaultTimeout = TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5)
        }, new TestLogger<RedisAsyncResponseChannel>());
        var rawPublisher = (IRawAsyncResponsePublisher)channel;
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(1);

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr-a");
        await rawPublisher.SetRawResponseJson("""{"Status":2}""", "corr-a");
        await channel.SetException(new InvalidOperationException("remote failure"), "corr-a");

        _subscriber.Verify(s => s.PublishAsync(
            It.IsAny<RedisChannel>(),
            It.IsAny<RedisValue>(),
            It.IsAny<CommandFlags>()), Times.Exactly(3));
    }

    [Fact]
    public async Task SetResponse_WhenPublishFails_Propagates()
    {
        var failure = new InvalidOperationException("publish failed");
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(failure);
        var channel = CreateChannel();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr-a"));

        Assert.Same(failure, ex);
    }

    [Fact]
    public async Task SetRawResponseJson_WhenPublishFails_Propagates()
    {
        var failure = new InvalidOperationException("publish failed");
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(failure);
        var channel = CreateChannel();
        var rawPublisher = (IRawAsyncResponsePublisher)channel;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            rawPublisher.SetRawResponseJson("""{"Status":2}""", "corr-a"));

        Assert.Same(failure, ex);
    }

    [Fact]
    public async Task SetException_WhenPublishFails_Propagates()
    {
        var failure = new InvalidOperationException("publish failed");
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ThrowsAsync(failure);
        var channel = CreateChannel();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            channel.SetException(new InvalidOperationException("remote failure"), "corr-a"));

        Assert.Same(failure, ex);
    }

    [Fact]
    public async Task RedisWaiter_DisposeAsyncRunsCleanup()
    {
        var channel = CreateChannel();

        var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-dispose",
            timeout: TimeSpan.FromSeconds(5));

        await waiter.DisposeAsync();

        await Eventually(() => _channelSubscriber.UnsubscribeCount == 1);
        _store.Verify(s => s.TryDeleteAsync("corr-dispose", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SetException_CapsRemoteStackTrace_OnPublish()
    {
        var channel = CreateChannel(new RedisAsyncResponseOptions
        {
            DefaultTimeout = TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5),
            MaxRemoteStackTraceLength = 16
        });
        RedisValue publishedValue = default;
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .Callback<RedisChannel, RedisValue, CommandFlags>((_, value, _) => publishedValue = value)
            .ReturnsAsync(1);

        await channel.SetException(MakeThrownException(), "corr-a");

        using var document = JsonDocument.Parse(publishedValue.ToString());
        var stackTrace = document.RootElement.GetProperty("ExceptionStackTrace").GetString();
        Assert.NotNull(stackTrace);
        Assert.Contains("truncated", stackTrace);
        Assert.True(stackTrace!.Length < 80, $"stack trace was not capped: length {stackTrace.Length}");
    }

    [Fact]
    public async Task SetException_OmitsRemoteStackTrace_WhenDisabled()
    {
        var channel = CreateChannel(new RedisAsyncResponseOptions
        {
            DefaultTimeout = TimeSpan.FromSeconds(5),
            RecoveryStateExpiry = TimeSpan.FromMinutes(5),
            IncludeRemoteStackTrace = false
        });
        RedisValue publishedValue = default;
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .Callback<RedisChannel, RedisValue, CommandFlags>((_, value, _) => publishedValue = value)
            .ReturnsAsync(1);

        await channel.SetException(MakeThrownException(), "corr-a");

        using var document = JsonDocument.Parse(publishedValue.ToString());
        var hasStackTrace = document.RootElement.TryGetProperty("ExceptionStackTrace", out var element)
            && element.ValueKind != JsonValueKind.Null;
        Assert.False(hasStackTrace);
    }

    private static Exception MakeThrownException()
    {
        try
        {
            throw new InvalidOperationException("boom with a real stack trace attached");
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private RedisAsyncResponseChannel CreateChannel() => CreateChannel(new RedisAsyncResponseOptions
    {
        DefaultTimeout = TimeSpan.FromSeconds(5),
        RecoveryStateExpiry = TimeSpan.FromMinutes(5)
    });

    private RedisAsyncResponseChannel CreateChannel(
        RedisAsyncResponseOptions options,
        ILogger<RedisAsyncResponseChannel>? logger = null) => new(
        _services.GetRequiredService<IServiceScopeFactory>(),
        _multiplexer.Object,
        _store.Object,
        Options.Create(options),
        new AsyncResponseContextPropagation([]),
        logger ?? NullLogger<RedisAsyncResponseChannel>.Instance,
        _channelSubscriber);

    [Fact]
    public async Task CreateResponseWaiter_UnsupportedEnvelopeSchema_FaultsWaiter()
    {
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-schema",
            timeout: TimeSpan.FromSeconds(5));

        await PublishEnvelope(new AsyncResponseEnvelope<OperationResult>
        {
            SchemaVersion = AsyncResponseEnvelopeSchema.Current + 1,
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed }
        });

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => waiter.ResponseTask);
        Assert.IsType<InvalidOperationException>(ex);
    }

    private Task PublishSuccess(OperationResult payload)
        => PublishEnvelope(new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = payload
        });

    private Task PublishEnvelope(AsyncResponseEnvelope<OperationResult> envelope)
    {
        var json = JsonSerializer.Serialize(envelope, AsyncResponseEnvelopeOptions<OperationResult>.Instance);
        return _channelSubscriber.Handler!.Invoke(_channelSubscriber.SubscribedChannel, json);
    }

    private async Task DuplicateFaultAsync(
        RedisAsyncResponseChannel channel,
        string correlationId,
        RedisValue message,
        Type exceptionType)
    {
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(correlationId);
        var handler = _channelSubscriber.Handler!;
        var subscribed = _channelSubscriber.SubscribedChannel;
        await handler(subscribed, message);
        await handler(subscribed, message);

        var exception = await Assert.ThrowsAnyAsync<Exception>(() =>
            waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.True(exceptionType.IsAssignableFrom(exception.GetType()));
    }

    private static async Task Eventually(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        while (!condition())
        {
            await Task.Delay(10, timeout.Token);
        }
    }

    [Fact]
    public void ServiceCollectionExtensions_ThrowsWhenNoConnectionStringOrMultiplexer()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncResponse().WithRedisChannel();

        var provider = services.BuildServiceProvider();
        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<RedisAsyncResponseChannel>());
    }

    [Fact]
    public async Task CreateResponseWaiter_SynchronousCompletion_HandlesDisposedCts()
    {
        var channelName = "corr-sync";
        var validEnvelope = new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed }
        };
        var json = JsonSerializer.Serialize(validEnvelope, AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        // Deliver the terminal message during SubscribeAsync itself, so cleanup can dispose the
        // timeout CTS before CreateResponseWaiterCore reaches CancelAfter.
        _channelSubscriber.InvokeOnSubscribe = json;
        var channel = CreateChannel(new RedisAsyncResponseOptions(), new TestLogger<RedisAsyncResponseChannel>());

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(channelName);
        var result = await waiter.ResponseTask;
        Assert.Equal(OperationStatus.Completed, result.Status);
    }

    [Fact]
    public async Task CountActiveSubscribersAsync_HandlesServerException()
    {
        var mockSubscriber = new Mock<ISubscriber>();
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetSubscriber(It.IsAny<object?>())).Returns(mockSubscriber.Object);

        var endPoint = new Mock<EndPoint>().Object;
        multiplexer.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns([endPoint]);

        var server = new Mock<IServer>();
        server.SetupGet(s => s.IsConnected).Returns(true);
        server.Setup(s => s.SubscriptionSubscriberCount(It.IsAny<RedisChannel>(), It.IsAny<CommandFlags>()))
            .Throws(new InvalidOperationException("Redis command failed"));

        multiplexer.Setup(m => m.GetServer(endPoint, It.IsAny<object?>())).Returns(server.Object);

        var channel = new RedisAsyncResponseChannel(
            _services.GetRequiredService<IServiceScopeFactory>(),
            multiplexer.Object,
            _store.Object,
            Options.Create(new RedisAsyncResponseOptions()),
            new AsyncResponseContextPropagation([]),
            new TestLogger<RedisAsyncResponseChannel>());

        var count = await channel.CountActiveSubscribersAsync("corr");
        Assert.Equal(0, count);
    }
}
