using AsyncResponse.Transports.MongoDB;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class MongoDbDispatcherTests
{
    [Fact]
    public async Task AckAfterHandlerCompletes_AcksOnlyAfterSuccessfulHandler()
    {
        var calls = new Calls();
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                calls.Handler++;
                Assert.Equal(0, calls.Ack);
                return Task.CompletedTask;
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions(),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(1, calls.Handler);
        Assert.Equal(1, calls.Ack);
        Assert.Equal(0, calls.Nak);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task HandlerExecution_EmitsMongoDbReceiveSpanWithMessagingTags()
    {
        using var collector = new AsyncResponseActivityCollector();
        var options = new MongoDbAsyncResponseTransportOptions();
        var id = Guid.NewGuid();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { [options.CorrelationIdHeader] = "corr-mongo" };
        var delivery = new MongoDbTransportDelivery(
            id,
            "worker",
            "{}",
            headers,
            1,
            () => ValueTask.CompletedTask,
            _ => ValueTask.CompletedTask,
            (_, _, _) => ValueTask.FromResult(true));
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) => Task.CompletedTask,
            options,
            new MongoDbSubscriberOptions(),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(delivery, CancellationToken.None);

        var activity = collector.Single("asyncresponse.mongodb.receive", "asyncresponse.transport", "mongodb");
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal("Worker", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.mongodb.role"));
        Assert.Equal(nameof(MongoDbAckMode.AckAfterHandlerCompletes), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.mongodb.ack_mode"));
        Assert.Equal("mongodb", AsyncResponseActivityCollector.Tag(activity, "messaging.system"));
        Assert.Equal("worker", AsyncResponseActivityCollector.Tag(activity, "messaging.destination.name"));
        Assert.Equal(id.ToString(), AsyncResponseActivityCollector.Tag(activity, "messaging.message.id"));
        Assert.Equal(1, AsyncResponseActivityCollector.Tag(activity, "messaging.message.delivery_attempt"));
        Assert.Equal("corr-mongo", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.correlation_id"));
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundHandlerStillEmitsReceiveSpan()
    {
        using var collector = new AsyncResponseActivityCollector();
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                return Task.CompletedTask;
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Regression guard for the "both ACK modes emit the span" claim: the early-ACK path runs the
        // handler on a background worker, and a refactor that splits it from ExecuteHandlerAsync
        // would lose the span silently.
        await WaitUntilAsync(() => collector.Count("asyncresponse.mongodb.receive") == 1);
        var activity = collector.Single("asyncresponse.mongodb.receive", "asyncresponse.transport", "mongodb");
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal(nameof(MongoDbAckMode.AckAfterEnqueue), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.mongodb.ack_mode"));
    }

    [Fact]
    public async Task HandlerFailure_MarksMongoDbReceiveSpanError()
    {
        using var collector = new AsyncResponseActivityCollector();
        var calls = new Calls();
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 1), CancellationToken.None);

        var activity = collector.Single("asyncresponse.mongodb.receive", "asyncresponse.transport", "mongodb");
        Assert.Equal(typeof(InvalidOperationException).FullName, AsyncResponseActivityCollector.Tag(activity, "error.type"));
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_NaksBeforeMaxAttempts()
    {
        var calls = new Calls();
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 1), CancellationToken.None);

        Assert.Equal(0, calls.Ack);
        Assert.Equal(1, calls.Nak);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_DeadLettersAtMaxAttempts()
    {
        var calls = new Calls();
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions { MaxDeliveryAttempts = 2 },
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 2), CancellationToken.None);

        Assert.Equal(0, calls.Nak);
        Assert.Equal(1, calls.DeadLetter);
        Assert.True(calls.DeleteOriginalOnDeadLetter);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_NaksWhenDeadLetterPublishFails()
    {
        var calls = new Calls { DeadLetterResult = false };
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions { MaxDeliveryAttempts = 1 },
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        Assert.Equal(1, calls.DeadLetter);
        Assert.True(calls.DeleteOriginalOnDeadLetter);
        Assert.Equal(1, calls.Nak);
        Assert.Equal(TimeSpan.FromSeconds(5), calls.LastNakDelay);
    }

    [Fact]
    public async Task AckAfterHandlerCompletes_UnlimitedAttemptsAlwaysNaks()
    {
        var calls = new Calls();
        var dispatcher = new MongoDbMessageDispatcher(
            (_, _) => throw new InvalidOperationException("boom"),
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions { MaxDeliveryAttempts = 0 },
            NullLogger.Instance,
            MongoDbSubscriberRole.ResponseIngress);

        await dispatcher.HandleAsync(Delivery(calls, attempt: 99), CancellationToken.None);

        Assert.Equal(0, calls.Ack);
        Assert.Equal(1, calls.Nak);
        Assert.Equal(0, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterEnqueue_AcksOnReceive_WithoutWaitingForHandler()
    {
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                calls.Handler++;
                handled.SetResult();
                return Task.CompletedTask;
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        // AckAfterEnqueue acks the document as part of accepting it into the background queue, so it
        // is already acknowledged the moment HandleAsync returns — without waiting for the handler.
        // The handler runs on the background worker and may execute concurrently with the ack, so the
        // test must not assert their relative ordering.
        Assert.Equal(1, calls.Ack);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, calls.Handler);
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailure_DeadLettersWithoutDeletingOriginal_AndInvokesCallback()
    {
        var calls = new Calls();
        MongoDbBackgroundFailureContext? callback = null;
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new InvalidOperationException("background boom");
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions
            {
                OnBackgroundFailure = context =>
                {
                    callback = context;
                    return ValueTask.CompletedTask;
                }
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls, headers: new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["AR-Correlation-Id"] = "corr-background"
        }), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => calls.DeadLetter == 1 && callback is not null);

        Assert.Equal(1, calls.Ack);
        Assert.Equal(1, calls.DeadLetter);
        Assert.False(calls.DeleteOriginalOnDeadLetter);
        Assert.NotNull(callback);
        Assert.Equal("worker", callback!.Queue);
        Assert.Equal("Worker", callback.SubscriberRole);
        Assert.Equal("corr-background", callback.CorrelationId);
        Assert.IsType<InvalidOperationException>(callback.Exception);
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailureCallbackExceptionsAreSwallowed()
    {
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new InvalidOperationException("background boom");
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions
            {
                OnBackgroundFailure = _ => throw new InvalidOperationException("callback boom")
            }.UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => calls.DeadLetter == 1);
        Assert.Equal(1, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterEnqueue_Overflow_ReleasesDocumentForRetry()
    {
        var calls = new Calls();
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                entered.Set();
                // Held while the test overflows the queue; must not lapse early on a
                // starved runner or the overflow scenario silently collapses.
                release.Wait(TimeSpan.FromSeconds(30));
                return Task.CompletedTask;
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 1, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        Assert.True(entered.Wait(TimeSpan.FromSeconds(30)));
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await WaitUntilAsync(() => calls.Nak == 1);
        Assert.Equal(TimeSpan.FromSeconds(5), calls.LastNakDelay);

        release.Set();
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundFailureWithoutCallback_DeadLetters()
    {
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new InvalidOperationException("background boom");
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.ResponseIngress);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => calls.DeadLetter == 1);
        Assert.Equal(1, calls.DeadLetter);
    }

    [Fact]
    public async Task AckAfterEnqueue_BackgroundOperationCanceled_DeadLettersLikeHandlerFailure()
    {
        var calls = new Calls();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var dispatcher = new MongoDbMessageDispatcher(
            (_, _) =>
            {
                handled.SetResult();
                throw new OperationCanceledException();
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromSeconds(5)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);

        await handled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => calls.DeadLetter == 1);
        Assert.Equal(1, calls.DeadLetter);
    }

    [Fact]
    public async Task DisposeAsync_TimesOutWhenBackgroundWorkerDoesNotDrain()
    {
        var calls = new Calls();
        var handlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = new MongoDbMessageDispatcher(
            async (_, _) =>
            {
                handlerEntered.SetResult();
                await release.Task;
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(1)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await handlerEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await dispatcher.DisposeAsync();
        release.SetResult();

        Assert.Equal(1, calls.Ack);
    }

    [Fact]
    public async Task DisposeAsync_DrainTimeout_RunningWorkerStillDeadLettersAfterDispose()
    {
        var calls = new Calls();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatcher = new MongoDbMessageDispatcher(
            async (_, cancellationToken) =>
            {
                started.TrySetResult();
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            },
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions().UseAckAfterEnqueue(1, 8, TimeSpan.FromMilliseconds(50)),
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery(calls), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5)); // worker is blocked in the handler

        await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));

        // The drain CTS must stay alive until the worker actually finishes: after the timed-out dispose the
        // cancelled handler still dead-letters through the background failure path (no ObjectDisposedException).
        await WaitUntilAsync(() => calls.DeadLetter == 1);
        Assert.False(calls.DeleteOriginalOnDeadLetter);
    }

    [Fact]
    public void Constructor_RejectsInvalidSubscriberOptions()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new MongoDbMessageDispatcher(
            (_, _) => Task.CompletedTask,
            new MongoDbAsyncResponseTransportOptions(),
            new MongoDbSubscriberOptions { AckMode = MongoDbAckMode.AckAfterEnqueue },
            NullLogger.Instance,
            MongoDbSubscriberRole.Worker));

        Assert.Contains(nameof(MongoDbSubscriberOptions.BackgroundWorkerCount), ex.Message);
    }

    private static MongoDbTransportDelivery Delivery(
        Calls calls,
        int attempt = 1,
        IReadOnlyDictionary<string, string>? headers = null)
        => new(
            Guid.NewGuid(),
            "worker",
            "{}",
            headers ?? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
            attempt,
            () =>
            {
                calls.Ack++;
                return ValueTask.CompletedTask;
            },
            _ =>
            {
                calls.Nak++;
                calls.LastNakDelay = _;
                return ValueTask.CompletedTask;
            },
            (_, deleteOriginal, _) =>
            {
                calls.DeadLetter++;
                calls.DeleteOriginalOnDeadLetter = deleteOriginal;
                return ValueTask.FromResult(calls.DeadLetterResult);
            });

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
        {
            cts.Token.ThrowIfCancellationRequested();
            await Task.Delay(20, cts.Token);
        }
    }

    private sealed class Calls
    {
        public Calls() => DeadLetterResult = true;

        public int Handler;
        public int Ack;
        public int Nak;
        public int DeadLetter;
        public bool DeleteOriginalOnDeadLetter;
        public bool DeadLetterResult;
        public TimeSpan LastNakDelay;
    }
}
