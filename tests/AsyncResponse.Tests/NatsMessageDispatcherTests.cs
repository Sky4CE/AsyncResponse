using AsyncResponse.Transports.NATS;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using Xunit;

namespace AsyncResponse.Tests;

public class NatsMessageDispatcherTests
{
    private const string DeadLetterSubject = "asyncresponse.transport.deadletter";
    private readonly FakeNatsJetStreamTransport _jetStream = new();

    private NatsMessageDispatcher CreateDispatcher(
        Func<NatsJobDelivery, CancellationToken, Task> handler,
        NatsSubscriberOptions subscriber,
        NatsAsyncResponseTransportOptions? options = null)
    {
        options ??= new NatsAsyncResponseTransportOptions();
        return new NatsMessageDispatcher(
            handler,
            _jetStream,
            options,
            subscriber,
            new NatsTransportSubjectSchema(options),
            new TestLogger(),
            NatsSubscriberRole.Worker,
            "test-consumer");
    }

    [Fact]
    public async Task HandlerCompletes_AcksMessage()
    {
        var rec = new RecordingDelivery();
        await using var dispatcher = CreateDispatcher((_, _) => Task.CompletedTask, new NatsSubscriberOptions());

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);

        Assert.Equal(1, rec.Acks);
        Assert.Empty(rec.Naks);
        Assert.Equal(0, rec.Terms);
        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task HandlerExecution_EmitsNatsReceiveSpanWithMessagingTags()
    {
        using var collector = new AsyncResponseActivityCollector();
        var rec = new RecordingDelivery();
        var options = new NatsAsyncResponseTransportOptions();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { [options.CorrelationIdHeader] = "corr-nats" };
        await using var dispatcher = CreateDispatcher((_, _) => Task.CompletedTask, new NatsSubscriberOptions(), options);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1, headers: headers), CancellationToken.None);

        var activity = collector.Single("asyncresponse.nats.receive", "asyncresponse.transport", "nats");
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal("Worker", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.nats.role"));
        Assert.Equal(nameof(NatsAckMode.AckAfterHandlerCompletes), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.nats.ack_mode"));
        Assert.Equal("nats", AsyncResponseActivityCollector.Tag(activity, "messaging.system"));
        Assert.Equal("asyncresponse.transport.worker", AsyncResponseActivityCollector.Tag(activity, "messaging.destination.name"));
        Assert.Equal(1L, AsyncResponseActivityCollector.Tag(activity, "messaging.nats.num_delivered"));
        Assert.Equal("corr-nats", AsyncResponseActivityCollector.Tag(activity, "asyncresponse.correlation_id"));
    }

    [Fact]
    public async Task EarlyAck_BackgroundHandlerStillEmitsReceiveSpan()
    {
        using var collector = new AsyncResponseActivityCollector();
        var processed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions().UseAckAfterReceive(backgroundWorkerCount: 1, backgroundQueueCapacity: 4);
        await using var dispatcher = CreateDispatcher((_, _) => { processed.TrySetResult(); return Task.CompletedTask; }, subscriber);

        var rec = new RecordingDelivery();
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);
        await processed.Task.WaitAsync(TimeSpan.FromSeconds(2));

        // Regression guard for the "both ACK modes emit the span" claim: the early-ACK path runs the
        // handler on a background worker, and a refactor that splits it from the shared handler
        // execution path would lose the span silently.
        await WaitUntilAsync(() => collector.Count("asyncresponse.nats.receive") == 1);
        var activity = collector.Single("asyncresponse.nats.receive", "asyncresponse.transport", "nats");
        Assert.Equal(ActivityKind.Consumer, activity.Kind);
        Assert.Equal(nameof(NatsAckMode.AckAfterReceive), AsyncResponseActivityCollector.Tag(activity, "asyncresponse.nats.ack_mode"));
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
        {
            cts.Token.ThrowIfCancellationRequested();
            await Task.Delay(20, cts.Token);
        }
    }

    [Fact]
    public async Task HandlerFailure_MarksNatsReceiveSpanError()
    {
        using var collector = new AsyncResponseActivityCollector();
        var rec = new RecordingDelivery();
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 5 };
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("boom"), subscriber);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);

        var activity = collector.Single("asyncresponse.nats.receive", "asyncresponse.transport", "nats");
        Assert.Equal(typeof(InvalidOperationException).FullName, AsyncResponseActivityCollector.Tag(activity, "error.type"));
        Assert.Equal(ActivityStatusCode.Error, activity.Status);
    }

    [Fact]
    public async Task HandlerFailureBelowMaxAttempts_NaksForRedelivery()
    {
        var rec = new RecordingDelivery();
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 5, RedeliveryDelay = TimeSpan.FromSeconds(7) };
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("boom"), subscriber);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 2), CancellationToken.None);

        Assert.Equal(0, rec.Acks);
        Assert.Equal(0, rec.Terms);
        Assert.Equal([TimeSpan.FromSeconds(7)], rec.Naks);
        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task HandlerFailureAtMaxAttempts_DeadLettersAndTerminates()
    {
        var rec = new RecordingDelivery();
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 5 };
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("boom"), subscriber);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 5), CancellationToken.None);

        Assert.Equal(1, rec.Terms);
        Assert.Empty(rec.Naks);
        var deadLettered = Assert.Single(_jetStream.Published);
        Assert.Equal(DeadLetterSubject, deadLettered.Subject);
        Assert.Equal("payload", deadLettered.Payload);
        Assert.Equal("boom", deadLettered.Headers!["AR-DeadLetter-Reason"]);
    }

    [Fact]
    public async Task HandlerFailureAtMaxAttempts_WithDeadLetterDisabled_TerminatesWithoutPublishing()
    {
        var rec = new RecordingDelivery();
        var options = new NatsAsyncResponseTransportOptions { DeadLetterEnabled = false };
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 3 };
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("boom"), subscriber, options);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 3), CancellationToken.None);

        Assert.Equal(1, rec.Terms);
        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task EarlyAck_AcksImmediately_AndProcessesInBackground()
    {
        var processed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions().UseAckAfterReceive(backgroundWorkerCount: 1, backgroundQueueCapacity: 4);
        await using var dispatcher = CreateDispatcher((delivery, _) => { processed.TrySetResult(delivery.Payload); return Task.CompletedTask; }, subscriber);

        var rec = new RecordingDelivery();
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);

        Assert.Equal(1, rec.Acks);
        Assert.Equal("payload", await processed.Task.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact]
    public async Task EarlyAck_BackgroundFailure_DeadLettersAndReportsContext()
    {
        var failure = new TaskCompletionSource<NatsBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions { OnBackgroundFailure = ctx => { failure.TrySetResult(ctx); return ValueTask.CompletedTask; } }
            .UseAckAfterReceive(backgroundWorkerCount: 1, backgroundQueueCapacity: 4);
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("bg-boom"), subscriber);

        var rec = new RecordingDelivery();
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["AR-Correlation-Id"] = "c1" };
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1, headers: headers), CancellationToken.None);

        Assert.Equal(1, rec.Acks);
        var context = await failure.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("c1", context.CorrelationId);
        Assert.Equal("bg-boom", context.Exception.Message);
        Assert.Equal("test-consumer", context.Consumer);
        // Dead-lettering happens before the failure callback, so the DLQ entry is present.
        Assert.Contains(_jetStream.Published, p => p.Subject == DeadLetterSubject);
    }

    [Fact]
    public async Task EarlyAck_WhenQueueFull_NaksForRedelivery()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions().UseAckAfterReceive(backgroundWorkerCount: 1, backgroundQueueCapacity: 1);
        await using var dispatcher = CreateDispatcher(async (_, _) => { started.TrySetResult(); await gate.Task; }, subscriber);

        var first = new RecordingDelivery();
        await dispatcher.HandleAsync(first.Create("p1", 1), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2)); // worker is now busy on p1; queue is empty

        var second = new RecordingDelivery();
        await dispatcher.HandleAsync(second.Create("p2", 1), CancellationToken.None); // fills the single queue slot

        var third = new RecordingDelivery();
        await dispatcher.HandleAsync(third.Create("p3", 1), CancellationToken.None); // queue full → NAK

        Assert.Equal(1, first.Acks);
        Assert.Equal(1, second.Acks);
        Assert.Equal(0, third.Acks);
        Assert.Single(third.Naks);

        gate.TrySetResult();
    }

    [Fact]
    public async Task HandlerFailureAtMaxAttempts_WhenDeadLetterPublishFails_NaksForRedelivery()
    {
        _jetStream.PublishFailureForAttempt = _ => new InvalidOperationException("dead-letter stream down");
        var rec = new RecordingDelivery();
        var subscriber = new NatsSubscriberOptions { MaxDeliveryAttempts = 1, RedeliveryDelay = TimeSpan.FromSeconds(11) };
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("boom"), subscriber);

        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);

        Assert.Equal(0, rec.Terms);
        Assert.Equal([TimeSpan.FromSeconds(11)], rec.Naks);
        Assert.Empty(_jetStream.Published);
    }

    [Fact]
    public async Task EarlyAck_ToleratesThrowingBackgroundFailureCallback()
    {
        var callbackInvoked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions
        {
            OnBackgroundFailure = _ =>
            {
                callbackInvoked.TrySetResult();
                throw new InvalidOperationException("callback boom");
            }
        }.UseAckAfterReceive(backgroundWorkerCount: 1, backgroundQueueCapacity: 4);
        await using var dispatcher = CreateDispatcher((_, _) => throw new InvalidOperationException("bg"), subscriber);

        var rec = new RecordingDelivery();
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);

        // The throwing OnBackgroundFailure callback is invoked and its exception is swallowed (no crash).
        await callbackInvoked.Task.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task DisposeAsync_LogsAndCompletes_WhenBackgroundDrainTimesOut()
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions().UseAckAfterReceive(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 4,
            backgroundDrainTimeout: TimeSpan.FromMilliseconds(50));
        var dispatcher = CreateDispatcher(async (_, _) => { started.TrySetResult(); await gate.Task; }, subscriber);

        var rec = new RecordingDelivery();
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2)); // worker is blocked in the handler

        // Drain cannot complete within the timeout; DisposeAsync must still return (not hang).
        await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));

        gate.TrySetResult();
    }

    [Fact]
    public async Task DisposeAsync_DrainTimeout_RunningWorkerStillCompletesFailurePathAfterDispose()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failure = new TaskCompletionSource<NatsBackgroundFailureContext>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new NatsSubscriberOptions
        {
            OnBackgroundFailure = ctx =>
            {
                failure.TrySetResult(ctx);
                return ValueTask.CompletedTask;
            }
        }.UseAckAfterReceive(
            backgroundWorkerCount: 1,
            backgroundQueueCapacity: 4,
            backgroundDrainTimeout: TimeSpan.FromMilliseconds(50));
        var dispatcher = CreateDispatcher(
            async (_, cancellationToken) =>
            {
                started.TrySetResult();
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken);
            },
            subscriber);

        var rec = new RecordingDelivery();
        await dispatcher.HandleAsync(rec.Create("payload", numDelivered: 1), CancellationToken.None);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2)); // worker is blocked in the handler

        await dispatcher.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));

        // The drain CTS must stay alive until the worker actually finishes: after the timed-out dispose the
        // cancelled handler still dead-letters and reports through the failure callback (no ObjectDisposedException).
        var context = await failure.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.IsAssignableFrom<OperationCanceledException>(context.Exception);
        Assert.Contains(_jetStream.Published, p => p.Subject == DeadLetterSubject);
    }
}
