using AsyncResponse.Channels.NATS;
using AsyncResponse.Testing;
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
    public async Task CreateResponseWaiter_TerminalDeliveryDuringRecoverySave_CompensatesTheOrphanedRegistration()
    {
        // The registration-save race: the consume loop is live before the recovery state is
        // saved. A terminal response landing in that window runs cleanup, whose delete no-ops
        // (nothing saved yet); the save then commits an orphaned callback-armed registration that
        // would resurrect recovery for a completed wait on any later publish. The creator must
        // compensate with a second delete after the save — and must not flush again for a wait
        // that already ended (the one subscription flush happens BEFORE the save, establishing
        // "recovery state visible => subscription visible").
        //
        // Determinism: the consume loop processes the pushed message on its own schedule, so the
        // save completes only once cleanup's delete has been OBSERVED — cleanup sets
        // cleanupStarted before that delete, so the post-save check deterministically sees it.
        var saveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleanupDeleteIssued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                saveStarted.TrySetResult();
                await cleanupDeleteIssued.Task;
            });
        _store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Callback(() => cleanupDeleteIssued.TrySetResult());
        var channel = CreateChannel();

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-save-race");
        await saveStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        _client.Push(JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed, Message = "fast" }
        }, AsyncResponseEnvelopeOptions<OperationResult>.Instance));

        await using var waiter = await waiterTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(OperationStatus.Completed, (await waiter.ResponseTask).Status);

        // One delete from cleanup (pre-save no-op) plus the post-save compensation. Exactly the
        // one pre-save subscription flush — the reorder that closed the save-before-visibility
        // window — and no second flush for a wait that already ended (its lifetime token is
        // disposed by cleanup).
        _store.Verify(
            s => s.TryDeleteAsync("corr-save-race", It.IsAny<Guid>(), It.IsAny<CancellationToken>()),
            Times.AtLeast(2));
        Assert.Equal(1, _client.FlushCount);
    }

    [Fact]
    public async Task CreateResponseWaiter_FlushesTheSubscriptionBeforeSavingRecoveryState()
    {
        // Regression: the recovery state was saved BEFORE the flush that makes the subscription
        // server-visible, inverting the "recovery state visible => subscription visible" ordering
        // the DB base documents and Redis follows. A publish landing in that window found the
        // registration, probed the not-yet-visible subscription, and consumed a live waiter's
        // recovery arm — the waiter then resumed twice.
        var flushesWhenSaveStarted = -1;
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                flushesWhenSaveStarted = _client.FlushCount;
                return Task.CompletedTask;
            });
        var channel = CreateChannel();

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-flush-order");

        Assert.Equal(1, flushesWhenSaveStarted);
    }

    [Fact]
    public async Task CreateResponseWaiter_TimeoutFiresOnTheInjectedClock()
    {
        // Redis-parity regression: the waiter timeout was armed on a default
        // CancellationTokenSource (the system clock) while RegisteredAtUtc came from the injected
        // TimeProvider — under a virtual clock a production-sized timeout could never fire.
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        var channel = CreateChannel(timeProvider: clock);

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-virtual-timeout",
            timeout: TimeSpan.FromMinutes(10));

        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(5);
        while (!waiter.ResponseTask.IsCompleted)
        {
            Assert.True(TimeProvider.System.GetUtcNow() < guard, "advancing the virtual clock never fired the waiter timeout");
            clock.Advance(TimeSpan.FromMinutes(11));
            await Task.Delay(TimeSpan.FromMilliseconds(5));
        }

        var timeout = await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask);
        Assert.Contains("corr-virtual-timeout", timeout.Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_SubscribeWaitingOnAReconnectingConnection_ThrowsOnceTheWaiterTimeoutLapses()
    {
        // While the NATS connection reconnects, subscribe waits for it to reopen — forever, as far
        // as NATS.Net is concerned — and the waiter timeout was only armed after registration, so
        // CreateResponseWaiter hung for the whole outage. The resolved timeout now bounds
        // registration; the abandoned subscribe's lifetime token is cancelled so it can never
        // install orphan interest after the reconnect.
        var clock = new VirtualTimeProvider();
        var subscribeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _client.SubscribeBehavior = async token =>
        {
            subscribeEntered.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
        };
        var channel = CreateChannel(timeProvider: clock);

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-reg-subscribe", timeout: TimeSpan.FromSeconds(30));
        await subscribeEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        clock.Advance(TimeSpan.FromSeconds(31));

        Assert.Same(waiterTask, await Task.WhenAny(waiterTask, Task.Delay(TimeSpan.FromSeconds(10))));
        var timeout = await Assert.ThrowsAsync<TimeoutException>(() => waiterTask);
        Assert.Contains("corr-reg-subscribe", timeout.Message, StringComparison.Ordinal);
        Assert.True(_client.SubscriptionLifetime.IsCancellationRequested);
        Assert.False(_client.HasSubscription);
        _store.Verify(
            s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task CreateResponseWaiter_FlushWaitingOnAReconnectingConnection_ThrowsAndTearsTheSubscriptionDown()
    {
        var clock = new VirtualTimeProvider();
        var flushEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _client.FlushBehavior = async token =>
        {
            flushEntered.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
        };
        var channel = CreateChannel(timeProvider: clock);

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-reg-flush", timeout: TimeSpan.FromSeconds(30));
        await flushEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        clock.Advance(TimeSpan.FromSeconds(31));

        Assert.Same(waiterTask, await Task.WhenAny(waiterTask, Task.Delay(TimeSpan.FromSeconds(10))));
        await Assert.ThrowsAsync<TimeoutException>(() => waiterTask);
        _store.Verify(
            s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()),
            Times.Never);
        await Eventually(() => _client.SubscriptionDisposeCount == 1);
    }

    [Fact]
    public async Task CreateResponseWaiter_RecoverySaveWaitingOnAReconnectingConnection_ThrowsAndStillDeletesTheRegistration()
    {
        var clock = new VirtualTimeProvider();
        var saveEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns((string _, RecoveryState _, TimeSpan _, CancellationToken token) =>
            {
                saveEntered.TrySetResult();
                return Task.Delay(Timeout.Infinite, token);
            });
        var channel = CreateChannel(timeProvider: clock);

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-reg-save", timeout: TimeSpan.FromSeconds(30));
        await saveEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        clock.Advance(TimeSpan.FromSeconds(31));

        Assert.Same(waiterTask, await Task.WhenAny(waiterTask, Task.Delay(TimeSpan.FromSeconds(10))));
        await Assert.ThrowsAsync<TimeoutException>(() => waiterTask);
        // The save may have committed before the connection dropped; the background cleanup
        // still deletes it and ends the subscription.
        await Eventually(() => _store.Invocations.Any(i => i.Method.Name == nameof(IRecoveryStateStore.TryDeleteAsync)));
        await Eventually(() => _client.SubscriptionDisposeCount == 1);
    }

    [Fact]
    public async Task CreateResponseWaiter_SaveFailureAfterTerminalSettledTheWait_ReturnsTheCompletedWaiter()
    {
        // A terminal delivery settles the wait while the recovery-state save is in flight, and
        // the save then FAILS. Rethrowing (the plain subscribe-failure contract) would discard a
        // response the waiter already holds — the exact loss the library exists to prevent — and
        // the same interleaving with a healthy store returns the completed waiter. The save is
        // released by failing only once cleanup's delete has been observed, so the failure is
        // deterministically post-settlement.
        var saveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleanupDeleteIssued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                saveStarted.TrySetResult();
                await cleanupDeleteIssued.Task;
                throw new InvalidOperationException("recovery save failed");
            });
        _store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Callback(() => cleanupDeleteIssued.TrySetResult());
        var channel = CreateChannel();

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-save-fail");
        await saveStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        _client.Push(JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed, Message = "settled" }
        }, AsyncResponseEnvelopeOptions<OperationResult>.Instance));

        await using var waiter = await waiterTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("settled", (await waiter.ResponseTask).Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_SaveFailureWhileADeliveryIsStillInsideUntil_ReturnsTheWaiterTheDrainLetSettle()
    {
        // The settled-waiter filter ran BEFORE the generic catch's drain, and the drain JOINS the
        // consume loop: a terminal delivery still inside the Until predicate when the save failed
        // settled the wait during that drain, and the unconditional rethrow then discarded a
        // response the publisher had been told was delivered. The create now re-checks after the
        // drain. Deterministic: the predicate is released only from inside the drain's
        // registration delete — strictly after the filter was evaluated.
        var insidePredicate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePredicate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var saveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                saveStarted.TrySetResult();
                await insidePredicate.Task;
                throw new InvalidOperationException("recovery save failed");
            });
        _store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Callback(() => releasePredicate.TrySetResult());
        var channel = CreateChannel();

        var waiterTask = channel.CreateResponseWaiter<OperationResult>(
            "corr-save-fail-mid-until",
            completionPredicate: async _ =>
            {
                insidePredicate.TrySetResult();
                await releasePredicate.Task;
                return true;
            });
        await saveStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        _client.Push(JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed, Message = "in-flight" }
        }, AsyncResponseEnvelopeOptions<OperationResult>.Instance));

        await using var waiter = await waiterTask.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("in-flight", (await waiter.ResponseTask).Message);
    }

    [Fact]
    public async Task CreateResponseWaiter_RegistrationFailureBehindAThrowingLogger_StillCleansUpAndThrowsTheFailure()
    {
        // The generic catch logged BEFORE cleaning up, so a throwing logging provider (MEL rethrows
        // provider failures) skipped the cleanup: the subscription and registration stayed behind
        // with no timer — read as a live waiter by the probe, consuming the next response for the
        // id — and the caller got the logger's exception instead of the registration failure.
        var failure = new InvalidOperationException("save failed");
        _store.Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(failure);
        var logger = new RecordingThrowingLogger<NatsAsyncResponseChannel> { ThrowOnMessageContaining = "Failed to subscribe" };
        var channel = CreateChannel(logger: logger);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => channel.CreateResponseWaiter<OperationResult>("corr-throwing-logger", timeout: TimeSpan.FromSeconds(5)));

        Assert.Same(failure, ex);
        Assert.Equal(1, _client.SubscriptionDisposeCount);
        _store.Verify(s => s.TryDeleteAsync("corr-throwing-logger", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CreateResponseWaiter_RegistrationBudgetLapseBehindAThrowingLogger_StillCancelsTheAbandonedSubscribe()
    {
        // The budget-lapse catch logged BEFORE cancelling the abandoned subscribe's lifetime token:
        // behind a throwing logging provider the pending subscribe later installed orphan interest
        // after the reconnect — the leak that catch exists to prevent.
        var clock = new VirtualTimeProvider();
        var subscribeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _client.SubscribeBehavior = async token =>
        {
            subscribeEntered.TrySetResult();
            await Task.Delay(Timeout.Infinite, token);
        };
        var logger = new RecordingThrowingLogger<NatsAsyncResponseChannel> { ThrowOnMessageContaining = "did not complete within" };
        var channel = CreateChannel(timeProvider: clock, logger: logger);

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-lapse-throwing-logger", timeout: TimeSpan.FromSeconds(30));
        await subscribeEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        clock.Advance(TimeSpan.FromSeconds(31));

        await Assert.ThrowsAsync<TimeoutException>(() => waiterTask.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.True(_client.SubscriptionLifetime.IsCancellationRequested);
    }

    [Fact]
    public async Task CreateResponseWaiter_SettledWaiterWarningBehindAThrowingLogger_StillReturnsTheDeliveredResponse()
    {
        // The settled-waiter Warning was unguarded: a throwing logging provider turned a delivered
        // response into a create failure after all — the loss that branch exists to prevent.
        var saveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleanupDeleteIssued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                saveStarted.TrySetResult();
                await cleanupDeleteIssued.Task;
                throw new InvalidOperationException("recovery save failed");
            });
        _store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Callback(() => cleanupDeleteIssued.TrySetResult());
        var logger = new RecordingThrowingLogger<NatsAsyncResponseChannel> { ThrowOnMessageContaining = "Registration step failed after a delivery settled" };
        var channel = CreateChannel(logger: logger);

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-settled-throwing-logger");
        await saveStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        _client.Push(JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed, Message = "settled" }
        }, AsyncResponseEnvelopeOptions<OperationResult>.Instance));

        await using var waiter = await waiterTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("settled", (await waiter.ResponseTask).Message);
    }

    [Fact]
    public async Task ConsumeLoop_AnAcknowledgementStuckBehindADisconnect_DoesNotHoldBackTheResponse()
    {
        // The consume loop awaited the reply (ack) publish before processing the message, and while
        // the connection reconnects that publish waits for the reconnect. A response already
        // received sat behind the outage until the waiter timed out, and was then processed into a
        // settled wait and dropped — a durable step restarted and re-triggered the remote operation.
        var channel = CreateChannel();
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-stuck-ack", timeout: TimeSpan.FromMinutes(1));
        var reconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        _client.PushWithReply(
            JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
            {
                Success = true,
                Payload = new OperationResult { Status = OperationStatus.Completed, Message = "received" }
            }, AsyncResponseEnvelopeOptions<OperationResult>.Instance),
            () => new ValueTask(reconnected.Task));

        try
        {
            Assert.Equal("received", (await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5))).Message);
        }
        finally
        {
            reconnected.TrySetResult();
        }
    }

    [Fact]
    public async Task ConsumeLoop_AMessageTheClientDropped_FaultsTheWaitAsOverloaded_AndCountsIt()
    {
        // NATS.Net buffers a subscription's inbound messages in a bounded channel (16,384 by
        // default) and DROPS the newest once it is full — behind a slow Until predicate under a
        // flood of responses. The dropped message may have been the terminal one, and its publisher
        // saw no reply and counted it delivered: a silent loss on both sides. Redis parity: the
        // wait is faulted with the overload form of the indeterminate contract, ended, and counted.
        var measurements = new List<string?>();
        using var listener = new System.Diagnostics.Metrics.MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == AsyncResponseDiagnostics.MeterName && instrument.Name == "asyncresponse.channel.overloaded_waits")
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "channel")
                {
                    lock (measurements)
                        measurements.Add(tag.Value?.ToString());
                }
            }
        });
        listener.Start();

        using var activities = new AsyncResponseActivityCollector();
        var channel = CreateChannel();
        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-overloaded", timeout: TimeSpan.FromMinutes(1));

        _client.DropMessage(buffered: 16_384);
        _client.DropMessage(buffered: 16_384); // every further drop reports again; the wait is faulted once

        var overload = await Assert.ThrowsAsync<AsyncResponseIndeterminateDeliveryException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal("corr-overloaded", overload.CorrelationId);
        Assert.Equal(16_384, overload.BufferedMessages);
        await Eventually(() => _client.SubscriptionDisposeCount == 1); // ended: registration deleted, stream unsubscribed
        _store.Verify(s => s.TryDeleteAsync("corr-overloaded", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Once);
        lock (measurements)
            Assert.Single(measurements, channelTag => channelTag == "nats");
        await waiter.DisposeAsync();
        Assert.Equal("overloaded", AsyncResponseActivityCollector.Tag(activities.Single("asyncresponse.wait", "asyncresponse.channel", "nats"), "error.type"));
    }

    [Fact]
    public async Task DuplicateErrorEnvelope_AfterTheWaitSettled_IsNotLoggedWithTheRemoteMessage()
    {
        // The "already completed" branch still attached the remote failure to its Warning — the
        // remote-chosen message (uncapped, with CR/LF that forge log entries) that the first
        // error's log line deliberately leaves out. Redis parity: the drop is logged without it.
        var logger = new CollectingLogger();
        var channel = CreateChannel(logger: logger.For<NatsAsyncResponseChannel>());
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-dup-error", timeout: TimeSpan.FromSeconds(5));
        var hostile = JsonSerializer.Serialize(
            new AsyncResponseEnvelope<OperationResult> { Success = false, ExceptionMessage = "boom\r\nFORGED entry" },
            AsyncResponseEnvelopeOptions<OperationResult>.Instance);

        _client.Push(hostile);
        _client.Push(hostile);

        await Assert.ThrowsAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
        await logger.WaitForAsync("the error response was dropped");
        Assert.All(logger.Entries, entry =>
        {
            Assert.DoesNotContain("FORGED", entry.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("FORGED", entry.Exception?.Message ?? string.Empty, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task CreateResponseWaiter_ConsumeLoopFailureDuringRegistration_ThrowsInsteadOfReturningAFaultedWaiter()
    {
        // A consume-loop death faults the response task with a TRANSPORT error — nothing was
        // delivered and nothing ever will be. Treating that faulted task as a settled wait
        // returned a waiter, and the builder then fired the remote trigger with no live
        // subscription and no recovery state left to route its response. Registration must throw
        // so the operation never starts. Deterministic: the loop's cleanup deletes the recovery
        // state, and observing that delete is what releases the parked save — so the loop death
        // is strictly inside the registration window.
        var saveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleanupDeleteIssued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                saveStarted.TrySetResult();
                await cleanupDeleteIssued.Task;
            });
        _store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Callback(() => cleanupDeleteIssued.TrySetResult());
        var channel = CreateChannel();

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-loop-death");
        await saveStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        _client.FailSubscription(new IOException("stream down"));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => waiterTask.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Contains("failed before registration completed", ex.Message, StringComparison.Ordinal);
        Assert.IsType<IOException>(ex.InnerException);
    }

    [Fact]
    public async Task CreateResponseWaiter_LoopFaultLosingToATerminalPayload_StillReturnsTheDeliveredResponse()
    {
        // The other direction of the loop-death guard: a terminal payload settles the wait, and
        // the consume loop THEN faults (its next read observes the failed stream). The loop's
        // settlement attempt loses — so it must leave no mark: aborting the registration here
        // would discard a response the waiter already holds. A side-band flag raced exactly this
        // way; the settlement-source marker cannot (only a fault that WINS the settlement marks
        // the task). Deterministic: the terminal message and the stream failure are enqueued
        // before the loop processes either, and the parked save releases only once the terminal's
        // cleanup delete is observed.
        var saveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleanupDeleteIssued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                saveStarted.TrySetResult();
                await cleanupDeleteIssued.Task;
            });
        _store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Callback(() => cleanupDeleteIssued.TrySetResult());
        var channel = CreateChannel();

        var waiterTask = channel.CreateResponseWaiter<OperationResult>("corr-loop-lost-race");
        await saveStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        _client.Push(JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = OperationStatus.Completed, Message = "delivered" }
        }, AsyncResponseEnvelopeOptions<OperationResult>.Instance));
        _client.FailSubscription(new IOException("stream down after delivery"));

        await using var waiter = await waiterTask.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("delivered", (await waiter.ResponseTask).Message);
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
    public async Task CreateResponseWaiter_RemoteFailureMessage_NeverReachesTheLogOrTheSpanRawOrUnbounded()
    {
        // The stack trace was capped on receive, but the remote's message went raw into a Warning
        // on every delivery and into the span status — up to the whole inbound budget (8 Mi chars),
        // with CR/LF that forge log lines. The DB channels never logged it at all.
        using var activities = new AsyncResponseActivityCollector();
        var logger = new CollectingLogger();
        var channel = CreateChannel(logger: logger.For<NatsAsyncResponseChannel>());
        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-hostile", timeout: TimeSpan.FromSeconds(5));
        var hostile = "boom\r\nFORGED entry " + new string('x', 100_000);

        _client.Push(JsonSerializer.Serialize(
            new AsyncResponseEnvelope<OperationResult> { Success = false, ExceptionMessage = hostile },
            AsyncResponseEnvelopeOptions<OperationResult>.Instance));

        // The waiter itself still receives the message untouched.
        var ex = await Assert.ThrowsAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(hostile, ex.Message);
        await waiter.DisposeAsync();

        Assert.Contains(logger.Messages, message => message.Contains("Received error response for correlationId corr-hostile", StringComparison.Ordinal));
        Assert.All(logger.Messages, message => Assert.DoesNotContain("FORGED", message, StringComparison.Ordinal));
        var status = activities.Single("asyncresponse.wait", "asyncresponse.channel", "nats").StatusDescription!;
        Assert.DoesNotContain('\r', status);
        Assert.DoesNotContain('\n', status);
        Assert.True(status.Length < 2_000, $"span status carried {status.Length} characters");
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

        // The body-free parse failure (JsonSafety), not the raw reader's JsonException.
        await Assert.ThrowsAsync<InvalidDataException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
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
    public async Task WaiterTimeout_DrainsTheConsumeLoopBeforeFaulting_SoAnInFlightDeliveryStillWins()
    {
        // Regression (round 29): the timeout faulted the waiter BEFORE draining the consume loop, so
        // a message the subscription had already received — sitting mid Until-predicate, and which
        // the publisher was told was delivered — lost the race and a consumed response was reported
        // as a timeout. The terminal exception is now applied after the drain, where TrySet loses.
        var channel = CreateChannel();
        var insidePredicate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releasePredicate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-drain-timeout",
            completionPredicate: async _ =>
            {
                insidePredicate.TrySetResult();
                await releasePredicate.Task;
                return true;
            },
            timeout: TimeSpan.FromMilliseconds(50));

        _client.Push(JsonSerializer.Serialize(
            new AsyncResponseEnvelope<OperationResult>
            {
                Success = true,
                Payload = new OperationResult { Status = OperationStatus.Completed, Message = "delivered" }
            },
            AsyncResponseEnvelopeOptions<OperationResult>.Instance));
        await insidePredicate.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // The 50ms timeout fires while the predicate still holds the consume loop.
        await Task.Delay(300);
        Assert.False(waiter.ResponseTask.IsCompleted, "the waiter was settled before the in-flight delivery had drained");

        releasePredicate.TrySetResult();

        var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal("delivered", result.Message);
    }

    [Fact]
    public async Task WaiterTimeout_DeletesTheRecoveryRegistrationWhileTheSubscriptionIsStillLive()
    {
        // The subscription was bound to the timeout source, and NATS.Net ends a subscription the
        // moment that token is cancelled — so at CancelAfter the waiter lost its server-side
        // interest, and the drain then unsubscribed before the recovery registration was deleted.
        // A publish in that window saw "no responders, registration present" and fired the
        // recovery callback while the waiter faulted with a timeout.
        var clock = new VirtualTimeProvider();
        (int Disposed, bool LifetimeCancelled)? atDelete = null;
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Callback(() => atDelete ??= (_client.SubscriptionDisposeCount, _client.SubscriptionLifetime.IsCancellationRequested));
        var channel = CreateChannel(timeProvider: clock);
        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-timeout-order", timeout: TimeSpan.FromMinutes(10));

        clock.Advance(TimeSpan.FromMinutes(11));
        await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
        await Eventually(() => atDelete is not null);

        Assert.Equal<(int, bool)?>((0, false), atDelete);
    }

    [Fact]
    public async Task WaiterDispose_DeletesTheRecoveryRegistrationBeforeUnsubscribing()
    {
        (int Disposed, bool LifetimeCancelled)? atDelete = null;
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Callback(() => atDelete ??= (_client.SubscriptionDisposeCount, _client.SubscriptionLifetime.IsCancellationRequested));
        var channel = CreateChannel();
        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-dispose-order", timeout: TimeSpan.FromMinutes(1));

        await waiter.DisposeAsync();

        Assert.Equal<(int, bool)?>((0, false), atDelete);
        Assert.Equal(1, _client.SubscriptionDisposeCount);
        // One delete: the drain's, the cleanup core does not repeat it.
        _store.Verify(s => s.TryDeleteAsync("corr-dispose-order", It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task WaiterTimeout_RegistrationDeleteWaitingOnAReconnectingConnection_StillSettlesWithinTheDrainBudget()
    {
        // Pre-commit review of fixpoint round 1: the timeout drain awaited the recovery-registration
        // delete — a KV round trip with no token — before anything else, so during a NATS outage a
        // timed-out waiter's ResponseTask stayed pending until the connection came back ("waits are
        // never infinite"). The delete now runs inside the drain budget. Its lapse proves nothing
        // about a delivery the live subscription may hold, so the waiter faults as indeterminate.
        //
        // Fixpoint round 2: the cleanup core then RETRIED that delete with no bound, so disposing
        // the timed-out waiter still blocked until the reconnect. It now waits on the drain's own
        // attempt — never a second delete beside it — for at most one more DisposalDrainTimeout,
        // then tears the subscription down (the ordering is moot while disconnected).
        var clock = new VirtualTimeProvider();
        var reconnected = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var deletes = 0;
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                Interlocked.Increment(ref deletes);
                return reconnected.Task;
            });
        var channel = CreateChannel(drainTimeout: TimeSpan.FromMilliseconds(200), timeProvider: clock);
        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-timeout-outage", timeout: TimeSpan.FromMinutes(10));

        clock.Advance(TimeSpan.FromMinutes(11));

        var fault = await Assert.ThrowsAsync<AsyncResponseIndeterminateDeliveryException>(
            () => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal("corr-timeout-outage", fault.CorrelationId);

        try
        {
            // Still inside the outage: disposal completes without the reconnect.
            await waiter.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(1, Volatile.Read(ref deletes));
            Assert.Equal(1, _client.SubscriptionDisposeCount);
        }
        finally
        {
            reconnected.TrySetResult(true);
        }
    }

    [Fact]
    public async Task WaiterDispose_DuringANatsOutage_CompletesWithoutWaitingForTheReconnect()
    {
        // Fixpoint round 2: disposing an ordinary waiter during an outage blocked until the
        // connection came back — the drain's delete lapsed its budget as intended, but the latched
        // cleanup core then retried the KV delete with no bound (NATS.Net waits for the reconnect).
        // Every `await using` waiter and every flow step disposing its waiter (holding its delivery
        // and lease) hung for the whole outage. Disposal is now bounded by the drain budget plus
        // one more for the core, with a single delete attempt.
        var channel = CreateChannel(drainTimeout: TimeSpan.FromMilliseconds(200));
        var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-dispose-outage", timeout: TimeSpan.FromMinutes(10));

        // The outage begins: every KV round trip now waits for a reconnect that has not happened.
        var reconnected = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var deletes = 0;
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                Interlocked.Increment(ref deletes);
                return reconnected.Task;
            });

        try
        {
            await waiter.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

            // The drain's lapse proves nothing about a delivery the live subscription may hold.
            await Assert.ThrowsAsync<AsyncResponseIndeterminateDeliveryException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.Equal(1, Volatile.Read(ref deletes));
            Assert.Equal(1, _client.SubscriptionDisposeCount);
        }
        finally
        {
            reconnected.TrySetResult(true);
        }
    }

    [Fact]
    public async Task CreateResponseWaiter_RegistrationFailureDuringAnOutage_IsNotReportedAsAnIndeterminateDelivery()
    {
        // A registration that fails (here the save) while the NATS connection is down: the drain's
        // delete lapses too, the drain's generic catch faulted the never-returned task as
        // indeterminate — an UnobservedTaskException, a false "faulting the waiter as
        // indeterminate" Warning, and the span's subscribe_failure status overwritten — and the
        // unbounded core retry then held CreateResponseWaiter itself until the reconnect. No
        // waiter was handed out: the create now throws the registration failure, bounded.
        using var activities = new AsyncResponseActivityCollector();
        var failure = new InvalidOperationException("save failed");
        var reconnected = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        _store.Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(failure);
        _store.Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .Returns(() => reconnected.Task);
        var logger = new RecordingThrowingLogger<NatsAsyncResponseChannel>();
        var channel = CreateChannel(drainTimeout: TimeSpan.FromMilliseconds(200), logger: logger);

        try
        {
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => channel.CreateResponseWaiter<OperationResult>("corr-reg-fail-outage", timeout: TimeSpan.FromSeconds(5)).WaitAsync(TimeSpan.FromSeconds(10)));

            Assert.Same(failure, ex);
            Assert.False(logger.HasEntry(LogLevel.Warning, "faulting the waiter as indeterminate"));
            var span = activities.Single("asyncresponse.wait", "asyncresponse.channel", "nats");
            Assert.Equal("subscribe_failure", AsyncResponseActivityCollector.Tag(span, "error.type"));
            Assert.Equal(1, _client.SubscriptionDisposeCount);
        }
        finally
        {
            reconnected.TrySetResult(true);
        }
    }

    [Fact]
    public async Task CreateRecoverableResponseWaiter_StampsRegisteredAtFromTheInjectedTimeProvider()
    {
        // Regression (round 29): the stamp came from DateTime.UtcNow, which no host can substitute.
        // The watchdog judges staleness as "utcNow - RegisteredAtUtc" from whichever host scans, so
        // a skewed stamp made registrations either never age (a stuck flow stays invisible) or age
        // instantly (healthy waits page the operator every scan).
        var time = new VirtualTimeProvider(new DateTimeOffset(2031, 5, 4, 3, 2, 1, TimeSpan.Zero));
        RecoveryState? saved = null;
        _store
            .Setup(s => s.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .Callback((string _, RecoveryState state, TimeSpan _, CancellationToken _) => saved = state)
            .Returns(Task.CompletedTask);

        var channel = CreateChannel(timeProvider: time);

        await using var waiter = await channel.CreateRecoverableResponseWaiter<OperationResult>(
            "corr-registered-at",
            resumeCallback: new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IAsyncResponsePublisher).FullName!,
                MethodName = "Resume",
                Params = []
            },
            timeout: TimeSpan.FromSeconds(5));

        Assert.NotNull(saved);
        Assert.Equal(time.GetUtcNow().UtcDateTime, saved!.RegisteredAtUtc);
    }

    [Fact]
    public async Task WaiterTimeout_WhenTimeoutHandlingThrows_LogsInsteadOfLeavingAnUnobservedFault()
    {
        // The timeout body runs on a fire-and-forget Task.Run; an exception escaping it (here a
        // logger provider that throws on the timeout warning) must be caught and logged through
        // the error path, not die as an unobserved task fault.
        var logger = new RecordingThrowingLogger<NatsAsyncResponseChannel> { ThrowOnMessageContaining = "Timed out waiting" };
        var channel = new NatsAsyncResponseChannel(
            _services.GetRequiredService<IServiceScopeFactory>(),
            _client,
            _store.Object,
            Options.Create(new NatsAsyncResponseChannelOptions
            {
                DefaultTimeout = TimeSpan.FromSeconds(5),
                RecoveryStateExpiry = TimeSpan.FromMinutes(5),
                DisposalDrainTimeout = TimeSpan.FromSeconds(30)
            }),
            new AsyncResponseContextPropagation([]),
            logger);

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>(
            "corr-timeout-throws",
            timeout: TimeSpan.FromMilliseconds(5));

        await Eventually(() => logger.HasEntry(LogLevel.Error, "Error handling waiter timeout"));
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

        // NoReply means interest EXISTED and the ping was delivered — only the ack was slow, which
        // is routine for a live waiter inside a slow Until predicate (the consume loop acks a probe
        // only when it reads it, after the previous message's predicate returns). Reporting 0 there
        // flagged healthy waiters stale and let the lost-subscriber dispatcher consume their
        // recovery registration; -1 is the watchdog's "could not be probed".
        _client.OutcomeForProbe = _ => NatsDeliveryOutcome.NoReply;
        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("corr-a"));
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
    public async Task SetRawResponseJson_ThrowsAndKeepsRegistration_WhenLivenessCannotBeProbed()
    {
        // Regression (r24): the lost-subscriber re-check routed through the public probe, which
        // swallowed every failure into 0 — an UNPROBEABLE server read as "no live waiter", so a
        // live waiter's recovery registration was consumed (and its callback fired) during a
        // request blip while the waiter was still awaiting. An unprobeable result now propagates:
        // the publish throws, the registration stays intact, and the caller's retry machinery
        // re-attempts (DB-channel parity).
        _client.NextOutcome = NatsDeliveryOutcome.NoResponders;
        _client.OutcomeForProbe = _ => throw new InvalidOperationException("probe request failed");
        _store.Setup(s => s.GetAllAsync("corr-unprobeable", It.IsAny<CancellationToken>()))
            .ReturnsAsync([new RecoveryState { CorrelationId = "corr-unprobeable", RegistrationId = Guid.NewGuid() }]);
        var channel = CreateChannel();
        var raw = (IRawAsyncResponsePublisher)channel;

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => raw.SetRawResponseJson("""{"Status":2,"Message":"late"}""", "corr-unprobeable"));
        Assert.Contains("could not be probed", exception.Message);

        _store.Verify(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CountActiveSubscribers_ReturnsNegativeWhenProbeFails()
    {
        _client.OutcomeForProbe = _ => throw new InvalidOperationException("probe failed");
        var channel = CreateChannel();

        // Negative = "could not be probed" (the watchdog's unknown-liveness contract): 0 would
        // assert there is definitively no live waiter.
        Assert.Equal(-1, await channel.CountActiveSubscribersAsync("corr-a"));
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

    private NatsAsyncResponseChannel CreateChannel(bool useRecoveryExpiry = false, TimeSpan? drainTimeout = null, TimeProvider? timeProvider = null, ILogger<NatsAsyncResponseChannel>? logger = null) => new(
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
        logger ?? new TestLogger<NatsAsyncResponseChannel>(),
        timeProvider);

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

        mockClient.Setup(c => c.SubscribeAsync(It.IsAny<string>(), It.IsAny<Action<int>>(), It.IsAny<CancellationToken>()))
            .Callback<string, Action<int>, CancellationToken>((sub, _, token) =>
            {
                messageChannel.Writer.TryWrite(new NatsInboundResponse(json, false, () => ValueTask.CompletedTask));
            })
            .ReturnsAsync(subscriptionMock.Object);

        await using var waiter = await channel.CreateResponseWaiter<OperationResult>("corr-sync");
        var result = await waiter.ResponseTask;
        Assert.Equal(OperationStatus.Completed, result.Status);
    }
}
