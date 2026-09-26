using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The registry coordinates per-channel serial-executor lifecycle. Its core guarantee is that a
/// message handed to <see cref="SerialExecutorRegistry.EnqueueAsync"/> for a channel with a live
/// registration is never dropped because the executor it would have used is concurrently being
/// retired — the lifecycle race the old ConcurrentDictionary + fire-and-forget removal scheme was
/// vulnerable to when a correlation id was reused mid-drain. Channels with no registration are
/// tombstoned after retirement so a straggling enqueue cannot recreate (and leak) an executor.
/// </summary>
public class SerialExecutorRegistryTests
{
    [Fact]
    public async Task Enqueue_RunsWork()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        var ran = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await registry.EnqueueAsync("cid", () => { ran.TrySetResult(); return Task.CompletedTask; });

        await ran.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await registry.RemoveAsync("cid");
    }

    [Fact]
    public async Task SingleChannel_NoRemoval_RunsInOrder()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        var order = new ConcurrentQueue<int>();

        for (var i = 0; i < 100; i++)
        {
            var index = i;
            await registry.EnqueueAsync("cid", async () => { await Task.Yield(); order.Enqueue(index); });
        }

        await registry.RemoveAsync("cid"); // drains

        Assert.Equal(Enumerable.Range(0, 100).ToArray(), order.ToArray());
    }

    [Fact]
    public async Task RemoveAsync_DrainsQueuedWork()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        var completed = 0;

        for (var i = 0; i < 20; i++)
            await registry.EnqueueAsync("cid", async () => { await Task.Delay(2); Interlocked.Increment(ref completed); });

        await registry.RemoveAsync("cid");

        Assert.Equal(20, completed);
    }

    [Fact]
    public async Task ConcurrentRemovers_ShareOneRetirement()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await registry.EnqueueAsync("cid", async () =>
        {
            started.TrySetResult();
            await release.Task;
        });
        await started.Task.WaitAsync(TimeSpan.FromSeconds(2));

        var first = registry.RemoveAsync("cid").AsTask();
        var second = registry.RemoveAsync("cid").AsTask();
        Assert.False(first.IsCompleted);
        Assert.False(second.IsCompleted);

        release.TrySetResult();
        await Task.WhenAll(first, second).WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task EnqueueAfterRemove_CreatesFreshExecutorAndRuns()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);

        // A live registration keeps the retirement tombstone from dropping the later enqueue —
        // the same bookkeeping every channel performs for its active subscriptions.
        registry.OnSubscriptionRegistered("cid");
        await registry.EnqueueAsync("cid", () => Task.CompletedTask);
        await registry.RemoveAsync("cid");

        var ranAgain = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await registry.EnqueueAsync("cid", () => { ranAgain.TrySetResult(); return Task.CompletedTask; });

        await ranAgain.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await registry.RemoveAsync("cid");
    }

    [Fact]
    public async Task ConcurrentEnqueueAndRemove_NeverLosesWork()
    {
        // Reproduces the correlation-id-reuse race: a producer keeps enqueuing while a remover keeps
        // retiring the channel's executor. Every enqueued item must still run exactly once — under
        // the lock each enqueue targets a live executor, and retiring drains what was queued.
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        var executed = 0;
        const int total = 500;

        // The producer stands in for dispatch on behalf of a live waiter, so register the
        // subscription the way the channels do — otherwise the retirement tombstone would
        // (correctly) drop work for a channel nobody is subscribed to.
        registry.OnSubscriptionRegistered("cid");

        var producer = Task.Run(async () =>
        {
            for (var i = 0; i < total; i++)
                await registry.EnqueueAsync("cid", () => { Interlocked.Increment(ref executed); return Task.CompletedTask; });
        });

        var remover = Task.Run(async () =>
        {
            for (var i = 0; i < 60; i++)
            {
                await registry.RemoveAsync("cid");
                await Task.Yield();
            }
        });

        await Task.WhenAll(producer, remover);

        // Drain whatever executor is current after the interleaving settles.
        await registry.RemoveAsync("cid");

        Assert.Equal(total, executed);
    }

    [Fact]
    public async Task RemoveDuringBackpressure_DrainsBeforeReplacementWithoutOverlap()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        // A live registration models the waiter this backpressure race happens on behalf of, so
        // the enqueue racing the retirement is recreated rather than tombstone-dropped.
        registry.OnSubscriptionRegistered("cid");
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var order = new ConcurrentQueue<int>();
        var active = 0;
        var overlap = 0;

        async Task RecordAsync(int value, Task? gate = null)
        {
            if (Interlocked.Increment(ref active) > 1)
                Interlocked.Exchange(ref overlap, 1);
            if (value == 1)
                firstStarted.TrySetResult();
            if (gate is not null)
                await gate;
            order.Enqueue(value);
            Interlocked.Decrement(ref active);
        }

        await registry.EnqueueAsync("cid", () => RecordAsync(1, releaseFirst.Task));
        await firstStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Fill the default bounded queue while the first item is running, then leave one enqueue
        // waiting for capacity so retirement races the exact admission boundary.
        for (var index = 0; index < ChannelSerialExecutor.DefaultCapacity; index++)
            await registry.EnqueueAsync("cid", () => Task.CompletedTask);
        var beforeRetirement = registry.EnqueueAsync("cid", () => RecordAsync(2)).AsTask();
        Assert.False(beforeRetirement.IsCompleted);

        var retirement = registry.RemoveAsync("cid").AsTask();
        var afterRetirement = registry.EnqueueAsync("cid", () => RecordAsync(3)).AsTask();
        await Task.Delay(30);
        Assert.False(afterRetirement.IsCompleted);

        releaseFirst.TrySetResult();
        await Task.WhenAll(beforeRetirement, retirement, afterRetirement).WaitAsync(TimeSpan.FromSeconds(10));
        await registry.RemoveAsync("cid");

        Assert.Equal(0, overlap);
        Assert.Equal([1, 2, 3], order.ToArray());
    }

    [Fact]
    public async Task DifferentChannels_ExecuteInParallel_WhileEachChannelStaysSequential()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        const int channelCount = 4;
        const int perChannel = 6;

        // An async gate sized to the channel count: each channel's FIRST work item awaits it, and it
        // releases only once all channelCount first-items are running at the same time — which proves
        // distinct correlation ids execute in parallel. If the registry serialized across channels,
        // the gate would never reach its count and the await would time out (failing the test).
        var startedCount = 0;
        var allChannelsStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var perChannelConcurrent = new int[channelCount];
        var perChannelOverlap = new int[channelCount];
        var completionOrder = new ConcurrentQueue<int>[channelCount];
        for (var c = 0; c < channelCount; c++)
            completionOrder[c] = new ConcurrentQueue<int>();

        for (var c = 0; c < channelCount; c++)
        {
            var channel = c;
            for (var i = 0; i < perChannel; i++)
            {
                var index = i;
                await registry.EnqueueAsync($"cid-{channel}", async () =>
                {
                    // If two items for the SAME channel are ever active at once, record the violation.
                    if (Interlocked.Increment(ref perChannelConcurrent[channel]) > 1)
                        Interlocked.Exchange(ref perChannelOverlap[channel], 1);

                    if (index == 0)
                    {
                        if (Interlocked.Increment(ref startedCount) == channelCount)
                            allChannelsStarted.TrySetResult();
                        await allChannelsStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
                    }

                    await Task.Yield();
                    completionOrder[channel].Enqueue(index);
                    Interlocked.Decrement(ref perChannelConcurrent[channel]);
                });
            }
        }

        for (var c = 0; c < channelCount; c++)
            await registry.RemoveAsync($"cid-{c}");

        for (var c = 0; c < channelCount; c++)
        {
            Assert.Equal(0, perChannelOverlap[c]);  // never two at once within one correlation id
            Assert.Equal(Enumerable.Range(0, perChannel).ToArray(), completionOrder[c].ToArray()); // FIFO per cid
        }
    }

    [Fact]
    public async Task EnqueueAfterRetirement_WithoutRegistration_IsDroppedByTombstone()
    {
        // The leak fix: cleanup schedules RemoveAsync while a straggling enqueue is still in
        // flight. With no registration left for the channel, recreating an executor would leak it
        // (nothing retires it again) and the work would no-op anyway — so the tombstone drops it.
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        await registry.EnqueueAsync("cid", () => Task.CompletedTask);
        await registry.RemoveAsync("cid");

        var ran = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await registry.EnqueueAsync("cid", () => { ran.TrySetResult(); return Task.CompletedTask; });

        // RemoveAsync now completes immediately (no executor was recreated), and the work never ran.
        await registry.RemoveAsync("cid");
        Assert.False(ran.Task.IsCompleted);
    }

    [Fact]
    public async Task NewRegistration_LiftsTombstone_SoReusedChannelDeliversAgain()
    {
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        registry.OnSubscriptionRegistered("cid");
        await registry.EnqueueAsync("cid", () => Task.CompletedTask);
        registry.OnSubscriptionRetired("cid");
        await registry.RemoveAsync("cid"); // tombstoned: no registration remains

        // A new waiter reuses the correlation id: registration must lift the tombstone so its
        // deliveries run immediately instead of being dropped for the tombstone lifetime.
        registry.OnSubscriptionRegistered("cid");
        var ran = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await registry.EnqueueAsync("cid", () => { ran.TrySetResult(); return Task.CompletedTask; });

        await ran.Task.WaitAsync(TimeSpan.FromSeconds(5));
        registry.OnSubscriptionRetired("cid");
        await registry.RemoveAsync("cid");
    }

    [Fact]
    public async Task EnqueueDroppedByTombstone_LogsAWarning()
    {
        // The tombstone drop is deliberate (see EnqueueAfterRetirement_WithoutRegistration_
        // IsDroppedByTombstone), but it must not be silent: the warning is the only trace an
        // upstream ordering bug — a delivery arriving before the channel registered its
        // subscription — would leave behind.
        var logger = new CapturingLogger();
        var registry = new SerialExecutorRegistry(logger);
        await registry.EnqueueAsync("cid", () => Task.CompletedTask);
        await registry.RemoveAsync("cid");

        var ran = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await registry.EnqueueAsync("cid", () => { ran.TrySetResult(); return Task.CompletedTask; });

        Assert.False(ran.Task.IsCompleted);
        var warning = Assert.Single(logger.Entries, entry => entry.Level == LogLevel.Warning);
        Assert.Contains("cid", warning.Message);
    }

    [Fact]
    public async Task RemoveAsync_WithAHungWorkItem_StillRetiresTheChannel_SoLaterEnqueuesRun()
    {
        // A dispatched work item runs arbitrary user code — an Until predicate that never
        // completes. Retirement's dispose used to await the reader loop unbounded, so one hung
        // item left Retiring set forever and every later enqueue for the correlation id (including
        // a NEW waiter's) parked on the never-completed retirement. The bounded dispose abandons
        // the hung item, logs it, and still retires the entry.
        var logger = new CapturingLogger();
        var registry = new SerialExecutorRegistry(logger, disposeDrainLimit: TimeSpan.FromMilliseconds(200));
        registry.OnSubscriptionRegistered("cid");
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await registry.EnqueueAsync("cid", async () =>
        {
            started.TrySetResult();
            await release.Task;
        });
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await registry.RemoveAsync("cid").AsTask().WaitAsync(TimeSpan.FromSeconds(10));

        var warning = Assert.Single(logger.Entries, entry => entry.Level == LogLevel.Warning);
        Assert.Contains("cid", warning.Message);

        // The channel key is usable again: a re-registered waiter's delivery must run, not park.
        var ranAgain = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await registry.EnqueueAsync("cid", () => { ranAgain.TrySetResult(); return Task.CompletedTask; })
            .AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        await ranAgain.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Unwedge the abandoned item; its loop drains against the already-completed writer and exits.
        release.TrySetResult();
        registry.OnSubscriptionRetired("cid");
        await registry.RemoveAsync("cid");
    }

    /// <summary>Captures log entries so a test can assert a drop was reported rather than silent.</summary>
    private sealed class CapturingLogger : ILogger
    {
        private readonly List<(LogLevel Level, string Message)> _entries = [];

        public IReadOnlyList<(LogLevel Level, string Message)> Entries
        {
            get { lock (_entries) return _entries.ToArray(); }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_entries)
                _entries.Add((logLevel, formatter(state, exception)));
        }
    }

    [Fact]
    public async Task Retirement_WithAnotherWaiterStillRegistered_DoesNotDropItsWork()
    {
        // Fan-out: two waiters share one correlation id. The first waiter's cleanup retires the
        // shared executor; the second waiter's deliveries must keep flowing (the tombstone only
        // applies once no registration remains).
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        registry.OnSubscriptionRegistered("cid");
        registry.OnSubscriptionRegistered("cid");
        await registry.EnqueueAsync("cid", () => Task.CompletedTask);

        registry.OnSubscriptionRetired("cid");
        await registry.RemoveAsync("cid"); // first waiter's cleanup

        var ran = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await registry.EnqueueAsync("cid", () => { ran.TrySetResult(); return Task.CompletedTask; });

        await ran.Task.WaitAsync(TimeSpan.FromSeconds(5));
        registry.OnSubscriptionRetired("cid");
        await registry.RemoveAsync("cid");
    }

    [Fact]
    public async Task RetireIfUnreferenced_WithASiblingStillRegistered_LeavesTheSharedExecutorAdmitting()
    {
        // Fan-out: the first waiter's cleanup must not retire the executor its sibling still
        // uses. Pre-fix the per-waiter cleanup retired it unconditionally, and while that
        // retirement drained the sibling's in-flight item every non-blocking TryEnqueue for the
        // sibling read "Full" — which the Redis channel answers by faulting the wait as overloaded.
        var registry = new SerialExecutorRegistry(NullLogger.Instance);
        registry.OnSubscriptionRegistered("cid");
        registry.OnSubscriptionRegistered("cid");
        var siblingStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSibling = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Accepted, registry.TryEnqueue("cid", async () =>
        {
            siblingStarted.TrySetResult();
            await releaseSibling.Task;
        }));
        await siblingStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        registry.OnSubscriptionRetired("cid");
        var firstCleanup = registry.RetireIfUnreferencedAsync("cid").AsTask();

        // Nothing to wait for: the sibling keeps the executor, so no retirement (and no drain) began.
        Assert.True(firstCleanup.IsCompletedSuccessfully);
        var ran = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Accepted, registry.TryEnqueue("cid", () =>
        {
            ran.TrySetResult();
            return Task.CompletedTask;
        }));

        releaseSibling.TrySetResult();
        await ran.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // The last cleanup out retires it, which lays the tombstone for stragglers.
        registry.OnSubscriptionRetired("cid");
        await registry.RetireIfUnreferencedAsync("cid").AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Suppressed, registry.TryEnqueue("cid", () => Task.CompletedTask));
    }

    [Fact]
    public async Task Registration_DuringADepartedWaitersRetirement_GetsASuccessorExecutor()
    {
        // A re-attached waiter registering on the id a departed waiter's cleanup is still
        // draining. Pre-fix the retiring entry stayed in the map for the whole drain — up to the
        // dispose budget behind a wedged predicate — and every non-blocking TryEnqueue for the new
        // waiter read "Full": the Redis channel faulted its wait as overloaded, and the DB
        // channels routed its response to recovery. Registering now detaches the retiring
        // executor, so the new waiter's first delivery creates a successor and runs at once.
        // On a virtual clock nobody advances: the departed drain's 30 s budget can never lapse in
        // real time and flip the "still draining" assertions below on a stalled runner.
        var registry = new SerialExecutorRegistry(NullLogger.Instance, timeProvider: new AsyncResponse.Testing.VirtualTimeProvider());
        registry.OnSubscriptionRegistered("cid");
        var departedStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseDeparted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Accepted, registry.TryEnqueue("cid", async () =>
        {
            departedStarted.TrySetResult();
            await releaseDeparted.Task;
        }));
        await departedStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        registry.OnSubscriptionRetired("cid");
        var departedRetirement = registry.RetireIfUnreferencedAsync("cid").AsTask();
        Assert.False(departedRetirement.IsCompleted); // draining the wedged item

        registry.OnSubscriptionRegistered("cid");
        try
        {
            var ran = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Accepted, registry.TryEnqueue("cid", () =>
            {
                ran.TrySetResult();
                return Task.CompletedTask;
            }));
            await ran.Task.WaitAsync(TimeSpan.FromSeconds(5));

            // The blocking path (a waiter's drain marker) no longer waits out the departed drain either.
            var markerRan = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Assert.True(await registry.EnqueueAsync("cid", () =>
            {
                markerRan.TrySetResult();
                return Task.CompletedTask;
            }).AsTask().WaitAsync(TimeSpan.FromSeconds(5)));
            await markerRan.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(departedRetirement.IsCompleted);
        }
        finally
        {
            releaseDeparted.TrySetResult();
        }

        await departedRetirement.WaitAsync(TimeSpan.FromSeconds(5));

        // The departed retirement removed only its own entry: the successor still admits for the
        // live waiter, and the waiter's own cleanup retires it and lays the tombstone.
        var ranAfter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Accepted, registry.TryEnqueue("cid", () =>
        {
            ranAfter.TrySetResult();
            return Task.CompletedTask;
        }));
        await ranAfter.Task.WaitAsync(TimeSpan.FromSeconds(5));
        registry.OnSubscriptionRetired("cid");
        await registry.RetireIfUnreferencedAsync("cid").AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Suppressed, registry.TryEnqueue("cid", () => Task.CompletedTask));
    }

    [Fact]
    public async Task EnqueueDrainTimeout_UnderAThrowingLoggingProvider_StillDisposesTheExecutor()
    {
        // Retirement's bounded wait for a parked producer lapses and logs a warning. Pre-fix a
        // provider that throws there (MEL rethrows provider failures) escaped before the disposal:
        // the retirement faulted, and the executor's writer was never completed — its reader loop
        // (and the producer parked on its full queue) outlived the retirement forever.
        var time = new AsyncResponse.Testing.VirtualTimeProvider();
        var logger = new CollectingLogger { ThrowOnMessageContaining = "Timed out after" };
        var registry = new SerialExecutorRegistry(
            logger, disposeDrainLimit: TimeSpan.FromSeconds(1), enqueueDrainLimit: TimeSpan.FromSeconds(1), timeProvider: time);
        registry.OnSubscriptionRegistered("cid");
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Assert.True(await registry.EnqueueAsync("cid", async () =>
        {
            started.TrySetResult();
            await release.Task;
        }));
        await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
        for (var i = 0; i < ChannelSerialExecutor.DefaultCapacity; i++)
            Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Accepted, registry.TryEnqueue("cid", () => Task.CompletedTask));

        var markerRan = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var parked = registry.EnqueueAsync("cid", () =>
        {
            markerRan.TrySetResult();
            return Task.CompletedTask;
        }).AsTask();
        Assert.False(parked.IsCompleted); // the queue is full behind the wedged item

        var removal = registry.RemoveAsync("cid").AsTask();
        Assert.False(removal.IsCompleted);
        time.Advance(TimeSpan.FromSeconds(1)); // the in-flight-enqueue wait lapses and logs
        release.TrySetResult();

        await removal.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(await parked.WaitAsync(TimeSpan.FromSeconds(10)));
        await markerRan.Task.WaitAsync(TimeSpan.FromSeconds(10));
        registry.OnSubscriptionRetired("cid");
        await registry.RemoveAsync("cid").AsTask().WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Tombstone_OutlivesAForwardWallClockStep()
    {
        // A tombstone bounds how long an in-flight enqueue may still arrive — elapsed time. Pre-fix
        // its deadline was a wall-clock instant, so a forward clock step (NTP, a resumed VM) expired
        // it early and the straggler it exists to drop recreated an executor nothing would retire.
        var clock = new SteppedWallClock();
        var registry = new SerialExecutorRegistry(NullLogger.Instance, timeProvider: clock);
        registry.OnSubscriptionRegistered("cid");
        Assert.True(await registry.EnqueueAsync("cid", () => Task.CompletedTask));
        registry.OnSubscriptionRetired("cid");
        await registry.RemoveAsync("cid");

        clock.Wall += TimeSpan.FromHours(1);

        // No monotonic time elapsed: the straggler is still suppressed.
        Assert.False(await registry.EnqueueAsync("cid", () => Task.CompletedTask));
    }

    /// <summary>A clock whose wall time can jump while its monotonic timestamp stands still.</summary>
    private sealed class SteppedWallClock : TimeProvider
    {
        private readonly long _timestamp = global::System.Diagnostics.Stopwatch.GetTimestamp();

        public DateTimeOffset Wall { get; set; } = DateTimeOffset.UtcNow;

        public override DateTimeOffset GetUtcNow() => Wall;

        public override long GetTimestamp() => _timestamp;
    }
}
