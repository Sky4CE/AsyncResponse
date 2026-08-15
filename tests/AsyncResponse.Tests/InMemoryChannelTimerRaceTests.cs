using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// ArmTimeout and StartCleanupAsync form a Dekker pair over (_timeoutTimer, _cleanupStarted):
/// each side stores its own field and then reads the other's, so each store needs a full fence
/// (Interlocked.Exchange) — release-store/acquire-load does not order StoreLoad, and a mutual
/// miss would leave the timeout timer armed in the timer queue, rooting the subscription graph
/// until it fires. This race pins that a timer armed while a publish races waiter creation is
/// always disposed once both sides settle.
/// </summary>
public sealed class InMemoryChannelTimerRaceTests
{
    [Fact]
    public async Task ArmTimeoutRacingCleanup_AlwaysDisposesTheTimer()
    {
        var time = new CountingTimeProvider();
        var provider = new ServiceCollection().BuildServiceProvider();
        var channel = new InMemoryAsyncResponseChannel(
            provider.GetRequiredService<IServiceScopeFactory>(),
            new InMemoryRecoveryStateStore(),
            Options.Create(new InMemoryAsyncResponseOptions
            {
                DefaultTimeout = TimeSpan.FromMinutes(5),
                RecoveryStateExpiry = TimeSpan.FromMinutes(5)
            }),
            new AsyncResponseContextPropagation([]),
            NullLogger<InMemoryAsyncResponseChannel>.Instance,
            time);

        for (var i = 0; i < 500; i++)
        {
            var correlationId = $"timer-race-{i}";
            var create = channel.CreateResponseWaiter<OperationResult>(correlationId);
            var publish = Task.Run(() => channel.SetResponse(
                new OperationResult { Status = OperationStatus.Completed },
                correlationId));

            var waiter = await create;
            await publish;
            // Settles the wait (if the publish lost the race entirely) and runs cleanup to
            // completion, so both halves of the pair have finished when the loop advances.
            await waiter.DisposeAsync();
        }

        Assert.All(time.Timers, timer => Assert.True(timer.Disposed, "an armed timeout timer was never disposed"));
    }

    private sealed class CountingTimeProvider : TimeProvider
    {
        public ConcurrentBag<CountingTimer> Timers { get; } = [];

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new CountingTimer(base.CreateTimer(callback, state, dueTime, period));
            Timers.Add(timer);
            return timer;
        }
    }

    private sealed class CountingTimer(ITimer _inner) : ITimer
    {
        private int _disposed;

        public bool Disposed => Volatile.Read(ref _disposed) != 0;

        public bool Change(TimeSpan dueTime, TimeSpan period) => _inner.Change(dueTime, period);

        public void Dispose()
        {
            Interlocked.Exchange(ref _disposed, 1);
            _inner.Dispose();
        }

        public async ValueTask DisposeAsync()
        {
            Interlocked.Exchange(ref _disposed, 1);
            await _inner.DisposeAsync();
        }
    }
}
