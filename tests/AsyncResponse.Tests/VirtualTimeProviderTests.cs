using AsyncResponse.Testing;
using Xunit;

namespace AsyncResponse.Tests;

public class VirtualTimeProviderTests
{
    [Fact]
    public void StartsAtTheFixedDefaultEpoch_AndNeverMovesOnItsOwn()
    {
        var clock = new VirtualTimeProvider();
        Assert.Equal(VirtualTimeProvider.DefaultStartTime, clock.GetUtcNow());
        Assert.Equal(VirtualTimeProvider.DefaultStartTime, clock.GetUtcNow());
        Assert.Equal(TimeZoneInfo.Utc, clock.LocalTimeZone);
    }

    [Fact]
    public void Advance_MovesTimeExactly_AndFiresDueTimersInOrder()
    {
        var clock = new VirtualTimeProvider();
        var fired = new List<string>();
        using var late = clock.CreateTimer(_ => fired.Add("late"), null, TimeSpan.FromSeconds(30), Timeout.InfiniteTimeSpan);
        using var early = clock.CreateTimer(_ => fired.Add("early"), null, TimeSpan.FromSeconds(10), Timeout.InfiniteTimeSpan);
        using var never = clock.CreateTimer(_ => fired.Add("never"), null, TimeSpan.FromMinutes(5), Timeout.InfiniteTimeSpan);

        clock.Advance(TimeSpan.FromMinutes(1));

        Assert.Equal(["early", "late"], fired);
        Assert.Equal(VirtualTimeProvider.DefaultStartTime + TimeSpan.FromMinutes(1), clock.GetUtcNow());
    }

    [Fact]
    public void SameDueInstant_FiresInCreationOrder()
    {
        var clock = new VirtualTimeProvider();
        var fired = new List<int>();
        using var first = clock.CreateTimer(_ => fired.Add(1), null, TimeSpan.FromSeconds(5), Timeout.InfiniteTimeSpan);
        using var second = clock.CreateTimer(_ => fired.Add(2), null, TimeSpan.FromSeconds(5), Timeout.InfiniteTimeSpan);

        clock.Advance(TimeSpan.FromSeconds(5));

        Assert.Equal([1, 2], fired);
    }

    [Fact]
    public void CallbackObservesTheFireInstant_NotTheAdvanceTarget()
    {
        var clock = new VirtualTimeProvider();
        DateTimeOffset? observed = null;
        using var timer = clock.CreateTimer(_ => observed = clock.GetUtcNow(), null, TimeSpan.FromSeconds(10), Timeout.InfiniteTimeSpan);

        clock.Advance(TimeSpan.FromMinutes(10));

        Assert.Equal(VirtualTimeProvider.DefaultStartTime + TimeSpan.FromSeconds(10), observed);
    }

    [Fact]
    public void PeriodicTimer_FiresOncePerPeriodWithinOneAdvance()
    {
        var clock = new VirtualTimeProvider();
        var fires = 0;
        using var timer = clock.CreateTimer(_ => fires++, null, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10));

        clock.Advance(TimeSpan.FromSeconds(35));

        Assert.Equal(3, fires); // 10s, 20s, 30s
    }

    [Fact]
    public void TimersArmedByAFiringCallback_FireWithinTheSameAdvance()
    {
        var clock = new VirtualTimeProvider();
        var fired = new List<string>();
        using var chainStart = clock.CreateTimer(
            _ =>
            {
                fired.Add("first");
                // Chained one-shot due 5s after the FIRST timer's fire instant (t=10s → t=15s).
                _ = clock.CreateTimer(_ => fired.Add("chained"), null, TimeSpan.FromSeconds(5), Timeout.InfiniteTimeSpan);
            },
            null,
            TimeSpan.FromSeconds(10),
            Timeout.InfiniteTimeSpan);

        clock.Advance(TimeSpan.FromSeconds(20));

        Assert.Equal(["first", "chained"], fired);
    }

    [Fact]
    public void Change_ReschedulesAndInfiniteDisarms()
    {
        var clock = new VirtualTimeProvider();
        var fires = 0;
        var timer = clock.CreateTimer(_ => fires++, null, TimeSpan.FromSeconds(10), Timeout.InfiniteTimeSpan);

        timer.Change(TimeSpan.FromSeconds(30), Timeout.InfiniteTimeSpan);
        clock.Advance(TimeSpan.FromSeconds(20));
        Assert.Equal(0, fires);

        clock.Advance(TimeSpan.FromSeconds(10));
        Assert.Equal(1, fires);

        timer.Change(TimeSpan.FromSeconds(5), Timeout.InfiniteTimeSpan);
        timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        clock.Advance(TimeSpan.FromMinutes(1));
        Assert.Equal(1, fires);

        timer.Dispose();
        Assert.False(timer.Change(TimeSpan.FromSeconds(1), Timeout.InfiniteTimeSpan));
    }

    [Fact]
    public void DisposedTimer_NeverFires()
    {
        var clock = new VirtualTimeProvider();
        var fires = 0;
        var timer = clock.CreateTimer(_ => fires++, null, TimeSpan.FromSeconds(10), Timeout.InfiniteTimeSpan);
        timer.Dispose();

        clock.Advance(TimeSpan.FromMinutes(1));

        Assert.Equal(0, fires);
    }

    [Fact]
    public void AdvancingBackwards_Throws()
    {
        var clock = new VirtualTimeProvider();
        Assert.Throws<ArgumentOutOfRangeException>(() => clock.Advance(TimeSpan.FromSeconds(-1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => clock.AdvanceTo(clock.GetUtcNow() - TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public async Task TaskDelay_OnTheVirtualClock_CompletesOnAdvance()
    {
        var clock = new VirtualTimeProvider();
        var delay = Task.Delay(TimeSpan.FromDays(3), clock);
        Assert.False(delay.IsCompleted);

        clock.Advance(TimeSpan.FromDays(3));
        await delay.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task WaitAsync_OnTheVirtualClock_TimesOutOnAdvance()
    {
        var clock = new VirtualTimeProvider();
        var never = new TaskCompletionSource();
        var wait = never.Task.WaitAsync(TimeSpan.FromMinutes(5), clock);

        clock.Advance(TimeSpan.FromMinutes(5));

        await Assert.ThrowsAsync<TimeoutException>(() => wait.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void CancellationTokenSource_OnTheVirtualClock_CancelsOnAdvance()
    {
        var clock = new VirtualTimeProvider();
        using var cts = new CancellationTokenSource(TimeSpan.FromHours(12), clock);
        Assert.False(cts.IsCancellationRequested);

        clock.Advance(TimeSpan.FromHours(12));

        Assert.True(cts.IsCancellationRequested);
    }

    [Fact]
    public void NextTimerDueAt_PeeksTheEarliestArmedTimer()
    {
        var clock = new VirtualTimeProvider();
        Assert.Null(clock.NextTimerDueAt);

        using var timer = clock.CreateTimer(_ => { }, null, TimeSpan.FromSeconds(42), Timeout.InfiniteTimeSpan);
        Assert.Equal(VirtualTimeProvider.DefaultStartTime + TimeSpan.FromSeconds(42), clock.NextTimerDueAt);
    }
}
