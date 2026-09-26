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
    public void ArmSequence_AdvancesWhenATimerIsArmed_NeverWhenOneIsDisposed()
    {
        // Regression (fixpoint r1): the harness's settle watched NextTimerDueAt for "a job just
        // began a virtual-time wait", but disposing the EARLIEST timer changes that too — the
        // settle then ended while the job that disposed it was still running code, and the clock
        // advanced under it. The arm sequence moves only when a timer is armed.
        var clock = new VirtualTimeProvider();
        var start = clock.ArmSequence;

        var earliest = clock.CreateTimer(_ => { }, null, TimeSpan.FromSeconds(1), Timeout.InfiniteTimeSpan);
        using var later = clock.CreateTimer(_ => { }, null, TimeSpan.FromMinutes(1), Timeout.InfiniteTimeSpan);
        var armed = clock.ArmSequence;
        Assert.Equal(start + 2, armed);

        // Disposing the earliest timer moves NextTimerDueAt, but arms nothing.
        var nextBefore = clock.NextTimerDueAt;
        earliest.Dispose();
        Assert.NotEqual(nextBefore, clock.NextTimerDueAt);
        Assert.Equal(armed, clock.ArmSequence);

        // Disarming (an infinite due time) arms nothing either; re-arming does — even to a due
        // time LATER than the earliest, which NextTimerDueAt cannot show.
        later.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        Assert.Equal(armed, clock.ArmSequence);
        using var earliestAgain = clock.CreateTimer(_ => { }, null, TimeSpan.FromSeconds(1), Timeout.InfiniteTimeSpan);
        var dueBefore = clock.NextTimerDueAt;
        later.Change(TimeSpan.FromMinutes(5), Timeout.InfiniteTimeSpan);
        Assert.Equal(dueBefore, clock.NextTimerDueAt);
        Assert.Equal(armed + 2, clock.ArmSequence);
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

    // ---------------------------------------------------------------------------------------
    // Re-entrant and concurrent advances. The advance loop re-validated its target against the
    // clock on EVERY iteration, so anything else that moved time past it mid-advance — the firing
    // callback's own Advance, or a second thread's — surfaced as "Cannot advance virtual time
    // backwards" out of an advance that never asked for that. Two threads also fired callbacks
    // side by side (a later instant's callback running while an earlier one was still in
    // progress, reading a clock already past its own fire instant), and two relative advances
    // that read the same "now" landed as one.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void ReentrantAdvance_FromACallback_NestsInline_AndAnOvertakenOuterAdvanceSimplyCompletes()
    {
        var clock = new VirtualTimeProvider();
        var start = clock.GetUtcNow();
        var fired = new List<string>();
        DateTimeOffset? afterNested = null;

        using var reentrant = clock.CreateTimer(
            _ =>
            {
                fired.Add($"reentrant@{(clock.GetUtcNow() - start).TotalSeconds}");
                clock.Advance(TimeSpan.FromSeconds(20)); // 5s → 25s: past the outer target (10s)
                afterNested = clock.GetUtcNow();
            },
            null,
            TimeSpan.FromSeconds(5),
            Timeout.InfiniteTimeSpan);
        using var inside = clock.CreateTimer(_ => fired.Add($"inside@{(clock.GetUtcNow() - start).TotalSeconds}"), null, TimeSpan.FromSeconds(8), Timeout.InfiniteTimeSpan);
        using var beyond = clock.CreateTimer(_ => fired.Add("beyond"), null, TimeSpan.FromSeconds(30), Timeout.InfiniteTimeSpan);

        clock.Advance(TimeSpan.FromSeconds(10));

        // The nested advance ran to completion inside the callback, firing what fell due in order
        // at its own instant; the outer advance found its target already behind the clock and
        // finished — time never rewinds, and nothing threw.
        Assert.Equal(["reentrant@5", "inside@8"], fired);
        Assert.Equal(start + TimeSpan.FromSeconds(25), afterNested);
        Assert.Equal(start + TimeSpan.FromSeconds(25), clock.GetUtcNow());
    }

    [Fact]
    public void ReentrantAdvance_WithinTheOuterTarget_LetsTheOuterAdvanceFinishItsOwnTarget()
    {
        var clock = new VirtualTimeProvider();
        var start = clock.GetUtcNow();
        var fired = new List<string>();

        using var reentrant = clock.CreateTimer(
            _ =>
            {
                fired.Add("reentrant");
                clock.Advance(TimeSpan.FromSeconds(2)); // 5s → 7s, fires "nested" on the way
            },
            null,
            TimeSpan.FromSeconds(5),
            Timeout.InfiniteTimeSpan);
        using var nested = clock.CreateTimer(_ => fired.Add("nested"), null, TimeSpan.FromSeconds(6), Timeout.InfiniteTimeSpan);
        using var outer = clock.CreateTimer(_ => fired.Add("outer"), null, TimeSpan.FromSeconds(9), Timeout.InfiniteTimeSpan);

        clock.Advance(TimeSpan.FromSeconds(10));

        Assert.Equal(["reentrant", "nested", "outer"], fired);
        Assert.Equal(start + TimeSpan.FromSeconds(10), clock.GetUtcNow());
    }

    [Fact]
    public void ConcurrentAdvance_WaitsForTheRunningOne_SoCallbacksNeverOverlapOrSeeALaterClock()
    {
        var clock = new VirtualTimeProvider();
        var start = clock.GetUtcNow();
        var firstEntered = new ManualResetEventSlim();
        var secondThreadStarted = new ManualResetEventSlim();
        var laterFired = new ManualResetEventSlim();
        var firstInProgress = 0;
        var overlapped = false;
        DateTimeOffset? firstSawOnExit = null;

        using var first = clock.CreateTimer(
            _ =>
            {
                Volatile.Write(ref firstInProgress, 1);
                firstEntered.Set();
                // Hold the callback open long enough for the second thread's advance to get going.
                // Pre-fix it ran right through this callback and fired the later timer (ending
                // the wait early); serialized, it cannot, and the wait simply lapses.
                secondThreadStarted.Wait(TimeSpan.FromSeconds(10));
                laterFired.Wait(TimeSpan.FromMilliseconds(250));
                firstSawOnExit = clock.GetUtcNow();
                Volatile.Write(ref firstInProgress, 0);
            },
            null,
            TimeSpan.FromSeconds(5),
            Timeout.InfiniteTimeSpan);
        using var later = clock.CreateTimer(
            _ =>
            {
                if (Volatile.Read(ref firstInProgress) == 1)
                    overlapped = true;
                laterFired.Set();
            },
            null,
            TimeSpan.FromSeconds(7),
            Timeout.InfiniteTimeSpan);

        Exception? secondFailure = null;
        var second = new Thread(() =>
        {
            firstEntered.Wait(TimeSpan.FromSeconds(10));
            secondThreadStarted.Set();
            try
            {
                clock.Advance(TimeSpan.FromSeconds(10));
            }
            catch (Exception ex)
            {
                secondFailure = ex;
            }
        });
        second.Start();

        clock.Advance(TimeSpan.FromSeconds(10)); // pre-fix: threw "backwards" once the second thread had overtaken it
        Assert.True(second.Join(TimeSpan.FromSeconds(30)));

        Assert.Null(secondFailure);
        Assert.False(overlapped, "the 7s timer fired on a second thread while the 5s timer's callback was still running");
        Assert.Equal(start + TimeSpan.FromSeconds(5), firstSawOnExit);
        // Both relative advances landed, one after the other.
        Assert.Equal(start + TimeSpan.FromSeconds(20), clock.GetUtcNow());
    }

    [Fact]
    public void ConcurrentRelativeAdvances_AllLand()
    {
        var clock = new VirtualTimeProvider();
        var start = clock.GetUtcNow();
        const int threads = 8;
        const int advancesPerThread = 2_000;
        using var barrier = new Barrier(threads);
        var failures = new List<Exception>();

        var workers = Enumerable.Range(0, threads).Select(_ => new Thread(() =>
        {
            try
            {
                barrier.SignalAndWait(TimeSpan.FromSeconds(30));
                for (var i = 0; i < advancesPerThread; i++)
                    clock.Advance(TimeSpan.FromSeconds(1));
            }
            catch (Exception ex)
            {
                lock (failures)
                    failures.Add(ex);
            }
        })).ToArray();

        foreach (var worker in workers)
            worker.Start();
        foreach (var worker in workers)
            Assert.True(worker.Join(TimeSpan.FromSeconds(60)));

        // Pre-fix the target was computed from a "now" read before the advance began: two threads
        // reading the same instant collapsed into one step (or one of them threw "backwards").
        Assert.Empty(failures);
        Assert.Equal(start + TimeSpan.FromSeconds(threads * advancesPerThread), clock.GetUtcNow());
    }

    [Fact]
    public void AdvanceToAtLeast_ATargetAnotherDriverAlreadyPassed_NeverMovesBackwards_ButFiresWhatIsDueNow()
    {
        // Pre-commit review (fixpoint r2, C4): the harness has two clock drivers — a pending
        // publish driving its own backoffs, and the test's AdvanceAsync. Each read "now" outside
        // the advance gate and then called AdvanceTo with a target computed from it; when the
        // other driver moved past that target in between, AdvanceTo threw "Cannot advance virtual
        // time backwards" and faulted the publish or the test's advance.
        var clock = new VirtualTimeProvider();
        var start = clock.GetUtcNow();
        var staleTarget = start + TimeSpan.FromSeconds(1); // read before the other driver moved

        clock.Advance(TimeSpan.FromSeconds(10)); // the other driver
        var fired = 0;
        using var dueNow = clock.CreateTimer(_ => fired++, null, TimeSpan.Zero, Timeout.InfiniteTimeSpan);

        clock.AdvanceToAtLeast(staleTarget);

        Assert.Equal(start + TimeSpan.FromSeconds(10), clock.GetUtcNow());
        Assert.Equal(1, fired); // a timer due at the current instant still fires, as AdvanceTo(now) fired it

        clock.AdvanceToAtLeast(start + TimeSpan.FromSeconds(12));
        Assert.Equal(start + TimeSpan.FromSeconds(12), clock.GetUtcNow());
    }

    // ---------------------------------------------------------------------------------------
    // Argument parity with the real timer. Code that arms a timer the BCL rejects (beyond the
    // ~49.7-day ceiling, a negative period) used to pass on the virtual clock and throw
    // ArgumentOutOfRangeException in production. The expectation is PROBED from
    // TimeProvider.System at run time — exception type and parameter name — not hard-coded.
    // ---------------------------------------------------------------------------------------

    private static readonly long _ceilingTicks = TimeSpan.FromMilliseconds(uint.MaxValue - 1).Ticks;
    private static readonly long _infiniteTicks = Timeout.InfiniteTimeSpan.Ticks;

    public static TheoryData<string, long, long> TimerArguments => new()
    {
        { "zero due, no period", 0, _infiniteTicks },
        { "never", _infiniteTicks, _infiniteTicks },
        { "due at the ceiling", _ceilingTicks, _infiniteTicks },
        { "due one ms past the ceiling", _ceilingTicks + TimeSpan.TicksPerMillisecond, _infiniteTicks },
        { "due 50 days", TimeSpan.FromDays(50).Ticks, _infiniteTicks },
        { "due TimeSpan.MaxValue", TimeSpan.MaxValue.Ticks, _infiniteTicks },
        { "due -2 ms", -2 * TimeSpan.TicksPerMillisecond, _infiniteTicks },
        { "due -1 tick (truncates to 0 ms)", -1, _infiniteTicks },
        { "due -1.5 ms (truncates to -1 ms)", -15_000, _infiniteTicks },
        { "due TimeSpan.MinValue", TimeSpan.MinValue.Ticks, _infiniteTicks },
        { "period zero", TimeSpan.TicksPerSecond, 0 },
        { "period at the ceiling", TimeSpan.TicksPerSecond, _ceilingTicks },
        { "period one ms past the ceiling", TimeSpan.TicksPerSecond, _ceilingTicks + TimeSpan.TicksPerMillisecond },
        { "period 50 days", TimeSpan.TicksPerSecond, TimeSpan.FromDays(50).Ticks },
        { "period -2 ms", TimeSpan.TicksPerSecond, -2 * TimeSpan.TicksPerMillisecond },
        { "period -1 tick (truncates to 0 ms)", TimeSpan.TicksPerSecond, -1 },
        { "both out of range", TimeSpan.FromDays(50).Ticks, TimeSpan.FromDays(50).Ticks },
    };

    [Theory]
    [MemberData(nameof(TimerArguments))]
    public void CreateTimer_AcceptsAndRejects_ExactlyWhatTheSystemTimerDoes(string label, long dueTicks, long periodTicks)
    {
        var due = TimeSpan.FromTicks(dueTicks);
        var period = TimeSpan.FromTicks(periodTicks);

        var expected = Outcome(() => TimeProvider.System.CreateTimer(_ => { }, null, due, period).Dispose());
        var actual = Outcome(() => new VirtualTimeProvider().CreateTimer(_ => { }, null, due, period).Dispose());

        Assert.True(expected == actual, $"{label}: TimeProvider.System → {expected}; VirtualTimeProvider → {actual}");
    }

    [Theory]
    [MemberData(nameof(TimerArguments))]
    public void Change_AcceptsAndRejects_ExactlyWhatTheSystemTimerDoes(string label, long dueTicks, long periodTicks)
    {
        var due = TimeSpan.FromTicks(dueTicks);
        var period = TimeSpan.FromTicks(periodTicks);
        using var real = TimeProvider.System.CreateTimer(_ => { }, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        using var virtualTimer = new VirtualTimeProvider().CreateTimer(_ => { }, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

        var expected = Outcome(() => real.Change(due, period));
        var actual = Outcome(() => virtualTimer.Change(due, period));

        Assert.True(expected == actual, $"{label}: TimeProvider.System → {expected}; VirtualTimeProvider → {actual}");
    }

    [Fact]
    public void CreateTimer_NullCallback_ThrowsLikeTheSystemTimer()
    {
        var expected = Outcome(() => TimeProvider.System.CreateTimer(null!, null, TimeSpan.Zero, Timeout.InfiniteTimeSpan).Dispose());
        var actual = Outcome(() => new VirtualTimeProvider().CreateTimer(null!, null, TimeSpan.Zero, Timeout.InfiniteTimeSpan).Dispose());

        Assert.Equal(expected, actual);
        Assert.StartsWith(nameof(ArgumentNullException), actual, StringComparison.Ordinal);
    }

    [Fact]
    public void ArgumentsTheSystemTimerTruncatesToWholeMilliseconds_MeanTheSameThingHere()
    {
        var clock = new VirtualTimeProvider();
        var fired = new List<string>();

        // (long)TotalMilliseconds, as the BCL computes it: -0.0001 ms is 0 (fire now), -1.5 ms is
        // -1 (never), and a 0.5 ms period is 0 (one-shot).
        using var now = clock.CreateTimer(_ => fired.Add("due -1 tick"), null, TimeSpan.FromTicks(-1), Timeout.InfiniteTimeSpan);
        using var never = clock.CreateTimer(_ => fired.Add("due -1.5 ms"), null, TimeSpan.FromTicks(-15_000), Timeout.InfiniteTimeSpan);
        using var oneShot = clock.CreateTimer(_ => fired.Add("period 0.5 ms"), null, TimeSpan.FromSeconds(1), TimeSpan.FromTicks(5_000));

        Assert.Equal(clock.GetUtcNow(), clock.NextTimerDueAt); // never before "now"
        clock.Advance(TimeSpan.FromSeconds(3));

        Assert.Equal(["due -1 tick", "period 0.5 ms"], fired);
    }

    private static string Outcome(Action act)
    {
        try
        {
            act();
            return "accepted";
        }
        catch (ArgumentException ex)
        {
            return $"{ex.GetType().Name}({ex.ParamName})";
        }
    }
}
