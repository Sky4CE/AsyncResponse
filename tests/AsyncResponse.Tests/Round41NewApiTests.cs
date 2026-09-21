using System.Diagnostics;
using Xunit;
using static AsyncResponse.Tests.Round40FlowLeaseTestSupport;

namespace AsyncResponse.Tests;

/// <summary>
/// Review of 2026-09-21 — pins for the API that round added:
/// <see cref="DurableFlowOptions.MaxLeaseContentionWait"/>,
/// <see cref="PortableText.TruncateWellFormed"/> and
/// <see cref="ScheduledFlowService.RecentOccurrences"/>. The behavioural proofs against the old
/// code are in <see cref="Round41RegressionTests"/>.
/// </summary>
public sealed class Round41NewApiTests
{
    private static DurableFlowOptions ShortLease(TimeSpan maxContentionWait) => new()
    {
        ExecutionLeaseDuration = TimeSpan.FromMilliseconds(120),
        ExecutionLeaseRenewInterval = TimeSpan.FromMilliseconds(30),
        MaxLeaseContentionWait = maxContentionWait
    };

    // ---------- F1: DurableFlowOptions.MaxLeaseContentionWait ----------

    [Fact]
    public void MaxLeaseContentionWait_DefaultsToOneHour_AndMustBePositive()
    {
        Assert.Equal(TimeSpan.FromHours(1), new DurableFlowOptions().MaxLeaseContentionWait);

        FlowStateConcurrency.ValidateOptions(new DurableFlowOptions { MaxLeaseContentionWait = TimeSpan.FromTicks(1) });
        // A persistence-sized value is fine: it is compared with elapsed time, it never arms a timer.
        FlowStateConcurrency.ValidateOptions(new DurableFlowOptions { MaxLeaseContentionWait = TimeSpan.FromDays(90) });

        foreach (var invalid in new[] { TimeSpan.Zero, TimeSpan.FromSeconds(-1) })
        {
            var rejection = Assert.Throws<InvalidOperationException>(
                () => FlowStateConcurrency.ValidateOptions(new DurableFlowOptions { MaxLeaseContentionWait = invalid }));
            Assert.Contains(nameof(DurableFlowOptions.MaxLeaseContentionWait), rejection.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task PersistedExpiryBeyondTheBudget_FailsAtTheBudget_AndSaysWhichKnobAndWhy()
    {
        var persistedExpiry = DateTime.UtcNow.AddDays(3);
        var store = await Round41RegressionTests.UnacquirableStore.CreateAsync(
            "r41-budget", new FlowLeaseObservation("skewed-owner", persistedExpiry));
        await using var harness = CreateHarness(store, ShortLease(TimeSpan.FromMilliseconds(600)));

        var waited = Stopwatch.StartNew();
        var contended = await Assert.ThrowsAsync<DurableFlowLeaseContendedException>(
            () => harness.Executor.ExecuteAsync("r41-budget").WaitAsync(TimeSpan.FromSeconds(30)));
        waited.Stop();

        Assert.True(waited.Elapsed >= TimeSpan.FromMilliseconds(550), $"Gave up after {waited.ElapsedMilliseconds} ms — before the budget.");
        Assert.Contains(nameof(DurableFlowOptions.MaxLeaseContentionWait), contended.Reason, StringComparison.Ordinal);
        Assert.Contains("skewed-owner", contended.Reason, StringComparison.Ordinal);
        Assert.Contains("clock skew", contended.Reason, StringComparison.Ordinal);
        Assert.Equal(0, harness.Flow.Executions);
    }

    [Fact]
    public async Task BudgetBelowTheLocalLeaseWindow_NeverShortensThatWindow()
    {
        // The local window (150 ms) is this host's own proof budget and is always waited: a tiny
        // MaxLeaseContentionWait only refuses the store-driven EXTENSION.
        var store = await Round41RegressionTests.UnacquirableStore.CreateAsync(
            "r41-floor", new FlowLeaseObservation("owner", DateTime.UtcNow.AddDays(3)));
        await using var harness = CreateHarness(store, ShortLease(TimeSpan.FromMilliseconds(1)));

        var waited = Stopwatch.StartNew();
        await Assert.ThrowsAsync<DurableFlowLeaseContendedException>(
            () => harness.Executor.ExecuteAsync("r41-floor").WaitAsync(TimeSpan.FromSeconds(30)));
        waited.Stop();

        Assert.True(waited.Elapsed >= TimeSpan.FromMilliseconds(140), $"Gave up after {waited.ElapsedMilliseconds} ms — inside this host's own lease window.");
    }

    [Fact]
    public async Task PersistedExpiryInsideTheBudget_IsStillWaitedOut_AndTakenOver()
    {
        // Round 40's guarantee, under an explicit budget: a crashed owner's 600 ms lease outlives
        // the successor's 150 ms window, fits the 10 s budget, and is taken over when it lapses.
        var store = new InMemoryFlowStateStore();
        var state = RunnableState("r41-within-budget");
        await store.TryCreateAsync(state.FlowId!, state, TimeSpan.FromMinutes(5));
        Assert.True(await store.TryAcquireLeaseAsync(state.FlowId!, "crashed-owner", TimeSpan.FromMilliseconds(600)));
        await using var harness = CreateHarness(store, ShortLease(TimeSpan.FromSeconds(10)));

        await harness.Executor.ExecuteAsync(state.FlowId!).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(state.FlowId!))!.Status);
        Assert.Equal(1, harness.Flow.Executions);
    }

    // ---------- F7/F8: PortableText.TruncateWellFormed ----------

    [Theory]
    [InlineData("", 0, "")]
    [InlineData("abc", 0, "")]
    [InlineData("abc", 3, "abc")]
    [InlineData("abc", 10, "abc")]
    [InlineData("abcdef", 4, "abcd")]
    public void TruncateWellFormed_IsAPlainPrefixAwayFromSurrogates(string value, int maxLength, string expected)
        => Assert.Equal(expected, PortableText.TruncateWellFormed(value, maxLength));

    [Fact]
    public void TruncateWellFormed_StepsBackOneUnit_RatherThanSplitAPair()
    {
        const string grin = "\U0001F600";
        var value = "ab" + grin + "cd";

        Assert.Equal("ab", PortableText.TruncateWellFormed(value, 3));       // the cut falls inside the pair
        Assert.Equal("ab" + grin, PortableText.TruncateWellFormed(value, 4)); // the pair fits exactly
        Assert.Equal("ab" + grin + "c", PortableText.TruncateWellFormed(value, 5));
        Assert.Equal(string.Empty, PortableText.TruncateWellFormed(grin, 1));
    }

    [Fact]
    public void TruncateWellFormed_DoesNotRepairASurrogateThatWasAlreadyUnpaired()
    {
        // A lone high surrogate followed by an ordinary character is not a pair to protect: the
        // cut keeps the budget, and the input's defect stays the caller's to report.
        var value = "ab\uD83D" + "cd";

        Assert.Equal("ab\uD83D", PortableText.TruncateWellFormed(value, 3));
        Assert.Throws<ArgumentOutOfRangeException>(() => PortableText.TruncateWellFormed(value, -1));
    }

    // ---------- F4: ScheduledFlowService.RecentOccurrences ----------

    [Theory]
    [InlineData("* * * * *", 1)]          // dense: the first look-back already holds more than enough
    [InlineData("*/30 * * * *", 1)]       // one hour holds two: the look-back has to grow
    [InlineData("0 3 * * *", 40)]         // daily over forty days
    [InlineData("0 0 1 * *", 800)]        // monthly over two years: fewer than the cap exist at all
    [InlineData("* * 1 1 *", 800)]        // dense inside one day a year, empty everywhere else
    [InlineData("0 0 30 2 *", 800)]       // never
    public void RecentOccurrences_AreExactlyTheTailOfTheWholeWindowWalk(string cron, int windowDays)
    {
        var schedule = CronSchedule.Parse(cron, TimeZoneInfo.Utc);
        var now = new DateTimeOffset(2030, 3, 15, 12, 0, 30, TimeSpan.Zero);
        var window = TimeSpan.FromDays(windowDays);

        // The walk the probe used to do: everything in (now - window, now], then keep the tail.
        var everything = new List<DateTimeOffset>();
        var cursor = now - window;
        while (schedule.GetNextOccurrence(cursor) is { } occurrence && occurrence <= now)
        {
            everything.Add(occurrence);
            cursor = occurrence;
        }

        var expected = everything.Skip(Math.Max(0, everything.Count - ScheduledFlowService.MaxStartupProbes)).ToList();

        Assert.Equal(expected, ScheduledFlowService.RecentOccurrences(schedule, now, window, ScheduledFlowService.MaxStartupProbes));
    }

    [Fact]
    public void RecentOccurrences_AWindowLongerThanTimeItself_IsClampedAtTheEpoch_AndStaysCheap()
    {
        var schedule = CronSchedule.Parse("* * * * *", TimeZoneInfo.Utc);
        var now = new DateTimeOffset(2030, 1, 1, 0, 0, 30, TimeSpan.Zero);

        var walked = Stopwatch.StartNew();
        var recent = ScheduledFlowService.RecentOccurrences(schedule, now, TimeSpan.MaxValue, ScheduledFlowService.MaxStartupProbes);
        walked.Stop();

        Assert.Equal(ScheduledFlowService.MaxStartupProbes, recent.Count);
        Assert.Equal(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero), recent[^1]);
        Assert.Equal(recent[^1] - TimeSpan.FromMinutes(ScheduledFlowService.MaxStartupProbes - 1), recent[0]);
        Assert.True(walked.Elapsed < TimeSpan.FromSeconds(2), $"Walked for {walked.Elapsed}.");
    }

    [Fact]
    public void RecentOccurrences_ASparseScheduleStillReachesTheFarEndOfItsWindow()
    {
        // The only occurrence sits 399 days back — past every early look-back, inside the window.
        var schedule = CronSchedule.Parse("0 0 10 2 *", TimeZoneInfo.Utc);
        var now = new DateTimeOffset(2030, 3, 15, 12, 0, 0, TimeSpan.Zero);

        var recent = ScheduledFlowService.RecentOccurrences(schedule, now, TimeSpan.FromDays(400), ScheduledFlowService.MaxStartupProbes);

        Assert.Equal([new DateTimeOffset(2029, 2, 10, 0, 0, 0, TimeSpan.Zero), new DateTimeOffset(2030, 2, 10, 0, 0, 0, TimeSpan.Zero)], recent);
    }
}
