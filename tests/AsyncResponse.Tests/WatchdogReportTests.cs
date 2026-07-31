using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Pure-evaluation tests for the watchdog: which persisted recovery entries count as stale
/// (waiter gone, nothing arrived for too long) versus healthy.
/// </summary>
public class WatchdogReportTests
{
    private static readonly DateTime Now = new(2026, 6, 11, 12, 0, 0, DateTimeKind.Utc);
    private static readonly TimeSpan StaleAfter = TimeSpan.FromHours(24);

    [Fact]
    public void OldEntryWithoutSubscriber_IsStale()
    {
        var entry = Entry(registeredAgo: TimeSpan.FromHours(30), activeSubscribers: 0);

        var report = AsyncResponseWatchdogReport.Evaluate([entry], Now, StaleAfter);

        Assert.Equal(1, report.TotalEntries);
        Assert.Equal(0, report.EntriesWithActiveWaiter);
        Assert.Same(entry, Assert.Single(report.StaleEntries));
        Assert.Equal(0, report.UnknownAgeEntries);
    }

    [Fact]
    public void OldEntryWithLiveWaiter_IsNotStale()
    {
        // A live subscriber means a waiter is legitimately awaiting (long-running flow).
        var entry = Entry(registeredAgo: TimeSpan.FromHours(72), activeSubscribers: 1);

        var report = AsyncResponseWatchdogReport.Evaluate([entry], Now, StaleAfter);

        Assert.Equal(1, report.EntriesWithActiveWaiter);
        Assert.Empty(report.StaleEntries);
    }

    [Fact]
    public void YoungEntryWithoutSubscriber_IsNotStale()
    {
        // Armed recovery state: the waiter died (e.g. redeploy) but the response — and with it
        // the lost-subscriber recovery — may still arrive.
        var entry = Entry(registeredAgo: TimeSpan.FromHours(2), activeSubscribers: 0);

        var report = AsyncResponseWatchdogReport.Evaluate([entry], Now, StaleAfter);

        Assert.Empty(report.StaleEntries);
        Assert.Equal(0, report.UnknownAgeEntries);
    }

    [Fact]
    public void EntryWithoutTimestamp_IsCountedAsUnknownAge_NotStale()
    {
        var entry = new RecoveryStateObservation("legacy", RegisteredAtUtc: null, ActiveSubscribers: 0, PayloadTypeFullName: null);

        var report = AsyncResponseWatchdogReport.Evaluate([entry], Now, StaleAfter);

        Assert.Empty(report.StaleEntries);
        Assert.Equal(1, report.UnknownAgeEntries);
    }

    [Fact]
    public void UnknownLiveness_IsNotStaleOrUnknownAge()
    {
        var entry = Entry(registeredAgo: TimeSpan.FromDays(3), activeSubscribers: -1);

        var report = AsyncResponseWatchdogReport.Evaluate([entry], Now, StaleAfter);

        Assert.Equal(1, report.TotalEntries);
        Assert.Empty(report.StaleEntries);
        Assert.Equal(0, report.UnknownAgeEntries);
    }

    [Fact]
    public void MixedSnapshot_ReportsAllBucketsCorrectly()
    {
        var stale = Entry(TimeSpan.FromDays(3), activeSubscribers: 0, correlationId: "stale");
        var healthyActive = Entry(TimeSpan.FromDays(3), activeSubscribers: 2, correlationId: "active");
        var armedYoung = Entry(TimeSpan.FromMinutes(10), activeSubscribers: 0, correlationId: "young");
        var legacy = new RecoveryStateObservation("legacy", null, 0, null);

        var report = AsyncResponseWatchdogReport.Evaluate([stale, healthyActive, armedYoung, legacy], Now, StaleAfter);

        Assert.Equal(4, report.TotalEntries);
        Assert.Equal(1, report.EntriesWithActiveWaiter);
        Assert.Equal(1, report.UnknownAgeEntries);
        Assert.Equal("stale", report.StaleEntries.Single().CorrelationId);
    }

    [Fact]
    public void DuplicateCorrelationIds_AreCountedOnce()
    {
        var first = Entry(TimeSpan.FromDays(3), activeSubscribers: 0, correlationId: "same-cid");
        var second = Entry(TimeSpan.FromDays(3), activeSubscribers: 0, correlationId: "same-cid");

        var report = AsyncResponseWatchdogReport.Evaluate([first, second], Now, StaleAfter);

        Assert.Equal(1, report.TotalEntries);
        Assert.Single(report.StaleEntries);
    }

    private static RecoveryStateObservation Entry(TimeSpan registeredAgo, long activeSubscribers, string correlationId = "corr-1")
        => new(
            correlationId,
            Now - registeredAgo,
            activeSubscribers,
            typeof(OperationResult).FullName);

    [Fact]
    public void Evaluate_DuplicateCorrelationIds_KeepsOldestRegistration()
    {
        // The scanner contract promises no ordering: a young sibling yielded first must not mask
        // an older stale one, so dedupe prefers the oldest registration per correlation id.
        var young = Entry(registeredAgo: TimeSpan.FromMinutes(5), activeSubscribers: 0, correlationId: "cid");
        var oldStale = Entry(registeredAgo: TimeSpan.FromDays(2), activeSubscribers: 0, correlationId: "cid");

        var report = AsyncResponseWatchdogReport.Evaluate([young, oldStale], Now, StaleAfter);

        Assert.Equal(1, report.TotalEntries);
        var stale = Assert.Single(report.StaleEntries);
        Assert.Equal(oldStale.RegisteredAtUtc, stale.RegisteredAtUtc);
    }
}
