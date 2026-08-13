using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Globalization;
using Xunit;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// Cron-scheduled flows on the harness: occurrences fire on virtual time with deterministic run
// ids, replicas dedup through the store's atomic create, missed occurrences are skipped, and
// registration validates the expression up front.
// ---------------------------------------------------------------------------------------------

public sealed record ReportInput(DateTimeOffset Occurrence);

public sealed class NightlyReportFlow(StepRecorder _recorder) : IDurableFlow<ReportInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ReportInput input)
    {
        await flow.StepAsync("build-report", () =>
        {
            _recorder.Record($"report:{input.Occurrence:yyyyMMdd'T'HHmm}");
            return Task.CompletedTask;
        });
    }
}

public sealed class CapturingLogger<T> : ILogger<T>
{
    private readonly List<(LogLevel Level, string Message)> _entries = [];

    public IReadOnlyList<(LogLevel Level, string Message)> Entries
    {
        get { lock (_entries) return [.. _entries]; }
    }

    /// <summary>
    /// Waits for the logger itself to record a matching entry. The scheduler's own work is the
    /// thing under test in some of these facts, and it does not always enqueue a worker job —
    /// an input factory that throws, for instance, fails before anything reaches the queue. Waiting
    /// for worker idleness there proves nothing: the queue is already idle, so the assertion runs
    /// against whatever the scheduler happens to have logged by then, and passes or fails with the
    /// machine's load. Wait for the signal the assertion is actually about.
    /// </summary>
    public async Task<(LogLevel Level, string Message)> WaitForEntryAsync(
        Func<(LogLevel Level, string Message), bool> match,
        string expectation)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (true)
        {
            if (Entries.FirstOrDefault(match) is { Message: not null } found)
                return found;

            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"Timed out waiting for {expectation}. Logged: [{string.Join("; ", Entries.Select(entry => $"{entry.Level}: {entry.Message}"))}]");

            await Task.Delay(20);
        }
    }

    IDisposable? ILogger.BeginScope<TState>(TState state) => null;

    bool ILogger.IsEnabled(LogLevel logLevel) => true;

    void ILogger.Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        lock (_entries)
            _entries.Add((logLevel, formatter(state, exception)));
    }
}

public class ScheduledFlowTests
{
    [Fact]
    public async Task DailySchedule_StartsOneRunPerOccurrence_WithDeterministicIds()
    {
        var recorder = new StepRecorder();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.StartTime = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
            options.ConfigureServices = services => services.AddSingleton(recorder);
            options.ConfigureAsyncResponse = builder => builder.WithScheduledFlow<NightlyReportFlow, ReportInput>(
                "nightly-report",
                "0 6 * * *",
                occurrence => new ReportInput(occurrence));
        });

        // Nothing before 06:00.
        await harness.AdvanceAsync(TimeSpan.FromHours(5));
        Assert.Empty(recorder.Entries);

        // Cross 06:00 — exactly one run, with the deterministic occurrence id.
        await harness.AdvanceAsync(TimeSpan.FromHours(2));
        var firstId = "sched:nightly-report:20300101T060000Z";
        var firstRun = harness.Attach(firstId);
        Assert.Equal(FlowRunStatus.Succeeded, await firstRun.WaitForFinishedAsync());
        Assert.Equal(["report:20300101T0600"], recorder.Entries);

        // The next day fires the next occurrence.
        await harness.AdvanceAsync(TimeSpan.FromDays(1));
        var secondRun = harness.Attach("sched:nightly-report:20300102T060000Z");
        Assert.Equal(FlowRunStatus.Succeeded, await secondRun.WaitForFinishedAsync());
        Assert.Equal(2, recorder.Entries.Count);
    }

    [Fact]
    public async Task TwoSchedulerReplicas_OneRunPerOccurrence_AtomicCreateDedups()
    {
        var recorder = new StepRecorder();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.StartTime = new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
            options.ConfigureServices = services => services.AddSingleton(recorder);
            options.ConfigureAsyncResponse = builder =>
            {
                builder.WithScheduledFlow<NightlyReportFlow, ReportInput>(
                    "hourly",
                    "0 * * * *",
                    occurrence => new ReportInput(occurrence));

                // A second scheduler instance over the same registrations — two replicas racing
                // the same occurrence against one store.
                builder.Services.AddSingleton<IHostedService>(provider => new ScheduledFlowService(
                    provider.GetRequiredService<IDurableFlows>(),
                    provider.GetServices<ScheduledFlowRegistration>(),
                    NullLogger<ScheduledFlowService>.Instance,
                    provider.GetService<TimeProvider>()));
            };
        });

        await harness.AdvanceAsync(TimeSpan.FromHours(1) + TimeSpan.FromMinutes(1));
        var run = harness.Attach("sched:hourly:20300101T010000Z");
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());

        // Both replicas computed the same occurrence; the ledger's atomic create let exactly one
        // run exist, so the side effect happened once.
        Assert.Equal(["report:20300101T0100"], recorder.Entries);
    }

    [Fact]
    public async Task MissedOccurrences_AreSkipped_NotReplayedInABurst()
    {
        var recorder = new StepRecorder();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.StartTime = new DateTimeOffset(2030, 1, 1, 0, 30, 0, TimeSpan.Zero);
            options.ConfigureServices = services => services.AddSingleton(recorder);
            options.ConfigureAsyncResponse = builder => builder.WithScheduledFlow<NightlyReportFlow, ReportInput>(
                "hourly",
                "0 * * * *",
                occurrence => new ReportInput(occurrence));
        });

        // An OUTAGE: the host is down while four hours pass. On restart the scheduler resumes
        // from "now" — the 01:00–04:00 occurrences that fell into the downtime are skipped by
        // policy (at-most-once schedule), never replayed in a burst.
        await harness.Engine.SimulateRestartAsync(whileDown: () => harness.Clock.Advance(TimeSpan.FromHours(4)));
        await harness.Engine.WaitForWorkerIdleAsync();
        Assert.Empty(recorder.Entries);

        // The next scheduled occurrence after the outage (05:00) fires normally.
        await harness.AdvanceAsync(TimeSpan.FromMinutes(30));
        var run = harness.Attach("sched:hourly:20300101T050000Z");
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
        Assert.Equal(["report:20300101T0500"], recorder.Entries);
    }

    [Fact]
    public async Task InputFactoryThrowingInvalidOperation_IsAFailedStart_NotAReportedDuplicate()
    {
        var logger = new CapturingLogger<ScheduledFlowService>();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.StartTime = new DateTimeOffset(2030, 1, 1, 0, 30, 0, TimeSpan.Zero);
            options.ConfigureServices = services => services.AddSingleton<ILogger<ScheduledFlowService>>(logger);
            options.ConfigureAsyncResponse = builder => builder.WithScheduledFlow<NightlyReportFlow, ReportInput>(
                "hourly",
                "0 * * * *",
                occurrence => throw new InvalidOperationException("input factory exploded"));
        });

        await harness.AdvanceAsync(TimeSpan.FromMinutes(31));

        // NOTHING was started — the user's input factory threw before any store call. Pre-fix the
        // broad InvalidOperationException catch classified this as the benign deterministic-id
        // duplicate and logged "already started with different input … ran exactly once" — a
        // successful occurrence report for an occurrence that never ran. It is a failed start.
        // Waited for through the scheduler's own log rather than worker idleness: this occurrence
        // never reaches the worker queue, so the queue is idle from the start and proves nothing.
        var error = await logger.WaitForEntryAsync(
            entry => entry.Level == LogLevel.Error,
            "the scheduler to log the failed start");
        Assert.Contains("failed to start occurrence", error.Message, StringComparison.Ordinal);
        Assert.Single(logger.Entries, entry => entry.Level == LogLevel.Error);
        Assert.DoesNotContain(logger.Entries, entry => entry.Message.Contains("already started with different input", StringComparison.Ordinal));
    }

    [Fact]
    public async Task NonDeterministicInputFactory_AcrossReplicas_StillReportsTheDuplicateWarning()
    {
        // The counterpart guard: a REAL deterministic-id conflict (the loser replica's factory
        // produced different input) must keep the benign duplicate reading — if the dedicated
        // conflict exception ever stopped flowing, this warning would disappear and the loser
        // would report a scary error for an occurrence that ran fine.
        var recorder = new StepRecorder();
        var logger = new CapturingLogger<ScheduledFlowService>();
        var sequence = 0;
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.StartTime = new DateTimeOffset(2030, 1, 1, 0, 30, 0, TimeSpan.Zero);
            options.ConfigureServices = services =>
            {
                services.AddSingleton(recorder);
                services.AddSingleton<ILogger<ScheduledFlowService>>(logger);
            };
            options.ConfigureAsyncResponse = builder =>
            {
                builder.WithScheduledFlow<NightlyReportFlow, ReportInput>(
                    "hourly",
                    "0 * * * *",
                    occurrence => new ReportInput(occurrence.AddTicks(Interlocked.Increment(ref sequence))));

                // A second scheduler replica over the same registrations, racing the same store.
                builder.Services.AddSingleton<IHostedService>(provider => new ScheduledFlowService(
                    provider.GetRequiredService<IDurableFlows>(),
                    provider.GetServices<ScheduledFlowRegistration>(),
                    provider.GetRequiredService<ILogger<ScheduledFlowService>>(),
                    provider.GetService<TimeProvider>()));
            };
        });

        await harness.AdvanceAsync(TimeSpan.FromMinutes(31));
        var run = harness.Attach("sched:hourly:20300101T010000Z");
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());

        Assert.Single(recorder.Entries);

        // Same reasoning as the fact above, one step removed: the assertion is about the LOSER
        // replica's log line, and the winner's run finishing says nothing about when the loser
        // reached its catch. Wait for the line itself.
        await logger.WaitForEntryAsync(
            entry => entry.Level == LogLevel.Warning
                && entry.Message.Contains("not deterministic across replicas", StringComparison.Ordinal),
            "the losing replica to report the benign duplicate");
        Assert.DoesNotContain(logger.Entries, entry => entry.Level == LogLevel.Error);
    }

    [Fact]
    public async Task DisabledSchedule_NeverFires()
    {
        var recorder = new StepRecorder();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(recorder);
            options.ConfigureAsyncResponse = builder => builder.WithScheduledFlow<NightlyReportFlow, ReportInput>(
                "paused",
                "* * * * *",
                occurrence => new ReportInput(occurrence),
                schedule => schedule.Enabled = false);
        });

        await harness.AdvanceAsync(TimeSpan.FromMinutes(10));
        Assert.Empty(recorder.Entries);
    }

    [Fact]
    public void InvalidCronExpression_FailsAtRegistration()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncResponse();
        Assert.Throws<FormatException>(() => builder.WithScheduledFlow<NightlyReportFlow, ReportInput>(
            "broken",
            "61 * * * *",
            occurrence => new ReportInput(occurrence)));
    }

    [Fact]
    public void DuplicateScheduleNames_FailAtRegistration()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncResponse()
            .WithScheduledFlow<NightlyReportFlow, ReportInput>("dup", "0 * * * *", occurrence => new ReportInput(occurrence));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            builder.WithScheduledFlow<NightlyReportFlow, ReportInput>("dup", "30 * * * *", occurrence => new ReportInput(occurrence)));
        Assert.Contains("dup", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UnsatisfiableCronExpression_FailsAtRegistration()
    {
        // Well-formed but impossible ("Feb 30"): before the satisfiability probe this registered
        // cleanly and the schedule's loop died at startup with one warning — the silent 3 a.m.
        // failure registration-time validation exists to prevent.
        var services = new ServiceCollection();
        var builder = services.AddAsyncResponse();
        var ex = Assert.Throws<ArgumentException>(() => builder.WithScheduledFlow<NightlyReportFlow, ReportInput>(
            "impossible",
            "0 0 30 2 *",
            occurrence => new ReportInput(occurrence)));
        Assert.Contains("no future occurrence", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ScheduleName_ThatOverflowsThePortableFlowIdLimit_FailsAtRegistration()
    {
        // "sched:{name}:{timestamp}" must fit DurableFlowOptions.MaxFlowIdLength. Pre-fix a
        // 400-character name registered cleanly and every occurrence then failed at start time —
        // and only on the stores whose flow_id column is 400 characters.
        var services = new ServiceCollection();
        var builder = services.AddAsyncResponse();
        var name = new string('n', DurableFlowOptions.MaxFlowIdLength);
        var ex = Assert.Throws<ArgumentException>(() => builder.WithScheduledFlow<NightlyReportFlow, ReportInput>(
            name,
            "0 * * * *",
            occurrence => new ReportInput(occurrence)));
        Assert.Contains("portable maximum is 400", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    // Every rule of the portable contract, not just the length one: a name carrying any of these
    // registered cleanly and then failed on EVERY occurrence at 3 a.m., logged and discarded.
    // A name's own trailing space is NOT one of them: it lands mid-id ("sched:nightly :…"), where
    // no store folds it — only surrounding spaces are invisible to a padded comparison.
    [InlineData("reports/nightly", "not portable")]
    [InlineData("nightly?", "not portable")]
    [InlineData("nightly#1", "not portable")]
    public void ScheduleName_ThatMakesOccurrenceIdsNonPortable_FailsAtRegistration(string name, string expectedMessage)
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncResponse();
        var ex = Assert.Throws<ArgumentException>(() => builder.WithScheduledFlow<NightlyReportFlow, ReportInput>(
            name,
            "0 * * * *",
            occurrence => new ReportInput(occurrence)));
        Assert.Contains(expectedMessage, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OccurrenceFlowId_IsCultureInvariant()
    {
        // The deterministic occurrence id is the cross-replica exactly-once key: a culture whose
        // default calendar rewrites the digits (Buddhist 2573..., UmAlQura 1452...) would mint a
        // different id on that replica and the occurrence would run twice.
        var occurrence = new DateTimeOffset(2030, 6, 15, 12, 30, 0, TimeSpan.Zero);
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("th-TH");
            Assert.Equal("sched:daily:20300615T123000Z", ScheduledFlowService.OccurrenceFlowId("daily", occurrence));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }
}
