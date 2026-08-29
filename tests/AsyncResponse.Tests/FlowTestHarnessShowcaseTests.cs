using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// The FlowTestHarness showcase: a production-shaped flow — remote awaited steps with progress,
// a durable timer, conditional local steps — tested with scripted replies, virtual time, and a
// crash-at-every-checkpoint matrix. The flow class carries ZERO test instrumentation: no probes,
// no captured correlation ids, no crash hooks. This suite doubles as the documentation example.
// ---------------------------------------------------------------------------------------------

public sealed record OnboardingInput(long TenantId, bool SendWelcome = true);

/// <summary>The remote system's client — a plain DI dependency the test fakes.</summary>
public interface IProvisioningClient
{
    Task StartMigrationAsync(long tenantId, string correlationId);
    Task StartImportAsync(long tenantId, string correlationId);
}

public sealed class RecordingProvisioningClient : IProvisioningClient
{
    private readonly List<string> _calls = [];

    public IReadOnlyList<string> Calls
    {
        get { lock (_calls) return [.. _calls]; }
    }

    public Task StartMigrationAsync(long tenantId, string correlationId)
    {
        lock (_calls)
            _calls.Add($"migrate:{tenantId}");
        return Task.CompletedTask;
    }

    public Task StartImportAsync(long tenantId, string correlationId)
    {
        lock (_calls)
            _calls.Add($"import:{tenantId}");
        return Task.CompletedTask;
    }
}

public sealed class TenantOnboardingFlow(IProvisioningClient _client, StepRecorder _recorder) : IDurableFlow<OnboardingInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, OnboardingInput input)
    {
        var workspaceId = await flow.StepAsync("create-workspace", () =>
        {
            _recorder.Record("create-workspace");
            return Task.FromResult($"ws-{input.TenantId}");
        });

        var migration = await flow.AwaitStepAsync<OperationResult>(
            "run-migration",
            trigger: cid => _client.StartMigrationAsync(input.TenantId, cid));

        if (migration.Status == OperationStatus.Failed)
            throw new DurableFlowFailedException($"Migration failed: {migration.Message}");

        // Progress-aware: Running responses report progress and keep the wait open.
        await flow.AwaitStepAsync<OperationResult>(
            "import-data",
            trigger: cid => _client.StartImportAsync(input.TenantId, cid),
            until: r => r.Status != OperationStatus.Running);

        // Let the imported data settle before welcoming the tenant (a durable timer).
        await flow.DelayAsync("settle", TimeSpan.FromHours(6));

        if (input.SendWelcome)
        {
            await flow.StepAsync("send-welcome", () =>
            {
                _recorder.Record($"welcome:{workspaceId}");
                return Task.CompletedTask;
            });
        }
    }
}

public sealed record ScopedCrashInput(string Name);

/// <summary>One plain checkpointed step, for pinning flow-scoped crash injection.</summary>
public sealed class ScopedCrashFlow(StepRecorder _recorder) : IDurableFlow<ScopedCrashInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ScopedCrashInput input)
    {
        await flow.StepAsync("work", () =>
        {
            _recorder.Record($"work:{input.Name}");
            return Task.CompletedTask;
        });
    }
}

public sealed record DrainSuspendInput(long Id);

/// <summary>
/// Gates mid-execution so a test can force its durable-timer suspend to happen while the OLD
/// harness incarnation is draining: the wake-up is then published into a stopping transport,
/// exactly the window <c>SimulateRestartAsync</c>'s drain retention exists for.
/// </summary>
public sealed class DrainSuspendFlow : IDurableFlow<DrainSuspendInput>
{
    public static TaskCompletionSource EnteredExecution = NewSource();
    public static TaskCompletionSource ReleaseToSuspend = NewSource();

    public static TaskCompletionSource NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    public async Task ExecuteAsync(IDurableFlowContext flow, DrainSuspendInput input)
    {
        EnteredExecution.TrySetResult();
        await ReleaseToSuspend.Task.ConfigureAwait(false);
        await flow.DelayAsync("two-day-settle", TimeSpan.FromDays(2));
    }
}

public sealed record LongSettleInput(long Id);

/// <summary>A flow whose single durable timer exceeds the in-memory transport's ~49.7-day per-hop ceiling.</summary>
public sealed class SixtyDaySettleFlow : IDurableFlow<LongSettleInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, LongSettleInput input)
        => await flow.DelayAsync("sixty-day-settle", TimeSpan.FromDays(60));
}

public class FlowTestHarnessShowcaseTests
{
    private static async Task<(FlowTestHarness Harness, RecordingProvisioningClient Client, StepRecorder Recorder)> StartAsync()
    {
        var client = new RecordingProvisioningClient();
        var recorder = new StepRecorder();
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services =>
            {
                services.AddSingleton<IProvisioningClient>(client);
                services.AddSingleton(recorder);
            };
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<TenantOnboardingFlow, OnboardingInput>();
        });
        return (harness, client, recorder);
    }

    [Fact]
    public async Task FullRun_ScriptedReplies_ProgressAndTimer_NoBrokerNoSleeps()
    {
        var (harness, client, recorder) = await StartAsync();
        await using var _ = harness;

        var run = await harness.StartFlowAsync<TenantOnboardingFlow, OnboardingInput>(new OnboardingInput(7));

        // The flow triggered the migration with a correlation id the harness observed — reply as
        // the remote system would.
        await run.WaitForAwaitingStepAsync("run-migration");
        Assert.Contains("migrate:7", client.Calls);
        await run.ReplyAsync(new OperationResult { Status = OperationStatus.Completed, Message = "migrated" });

        // The import reports progress twice before completing; Running keeps the wait open.
        await run.WaitForAwaitingStepAsync("import-data");
        await run.ReplyAsync(new OperationResult { Status = OperationStatus.Running, Message = "10%" });
        await run.ReplyAsync(new OperationResult { Status = OperationStatus.Running, Message = "60%" });
        await run.ReplyAsync(new OperationResult { Status = OperationStatus.Completed, Message = "imported" });

        // The six-hour settle timer parks the run; virtual time skips it.
        await run.WaitForTimerStepAsync("settle");
        await harness.AdvanceAsync(TimeSpan.FromHours(6));

        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
        Assert.Equal(["create-workspace", "welcome:ws-7"], recorder.Entries);

        // The memoized checkpoint holds the terminal payload, not the progress messages.
        var state = await run.GetStateAsync();
        Assert.Contains("imported", state!.Steps!["import-data"].ResultJson);
    }

    [Fact]
    public async Task RemoteFailure_ReplyException_FailsTheAwaitedStepAndRestartsItFresh()
    {
        var (harness, client, _) = await StartAsync();
        await using var _1 = harness;

        var run = await harness.StartFlowAsync<TenantOnboardingFlow, OnboardingInput>(new OnboardingInput(9));

        await run.WaitForAwaitingStepAsync("run-migration");
        await run.ReplyExceptionAsync(new InvalidOperationException("migration service exploded"));

        // The failed attempt marks the step faulted; the transport's redelivery (backoff on the
        // virtual clock) restarts the idempotent step FRESH — a second trigger, a new correlation
        // id — and parks awaiting it, so wait for the trigger, not for an idle worker.
        await harness.AdvanceAsync(TimeSpan.FromSeconds(2));
        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
        while (client.Calls.Count(c => c == "migrate:9") < 2)
        {
            Assert.True(TimeProvider.System.GetUtcNow() < guard, "the faulted step was not re-triggered");
            await Task.Delay(TimeSpan.FromMilliseconds(5));
        }

        Assert.Equal(2, client.Calls.Count(c => c == "migrate:9"));

        // This time the migration succeeds and the flow proceeds.
        await run.ReplyAsync(new OperationResult { Status = OperationStatus.Completed });
        await run.WaitForAwaitingStepAsync("import-data");
        await run.ReplyAsync(new OperationResult { Status = OperationStatus.Completed });
        await run.WaitForTimerStepAsync("settle");
        await harness.AdvanceAsync(TimeSpan.FromHours(6));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    [Fact]
    public async Task FailedAttempt_ClearsTheQuiesceProbesParkedEntry()
    {
        // Regression: a step parked in-process, then its wait faulted (a published exception the
        // flow does not treat as terminal). The attempt failed for redelivery, releasing its
        // worker slot — but no step-completed or run-finished event ever fired, so the harness's
        // quiesce probe kept the parked entry for the rest of the incarnation. From then on
        // SettleAsync's guard under-counted busy jobs by one, and AdvanceAsync could move the
        // virtual clock while another flow's step was still mid-user-code.
        var (harness, _, _) = await StartAsync();
        await using var _1 = harness;

        var run = await harness.StartFlowAsync<TenantOnboardingFlow, OnboardingInput>(new OnboardingInput(13));
        await run.WaitForAwaitingStepAsync("run-migration");

        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
        while (ParkedCount(harness) != 1)
        {
            Assert.True(TimeProvider.System.GetUtcNow() < guard, "the awaited step never registered as parked");
            await Task.Delay(TimeSpan.FromMilliseconds(5));
        }

        await run.ReplyExceptionAsync(new InvalidOperationException("migration service exploded"));

        // The failed attempt released its worker slot; until the redelivery re-parks the step,
        // nothing is parked in process — the probe must agree instead of leaking the entry.
        while (ParkedCount(harness) != 0)
        {
            Assert.True(TimeProvider.System.GetUtcNow() < guard, "the parked entry leaked after the failed attempt");
            await Task.Delay(TimeSpan.FromMilliseconds(5));
        }
    }

    [Fact]
    public async Task FlowScopedCrash_FiresOnTheNamedRun_NotWhicheverReachesTheStepFirst()
    {
        // Regression (round 31): the one-shot crash slot matched on step name alone, so with two
        // runs (or a parent and a child flow reusing step names) the crash landed on whichever
        // reached the step first — with the default single worker, deterministically the
        // first-started run. A test arming a crash for run B then asserted B's replay while B ran
        // clean and the recovery path never executed. Passing the flow id pins the crash to its
        // run: another run reaching the step first must NOT consume it.
        var recorder = new StepRecorder();
        await using var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(recorder);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ScopedCrashFlow, ScopedCrashInput>();
        });

        harness.CrashBeforeStep("work", flowId: "run-b");

        var a = await harness.StartFlowAsync<ScopedCrashFlow, ScopedCrashInput>(new ScopedCrashInput("a"), "run-a");
        Assert.Equal(FlowRunStatus.Succeeded, await a.WaitForFinishedAsync());

        var b = await harness.StartFlowAsync<ScopedCrashFlow, ScopedCrashInput>(new ScopedCrashInput("b"), "run-b");
        // The crashed delivery's redelivery waits out the transport's retry backoff on the
        // virtual clock; advance until the replay lands (CrashMatrix pattern).
        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
        while ((await b.GetStateAsync())?.Status != FlowRunStatus.Succeeded)
        {
            Assert.True(TimeProvider.System.GetUtcNow() < guard, "run-b never finished");
            await harness.AdvanceAsync(TimeSpan.FromMinutes(1));
        }

        Assert.Equal(FlowRunStatus.Succeeded, await b.WaitForFinishedAsync());

        // Run A passed the armed step first and did not consume the crash; run B crashed before
        // its step (two Starting events) and replayed, with the side effect still exactly-once.
        Assert.Equal(1, a.StepExecutions("work"));
        Assert.Equal(2, b.StepExecutions("work"));
        Assert.Single(recorder.Entries, entry => entry == "work:a");
        Assert.Single(recorder.Entries, entry => entry == "work:b");
    }

    [Fact]
    public async Task ArmingASecondCrash_WhileOneIsStillArmed_Throws()
    {
        // Regression: the second arm silently overwrote a still-unfired first one, so a test
        // arming two crashes believed it exercised a crash/resume path that never ran. One slot,
        // loudly: arm the next crash after the current one has fired.
        var (harness, _, _) = await StartAsync();
        await using var _1 = harness;

        harness.CrashBeforeStep("run-migration");

        var ex = Assert.Throws<InvalidOperationException>(() => harness.CrashAfterStep("import-data"));
        Assert.Contains("already armed", ex.Message);
    }

    private static int ParkedCount(FlowTestHarness harness)
    {
        var quiesce = typeof(AsyncResponseTestHarness)
            .GetField("_quiesce", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!
            .GetValue(harness.Engine)!;
        return (int)quiesce.GetType().GetProperty("ParkedCount")!.GetValue(quiesce)!;
    }

    [Fact]
    public async Task DeclaredTerminalFailure_FailsTheRun()
    {
        var (harness, _, recorder) = await StartAsync();
        await using var _1 = harness;

        var run = await harness.StartFlowAsync<TenantOnboardingFlow, OnboardingInput>(new OnboardingInput(11));
        await run.WaitForAwaitingStepAsync("run-migration");
        await run.ReplyAsync(new OperationResult { Status = OperationStatus.Failed, Message = "no capacity" });

        Assert.Equal(FlowRunStatus.Failed, await run.WaitForFinishedAsync());
        var state = await run.GetStateAsync();
        Assert.Contains("no capacity", state!.LastMessage);
        Assert.DoesNotContain(recorder.Entries, e => e.StartsWith("welcome:", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("create-workspace", true)]
    [InlineData("create-workspace", false)]
    [InlineData("run-migration", true)]
    [InlineData("import-data", true)]
    [InlineData("settle", true)]
    [InlineData("send-welcome", true)]
    [InlineData("send-welcome", false)]
    public async Task CrashMatrix_AtEveryCheckpoint_ResumesWithExactlyOnceSideEffects(string crashStep, bool beforeStep)
    {
        var (harness, client, recorder) = await StartAsync();
        await using var _ = harness;

        if (beforeStep)
            harness.CrashBeforeStep(crashStep);
        else
            harness.CrashAfterStep(crashStep);

        var run = await harness.StartFlowAsync<TenantOnboardingFlow, OnboardingInput>(new OnboardingInput(21));

        // Drive the flow to completion, answering each awaited step ONCE as it appears and
        // skipping time over the settle timer and every crash-retry backoff. The 10-minute hops
        // stay well inside the in-memory channel's 30-minute default wait timeout, so no awaited
        // step ever times out under the driver — the only disturbance is the injected crash.
        var answered = new HashSet<string>(StringComparer.Ordinal);
        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(30);
        while (true)
        {
            var state = await run.GetStateAsync();
            if (state is { Status: FlowRunStatus.Succeeded })
                break;
            Assert.True(TimeProvider.System.GetUtcNow() < guard, $"run did not finish (status: {state?.Status})");

            if (harness.Probe_LatestAwaitedCid(run) is { } correlationId && answered.Add(correlationId))
            {
                // Terminal for both awaited steps here (the progress-aware one accepts Completed).
                await run.ReplyAsync(new OperationResult { Status = OperationStatus.Completed });
            }

            await harness.AdvanceAsync(TimeSpan.FromMinutes(10));
        }

        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());

        // The invariant the whole library exists for: a crash at ANY checkpoint costs a delivery,
        // never a duplicated side effect.
        Assert.Equal(1, recorder.Count("create-workspace"));
        Assert.Equal(1, recorder.Entries.Count(e => e.StartsWith("welcome:", StringComparison.Ordinal)));
        Assert.Equal(1, client.Calls.Count(c => c == "migrate:21"));
        Assert.True(client.Calls.Count(c => c == "import:21") >= 1);
    }

    [Fact]
    public async Task SimulateRestart_RetainsDelayedWakeUpPublishedWhileTheOldIncarnationDrains()
    {
        // Regression (review fix): SimulateRestartAsync used to SNAPSHOT delayed jobs before the
        // stop, so a flow suspending while the old incarnation drained published its wake-up into
        // a transport that rejected it — the flow slept forever and the restart lost the timer.
        // The drain now RETAINS delayed jobs, including delayed publishes made mid-drain.
        DrainSuspendFlow.EnteredExecution = DrainSuspendFlow.NewSource();
        DrainSuspendFlow.ReleaseToSuspend = DrainSuspendFlow.NewSource();
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<DrainSuspendFlow, DrainSuspendInput>());
        await using var _ = harness;

        var run = await harness.StartFlowAsync<DrainSuspendFlow, DrainSuspendInput>(new DrainSuspendInput(1));
        await DrainSuspendFlow.EnteredExecution.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // The restart starts draining with the flow still executing; the gate release lands while
        // the drain waits for it, so the flow's suspend publishes its 2-day wake mid-drain.
        var restart = harness.Engine.SimulateRestartAsync();
        var release = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromMilliseconds(130));
            DrainSuspendFlow.ReleaseToSuspend.TrySetResult();
        });
        await restart;
        await release;

        await harness.AdvanceAsync(TimeSpan.FromDays(2) + TimeSpan.FromMinutes(1));

        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    [Fact]
    public async Task SimulateRestart_ClampsRetainedWakeUpsBeyondTheTransportMaxDelay()
    {
        // Regression (review fix): a retained wake-up with more than ~49.7 days remaining used to
        // be republished unclamped, throwing ArgumentOutOfRangeException out of the restart (and
        // silently losing the rest of the retained list). The republish must clamp each hop to the
        // transport's MaxPublishDelay — NotBeforeUtc rides the envelope, so the executor re-delays
        // the remainder on delivery, exactly as every production publisher chains long sleeps.
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<SixtyDaySettleFlow, LongSettleInput>());
        await using var _ = harness;

        var run = await harness.StartFlowAsync<SixtyDaySettleFlow, LongSettleInput>(new LongSettleInput(2));
        await run.WaitForTimerStepAsync("sixty-day-settle");
        await harness.Engine.WaitForWorkerIdleAsync();

        await harness.Engine.SimulateRestartAsync();

        await harness.AdvanceAsync(TimeSpan.FromDays(60) + TimeSpan.FromMinutes(1));
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    [Fact]
    public async Task TimedOutProbeWaiters_AreRemoved_NotRetained()
    {
        // Fully qualified: this test assembly declares its own (unrelated) FlowProbe helper type.
        var probe = new Testing.FlowProbe();

        await Assert.ThrowsAsync<TimeoutException>(() =>
            probe.WaitForAsync("flow-x", _ => false, TimeSpan.FromMilliseconds(50), "an event that never happens"));
        await Assert.ThrowsAsync<TimeoutException>(() =>
            probe.WaitForRunAsync("flow-x", TimeSpan.FromMilliseconds(50), "a run that never finishes"));

        // Pre-fix, timed-out waiters stayed registered forever: every future event re-evaluated
        // their predicates and their completion sources pinned the captured closures — a steady
        // leak in suites that probe with short guards in retry loops.
        Assert.Equal(0, probe.PendingWaiterCount);
    }
}

internal static class FlowTestHarnessTestExtensions
{
    /// <summary>Peeks whether the run has an outstanding awaited-step correlation id (no waiting).</summary>
    public static string? Probe_LatestAwaitedCid(this FlowTestHarness harness, FlowRunHandle run)
        => harness.Probe.LatestAwaitedCorrelationId(run.FlowId, stepName: null);
}
