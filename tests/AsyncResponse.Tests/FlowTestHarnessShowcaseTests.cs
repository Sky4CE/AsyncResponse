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
