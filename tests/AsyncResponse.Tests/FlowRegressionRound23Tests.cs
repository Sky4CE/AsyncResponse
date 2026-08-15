using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using System.Collections;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// Round-23 review regressions in the durable-flow engine and the testing harness, driven through
// FlowTestHarness (virtual clock, in-memory channel/transport/store).
// ---------------------------------------------------------------------------------------------

public sealed record R23Input(long Id);

/// <summary>Records every correlation id its awaited-step trigger hands out.</summary>
public sealed class CidRecorder
{
    private readonly List<string> _cids = [];

    public IReadOnlyList<string> Cids
    {
        get { lock (_cids) return [.. _cids]; }
    }

    public void Record(string cid)
    {
        lock (_cids)
            _cids.Add(cid);
    }
}

/// <summary>Awaits one step with a short timeout; a timed-out attempt restarts it fresh.</summary>
public sealed class TimeoutRetryFlow(CidRecorder _cids) : IDurableFlow<R23Input>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, R23Input input)
    {
        await flow.AwaitStepAsync<OperationResult>(
            "charge",
            trigger: cid =>
            {
                _cids.Record(cid);
                return Task.CompletedTask;
            },
            timeout: TimeSpan.FromMinutes(1));
    }
}

/// <summary>Awaits one step whose timeout far exceeds the default StateExpiry.</summary>
public sealed class LongWaitFlow : IDurableFlow<R23Input>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, R23Input input)
    {
        await flow.AwaitStepAsync<OperationResult>(
            "slow",
            trigger: _ => Task.CompletedTask,
            timeout: TimeSpan.FromDays(30));
    }
}

/// <summary>
/// <see cref="LongWaitFlow"/> with an entered-execution latch, so a test can tell exactly when a
/// REDELIVERED execution is past the executor's per-attempt save (which resets the ledger TTL to
/// the plain StateExpiry) and is about to re-attach to the 30-day wait.
/// </summary>
public sealed class LongWaitReattachFlow : IDurableFlow<R23Input>
{
    public static TaskCompletionSource EnteredExecution = ReattachRaceFlow.NewSource();

    public async Task ExecuteAsync(IDurableFlowContext flow, R23Input input)
    {
        EnteredExecution.TrySetResult();
        await flow.AwaitStepAsync<OperationResult>(
            "slow",
            trigger: _ => Task.CompletedTask,
            timeout: TimeSpan.FromDays(30));
    }
}

/// <summary>
/// Gates mid-execution (before its awaited step) so a test can land concurrent lease-bypassing
/// writes — RecoverAsync completing the step, FailAsync terminally failing the run — between the
/// execution's state load and its re-attach to the awaited step.
/// </summary>
public sealed class ReattachRaceFlow : IDurableFlow<R23Input>
{
    public static TaskCompletionSource Gate = CompletedSource();
    public static TaskCompletionSource EnteredExecution = NewSource();

    public async Task ExecuteAsync(IDurableFlowContext flow, R23Input input)
    {
        EnteredExecution.TrySetResult();
        await Gate.Task;
        await flow.AwaitStepAsync<OperationResult>(
            "s",
            trigger: _ => Task.CompletedTask,
            timeout: TimeSpan.FromMinutes(1));
    }

    public static TaskCompletionSource NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    public static TaskCompletionSource CompletedSource()
    {
        var source = NewSource();
        source.TrySetResult();
        return source;
    }
}

public sealed class FlowRegressionRound23Tests
{
    [Fact]
    public async Task WaitForAwaitingStep_AfterAFaultedAttempt_ReturnsTheFreshCorrelationId()
    {
        // Regression (r23): FlowProbe.WaitForAsync replayed history front-to-back, so after a
        // faulted attempt re-awaited the step with a fresh correlation id the public barrier
        // still handed back the ABANDONED id of the first attempt — a reply published to it was
        // silently dropped and the live wait never resolved.
        var cids = new CidRecorder();
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.ConfigureServices = services => services.AddSingleton(cids);
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<TimeoutRetryFlow, R23Input>();
        });
        await using var _ = harness;

        var run = await harness.StartFlowAsync<TimeoutRetryFlow, R23Input>(new R23Input(1));
        var firstCid = await run.WaitForAwaitingStepAsync("charge");

        // The step timeout faults attempt 1; the transport's redelivery (backoff on the virtual
        // clock) restarts the idempotent step fresh with a new correlation id.
        await harness.AdvanceAsync(TimeSpan.FromMinutes(1) + TimeSpan.FromSeconds(1));
        await harness.AdvanceAsync(TimeSpan.FromSeconds(2));
        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
        while (cids.Cids.Count < 2)
        {
            Assert.True(TimeProvider.System.GetUtcNow() < guard, "the faulted step was not re-triggered");
            await Task.Delay(TimeSpan.FromMilliseconds(5));
        }

        var awaited = await run.WaitForAwaitingStepAsync("charge");
        Assert.NotEqual(firstCid, awaited);
        Assert.Equal(cids.Cids[1], awaited);

        // The returned id is live: replying to it completes the run.
        await harness.Engine.PublishAsync(new OperationResult { Status = OperationStatus.Completed }, awaited);
        Assert.Equal(FlowRunStatus.Succeeded, await run.WaitForFinishedAsync());
    }

    [Fact]
    public async Task AwaitedStepBreadcrumb_StampsALedgerTtlCoveringTheStepTimeout()
    {
        // Regression (r23): the awaited-step breadcrumb save stamped the plain StateExpiry TTL
        // (14 days by default), so a step timeout longer than that let the ledger row expire
        // mid-wait — lease renewal is gated on the row, so the run was cancelled with a
        // lost-lease error against a ledger that no longer existed and ResumeAsync had nothing
        // to find. The breadcrumb now stamps timeout + StateExpiry, exactly like a timer sleep.
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<LongWaitFlow, R23Input>());
        await using var _ = harness;

        var run = await harness.StartFlowAsync<LongWaitFlow, R23Input>(new R23Input(2));
        await run.WaitForAwaitingStepAsync("slow");

        using var scope = harness.Engine.Services.CreateScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();
        var entriesField = store.GetType().GetField("_entries", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(entriesField);
        var entries = (IDictionary)entriesField!.GetValue(store)!;
        var entry = entries[run.FlowId];
        Assert.NotNull(entry);
        var expires = (DateTime)entry!.GetType().GetProperty("ExpiresAtUtc")!.GetValue(entry)!;

        var now = harness.Clock.GetUtcNow().UtcDateTime;
        Assert.True(
            expires > now + TimeSpan.FromDays(30),
            $"the parked ledger expires in {expires - now}; it must outlive the 30-day step timeout.");
    }

    [Fact]
    public async Task ReattachedAwaitedStep_ReextendsTheLedgerTtl_AfterRedelivery()
    {
        // Regression (r24): the FRESH awaited-step path stamps timeout + StateExpiry (r23 above),
        // but a REDELIVERED execution re-attaching to the in-flight wait only logged — and the
        // executor's unconditional per-attempt save had just reset the row's TTL to the plain
        // StateExpiry, so a step timeout longer than StateExpiry out-lived its own ledger: at
        // expiry the lease renewal failed against the vanished row and the run was silently lost.
        // The re-attach branch now re-extends exactly like the timer path's replay branch.
        LongWaitReattachFlow.EnteredExecution = ReattachRaceFlow.NewSource();
        var harness = await FlowTestHarness.StartAsync(options =>
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<LongWaitReattachFlow, R23Input>());
        await using var _ = harness;

        var run = await harness.StartFlowAsync<LongWaitReattachFlow, R23Input>(new R23Input(4));
        await LongWaitReattachFlow.EnteredExecution.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await run.WaitForAwaitingStepAsync("slow");

        // Kill the parked execution without faulting the breadcrumb; the redelivered execution's
        // per-attempt save resets the ledger TTL to the plain StateExpiry BEFORE the flow body
        // runs, so once the latch fires again the old 30d+StateExpiry stamp is gone and only the
        // re-attach branch can re-extend it.
        await harness.Engine.SimulateRestartAsync();
        LongWaitReattachFlow.EnteredExecution = ReattachRaceFlow.NewSource();
        await run.ResumeAsync();
        await LongWaitReattachFlow.EnteredExecution.Task.WaitAsync(TimeSpan.FromSeconds(10));

        using var scope = harness.Engine.Services.CreateScope();
        var store = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();
        var entriesField = store.GetType().GetField("_entries", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(entriesField);
        var entries = (IDictionary)entriesField!.GetValue(store)!;

        // The re-attach save races this read, so poll until the redelivered execution has
        // re-extended the TTL past the 30-day wait — on the old code it never does.
        var guard = TimeProvider.System.GetUtcNow() + TimeSpan.FromSeconds(10);
        while (true)
        {
            var entry = entries[run.FlowId];
            Assert.NotNull(entry);
            var expires = (DateTime)entry!.GetType().GetProperty("ExpiresAtUtc")!.GetValue(entry)!;
            var now = harness.Clock.GetUtcNow().UtcDateTime;
            if (expires > now + TimeSpan.FromDays(30))
                break;

            Assert.True(
                TimeProvider.System.GetUtcNow() < guard,
                $"the re-attached ledger still expires in {expires - now}; it must outlive the 30-day step timeout.");
            await Task.Delay(TimeSpan.FromMilliseconds(10));
        }
    }

    [Fact]
    public async Task ReattachShortCircuit_DoesNotResurrectARunFailedByAConcurrentWriter()
    {
        // Regression (r23): TryShortCircuitRecoveredCheckpointAsync adopted the concurrent
        // writer's REVISION without its STATUS. When RecoverAsync checkpointed the step and its
        // escalation then failed the run (Status=Failed), the re-attaching execution adopted the
        // bumped revision, short-circuited, and its next save's compare-and-swap SUCCEEDED —
        // writing Status=Running (and later Succeeded) over the terminal transition. The
        // short-circuit now declines unless the persisted run is still Running, so the stale
        // execution loses the CAS as designed and the run stays Failed.
        ReattachRaceFlow.Gate = ReattachRaceFlow.CompletedSource();
        ReattachRaceFlow.EnteredExecution = ReattachRaceFlow.NewSource();
        var harness = await FlowTestHarness.StartAsync(options =>
        {
            options.Transport = transport => transport.WorkerCount = 1;
            options.ConfigureAsyncResponse = builder => builder.WithDurableFlow<ReattachRaceFlow, R23Input>();
        });
        await using var _ = harness;

        var run = await harness.StartFlowAsync<ReattachRaceFlow, R23Input>(new R23Input(3));
        var cid = await run.WaitForAwaitingStepAsync("s");

        // Kill the parked execution without faulting the breadcrumb: the next delivery re-attaches.
        ReattachRaceFlow.Gate = ReattachRaceFlow.NewSource();
        ReattachRaceFlow.EnteredExecution = ReattachRaceFlow.NewSource();
        await harness.Engine.SimulateRestartAsync();

        // The redelivered execution loads Running state and parks on the gate — the window in
        // which the late response lands elsewhere.
        await run.ResumeAsync();
        await ReattachRaceFlow.EnteredExecution.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // A late response recovers the step (revision bump), and its wake-up escalation then
        // terminally fails the run (second bump) — both lease-bypassing writes, exactly like the
        // production ingress path.
        await harness.Engine.FlowExecutor.RecoverAsync(
            run.FlowId,
            new OperationResult { Status = OperationStatus.Completed, Message = "late response" },
            cid);
        await harness.Engine.FlowExecutor.FailAsync(run.FlowId, new InvalidOperationException("wake-up enqueue kept failing"));

        // Release the gated execution: it re-attaches to the completed checkpoint of a run that
        // is no longer Running.
        ReattachRaceFlow.Gate.TrySetResult();

        // Let the stale attempt resolve (step timeout on the virtual clock) and every queued
        // delivery observe the terminal state.
        await harness.AdvanceAsync(TimeSpan.FromMinutes(1) + TimeSpan.FromSeconds(1));
        await harness.AdvanceAsync(TimeSpan.FromSeconds(30));
        await harness.Engine.WaitForWorkerIdleAsync();

        var state = await run.GetStateAsync();
        Assert.NotNull(state);
        Assert.Equal(FlowRunStatus.Failed, state!.Status);
    }
}
