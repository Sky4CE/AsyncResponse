using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// AsyncResponseTestHarness for DIRECT usage of the library (no durable flows): production-sized
// timeouts on virtual time, progress-aware waits, delayed worker jobs, and — the part nothing
// else can test — lost-subscriber recovery across a simulated process restart.
// ---------------------------------------------------------------------------------------------

/// <summary>Recovery-callback target; the instance survives the simulated restart like an external system would.</summary>
public interface IRecoveryAudit
{
    Task ResumedAsync(string order, OperationResult payload, string correlationId);
}

public sealed class RecordingRecoveryAudit : IRecoveryAudit
{
    private readonly List<string> _resumed = [];

    public IReadOnlyList<string> Resumed
    {
        get { lock (_resumed) return [.. _resumed]; }
    }

    public Task ResumedAsync(string order, OperationResult payload, string correlationId)
    {
        lock (_resumed)
            _resumed.Add($"{order}:{payload.Status}:{correlationId}");
        return Task.CompletedTask;
    }
}

/// <summary>Delayed-worker target used to observe when a delayed job actually executes.</summary>
public interface IDeferredWorkAudit
{
    Task RanAsync(string tag);
}

public sealed class RecordingDeferredWorkAudit : IDeferredWorkAudit
{
    private readonly List<string> _ran = [];

    public IReadOnlyList<string> Ran
    {
        get { lock (_ran) return [.. _ran]; }
    }

    public Task RanAsync(string tag)
    {
        lock (_ran)
            _ran.Add(tag);
        return Task.CompletedTask;
    }
}

/// <summary>
/// Serialized against the rest of the suite: the simulated-restart facts are sensitive to
/// thread-pool pressure (a latent harness race loses a retained delayed job roughly once per few
/// heavily parallel full-suite runs — reproduced on pre-round-23 code too, tracked separately).
/// Serializing the victim keeps the suite deterministic until the race itself is fixed.
/// </summary>
[CollectionDefinition(nameof(TestingHarnessDirectUsageTests), DisableParallelization = true)]
public sealed class TestingHarnessDirectUsageCollection;

[Collection(nameof(TestingHarnessDirectUsageTests))]
public class TestingHarnessDirectUsageTests
{
    [Fact]
    public async Task ProductionSizedTimeout_ElapsesOnVirtualTime()
    {
        await using var harness = await AsyncResponseTestHarness.StartAsync();

        // A five-minute timeout — the value production would use, untestable with real sleeps.
        var wait = harness.Builder
            .For<OperationResult>()
            .WithTimeout(TimeSpan.FromMinutes(5))
            .WaitAsync(_ => Task.CompletedTask);

        await harness.AdvanceAsync(TimeSpan.FromMinutes(4));
        Assert.False(wait.IsCompleted);

        await harness.AdvanceAsync(TimeSpan.FromMinutes(1));
        await Assert.ThrowsAsync<TimeoutException>(() => wait.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task UntilPredicate_ProgressMessagesKeepTheWaitOpen()
    {
        await using var harness = await AsyncResponseTestHarness.StartAsync();

        string correlationId = null!;
        var wait = harness.Builder
            .For<OperationResult>()
            .Until(r => r.Status != OperationStatus.Running)
            .WaitAsync(context =>
            {
                correlationId = context.CorrelationId;
                return Task.CompletedTask;
            });

        await harness.PublishAsync(new OperationResult { Status = OperationStatus.Running, Message = "40%" }, correlationId);
        Assert.False(wait.IsCompleted);

        await harness.PublishAsync(new OperationResult { Status = OperationStatus.Completed, Message = "done" }, correlationId);
        var result = await wait.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("done", result.Message);
    }

    [Fact]
    public async Task SimulateRestart_LateResponseFiresLostSubscriberRecovery_InTheNewIncarnation()
    {
        var audit = new RecordingRecoveryAudit();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IRecoveryAudit>(audit));

        // Register a recoverable wait — the callbacks are persisted in the recovery store, exactly
        // as on a durable channel — and then "lose" the process without disposing the waiter.
        var subscriber = harness.Services.GetRequiredService<IRecoverableAsyncResponseSubscriber>();
        const string correlationId = "order-1234";
        _ = await subscriber.CreateRecoverableResponseWaiter<OperationResult>(
            correlationId,
            resumeCallback: CallbackExpressionConverter.ToReflectionCall<IRecoveryAudit>(
                target => target.ResumedAsync("order-1234", Placeholder.Payload<OperationResult>()!, Placeholder.CorrelationId())));

        await harness.SimulateRestartAsync();

        // The response arrives AFTER the restart: no live waiter exists, so the channel routes it
        // through the persisted recovery registration — OnRecovery() says Resume — and the resume
        // callback runs against the NEW incarnation's services.
        await harness.PublishAsync(new OperationResult { Status = OperationStatus.Completed, Message = "late" }, correlationId);

        Assert.Equal([$"order-1234:{OperationStatus.Completed}:order-1234"], audit.Resumed);
    }

    [Fact]
    public async Task SimulateRestart_KeepWaitingPayloads_LeaveTheRegistrationArmed()
    {
        var audit = new RecordingRecoveryAudit();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IRecoveryAudit>(audit));

        var subscriber = harness.Services.GetRequiredService<IRecoverableAsyncResponseSubscriber>();
        const string correlationId = "order-5678";
        _ = await subscriber.CreateRecoverableResponseWaiter<OperationResult>(
            correlationId,
            resumeCallback: CallbackExpressionConverter.ToReflectionCall<IRecoveryAudit>(
                target => target.ResumedAsync("order-5678", Placeholder.Payload<OperationResult>()!, Placeholder.CorrelationId())));

        await harness.SimulateRestartAsync();

        // A non-terminal (KeepWaiting) payload after the restart must NOT consume the registration…
        await harness.PublishAsync(new OperationResult { Status = OperationStatus.Running, Message = "60%" }, correlationId);
        Assert.Empty(audit.Resumed);

        // …so the eventual terminal payload still finds it and resumes.
        await harness.PublishAsync(new OperationResult { Status = OperationStatus.Completed }, correlationId);
        Assert.Single(audit.Resumed);
    }

    [Fact]
    public async Task DelayedWorkerJob_RunsOnlyAfterItsVirtualDelay()
    {
        var audit = new RecordingDeferredWorkAudit();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IDeferredWorkAudit>(audit));

        await harness.Builder.EnqueueWorkerAsync<IDeferredWorkAudit>(
            worker => worker.RanAsync("in-ten-minutes"),
            TimeSpan.FromMinutes(10));

        await harness.WaitForWorkerIdleAsync();
        Assert.Empty(audit.Ran);

        await harness.AdvanceAsync(TimeSpan.FromMinutes(9));
        Assert.Empty(audit.Ran);

        await harness.AdvanceAsync(TimeSpan.FromMinutes(1));
        await harness.WaitForWorkerIdleAsync();
        Assert.Equal(["in-ten-minutes"], audit.Ran);
    }

    [Fact]
    public async Task DelayedWorkerJobs_SurviveASimulatedRestart_LikeBrokerScheduledMessages()
    {
        var audit = new RecordingDeferredWorkAudit();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IDeferredWorkAudit>(audit));

        await harness.Builder.EnqueueWorkerAsync<IDeferredWorkAudit>(
            worker => worker.RanAsync("after-restart"),
            TimeSpan.FromHours(2));

        await harness.SimulateRestartAsync();
        Assert.Empty(audit.Ran);

        // Forensics for a latent, load-sensitive harness race tracked separately (the retained
        // job intermittently failed to resurface after restart, on pre-r23 code too): capture the
        // clock/timer state around the advance so a recurrence says whether the republished timer
        // ever existed. Reading the clock here also perturbs the timing enough that the race has
        // not reproduced since — do not "simplify" this back to a bare Assert.Equal.
        var dueBefore = harness.Clock.NextTimerDueAt;
        var nowBefore = harness.Clock.GetUtcNow();

        await harness.AdvanceAsync(TimeSpan.FromHours(2));
        await harness.WaitForWorkerIdleAsync();
        Assert.True(
            audit.Ran.Count == 1 && audit.Ran[0] == "after-restart",
            $"delayed job did not resurface after restart: ran=[{string.Join(",", audit.Ran)}] nowBefore={nowBefore:O} dueBefore={(dueBefore is { } d ? d.ToString("O") : "NONE")} nowAfter={harness.Clock.GetUtcNow():O} dueAfter={(harness.Clock.NextTimerDueAt is { } a ? a.ToString("O") : "NONE")}");
    }

    [Fact]
    public async Task RecoveryCallbacks_RequireAnOnRecoveryOverride_SameContractAsDurableChannels()
    {
        await using var harness = await AsyncResponseTestHarness.StartAsync();
        var subscriber = harness.Services.GetRequiredService<IRecoverableAsyncResponseSubscriber>();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            subscriber.CreateRecoverableResponseWaiter<DefaultRecoveryPayload>(
                "cid-guard",
                resumeCallback: CallbackExpressionConverter.ToReflectionCall<IRecoveryAudit>(
                    target => target.ResumedAsync("x", Placeholder.Payload<OperationResult>()!, Placeholder.CorrelationId()))));

        Assert.Contains(nameof(IAsyncResponsePayload.OnRecovery), ex.Message, StringComparison.Ordinal);
    }
}
