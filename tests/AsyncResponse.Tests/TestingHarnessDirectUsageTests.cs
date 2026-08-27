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
    public async Task SimulateRestart_AbandonsThePreRestartWaiter_AndLeavesItsRegistrationRecoverable()
    {
        // Regression (round 29): the in-memory channel arms its waiter timeouts on the injected
        // TimeProvider, and the harness shares that clock across incarnations. With no abandon hook
        // the DEAD incarnation's timers stayed armed on the shared clock: advancing time fired
        // them, completed a ResponseTask a real crash leaves hanging forever, and — worse — ran the
        // cleanup that DELETES the registration from the shared recovery store, so the late
        // response this scenario exists to test found no waiter AND no registration.
        var audit = new RecordingRecoveryAudit();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IRecoveryAudit>(audit));

        var subscriber = harness.Services.GetRequiredService<IRecoverableAsyncResponseSubscriber>();
        const string correlationId = "order-abandoned";
        var waiter = await subscriber.CreateRecoverableResponseWaiter<OperationResult>(
            correlationId,
            resumeCallback: CallbackExpressionConverter.ToReflectionCall<IRecoveryAudit>(
                target => target.ResumedAsync("order-abandoned", Placeholder.Payload<OperationResult>()!, Placeholder.CorrelationId())),
            timeout: TimeSpan.FromMinutes(5));

        await harness.SimulateRestartAsync();

        // Past the dead waiter's five-minute timeout on the SHARED clock.
        await harness.AdvanceAsync(TimeSpan.FromMinutes(6));

        // A crashed process leaves its waiter unresolvable — never "timed out", never completed.
        Assert.True(waiter.ResponseTask.IsCanceled, "the dead incarnation's waiter was settled by a timer that outlived its process");

        // And the registration is still there, so the late response routes through recovery.
        await harness.PublishAsync(new OperationResult { Status = OperationStatus.Completed }, correlationId);
        Assert.Equal([$"order-abandoned:{OperationStatus.Completed}:{correlationId}"], audit.Resumed);
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

        await harness.AdvanceAsync(TimeSpan.FromHours(2));
        await harness.WaitForWorkerIdleAsync();
        Assert.Equal(["after-restart"], audit.Ran);
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
