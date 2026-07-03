using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Threading.Channels;
using Xunit;

namespace AsyncResponse.Tests;

// ---------------------------------------------------------------------------------------------
// A production-shaped pipeline mirroring the flow patterns durable flows were extracted from:
// subset runs, conditional steps, progress-aware awaited steps with different payload types,
// a catch-and-continue step, injected push notifications (SignalR stand-in), memoized values,
// and crash injection between steps.
// ---------------------------------------------------------------------------------------------

public sealed record PipelineInput(
    long TenantId,
    bool TicketOnly = false,
    bool HasAttributeChanges = false,
    bool LineageEnabled = false);

/// <summary>A second awaited-step payload type (Airflow-DAG-shaped), distinct from OperationResult.</summary>
public sealed class DagRunResult : IAsyncResponsePayload
{
    public DagRunState State { get; set; }
    public string? Message { get; set; }

    public bool ShouldResumeOnRecovery() => State != DagRunState.Failed;
}

public enum DagRunState
{
    Queued = 0,
    Running = 1,
    Success = 2,
    Failed = 3
}

/// <summary>SignalR stand-in: any DI service a flow wants to call from steps or until predicates.</summary>
public interface IPipelineNotifier
{
    Task PushAsync(string message);
}

public sealed class CapturingPipelineNotifier : IPipelineNotifier
{
    private readonly List<string> _messages = [];

    public IReadOnlyList<string> Messages
    {
        get { lock (_messages) return [.. _messages]; }
    }

    public Task PushAsync(string message)
    {
        lock (_messages)
            _messages.Add(message);
        return Task.CompletedTask;
    }
}

/// <summary>Cross-execution observation + crash injection + trigger recording for the pipeline.</summary>
public sealed class PipelineProbe
{
    private readonly Dictionary<string, int> _stepRuns = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> _triggerRuns = new(StringComparer.Ordinal);
    private readonly Channel<(string Step, string CorrelationId)> _triggers = Channel.CreateUnbounded<(string, string)>();
    private string? _armedCrash;
    private int _crashFired;

    public ChannelReader<(string Step, string CorrelationId)> Triggers => _triggers.Reader;

    public int StepRuns(string step)
    {
        lock (_stepRuns) return _stepRuns.TryGetValue(step, out var n) ? n : 0;
    }

    public int TriggerRuns(string step)
    {
        lock (_triggerRuns) return _triggerRuns.TryGetValue(step, out var n) ? n : 0;
    }

    public void Bump(string step)
    {
        lock (_stepRuns) _stepRuns[step] = StepRuns(step) + 1;
    }

    public Task RecordTriggerAsync(string step, string correlationId)
    {
        lock (_triggerRuns) _triggerRuns[step] = TriggerRuns(step) + 1;
        _triggers.Writer.TryWrite((step, correlationId));
        return Task.CompletedTask;
    }

    /// <summary>Arms a one-shot crash at the named checkpoint (fires on the next pass only).</summary>
    public void ArmCrash(string checkpoint)
    {
        _armedCrash = checkpoint;
        _crashFired = 0;
    }

    public void MaybeCrashOnce(string checkpoint)
    {
        if (_armedCrash == checkpoint && Interlocked.Exchange(ref _crashFired, 1) == 0)
            throw new InvalidOperationException($"injected crash at {checkpoint}");
    }
}

public sealed class PipelineFlow(PipelineProbe _probe, IPipelineNotifier _notifier) : IDurableFlow<PipelineInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, PipelineInput input)
    {
        // Subset run ("only create the ticket"): plain C#, no pre-marked flags needed.
        await flow.StepAsync("create-ticket", async () =>
        {
            _probe.Bump("create-ticket");
            await _notifier.PushAsync($"{flow.FlowId}: ticket created for tenant {input.TenantId}");
        });
        if (input.TicketOnly)
            return;

        _probe.MaybeCrashOnce("before:pre-script");

        // Awaited step with progress: intermediate responses notify AND persist progress.
        var preScript = await flow.AwaitStepAsync<OperationResult>(
            "run-pre-script",
            trigger: cid => _probe.RecordTriggerAsync("run-pre-script", cid),
            until: async r =>
            {
                if (r.Status == OperationStatus.Running)
                {
                    await _notifier.PushAsync($"pre-script progress: {r.Message}");
                    await flow.ReportProgressAsync($"pre-script: {r.Message}");
                    return false;
                }
                return true;
            },
            timeout: TimeSpan.FromSeconds(10));

        if (preScript.Status == OperationStatus.Failed)
            throw new DurableFlowFailedException($"pre-script failed: {preScript.Message}");

        _probe.MaybeCrashOnce("before:swap");
        await flow.StepAsync("swap", () =>
        {
            _probe.Bump("swap");
            return Task.CompletedTask;
        });

        // Conditional step: driven by input, an ordinary if.
        if (input.HasAttributeChanges)
        {
            _probe.MaybeCrashOnce("before:attributes");
            await flow.AwaitStepAsync<OperationResult>(
                "apply-attributes",
                trigger: cid => _probe.RecordTriggerAsync("apply-attributes", cid),
                timeout: TimeSpan.FromSeconds(10));
        }
        else
        {
            await flow.ReportProgressAsync("Skipping apply-attributes: no attribute changes requested");
        }

        // Catch-and-continue step with a DIFFERENT payload type: a best-effort stage whose
        // failure must not sink the pipeline.
        if (input.LineageEnabled)
        {
            try
            {
                await flow.AwaitStepAsync<DagRunResult>(
                    "run-lineage",
                    trigger: cid => _probe.RecordTriggerAsync("run-lineage", cid),
                    until: r => r.State != DagRunState.Queued && r.State != DagRunState.Running,
                    timeout: TimeSpan.FromMilliseconds(250));
            }
            catch (Exception ex)
            {
                await _notifier.PushAsync($"lineage failed, continuing: {ex.Message}");
                await flow.ReportProgressAsync("run-lineage failed; continuing pipeline");
            }
        }

        var stamp = await flow.StepAsync("compute-final-stamp", () =>
        {
            _probe.Bump("compute-final-stamp");
            return Task.FromResult($"stamp-{Guid.NewGuid():N}");
        });
        await flow.SetValueAsync("final-stamp", stamp);

        _probe.MaybeCrashOnce("after:stamp");

        await flow.StepAsync("finalize", async () =>
        {
            _probe.Bump("finalize");
            await _notifier.PushAsync($"{flow.FlowId}: done, stamp={flow.GetValue<string>("final-stamp")}");
        });
    }
}

public class DurableFlowScenarioTests
{
    private static ServiceProvider CreateProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<PipelineProbe>();
        services.AddSingleton<CapturingPipelineNotifier>();
        services.AddSingleton<IPipelineNotifier>(sp => sp.GetRequiredService<CapturingPipelineNotifier>());
        services.AddScoped<PipelineFlow>();
        services.AddAsyncResponse()
            .WithInMemoryChannel(options =>
            {
                options.DefaultTimeout = TimeSpan.FromSeconds(10);
                options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
            })
            .WithInMemoryTransport();
        return services.BuildServiceProvider();
    }

    /// <summary>Answers awaited-step triggers like the remote systems would.</summary>
    private static Task StartResponder(ServiceProvider provider, CancellationToken token, bool answerLineage = true)
    {
        var probe = provider.GetRequiredService<PipelineProbe>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        return Task.Run(async () =>
        {
            await foreach (var (step, cid) in probe.Triggers.ReadAllAsync(token))
            {
                switch (step)
                {
                    case "run-pre-script":
                        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Running, Message = "50%" }, cid);
                        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "pre-script done" }, cid);
                        break;
                    case "apply-attributes":
                        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "attributes applied" }, cid);
                        break;
                    case "run-lineage" when answerLineage:
                        await publisher.SetResponse(new DagRunResult { State = DagRunState.Running, Message = "dag scheduled" }, cid);
                        await publisher.SetResponse(new DagRunResult { State = DagRunState.Success, Message = "dag done" }, cid);
                        break;
                }
            }
        }, token);
    }

    [Fact]
    public async Task Pipeline_FullRun_CompletesAllSteps_WithProgressAndNotifications()
    {
        await using var provider = CreateProvider();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        _ = StartResponder(provider, cts.Token);

        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var probe = provider.GetRequiredService<PipelineProbe>();
        var notifier = provider.GetRequiredService<CapturingPipelineNotifier>();

        var flowId = await flows.StartAsync<PipelineFlow, PipelineInput>(
            new PipelineInput(7, HasAttributeChanges: true, LineageEnabled: true));
        await executor.ExecuteAsync(flowId).WaitAsync(TimeSpan.FromSeconds(15));

        var state = await flows.GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Succeeded, state!.Status);
        foreach (var step in new[] { "create-ticket", "run-pre-script", "swap", "apply-attributes", "run-lineage", "compute-final-stamp", "finalize" })
            Assert.True(state.Steps![step].Completed, $"step '{step}' should be completed");

        // Every local step and every trigger fired exactly once.
        Assert.Equal(1, probe.StepRuns("create-ticket"));
        Assert.Equal(1, probe.StepRuns("swap"));
        Assert.Equal(1, probe.StepRuns("compute-final-stamp"));
        Assert.Equal(1, probe.StepRuns("finalize"));
        Assert.Equal(1, probe.TriggerRuns("run-pre-script"));
        Assert.Equal(1, probe.TriggerRuns("apply-attributes"));
        Assert.Equal(1, probe.TriggerRuns("run-lineage"));

        // Injected notifier (SignalR stand-in) was reachable from steps AND until predicates.
        Assert.Contains(notifier.Messages, m => m.Contains("ticket created"));
        Assert.Contains(notifier.Messages, m => m.Contains("pre-script progress: 50%"));
        Assert.Contains(notifier.Messages, m => m.Contains("done, stamp=stamp-"));

        // The memoized stamp and the value bag agree.
        Assert.Contains("stamp-", state.Values!["final-stamp"]);
        Assert.Contains("stamp-", state.Steps["compute-final-stamp"].ResultJson);
    }

    [Fact]
    public async Task Pipeline_TicketOnly_RunsSubsetAndSucceeds()
    {
        await using var provider = CreateProvider();
        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var probe = provider.GetRequiredService<PipelineProbe>();

        var flowId = await flows.StartAsync<PipelineFlow, PipelineInput>(new PipelineInput(7, TicketOnly: true));
        await executor.ExecuteAsync(flowId).WaitAsync(TimeSpan.FromSeconds(10));

        var state = await flows.GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Succeeded, state!.Status);
        Assert.True(state.Steps!["create-ticket"].Completed);
        Assert.DoesNotContain("run-pre-script", state.Steps.Keys);
        Assert.DoesNotContain("finalize", state.Steps.Keys);
        Assert.Equal(1, probe.StepRuns("create-ticket"));
        Assert.Equal(0, probe.TriggerRuns("run-pre-script"));
    }

    [Fact]
    public async Task Pipeline_WithoutAttributeChanges_SkipsConditionalStep()
    {
        await using var provider = CreateProvider();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        _ = StartResponder(provider, cts.Token);

        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var probe = provider.GetRequiredService<PipelineProbe>();

        var flowId = await flows.StartAsync<PipelineFlow, PipelineInput>(new PipelineInput(7));
        await executor.ExecuteAsync(flowId).WaitAsync(TimeSpan.FromSeconds(15));

        var state = await flows.GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Succeeded, state!.Status);
        Assert.DoesNotContain("apply-attributes", state.Steps!.Keys);
        Assert.Equal(0, probe.TriggerRuns("apply-attributes"));
    }

    [Fact]
    public async Task Pipeline_LineageTimeout_IsCaughtAndFlowContinues()
    {
        await using var provider = CreateProvider();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        _ = StartResponder(provider, cts.Token, answerLineage: false); // lineage never answers → times out

        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var notifier = provider.GetRequiredService<CapturingPipelineNotifier>();

        var flowId = await flows.StartAsync<PipelineFlow, PipelineInput>(new PipelineInput(7, LineageEnabled: true));
        await executor.ExecuteAsync(flowId).WaitAsync(TimeSpan.FromSeconds(15));

        var state = await flows.GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Succeeded, state!.Status);

        // The best-effort step is recorded as faulted, not completed — and the flow went on.
        Assert.False(state.Steps!["run-lineage"].Completed);
        Assert.True(state.Steps["run-lineage"].Faulted);
        Assert.True(state.Steps["finalize"].Completed);
        Assert.Contains(notifier.Messages, m => m.Contains("lineage failed, continuing"));
    }

    [Theory]
    [InlineData("before:pre-script")]
    [InlineData("before:swap")]
    [InlineData("before:attributes")]
    [InlineData("after:stamp")]
    public async Task Pipeline_CrashAtAnyCheckpoint_ResumesAndRunsEachStepExactlyOnce(string crashPoint)
    {
        await using var provider = CreateProvider();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        _ = StartResponder(provider, cts.Token);

        var flows = provider.GetRequiredService<IDurableFlows>();
        var executor = provider.GetRequiredService<IDurableFlowExecutor>();
        var probe = provider.GetRequiredService<PipelineProbe>();

        probe.ArmCrash(crashPoint);
        var flowId = await flows.StartAsync<PipelineFlow, PipelineInput>(new PipelineInput(7, HasAttributeChanges: true));

        // First execution dies at the checkpoint (retriable → the transport would redeliver).
        await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync(flowId).WaitAsync(TimeSpan.FromSeconds(15)));
        Assert.Equal(FlowRunStatus.Running, (await flows.GetStateAsync(flowId))!.Status);

        // "Redelivery": the same run again, from the top.
        await executor.ExecuteAsync(flowId).WaitAsync(TimeSpan.FromSeconds(15));

        var state = await flows.GetStateAsync(flowId);
        Assert.Equal(FlowRunStatus.Succeeded, state!.Status);
        Assert.Equal(2, state.Attempts);

        // No step ran twice, no trigger fired twice — regardless of where the crash landed.
        Assert.Equal(1, probe.StepRuns("create-ticket"));
        Assert.Equal(1, probe.StepRuns("swap"));
        Assert.Equal(1, probe.StepRuns("compute-final-stamp"));
        Assert.Equal(1, probe.StepRuns("finalize"));
        Assert.Equal(1, probe.TriggerRuns("run-pre-script"));
        Assert.Equal(1, probe.TriggerRuns("apply-attributes"));
    }

    [Fact]
    public async Task Pipeline_ExecutesViaWorkerTransport_EndToEnd()
    {
        await using var provider = CreateProvider();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        _ = StartResponder(provider, cts.Token);

        // Start the hosted services so StartAsync's enqueued worker job actually executes the
        // flow — the same path production takes (start → worker transport → executor).
        var hosted = provider.GetServices<IHostedService>().ToList();
        foreach (var service in hosted)
            await service.StartAsync(CancellationToken.None);

        try
        {
            var flows = provider.GetRequiredService<IDurableFlows>();
            var flowId = await flows.StartAsync<PipelineFlow, PipelineInput>(
                new PipelineInput(7, HasAttributeChanges: true));

            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
            FlowState? state;
            do
            {
                state = await flows.GetStateAsync(flowId);
                if (state?.Status == FlowRunStatus.Succeeded)
                    break;
                await Task.Delay(25);
            } while (DateTime.UtcNow < deadline);

            Assert.Equal(FlowRunStatus.Succeeded, state!.Status);
            Assert.True(state.Steps!["finalize"].Completed);
        }
        finally
        {
            foreach (var service in hosted)
                await service.StopAsync(CancellationToken.None);
        }
    }
}
