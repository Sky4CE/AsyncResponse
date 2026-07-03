namespace AsyncResponse.Sample;

/// <summary>Input for the durable provisioning flow demo (persisted with the flow state).</summary>
public sealed record ProvisioningFlowInput(string Name, bool IncludeAudit = true, bool FailAtImport = false);

/// <summary>
/// The durable-flows showcase: the same remote systems as the other scenarios
/// (<see cref="RemoteWorkSimulator"/> publishing progress + terminal responses through the
/// ingress), orchestrated as one checkpointed flow — a memoized local step, two awaited remote
/// steps (one progress-aware, one whose domain outcome can terminally fail the run), a
/// conditional step, the persisted value bag, and a final step that records to
/// <see cref="FlowRecorder"/> so integration tests (and the dashboard) can observe completion.
/// </summary>
public sealed class SampleProvisioningFlow(
    RemoteWorkSimulator _remote,
    FlowRecorder _recorder) : IDurableFlow<ProvisioningFlowInput>
{
    public async Task ExecuteAsync(IDurableFlowContext flow, ProvisioningFlowInput input)
    {
        // Local step with a memoized result: recomputed never, even across crash-resumes.
        var workspace = await flow.StepAsync("create-workspace", () =>
        {
            _recorder.Record(
                $"flow-step:{flow.FlowId}:create-workspace",
                new FlowCall("flow-step", flow.FlowId, SampleTraceContext.Current, SampleTenantContext.Current, null, "create-workspace"));
            return Task.FromResult($"ws-{input.Name}");
        });

        // Awaited remote step with progress: the simulator publishes Running then Completed.
        await flow.AwaitStepAsync<OperationResult>(
            "run-migration",
            trigger: correlationId =>
            {
                _remote.Start(correlationId, RemoteBehavior.Succeed);
                return Task.CompletedTask;
            },
            until: async result =>
            {
                if (result.Status == OperationStatus.Running)
                {
                    await flow.ReportProgressAsync($"migration: {result.Message}");
                    return false;
                }
                return true;
            });

        // Awaited remote step whose domain outcome the input controls: a Failed payload is a
        // valid response for the live waiter — the flow decides it is terminal.
        var import = await flow.AwaitStepAsync<OperationResult>(
            "import-data",
            trigger: correlationId =>
            {
                _remote.Start(correlationId, input.FailAtImport ? RemoteBehavior.FailDomain : RemoteBehavior.Succeed);
                return Task.CompletedTask;
            },
            until: result => result.Status != OperationStatus.Running);

        if (import.Status == OperationStatus.Failed)
            throw new DurableFlowFailedException($"import failed: {import.Message}");

        // Conditional step: plain C#.
        if (input.IncludeAudit)
        {
            await flow.StepAsync("audit", () =>
            {
                _recorder.Record(
                    $"flow-step:{flow.FlowId}:audit",
                    new FlowCall("flow-step", flow.FlowId, SampleTraceContext.Current, SampleTenantContext.Current, null, "audit"));
                return Task.CompletedTask;
            });
        }

        await flow.SetValueAsync("workspace", workspace);

        await flow.StepAsync("notify", () =>
        {
            _recorder.Record(
                $"flow-done:{flow.FlowId}",
                new FlowCall("flow-done", flow.FlowId, SampleTraceContext.Current, SampleTenantContext.Current, OperationStatus.Completed, workspace));
            return Task.CompletedTask;
        });
    }
}
