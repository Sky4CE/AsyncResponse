namespace AsyncResponse.IntegrationTests.CrashWorker;

/// <summary>Input of <see cref="CrashSuiteFlow"/>: the scenario name, for the ledger's readability.</summary>
internal sealed record CrashSuiteInput(string Scenario);

/// <summary>
/// The flow the suite crashes. Plain checkpointed steps only — no awaited step, so no response
/// channel is involved and the whole scenario lives in one PostgreSQL database: the queue that
/// delivers the wake-up, the ledger that holds the lease, and the journal the steps write to.
/// It still crosses every boundary a crash can land on: a lease acquisition, checkpoints between
/// local steps, a worker-job publication inside a step, and a child flow (ledger, breadcrumb,
/// publish, suspend, child completion waking the parent).
/// </summary>
internal sealed class CrashSuiteFlow(
    IAsyncResponseBuilder asyncResponse,
    CrashSuiteJournal journal,
    CrashWorkerSettings settings) : IDurableFlow<CrashSuiteInput>
{
    /// <inheritdoc />
    public async Task ExecuteAsync(IDurableFlowContext flow, CrashSuiteInput input)
    {
        var flowId = flow.FlowId;

        await flow.StepAsync(CrashWorkerContract.Steps.First, () => journal.RecordEffectAsync(flowId, CrashWorkerContract.Steps.First));
        await flow.StepAsync(CrashWorkerContract.Steps.Second, () => journal.RecordEffectAsync(flowId, CrashWorkerContract.Steps.Second));

        await flow.StepAsync(CrashWorkerContract.Steps.Publish, async () =>
        {
            await journal.RecordEffectAsync(flowId, CrashWorkerContract.Steps.Publish);
            await asyncResponse.EnqueueWorkerAsync<ICrashSuiteJobs>(jobs => jobs.RecordPublishedJobAsync(flowId));

            // The publish above is committed; the step's checkpoint is not written until this
            // body returns. Dying here leaves a published job the ledger knows nothing about.
            if (settings.IsArmedFor(CrashWorkerContract.CrashPoints.AfterPublishBeforeCheckpoint, flowId))
                ProcessCrash.Die(settings.CrashPoint, flowId);
        });

        await flow.AwaitChildFlowAsync<CrashSuiteChildFlow, CrashSuiteInput>(CrashWorkerContract.Steps.Child, input);

        await flow.StepAsync(CrashWorkerContract.Steps.Last, () => journal.RecordEffectAsync(flowId, CrashWorkerContract.Steps.Last));
    }
}

/// <summary>The child <see cref="CrashSuiteFlow"/> awaits: one journaled local step.</summary>
internal sealed class CrashSuiteChildFlow(CrashSuiteJournal journal) : IDurableFlow<CrashSuiteInput>
{
    /// <inheritdoc />
    public Task ExecuteAsync(IDurableFlowContext flow, CrashSuiteInput input)
        => flow.StepAsync(CrashWorkerContract.Steps.ChildWork, () => journal.RecordEffectAsync(flow.FlowId, CrashWorkerContract.Steps.ChildWork));
}

/// <summary>Target of the worker job <see cref="CrashSuiteFlow"/>'s publish step enqueues.</summary>
internal interface ICrashSuiteJobs
{
    /// <summary>Journals that the published job was delivered and handled.</summary>
    Task RecordPublishedJobAsync(string flowId);
}

/// <inheritdoc cref="ICrashSuiteJobs" />
internal sealed class CrashSuiteJobs(CrashSuiteJournal journal) : ICrashSuiteJobs
{
    /// <inheritdoc />
    public Task RecordPublishedJobAsync(string flowId)
        => journal.RecordEffectAsync(flowId, CrashWorkerContract.Steps.PublishedJob);
}
