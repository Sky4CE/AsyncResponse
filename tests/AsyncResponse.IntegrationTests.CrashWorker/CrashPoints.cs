using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.ExceptionServices;

namespace AsyncResponse.IntegrationTests.CrashWorker;

/// <summary>The abrupt death itself.</summary>
internal static class ProcessCrash
{
    /// <summary>
    /// Kills this process the way an OOM-killer or a pulled plug does: SIGKILL on Unix,
    /// TerminateProcess on Windows. No <c>finally</c> block runs, the host never stops, the
    /// execution lease is not released, the queue claim is not settled, and no connection says
    /// goodbye — deliberately NOT <c>Environment.Exit</c> (runs shutdown handlers) and not
    /// <c>Environment.FailFast</c> (same outcome, but drags in the platform crash reporter).
    /// </summary>
    [DoesNotReturn]
    public static void Die(string crashPoint, string flowId)
    {
        // The marker is the suite's proof that the process died where the scenario says it did,
        // rather than of a startup failure that merely also exits non-zero.
        Console.Out.WriteLine($"{CrashWorkerContract.CrashMarker} {crashPoint} flow={flowId}");
        Console.Out.Flush();

        Process.GetCurrentProcess().Kill();
        Thread.Sleep(Timeout.Infinite);
        throw new UnreachableException();
    }
}

/// <summary>
/// The step-boundary crash points, on the engine's own observer seam. Observers run synchronously on
/// the execution path, and <see cref="OnStepCompletedAsync"/> fires only after the step's checkpoint
/// write returned — so dying here is dying between a persisted checkpoint and the next step.
/// </summary>
internal sealed class CrashPointObserver(CrashWorkerSettings settings) : IDurableFlowExecutionObserver
{
    public ValueTask OnStepCompletedAsync(DurableFlowStepEvent step)
    {
        if (step.StepName == CrashWorkerContract.Steps.First
            && settings.IsArmedFor(CrashWorkerContract.CrashPoints.AfterStepCheckpoint, step.FlowId))
        {
            ProcessCrash.Die(settings.CrashPoint, step.FlowId);
        }

        return default;
    }

    public ValueTask OnStepWaitingAsync(DurableFlowStepEvent step)
    {
        // For a child-flow step this fires after the child ledger was created and the parent's
        // breadcrumb checkpoint was saved, immediately before the parent suspends and publishes
        // the job that executes the child.
        if (step.Kind == DurableFlowStepKind.ChildFlow
            && step.StepName == CrashWorkerContract.Steps.Child
            && settings.IsArmedFor(CrashWorkerContract.CrashPoints.AfterChildCheckpointBeforePublish, step.FlowId))
        {
            ProcessCrash.Die(settings.CrashPoint, step.FlowId);
        }

        return default;
    }
}

/// <summary>
/// Wraps the real flow-state store to journal every execution-lease acquisition and, when armed, to
/// die the instant the store has granted one — after the lease row is committed, before the executor
/// has loaded the ledger, bumped <c>Attempts</c>, or started its renewal loop.
/// <para>
/// A <see cref="DispatchProxy"/> rather than a hand-written decorator, on purpose. A decorator that
/// forgets a member with a default interface implementation still compiles, and silently answers
/// with the DEFAULT instead of the wrapped store's override — for
/// <c>IFlowStateStore.ObserveLeaseAsync</c> that would mean "this store cannot report leases" and
/// change the very takeover behavior this suite exists to pin. The proxy forwards every member the
/// interface has, including ones added after this file was written.
/// </para>
/// </summary>
internal class CrashingFlowStateStore : DispatchProxy
{
    private IFlowStateStore _inner = null!;
    private CrashSuiteJournal _journal = null!;
    private CrashWorkerSettings _settings = null!;

    public static IFlowStateStore Wrap(IFlowStateStore inner, CrashSuiteJournal journal, CrashWorkerSettings settings)
    {
        var proxy = Create<IFlowStateStore, CrashingFlowStateStore>();
        var self = (CrashingFlowStateStore)(object)proxy;
        self._inner = inner;
        self._journal = journal;
        self._settings = settings;
        return proxy;
    }

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
    {
        ArgumentNullException.ThrowIfNull(targetMethod);

        object? result;
        try
        {
            result = targetMethod.Invoke(_inner, args);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            // Synchronous argument validation in the store: rethrow what the caller would have seen.
            ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }

        if (targetMethod.Name == nameof(IFlowStateStore.TryAcquireLeaseAsync)
            && result is Task<bool> acquiring
            && args is [string flowId, ..])
        {
            return ObserveAcquireAsync(acquiring, flowId);
        }

        return result;
    }

    private async Task<bool> ObserveAcquireAsync(Task<bool> acquiring, string flowId)
    {
        var acquired = await acquiring.ConfigureAwait(false);
        await _journal.RecordLeaseAttemptAsync(flowId, acquired).ConfigureAwait(false);

        if (acquired && _settings.IsArmedFor(CrashWorkerContract.CrashPoints.AfterLeaseAcquired, flowId))
            ProcessCrash.Die(_settings.CrashPoint, flowId);

        return acquired;
    }
}
