using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;

namespace AsyncResponse;

/// <summary>
/// Provides diagnostic identifiers and sources for the AsyncResponse library.
/// </summary>
public static class AsyncResponseDiagnostics
{
    /// <summary>
    /// The name of the OpenTelemetry <see cref="ActivitySource"/> used by AsyncResponse.
    /// Consumers can subscribe to this name to capture distributed traces.
    /// </summary>
    public const string ActivitySourceName = "AsyncResponse";

    /// <summary>
    /// The shared OpenTelemetry <see cref="ActivitySource"/> instance for the AsyncResponse library.
    /// </summary>
    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    /// <summary>
    /// The name of the <see cref="System.Diagnostics.Metrics.Meter"/> used by AsyncResponse.
    /// Consumers subscribe to this name (e.g. via OpenTelemetry) to collect the library's metrics.
    /// </summary>
    public const string MeterName = "AsyncResponse";

    /// <summary>The shared <see cref="System.Diagnostics.Metrics.Meter"/> for the AsyncResponse library.</summary>
    internal static readonly Meter Meter = new(MeterName);

    // The core SLO instrument: every time a response or exception is published with no live waiter,
    // the recovery path is entered. Tags break it down by kind (response/exception), route
    // (resume/failure/unclassified), and whether a recovery callback actually ran — so an operator
    // can graph "how often does recovery fire" and the resume-vs-fail split that this library exists
    // to provide.
    private static readonly Counter<long> LostSubscriberDispatches =
        Meter.CreateCounter<long>("asyncresponse.lost_subscriber.dispatches", unit: "{dispatch}",
            description: "Responses/exceptions published with no live subscriber (the recovery path was entered).");

    private static readonly Counter<long> WaiterTimeoutsCounter =
        Meter.CreateCounter<long>("asyncresponse.waiter.timeouts", unit: "{timeout}",
            description: "Waiters that faulted because no response arrived before the timeout.");

    private static readonly Counter<long> WorkerJobsCounter =
        Meter.CreateCounter<long>("asyncresponse.worker.jobs", unit: "{job}",
            description: "Worker jobs processed, tagged by outcome (executed/failed/rejected).");

    private static readonly Counter<long> TypeResolutionFailures =
        Meter.CreateCounter<long>("asyncresponse.type_resolution.unresolved", unit: "{failure}",
            description: "Persisted service/payload type names that could not be resolved; the callback may silently fail to route.");

    private static int _watchdogGaugesRegistered;

    internal static Activity? StartActivity(
        string name,
        ActivityKind kind = ActivityKind.Internal,
        string? correlationId = null)
    {
        if (!ActivitySource.HasListeners())
            return null;

        var activity = ActivitySource.StartActivity(name, kind);
        if (correlationId is not null)
            SetCorrelationId(activity, correlationId);

        return activity;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void SetCorrelationId(Activity? activity, string? correlationId)
    {
        if (activity is null || string.IsNullOrWhiteSpace(correlationId))
            return;

        activity.SetTag("asyncresponse.correlation_id", correlationId);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void SetPayloadType(Activity? activity, Type? payloadType)
    {
        if (activity is null || payloadType is null)
            return;

        activity.SetTag("asyncresponse.payload_type", payloadType.FullName ?? payloadType.Name);
    }

    internal static void SetReplyTarget(Activity? activity, AsyncResponseReplyTarget? replyTarget)
    {
        if (replyTarget is null)
            return;

        activity?.SetTag("asyncresponse.reply_target.name", replyTarget.Name);
        activity?.SetTag("asyncresponse.reply_target.transport", replyTarget.Transport);
    }

    internal static void SetWorker(Activity? activity, ReflectionCallDto? call)
    {
        if (call is null)
            return;

        activity?.SetTag("asyncresponse.worker.service", call.ServiceInterfaceFullName);
        activity?.SetTag("asyncresponse.worker.method", call.MethodName);
    }

    internal static void SetLostSubscriberRoute(Activity? activity, bool? shouldResume)
        => activity?.SetTag("asyncresponse.lost_subscriber_route", shouldResume switch
        {
            true => "resume",
            false => "failure",
            _ => "unclassified"
        });

    internal static void SetError(Activity? activity, Exception exception)
    {
        activity?.SetTag("error.type", exception.GetType().FullName ?? exception.GetType().Name);
        activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
    }

    internal static void SetError(Activity? activity, string errorType, string? description = null)
    {
        activity?.SetTag("error.type", errorType);
        activity?.SetStatus(ActivityStatusCode.Error, description);
    }

    /// <summary>Records one lost-subscriber dispatch (the recovery path was entered for a publish).</summary>
    internal static void RecordLostSubscriber(string kind, bool? shouldResume, bool callbackInvoked)
    {
        if (!LostSubscriberDispatches.Enabled)
            return;

        var route = shouldResume switch { true => "resume", false => "failure", _ => "unclassified" };
        LostSubscriberDispatches.Add(
            1,
            new KeyValuePair<string, object?>("kind", kind),
            new KeyValuePair<string, object?>("route", route),
            new KeyValuePair<string, object?>("invoked", callbackInvoked));
    }

    /// <summary>Records one waiter timeout on the given channel kind.</summary>
    internal static void RecordWaiterTimeout(string channel)
    {
        if (WaiterTimeoutsCounter.Enabled)
            WaiterTimeoutsCounter.Add(1, new KeyValuePair<string, object?>("channel", channel));
    }

    /// <summary>Records one worker-job outcome (executed/failed/rejected).</summary>
    internal static void RecordWorkerOutcome(string outcome)
    {
        if (WorkerJobsCounter.Enabled)
            WorkerJobsCounter.Add(1, new KeyValuePair<string, object?>("outcome", outcome));
    }

    /// <summary>
    /// Records that a persisted type name (kind = "service" or "payload") could not be resolved, so
    /// operators can correlate a silently-failing recovery callback with a missing/ALC-loaded type.
    /// </summary>
    internal static void RecordTypeResolutionFailure(string kind)
    {
        if (TypeResolutionFailures.Enabled)
            TypeResolutionFailures.Add(1, new KeyValuePair<string, object?>("kind", kind));
    }

    /// <summary>
    /// Registers observable gauges reporting the latest watchdog scan: outstanding recovery
    /// registrations, those with a live waiter, and stale ones (no waiter, past the threshold).
    /// Registered once process-wide; the watchdog is a singleton, and the guard keeps repeated test
    /// constructions from stacking duplicate gauges on the shared meter.
    /// </summary>
    internal static void EnsureWatchdogGauges(AsyncResponseWatchdogState state)
    {
        if (Interlocked.Exchange(ref _watchdogGaugesRegistered, 1) != 0)
            return;

        Meter.CreateObservableGauge("asyncresponse.recovery.outstanding",
            () => (long)(state.Latest?.Report?.TotalEntries ?? 0), unit: "{entry}",
            description: "Outstanding recovery registrations at the last watchdog scan.");
        Meter.CreateObservableGauge("asyncresponse.recovery.active_waiters",
            () => (long)(state.Latest?.Report?.EntriesWithActiveWaiter ?? 0), unit: "{entry}",
            description: "Recovery registrations with a live waiter at the last watchdog scan.");
        Meter.CreateObservableGauge("asyncresponse.recovery.stale",
            () => (long)(state.Latest?.Report?.StaleEntries.Count ?? 0), unit: "{entry}",
            description: "Stale recovery registrations (no live waiter, past the threshold) at the last watchdog scan.");
    }
}
