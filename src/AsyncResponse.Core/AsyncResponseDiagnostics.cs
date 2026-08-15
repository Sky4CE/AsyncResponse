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

    private static readonly Counter<long> UnroutableResponsesCounter =
        Meter.CreateCounter<long>("asyncresponse.ingress.unroutable_responses", unit: "{message}",
            description: "Inbound response messages acknowledged without routing because they carry no correlation id (deliberate poison guard — redelivery could never route them).");

    private static int _watchdogGaugesRegistered;
    private static AsyncResponseWatchdogState? _watchdogState;

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

    internal static void SetLostSubscriberRoute(Activity? activity, RecoveryAction? action, bool mixed = false)
        => activity?.SetTag("asyncresponse.lost_subscriber_route", LostSubscriberRouteName(action, mixed));

    private static string LostSubscriberRouteName(RecoveryAction? action, bool mixed) => mixed
        ? "mixed"
        : action switch
        {
            RecoveryAction.Resume => "resume",
            RecoveryAction.Fail => "failure",
            RecoveryAction.KeepWaiting => "keep_waiting",
            _ => "unclassified"
        };

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
    internal static void RecordLostSubscriber(string kind, RecoveryAction? action, bool callbackInvoked, bool mixed = false)
    {
        if (!LostSubscriberDispatches.Enabled)
            return;

        LostSubscriberDispatches.Add(
            1,
            new KeyValuePair<string, object?>("kind", kind),
            new KeyValuePair<string, object?>("route", LostSubscriberRouteName(action, mixed)),
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
    /// Records an inbound response acknowledged without routing because it carried no correlation
    /// id — every occurrence is a producer-side contract violation worth alerting on.
    /// </summary>
    internal static void RecordUnroutableResponse()
    {
        if (UnroutableResponsesCounter.Enabled)
            UnroutableResponsesCounter.Add(1);
    }

    /// <summary>
    /// Registers observable gauges reporting the latest watchdog scan: outstanding recovery
    /// registrations, those with a live waiter, and stale ones (no waiter, past the threshold).
    /// Registered once process-wide; the watchdog is a singleton, and the guard keeps repeated test
    /// constructions from stacking duplicate gauges on the shared meter.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    internal static void EnsureWatchdogGauges(AsyncResponseWatchdogState state)
    {
        // The NEWEST watchdog's state wins: the gauges are registered once process-wide on the
        // static meter, so capturing the first state instance in the gauge callbacks would pin a
        // disposed host's last snapshot forever in any process that builds more than one host
        // (test harnesses, WebApplicationFactory, host-per-tenant workers). The callbacks read
        // this holder instead, which every newly constructed watchdog state re-publishes.
        Volatile.Write(ref _watchdogState, state);

        if (Interlocked.Exchange(ref _watchdogGaugesRegistered, 1) != 0)
            return;

        Meter.CreateObservableGauge("asyncresponse.recovery.outstanding",
            static () => (long)(Volatile.Read(ref _watchdogState)?.Latest?.Report?.TotalEntries ?? 0), unit: "{entry}",
            description: "Outstanding recovery registrations at the last watchdog scan.");
        Meter.CreateObservableGauge("asyncresponse.recovery.active_waiters",
            static () => (long)(Volatile.Read(ref _watchdogState)?.Latest?.Report?.EntriesWithActiveWaiter ?? 0), unit: "{entry}",
            description: "Recovery registrations with a live waiter at the last watchdog scan.");
        Meter.CreateObservableGauge("asyncresponse.recovery.stale",
            static () => (long)(Volatile.Read(ref _watchdogState)?.Latest?.Report?.StaleEntries.Count ?? 0), unit: "{entry}",
            description: "Stale recovery registrations (no live waiter, past the threshold) at the last watchdog scan.");
        Meter.CreateObservableGauge("asyncresponse.recovery.scan_truncated",
            static () => Volatile.Read(ref _watchdogState)?.Latest?.Report?.Truncated == true ? 1L : 0L, unit: "{scan}",
            description: "1 when the last watchdog scan stopped at the MaxScanEntries buffer cap — outstanding/stale then describe the buffered subset only.");
    }
}
