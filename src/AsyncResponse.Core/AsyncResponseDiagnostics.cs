using System.Diagnostics;

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

    internal static Activity? StartActivity(
        string name,
        ActivityKind kind = ActivityKind.Internal,
        string? correlationId = null)
    {
        var activity = ActivitySource.StartActivity(name, kind);
        SetCorrelationId(activity, correlationId);
        return activity;
    }

    internal static void SetCorrelationId(Activity? activity, string? correlationId)
    {
        if (!string.IsNullOrWhiteSpace(correlationId))
            activity?.SetTag("asyncresponse.correlation_id", correlationId);
    }

    internal static void SetPayloadType(Activity? activity, Type? payloadType)
    {
        if (payloadType is not null)
            activity?.SetTag("asyncresponse.payload_type", payloadType.FullName ?? payloadType.Name);
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
}
