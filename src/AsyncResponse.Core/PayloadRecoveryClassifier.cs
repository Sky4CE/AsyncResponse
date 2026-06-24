using System.Collections.Concurrent;
using System.Text.Json;

namespace AsyncResponse;

/// <summary>
/// Resolves the lost-subscriber recovery route for a response payload by asking it
/// <see cref="IAsyncResponsePayload.ShouldResumeOnRecovery"/>.
/// <para>
/// Payloads arriving through a broker ingress are untyped (a raw <see cref="JsonElement"/> / JSON
/// string), so the payload type the original waiter registered for (persisted in the recovery
/// state) is used to materialize the payload before asking it.
/// </para>
/// </summary>
internal static class PayloadRecoveryClassifier
{
    private static readonly ConcurrentDictionary<string, Type> PayloadTypes = new(StringComparer.Ordinal);

    /// <summary>
    /// Attempts to decide whether <paramref name="payload"/> should resume the flow on the
    /// lost-subscriber path.
    /// </summary>
    /// <param name="payload">
    /// The payload as received by <c>SetResponse</c>: either an already-typed
    /// <see cref="IAsyncResponsePayload"/>, or raw JSON (<see cref="JsonElement"/> / JSON string)
    /// when the response came through a broker ingress.
    /// </param>
    /// <param name="payloadTypeFullName">
    /// Full name of the payload type the waiter subscribed for, from the recovery state.
    /// </param>
    /// <returns>
    /// <c>true</c> to resume, <c>false</c> to fail, or <c>null</c> when the payload cannot be
    /// classified — a <c>null</c> payload, missing/unresolvable type information, or a conversion
    /// failure. Callers must treat <c>null</c> conservatively as "do not resume", so a payload that
    /// cannot be understood never takes the happy path.
    /// </returns>
    public static bool? ShouldResume(object? payload, string? payloadTypeFullName)
    {
        try
        {
            // Typed payloads (published directly by in-process services) answer for themselves.
            if (payload is IAsyncResponsePayload typedPayload)
            {
                return typedPayload.ShouldResumeOnRecovery();
            }

            if (payload is null || string.IsNullOrWhiteSpace(payloadTypeFullName))
            {
                return null;
            }

            var payloadType = ResolvePayloadType(payloadTypeFullName!);
            if (payloadType is null || !typeof(IAsyncResponsePayload).IsAssignableFrom(payloadType))
            {
                return null;
            }

            return payload.ConvertTo(payloadType) is IAsyncResponsePayload materialized
                ? materialized.ShouldResumeOnRecovery()
                : null;
        }
        catch
        {
            // A payload that cannot be materialized as the registered type carries no usable domain
            // state; treat it conservatively as "do not resume". The failure callback invocation
            // performs the same conversion and surfaces the error through the existing path.
            return null;
        }
    }

    private static Type? ResolvePayloadType(string payloadTypeFullName)
    {
        if (PayloadTypes.TryGetValue(payloadTypeFullName, out var cached))
            return cached;

        var resolved = AppDomain.CurrentDomain
            .GetAssemblies()
            .Select(a => a.GetType(payloadTypeFullName, throwOnError: false))
            .FirstOrDefault(t => t != null);

        // Opt-in fallback for payload types loaded into a non-default AssemblyLoadContext (plugins).
        resolved ??= AsyncResponseTypeResolution.Resolve(payloadTypeFullName);

        if (resolved is not null)
        {
            PayloadTypes.TryAdd(payloadTypeFullName, resolved);
        }
        else
        {
            // Surface the silent "couldn't materialize the payload type" path so operators can
            // correlate a recovery that routed to failure with a missing/ALC-loaded type.
            AsyncResponseDiagnostics.RecordTypeResolutionFailure("payload");
        }

        return resolved;
    }
}
