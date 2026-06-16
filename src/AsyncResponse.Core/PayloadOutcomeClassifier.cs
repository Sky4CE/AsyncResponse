using System.Collections.Concurrent;
using System.Text.Json;

namespace AsyncResponse;

/// <summary>
/// Classifies the domain outcome of an async-response payload for the lost-subscriber fallback.
/// <para>
/// Payloads arriving through a broker ingress are untyped (a raw <see cref="JsonElement"/>), so
/// the payload type the original waiter registered for (persisted in the recovery state) is used
/// to materialize the payload before asking it for its
/// <see cref="IAsyncResponsePayload.ClassifyOutcome"/>.
/// </para>
/// </summary>
internal static class PayloadOutcomeClassifier
{
    private static readonly ConcurrentDictionary<string, Type> PayloadTypes = new(StringComparer.Ordinal);

    /// <summary>
    /// Attempts to classify the domain outcome of <paramref name="payload"/>.
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
    /// The classified outcome, or <c>null</c> when the payload cannot be classified — a
    /// <c>null</c> payload, missing/unresolvable type information, or a conversion failure.
    /// Callers must treat <c>null</c> as "no domain knowledge" and keep the resume routing;
    /// note this is distinct from <see cref="AsyncResponseOutcome.Unknown"/>, which is an
    /// explicit verdict by the payload's own classifier and routes to the failure callback.
    /// </returns>
    public static AsyncResponseOutcome? TryClassify(object? payload, string? payloadTypeFullName)
    {
        try
        {
            // Typed payloads (published directly by in-process services) classify themselves.
            if (payload is IAsyncResponsePayload typedPayload)
            {
                return typedPayload.ClassifyOutcome();
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
                ? materialized.ClassifyOutcome()
                : null;
        }
        catch
        {
            // A payload that cannot be materialized as the registered type carries no usable
            // domain state; fall back to the resume routing. The callback invocation itself
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

        if (resolved is not null)
            PayloadTypes.TryAdd(payloadTypeFullName, resolved);

        return resolved;
    }
}
