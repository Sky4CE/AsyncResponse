using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace AsyncResponse;

/// <summary>
/// The one place the portable correlation-id contract
/// (<see cref="AsyncResponseChannelOptions.CorrelationIdNotPortable"/>) is applied to a public
/// string, so that every channel enforces the same rule at the same boundary. An id that violates
/// it is not a cosmetic problem: it is truncated or rejected at its first relational write, and a
/// space-padded one is the SAME key as its trimmed form to a database while the library compares
/// ids ordinally — a response stored under it can surface at another waiter.
/// <para>
/// The two sides of a channel answer differently on purpose, matching what each caller can do
/// about it. Subscribing takes the id from the application, so a violation is an argument error and
/// throws. Publishing may be driven by an inbound broker message, where throwing turns one bad id
/// into an endless redelivery loop — so it takes the route blank ids already take: log loudly,
/// acknowledge, and never write the row.
/// </para>
/// </summary>
internal static class CorrelationIdGuard
{
    /// <summary>
    /// Validates an id the application supplied for a WAIT, before any subscription or
    /// recovery-state side effect exists to leak.
    /// </summary>
    internal static void ThrowIfUnusable([NotNull] string? correlationId)
    {
        if (string.IsNullOrWhiteSpace(correlationId))
            throw new ArgumentNullException(nameof(correlationId), "CorrelationId must not be empty or whitespace.");

        if (AsyncResponseChannelOptions.CorrelationIdNotPortable(correlationId) is { } rejection)
            throw new ArgumentException(rejection, nameof(correlationId));
    }

    /// <summary>
    /// Reports whether a PUBLISH must be abandoned because its correlation id cannot route,
    /// having already logged the reason and marked <paramref name="activity"/>. Returns
    /// <c>false</c> — and narrows <paramref name="correlationId"/> to non-null — when the publish
    /// may proceed.
    /// </summary>
    /// <param name="correlationId">The id the publish was addressed to.</param>
    /// <param name="logger">The publishing channel's logger.</param>
    /// <param name="activity">The publish activity, marked with the failure when there is one.</param>
    /// <param name="what">What is being dropped, for the log message: "the response", "the exception", …</param>
    /// <param name="dropped">
    /// The exception a <c>SetException</c> publish was carrying; included in the log so the
    /// technical failure it described is not lost along with its unroutable id.
    /// </param>
    internal static bool IsUnroutable(
        [NotNullWhen(false)] string? correlationId,
        ILogger logger,
        Activity? activity,
        string what,
        Exception? dropped = null)
    {
        if (string.IsNullOrWhiteSpace(correlationId))
        {
            if (dropped is null)
                logger.LogWarning("CorrelationId is null; cannot publish {What}.", what);
            else
                logger.LogWarning("CorrelationId is null; cannot publish {What}. Exception: {ExceptionMessage}", what, dropped.Message);

            AsyncResponseDiagnostics.SetError(activity, "correlation_id_null", $"CorrelationId is null; cannot publish {what}.");
            return true;
        }

        if (AsyncResponseChannelOptions.CorrelationIdNotPortable(correlationId) is not { } rejection)
            return false;

        // Error, not warning: unlike a missing id this one looks routable, so it would otherwise
        // fail much later — at the storage write, or worse, at somebody else's waiter.
        if (dropped is null)
            logger.LogError("Cannot publish {What}; the correlation id is outside the portable contract. {Rejection}", what, rejection);
        else
            logger.LogError("Cannot publish {What}; the correlation id is outside the portable contract. {Rejection} Exception: {ExceptionMessage}", what, rejection, dropped.Message);

        AsyncResponseDiagnostics.SetError(activity, "correlation_id_not_portable", rejection);
        return true;
    }
}
