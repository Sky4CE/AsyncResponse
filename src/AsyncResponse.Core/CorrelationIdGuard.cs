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
/// Two failures, told apart because callers can do different things about them. A BLANK id carries
/// no information to act on, so it is logged and skipped wherever it appears — that has always been
/// the behaviour and an inbound message with no id can only be dropped. A NON-BLANK id that breaks
/// the contract is a caller bug: the library throws it back at every public entry point, because
/// swallowing it turns a typo into a waiter that simply times out much later, with nothing at the
/// call site to explain why. Only the untrusted edge — <see cref="IAsyncResponseIngress"/> and the
/// raw publish path it drives — downgrades that to a drop, since a broker message that throws comes
/// straight back around on redelivery, forever.
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
    /// Classifies an id without acting on it: the shared answer behind every caller below, and
    /// behind the ingress's own drop decision.
    /// </summary>
    internal static bool IsUnroutable([NotNullWhen(false)] string? correlationId, out UnroutableReason reason)
    {
        if (string.IsNullOrWhiteSpace(correlationId))
        {
            reason = new("correlation_id_null", "no correlation id", ContractViolation: false);
            return true;
        }

        if (AsyncResponseChannelOptions.CorrelationIdNotPortable(correlationId) is { } rejection)
        {
            reason = new("correlation_id_not_portable", rejection, ContractViolation: true);
            return true;
        }

        reason = default;
        return false;
    }

    /// <summary>
    /// Guards a PUBLISH. Returns <c>false</c> — narrowing <paramref name="correlationId"/> to
    /// non-null — when the publish may proceed, and <c>true</c> when it must be abandoned, having
    /// already logged the reason and marked <paramref name="activity"/>. A non-blank id that breaks
    /// the contract throws instead, unless <paramref name="dropContractViolations"/> says this
    /// caller is the untrusted edge.
    /// </summary>
    /// <param name="correlationId">The id the publish was addressed to.</param>
    /// <param name="logger">The publishing channel's logger.</param>
    /// <param name="activity">The publish activity, marked with the failure when there is one.</param>
    /// <param name="what">What is being dropped, for the log message: "the response", "the exception", …</param>
    /// <param name="dropped">
    /// The exception a <c>SetException</c> publish was carrying; included in the log so the
    /// technical failure it described is not lost along with its unroutable id.
    /// </param>
    /// <param name="dropContractViolations">
    /// <c>true</c> only on the raw publish path, which exists to serve inbound broker messages:
    /// throwing there would fail the delivery, and the transport would redeliver an id that can
    /// never become valid. The ingress rejects such ids before this point; this is the backstop.
    /// </param>
    internal static bool IsUnpublishable(
        [NotNullWhen(false)] string? correlationId,
        ILogger logger,
        Activity? activity,
        string what,
        Exception? dropped = null,
        bool dropContractViolations = false)
    {
        if (!IsUnroutable(correlationId, out var reason))
            return false;

        if (reason.ContractViolation && !dropContractViolations)
            throw new ArgumentException(reason.Description, nameof(correlationId));

        if (reason.ContractViolation)
        {
            // Error, not warning: unlike a missing id this one looks routable, so left alone it
            // would fail much later — at the storage write, or worse, at somebody else's waiter.
            if (dropped is null)
                logger.LogError("Cannot publish {What}; the correlation id is outside the portable contract. {Rejection}", what, reason.Description);
            else
                logger.LogError("Cannot publish {What}; the correlation id is outside the portable contract. {Rejection} Exception: {ExceptionMessage}", what, reason.Description, dropped.Message);
        }
        else if (dropped is null)
        {
            logger.LogWarning("CorrelationId is null; cannot publish {What}.", what);
        }
        else
        {
            logger.LogWarning("CorrelationId is null; cannot publish {What}. Exception: {ExceptionMessage}", what, dropped.Message);
        }

        AsyncResponseDiagnostics.SetError(activity, reason.ErrorType, $"Cannot publish {what}: {reason.Description}.");
        return true;
    }

    /// <summary>
    /// Why an id cannot route: the span's <c>error.type</c> tag, the human-readable phrase, and
    /// whether it is a caller's contract violation (throwable) rather than a missing id.
    /// </summary>
    internal readonly record struct UnroutableReason(string ErrorType, string Description, bool ContractViolation);
}
