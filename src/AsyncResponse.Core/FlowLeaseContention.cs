using System.Security.Cryptography;
using System.Text;

namespace AsyncResponse;

/// <summary>What a contended wake-up may conclude from two observations of the lease in its way.</summary>
internal enum FlowLeaseContentionVerdict
{
    /// <summary>
    /// The lease has not changed since the baseline: no proof of a live holder. A dead holder's
    /// lease reads exactly like this, so the wake-up keeps waiting (to the persisted expiry).
    /// </summary>
    KeepWaiting,

    /// <summary>
    /// A live worker acquired or renewed the lease while this wake-up waited, and that execution
    /// is driven by a DIFFERENT job (or one of the two carries no job identity). The holder's own
    /// job is still unacknowledged at the broker, so this delivery is redundant and safe to ack.
    /// </summary>
    AcknowledgeDuplicate,

    /// <summary>
    /// A live worker holds the lease — and the job driving it is THIS delivery's job. The broker
    /// redelivered a job whose handler is still running (an in-flight ceiling lapsed), so this
    /// delivery is the only copy of the run's wake-up the broker still has: never a duplicate.
    /// </summary>
    HolderOwnJobRedelivered
}

/// <summary>
/// The job identity recorded with a durable-flow execution lease, and the decision a contended
/// wake-up takes from it. Pure, so the rule is testable without a store, a clock, or a transport.
/// <para>
/// The identity rides INSIDE the lease id — <c>{guid:N}.{tag}</c> — because that is the one value
/// every store already writes atomically with the acquire and reports back through
/// <see cref="IFlowStateStore.ObserveLeaseAsync"/>: no store, schema, or wire change. The tag is a
/// fixed-width digest of <see cref="WorkerJobEnvelope.JobId"/> rather than the id itself, since a
/// job id is a wire value a foreign producer controls and the relational stores keep the lease id
/// in a 64-character column (32 + 1 + <see cref="JobTagLength"/> = 55).
/// </para>
/// </summary>
internal static class FlowLeaseContention
{
    /// <summary>Characters of base64url(SHA-256) kept as the tag: 132 bits.</summary>
    internal const int JobTagLength = 22;

    private const int GuidLength = 32;
    private const char Separator = '.';

    /// <summary>The lease tag for <paramref name="jobId"/>, or <c>null</c> for a job without an identity.</summary>
    public static string? JobTag(string? jobId)
    {
        if (string.IsNullOrEmpty(jobId))
            return null;

        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Encoding.UTF8.GetBytes(jobId), digest);

        // 32 bytes encode to 44 base64 characters (padding included); the tag is the first 22,
        // rewritten to the URL-safe alphabet so it is inert in every store's id column.
        Span<char> encoded = stackalloc char[44];
        Convert.TryToBase64Chars(digest, encoded, out _);
        for (var i = 0; i < JobTagLength; i++)
        {
            encoded[i] = encoded[i] switch
            {
                '+' => '-',
                '/' => '_',
                var other => other
            };
        }

        return new string(encoded[..JobTagLength]);
    }

    /// <summary>
    /// A fresh lease id recording <paramref name="jobTag"/>; the plain 32-character id when the
    /// execution is not driven by an identified job (a direct call, or a job written before
    /// <see cref="WorkerJobEnvelope.JobId"/> existed).
    /// </summary>
    public static string NewLeaseId(string? jobTag)
    {
        var unique = Guid.NewGuid().ToString("N");
        return jobTag is null ? unique : string.Concat(unique, ".", jobTag);
    }

    /// <summary>
    /// The job tag recorded in <paramref name="leaseId"/>, or <c>null</c> when it carries none: a
    /// lease issued by an older build, by a direct execution, or by anything else that does not
    /// write this exact shape.
    /// </summary>
    public static string? JobTagOf(string? leaseId)
        => leaseId is { Length: GuidLength + 1 + JobTagLength } && leaseId[GuidLength] == Separator
            ? leaseId[(GuidLength + 1)..]
            : null;

    /// <summary>
    /// Judges the lease in the way of a wake-up driven by the job tagged <paramref name="ownJobTag"/>
    /// (<c>null</c> when the delivery has no identity), given the first observation of that lease
    /// and the current one.
    /// </summary>
    public static FlowLeaseContentionVerdict Judge(
        FlowLeaseObservation baseline,
        FlowLeaseObservation observed,
        string? ownJobTag)
    {
        // Only a worker that acquired or renewed the lease AFTER the baseline can have written a
        // different owner or a later expiry; an unchanged pair proves nothing.
        var changed = !string.Equals(observed.LeaseId, baseline.LeaseId, StringComparison.Ordinal)
            || observed.ExpiresAtUtc > baseline.ExpiresAtUtc;
        if (!changed)
            return FlowLeaseContentionVerdict.KeepWaiting;

        return ownJobTag is not null
            && string.Equals(JobTagOf(observed.LeaseId), ownJobTag, StringComparison.Ordinal)
                ? FlowLeaseContentionVerdict.HolderOwnJobRedelivered
                : FlowLeaseContentionVerdict.AcknowledgeDuplicate;
    }
}
