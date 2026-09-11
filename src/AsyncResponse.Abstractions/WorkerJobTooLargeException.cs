namespace AsyncResponse;

/// <summary>
/// Thrown by every <c>EnqueueWorkerAsync</c> overload — and by <c>IDurableFlows.StartAsync</c>,
/// whose start job carries the initial ledger — when the serialized worker envelope exceeds
/// <c>AsyncResponseOptions.MaxInboundMessageChars</c>, the budget the consuming ingress enforces.
/// Nothing was published.
/// <para>
/// Without this producer-side check the job left the process: the transport accepted it (every
/// database transport and most brokers take far more than the engine's default 8 Mi
/// characters), the ingress then acknowledged it <em>without executing it</em> — an oversized
/// message never gets smaller, so redelivering it would hot-loop — and the caller held a flow id
/// for a run recorded as <c>Running</c> that nothing would ever execute. Failing here, in the
/// caller's stack, is the only place the mistake can still be corrected.
/// </para>
/// <para>
/// The measurement is exact: the envelope is serialized the way the transports serialize it and
/// compared in UTF-16 code units, which is what the ingress compares. JSON escaping counts — a
/// 5 Mi-character argument of quotes or non-ASCII text serializes to several times its length.
/// The producer's own <c>MaxInboundMessageChars</c> stands in for the consumer's; keep the option
/// identical across the processes of one deployment. Put large arguments behind a claim check
/// (persist the data and pass a reference) rather than raising the limit.
/// </para>
/// </summary>
public sealed class WorkerJobTooLargeException : InvalidOperationException
{
    /// <summary>Creates the exception for an envelope of <paramref name="serializedLength"/> code units over <paramref name="limit"/>.</summary>
    public WorkerJobTooLargeException(int serializedLength, int limit)
        : base(
            $"The worker job envelope serializes to {serializedLength} UTF-16 code units, over the {limit} the consuming ingress " +
            "accepts (AsyncResponseOptions.MaxInboundMessageChars); it would be acknowledged without ever executing. Nothing was " +
            "published. Put large arguments behind a claim check (persist the data and pass a reference) rather than raising the limit.")
    {
        SerializedLength = serializedLength;
        Limit = limit;
    }

    /// <summary>The envelope's serialized length in UTF-16 code units, as the ingress would measure it.</summary>
    public int SerializedLength { get; }

    /// <summary>The configured budget the envelope exceeded.</summary>
    public int Limit { get; }
}
