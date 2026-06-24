namespace AsyncResponse;

/// <summary>
/// Applies the wire policy for a remote exception stack trace shared across the durable channels:
/// whether it travels on the wire at all, and a hard length cap so a buggy or hostile remote cannot
/// push a multi-megabyte trace into the envelope (and from there into generic exception logs).
/// </summary>
internal static class RemoteStackTrace
{
    private const string TruncationMarker = "… [stack trace truncated by AsyncResponse]";

    /// <summary>
    /// The publish-side policy: returns <c>null</c> when <paramref name="include"/> is <c>false</c>,
    /// otherwise the trace capped to <paramref name="maxLength"/>.
    /// </summary>
    public static string? ForWire(string? stackTrace, bool include, int maxLength)
        => include ? Cap(stackTrace, maxLength) : null;

    /// <summary>
    /// Truncates <paramref name="stackTrace"/> to <paramref name="maxLength"/> characters, appending a
    /// marker when truncated. Used on both publish (bound what we emit) and receive (bound what we
    /// accept from a remote we do not control). A non-positive cap leaves the input unchanged so a
    /// misconfiguration cannot silently erase diagnostics.
    /// </summary>
    public static string? Cap(string? stackTrace, int maxLength)
    {
        if (string.IsNullOrEmpty(stackTrace) || maxLength <= 0 || stackTrace.Length <= maxLength)
            return stackTrace;

        return string.Concat(stackTrace.AsSpan(0, maxLength), TruncationMarker);
    }
}
