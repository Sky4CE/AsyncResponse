namespace AsyncResponse;

/// <summary>
/// Shared options for an async-response channel (the response/recovery substrate). Concrete channels
/// extend this with their own transport-specific settings (key/subject prefixes, bucket names, …).
/// </summary>
public abstract class AsyncResponseChannelOptions
{
    /// <summary>
    /// How long persisted <see cref="RecoveryState"/> entries live, bounding how long after a
    /// crash/redeploy a late response can still trigger the lost-subscriber callbacks. Set it
    /// comfortably above your longest-running flow. (For the in-memory channel this is process-local
    /// and lost on exit.)
    /// </summary>
    public TimeSpan RecoveryStateExpiry { get; set; } = TimeSpan.FromDays(7);

    /// <summary>
    /// Default timeout applied to waiters that do not specify <c>WithTimeout</c>. When <c>null</c>
    /// (the default), <see cref="RecoveryStateExpiry"/> is used — waits are never infinite, so a
    /// response that never arrives faults the waiter with a <see cref="TimeoutException"/> instead of
    /// hanging forever.
    /// </summary>
    public TimeSpan? DefaultTimeout { get; set; }
}

/// <summary>
/// Shared options for a <em>durable</em> async-response channel that serializes failures onto the
/// wire (Redis, NATS). Adds the remote stack-trace policy on top of
/// <see cref="AsyncResponseChannelOptions"/>.
/// </summary>
public abstract class DurableAsyncResponseChannelOptions : AsyncResponseChannelOptions
{
    /// <summary>
    /// Whether a failed response envelope carries the remote exception's stack trace on the wire
    /// (surfaced to the waiter via <c>Exception.Data["RemoteStackTrace"]</c>). Stack traces aid
    /// debugging but can carry file paths; set to <c>false</c> to omit them. Default: <c>true</c>.
    /// </summary>
    public bool IncludeRemoteStackTrace { get; set; } = true;

    /// <summary>
    /// Maximum length, in characters, of a remote stack trace placed on the wire and restored on the
    /// waiter side; longer traces are truncated with a marker. Bounds what a buggy or hostile remote
    /// can push into logs (a multi-megabyte trace). Applied on both publish and receive. Default: 16384.
    /// </summary>
    public int MaxRemoteStackTraceLength { get; set; } = 16 * 1024;
}
