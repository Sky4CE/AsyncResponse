namespace AsyncResponse;

/// <summary>
/// Carries ambient async-response metadata that flows with the async call stack via
/// <see cref="AsyncLocal{T}"/>, so it is unique per logical operation.
/// <para>
/// Publishers fall back to this ambient value when <c>SetResponse</c>/<c>SetException</c> are
/// called without an explicit correlation id, and worker jobs restore it before executing so
/// downstream publishes correlate automatically. Reply targets are available to outbound
/// integration code that needs to tell a remote system where to publish its response.
/// Prefer passing values explicitly where practical; the ambient context exists for integration
/// points that cannot.
/// </para>
/// </summary>
public static class AsyncResponseContext
{
    private static readonly AsyncLocal<string?> _currentCorrelationId = new();
    private static readonly AsyncLocal<AsyncResponseReplyTarget?> _currentReplyTarget = new();

    /// <summary>Gets the correlation id of the current logical operation, if any.</summary>
    public static string? CorrelationId => _currentCorrelationId.Value;

    /// <summary>Gets the reply target selected for the current logical operation, if any.</summary>
    public static AsyncResponseReplyTarget? ReplyTarget => _currentReplyTarget.Value;

    /// <summary>Generates a new correlation id, stores it in the ambient context, and returns it.</summary>
    public static string CreateCorrelationId()
    {
        var correlationId = GenerateCorrelationId();
        _currentCorrelationId.Value = correlationId;
        return correlationId;
    }

    /// <summary>Generates a new correlation id without storing it.</summary>
    public static string GenerateCorrelationId() => Guid.NewGuid().ToString();

    /// <summary>Ensures the ambient correlation id is non-empty, generating one if missing.</summary>
    public static string EnsureCorrelationId()
    {
        if (string.IsNullOrWhiteSpace(_currentCorrelationId.Value))
        {
            _currentCorrelationId.Value = GenerateCorrelationId();
        }

        return _currentCorrelationId.Value!;
    }

    /// <summary>Sets the ambient correlation id for the current logical operation.</summary>
    /// <exception cref="ArgumentException">Thrown when <paramref name="correlationId"/> is null or whitespace.</exception>
    public static void SetCorrelationId(string correlationId)
    {
        _currentCorrelationId.Value = !string.IsNullOrWhiteSpace(correlationId)
            ? correlationId
            : throw new ArgumentException("CorrelationId cannot be null or whitespace.", nameof(correlationId));
    }

    /// <summary>Sets the ambient reply target for the current logical operation.</summary>
    public static void SetReplyTarget(AsyncResponseReplyTarget replyTarget)
    {
        ArgumentNullException.ThrowIfNull(replyTarget);
        ValidateReplyTarget(replyTarget);
        _currentReplyTarget.Value = replyTarget;
    }

    /// <summary>
    /// Temporarily sets the ambient correlation id for the current logical operation and
    /// restores the previous value when the returned scope is disposed. Passing <c>null</c> or
    /// whitespace clears the ambient id for the scope.
    /// </summary>
    internal static IDisposable PushCorrelationId(string? correlationId)
    {
        var previousCorrelationId = _currentCorrelationId.Value;
        _currentCorrelationId.Value = !string.IsNullOrWhiteSpace(correlationId) ? correlationId : null;
        return new CorrelationScope(previousCorrelationId);
    }

    /// <summary>
    /// Temporarily sets the ambient async-response context and restores the previous values when
    /// the returned scope is disposed.
    /// </summary>
    internal static IDisposable PushContext(string? correlationId, AsyncResponseReplyTarget? replyTarget)
    {
        if (replyTarget is not null)
        {
            ValidateReplyTarget(replyTarget);
        }

        var previousCorrelationId = _currentCorrelationId.Value;
        var previousReplyTarget = _currentReplyTarget.Value;

        _currentCorrelationId.Value = !string.IsNullOrWhiteSpace(correlationId) ? correlationId : null;
        _currentReplyTarget.Value = replyTarget;

        return new ContextScope(previousCorrelationId, previousReplyTarget);
    }

    /// <summary>Clears the ambient correlation id for the current logical operation.</summary>
    public static void ClearCorrelationId() => _currentCorrelationId.Value = null;

    /// <summary>Clears the ambient reply target for the current logical operation.</summary>
    public static void ClearReplyTarget() => _currentReplyTarget.Value = null;

    private static void ValidateReplyTarget(AsyncResponseReplyTarget replyTarget)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(replyTarget.Name);
        ArgumentException.ThrowIfNullOrWhiteSpace(replyTarget.Transport);
        ArgumentException.ThrowIfNullOrWhiteSpace(replyTarget.Address);
    }

    private sealed class CorrelationScope(string? _previousCorrelationId) : IDisposable
    {
        private int _disposed;

        /// <summary>Releases resources held by this instance.</summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _currentCorrelationId.Value = _previousCorrelationId;
            }
        }
    }

    private sealed class ContextScope(
        string? _previousCorrelationId,
        AsyncResponseReplyTarget? _previousReplyTarget) : IDisposable
    {
        private int _disposed;

        /// <summary>Releases resources held by this instance.</summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _currentCorrelationId.Value = _previousCorrelationId;
                _currentReplyTarget.Value = _previousReplyTarget;
            }
        }
    }
}
