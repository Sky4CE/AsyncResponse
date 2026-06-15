namespace AsyncResponse;

/// <summary>
/// Carries the ambient correlation id that flows with the async call stack via
/// <see cref="AsyncLocal{T}"/>, so it is unique per logical operation.
/// <para>
/// Publishers fall back to this ambient value when <c>SetResponse</c>/<c>SetException</c> are
/// called without an explicit correlation id, and worker jobs restore it before executing so
/// downstream publishes correlate automatically. Prefer passing correlation ids explicitly
/// where practical; the ambient context exists for integration points that cannot.
/// </para>
/// </summary>
public static class AsyncResponseContext
{
    private static readonly AsyncLocal<string?> _currentCorrelationId = new();

    /// <summary>Gets the correlation id of the current logical operation, if any.</summary>
    public static string? CorrelationId => _currentCorrelationId.Value;

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

    /// <summary>Clears the ambient correlation id for the current logical operation.</summary>
    public static void ClearCorrelationId() => _currentCorrelationId.Value = null;

    private sealed class CorrelationScope(string? _previousCorrelationId) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _currentCorrelationId.Value = _previousCorrelationId;
            }
        }
    }
}
