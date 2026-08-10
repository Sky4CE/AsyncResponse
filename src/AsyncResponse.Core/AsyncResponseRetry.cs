namespace AsyncResponse;

internal static class AsyncResponseRetry
{
    /// <summary>Runs this background operation until cancellation is requested.</summary>
    public static async Task<T> ExecuteAsync<T>(
        Func<CancellationToken, Task<T>> action,
        Func<Exception, bool> isTransient,
        int maxAttempts,
        TimeSpan baseDelay,
        TimeSpan maxDelay,
        CancellationToken cancellationToken,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(action);
        ArgumentNullException.ThrowIfNull(isTransient);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxAttempts);

        var attempt = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            attempt++;

            try
            {
                return await action(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < maxAttempts && isTransient(ex))
            {
                await Task.Delay(Backoff(attempt, baseDelay, maxDelay), timeProvider ?? TimeProvider.System, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Computes the retry backoff delay: exponential with half-jitter.</summary>
    public static TimeSpan Backoff(int completedAttempts, TimeSpan baseDelay, TimeSpan maxDelay)
    {
        var multiplier = 1 << Math.Min(Math.Max(completedAttempts, 1) - 1, 10);
        var milliseconds = Math.Min(maxDelay.TotalMilliseconds, baseDelay.TotalMilliseconds * multiplier);

        // Half-jitter: keep at least half the exponential step so backoff still backs off, and
        // randomize the rest — a broker blip fails many waiters across many replicas at once, and
        // un-jittered exponential delays would send them all reconnecting in lockstep waves.
        milliseconds = milliseconds / 2 + Random.Shared.NextDouble() * (milliseconds / 2);
        return TimeSpan.FromMilliseconds(Math.Max(1, milliseconds));
    }
}
