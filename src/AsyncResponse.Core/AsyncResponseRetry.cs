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
        CancellationToken cancellationToken)
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
                await Task.Delay(Backoff(attempt, baseDelay, maxDelay), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Computes the retry backoff delay.</summary>
    public static TimeSpan Backoff(int completedAttempts, TimeSpan baseDelay, TimeSpan maxDelay)
    {
        var multiplier = 1 << Math.Min(completedAttempts - 1, 10);
        var milliseconds = Math.Min(maxDelay.TotalMilliseconds, baseDelay.TotalMilliseconds * multiplier);
        return TimeSpan.FromMilliseconds(Math.Max(1, milliseconds));
    }
}
