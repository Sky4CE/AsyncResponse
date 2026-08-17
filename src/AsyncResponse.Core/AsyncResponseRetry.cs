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
            // The filter deliberately tests only the cheap, throw-free attempt count. isTransient
            // is caller-supplied and runs in the BODY below: the CLR swallows an exception thrown
            // inside an exception filter and evaluates the filter as false, so a predicate that
            // faults (a null-deref on ex.InnerException, say) would silently reclassify every
            // retryable fault as permanent and burn the whole retry budget with its own bug
            // invisible in every log and trace.
            catch (Exception ex) when (attempt < maxAttempts)
            {
                if (!IsTransientOrThrow(isTransient, ex))
                    throw;

                await Task.Delay(Backoff(attempt, baseDelay, maxDelay), timeProvider ?? TimeProvider.System, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Evaluates the caller's transience predicate OUTSIDE an exception filter, so a predicate
    /// that throws surfaces instead of being silently read as "not transient". Both failures are
    /// raised together: the predicate's own fault (the bug to fix) and the exception it was
    /// judging (the reason the retry ran at all), so neither is lost. Only reachable when the
    /// predicate itself is broken; a well-behaved one returns and this is a plain call.
    /// </summary>
    private static bool IsTransientOrThrow(Func<Exception, bool> isTransient, Exception ex)
    {
        try
        {
            return isTransient(ex);
        }
        catch (Exception predicateFailure)
        {
            throw new AggregateException(
                "The retry policy's isTransient predicate threw while classifying a fault. The predicate's own failure and the " +
                "exception it was judging are both attached; fix the predicate — until then no fault can be classified as transient.",
                ex,
                predicateFailure);
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
