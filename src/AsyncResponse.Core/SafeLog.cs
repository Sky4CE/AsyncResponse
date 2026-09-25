namespace AsyncResponse;

/// <summary>
/// Log calls on paths that must keep going when the logging pipeline itself fails: a worker
/// loop's failure handling, its backstop, and a shutdown drain. A logging provider can throw —
/// Microsoft.Extensions.Logging's aggregate logger rethrows a provider's failure, and a test
/// output sink throws once its test has ended — and on these paths the log line is the one part
/// that is optional. Letting it escape ended the loop the log was describing (with nothing left
/// to run the jobs queued behind it), skipped the bookkeeping after it, or aborted a drain before
/// the writer was completed.
/// </summary>
internal static class SafeLog
{
    /// <summary>Runs <paramref name="log"/>, swallowing anything it throws.</summary>
    public static void Try(Action log)
    {
        try
        {
            log();
        }
        catch
        {
            // The logger is what is failing; there is nowhere left to report to.
        }
    }

    /// <summary>
    /// Runs <paramref name="log"/> on <paramref name="state"/>, swallowing anything it throws. For
    /// hot paths: with a <c>static</c> lambda and its arguments passed as state, nothing is
    /// allocated until the line actually logs — a capturing lambda allocates its closure when the
    /// enclosing scope is ENTERED, whether or not the log line ever runs.
    /// </summary>
    public static void Try<TState>(TState state, Action<TState> log)
    {
        try
        {
            log(state);
        }
        catch
        {
            // The logger is what is failing; there is nowhere left to report to.
        }
    }
}
