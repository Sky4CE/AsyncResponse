namespace AsyncResponse.Transports;

// Shared source for every transport's outer BackgroundService.ExecuteAsync supervise-and-retry
// loop: each csproj pulls this file in via <Compile Include="..\Shared\SubscriberSupervisor.cs" />,
// so it compiles INTO each provider assembly. Per-transport setup that must run once before the
// loop starts (topology warnings, resolving a queue/subscription name) stays in the caller, ahead
// of the call into RunAsync below; only the retry-with-backoff loop shape is shared.

/// <summary>
/// Runs a subscriber's connect-and-consume operation with retry-with-backoff on failure. A
/// cancellation that matches host shutdown exits quietly; any other exception — including a
/// cancellation NOT caused by host shutdown, e.g. a transport-internal timeout — increments the
/// failure count, asks the caller-supplied delay policy how long to wait, reports the retry through
/// the caller-supplied callback, and waits before trying again.
/// </summary>
internal static class SubscriberSupervisor
{
    /// <summary>
    /// Runs <paramref name="run"/> until it completes or <paramref name="stoppingToken"/> requests
    /// shutdown. <paramref name="delayPolicy"/> receives the 1-based consecutive-failure count and
    /// returns how long to wait before the next attempt; <paramref name="logRetry"/> renders the
    /// per-transport log line for that wait.
    /// </summary>
    public static async Task RunAsync(
        Func<CancellationToken, Task> run,
        CancellationToken stoppingToken,
        Func<int, TimeSpan> delayPolicy,
        Action<Exception, TimeSpan> logRetry)
    {
        var failures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await run(stoppingToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                failures++;
                var retryDelay = delayPolicy(failures);
                logRetry(ex, retryDelay);
                await Task.Delay(retryDelay, stoppingToken).ConfigureAwait(false);
            }
        }
    }
}
