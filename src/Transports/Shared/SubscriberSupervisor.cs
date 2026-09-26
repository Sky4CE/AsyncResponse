namespace AsyncResponse.Transports;

// Shared source for every transport's outer BackgroundService.ExecuteAsync supervise-and-retry
// loop: each csproj pulls this file in via <Compile Include="..\Shared\SubscriberSupervisor.cs" />,
// so it compiles INTO each provider assembly. Per-transport setup that must run once before the
// loop starts (topology warnings, resolving a queue/subscription name) stays in the caller, ahead
// of the call into RunAsync below; only the retry-with-backoff loop shape is shared.

/// <summary>
/// Runs a subscriber's connect-and-consume operation with retry-with-backoff on failure. Any
/// exception observed once host shutdown has been requested exits quietly — the cancellation
/// itself, and equally the ObjectDisposed/connection-closed errors broker clients raise when the
/// stop tears a connection down mid-consume; any other exception — including a cancellation NOT
/// caused by host shutdown, e.g. a transport-internal timeout — increments the failure count, asks
/// the caller-supplied delay policy how long to wait, reports the retry through the caller-supplied
/// callback, and waits before trying again. The count is of CONSECUTIVE failures: a run that stayed
/// up at least as long as the longest retry delay the policy can produce before failing starts the
/// count over.
/// </summary>
internal static class SubscriberSupervisor
{
    /// <summary>
    /// Runs <paramref name="run"/> until it completes or <paramref name="stoppingToken"/> requests
    /// shutdown. <paramref name="delayPolicy"/> receives the 1-based consecutive-failure count and
    /// returns how long to wait before the next attempt; <paramref name="logRetry"/> renders the
    /// per-transport log line for that wait. <paramref name="healthyRunThreshold"/> — the longest
    /// delay the policy can actually produce (<c>AsyncResponseRetry.MaxAttainableDelay</c>: the
    /// configured maximum, or the base times the backoff's 1024× multiplier cap when that is lower)
    /// — is the healthy-run threshold past which a failed run no longer counts as consecutive with
    /// the one before it. It is passed, not asked of the policy: every
    /// transport's policy is the half-jittered <c>AsyncResponseRetry.Backoff</c>, whose answer for
    /// any failure count is only a sample in [max/2, max], so a threshold drawn from it once reset
    /// the streak for runs that died well short of the maximum.
    /// <paramref name="timeProvider"/> clocks both the run and the wait (a test seam; the system
    /// clock when omitted).
    /// </summary>
    public static async Task RunAsync(
        Func<CancellationToken, Task> run,
        CancellationToken stoppingToken,
        Func<int, TimeSpan> delayPolicy,
        Action<Exception, TimeSpan> logRetry,
        TimeSpan healthyRunThreshold,
        TimeProvider? timeProvider = null)
    {
        var clock = timeProvider ?? TimeProvider.System;
        var failures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            var startedAt = clock.GetTimestamp();
            try
            {
                await run(stoppingToken).ConfigureAwait(false);
                return;
            }
            catch (Exception) when (stoppingToken.IsCancellationRequested)
            {
                // Shutdown. Not only the OperationCanceledException: a client whose connection the
                // stop just closed throws its own ObjectDisposed/AlreadyClosed/connection error
                // instead, and with no arm for that shape it escaped ExecuteAsync — faulting the
                // BackgroundService (a critical "BackgroundService failed" log, and StopApplication
                // under the default behavior) on every clean redeploy.
                return;
            }
            catch (Exception ex)
            {
                // Consecutive, not lifetime: a subscriber runs for weeks, and with a count that only
                // ever grew, a handful of unrelated blips spread over that time pinned EVERY later
                // reconnect at the policy's longest delay — a one-second broker failover then cost
                // the full maximum on every replica, for the rest of the process's life. A run that
                // outlived the longest delay the policy can impose was healthy, so this failure
                // starts a new streak. (A run that merely took that long to fail — a black-holed
                // connect — resets too, harmlessly: its own duration already paces the retries.)
                if (failures > 0 && clock.GetElapsedTime(startedAt) >= healthyRunThreshold)
                    failures = 0;

                failures++;
                var retryDelay = delayPolicy(failures);

                // SafeLog: a logging provider that throws (MEL rethrows a provider's failure) turned
                // this retry's log line into an exception out of the loop — the BackgroundService
                // faulted, stopping the host under the default behavior and leaving the subscriber
                // dead under Ignore, for a failure the loop exists to ride out.
                SafeLog.Try((LogRetry: logRetry, Error: ex, Delay: retryDelay), static state => state.LogRetry(state.Error, state.Delay));
                await Task.Delay(retryDelay, clock, stoppingToken).ConfigureAwait(false);
            }
        }
    }
}
