using AsyncResponse.Testing;
using AsyncResponse.Transports.NATS;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// <c>src/Transports/Shared/SubscriberSupervisor.cs</c> is source-linked into all 10 transport
/// packages, so it is exercised here by reflection against one compiled copy (NATS, arbitrarily —
/// the type is byte-identical in every package, like <see cref="SqlServerRelationVerifierTests"/>
/// does for its own shared source file) instead of through a concrete subscriber's own fixtures,
/// which additionally exercise that subscriber's own <c>RunSubscriberAsync</c> body. The 10
/// concrete per-transport subscriber test classes separately pin that each transport's
/// <c>ExecuteAsync</c> override still renders its own byte-identical log line and honors its own
/// pre-loop setup through this shared loop.
/// </summary>
public sealed class SubscriberSupervisorTests
{
    private static readonly MethodInfo RunAsyncMethod = typeof(NatsAsyncResponseTransportOptions).Assembly
        .GetType("AsyncResponse.Transports.SubscriberSupervisor", throwOnError: true)!
        .GetMethod("RunAsync", BindingFlags.Public | BindingFlags.Static)!;

    private static Task RunAsync(
        Func<CancellationToken, Task> run,
        CancellationToken stoppingToken,
        Func<int, TimeSpan> delayPolicy,
        Action<Exception, TimeSpan> logRetry,
        TimeProvider? timeProvider = null)
        => (Task)RunAsyncMethod.Invoke(null, [run, stoppingToken, delayPolicy, logRetry, timeProvider])!;

    [Fact]
    public async Task RunAsync_ReturnsWithoutRetrying_WhenRunSucceedsImmediately()
    {
        var runCalls = 0;
        var delayCalls = 0;
        var logCalls = 0;

        await RunAsync(
            _ => { runCalls++; return Task.CompletedTask; },
            CancellationToken.None,
            _ => { delayCalls++; return TimeSpan.Zero; },
            (_, _) => logCalls++);

        Assert.Equal(1, runCalls);
        Assert.Equal(0, delayCalls);
        Assert.Equal(0, logCalls);
    }

    [Fact]
    public async Task RunAsync_RetriesThenSucceeds_FeedingTheEscalatingFailureCountToTheDelayPolicy()
    {
        var attempt = 0;
        var observedFailureCounts = new List<int>();
        var loggedDelays = new List<TimeSpan>();

        await RunAsync(
            _ =>
            {
                attempt++;
                if (attempt <= 3)
                    throw new InvalidOperationException($"attempt {attempt} fails");
                return Task.CompletedTask;
            },
            CancellationToken.None,
            failures =>
            {
                // int.MaxValue is the supervisor asking for the policy's longest delay (its
                // healthy-run threshold), not a failure count; a day keeps these instant runs
                // consecutive.
                if (failures == int.MaxValue)
                    return TimeSpan.FromDays(1);

                observedFailureCounts.Add(failures);
                return TimeSpan.FromMilliseconds(failures); // deterministic, distinguishable per call
            },
            (_, delay) => loggedDelays.Add(delay));

        Assert.Equal(4, attempt); // 3 failed attempts + the succeeding one
        Assert.Equal(new List<int> { 1, 2, 3 }, observedFailureCounts); // 1-based and strictly increasing
        Assert.Equal(
            new List<TimeSpan> { TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(2), TimeSpan.FromMilliseconds(3) },
            loggedDelays); // logRetry observes the SAME delay the policy just computed for that failure count
    }

    [Fact]
    public async Task RunAsync_StopsPromptly_WhenCancelledWhileWaitingOutARetryDelay()
    {
        using var cts = new CancellationTokenSource();
        var delayCalls = 0;

        var task = RunAsync(
            _ => throw new InvalidOperationException("always fails"),
            cts.Token,
            _ =>
            {
                delayCalls++;
                return TimeSpan.FromSeconds(30); // would time out this test if cancellation were ignored
            },
            (_, _) => { });

        await WaitUntilAsync(() => delayCalls >= 1); // let it enter the 30s retry delay
        cts.Cancel();

        // Task.Delay(retryDelay, stoppingToken) honors cancellation instead of running the full
        // delay: that await is not wrapped by either catch clause, so cancelling here surfaces as
        // a thrown OperationCanceledException out of RunAsync rather than a quiet return —
        // BackgroundService's own shutdown path tolerates exactly that from ExecuteAsync.
        var completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(task, completed);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => task);
    }

    [Fact]
    public async Task RunAsync_ReturnsWithoutInvokingDelayPolicyOrLog_WhenRunThrowsCancellationMatchingShutdown()
    {
        using var cts = new CancellationTokenSource();
        var delayCalls = 0;
        var logCalls = 0;

        await RunAsync(
            ct =>
            {
                // run() observes shutdown and reacts the way every real RunSubscriberAsync does:
                // an OperationCanceledException once the token it was handed is cancelled.
                cts.Cancel();
                throw new OperationCanceledException(ct);
            },
            cts.Token,
            _ => { delayCalls++; return TimeSpan.Zero; },
            (_, _) => logCalls++);

        Assert.Equal(0, delayCalls);
        Assert.Equal(0, logCalls);
    }

    [Fact]
    public async Task RunAsync_ExitsQuietly_WhenANonCancellationExceptionRacesShutdown()
    {
        // The while-loop condition passes (the token is not cancelled yet), then run() cancels it
        // as a side effect and throws something other than OperationCanceledException — the
        // ObjectDisposed/connection-closed shape every broker client raises when the stop tears
        // its connection down mid-consume. That used to match NEITHER catch clause (one required
        // the cancellation type, the other's filter required "not shutting down") and escaped
        // ExecuteAsync, faulting the BackgroundService on every clean redeploy. Shutdown exits
        // quietly whatever the exception type: no throw, no retry, no retry log.
        using var cts = new CancellationTokenSource();
        var delayCalls = 0;
        var logCalls = 0;

        await RunAsync(
            _ =>
            {
                cts.Cancel();
                throw new InvalidOperationException("boom");
            },
            cts.Token,
            _ => { delayCalls++; return TimeSpan.Zero; },
            (_, _) => logCalls++);

        Assert.Equal(0, delayCalls);
        Assert.Equal(0, logCalls);
    }

    [Fact]
    public async Task RunAsync_NeverCallsRun_WhenAlreadyCancelledBeforeStarting()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var runCalls = 0;

        await RunAsync(
            _ => { runCalls++; return Task.CompletedTask; },
            cts.Token,
            _ => TimeSpan.Zero,
            (_, _) => { });

        Assert.Equal(0, runCalls);
    }

    /// <summary>
    /// Regression: the documented "consecutive-failure count" was lifetime-cumulative. A subscriber
    /// runs for weeks, so a handful of unrelated blips spread over that time pinned EVERY later
    /// reconnect at the policy's longest delay — in all 10 transports. A run that stayed up at
    /// least as long as the longest delay the policy can impose was healthy: the next failure
    /// starts a new streak.
    /// </summary>
    [Fact]
    public async Task RunAsync_StartsTheFailureCountOver_AfterARunThatOutlivedThePolicysLongestDelay()
    {
        var clock = new VirtualTimeProvider();
        var longestDelay = TimeSpan.FromSeconds(30);
        var attempt = 0;
        var observedFailureCounts = new List<int>();

        await RunAsync(
            _ =>
            {
                attempt++;
                switch (attempt)
                {
                    case <= 3:
                        throw new InvalidOperationException("a burst of failures: the broker is down");
                    case 4:
                        // The reconnect worked and the subscriber then consumed for an hour.
                        clock.Advance(TimeSpan.FromHours(1));
                        throw new InvalidOperationException("an unrelated blip, an hour later");
                    case 5:
                        // Stayed up for exactly the longest delay: healthy too (inclusive bound).
                        clock.Advance(longestDelay);
                        throw new InvalidOperationException("and another");
                    default:
                        return Task.CompletedTask;
                }
            },
            CancellationToken.None,
            failures =>
            {
                if (failures == int.MaxValue)
                    return longestDelay;

                observedFailureCounts.Add(failures);
                return TimeSpan.Zero; // a zero wait completes inline on any clock
            },
            (_, _) => { },
            clock);

        // The old lifetime count fed 1, 2, 3, 4, 5 here: the blip an hour later already waited
        // like a fourth consecutive failure, and every one after it waited longer still.
        Assert.Equal(new List<int> { 1, 2, 3, 1, 1 }, observedFailureCounts);
    }

    /// <summary>
    /// The other half: runs that fail FASTER than the policy's longest delay are still one streak
    /// and keep escalating — a crash loop must not reset itself back to the shortest delay.
    /// </summary>
    [Fact]
    public async Task RunAsync_KeepsEscalating_WhileRunsFailBeforeThePolicysLongestDelay()
    {
        var clock = new VirtualTimeProvider();
        var longestDelay = TimeSpan.FromSeconds(30);
        var attempt = 0;
        var observedFailureCounts = new List<int>();

        await RunAsync(
            _ =>
            {
                attempt++;
                if (attempt > 4)
                    return Task.CompletedTask;

                // Connected, consumed briefly, died: one tick short of a healthy run.
                clock.Advance(longestDelay - TimeSpan.FromTicks(1));
                throw new InvalidOperationException($"attempt {attempt} fails");
            },
            CancellationToken.None,
            failures =>
            {
                if (failures == int.MaxValue)
                    return longestDelay;

                observedFailureCounts.Add(failures);
                return TimeSpan.Zero;
            },
            (_, _) => { },
            clock);

        Assert.Equal(new List<int> { 1, 2, 3, 4 }, observedFailureCounts);
    }

    /// <summary>The retry wait runs on the supplied clock, so a virtual clock decides when it ends.</summary>
    [Fact]
    public async Task RunAsync_WaitsOutTheRetryDelay_OnTheSuppliedClock()
    {
        var clock = new VirtualTimeProvider();
        var attempt = 0;
        var waiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var task = RunAsync(
            _ =>
            {
                attempt++;
                return attempt == 1 ? throw new InvalidOperationException("fails once") : Task.CompletedTask;
            },
            CancellationToken.None,
            _ => TimeSpan.FromMinutes(5),
            (_, _) => waiting.TrySetResult(),
            clock);

        await waiting.Task.WaitAsync(TimeSpan.FromSeconds(30));
        await WaitUntilAsync(() => clock.NextTimerDueAt is not null);
        Assert.False(task.IsCompleted);

        clock.Advance(TimeSpan.FromMinutes(5));
        await task.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(2, attempt);
    }

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
            await Task.Delay(10, timeout.Token);
    }
}
