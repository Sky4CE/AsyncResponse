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
        Action<Exception, TimeSpan> logRetry)
        => (Task)RunAsyncMethod.Invoke(null, [run, stoppingToken, delayPolicy, logRetry])!;

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
    public async Task RunAsync_Propagates_WhenANonCancellationExceptionRacesShutdown()
    {
        // The while-loop condition passes (the token is not cancelled yet), then run() cancels it
        // as a side effect and throws something other than OperationCanceledException. Neither
        // catch clause matches: the OperationCanceledException clause requires that exception type,
        // and `when (!stoppingToken.IsCancellationRequested)` is now false — so the failure
        // propagates instead of being retried or swallowed.
        using var cts = new CancellationTokenSource();

        await Assert.ThrowsAsync<InvalidOperationException>(() => RunAsync(
            _ =>
            {
                cts.Cancel();
                throw new InvalidOperationException("boom");
            },
            cts.Token,
            _ => TimeSpan.Zero,
            (_, _) => { }));
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

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!condition())
            await Task.Delay(10, timeout.Token);
    }
}
