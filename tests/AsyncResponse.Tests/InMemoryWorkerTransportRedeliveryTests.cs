using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The in-memory transport's stand-in for broker redelivery: durable-flow wake-ups ride the worker
/// queue and rely on redelivery for contention recovery (the revision conflict's designed "abandon
/// and let the delivery retry"), so a failing job must be retried with backoff — the old
/// drop-on-first-failure silently stranded a flow a broker-backed transport would have recovered —
/// and a permanently failing job must end loudly without killing the worker loop.
/// </summary>
public sealed class InMemoryWorkerTransportRedeliveryTests
{
    public interface IRedeliveryProbe
    {
        Task RunAsync(string jobId);
    }

    private sealed class RedeliveryProbe : IRedeliveryProbe
    {
        private readonly Dictionary<string, int> _attempts = new(StringComparer.Ordinal);
        private readonly object _gate = new();

        /// <summary>Job id → how many attempts fail before it succeeds (int.MaxValue = always fails).</summary>
        public Dictionary<string, int> FailuresBeforeSuccess { get; } = new(StringComparer.Ordinal);
        public TaskCompletionSource<string> Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Attempts(string jobId)
        {
            lock (_gate)
            {
                return _attempts.GetValueOrDefault(jobId);
            }
        }

        public Task RunAsync(string jobId)
        {
            int attempt;
            lock (_gate)
            {
                attempt = _attempts.GetValueOrDefault(jobId) + 1;
                _attempts[jobId] = attempt;
            }

            if (attempt <= FailuresBeforeSuccess.GetValueOrDefault(jobId))
                throw new InvalidOperationException($"{jobId} transient failure {attempt}");

            Completed.TrySetResult(jobId);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task TransientlyFailingJob_IsRedeliveredUntilItSucceeds()
    {
        var probe = new RedeliveryProbe();
        probe.FailuresBeforeSuccess["wake-up"] = 2;
        await using var host = await StartHostAsync(probe, new InMemoryWorkerTransportOptions
        {
            MaxDeliveryAttempts = 5,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
            RetryMaxDelay = TimeSpan.FromMilliseconds(4)
        });

        await host.PublishAsync("wake-up");

        Assert.Equal("wake-up", await probe.Completed.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(3, probe.Attempts("wake-up"));
    }

    [Fact]
    public async Task PermanentlyFailingJob_IsDroppedAfterMaxAttempts_AndTheLoopKeepsServing()
    {
        var probe = new RedeliveryProbe();
        probe.FailuresBeforeSuccess["poison"] = int.MaxValue;
        var logger = new CollectingLogger();
        await using var host = await StartHostAsync(probe, new InMemoryWorkerTransportOptions
        {
            MaxDeliveryAttempts = 2,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
            RetryMaxDelay = TimeSpan.FromMilliseconds(2)
        }, logger);

        await host.PublishAsync("poison");
        // The job queued BEHIND the poisoned one still executes: dropping is per-job and the
        // worker loop survives it.
        await host.PublishAsync("healthy");

        Assert.Equal("healthy", await probe.Completed.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.Equal(2, probe.Attempts("poison"));
        await logger.WaitForAsync("failed after 2 attempts; dropping it");
        await logger.WaitForAsync("failed on attempt 1; retrying");
    }

    [Fact]
    public async Task ThrowingLogger_OnTheFailurePath_DoesNotEndTheWorkerLoop()
    {
        // Regression (fixpoint r1): a logging provider that throws (MEL's aggregate logger
        // rethrows a provider's failure; a test-output sink throws after its test ended) turned
        // the retry ladder's warning into an exception out of its catch, and the backstop's own
        // error log threw again — out of the only worker loop. Nothing ran any job after that.
        var probe = new RedeliveryProbe();
        probe.FailuresBeforeSuccess["poison"] = int.MaxValue;
        var logger = new CollectingLogger { ThrowOnMessageContaining = "failed" };
        await using var host = await StartHostAsync(probe, new InMemoryWorkerTransportOptions
        {
            MaxDeliveryAttempts = 2,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
            RetryMaxDelay = TimeSpan.FromMilliseconds(2)
        }, logger);

        await host.PublishAsync("poison");
        await host.PublishAsync("healthy");

        Assert.Equal("healthy", await probe.Completed.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        // The ladder itself ran to its end too, although every one of its log lines threw.
        Assert.Equal(2, probe.Attempts("poison"));
        Assert.Contains(logger.Messages, message => message.Contains("failed after 2 attempts; dropping it", StringComparison.Ordinal));
    }

    [Fact]
    public async Task DurableFlowInterruption_WhileTheHostIsStillRunning_IsAbandonedWithoutARetry()
    {
        // Regression (fixpoint r1): DurableFlowInterruptedException is the flow engine handing a
        // delivery back at host stop — raised on ApplicationStopping, which can fire before this
        // host's own stop — a cancellation by contract. The ladder treated it as a failure:
        // warned, re-ran the flow (which threw the same interruption at once) and finally dropped
        // it with an Error and a `dropped` outcome, for every parked flow on every deploy. It is
        // now abandoned on the spot, with one warning naming the explicit resume.
        var probe = new InterruptedProbe();
        var behind = new RedeliveryProbe();
        var logger = new CollectingLogger();
        var provider = new ServiceCollection()
            .AddSingleton<IInterruptedProbe>(probe)
            .AddSingleton<IRedeliveryProbe>(behind)
            .BuildServiceProvider();
        var transport = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions
        {
            MaxDeliveryAttempts = 3,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
            RetryMaxDelay = TimeSpan.FromMilliseconds(2)
        }));
        var executor = new WorkerJobExecutor(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<WorkerJobExecutor>.Instance);
        var host = new InMemoryWorkerHost(transport, executor, logger.For<InMemoryWorkerHost>());
        await using var _ = new HostHandle(host, transport, provider);
        await host.StartAsync(CancellationToken.None);

        await transport.PublishAsync(new WorkerJobEnvelope
        {
            CorrelationId = "interrupted",
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IInterruptedProbe).FullName!,
                MethodName = nameof(IInterruptedProbe.RunAsync),
                Params = []
            }
        });
        // A job queued behind it: once it has run, the worker is past the interrupted job — so
        // every retry the ladder was going to make has already been made.
        await transport.PublishAsync(new WorkerJobEnvelope
        {
            CorrelationId = "behind",
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IRedeliveryProbe).FullName!,
                MethodName = nameof(IRedeliveryProbe.RunAsync),
                Params = [CallbackParam.ForValue("behind")]
            }
        });

        Assert.Equal("behind", await behind.Completed.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(1, probe.Attempts);
        Assert.Single(logger.Messages, message => message.Contains("interrupted by host shutdown", StringComparison.Ordinal));
        Assert.Contains(logger.Messages, message => message.Contains("resumed explicitly after restart", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Messages, message => message.Contains("retrying", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Messages, message => message.Contains("dropping it", StringComparison.Ordinal));
    }

    public interface IInterruptedProbe
    {
        Task RunAsync();
    }

    private sealed class InterruptedProbe : IInterruptedProbe
    {
        private int _attempts;

        public int Attempts => Volatile.Read(ref _attempts);

        public Task RunAsync()
        {
            Interlocked.Increment(ref _attempts);
            throw new DurableFlowInterruptedException("Host is stopping; the delivery is handed back.");
        }
    }

    public interface ITimedOutProbe
    {
        Task RunAsync();
    }

    /// <summary>Its first attempt runs into the drain and then fails with an unrelated timeout; the next one succeeds.</summary>
    private sealed class TimedOutProbe : ITimedOutProbe
    {
        private int _attempts;

        public int Attempts => Volatile.Read(ref _attempts);
        public TaskCompletionSource FirstStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource FailFirst { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Completed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task RunAsync()
        {
            if (Interlocked.Increment(ref _attempts) == 1)
            {
                FirstStarted.TrySetResult();
                await FailFirst.Task;
                throw new TaskCanceledException("an HttpClient timeout, nothing to do with the host's stop");
            }

            Completed.TrySetResult();
        }
    }

    [Fact]
    public async Task UnrelatedCancellation_DuringTheDrain_IsRetriedLikeAnyFailure_NotAbandoned()
    {
        // Regression (fixpoint r1 pre-commit review): the interruption classifier abandoned ANY
        // OperationCanceledException raised while the host was stopping as "interrupted by host
        // shutdown" — so a job's own timeout (an HttpClient's TaskCanceledException) during the
        // drain lost its remaining attempts, with a warning blaming the shutdown. Only the flow
        // engine's DurableFlowInterruptedException means "handed back at stop"; everything else
        // rides the drain's retry ladder as before.
        var probe = new TimedOutProbe();
        var logger = new CollectingLogger();
        var provider = new ServiceCollection()
            .AddSingleton<ITimedOutProbe>(probe)
            .BuildServiceProvider();
        var transport = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions
        {
            MaxDeliveryAttempts = 3,
            RetryBaseDelay = TimeSpan.FromMilliseconds(1),
            RetryMaxDelay = TimeSpan.FromMilliseconds(2)
        }));
        var executor = new WorkerJobExecutor(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<WorkerJobExecutor>.Instance);
        var host = new InMemoryWorkerHost(transport, executor, logger.For<InMemoryWorkerHost>());
        await using var _ = new HostHandle(host, transport, provider);
        await host.StartAsync(CancellationToken.None);

        await transport.PublishAsync(new WorkerJobEnvelope
        {
            CorrelationId = "timed-out",
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(ITimedOutProbe).FullName!,
                MethodName = nameof(ITimedOutProbe.RunAsync),
                Params = []
            }
        });
        await probe.FirstStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // The stop cancels the host's token synchronously; the attempt fails only after that.
        var stop = host.StopAsync(CancellationToken.None);
        probe.FailFirst.TrySetResult();
        await stop.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.True(probe.Completed.Task.IsCompletedSuccessfully, "the drain must retry the timed-out job");
        Assert.Equal(2, probe.Attempts);
        Assert.Contains(logger.Messages, message => message.Contains("failed on attempt 1 during the shutdown drain; retrying", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Messages, message => message.Contains("interrupted by host shutdown", StringComparison.Ordinal));
    }

    [Fact]
    public void Options_ValidateRedeliverySettings()
    {
        Assert.Throws<InvalidOperationException>(() => new InMemoryWorkerTransport(
            Options.Create(new InMemoryWorkerTransportOptions { MaxDeliveryAttempts = -1 })));
        Assert.Throws<InvalidOperationException>(() => new InMemoryWorkerTransport(
            Options.Create(new InMemoryWorkerTransportOptions { RetryBaseDelay = TimeSpan.Zero })));
        Assert.Throws<InvalidOperationException>(() => new InMemoryWorkerTransport(
            Options.Create(new InMemoryWorkerTransportOptions
            {
                RetryBaseDelay = TimeSpan.FromSeconds(2),
                RetryMaxDelay = TimeSpan.FromSeconds(1)
            })));

        // Delays past the BCL timer ceiling would throw inside Task.Delay at the FIRST retry and
        // silently void the redelivery contract — rejected up front instead.
        Assert.Throws<InvalidOperationException>(() => new InMemoryWorkerTransport(
            Options.Create(new InMemoryWorkerTransportOptions { RetryMaxDelay = TimeSpan.MaxValue })));
        Assert.Throws<InvalidOperationException>(() => new InMemoryWorkerTransport(
            Options.Create(new InMemoryWorkerTransportOptions
            {
                RetryBaseDelay = TimeSpan.FromDays(60),
                RetryMaxDelay = TimeSpan.FromDays(60)
            })));

        // 0 = unlimited retries is an accepted configuration.
        _ = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions { MaxDeliveryAttempts = 0 }));
    }

    [Fact]
    public async Task StopDuringARetryBackoff_DropsTheFailingJob_AndDrainsTheJobsBehindIt()
    {
        // Regression: the retry backoff took no cancellation token, so a stop request during it
        // left the (single, by default) worker parked for up to RetryMaxDelay per attempt through
        // the whole drain — and every job queued BEHIND the failing one was lost when the bounded
        // stop returned. The sleep now honours the stopping token: the failing job is dropped
        // loudly and the queue behind it drains.
        var probe = new RedeliveryProbe();
        probe.FailuresBeforeSuccess["poison"] = int.MaxValue;
        var logger = new CollectingLogger();
        var host = await StartHostAsync(probe, new InMemoryWorkerTransportOptions
        {
            MaxDeliveryAttempts = 0,
            RetryBaseDelay = TimeSpan.FromMinutes(10),
            RetryMaxDelay = TimeSpan.FromMinutes(10)
        }, logger);

        await host.PublishAsync("poison");
        await host.PublishAsync("healthy");
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (probe.Attempts("poison") < 1 && DateTime.UtcNow < deadline)
            await Task.Delay(10);
        Assert.Equal(1, probe.Attempts("poison")); // now parked in its ten-minute backoff

        using var cutoff = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await host.StopAsync(cutoff.Token);

        Assert.False(cutoff.IsCancellationRequested, "the stop should have drained on its own, not been cut off");
        Assert.True(probe.Completed.Task.IsCompletedSuccessfully, "the job queued behind the failing one must still run");
        Assert.Contains(logger.Messages, message => message.Contains("host shutdown interrupted its retry backoff", StringComparison.Ordinal));
        await host.DisposeAsync();
    }

    private static async Task<HostHandle> StartHostAsync(
        RedeliveryProbe probe,
        InMemoryWorkerTransportOptions options,
        CollectingLogger? logger = null)
    {
        var provider = new ServiceCollection()
            .AddSingleton<IRedeliveryProbe>(probe)
            .BuildServiceProvider();
        var transport = new InMemoryWorkerTransport(Options.Create(options));
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance);
        var host = new InMemoryWorkerHost(
            transport,
            executor,
            logger?.For<InMemoryWorkerHost>() ?? NullLogger<InMemoryWorkerHost>.Instance);
        await host.StartAsync(CancellationToken.None);
        return new HostHandle(host, transport, provider);
    }

    private sealed class HostHandle(
        InMemoryWorkerHost _host,
        InMemoryWorkerTransport _transport,
        ServiceProvider _provider) : IAsyncDisposable
    {
        public Task PublishAsync(string jobId)
            => _transport.PublishAsync(new WorkerJobEnvelope
            {
                CorrelationId = jobId,
                Call = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IRedeliveryProbe).FullName!,
                    MethodName = nameof(IRedeliveryProbe.RunAsync),
                    Params = [CallbackParam.ForValue(jobId)]
                }
            });

        public Task StopAsync(CancellationToken cancellationToken) => _host.StopAsync(cancellationToken);

        public async ValueTask DisposeAsync()
        {
            await _host.StopAsync(CancellationToken.None);
            _host.Dispose();
            await _provider.DisposeAsync();
        }
    }
}
