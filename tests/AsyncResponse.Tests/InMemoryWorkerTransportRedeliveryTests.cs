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
