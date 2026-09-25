using System.Text.Json;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace AsyncResponse.Tests;

/// <summary>
/// Shared plumbing for tests that drive a <see cref="DurableFlowContext"/> directly — one
/// execution at a time, on the virtual clock, against the in-memory store and a recording worker
/// transport — so a test can look at exactly what one delivery persisted and published.
/// </summary>
internal static class DurableFlowContextTestSupport
{
    public sealed record ParkInput(int TenantId);

    public sealed class ParkNoopFlow : IDurableFlow<ParkInput>
    {
        public Task ExecuteAsync(IDurableFlowContext flow, ParkInput input) => Task.CompletedTask;
    }

    /// <summary>
    /// Lease cadence sized for the virtual clock: a test jumps minutes or hours at once, and the
    /// lease's deadline is clock-based, so the default one-minute lease would read as lost.
    /// </summary>
    public static DurableFlowOptions Options(Action<DurableFlowOptions>? configure = null)
    {
        var options = new DurableFlowOptions
        {
            ExecutionLeaseDuration = TimeSpan.FromDays(30),
            ExecutionLeaseRenewInterval = TimeSpan.FromDays(10)
        };
        configure?.Invoke(options);
        return options;
    }

    public static ServiceProvider BuildProvider(IWorkerTransport transport, TimeProvider clock)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(clock);
        services
            .AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryDurableFlows()
            .WithDurableFlow<ParkNoopFlow, ParkInput>();
        services.AddSingleton(transport);
        return services.BuildServiceProvider();
    }

    public static FlowState State(string id, string? parent = null, FlowRunStatus status = FlowRunStatus.Running) => new()
    {
        FlowId = id,
        ParentFlowId = parent,
        Status = status,
        FlowTypeName = typeof(ParkNoopFlow).FullName,
        InputTypeName = typeof(ParkInput).FullName,
        InputJson = JsonSerializer.Serialize(new ParkInput(7)),
        CreatedAtUtc = VirtualTimeProvider.DefaultStartTime.UtcDateTime,
        UpdatedAtUtc = VirtualTimeProvider.DefaultStartTime.UtcDateTime
    };

    public static async Task<FlowExecutionLease> AcquireAsync(IFlowStateStore store, string flowId, DurableFlowOptions options, TimeProvider clock)
        => (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, flowId, options, NullLogger.Instance, clock))
            ?? throw new InvalidOperationException($"The execution lease of '{flowId}' is held.");

    public static DurableFlowContext CreateContext(
        ServiceProvider provider,
        FlowState state,
        IFlowStateStore store,
        FlowExecutionLease lease,
        DurableFlowOptions options,
        TimeProvider clock,
        IWorkerTransport transport,
        ILogger? logger = null,
        CancellationToken hostStopping = default)
        => new(
            state,
            store,
            provider.GetRequiredService<IAsyncResponseBuilder>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>(),
            options,
            provider.GetRequiredService<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            logger ?? NullLogger.Instance,
            lease,
            clock,
            workerTransport: transport,
            hostStopping: hostStopping);

    /// <summary>Waits (real time, bounded) until a wait of at most <paramref name="within"/> is armed on the virtual clock.</summary>
    public static async Task WaitForArmedTimerAsync(VirtualTimeProvider clock, TimeSpan within)
    {
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (!(clock.NextTimerDueAt is { } due && due <= clock.GetUtcNow() + within))
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException($"No timer due within {within} was armed (next: {clock.NextTimerDueAt:O}).");
            await Task.Delay(5);
        }
    }

    /// <summary>A host lifetime whose ApplicationStopping the test fires.</summary>
    public sealed class StoppingHost : Microsoft.Extensions.Hosting.IHostApplicationLifetime, IDisposable
    {
        private readonly CancellationTokenSource _stopping = new();

        public CancellationToken ApplicationStarted => CancellationToken.None;

        public CancellationToken ApplicationStopping => _stopping.Token;

        public CancellationToken ApplicationStopped => CancellationToken.None;

        public void StopApplication() => _stopping.Cancel();

        public void Dispose() => _stopping.Dispose();
    }

    /// <summary>
    /// A worker transport WITHOUT native delayed delivery (the Kafka / RabbitMQ / Pub/Sub / Redis /
    /// NATS shape): every durable timer waits in process. Records what is published and can fail
    /// publishes on demand.
    /// </summary>
    public class RecordingTransport : IWorkerTransport
    {
        private readonly List<WorkerJobEnvelope> _jobs = [];

        /// <summary>When set, the next publishes throw it (until cleared).</summary>
        public volatile Exception? FailPublishesWith;

        public IReadOnlyList<WorkerJobEnvelope> Jobs
        {
            get { lock (_jobs) return [.. _jobs]; }
        }

        public int Count
        {
            get { lock (_jobs) return _jobs.Count; }
        }

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            if (FailPublishesWith is { } failure)
                return Task.FromException(failure);

            lock (_jobs)
                _jobs.Add(job);
            return Task.CompletedTask;
        }
    }

    /// <summary>The same transport on a broker that caps how long one delivery may stay in flight.</summary>
    public sealed class CeilingTransport(TimeSpan? ceiling) : RecordingTransport, IWorkerTransportInFlightLimit
    {
        public TimeSpan? MaxInFlightDuration { get; } = ceiling;
    }

    /// <summary>A transport WITH native delayed delivery: long timers suspend on a delayed wake-up.</summary>
    public sealed class RecordingDelayedTransport : RecordingTransport, IDelayedWorkerTransport
    {
        public TimeSpan MaxPublishDelay => TimeSpan.FromDays(30);

        public Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default)
            => PublishAsync(job, cancellationToken);
    }
}
