using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Diagnostics.Metrics;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round-36 pins over API the round introduced — they do not compile against the pre-fix tree, so
/// they live apart from the behavior pins in <see cref="Round36RegressionTests"/>.
/// </summary>
public sealed class Round36NewApiTests
{
    private static WorkerJobEnvelope Job() => new()
    {
        Call = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "AsyncResponse.Tests.IRound36Probe",
            MethodName = "RunAsync",
            Params = []
        }
    };

    private static Task<int> PublishInJobAsync(InMemoryWorkerTransport transport, int count)
        => Task.Run(async () =>
        {
            InMemoryWorkerTransport.InJobScope.MarkActive();
            var accepted = 0;
            for (var i = 0; i < count; i++)
            {
                try
                {
                    await transport.PublishAsync(Job());
                    accepted++;
                }
                catch (InvalidOperationException)
                {
                    break;
                }
            }

            return accepted;
        });

    [Fact]
    public async Task InJobOverflowCapacity_BoundsTheOverflow_AndAWorkerFreeingASlotReopensIt()
    {
        var transport = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions
        {
            QueueCapacity = 1,
            InJobOverflowCapacity = 8
        }));

        // One queue slot plus eight overflow entries; the tenth is rejected.
        Assert.Equal(9, await PublishInJobAsync(transport, 20));
        Assert.Equal(8, transport.OverflowDepth);
        Assert.Equal(9, transport.OutstandingJobs);

        // A worker takes the queued job and pumps the overflow: one entry moves into the queue,
        // the depth drops, and the next follow-up publish is admitted again.
        Assert.True(transport.Reader.TryRead(out _));
        transport.PumpOverflow();
        Assert.Equal(7, transport.OverflowDepth);
        Assert.Equal(1, await PublishInJobAsync(transport, 1));
        Assert.Equal(8, transport.OverflowDepth);
    }

    [Fact]
    public async Task InJobOverflowCapacity_Zero_RejectsTheFirstFollowUpThatFindsTheQueueFull()
    {
        var transport = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions
        {
            QueueCapacity = 2,
            InJobOverflowCapacity = 0
        }));

        Assert.Equal(2, await PublishInJobAsync(transport, 5));
        Assert.Equal(0, transport.OverflowDepth);
    }

    [Fact]
    public void InJobOverflowCapacity_Negative_IsRejectedAtConstruction()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => new InMemoryWorkerTransport(
            Options.Create(new InMemoryWorkerTransportOptions { InJobOverflowCapacity = -1 })));

        Assert.Contains(nameof(InMemoryWorkerTransportOptions.InJobOverflowCapacity), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task OverflowRejections_AndDepth_AreObservableOnTheMeter()
    {
        var measurements = new List<(string Instrument, long Value)>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == AsyncResponseDiagnostics.MeterName)
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            lock (measurements)
                measurements.Add((instrument.Name, value));
        });
        listener.Start();

        var transport = new InMemoryWorkerTransport(Options.Create(new InMemoryWorkerTransportOptions
        {
            QueueCapacity = 1,
            InJobOverflowCapacity = 3
        }));
        Assert.Equal(4, await PublishInJobAsync(transport, 6));
        listener.RecordObservableInstruments();

        lock (measurements)
        {
            Assert.Contains(measurements, m => m.Instrument == "asyncresponse.worker.inmemory_overflow_rejections" && m.Value == 1);
            // Summed over every live transport in the process, so at least this one's three.
            Assert.Contains(measurements, m => m.Instrument == "asyncresponse.worker.inmemory_overflow_depth" && m.Value >= 3);
        }

        GC.KeepAlive(transport);
    }

    // ---------------------------------------------------------------------------------------------
    // Ancestor-depth ceiling: past MaxAncestorLedgerDepth the run fails terminally instead of the
    // chain being truncated in silence.

    private sealed record DepthInput(int Value);

    private sealed class DepthFlow : IDurableFlow<DepthInput>
    {
        public Task ExecuteAsync(IDurableFlowContext flow, DepthInput input) => Task.CompletedTask;
    }

    private sealed class RecordingDelayedTransport : IDelayedWorkerTransport
    {
        public int Published;

        public TimeSpan MaxPublishDelay => TimeSpan.FromDays(30);

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref Published);
            return Task.CompletedTask;
        }

        public Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default)
            => PublishAsync(job, cancellationToken);
    }

    [Fact]
    public async Task AncestorExtension_ChainDeeperThanTheCeiling_FailsTheRunTerminally()
    {
        Assert.Equal(256, DurableFlowContext.MaxAncestorLedgerDepth);

        var clock = new VirtualTimeProvider();
        var transport = new RecordingDelayedTransport();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<TimeProvider>(clock);
        services.AddAsyncResponse().WithInMemoryChannel().WithInMemoryDurableFlows();
        services.AddSingleton<IWorkerTransport>(transport);
        await using var provider = services.BuildServiceProvider();
        var store = provider.GetRequiredService<IFlowStateStore>();

        string? parent = null;
        FlowState leaf = null!;
        for (var level = 0; level <= DurableFlowContext.MaxAncestorLedgerDepth + 1; level++)
        {
            var id = parent is null ? "r36-ceiling" : $"{parent}:c";
            leaf = new FlowState
            {
                FlowId = id,
                ParentFlowId = parent,
                ParentStepName = parent is null ? null : "c",
                Status = FlowRunStatus.Running,
                FlowTypeName = typeof(DepthFlow).FullName,
                InputTypeName = typeof(DepthInput).FullName,
                InputJson = "{\"Value\":1}"
            };
            Assert.True(await store.TryCreateAsync(id, leaf, TimeSpan.FromMinutes(1)));
            parent = id;
        }

        var options = new DurableFlowOptions { StateExpiry = TimeSpan.FromMinutes(1), TimerInProcessThreshold = TimeSpan.Zero };
        await using var lease = (await FlowStateConcurrency.TryAcquireExecutionLeaseAsync(store, leaf.FlowId!, options, NullLogger.Instance, clock))!;
        var context = new DurableFlowContext(
            leaf,
            store,
            provider.GetRequiredService<IAsyncResponseBuilder>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>(),
            options,
            provider.GetRequiredService<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            NullLogger.Instance,
            lease,
            clock,
            workerTransport: transport);

        var ex = await Assert.ThrowsAsync<DurableFlowFailedException>(() => context.DelayAsync("long-wait", TimeSpan.FromHours(1)));

        Assert.Contains("nested more than 256", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, transport.Published);
    }
}
