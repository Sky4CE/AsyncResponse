using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The metrics layer (P4): a <see cref="Meter"/> alongside the tracing source so operators can graph
/// the library's core SLOs — chiefly "how often does recovery fire" and its resume-vs-fail split —
/// plus timeouts and worker outcomes. Verified by listening to the real meter.
/// </summary>
public class MetricsTests
{
    private sealed record Measurement(string Instrument, long Value, Dictionary<string, object?> Tags);

    // Collects all AsyncResponse meter measurements (counters during the action, observable gauges on
    // demand). Assertions use Contains, tolerating extra measurements from tests running in parallel.
    private static async Task<List<Measurement>> CollectAsync(Func<Task> action)
    {
        var measurements = new List<Measurement>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == AsyncResponseDiagnostics.MeterName)
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var tagDict = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var tag in tags)
                tagDict[tag.Key] = tag.Value;
            lock (measurements)
                measurements.Add(new Measurement(instrument.Name, value, tagDict));
        });
        listener.Start();

        await action();

        listener.RecordObservableInstruments();
        lock (measurements)
            return [.. measurements];
    }

    [Fact]
    public async Task LostSubscriberDispatch_IsRecorded_WithKindAndRouteTags()
    {
        var provider = CreateInMemoryProvider();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        var measurements = await CollectAsync(() =>
            publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "metrics-no-subscriber"));

        Assert.Contains(measurements, m =>
            m.Instrument == "asyncresponse.lost_subscriber.dispatches"
            && (string?)m.Tags["kind"] == "response"
            && m.Tags.ContainsKey("route")
            && m.Tags.ContainsKey("invoked"));
    }

    [Fact]
    public async Task WaiterTimeout_IsRecorded()
    {
        var provider = CreateInMemoryProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();

        var measurements = await CollectAsync(async () =>
        {
            await using var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
                "metrics-timeout", timeout: TimeSpan.FromMilliseconds(20));
            await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask);
        });

        Assert.Contains(measurements, m => m.Instrument == "asyncresponse.waiter.timeouts");
    }

    [Fact]
    public async Task WorkerOutcome_Rejected_IsRecorded()
    {
        await using var provider = new ServiceCollection().BuildServiceProvider();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<WorkerJobExecutor>.Instance);
        var job = new WorkerJobEnvelope
        {
            SchemaVersion = WorkerJobEnvelopeSchema.Current + 1,
            Call = new ReflectionCallDto { ServiceInterfaceFullName = "Svc", MethodName = "M", Params = [] },
            CorrelationId = "metrics-worker"
        };

        var measurements = await CollectAsync(() =>
            Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync(job)));

        Assert.Contains(measurements, m =>
            m.Instrument == "asyncresponse.worker.jobs" && (string?)m.Tags["outcome"] == "rejected");
    }

    [Fact]
    public async Task WatchdogGauges_AreRegisteredAndObservable()
    {
        var state = new AsyncResponseWatchdogState();
        AsyncResponseDiagnostics.EnsureWatchdogGauges(state);
        state.Publish(new AsyncResponseWatchdogSnapshot(
            DateTime.UtcNow, TimeSpan.FromHours(6),
            new AsyncResponseWatchdogReport(5, 3, [], 0), Error: null));

        var measurements = await CollectAsync(() => Task.CompletedTask);

        // The gauges are registered process-wide (bound to whichever watchdog state registered first),
        // so assert presence rather than exact values to stay order-independent across the suite.
        Assert.Contains(measurements, m => m.Instrument == "asyncresponse.recovery.outstanding");
        Assert.Contains(measurements, m => m.Instrument == "asyncresponse.recovery.stale");
        Assert.Contains(measurements, m => m.Instrument == "asyncresponse.recovery.active_waiters");
    }

    private static ServiceProvider CreateInMemoryProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse().WithInMemoryChannel(options =>
        {
            options.DefaultTimeout = TimeSpan.FromSeconds(5);
            options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
        });
        return services.BuildServiceProvider();
    }
}
