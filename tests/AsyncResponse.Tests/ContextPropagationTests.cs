using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Ambient-context propagation across the async-response boundary:
/// <list type="bullet">
/// <item><description>serializable baggage via <see cref="IAsyncResponseContextPropagator"/> for the
/// cross-process paths (worker via ingress, lost-subscriber recovery);</description></item>
/// <item><description>automatic <see cref="System.Threading.ExecutionContext"/> flow for the
/// in-process paths (response handler, in-memory worker).</description></item>
/// </list>
/// </summary>
public class ContextPropagationTests
{
    // ----- Baggage (cross-process) -----

    [Fact]
    public async Task EnqueueWorker_CapturesBaggageIntoEnvelope()
    {
        WorkerJobEnvelope? published = null;
        var transport = new Mock<IWorkerTransport>();
        transport
            .Setup(t => t.PublishAsync(It.IsAny<WorkerJobEnvelope>(), It.IsAny<CancellationToken>()))
            .Callback<WorkerJobEnvelope, CancellationToken>((job, _) => published = job)
            .Returns(Task.CompletedTask);

        var propagation = new AsyncResponseContextPropagation([new BaggagePropagator()]);
        var builder = new AsyncResponseBuilder(Mock.Of<IAsyncResponseSubscriber>(), transport.Object, null, propagation);

        BaggagePropagator.Set("hello");
        try
        {
            await builder.EnqueueWorkerAsync<IBaggageProbe>(p => p.Observe(7));
        }
        finally
        {
            BaggagePropagator.Set(null);
        }

        Assert.NotNull(published);
        Assert.Equal("hello", published!.Context?[BaggagePropagator.Key]);
    }

    [Fact]
    public async Task WithoutPropagator_EnvelopeContextIsNull()
    {
        WorkerJobEnvelope? published = null;
        var transport = new Mock<IWorkerTransport>();
        transport
            .Setup(t => t.PublishAsync(It.IsAny<WorkerJobEnvelope>(), It.IsAny<CancellationToken>()))
            .Callback<WorkerJobEnvelope, CancellationToken>((job, _) => published = job)
            .Returns(Task.CompletedTask);

        // No propagation aggregator at all (matches an app that registers no propagator).
        var builder = new AsyncResponseBuilder(Mock.Of<IAsyncResponseSubscriber>(), transport.Object);

        await builder.EnqueueWorkerAsync<IBaggageProbe>(p => p.Observe(7));

        Assert.NotNull(published);
        Assert.Null(published!.Context);
    }

    [Fact]
    public async Task WorkerBaggage_SurvivesSerialization_AndIsRestoredByIngress()
    {
        var probe = new BaggageProbe();
        var provider = BuildProvider(probe, b => b.WithInMemoryChannel().WithContextPropagator<BaggagePropagator>());
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        // The realistic cross-process shape: a JSON envelope (with baggage) arriving from a broker.
        var json = JsonSerializer.Serialize(new WorkerJobEnvelope
        {
            Call = Call(nameof(IBaggageProbe.Observe), 7),
            CorrelationId = "cid",
            Context = new Dictionary<string, string> { [BaggagePropagator.Key] = "hello" }
        });

        BaggagePropagator.Set(null);
        await ingress.HandleWorkerMessageAsync(json);

        var observed = await probe.Done.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("hello", observed);                 // restored before the job ran
        Assert.Null(BaggagePropagator.Current);          // scope disposed after the job
    }

    [Fact]
    public async Task RecoveryCallback_RestoresBaggageFromRecoveryState()
    {
        var probe = new BaggageProbe();
        var provider = BuildProvider(probe, b => b.WithInMemoryChannel().WithContextPropagator<BaggagePropagator>());
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        await store.SaveAsync(
            "cid",
            new RecoveryState
            {
                CorrelationId = "cid",
                PayloadTypeFullName = typeof(OperationResult).FullName,
                Context = new Dictionary<string, string> { [BaggagePropagator.Key] = "hello" },
                ResumeCallback = Call(nameof(IBaggageProbe.Observe), 7)
            },
            TimeSpan.FromMinutes(5));

        BaggagePropagator.Set(null);
        // No subscriber → lost-subscriber recovery; Completed routes to the resume callback.
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, "cid");

        var observed = await probe.Done.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("hello", observed);
        Assert.Null(BaggagePropagator.Current);
    }

    // ----- ExecutionContext (in-process) -----

    [Fact]
    public async Task InMemoryResponseHandler_RunsUnderSubscribeTimeExecutionContext()
    {
        var provider = BuildProvider(new BaggageProbe(), b => b.WithInMemoryChannel());
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        var ambient = new AsyncLocal<string?> { Value = "subscribe-ctx" };
        string? observedInPredicate = null;
        string? correlationId = null;
        var armed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var waitTask = asyncResponse
            .For<OperationResult>()
            .Until(r =>
            {
                observedInPredicate = ambient.Value;
                return r.Status != OperationStatus.Running;
            })
            .WaitAsync(ctx =>
            {
                correlationId = ctx.CorrelationId;
                armed.SetResult();
                return Task.CompletedTask;
            });

        await armed.Task;
        ambient.Value = "publisher-ctx"; // change the ambient in the publishing flow
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, correlationId!);
        await waitTask.WaitAsync(TimeSpan.FromSeconds(5));

        // The Until predicate ran under the subscribe-time ExecutionContext, not the publisher's.
        Assert.Equal("subscribe-ctx", observedInPredicate);
    }

    [Fact]
    public async Task InMemoryWorker_RunsJobUnderEnqueueTimeExecutionContext()
    {
        var probe = new EcProbe();
        var provider = BuildProvider(probe, b => b.WithInMemoryChannel().WithInMemoryTransport()); // no propagator
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var host = provider.GetServices<IHostedService>().OfType<InMemoryWorkerHost>().Single();

        await host.StartAsync(CancellationToken.None);
        try
        {
            EcProbe.Ambient.Value = "enqueue-ctx";
            await asyncResponse.EnqueueWorkerAsync<IEcProbe>(p => p.Observe(7));
            EcProbe.Ambient.Value = null;

            var observed = await probe.Done.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("enqueue-ctx", observed); // ambient AsyncLocal flowed via the captured ExecutionContext
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task MultiplePropagators_BothRestoredThroughIngress()
    {
        var probe = new BaggageProbe();
        var provider = BuildProvider(probe, b => b
            .WithInMemoryChannel()
            .WithContextPropagator<BaggagePropagator>()
            .WithContextPropagator<TenantPropagator>());
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        var json = JsonSerializer.Serialize(new WorkerJobEnvelope
        {
            Call = Call(nameof(IBaggageProbe.Observe), 7),
            CorrelationId = "cid",
            Context = new Dictionary<string, string>
            {
                [BaggagePropagator.Key] = "trace-1",
                [TenantPropagator.Key] = "acme",
            },
        });

        BaggagePropagator.Set(null);
        TenantPropagator.Set(null);
        await ingress.HandleWorkerMessageAsync(json);

        var trace = await probe.Done.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("trace-1", trace);
        Assert.Equal("acme", probe.Tenant);
        Assert.Null(BaggagePropagator.Current);   // both scopes disposed after the job
        Assert.Null(TenantPropagator.Current);
    }

    [Fact]
    public async Task InMemoryWorker_FlowsViaExecutionContext_WithoutInvokingPropagatorRestore()
    {
        var probe = new BaggageProbe();
        var provider = BuildProvider(probe, b => b
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithContextPropagator<BaggagePropagator>());
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var host = provider.GetServices<IHostedService>().OfType<InMemoryWorkerHost>().Single();

        await host.StartAsync(CancellationToken.None);
        try
        {
            BaggagePropagator.Reset("via-ec");
            await asyncResponse.EnqueueWorkerAsync<IBaggageProbe>(p => p.Observe(7));
            BaggagePropagator.Set(null);

            var observed = await probe.Done.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("via-ec", observed);                 // flowed via the captured ExecutionContext
            Assert.Equal(0, BaggagePropagator.RestoreCalls);  // baggage Restore is NOT used for the in-process worker
        }
        finally
        {
            await host.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task RecoveryFailureCallback_RestoresBaggage_OnSetException()
    {
        var probe = new BaggageProbe();
        var provider = BuildProvider(probe, b => b.WithInMemoryChannel().WithContextPropagator<BaggagePropagator>());
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        await store.SaveAsync(
            "cid",
            new RecoveryState
            {
                CorrelationId = "cid",
                Context = new Dictionary<string, string> { [BaggagePropagator.Key] = "hello" },
                FailureCallback = Call(nameof(IBaggageProbe.Observe), 7),
            },
            TimeSpan.FromMinutes(5));

        BaggagePropagator.Set(null);
        await publisher.SetException(new InvalidOperationException("boom"), "cid"); // no subscriber → failure callback

        var observed = await probe.Done.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("hello", observed);
    }

    [Fact]
    public async Task RecoveryFailureCallback_RestoresBaggage_OnDomainFailure()
    {
        var probe = new BaggageProbe();
        var provider = BuildProvider(probe, b => b.WithInMemoryChannel().WithContextPropagator<BaggagePropagator>());
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        await store.SaveAsync(
            "cid",
            new RecoveryState
            {
                CorrelationId = "cid",
                PayloadTypeFullName = typeof(OperationResult).FullName,
                Context = new Dictionary<string, string> { [BaggagePropagator.Key] = "hello" },
                FailureCallback = Call(nameof(IBaggageProbe.Observe), 7),
            },
            TimeSpan.FromMinutes(5));

        BaggagePropagator.Set(null);
        // A Failed payload with no subscriber routes to the failure callback via the dispatcher.
        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Failed }, "cid");

        var observed = await probe.Done.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("hello", observed);
    }

    [Fact]
    public async Task EnqueueWorker_BaggageRoundTrips_CaptureSerializeRestore()
    {
        WorkerJobEnvelope? published = null;
        var transport = new Mock<IWorkerTransport>();
        transport
            .Setup(t => t.PublishAsync(It.IsAny<WorkerJobEnvelope>(), It.IsAny<CancellationToken>()))
            .Callback<WorkerJobEnvelope, CancellationToken>((job, _) => published = job)
            .Returns(Task.CompletedTask);

        var probe = new BaggageProbe();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IBaggageProbe>(probe);
        services.AddSingleton(transport.Object);
        services.AddAsyncResponse().WithInMemoryChannel().WithContextPropagator<BaggagePropagator>();
        var provider = services.BuildServiceProvider();
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        BaggagePropagator.Set("hello");
        await asyncResponse.EnqueueWorkerAsync<IBaggageProbe>(p => p.Observe(7)); // captured into the envelope
        BaggagePropagator.Set(null);

        // Cross the wire (broker → ingress) and execute on the far side.
        await ingress.HandleWorkerMessageAsync(JsonSerializer.Serialize(published));

        var observed = await probe.Done.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal("hello", observed);
    }

    [Fact]
    public async Task CreateResponseWaiter_CapturesAmbientIntoRecoveryState()
    {
        var provider = BuildProvider(new BaggageProbe(), b => b.WithInMemoryChannel().WithContextPropagator<BaggagePropagator>());
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var store = provider.GetRequiredService<IRecoveryStateStore>();

        BaggagePropagator.Set("hello");
        string? correlationId = null;
        var armed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // Arm a waiter in the background; the trigger reports the generated id once recovery state exists.
        _ = asyncResponse.For<OperationResult>().WaitAsync(ctx =>
        {
            correlationId = ctx.CorrelationId;
            armed.SetResult();
            return Task.CompletedTask;
        });
        await armed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var state = Assert.Single(await store.GetAllAsync(correlationId!));
        Assert.Equal("hello", state.Context?[BaggagePropagator.Key]);
    }

    [Fact]
    public async Task PropagatorRegistered_NothingAmbient_EnvelopeContextIsNull()
    {
        WorkerJobEnvelope? published = null;
        var transport = new Mock<IWorkerTransport>();
        transport
            .Setup(t => t.PublishAsync(It.IsAny<WorkerJobEnvelope>(), It.IsAny<CancellationToken>()))
            .Callback<WorkerJobEnvelope, CancellationToken>((job, _) => published = job)
            .Returns(Task.CompletedTask);

        var propagation = new AsyncResponseContextPropagation([new BaggagePropagator()]);
        var builder = new AsyncResponseBuilder(Mock.Of<IAsyncResponseSubscriber>(), transport.Object, null, propagation);

        BaggagePropagator.Set(null); // nothing ambient to capture
        await builder.EnqueueWorkerAsync<IBaggageProbe>(p => p.Observe(7));

        Assert.NotNull(published);
        Assert.Null(published!.Context); // empty carrier collapses to null, not an empty dictionary
    }

    [Fact]
    public void Capture_ExposesDictionarySemanticsToPropagators()
    {
        var propagation = new AsyncResponseContextPropagation([new CarrierInspectionPropagator()]);

        Assert.Null(propagation.Capture());
    }

    // ----- helpers -----

    private static ServiceProvider BuildProvider<TProbe>(TProbe probe, Action<AsyncResponseRegistrationBuilder> configure)
        where TProbe : class
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(probe);
        if (probe is IBaggageProbe baggage) services.AddSingleton(baggage);
        if (probe is IEcProbe ec) services.AddSingleton(ec);
        configure(services.AddAsyncResponse());
        return services.BuildServiceProvider();
    }

    private static ReflectionCallDto Call(string method, int arg) => new()
    {
        ServiceInterfaceFullName = typeof(IBaggageProbe).FullName!,
        MethodName = method,
        Params = [CallbackParam.ForValue(arg)]
    };
}

/// <summary>Serializable-baggage propagator backed by an <see cref="AsyncLocal{T}"/>, with a
/// restore-call counter so tests can assert which boundary restored it.</summary>
public sealed class BaggagePropagator : IAsyncResponseContextPropagator
{
    public const string Key = "test.baggage";
    private static readonly AsyncLocal<string?> _value = new();
    private static int _restoreCalls;

    public static string? Current => _value.Value;
    public static int RestoreCalls => Volatile.Read(ref _restoreCalls);

    public static void Set(string? value) => _value.Value = value;

    public static void Reset(string? value = null)
    {
        _value.Value = value;
        Interlocked.Exchange(ref _restoreCalls, 0);
    }

    public void Capture(IDictionary<string, string> carrier)
    {
        if (_value.Value is { } v) carrier[Key] = v;
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> carrier)
    {
        Interlocked.Increment(ref _restoreCalls);
        if (!carrier.TryGetValue(Key, out var value))
            return NullScope.Instance;

        var previous = _value.Value;
        _value.Value = value;
        return new Restorer(previous);
    }

    private sealed class Restorer(string? _previous) : IDisposable
    {
        public void Dispose() => _value.Value = _previous;
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose() { }
    }
}

/// <summary>A second baggage propagator (different namespaced key) for composition tests.</summary>
public sealed class TenantPropagator : IAsyncResponseContextPropagator
{
    public const string Key = "test.tenant";
    private static readonly AsyncLocal<string?> _value = new();

    public static string? Current => _value.Value;
    public static void Set(string? value) => _value.Value = value;

    public void Capture(IDictionary<string, string> carrier)
    {
        if (_value.Value is { } v) carrier[Key] = v;
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> carrier)
    {
        if (!carrier.TryGetValue(Key, out var value))
            return NullScope.Instance;

        var previous = _value.Value;
        _value.Value = value;
        return new Restorer(previous);
    }

    private sealed class Restorer(string? _previous) : IDisposable
    {
        public void Dispose() => _value.Value = _previous;
    }

    private sealed class NullScope : IDisposable
    {
        public static readonly NullScope Instance = new();
        public void Dispose() { }
    }
}

public sealed class CarrierInspectionPropagator : IAsyncResponseContextPropagator
{
    public void Capture(IDictionary<string, string> carrier)
    {
        Assert.False(carrier.ContainsKey("missing"));
        Assert.False(carrier.TryGetValue("missing", out _));
        Assert.Empty(carrier.Keys);
        Assert.Empty(carrier.Values);

        carrier.Add("first", "1");
        Assert.Equal("1", carrier["first"]);
        Assert.True(carrier.Contains(new KeyValuePair<string, string>("first", "1")));

        var copy = new KeyValuePair<string, string>[1];
        carrier.CopyTo(copy, 0);
        Assert.Equal(new KeyValuePair<string, string>("first", "1"), copy[0]);
        Assert.True(carrier.Remove(new KeyValuePair<string, string>("first", "1")));

        carrier["second"] = "2";
        Assert.True(carrier.Remove("second"));

        carrier.Add(new KeyValuePair<string, string>("third", "3"));
        Assert.Collection(carrier, item => Assert.Equal(new KeyValuePair<string, string>("third", "3"), item));
        carrier.Clear();
    }

    public IDisposable Restore(IReadOnlyDictionary<string, string> carrier) => new Scope();

    private sealed class Scope : IDisposable
    {
        public void Dispose() { }
    }
}

public interface IBaggageProbe
{
    Task Observe(int n);
}

public sealed class BaggageProbe : IBaggageProbe
{
    public readonly TaskCompletionSource<string?> Done = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>The tenant baggage observed during execution (for composition tests).</summary>
    public string? Tenant;

    public Task Observe(int n)
    {
        Tenant = TenantPropagator.Current;
        Done.TrySetResult(BaggagePropagator.Current);
        return Task.CompletedTask;
    }
}

public interface IEcProbe
{
    Task Observe(int n);
}

public sealed class EcProbe : IEcProbe
{
    public static readonly AsyncLocal<string?> Ambient = new();
    public readonly TaskCompletionSource<string?> Done = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Observe(int n)
    {
        Done.TrySetResult(Ambient.Value);
        return Task.CompletedTask;
    }
}
