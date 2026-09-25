using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression (r24): the <see cref="IFlowStateStore"/> interface forward was ALWAYS scoped, so a
/// provider-package singleton store resolved through it was captured into every flow-execution
/// scope's disposable list — MS.DI's CaptureDisposable runs on whatever a scoped call site
/// returns, regardless of who owns the instance. The FIRST scope's disposal disposed the store
/// (and any connection it owned, e.g. the PostgreSQL store's NpgsqlDataSource on the documented
/// ConnectionString path), and every later durable-flow operation threw ObjectDisposedException
/// until process restart. The forward now mirrors the concrete registration's lifetime.
/// </summary>
public sealed class StoreLifetimeForwardTests
{
    [Fact]
    public async Task SingletonStore_ResolvedThroughTheForward_IsNotDisposedByAScope()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        // Mirrors the provider packages: the concrete store is pre-registered as a singleton
        // BEFORE the seam adds its forward.
        services.TryAddSingleton<DisposableStubStore>();
        services.AddAsyncResponse().WithInMemoryChannel().WithDurableFlows<DisposableStubStore>();
        var provider = services.BuildServiceProvider();

        var singleton = provider.GetRequiredService<DisposableStubStore>();
        using (var scope = provider.CreateScope())
            Assert.Same(singleton, scope.ServiceProvider.GetRequiredService<IFlowStateStore>());

        // On the old code the singleton was already dead here — its DisposeAsync ran with the
        // first scope's disposal.
        Assert.False(singleton.Disposed);

        using (var scope = provider.CreateScope())
            Assert.Same(singleton, scope.ServiceProvider.GetRequiredService<IFlowStateStore>());
        Assert.False(singleton.Disposed);

        // Root disposal still owns (and disposes) the singleton.
        await provider.DisposeAsync();
        Assert.True(singleton.Disposed);
    }

    [Fact]
    public void UnregisteredStore_KeepsTheScopedDefault_AndScopesOwnTheirInstance()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        // No pre-registration: the seam's TryAddScoped default wins, each scope builds its own
        // store, and scope disposal correctly disposes it — the pre-r24 behavior for app-owned
        // scoped stores is unchanged.
        services.AddAsyncResponse().WithInMemoryChannel().WithDurableFlows<DisposableStubStore>();
        using var provider = services.BuildServiceProvider();

        DisposableStubStore scoped;
        using (var scope = provider.CreateScope())
        {
            scoped = Assert.IsType<DisposableStubStore>(scope.ServiceProvider.GetRequiredService<IFlowStateStore>());
            Assert.Same(scoped, scope.ServiceProvider.GetRequiredService<DisposableStubStore>());
        }

        Assert.True(scoped.Disposed);
    }

    [Fact]
    public async Task StartupProbe_ScopedStoreThatIsOnlyAsyncDisposable_StartsAndDisposesIt()
    {
        // Regression: the startup validator's store probe disposed its scope SYNCHRONOUSLY, and
        // MS.DI's synchronous scope disposal throws for a captured service that implements only
        // IAsyncDisposable ("... Use DisposeAsync"). An application store on the scoped default
        // that owns, say, a data source therefore failed the host start with a misleading error,
        // although every runtime flow scope (CreateAsyncScope) disposes it cleanly.
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<StoreDisposalLog>();
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithDurableFlows<AsyncOnlyDisposableStubStore>();
        await using var provider = services.BuildServiceProvider();
        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        await validator.StartAsync(CancellationToken.None);

        // The probe still built the store (options validation in its constructor) and released it
        // asynchronously. (The scope tracks the instance twice — once for the concrete scoped
        // registration and once for the scoped IFlowStateStore forward that returned it — so
        // an idempotent DisposeAsync runs once per tracking entry.)
        Assert.True(provider.GetRequiredService<StoreDisposalLog>().AsyncDisposals >= 1);
    }

    [Fact]
    public async Task KeyedRegistrationOfTheConcreteStore_DoesNotDecideTheForwardLifetime()
    {
        // Regression: the forward-lifetime snapshot (Last(ServiceType == TStore)) and the startup
        // re-check did not skip keyed descriptors, although MS.DI's non-keyed resolution — the one
        // the forward performs — never picks a keyed one. A keyed scoped registration after the
        // app's singleton made the IFlowStateStore forward Scoped over a root singleton: the first
        // flow-execution scope's disposal disposed the store for the whole process (the r24 P1),
        // and the re-check read the same keyed descriptor and stayed silent.
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<DisposableStubStore>();
        services.AddKeyedScoped<DisposableStubStore>("reporting");
        services.AddAsyncResponse().WithInMemoryChannel().WithInMemoryTransport().WithDurableFlows<DisposableStubStore>();
        await using var provider = services.BuildServiceProvider();

        var singleton = provider.GetRequiredService<DisposableStubStore>();
        using (var scope = provider.CreateScope())
            Assert.Same(singleton, scope.ServiceProvider.GetRequiredService<IFlowStateStore>());
        Assert.False(singleton.Disposed);

        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();
        await validator.StartAsync(CancellationToken.None);
    }

    [Fact]
    public async Task KeyedRegistrationAddedAfterTheFluentChain_DoesNotFailTheLifetimeRecheck()
    {
        // The re-check's other direction: a keyed registration of the concrete store type added
        // after the chain is not the store the forward resolves, so it must not read as "the store
        // registration was changed after the fluent chain".
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<DisposableStubStore>();
        services.AddAsyncResponse().WithInMemoryChannel().WithInMemoryTransport().WithDurableFlows<DisposableStubStore>();
        services.AddKeyedScoped<DisposableStubStore>("reporting");
        await using var provider = services.BuildServiceProvider();

        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();
        await validator.StartAsync(CancellationToken.None);
    }

    /// <summary>Counts the probe store's disposals (a singleton the scoped store reports to).</summary>
    public sealed class StoreDisposalLog
    {
        private int _asyncDisposals;

        public int AsyncDisposals => Volatile.Read(ref _asyncDisposals);

        public void RecordAsyncDisposal() => Interlocked.Increment(ref _asyncDisposals);
    }

    /// <summary>
    /// An application store that owns an async-only resource: <see cref="IAsyncDisposable"/> and
    /// NOT <see cref="IDisposable"/>. Flow I/O is never exercised, so every member throws.
    /// </summary>
    public sealed class AsyncOnlyDisposableStubStore(StoreDisposalLog log) : IFlowStateStore, IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            log.RecordAsyncDisposal();
            return ValueTask.CompletedTask;
        }

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
    }

    /// <summary>Lifetime probe only: flow I/O is never exercised, so every member throws.</summary>
    public sealed class DisposableStubStore : IFlowStateStore, IDisposable
    {
        public bool Disposed { get; private set; }

        public void Dispose() => Disposed = true;

        public Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<bool> TryUpdateAsync(string flowId, FlowState state, long expectedRevision, TimeSpan ttl, string? leaseId = null, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
    }
}
