using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
