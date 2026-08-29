using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace AsyncResponse;

/// <summary>
/// Internal marker registered by each response-channel registration
/// (<c>.WithInMemoryChannel()</c> / <c>.WithRedisChannel()</c>). The
/// <see cref="AsyncResponseStartupValidator"/> asserts exactly one channel is present.
/// </summary>
internal sealed class AsyncResponseChannelMarker(string name)
{
    public string Name { get; } = name;

    /// <summary>
    /// The channel's RESOLVED default waiter timeout (<c>DefaultTimeout ?? RecoveryStateExpiry</c>),
    /// declared by the channel registration from its bound options — the window a timeout-less
    /// wait actually runs for. The startup validator requires the durable-flow ledger TTL to
    /// out-live it, and the flow engine extends a parked ledger by it, without either referencing
    /// channel option types. <c>null</c> when the registration declares nothing; both consumers
    /// then skip their checks.
    /// </summary>
    public TimeSpan? EffectiveDefaultWaitTimeout { get; init; }
}

/// <summary>
/// Internal marker registered by each worker-transport registration
/// (<c>.WithInMemoryTransport()</c> / <c>.WithGooglePubSubTransport(...)</c>). The
/// <see cref="AsyncResponseStartupValidator"/> asserts exactly one transport is present.
/// </summary>
internal sealed class AsyncResponseTransportMarker(string name)
{
    public string Name { get; } = name;

    /// <summary>
    /// Whether the transport's worker subscriber resolved to early ACK (<c>AckAfterEnqueue</c>).
    /// Declared by the transport's registration from its bound options so the startup validator
    /// can veto the combination with durable flows without referencing transport types.
    /// </summary>
    public bool WorkerSubscriberUsesEarlyAck { get; init; }

    /// <summary>Worker ack-mode option path, shown in the startup error.</summary>
    public string? WorkerAckModePath { get; init; }

    /// <summary>Whether the transport's response subscriber resolved to early ACK.</summary>
    public bool ResponseSubscriberUsesEarlyAck { get; init; }

    /// <summary>Response ack-mode option path, shown in the startup warning.</summary>
    public string? ResponseAckModePath { get; init; }
}

/// <summary>Internal marker registered by each durable-flow state-store registration.</summary>
internal sealed class AsyncResponseDurableFlowStoreMarker(
    Type storeType,
    ServiceLifetime? forwardLifetime = null,
    IServiceCollection? services = null)
{
    private IServiceCollection? _services = services;

    public Type StoreType { get; } = storeType;
    public string Name { get; } = storeType.FullName ?? storeType.Name;

    /// <summary>
    /// The <see cref="IFlowStateStore"/> forward mirrors the concrete registration's lifetime as
    /// seen WHEN <c>WithDurableFlows</c> ran. A concrete registration added after the fluent chain
    /// with a different lifetime leaves that snapshot stale — the worst shape being a Scoped
    /// forward to a root singleton, which every flow-execution scope captures as its own
    /// disposable: the first scope's disposal kills the store (and any connection it owns) for
    /// the whole process. MS.DI resolves the concrete last-wins, so the mismatch is re-checked
    /// here against the FINAL collection and fails startup with the ordering fix instead. Holds
    /// the service collection only until the check runs, then releases it.
    /// </summary>
    public void ValidateForwardLifetime()
    {
        var services = Interlocked.Exchange(ref _services, null);
        if (services is null || forwardLifetime is null)
            return;

        var finalLifetime = services.LastOrDefault(descriptor => descriptor.ServiceType == StoreType)?.Lifetime;
        if (finalLifetime is null || finalLifetime == forwardLifetime)
            return;

        throw new InvalidOperationException(
            $"The durable-flow store '{Name}' is registered as {finalLifetime}, but the IFlowStateStore forward mirrored " +
            $"{forwardLifetime} when WithDurableFlows<{StoreType.Name}>() ran — the store registration was added or changed " +
            "after the fluent chain. " +
            (forwardLifetime == ServiceLifetime.Scoped && finalLifetime == ServiceLifetime.Singleton
                ? "A Scoped forward to a Singleton store lets the first flow-execution scope dispose the store (and any connection it owns) for the whole process. "
                : "The forward and the store must share one lifetime, or resolutions through the interface and the concrete type diverge. ") +
            $"Register the store (e.g. services.Add{finalLifetime}<{StoreType.Name}>()) BEFORE the WithDurableFlows call.");
    }
}

/// <summary>
/// Registered by the durable-flow engine and evaluated by <see cref="AsyncResponseStartupValidator"/>
/// at host start: every <see cref="IDurableFlowExecutionObserver"/> must be a singleton, because the
/// singleton flow executor resolves observers once from the root provider and holds them for its
/// lifetime. Failing here names the offending registration and the fix; without it the first flow
/// job dies inside the transport's retry loop with an opaque "Cannot resolve scoped service ...
/// from root provider" (ValidateOnBuild does not descend into the executor's factory registration).
/// Holds the service collection only until the check runs, then releases it.
/// </summary>
internal sealed class DurableFlowObserverLifetimeAudit(IServiceCollection services)
{
    private IServiceCollection? _services = services;

    public void Validate()
    {
        var services = Interlocked.Exchange(ref _services, null);
        if (services is null)
            return;

        var nonSingleton = services.FirstOrDefault(descriptor =>
            descriptor.ServiceType == typeof(IDurableFlowExecutionObserver)
            && descriptor.Lifetime != ServiceLifetime.Singleton);
        if (nonSingleton is not null)
        {
            throw new InvalidOperationException(
                $"{nameof(IDurableFlowExecutionObserver)} '{(nonSingleton.ImplementationType ?? nonSingleton.ImplementationInstance?.GetType())?.Name ?? "(factory registration)"}' " +
                $"is registered as {nonSingleton.Lifetime}, but observers are held by the singleton flow executor for its lifetime and must be " +
                "singletons (services.AddSingleton<IDurableFlowExecutionObserver, ...>()). An observer that needs scoped services should " +
                "create its own scope inside the callback.");
        }
    }
}

/// <summary>
/// Validates at host startup that <c>AddAsyncResponse()</c> was paired with exactly one response
/// channel, one worker transport, and one durable-flow state store. These are mandatory core
/// choices; making each explicit keeps the fluent registration complete and prevents silently
/// unusable services from reaching production.
/// </summary>
internal sealed class AsyncResponseStartupValidator(
    IEnumerable<AsyncResponseChannelMarker> _channels,
    IEnumerable<AsyncResponseTransportMarker> _transports,
    IEnumerable<AsyncResponseDurableFlowStoreMarker> _flowStores,
    IOptions<AsyncResponseOptions> _options,
    IEnumerable<DurableFlowOptions>? _flowOptions = null,
    ILogger<AsyncResponseStartupValidator>? _logger = null,
    DurableFlowObserverLifetimeAudit? _observerAudit = null,
    IServiceProvider? _serviceProvider = null) : IHostedService
{
    /// <summary>Starts this service.</summary>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ValidateWatchdogOptions(_options.Value.Watchdog);
        ValidateInboundMessageBudget(_options.Value);
        _observerAudit?.Validate();

        var channelNames = _channels.Select(c => c.Name).Distinct(StringComparer.Ordinal).ToArray();

        if (channelNames.Length == 0)
            throw new InvalidOperationException(
                "AsyncResponse has no response channel registered. After AddAsyncResponse(), call " +
                ".WithInMemoryChannel() (AsyncResponse.Core) or .WithRedisChannel() (AsyncResponse.Channels.Redis). " +
                "Without a channel, waiters can never receive a response.");

        if (channelNames.Length > 1)
            throw new InvalidOperationException(
                $"AsyncResponse has multiple response channels registered ({string.Join(", ", channelNames)}). " +
                "Register exactly one channel.");

        var transportNames = _transports.Select(t => t.Name).Distinct(StringComparer.Ordinal).ToArray();

        if (transportNames.Length == 0)
            throw new InvalidOperationException(
                "AsyncResponse has no worker transport registered. After AddAsyncResponse(), call " +
                ".WithInMemoryTransport() (AsyncResponse.Core), .WithGooglePubSubTransport(...) " +
                "(AsyncResponse.Transports.GooglePubSub), or another full AsyncResponse transport package. " +
                "Without a transport, EnqueueWorkerAsync cannot dispatch worker jobs.");

        if (transportNames.Length > 1)
            throw new InvalidOperationException(
                $"AsyncResponse has multiple worker transports registered ({string.Join(", ", transportNames)}). " +
                "Register exactly one transport.");

        var flowStores = _flowStores.DistinctBy(store => store.StoreType).ToArray();
        if (flowStores.Length == 0)
        {
            throw new InvalidOperationException(
                "AsyncResponse has no durable-flow state store registered. After AddAsyncResponse(), call " +
                ".WithInMemoryDurableFlows() (AsyncResponse.Core), a provider registration such as " +
                ".WithPostgreSqlDurableFlows(...), or .WithDurableFlows<TStore>() for an application-owned store.");
        }

        if (flowStores.Length > 1)
        {
            throw new InvalidOperationException(
                $"AsyncResponse has multiple durable-flow state stores registered ({string.Join(", ", flowStores.Select(store => store.Name))}). " +
                "Register exactly one durable-flow store.");
        }

        foreach (var storeMarker in _flowStores)
            storeMarker.ValidateForwardLifetime();

        ValidateEarlyAckDeclarations();
        ValidateAwaitedStepLedgerCoverage();

        // Construct the flow store once, here: every provider store validates its options in its
        // constructor, but the store is otherwise resolved only inside per-execution scopes — so a
        // misconfigured table name (or any other store option) previously passed startup and first
        // threw inside the worker transport's retry loop, burning a real production run to the
        // delivery cap. Constructors do no I/O by convention; the scope disposes what it built.
        // (Null only in unit tests that construct the validator directly; DI always supplies it.)
        if (_serviceProvider is not null)
        {
            using var scope = _serviceProvider.CreateScope();
            _ = scope.ServiceProvider.GetRequiredService<IFlowStateStore>();
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// An awaited step with no explicit timeout and no <see cref="DurableFlowOptions.DefaultStepTimeout"/>
    /// waits out the CHANNEL's default timeout, so the ledger's idle TTL must out-live that window:
    /// with <see cref="DurableFlowOptions.StateExpiry"/> at or below it, the row (and the lease
    /// renewal anchored on it) is pruned mid-wait — the run becomes unrecoverable and no
    /// step-timeout fault ever fires. Channels declare their resolved default through the marker;
    /// a channel that declares nothing skips the check, and a configured
    /// <c>DefaultStepTimeout</c> makes the channel default unreachable, so the check does not apply.
    /// </summary>
    private void ValidateAwaitedStepLedgerCoverage()
    {
        var flowOptions = _flowOptions?.FirstOrDefault();
        if (flowOptions is null || flowOptions.DefaultStepTimeout is not null)
            return;

        foreach (var channel in _channels)
        {
            if (channel.EffectiveDefaultWaitTimeout is not { } effectiveDefault
                || flowOptions.StateExpiry > effectiveDefault)
                continue;

            throw new InvalidOperationException(
                $"{nameof(DurableFlowOptions)}.{nameof(DurableFlowOptions.StateExpiry)} ({flowOptions.StateExpiry}) does not exceed the " +
                $"{channel.Name} channel's effective default waiter timeout ({effectiveDefault} — DefaultTimeout, or RecoveryStateExpiry " +
                "when DefaultTimeout is null). An awaited step without an explicit timeout waits out that default, and a ledger whose TTL " +
                "does not out-live the wait is pruned mid-wait: the run becomes unrecoverable and no step-timeout fault ever fires. " +
                $"Raise StateExpiry above the channel default, shorten the channel default, or set " +
                $"{nameof(DurableFlowOptions)}.{nameof(DurableFlowOptions.DefaultStepTimeout)}.");
        }
    }

    /// <summary>
    /// Fails fast when a transport's worker subscriber is configured for early ACK: durable-flow
    /// wake-ups ride the worker queue and rely on broker redelivery for crash recovery (the
    /// executor's lease poll deliberately delegates dead-holder liveness to redelivery of the
    /// holder's own job). With early ACK, a crash between the ACK and the handler strands the run
    /// as Running — no lease, no queued job, and no store enumeration API to even discover it.
    /// A flow store is always registered (validated above), so the veto is unconditional unless
    /// the operator accepts the risk via <see cref="DurableFlowOptions.AllowEarlyAckWorkerSubscriber"/>.
    /// Early ACK on the response queue is at-most-once response delivery — a crash after the ACK
    /// destroys the broker's only copy, the waiter burns its full timeout and fails, and a durable
    /// flow then restarts the timed-out step fresh (re-sending its request; triggers must be
    /// idempotent, which the recovery contract already requires). Nothing strands, so it warns
    /// instead of throwing.
    /// </summary>
    private void ValidateEarlyAckDeclarations()
    {
        var flowOptions = _flowOptions?.FirstOrDefault();
        foreach (var transport in _transports)
        {
            if (transport.ResponseSubscriberUsesEarlyAck)
            {
                _logger?.LogWarning(
                    "The {Transport} response subscriber uses early ACK ({AckModePath} = AckAfterEnqueue): a crash after the ACK destroys the broker's only copy of the response — at-most-once delivery. The affected waiter burns its full timeout and fails, and a durable flow then restarts the timed-out step and re-sends its request (idempotent triggers required). Prefer AckAfterHandlerCompletes for the response queue.",
                    transport.Name,
                    transport.ResponseAckModePath);
            }

            if (!transport.WorkerSubscriberUsesEarlyAck)
                continue;

            if (flowOptions?.AllowEarlyAckWorkerSubscriber == true)
            {
                _logger?.LogWarning(
                    "The {Transport} worker subscriber uses early ACK with {OptOut} enabled: a crash after an ACK but before execution strands Running durable-flow runs until an operator resumes them.",
                    transport.Name,
                    $"{nameof(DurableFlowOptions)}.{nameof(DurableFlowOptions.AllowEarlyAckWorkerSubscriber)}");
                continue;
            }

            throw new InvalidOperationException(
                $"The {transport.Name} worker subscriber is configured for early ACK ({transport.WorkerAckModePath} = AckAfterEnqueue), and durable-flow wake-ups ride the worker queue. " +
                "Flow execution relies on broker redelivery for crash recovery: a process crash after an early ACK but before the handler runs strands the run as Running with no lease, no queued job, and no discovery API. " +
                $"Keep the worker subscriber on AckAfterHandlerCompletes (the default), or set {nameof(DurableFlowOptions)}.{nameof(DurableFlowOptions.AllowEarlyAckWorkerSubscriber)} = true to accept that a crash can strand flow runs until an operator resumes them " +
                "(see docs/durable-flows.md and docs/transport-semantics.md).");
        }
    }

    /// <summary>
    /// Fails fast on watchdog misconfiguration: a non-positive interval spins the scan loop, a
    /// non-positive stale threshold flags every entry, and an out-of-range delay (negative, or
    /// beyond the timer ceiling) throws deep inside <see cref="Task.Delay(TimeSpan)"/> long after
    /// registration. <c>StaleAfter</c> is only ever compared against entry age, so it needs no
    /// ceiling; <c>Interval</c> and <c>StartupDelay</c> arm timers, and zero is a valid startup
    /// delay ("scan immediately").
    /// </summary>
    /// <summary>
    /// Rejects an inbound size budget that would silently swallow traffic. The guard acknowledges
    /// oversized messages without dispatch, so a zero or negative limit does not fail loudly — it
    /// quietly drops EVERY non-empty message on both the response and worker routes and reports
    /// success. That is total data loss wearing the shape of a healthy service, and a plausible
    /// configuration typo (`0` read as "no limit"), so it has to be caught at startup where a
    /// misconfiguration is still visible.
    /// </summary>
    private static void ValidateInboundMessageBudget(AsyncResponseOptions options)
    {
        if (options.MaxInboundMessageChars is { } limit && limit <= 0)
        {
            throw new InvalidOperationException(
                $"{nameof(AsyncResponseOptions)}.{nameof(AsyncResponseOptions.MaxInboundMessageChars)} must be positive " +
                $"(got {limit}); use null to remove the limit. A non-positive budget acknowledges every inbound message " +
                "without dispatching it.");
        }
    }

    private static void ValidateWatchdogOptions(AsyncResponseWatchdogOptions watchdog)
    {
        const string optionsPath = $"{nameof(AsyncResponseOptions)}.{nameof(AsyncResponseOptions.Watchdog)}";
        AsyncResponseChannelOptions.EnsureTimerBacked(watchdog.Interval, optionsPath, nameof(AsyncResponseWatchdogOptions.Interval));
        if (watchdog.StaleAfter <= TimeSpan.Zero)
            throw new InvalidOperationException(
                $"{optionsPath}.{nameof(AsyncResponseWatchdogOptions.StaleAfter)} must be positive.");
        AsyncResponseChannelOptions.EnsureTimerBackedAllowZero(watchdog.StartupDelay, optionsPath, nameof(AsyncResponseWatchdogOptions.StartupDelay));
        if (watchdog.MaxScanEntries <= 0)
            throw new InvalidOperationException(
                $"{nameof(AsyncResponseOptions)}.{nameof(AsyncResponseOptions.Watchdog)}.{nameof(AsyncResponseWatchdogOptions.MaxScanEntries)} must be positive.");
    }

    /// <summary>Stops this service.</summary>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
