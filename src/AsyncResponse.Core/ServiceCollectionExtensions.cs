using AsyncResponse;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System.Diagnostics.CodeAnalysis;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Core registrations for AsyncResponse. Everything is configured through the fluent builder
/// returned by <see cref="AddAsyncResponse"/>: chain exactly one channel and exactly one worker
/// transport, and exactly one durable-flow state store.
/// </summary>
public static class AsyncResponseCoreServiceCollectionExtensions
{
    /// <summary>
    /// Registers the channel-agnostic AsyncResponse engine (fluent waiter builder, transport-neutral
    /// ingress, worker-job executor, and the recovery watchdog) and returns a builder to configure
    /// the rest. It deliberately registers <em>no</em> response channel: chain exactly one
    /// (<see cref="WithInMemoryChannel"/> or the Redis channel package's <c>WithRedisChannel</c>) and
    /// exactly one worker transport (<see cref="WithInMemoryTransport"/> or a broker transport
    /// package such as <c>WithGooglePubSubTransport</c> / <c>WithRabbitMqTransport</c>), and exactly
    /// one durable-flow state store (<see cref="WithInMemoryDurableFlows"/> or a provider package).
    /// An app that starts without any of these choices fails fast at host startup.
    /// </summary>
    public static AsyncResponseRegistrationBuilder AddAsyncResponse(
        this IServiceCollection services,
        Action<AsyncResponseOptions>? configure = null)
    {
        services.AddOptions();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        // Channel-agnostic engine.
        services.TryAddSingleton<AsyncResponseContextPropagation>();
        services.TryAddSingleton<WorkerJobExecutor>();
        services.TryAddSingleton<IAsyncResponseIngress, AsyncResponseIngress>();
        services.TryAddSingleton<IAsyncResponseBuilder>(provider => new AsyncResponseBuilder(
            provider.GetRequiredService<IAsyncResponseSubscriber>(),
            provider.GetService<IWorkerTransport>(),
            provider.GetService<IAsyncResponseReplyTargetProvider>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>()));

        // Fail fast before background services do any real work if the required channel,
        // transport, and durable-flow store choices were not made explicitly.
        services.AddHostedService<AsyncResponseStartupValidator>();

        // The recovery watchdog is part of the engine and runs by default for whatever channel is
        // registered (scanning + liveness go through IRecoveryStateScanner / IActiveSubscriberProbe).
        services.TryAddSingleton<AsyncResponseWatchdogState>();
        services.AddHostedService<AsyncResponseWatchdog>();

        return new AsyncResponseRegistrationBuilder(services);
    }

    /// <summary>
    /// Uses an application-owned store for durable-flow state. The store must implement atomic
    /// creation, revision-checked updates, and execution leases. Use
    /// <see cref="WithInMemoryDurableFlows"/> explicitly for a process-local development or test
    /// store; <see cref="AddAsyncResponse"/> does not select one implicitly.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithDurableFlows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TFlowStateStore>(
        this AsyncResponseRegistrationBuilder builder,
        Action<DurableFlowOptions>? configure = null)
        where TFlowStateStore : class, IFlowStateStore
        => builder.WithDurableFlows<TFlowStateStore, DurableFlowOptions>(configure);

    /// <summary>
    /// Registers an application or provider-owned durable-flow store whose options combine the
    /// common <see cref="DurableFlowOptions"/> settings with store-specific settings. Provider
    /// packages use this overload to expose one cohesive <c>With*DurableFlows(...)</c> callback.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithDurableFlows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TFlowStateStore, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] TOptions>(
        this AsyncResponseRegistrationBuilder builder,
        Action<TOptions>? configure = null)
        where TFlowStateStore : class, IFlowStateStore
        where TOptions : DurableFlowOptions
    {
        builder.Services.AddOptions<TOptions>();
        if (configure is not null)
            builder.Services.Configure(configure);

        // The engine consumes the same configured instance as the provider store, viewed through
        // the common base type. This keeps all durable-flow settings in one callback without a
        // second options object or per-execution adaptation/allocation.
        builder.Services.AddSingleton<DurableFlowOptions>(provider =>
            provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<TOptions>>().Value);

        // Forward the interface to the concrete registration so resolving either yields the same
        // instance within a scope. TryAdd lets callers (and the DurableFlows.* packages) pre-register
        // the concrete type with a different lifetime — e.g. singleton for stores whose dependencies
        // are all singletons — without this scoped default overriding it.
        builder.Services.TryAddScoped<TFlowStateStore>();
        builder.Services.AddScoped<IFlowStateStore>(provider => provider.GetRequiredService<TFlowStateStore>());
        builder.Services.AddSingleton(new AsyncResponseDurableFlowStoreMarker(typeof(TFlowStateStore)));
        AddDurableFlowEngine(builder.Services);
        return builder;
    }

    /// <summary>
    /// Registers a durable flow class for execution: adds it to DI (scoped, unless the app
    /// pre-registered it with another lifetime) and records a statically-typed execution route the
    /// flow executor prefers over reflection-based type-name resolution. Registration is optional
    /// on JIT deployments (unregistered flows resolve reflectively as before) and required for
    /// flows executed in trimmed/Native AOT apps, where persisted type names cannot root code.
    /// </summary>
    /// <typeparam name="TFlow">The flow class; its full name is the persisted <see cref="FlowState.FlowTypeName"/>.</typeparam>
    /// <typeparam name="TInput">The flow input type, persisted as JSON with the flow state.</typeparam>
    public static AsyncResponseRegistrationBuilder WithDurableFlow<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TFlow, TInput>(
        this AsyncResponseRegistrationBuilder builder)
        where TFlow : class, IDurableFlow<TInput>
    {
        builder.Services.TryAddScoped<TFlow>();
        builder.Services.AddSingleton(new DurableFlowRegistration
        {
            FlowTypeFullName = typeof(TFlow).FullName
                ?? throw new InvalidOperationException("Durable flow classes must have a FullName."),
            FlowType = typeof(TFlow),
            DeserializeInput = static json => JsonSafety.SafeDeserialize<TInput>(json),
            ExecuteAsync = static (flow, context, input) =>
                ((TFlow)flow).ExecuteAsync(context, input is null ? default! : (TInput)input)
        });
        return builder;
    }

    /// <summary>
    /// Uses an atomic process-local flow-state store. Intended for development, tests, and
    /// single-process apps; choose a DurableFlows provider package for multi-replica execution.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithInMemoryDurableFlows(
        this AsyncResponseRegistrationBuilder builder,
        Action<DurableFlowOptions>? configure = null)
    {
        builder.Services.TryAddSingleton<InMemoryFlowStateStore>();
        return builder.WithDurableFlows<InMemoryFlowStateStore>(configure);
    }

    // The executor's public methods are lost-subscriber callback targets persisted by name
    // (RecoverAsync / FailAsync / ExecuteAsync); root them explicitly so the reflective dispatch
    // finds them in trimmed apps without any user action.
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicMethods, typeof(IDurableFlowExecutor))]
    [DynamicDependency(DynamicallyAccessedMemberTypes.PublicMethods, typeof(DurableFlowExecutor))]
    private static void AddDurableFlowEngine(IServiceCollection services)
    {
        services.TryAddSingleton<IDurableFlowExecutor>(provider => new DurableFlowExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<IAsyncResponseBuilder>(),
            provider.GetRequiredService<IAsyncResponseSubscriber>(),
            provider.GetService<IRecoverableAsyncResponseSubscriber>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>(),
            provider.GetRequiredService<DurableFlowOptions>(),
            provider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<DurableFlowExecutor>>(),
            provider.GetServices<DurableFlowRegistration>()));
        services.TryAddSingleton<IDurableFlows>(provider => new DurableFlowService(
            provider.GetRequiredService<IServiceScopeFactory>(),
            provider.GetRequiredService<IAsyncResponseBuilder>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>(),
            provider.GetRequiredService<DurableFlowOptions>(),
            provider.GetRequiredService<Microsoft.Extensions.Logging.ILogger<DurableFlowService>>()));
    }

    /// <summary>
    /// Registers an application <see cref="IAsyncResponseContextPropagator"/> that carries ambient
    /// context (trace id, principal, tenant, …) across the serialization boundary into worker jobs
    /// and lost-subscriber recovery callbacks. In-process hops flow ambient state automatically via
    /// the captured <see cref="System.Threading.ExecutionContext"/>; propagators are only needed for
    /// context that must survive serialization (broker-backed workers, recovery after a redeploy).
    /// Register one per concern (e.g. a trace propagator and a principal propagator).
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithContextPropagator<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TPropagator>(this AsyncResponseRegistrationBuilder builder)
        where TPropagator : class, IAsyncResponseContextPropagator
    {
        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IAsyncResponseContextPropagator, TPropagator>());
        return builder;
    }

    /// <summary>
    /// Registers the process-local response channel and recovery store. Waiters, subscriptions,
    /// and recovery state all live in memory and disappear when the process exits — the simplest
    /// setup, with no durable recovery. Pair with <see cref="WithInMemoryTransport"/> for a fully
    /// in-memory setup including background worker jobs.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithInMemoryChannel(
        this AsyncResponseRegistrationBuilder builder,
        Action<InMemoryAsyncResponseOptions>? configure = null)
    {
        var services = builder.Services;
        services.AddOptions();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<InMemoryRecoveryStateStore>();
        services.TryAddSingleton<IRecoveryStateStore>(provider => provider.GetRequiredService<InMemoryRecoveryStateStore>());
        services.TryAddSingleton<IRecoveryStateScanner>(provider => provider.GetRequiredService<InMemoryRecoveryStateStore>());

        services.TryAddSingleton<InMemoryAsyncResponseChannel>();
        services.TryAddSingleton<IAsyncResponsePublisher>(provider => provider.GetRequiredService<InMemoryAsyncResponseChannel>());
        services.TryAddSingleton<IRawAsyncResponsePublisher>(provider => provider.GetRequiredService<InMemoryAsyncResponseChannel>());
        services.TryAddSingleton<IAsyncResponseSubscriber>(provider => provider.GetRequiredService<InMemoryAsyncResponseChannel>());
        services.TryAddSingleton<IActiveSubscriberProbe>(provider => provider.GetRequiredService<InMemoryAsyncResponseChannel>());

        services.AddSingleton(new AsyncResponseChannelMarker("InMemory"));
        return builder;
    }

    /// <summary>
    /// Registers the in-memory (in-process) worker transport and its background consumer. Jobs run
    /// in the current process and survive only as long as it does — suitable for development, tests,
    /// and single-node deployments. Chain exactly one transport after <see cref="AddAsyncResponse"/>;
    /// for distributed, durable execution use a full broker-backed transport package such as
    /// <c>WithGooglePubSubTransport</c> or <c>WithRabbitMqTransport</c>.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithInMemoryTransport(
        this AsyncResponseRegistrationBuilder builder,
        Action<InMemoryWorkerTransportOptions>? configure = null)
    {
        var services = builder.Services;
        services.AddOptions();
        if (configure is not null)
            services.Configure(configure);
        services.TryAddSingleton<WorkerJobExecutor>();
        services.TryAddSingleton<InMemoryWorkerTransport>();
        services.TryAddSingleton<IWorkerTransport>(provider => provider.GetRequiredService<InMemoryWorkerTransport>());
        services.AddHostedService<InMemoryWorkerHost>();
        services.AddSingleton(new AsyncResponseTransportMarker("InMemory"));
        return builder;
    }
}
