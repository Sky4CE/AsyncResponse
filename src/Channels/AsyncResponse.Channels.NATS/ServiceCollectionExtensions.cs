using AsyncResponse;
using AsyncResponse.Channels.NATS;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Net;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// DI registration for the NATS-backed AsyncResponse channel package.
/// </summary>
public static class NatsAsyncResponseChannelServiceCollectionExtensions
{
    /// <summary>
    /// Registers NATS Core request/reply as the response channel — behind
    /// <see cref="IAsyncResponsePublisher"/>, <see cref="IAsyncResponseSubscriber"/>, and
    /// <see cref="IRecoverableAsyncResponseSubscriber"/> — together with the durable NATS JetStream
    /// Key-Value recovery-state store, so a response that arrives after the waiter died (e.g. a
    /// redeploy) can still resume or fail the owning flow. It also registers
    /// <see cref="IRecoverableAsyncResponseBuilder"/> for fluent durable recovery flows. The same
    /// channel instance serves the engine's recovery-state scanner and active-subscriber probe, so the
    /// watchdog works against NATS automatically.
    /// <para>
    /// Requires a <c>NATS.Client.Core.INatsConnection</c> singleton registered by the host — for
    /// example via <c>builder.Services.AddNatsClient(...)</c> from
    /// <c>NATS.Extensions.Microsoft.DependencyInjection</c>. Reuse the application's existing
    /// connection; don't create a second one. The recovery store requires JetStream to be enabled on
    /// the NATS server. Publisher/subscriber resolve to one shared instance: per-interface instances
    /// would split the channel's internal state.
    /// </para>
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithNatsChannel(
        this AsyncResponseRegistrationBuilder builder,
        Action<NatsAsyncResponseChannelOptions>? configure = null)
    {
        var services = builder.Services;
        services.AddOptions();
        if (configure is not null)
            services.Configure(configure);

        // NATS JetStream Key-Value client adapter (lazily creates the recovery bucket on first use).
        services.TryAddSingleton<INatsKvStore>(provider => new NatsKvStoreAdapter(
            provider.GetRequiredService<INatsConnection>().CreateKeyValueStoreContext(),
            provider.GetRequiredService<IOptions<NatsAsyncResponseChannelOptions>>().Value));

        // Durable recovery store; also the watchdog's recovery-state scanner.
        services.TryAddSingleton<NatsRecoveryStateStore>();
        services.Replace(ServiceDescriptor.Singleton<IRecoveryStateStore>(provider => provider.GetRequiredService<NatsRecoveryStateStore>()));
        services.Replace(ServiceDescriptor.Singleton<IRecoveryStateScanner>(provider => provider.GetRequiredService<NatsRecoveryStateStore>()));

        // NATS Core request/reply client adapter (raw extension calls isolated in NatsRawRequester).
        services.TryAddSingleton<INatsResponseChannelClient>(provider => new NatsResponseChannelClient(
            new NatsRawRequester(provider.GetRequiredService<INatsConnection>())));

        // NATS response channel: publisher + subscriber + the watchdog's liveness probe, all one shared instance.
        services.TryAddSingleton<NatsAsyncResponseChannel>();
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponsePublisher>(provider => provider.GetRequiredService<NatsAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IRawAsyncResponsePublisher>(provider => provider.GetRequiredService<NatsAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseSubscriber>(provider => provider.GetRequiredService<NatsAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IRecoverableAsyncResponseSubscriber>(provider => provider.GetRequiredService<NatsAsyncResponseChannel>()));
        services.Replace(ServiceDescriptor.Singleton<IActiveSubscriberProbe>(provider => provider.GetRequiredService<NatsAsyncResponseChannel>()));

        // Durable recovery capability: expose the recoverable fluent builder only when the NATS channel
        // package is the selected response channel. Plain IAsyncResponseBuilder still works and shares
        // the same implementation, but its static type does not offer recovery callbacks.
        services.Replace(ServiceDescriptor.Singleton<IRecoverableAsyncResponseBuilder>(provider => new RecoverableAsyncResponseBuilder(
            provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>(),
            provider.GetService<IWorkerTransport>(),
            provider.GetService<IAsyncResponseReplyTargetProvider>(),
            provider.GetRequiredService<AsyncResponseContextPropagation>(),
            provider.GetService<TimeProvider>())));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseBuilder>(provider => provider.GetRequiredService<IRecoverableAsyncResponseBuilder>()));

        // The resolved default waiter timeout is declared through the marker so the startup
        // validator can require the durable-flow ledger TTL to out-live a timeout-less awaited
        // step, and the flow engine can extend a parked ledger by it — without either referencing
        // channel option types.
        services.AddSingleton(provider =>
        {
            var options = provider.GetRequiredService<IOptions<NatsAsyncResponseChannelOptions>>().Value;
            return new AsyncResponseChannelMarker(NatsAsyncResponseChannelOptions.ChannelName)
            {
                EffectiveDefaultWaitTimeout = options.DefaultTimeout ?? options.RecoveryStateExpiry
            };
        });
        return builder;
    }
}
