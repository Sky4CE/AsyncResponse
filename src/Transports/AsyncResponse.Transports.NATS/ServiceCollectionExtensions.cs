using AsyncResponse;
using AsyncResponse.Transports.NATS;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Net;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// DI registration for the NATS JetStream AsyncResponse transport.
/// </summary>
public static class NatsAsyncResponseTransportServiceCollectionExtensions
{
    /// <summary>
    /// Registers NATS JetStream as the worker transport and response ingress in one call:
    /// <list type="bullet">
    /// <item><description>worker jobs are published to the configured worker subject;</description></item>
    /// <item><description>a hosted durable-consumer subscriber reads worker messages and executes jobs;</description></item>
    /// <item><description>a hosted durable-consumer subscriber reads response messages and feeds them into <see cref="IAsyncResponseIngress"/>.</description></item>
    /// </list>
    /// NATS JetStream is a transport, not the waiter/recovery channel. Pair it with
    /// <c>.WithNatsChannel()</c> when late responses must survive redeploys, or with
    /// <c>.WithInMemoryChannel()</c> for simple single-process waits.
    /// <para>
    /// Requires a <c>NATS.Client.Core.INatsConnection</c> singleton registered by the host (for
    /// example via <c>builder.Services.AddNatsClient(...)</c>) and a NATS server with JetStream
    /// enabled. Reuse the same connection as the NATS channel package when both are installed.
    /// </para>
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithNatsTransport(
        this AsyncResponseRegistrationBuilder builder,
        Action<NatsAsyncResponseTransportOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var services = builder.Services;
        services.AddOptions();
        services.Configure(configure);

        // The worker transport and hosted subscribers each build their JetStream client adapter from the
        // host's INatsConnection via their public constructors (mirroring the other transport packages).
        services.TryAddSingleton<NatsWorkerTransport>();
        services.Replace(ServiceDescriptor.Singleton<IWorkerTransport>(provider =>
            provider.GetRequiredService<NatsWorkerTransport>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseReplyTargetProvider, NatsReplyTargetProvider>());
        services.AddSingleton(provider =>
        {
            // Resolved ack modes declared to the Core startup validator, which vetoes early ACK on
            // the worker queue durable-flow wake-ups ride (see AsyncResponseStartupValidator).
            var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<NatsAsyncResponseTransportOptions>>().Value;
            return new AsyncResponseTransportMarker(NatsAsyncResponseTransportOptions.TransportName)
            {
                WorkerSubscriberUsesEarlyAck = options.WorkerSubscriber.AckMode == NatsAckMode.AckAfterEnqueue,
                WorkerAckModePath = $"{nameof(NatsAsyncResponseTransportOptions)}.{nameof(options.WorkerSubscriber)}.{nameof(options.WorkerSubscriber.AckMode)}",
                ResponseSubscriberUsesEarlyAck = options.ResponseSubscriber.AckMode == NatsAckMode.AckAfterEnqueue,
                ResponseAckModePath = $"{nameof(NatsAsyncResponseTransportOptions)}.{nameof(options.ResponseSubscriber)}.{nameof(options.ResponseSubscriber.AckMode)}"
            };
        });

        services.AddHostedService<NatsWorkerSubscriber>();
        services.AddHostedService<NatsResponseIngressSubscriber>();

        return builder;
    }
}
