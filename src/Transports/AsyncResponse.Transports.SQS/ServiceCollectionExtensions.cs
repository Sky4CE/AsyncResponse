using AsyncResponse;
using AsyncResponse.Transports.SQS;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>DI registration for the AWS SQS AsyncResponse transport.</summary>
public static class SqsAsyncResponseServiceCollectionExtensions
{
    /// <summary>
    /// Registers AWS SQS as the worker transport and response ingress in one call:
    /// <list type="bullet">
    /// <item><description>worker jobs are published to <see cref="SqsAsyncResponseOptions.WorkerQueue"/>;</description></item>
    /// <item><description>a hosted subscriber long-polls that worker queue and executes worker jobs;</description></item>
    /// <item><description>a hosted subscriber long-polls <see cref="SqsAsyncResponseOptions.ResponseQueue"/> and feeds responses into <see cref="IAsyncResponseIngress"/>.</description></item>
    /// </list>
    /// Redelivery and dead-lettering stay native: a failed handler leaves the message for the
    /// visibility timeout, and the queue's redrive policy moves it to the dead-letter queue after
    /// <c>maxReceiveCount</c> receives (provision both with
    /// <see cref="SqsAsyncResponseOptions.CreateQueues"/> or your infrastructure tooling). SQS is a
    /// transport, not a recovery store: pair it with a channel (<c>.WithInMemoryChannel()</c> for
    /// simple apps, or a durable channel such as <c>.WithRedisChannel()</c>,
    /// <c>.WithPostgreSqlChannel()</c>, or <c>.WithNatsChannel()</c> when late responses must
    /// survive redeploys). An application-registered <c>Amazon.SQS.IAmazonSQS</c> singleton (for
    /// example from <c>AWSSDK.Extensions.NETCore.Setup</c>) is reused when present.
    /// </summary>
    public static AsyncResponseRegistrationBuilder WithSqsTransport(
        this AsyncResponseRegistrationBuilder builder,
        Action<SqsAsyncResponseOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var services = builder.Services;
        services.AddOptions();
        services.Configure(configure);

        services.TryAddSingleton(SqsClientResolver.Create);
        services.TryAddSingleton(provider => new SqsWorkerTransport(
            provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<SqsAsyncResponseOptions>>(),
            provider.GetRequiredService<ISqsClient>()));
        services.Replace(ServiceDescriptor.Singleton<IWorkerTransport>(provider =>
            provider.GetRequiredService<SqsWorkerTransport>()));
        services.Replace(ServiceDescriptor.Singleton<IAsyncResponseReplyTargetProvider, SqsReplyTargetProvider>());
        services.AddSingleton(provider =>
        {
            // Resolved ack modes declared to the Core startup validator, which vetoes early ACK on
            // the worker queue durable-flow wake-ups ride (see AsyncResponseStartupValidator).
            var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<SqsAsyncResponseOptions>>().Value;
            return new AsyncResponseTransportMarker(SqsAsyncResponseOptions.TransportName)
            {
                WorkerSubscriberUsesEarlyAck = options.WorkerSubscriber.AckMode == SqsAckMode.AckAfterEnqueue,
                WorkerAckModePath = $"{nameof(SqsAsyncResponseOptions)}.{nameof(options.WorkerSubscriber)}.{nameof(options.WorkerSubscriber.AckMode)}",
                ResponseSubscriberUsesEarlyAck = options.ResponseSubscriber.AckMode == SqsAckMode.AckAfterEnqueue,
                ResponseAckModePath = $"{nameof(SqsAsyncResponseOptions)}.{nameof(options.ResponseSubscriber)}.{nameof(options.ResponseSubscriber.AckMode)}"
            };
        });

        // Registered before the subscribers: hosted services start in registration order, so
        // CreateQueues provisioning completes before the first ReceiveMessage loop begins.
        services.AddHostedService<SqsQueueProvisioningService>();
        services.AddHostedService<SqsWorkerSubscriber>();
        services.AddHostedService<SqsResponseIngressSubscriber>();

        return builder;
    }
}
