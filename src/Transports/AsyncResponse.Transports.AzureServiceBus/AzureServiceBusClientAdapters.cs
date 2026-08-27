using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;

namespace AsyncResponse.Transports.AzureServiceBus;

internal interface IAzureServiceBusClient : IAsyncDisposable
{
    IAzureServiceBusSender CreateSender(string queue);
    IAzureServiceBusReceiver CreateReceiver(string queue, AzureServiceBusSubscriberOptions subscriberOptions);
}

internal sealed class AzureServiceBusClientAdapter(
    ServiceBusClient inner,
    bool ownsClient) : IAzureServiceBusClient
{
    /// <summary>Creates a sender for the requested queue.</summary>
    public IAzureServiceBusSender CreateSender(string queue)
        => new AzureServiceBusSenderAdapter(inner.CreateSender(queue));

    /// <summary>Creates a peek-lock receiver for the requested queue.</summary>
    public IAzureServiceBusReceiver CreateReceiver(
        string queue,
        AzureServiceBusSubscriberOptions subscriberOptions)
        => new AzureServiceBusReceiverAdapter(inner.CreateReceiver(
            queue,
            new ServiceBusReceiverOptions
            {
                ReceiveMode = ServiceBusReceiveMode.PeekLock,
                PrefetchCount = subscriberOptions.PrefetchCount
            }));

    /// <summary>Releases resources held by this instance.</summary>
    public async ValueTask DisposeAsync()
    {
        if (ownsClient)
            await inner.DisposeAsync().ConfigureAwait(false);
    }
}

internal static class AzureServiceBusClientResolver
{
    public static IAzureServiceBusClient Create(IServiceProvider provider)
    {
        if (provider.GetService<ServiceBusClient>() is { } registeredClient)
            return new AzureServiceBusClientAdapter(registeredClient, ownsClient: false);

        var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<AzureServiceBusAsyncResponseOptions>>().Value;
        var connectionString = AzureServiceBusOptionsValidator.Required(options.ConnectionString, nameof(options.ConnectionString));
        return new AzureServiceBusClientAdapter(new ServiceBusClient(connectionString), ownsClient: true);
    }
}

internal interface IAzureServiceBusSender : IAsyncDisposable
{
    Task SendMessageAsync(AzureServiceBusOutboundMessage message, CancellationToken cancellationToken = default);
    Task CloseAsync(CancellationToken cancellationToken = default);
}

internal sealed class AzureServiceBusSenderAdapter(ServiceBusSender inner) : IAzureServiceBusSender
{
    /// <summary>Sends the supplied outbound message.</summary>
    public Task SendMessageAsync(AzureServiceBusOutboundMessage message, CancellationToken cancellationToken = default)
    {
        var serviceBusMessage = new ServiceBusMessage(BinaryData.FromString(message.Body))
        {
            ContentType = "application/json",
            MessageId = message.MessageId,
            CorrelationId = message.CorrelationId
        };

        // Native delayed delivery: the broker holds a scheduled message and enqueues it at the
        // requested instant — the message survives restarts on the broker, unlike any client-side
        // timer.
        if (message.ScheduledEnqueueTime is { } scheduledEnqueueTime)
            serviceBusMessage.ScheduledEnqueueTime = scheduledEnqueueTime;

        foreach (var property in message.ApplicationProperties)
            serviceBusMessage.ApplicationProperties[property.Key] = property.Value;

        return inner.SendMessageAsync(serviceBusMessage, cancellationToken);
    }

    /// <summary>Closes the sender link.</summary>
    public Task CloseAsync(CancellationToken cancellationToken = default)
        => inner.CloseAsync(cancellationToken);

    /// <summary>Releases resources held by this instance.</summary>
    public ValueTask DisposeAsync() => inner.DisposeAsync();
}

internal interface IAzureServiceBusReceiver : IAsyncDisposable
{
    Task<IReadOnlyList<AzureServiceBusTransportDelivery>> ReceiveMessagesAsync(
        int maxMessages,
        TimeSpan maxWaitTime,
        CancellationToken cancellationToken = default);

    Task CloseAsync(CancellationToken cancellationToken = default);
}

internal sealed class AzureServiceBusReceiverAdapter(
    ServiceBusReceiver inner,
    string? queueOverride = null) : IAzureServiceBusReceiver
{
    /// <summary>Receives and wraps messages from Service Bus.</summary>
    public async Task<IReadOnlyList<AzureServiceBusTransportDelivery>> ReceiveMessagesAsync(
        int maxMessages,
        TimeSpan maxWaitTime,
        CancellationToken cancellationToken = default)
    {
        var messages = await inner.ReceiveMessagesAsync(maxMessages, maxWaitTime, cancellationToken).ConfigureAwait(false);
        if (messages.Count == 0)
            return [];

        var queue = queueOverride ?? inner.EntityPath;
        var deliveries = new List<AzureServiceBusTransportDelivery>(messages.Count);
        for (var i = 0; i < messages.Count; i++)
        {
            var message = messages[i];
            try
            {
                deliveries.Add(CreateDelivery(queue, message));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Projecting the message must not be able to abort the RECEIVE. The Body getter
                // throws for an AMQP Value/Sequence body — what a JMS or raw-AMQP producer sends —
                // and the throw used to escape the whole batch: nothing was settled, all N locks
                // lapsed, all N DeliveryCounts advanced, and the poison message crashed the loop
                // again next cycle while its innocent batch-mates burned attempts toward the
                // entity's MaxDeliveryCount without ever running. Bury this one and keep the rest.
                await DeadLetterUnprojectableAsync(message, ex).ConfigureAwait(false);
            }
        }

        return deliveries;
    }

    /// <summary>
    /// Buries a message this adapter cannot project (an unsupported AMQP body type). Best-effort:
    /// if the dead-letter itself fails the lock simply lapses and the broker redelivers, which is
    /// still strictly better than tearing down the receive loop.
    /// </summary>
    private async Task DeadLetterUnprojectableAsync(ServiceBusReceivedMessage message, Exception cause)
    {
        try
        {
            await inner.DeadLetterMessageAsync(
                message,
                deadLetterReason: "AsyncResponseUnsupportedBody",
                deadLetterErrorDescription: cause.GetType().Name,
                cancellationToken: CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // Swallowed deliberately: the caller is mid-batch and the alternative is losing the
            // deliveries already projected.
        }
    }

    private AzureServiceBusTransportDelivery CreateDelivery(
        string queue,
        ServiceBusReceivedMessage message)
        => new(
            queue,
            message.Body.ToString(),
            message.MessageId,
            message.CorrelationId,
            message.SequenceNumber,
            message.DeliveryCount,
            CopyApplicationProperties(message.ApplicationProperties),
            () => new ValueTask(inner.CompleteMessageAsync(message, CancellationToken.None)),
            () => new ValueTask(inner.AbandonMessageAsync(message, cancellationToken: CancellationToken.None)),
            (reason, description) => new ValueTask(inner.DeadLetterMessageAsync(
                message,
                deadLetterReason: reason,
                deadLetterErrorDescription: description,
                cancellationToken: CancellationToken.None)),
            // Settlement deliberately ignores cancellation so an in-flight message still settles
            // during shutdown. Lock renewal is a background courtesy and honors the caller's token:
            // on a degraded namespace each renew otherwise burns the SDK's full retry budget, and
            // the renewal loop must be interruptible mid-call for the batch (and shutdown) to
            // complete promptly.
            cancellationToken => new ValueTask(inner.RenewMessageLockAsync(message, cancellationToken)));

    // Indexer, not the copying constructor: AMQP application-property names are case-sensitive,
    // so a message legally carries keys differing only in case — the constructor's internal Add
    // would throw ArgumentException out of the receive path before any delivery in the batch is
    // settled, stalling the whole batch. Last-seen wins under the case-insensitive comparer the
    // lookups rely on (same shape as the SQS and Kafka adapters).
    private static Dictionary<string, object?> CopyApplicationProperties(IReadOnlyDictionary<string, object> applicationProperties)
    {
        var properties = new Dictionary<string, object?>(applicationProperties.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var property in applicationProperties)
            properties[property.Key] = property.Value;
        return properties;
    }

    /// <summary>Closes the receiver link.</summary>
    public Task CloseAsync(CancellationToken cancellationToken = default)
        => inner.CloseAsync(cancellationToken);

    /// <summary>Releases resources held by this instance.</summary>
    public ValueTask DisposeAsync() => inner.DisposeAsync();
}

internal sealed record AzureServiceBusOutboundMessage(
    string Body,
    string MessageId,
    string? CorrelationId,
    IReadOnlyDictionary<string, object?> ApplicationProperties,
    DateTimeOffset? ScheduledEnqueueTime = null);

internal sealed record AzureServiceBusTransportDelivery(
    string Queue,
    string Body,
    string MessageId,
    string? CorrelationId,
    long SequenceNumber,
    int DeliveryCount,
    IReadOnlyDictionary<string, object?> ApplicationProperties,
    Func<ValueTask> CompleteAsync,
    Func<ValueTask> AbandonAsync,
    Func<string, string?, ValueTask> DeadLetterAsync,
    Func<CancellationToken, ValueTask> RenewLockAsync);
