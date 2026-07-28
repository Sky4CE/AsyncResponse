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
        var deliveries = new AzureServiceBusTransportDelivery[messages.Count];
        for (var i = 0; i < messages.Count; i++)
            deliveries[i] = CreateDelivery(queue, messages[i]);

        return deliveries;
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
            new Dictionary<string, object?>(message.ApplicationProperties, StringComparer.OrdinalIgnoreCase),
            () => new ValueTask(inner.CompleteMessageAsync(message, CancellationToken.None)),
            () => new ValueTask(inner.AbandonMessageAsync(message, cancellationToken: CancellationToken.None)),
            (reason, description) => new ValueTask(inner.DeadLetterMessageAsync(
                message,
                deadLetterReason: reason,
                deadLetterErrorDescription: description,
                cancellationToken: CancellationToken.None)),
            () => new ValueTask(inner.RenewMessageLockAsync(message, CancellationToken.None)));

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
    IReadOnlyDictionary<string, object?> ApplicationProperties);

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
    Func<ValueTask> RenewLockAsync);
