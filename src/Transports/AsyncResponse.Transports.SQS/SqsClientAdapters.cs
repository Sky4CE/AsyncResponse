using Amazon;
using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.DependencyInjection;

namespace AsyncResponse.Transports.SQS;

internal interface ISqsClient : IAsyncDisposable
{
    Task<string> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken = default);
    Task<string> CreateQueueAsync(string queueName, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken = default);
    Task<string> GetQueueArnAsync(string queueUrl, CancellationToken cancellationToken = default);
    Task SetQueueAttributesAsync(string queueUrl, IReadOnlyDictionary<string, string> attributes, CancellationToken cancellationToken = default);
    Task<string> SendMessageAsync(SqsOutboundMessage message, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<SqsTransportDelivery>> ReceiveMessagesAsync(SqsReceiveRequest request, CancellationToken cancellationToken = default);
}

internal sealed class SqsClientAdapter(
    IAmazonSQS inner,
    bool ownsClient) : ISqsClient
{
    /// <summary>Resolves a queue name to its queue URL.</summary>
    public async Task<string> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken = default)
    {
        var response = await inner.GetQueueUrlAsync(queueName, cancellationToken).ConfigureAwait(false);
        return response.QueueUrl;
    }

    /// <summary>Creates the queue (idempotent for identical attributes) and returns its URL.</summary>
    public async Task<string> CreateQueueAsync(
        string queueName,
        IReadOnlyDictionary<string, string> attributes,
        CancellationToken cancellationToken = default)
    {
        var request = new CreateQueueRequest { QueueName = queueName };
        foreach (var attribute in attributes)
            (request.Attributes ??= []).Add(attribute.Key, attribute.Value);

        var response = await inner.CreateQueueAsync(request, cancellationToken).ConfigureAwait(false);
        return response.QueueUrl;
    }

    /// <summary>Reads the queue's ARN attribute.</summary>
    public async Task<string> GetQueueArnAsync(string queueUrl, CancellationToken cancellationToken = default)
    {
        var response = await inner.GetQueueAttributesAsync(
            new GetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                AttributeNames = [QueueAttributeName.QueueArn]
            },
            cancellationToken).ConfigureAwait(false);
        return response.QueueARN;
    }

    /// <summary>Applies the supplied attributes to an existing queue.</summary>
    public Task SetQueueAttributesAsync(
        string queueUrl,
        IReadOnlyDictionary<string, string> attributes,
        CancellationToken cancellationToken = default)
        => inner.SetQueueAttributesAsync(
            new SetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                Attributes = attributes.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal)
            },
            cancellationToken);

    /// <summary>Sends the supplied outbound message and returns the SQS-assigned message id.</summary>
    public async Task<string> SendMessageAsync(SqsOutboundMessage message, CancellationToken cancellationToken = default)
    {
        var request = new SendMessageRequest
        {
            QueueUrl = message.QueueUrl,
            MessageBody = message.Body,
            MessageGroupId = message.MessageGroupId,
            MessageDeduplicationId = message.MessageDeduplicationId
        };
        // Native per-message delay (standard queues; 0–900s). Longer waits arrive chunked: the
        // envelope's NotBeforeUtc re-publish chain in the worker-job executor takes the next hop.
        if (message.DelaySeconds is { } delaySeconds)
            request.DelaySeconds = delaySeconds;

        foreach (var attribute in message.MessageAttributes)
        {
            (request.MessageAttributes ??= []).Add(attribute.Key, new MessageAttributeValue
            {
                DataType = "String",
                StringValue = attribute.Value
            });
        }

        var response = await inner.SendMessageAsync(request, cancellationToken).ConfigureAwait(false);
        return response.MessageId;
    }

    /// <summary>Long-polls the queue and wraps the received messages as transport deliveries.</summary>
    public async Task<IReadOnlyList<SqsTransportDelivery>> ReceiveMessagesAsync(
        SqsReceiveRequest request,
        CancellationToken cancellationToken = default)
    {
        var receive = new ReceiveMessageRequest
        {
            QueueUrl = request.QueueUrl,
            MaxNumberOfMessages = request.MaxMessages,
            WaitTimeSeconds = WholeSeconds(request.WaitTime),
            MessageSystemAttributeNames = [MessageSystemAttributeName.ApproximateReceiveCount],
            MessageAttributeNames = ["All"]
        };
        if (request.VisibilityTimeout is { } visibilityTimeout)
            receive.VisibilityTimeout = WholeSeconds(visibilityTimeout);

        var response = await inner.ReceiveMessageAsync(receive, cancellationToken).ConfigureAwait(false);
        // AWS SDK v4 leaves collections null when the response carries no items.
        if (response.Messages is not { Count: > 0 } messages)
            return [];

        var deliveries = new SqsTransportDelivery[messages.Count];
        for (var i = 0; i < messages.Count; i++)
            deliveries[i] = CreateDelivery(request.QueueUrl, messages[i]);

        return deliveries;
    }

    private SqsTransportDelivery CreateDelivery(string queueUrl, Message message)
    {
        var receiveCount = 1;
        if (message.Attributes is { } systemAttributes
            && systemAttributes.TryGetValue(MessageSystemAttributeName.ApproximateReceiveCount, out var rawReceiveCount)
            && int.TryParse(rawReceiveCount, out var parsedReceiveCount))
        {
            receiveCount = parsedReceiveCount;
        }

        var messageAttributes = new Dictionary<string, string>(StringComparer.Ordinal);
        if (message.MessageAttributes is { } attributes)
        {
            foreach (var attribute in attributes)
            {
                if (attribute.Value?.StringValue is { } value)
                    messageAttributes[attribute.Key] = value;
            }
        }

        var receiptHandle = message.ReceiptHandle;
        return new SqsTransportDelivery(
            queueUrl,
            message.Body ?? string.Empty,
            message.MessageId ?? string.Empty,
            receiptHandle,
            receiveCount,
            messageAttributes,
            () => new ValueTask(inner.DeleteMessageAsync(queueUrl, receiptHandle, CancellationToken.None)),
            // The caller chooses the token: settlement paths pass None (settlement ignores
            // cancellation), while the visibility-renewal heartbeat passes its shutdown-linked
            // token so a degraded endpoint cannot hold the stop hostage for the SDK retry budget.
            (delay, cancellationToken) => new ValueTask(inner.ChangeMessageVisibilityAsync(
                queueUrl,
                receiptHandle,
                WholeSeconds(delay),
                cancellationToken)));
    }

    /// <summary>
    /// Converts a duration to the whole seconds SQS speaks, rounding UP so a positive value never
    /// becomes zero. Truncation is not a rounding nicety here: a 500 ms visibility timeout floored
    /// to 0 makes the message visible again immediately, and a second consumer picks it up while
    /// the first is still handling it — the exactly-one-handler guarantee the timeout exists for.
    /// A redelivery delay floored to 0 is a hot retry loop for the same reason. Zero itself is
    /// preserved, because "make it visible now" is a legitimate request. This matches the delayed
    /// publish path, which already rounds up.
    /// </summary>
    private static int WholeSeconds(TimeSpan value)
        => value <= TimeSpan.Zero ? 0 : (int)Math.Min(Math.Ceiling(value.TotalSeconds), int.MaxValue);

    /// <summary>Releases resources held by this instance.</summary>
    public ValueTask DisposeAsync()
    {
        if (ownsClient)
            inner.Dispose();
        return ValueTask.CompletedTask;
    }
}

internal static class SqsClientFactory
{
    /// <summary>Builds an SQS client from the transport options (endpoint, region, credentials).</summary>
    public static ISqsClient Create(SqsAsyncResponseOptions options)
    {
        var config = new AmazonSQSConfig();
        if (!string.IsNullOrWhiteSpace(options.ServiceUrl))
        {
            config.ServiceURL = options.ServiceUrl;
            // Custom endpoints (LocalStack, proxies) still need a signing region.
            config.AuthenticationRegion = options.Region ?? "us-east-1";
        }
        else if (!string.IsNullOrWhiteSpace(options.Region))
        {
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(options.Region);
        }

        var client = !string.IsNullOrWhiteSpace(options.AccessKey) && !string.IsNullOrWhiteSpace(options.SecretKey)
            ? new AmazonSQSClient(new BasicAWSCredentials(options.AccessKey, options.SecretKey), config)
            : new AmazonSQSClient(config);
        return new SqsClientAdapter(client, ownsClient: true);
    }
}

internal static class SqsClientResolver
{
    /// <summary>Reuses an application-registered <see cref="IAmazonSQS"/> or builds one from the options.</summary>
    public static ISqsClient Create(IServiceProvider provider)
    {
        if (provider.GetService<IAmazonSQS>() is { } registeredClient)
            return new SqsClientAdapter(registeredClient, ownsClient: false);

        var options = provider.GetRequiredService<Microsoft.Extensions.Options.IOptions<SqsAsyncResponseOptions>>().Value;
        return SqsClientFactory.Create(options);
    }
}

internal sealed record SqsOutboundMessage(
    string QueueUrl,
    string Body,
    string? CorrelationId,
    string? MessageGroupId,
    string? MessageDeduplicationId,
    IReadOnlyDictionary<string, string> MessageAttributes,
    int? DelaySeconds = null);

internal sealed record SqsReceiveRequest(
    string QueueUrl,
    int MaxMessages,
    TimeSpan WaitTime,
    TimeSpan? VisibilityTimeout);

internal sealed record SqsTransportDelivery(
    string QueueUrl,
    string Body,
    string MessageId,
    string ReceiptHandle,
    int ReceiveCount,
    IReadOnlyDictionary<string, string> MessageAttributes,
    Func<ValueTask> DeleteAsync,
    Func<TimeSpan, CancellationToken, ValueTask> ChangeVisibilityAsync);
