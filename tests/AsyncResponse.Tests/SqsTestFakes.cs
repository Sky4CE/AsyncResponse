using AsyncResponse.Transports.SQS;
using System.Threading.Channels;

namespace AsyncResponse.Tests;

/// <summary>
/// In-memory <see cref="ISqsClient"/> fake shared by the SQS unit tests: queue-name resolution,
/// send capture with configurable failures, channel-backed long-poll receive, and provisioning
/// call capture.
/// </summary>
internal sealed class FakeSqsClient : ISqsClient
{
    private readonly Channel<SqsTransportDelivery> _deliveries = Channel.CreateUnbounded<SqsTransportDelivery>();

    public static string UrlFor(string queueName)
        => $"https://sqs.test.local/000000000000/{queueName}";

    public static string ArnFor(string queueName)
        => $"arn:aws:sqs:us-east-1:000000000000:{queueName}";

    public int GetQueueUrlCalls { get; private set; }
    public int GetQueueUrlFailuresBeforeSuccess { get; set; }
    public List<string> ResolvedQueueNames { get; } = [];

    public List<SqsOutboundMessage> SentMessages { get; } = [];
    public int SendAttempts { get; private set; }
    public int SendFailuresBeforeSuccess { get; set; }
    public Exception? SendException { get; set; }

    public SqsReceiveRequest? LastReceiveRequest { get; private set; }
    public int ReceiveAttempts => Volatile.Read(ref _receiveAttempts);
    public int FailuresBeforeReceive { get; set; }

    /// <summary>Fails exactly the receive with this 1-based attempt number (a throttling blip mid-run).</summary>
    public int? FailOnReceiveAttempt { get; set; }

    /// <summary>
    /// Hands over everything queued regardless of the requested count. Real SQS never returns more
    /// than asked; the flag lets a test drive the batch loop, which must handle whatever batch a
    /// receive returns, although ACK-after-handler now asks for one message.
    /// </summary>
    public bool ReturnAllAvailable { get; set; }

    /// <summary>Runs just before a receive hands a non-empty batch over — e.g. the host stop beginning mid-poll.</summary>
    public Action? OnBatchReceived { get; set; }

    public List<(string QueueName, IReadOnlyDictionary<string, string> Attributes)> CreatedQueues { get; } = [];
    public HashSet<string> ExistingQueues { get; } = new(StringComparer.Ordinal);
    public int CreateQueueFailuresBeforeSuccess { get; set; }

    /// <summary>What a failing CreateQueue throws; a retryable 5xx unless a test says otherwise.</summary>
    public Func<Exception> CreateQueueFailure { get; set; } = () => SqsTransportTests.TransientSqsException("create queue failed");

    public int CreateQueueAttempts { get; private set; }
    private int _receiveAttempts;
    public List<string> ArnRequests { get; } = [];
    public List<(string QueueUrl, IReadOnlyDictionary<string, string> Attributes)> AttributeUpdates { get; } = [];

    public int DisposeCalls { get; private set; }

    public void Enqueue(SqsTransportDelivery delivery)
        => _deliveries.Writer.TryWrite(delivery);

    public Task<string> GetQueueUrlAsync(string queueName, CancellationToken cancellationToken = default)
    {
        GetQueueUrlCalls++;
        if (GetQueueUrlFailuresBeforeSuccess > 0)
        {
            GetQueueUrlFailuresBeforeSuccess--;
            throw new InvalidOperationException("queue url resolution failed");
        }

        ResolvedQueueNames.Add(queueName);
        return Task.FromResult(UrlFor(queueName));
    }

    public Task<string> CreateQueueAsync(
        string queueName,
        IReadOnlyDictionary<string, string> attributes,
        CancellationToken cancellationToken = default)
    {
        CreateQueueAttempts++;
        if (CreateQueueFailuresBeforeSuccess > 0)
        {
            CreateQueueFailuresBeforeSuccess--;
            throw CreateQueueFailure();
        }

        if (ExistingQueues.Contains(queueName))
            throw new Amazon.SQS.Model.QueueNameExistsException($"Queue '{queueName}' already exists with different attributes.");

        CreatedQueues.Add((queueName, new Dictionary<string, string>(attributes, StringComparer.Ordinal)));
        return Task.FromResult(UrlFor(queueName));
    }

    public Task<string> GetQueueArnAsync(string queueUrl, CancellationToken cancellationToken = default)
    {
        ArnRequests.Add(queueUrl);
        return Task.FromResult(ArnFor(SqsQueueAddress.QueueName(queueUrl)));
    }

    public Task SetQueueAttributesAsync(
        string queueUrl,
        IReadOnlyDictionary<string, string> attributes,
        CancellationToken cancellationToken = default)
    {
        AttributeUpdates.Add((queueUrl, new Dictionary<string, string>(attributes, StringComparer.Ordinal)));
        return Task.CompletedTask;
    }

    public Task<string> SendMessageAsync(SqsOutboundMessage message, CancellationToken cancellationToken = default)
    {
        SendAttempts++;
        if (SendFailuresBeforeSuccess > 0)
        {
            SendFailuresBeforeSuccess--;
            throw SqsTransportTests.TransientSqsException("transient send failed");
        }

        if (SendException is not null)
            throw SendException;

        SentMessages.Add(message);
        return Task.FromResult($"sqs-message-{SentMessages.Count}");
    }

    public async Task<IReadOnlyList<SqsTransportDelivery>> ReceiveMessagesAsync(
        SqsReceiveRequest request,
        CancellationToken cancellationToken = default)
    {
        var attempt = Interlocked.Increment(ref _receiveAttempts);
        LastReceiveRequest = request;
        if (FailuresBeforeReceive > 0)
        {
            FailuresBeforeReceive--;
            throw new InvalidOperationException("receive failed");
        }

        if (attempt == FailOnReceiveAttempt)
            throw SqsTransportTests.TransientSqsException("receive failed after the SDK retries");

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(request.WaitTime);
        try
        {
            if (!await _deliveries.Reader.WaitToReadAsync(timeout.Token).ConfigureAwait(false))
                return [];
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return [];
        }

        var messages = new List<SqsTransportDelivery>(request.MaxMessages);
        while ((ReturnAllAvailable || messages.Count < request.MaxMessages) && _deliveries.Reader.TryRead(out var delivery))
            messages.Add(delivery);
        if (messages.Count > 0)
            OnBatchReceived?.Invoke();
        return messages;
    }

    public ValueTask DisposeAsync()
    {
        DisposeCalls++;
        return ValueTask.CompletedTask;
    }
}
