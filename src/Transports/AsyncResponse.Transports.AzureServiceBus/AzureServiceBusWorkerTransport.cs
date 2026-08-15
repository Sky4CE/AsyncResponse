using Microsoft.Extensions.Options;
using Azure.Messaging.ServiceBus;
using System.Diagnostics;
using System.Text.Json;

namespace AsyncResponse.Transports.AzureServiceBus;

/// <summary>Publishes <see cref="WorkerJobEnvelope"/> messages to an Azure Service Bus queue.</summary>
/// <remarks>
/// A Service Bus sender is created lazily and reused for the lifetime of the transport. Send
/// operations are explicitly awaited so the method completes only after Service Bus accepts the
/// transfer or the Azure SDK reports a failure.
/// </remarks>
public sealed class AzureServiceBusWorkerTransport : IWorkerTransport, IDelayedWorkerTransport, IAsyncDisposable
{
    private readonly AzureServiceBusAsyncResponseOptions _options;
    private readonly IAzureServiceBusClient _client;
    private readonly bool _disposeClient;
    private readonly SemaphoreSlim _senderGate = new(1, 1);
    private IAzureServiceBusSender? _sender;
    private int _disposeGate;
    private bool _disposed;

    /// <summary>Creates a worker transport backed by the configured connection string.</summary>
    public AzureServiceBusWorkerTransport(IOptions<AzureServiceBusAsyncResponseOptions> options)
        : this(
            options,
            new AzureServiceBusClientAdapter(new ServiceBusClient(
                AzureServiceBusOptionsValidator.Required(options.Value.ConnectionString, nameof(options.Value.ConnectionString))),
                ownsClient: true),
            disposeClient: true)
    {
    }

    internal AzureServiceBusWorkerTransport(
        IOptions<AzureServiceBusAsyncResponseOptions> options,
        IAzureServiceBusClient client)
        : this(options, client, disposeClient: false)
    {
    }

    private AzureServiceBusWorkerTransport(
        IOptions<AzureServiceBusAsyncResponseOptions> options,
        IAzureServiceBusClient client,
        bool disposeClient)
    {
        _options = options.Value;
        AzureServiceBusOptionsValidator.ValidateCommon(_options);
        _client = client;
        _disposeClient = disposeClient;
    }

    private async Task<IAzureServiceBusSender> GetSenderAsync(CancellationToken cancellationToken)
    {
        var sender = Volatile.Read(ref _sender);
        if (sender is not null)
            return sender;

        await _senderGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _sender ??= _client.CreateSender(_options.WorkerQueue);
            return _sender;
        }
        finally
        {
            _senderGate.Release();
        }
    }

    /// <summary>Publishes the supplied worker job.</summary>
    public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        => PublishCoreAsync(job, delay: null, cancellationToken);

    /// <inheritdoc/>
    /// <remarks>Service Bus scheduled messages accept any future enqueue time; no per-hop chunking is needed.</remarks>
    public TimeSpan MaxPublishDelay => AsyncResponseChannelOptions.MaxPersistenceTtl;

    /// <inheritdoc/>
    public Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(delay, MaxPublishDelay);
        return PublishCoreAsync(job, delay > TimeSpan.Zero ? delay : null, cancellationToken);
    }

    private async Task PublishCoreAsync(WorkerJobEnvelope job, TimeSpan? delay, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(job);

        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.worker.publish",
            ActivityKind.Producer,
            job.CorrelationId);
        activity?.SetTag("asyncresponse.transport", "azure_service_bus");
        activity?.SetTag("messaging.system", "azure_service_bus");
        activity?.SetTag("messaging.destination.name", _options.WorkerQueue);
        AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
        AsyncResponseDiagnostics.SetWorker(activity, job.Call);

        try
        {
            // Every message carries a fresh MessageId: MessageId is the duplicate-detection key, so
            // reusing the correlation id would silently drop the second job of a flow published on a
            // dedup-enabled queue inside its detection window (distinct jobs of one flow share the
            // correlation id). The correlation id still travels in the Service Bus CorrelationId
            // system property and the configured application property.
            var messageId = Guid.NewGuid().ToString("N");
            var properties = new Dictionary<string, object?>(StringComparer.Ordinal);
            if (!string.IsNullOrWhiteSpace(job.CorrelationId))
                properties[_options.CorrelationIdProperty] = job.CorrelationId;
            var message = new AzureServiceBusOutboundMessage(
                AsyncResponseJson.Serialize(job),
                messageId,
                string.IsNullOrWhiteSpace(job.CorrelationId) ? null : job.CorrelationId,
                properties,
                delay is { } pending ? DateTimeOffset.UtcNow.Add(pending) : null);
            if (delay is { } delayTag)
                activity?.SetTag("asyncresponse.worker.delay_seconds", delayTag.TotalSeconds);

            var sender = await GetSenderAsync(cancellationToken).ConfigureAwait(false);
            await SendWithRetryAsync(sender, message, cancellationToken).ConfigureAwait(false);
            activity?.SetTag("messaging.message.id", messageId);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    private async Task SendWithRetryAsync(
        IAzureServiceBusSender sender,
        AzureServiceBusOutboundMessage message,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await sender.SendMessageAsync(message, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (IsTransient(ex) && attempt < _options.PublishMaxAttempts && !cancellationToken.IsCancellationRequested)
            {
                var delay = AsyncResponseRetry.Backoff(attempt, _options.PublishRetryBaseDelay, _options.PublishRetryMaxDelay);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Runs the IsTransient operation.</summary>
    internal static bool IsTransient(Exception exception)
        => exception is ServiceBusException { IsTransient: true };

    /// <summary>Releases resources held by this instance.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeGate, 1) != 0)
            return;

        await _senderGate.WaitAsync().ConfigureAwait(false);
        try
        {
            _disposed = true;
            if (_sender is not null)
            {
                using var cts = new CancellationTokenSource(_options.ShutdownTimeout);
                try
                {
                    await _sender.CloseAsync(cts.Token).ConfigureAwait(false);
                }
                catch
                {
                    // Best effort: the sender link may already be closed by broker-side shutdown.
                }

                await _sender.DisposeAsync().ConfigureAwait(false);
            }

            if (_disposeClient)
                await _client.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            // Release, never Dispose: SemaphoreSlim.Dispose does not complete pending WaitAsync
            // waiters, so disposing here would strand publishers parked on the gate forever (and
            // the first woken waiter's finally would throw trying to Release a disposed
            // semaphore, never handing the permit on). Released, each parked waiter wakes in
            // turn and observes _disposed; the gate holds no unmanaged resources, so leaving it
            // undisposed leaks nothing.
            _senderGate.Release();
        }
    }
}
