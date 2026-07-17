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
public sealed class AzureServiceBusWorkerTransport : IWorkerTransport, IAsyncDisposable
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
    public async Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
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
            var messageId = string.IsNullOrWhiteSpace(job.CorrelationId)
                ? Guid.NewGuid().ToString("N")
                : job.CorrelationId;
            var properties = new Dictionary<string, object?>(StringComparer.Ordinal);
            if (!string.IsNullOrWhiteSpace(job.CorrelationId))
                properties[_options.CorrelationIdProperty] = job.CorrelationId;
            var message = new AzureServiceBusOutboundMessage(
                AsyncResponseJson.Serialize(job),
                messageId,
                string.IsNullOrWhiteSpace(job.CorrelationId) ? null : job.CorrelationId,
                properties);

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
                var delay = RetryDelay(attempt);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Runs the IsTransient operation.</summary>
    internal static bool IsTransient(Exception exception)
        => exception is ServiceBusException { IsTransient: true };

    private TimeSpan RetryDelay(int failedAttempt)
    {
        var milliseconds = _options.PublishRetryBaseDelay.TotalMilliseconds * Math.Pow(2, failedAttempt - 1);
        return TimeSpan.FromMilliseconds(Math.Min(milliseconds, _options.PublishRetryMaxDelay.TotalMilliseconds));
    }

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
            _senderGate.Release();
            _senderGate.Dispose();
        }
    }
}
