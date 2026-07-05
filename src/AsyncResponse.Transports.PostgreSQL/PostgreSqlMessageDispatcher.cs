using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Channels;

namespace AsyncResponse.Transports.PostgreSQL;

/// <summary>
/// Applies acknowledgement, redelivery, and dead-letter policy to PostgreSQL transport deliveries.
/// </summary>
internal sealed class PostgreSqlMessageDispatcher : IAsyncDisposable
{
    private readonly Func<PostgreSqlTransportDelivery, CancellationToken, Task> _handler;
    private readonly PostgreSqlAsyncResponseTransportOptions _options;
    private readonly PostgreSqlSubscriberOptions _subscriberOptions;
    private readonly ILogger _logger;
    private readonly PostgreSqlSubscriberRole _role;

    private readonly Channel<PostgreSqlTransportDelivery>? _backgroundQueue;
    private readonly Task[]? _backgroundWorkers;
    private readonly CancellationTokenSource? _backgroundCts;

    public PostgreSqlMessageDispatcher(
        Func<PostgreSqlTransportDelivery, CancellationToken, Task> handler,
        PostgreSqlAsyncResponseTransportOptions options,
        PostgreSqlSubscriberOptions subscriberOptions,
        ILogger logger,
        PostgreSqlSubscriberRole role)
    {
        PostgreSqlTransportOptionsValidator.ValidateSubscriber(subscriberOptions, role.ToString());

        _handler = handler;
        _options = options;
        _subscriberOptions = subscriberOptions;
        _logger = logger;
        _role = role;

        if (subscriberOptions.AckMode is PostgreSqlAckMode.AckAfterReceive)
        {
            _backgroundQueue = Channel.CreateBounded<PostgreSqlTransportDelivery>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
            {
                SingleReader = false,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait
            });
            _backgroundCts = new CancellationTokenSource();
            _backgroundWorkers = new Task[subscriberOptions.BackgroundWorkerCount];
            for (var i = 0; i < _backgroundWorkers.Length; i++)
                _backgroundWorkers[i] = Task.Run(() => BackgroundWorkerLoopAsync(_backgroundCts.Token));
        }
    }

    /// <summary>Handles one claimed row.</summary>
    public async Task HandleAsync(PostgreSqlTransportDelivery delivery, CancellationToken cancellationToken)
    {
        if (_subscriberOptions.AckMode is PostgreSqlAckMode.AckAfterReceive)
        {
            await HandleEarlyAckAsync(delivery).ConfigureAwait(false);
            return;
        }

        try
        {
            await ExecuteHandlerAsync(delivery, cancellationToken).ConfigureAwait(false);
            await delivery.AckAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await HandleFailureAsync(delivery, ex, cancellationToken).ConfigureAwait(false);
        }
    }

    // Single choke point for handler execution so both ACK modes emit the consumer receive span.
    private async Task ExecuteHandlerAsync(PostgreSqlTransportDelivery delivery, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.postgresql.receive",
            ActivityKind.Consumer);
        activity?.SetTag("asyncresponse.transport", "postgresql");
        activity?.SetTag("asyncresponse.postgresql.role", _role.ToString());
        activity?.SetTag("asyncresponse.postgresql.ack_mode", _subscriberOptions.AckMode.ToString());
        activity?.SetTag("messaging.system", "postgresql");
        activity?.SetTag("messaging.destination.name", delivery.Queue);
        activity?.SetTag("messaging.message.id", delivery.Id.ToString());

        if (delivery.Headers.TryGetValue(_options.CorrelationIdHeader, out var correlationId))
            AsyncResponseDiagnostics.SetCorrelationId(activity, correlationId);

        try
        {
            await _handler(delivery, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    private async Task HandleEarlyAckAsync(PostgreSqlTransportDelivery delivery)
    {
        if (_backgroundQueue!.Writer.TryWrite(delivery))
        {
            await delivery.AckAsync().ConfigureAwait(false);
        }
        else
        {
            _logger.LogDebug("Background queue full for PostgreSQL {Role}; releasing row for redelivery.", _role);
            await delivery.NakAsync(_subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
        }
    }

    private async Task BackgroundWorkerLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var delivery in _backgroundQueue!.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                try
                {
                    await ExecuteHandlerAsync(delivery, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "PostgreSQL background handler failed for {Role} on queue {Queue} after early ACK.", _role, delivery.Queue);
                    await delivery.DeadLetterAsync(ex, false, CancellationToken.None).ConfigureAwait(false);
                    await InvokeBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task HandleFailureAsync(PostgreSqlTransportDelivery delivery, Exception exception, CancellationToken cancellationToken)
    {
        var maxAttempts = _subscriberOptions.MaxDeliveryAttempts;
        if (maxAttempts > 0 && delivery.Attempt >= maxAttempts)
        {
            _logger.LogError(
                exception,
                "PostgreSQL message on queue {Queue} ({Role}) failed after {Attempts} attempts; dead-lettering.",
                delivery.Queue,
                _role,
                delivery.Attempt);

            var deadLettered = await delivery.DeadLetterAsync(exception, true, cancellationToken).ConfigureAwait(false);
            if (!deadLettered)
            {
                _logger.LogWarning(exception, "PostgreSQL dead-letter publish failed for queue {Queue} ({Role}); releasing for retry.", delivery.Queue, _role);
                await delivery.NakAsync(_subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
            }
        }
        else
        {
            _logger.LogWarning(
                exception,
                "PostgreSQL message on queue {Queue} ({Role}) failed on attempt {Attempt}; releasing for redelivery.",
                delivery.Queue,
                _role,
                delivery.Attempt);
            await delivery.NakAsync(_subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
        }
    }

    private async Task InvokeBackgroundFailureAsync(PostgreSqlTransportDelivery delivery, Exception exception)
    {
        if (_subscriberOptions.OnBackgroundFailure is null)
            return;

        try
        {
            delivery.Headers.TryGetValue(_options.CorrelationIdHeader, out var correlationId);
            var context = new PostgreSqlBackgroundFailureContext(delivery.Queue, _role.ToString(), delivery.Attempt, correlationId, exception);
            await _subscriberOptions.OnBackgroundFailure(context).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PostgreSQL OnBackgroundFailure callback threw for {Role}.", _role);
        }
    }

    /// <inheritdoc />
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    public async ValueTask DisposeAsync()
    {
        if (_backgroundQueue is null)
            return;

        _backgroundQueue.Writer.TryComplete();
        try
        {
            await Task.WhenAll(_backgroundWorkers!).WaitAsync(_subscriberOptions.BackgroundDrainTimeout).ConfigureAwait(false);
            _backgroundCts!.Dispose();
        }
        catch (TimeoutException)
        {
            _logger.LogWarning("PostgreSQL background handlers for {Role} did not drain within {Timeout}.", _role, _subscriberOptions.BackgroundDrainTimeout);
            await _backgroundCts!.CancelAsync().ConfigureAwait(false);

            // The workers are still running and observe _backgroundCts.Token inside ReadAllAsync, so disposing
            // it now would throw ObjectDisposedException inside them. Dispose once they actually finish, off
            // the shutdown path, so the source is not leaked either.
            _ = Task.WhenAll(_backgroundWorkers!).ContinueWith(
                _ => _backgroundCts.Dispose(),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
        catch (Exception ex)
        {
            // WhenAll only completes once every worker has finished, so the source is safe to dispose here.
            _logger.LogDebug(ex, "PostgreSQL background worker drain for {Role} ended with an error.", _role);
            _backgroundCts!.Dispose();
        }
    }
}
