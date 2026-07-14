using Microsoft.Extensions.Logging;
using System.Threading.Channels;

namespace AsyncResponse.Transports.SqlServer;

/// <summary>
/// Applies acknowledgement, redelivery, and dead-letter policy to SQL Server transport deliveries.
/// </summary>
internal sealed class SqlServerMessageDispatcher : IAsyncDisposable
{
    private readonly Func<SqlServerTransportDelivery, CancellationToken, Task> _handler;
    private readonly SqlServerAsyncResponseTransportOptions _options;
    private readonly SqlServerSubscriberOptions _subscriberOptions;
    private readonly ILogger _logger;
    private readonly SqlServerSubscriberRole _role;

    private readonly Channel<SqlServerTransportDelivery>? _backgroundQueue;
    private readonly Task[]? _backgroundWorkers;
    private readonly CancellationTokenSource? _backgroundCts;

    public SqlServerMessageDispatcher(
        Func<SqlServerTransportDelivery, CancellationToken, Task> handler,
        SqlServerAsyncResponseTransportOptions options,
        SqlServerSubscriberOptions subscriberOptions,
        ILogger logger,
        SqlServerSubscriberRole role)
    {
        SqlServerTransportOptionsValidator.ValidateSubscriber(subscriberOptions, role.ToString());

        _handler = handler;
        _options = options;
        _subscriberOptions = subscriberOptions;
        _logger = logger;
        _role = role;

        if (subscriberOptions.AckMode is SqlServerAckMode.AckAfterEnqueue)
        {
            _backgroundQueue = Channel.CreateBounded<SqlServerTransportDelivery>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
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
    public async Task HandleAsync(SqlServerTransportDelivery delivery, CancellationToken cancellationToken)
    {
        if (_subscriberOptions.AckMode is SqlServerAckMode.AckAfterEnqueue)
        {
            await HandleEarlyAckAsync(delivery).ConfigureAwait(false);
            return;
        }

        try
        {
            await _handler(delivery, cancellationToken).ConfigureAwait(false);
            await delivery.AckAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await HandleFailureAsync(delivery, ex, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task HandleEarlyAckAsync(SqlServerTransportDelivery delivery)
    {
        if (_backgroundQueue!.Writer.TryWrite(delivery))
        {
            await delivery.AckAsync().ConfigureAwait(false);
        }
        else
        {
            _logger.LogDebug("Background queue full for SQL Server {Role}; releasing row for redelivery.", _role);
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
                    await _handler(delivery, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "SQL Server background handler failed for {Role} on queue {Queue} after early ACK.", _role, delivery.Queue);
                    await delivery.DeadLetterAsync(ex, false, CancellationToken.None).ConfigureAwait(false);
                    await InvokeBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task HandleFailureAsync(SqlServerTransportDelivery delivery, Exception exception, CancellationToken cancellationToken)
    {
        var maxAttempts = _subscriberOptions.MaxDeliveryAttempts;
        if (maxAttempts > 0 && delivery.Attempt >= maxAttempts)
        {
            _logger.LogError(
                exception,
                "SQL Server message on queue {Queue} ({Role}) failed after {Attempts} attempts; dead-lettering.",
                delivery.Queue,
                _role,
                delivery.Attempt);

            var deadLettered = await delivery.DeadLetterAsync(exception, true, cancellationToken).ConfigureAwait(false);
            if (!deadLettered)
            {
                _logger.LogWarning(exception, "SQL Server dead-letter publish failed for queue {Queue} ({Role}); releasing for retry.", delivery.Queue, _role);
                await delivery.NakAsync(_subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
            }
        }
        else
        {
            _logger.LogWarning(
                exception,
                "SQL Server message on queue {Queue} ({Role}) failed on attempt {Attempt}; releasing for redelivery.",
                delivery.Queue,
                _role,
                delivery.Attempt);
            await delivery.NakAsync(_subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
        }
    }

    private async Task InvokeBackgroundFailureAsync(SqlServerTransportDelivery delivery, Exception exception)
    {
        if (_subscriberOptions.OnBackgroundFailure is null)
            return;

        try
        {
            delivery.Headers.TryGetValue(_options.CorrelationIdHeader, out var correlationId);
            var context = new SqlServerBackgroundFailureContext(delivery.Queue, _role.ToString(), delivery.Attempt, correlationId, exception);
            await _subscriberOptions.OnBackgroundFailure(context).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SQL Server OnBackgroundFailure callback threw for {Role}.", _role);
        }
    }

    /// <inheritdoc />
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
            _logger.LogWarning("SQL Server background handlers for {Role} did not drain within {Timeout}.", _role, _subscriberOptions.BackgroundDrainTimeout);
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
            _logger.LogDebug(ex, "SQL Server background worker drain for {Role} ended with an error.", _role);
            _backgroundCts!.Dispose();
        }
    }
}
