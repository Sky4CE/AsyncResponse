using Microsoft.Extensions.Logging;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Channels;

namespace AsyncResponse.Transports;

// Shared source for the database-backed worker transports (PostgreSQL, SQL Server, MongoDB),
// mirroring src/Channels/Shared/DbChannelShared.cs: each transport csproj pulls this file in via
// <Compile Include="..\Shared\DbTransportShared.cs" />, so the base class compiles INTO each
// provider assembly against that provider's concrete seam types. The seam is bound per project
// with global using aliases (declared at the top of the provider's MessageDispatcher file):
//
//   DbTransportOptions          -> the provider's transport options (e.g. PostgreSqlAsyncResponseTransportOptions)
//   DbSubscriberOptions         -> the provider's subscriber options (e.g. PostgreSqlSubscriberOptions)
//   DbTransportDelivery         -> the provider's claimed-delivery type (e.g. PostgreSqlTransportDelivery)
//   DbSubscriberRole            -> the provider's subscriber-role enum
//   DbAckMode                   -> the provider's ack-mode enum
//   DbTransportOptionsValidator -> the provider's static options validator
//   DbBackgroundFailureContext  -> the provider's OnBackgroundFailure context type
//
// Because the aliases resolve to concrete sealed types at compile time, delivery calls stay
// direct — no interface dispatch on the per-message path. The only provider-specific inputs are
// three display strings supplied by the derived constructor: the provider name rendered into log
// messages, the queue-item noun ("row"/"document"), and the lowercase telemetry tag. Rendered log
// output and activity tags are byte-identical to the pre-extraction per-provider sources.

/// <summary>
/// Applies acknowledgement, redelivery, and dead-letter policy to database transport deliveries:
/// ack-after-handler with fenced lease renewal, opt-in early ACK behind a bounded in-process
/// queue with drain-on-dispose, attempt-capped dead-lettering, and the consumer receive span.
/// Derived dispatchers supply only the provider display name, queue-item noun, and telemetry tag.
/// </summary>
internal abstract class DbMessageDispatcherBase : IAsyncDisposable
{
    private readonly Func<DbTransportDelivery, CancellationToken, Task> _handler;
    private readonly DbTransportOptions _options;
    private readonly DbSubscriberOptions _subscriberOptions;
    private readonly ILogger _logger;
    private readonly DbSubscriberRole _role;
    private readonly string _providerName;
    private readonly string _unitNoun;
    private readonly string _receiveActivityName;
    private readonly string _transportTag;
    private readonly string _roleTagName;
    private readonly string _ackModeTagName;

    private readonly Channel<DbTransportDelivery>? _backgroundQueue;
    private readonly Task[]? _backgroundWorkers;
    private readonly CancellationTokenSource? _backgroundCts;

    protected DbMessageDispatcherBase(
        Func<DbTransportDelivery, CancellationToken, Task> handler,
        DbTransportOptions options,
        DbSubscriberOptions subscriberOptions,
        ILogger logger,
        DbSubscriberRole role,
        string providerName,
        string unitNoun,
        string telemetryName)
    {
        DbTransportOptionsValidator.ValidateSubscriber(options, subscriberOptions, role.ToString());

        _handler = handler;
        _options = options;
        _subscriberOptions = subscriberOptions;
        _logger = logger;
        _role = role;
        _providerName = providerName;
        _unitNoun = unitNoun;
        _receiveActivityName = $"asyncresponse.{telemetryName}.receive";
        _transportTag = telemetryName;
        _roleTagName = $"asyncresponse.{telemetryName}.role";
        _ackModeTagName = $"asyncresponse.{telemetryName}.ack_mode";

        if (subscriberOptions.AckMode is DbAckMode.AckAfterEnqueue)
        {
            _backgroundQueue = Channel.CreateBounded<DbTransportDelivery>(new BoundedChannelOptions(subscriberOptions.BackgroundQueueCapacity)
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

    /// <summary>Handles one claimed queue item.</summary>
    public async Task HandleAsync(DbTransportDelivery delivery, CancellationToken cancellationToken)
    {
        if (_subscriberOptions.AckMode is DbAckMode.AckAfterEnqueue)
        {
            await HandleEarlyAckAsync(delivery).ConfigureAwait(false);
            return;
        }

        try
        {
            // While the handler runs, a fenced heartbeat keeps extending the claim's lease at
            // LockTimeout/2 cadence so a slow handler does not let the lock lapse and a competing
            // subscriber re-claim (and duplicate-process) the queue item.
            using var renewalCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var renewalTask = RenewLeaseLoopAsync(delivery, renewalCancellation.Token);
            try
            {
                await ExecuteHandlerAsync(delivery, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                renewalCancellation.Cancel();
                await renewalTask.ConfigureAwait(false);
            }

            await delivery.AckAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await HandleFailureAsync(delivery, ex, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task RenewLeaseLoopAsync(DbTransportDelivery delivery, CancellationToken cancellationToken)
    {
        var interval = TimeSpan.FromTicks(Math.Max(1, _options.LockTimeout.Ticks / 2));
        try
        {
            while (true)
            {
                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);

                bool renewed;
                try
                {
                    renewed = await delivery.RenewAsync().ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Failed to renew the lease of {Provider} message {MessageId} on queue {Queue} ({Role}); retrying next beat.",
                        _providerName,
                        delivery.Id,
                        delivery.Queue,
                        _role);
                    continue;
                }

                if (!renewed)
                {
                    // The lock_id fence no longer matches: the lease expired and another subscriber
                    // claimed the queue item. Stop renewing; the fenced ack/NAK will no-op for this
                    // claim.
                    _logger.LogWarning(
                        "Lease of {Provider} message {MessageId} on queue {Queue} ({Role}) was lost; another subscriber may process it (at-least-once preserved).",
                        _providerName,
                        delivery.Id,
                        delivery.Queue,
                        _role);
                    return;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // The handler finished or the subscriber is stopping.
        }
    }

    // Single choke point for handler execution so both ACK modes emit the consumer receive span.
    private async Task ExecuteHandlerAsync(DbTransportDelivery delivery, CancellationToken cancellationToken)
    {
        using var activity = AsyncResponseDiagnostics.StartActivity(
            _receiveActivityName,
            System.Diagnostics.ActivityKind.Consumer);
        activity?.SetTag("asyncresponse.transport", _transportTag);
        activity?.SetTag(_roleTagName, _role.ToString());
        activity?.SetTag(_ackModeTagName, _subscriberOptions.AckMode.ToString());
        activity?.SetTag("messaging.system", _transportTag);
        activity?.SetTag("messaging.destination.name", delivery.Queue);
        activity?.SetTag("messaging.message.id", delivery.Id.ToString());
        activity?.SetTag("messaging.message.delivery_attempt", delivery.Attempt);

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

    private async Task HandleEarlyAckAsync(DbTransportDelivery delivery)
    {
        if (_backgroundQueue!.Writer.TryWrite(delivery))
        {
            await delivery.AckAsync().ConfigureAwait(false);
        }
        else
        {
            _logger.LogDebug("Background queue full for {Provider} {Role}; releasing {Unit} for redelivery.", _providerName, _role, _unitNoun);
            await delivery.NakAsync(_subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
        }
    }

    private async Task BackgroundWorkerLoopAsync(CancellationToken cancellationToken)
    {
        // Token-less ReadAllAsync: on shutdown the queue is completed and fully drained, so every
        // already-ACKed queue item is attempted (with the drain token once the drain budget lapses)
        // instead of being silently dropped; each failure is dead-lettered and surfaced below.
        await foreach (var delivery in _backgroundQueue!.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            try
            {
                await ExecuteHandlerAsync(delivery, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{Provider} background handler failed for {Role} on queue {Queue} after early ACK.", _providerName, _role, delivery.Queue);
                if (!await delivery.DeadLetterAsync(ex, false, CancellationToken.None).ConfigureAwait(false))
                {
                    _logger.LogError(
                        "Failed to dead-letter already-ACKed {Provider} message {MessageId} on queue {Queue} ({Role}); the failure is only observable via logs and OnBackgroundFailure.",
                        _providerName,
                        delivery.Id,
                        delivery.Queue,
                        _role);
                }

                await InvokeBackgroundFailureAsync(delivery, ex).ConfigureAwait(false);
            }
        }
    }

    private async Task HandleFailureAsync(DbTransportDelivery delivery, Exception exception, CancellationToken cancellationToken)
    {
        var maxAttempts = _subscriberOptions.MaxDeliveryAttempts;
        if (maxAttempts > 0 && delivery.Attempt >= maxAttempts)
        {
            _logger.LogError(
                exception,
                "{Provider} message on queue {Queue} ({Role}) failed after {Attempts} attempts; dead-lettering.",
                _providerName,
                delivery.Queue,
                _role,
                delivery.Attempt);

            var deadLettered = await delivery.DeadLetterAsync(exception, true, cancellationToken).ConfigureAwait(false);
            if (!deadLettered)
            {
                _logger.LogWarning(exception, "{Provider} dead-letter publish failed for queue {Queue} ({Role}); releasing for retry.", _providerName, delivery.Queue, _role);
                await delivery.NakAsync(_subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
            }
        }
        else
        {
            _logger.LogWarning(
                exception,
                "{Provider} message on queue {Queue} ({Role}) failed on attempt {Attempt}; releasing for redelivery.",
                _providerName,
                delivery.Queue,
                _role,
                delivery.Attempt);
            await delivery.NakAsync(_subscriberOptions.RedeliveryDelay).ConfigureAwait(false);
        }
    }

    private async Task InvokeBackgroundFailureAsync(DbTransportDelivery delivery, Exception exception)
    {
        if (_subscriberOptions.OnBackgroundFailure is null)
            return;

        try
        {
            delivery.Headers.TryGetValue(_options.CorrelationIdHeader, out var correlationId);
            var context = new DbBackgroundFailureContext(delivery.Queue, _role.ToString(), delivery.Attempt, correlationId, exception);
            await _subscriberOptions.OnBackgroundFailure(context).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{Provider} OnBackgroundFailure callback threw for {Role}.", _providerName, _role);
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
            _logger.LogWarning("{Provider} background handlers for {Role} did not drain within {Timeout}.", _providerName, _role, _subscriberOptions.BackgroundDrainTimeout);
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
            _logger.LogDebug(ex, "{Provider} background worker drain for {Role} ended with an error.", _providerName, _role);
            _backgroundCts!.Dispose();
        }
    }
}

/// <summary>
/// Extracts the AsyncResponse correlation id from the queue item's metadata first, then from the
/// JSON response body via configured paths. Shared verbatim by the three database transports —
/// the header name and JSON paths both come from the aliased options type.
/// </summary>
internal static class DbCorrelationIdExtractor
{
    public static string? Extract(
        IReadOnlyDictionary<string, string>? headers,
        string messageJson,
        DbTransportOptions options)
    {
        var headerName = DbTransportOptionsValidator.Required(options.CorrelationIdHeader, nameof(options.CorrelationIdHeader));
        if (headers is not null && headers.TryGetValue(headerName, out var headerValue) && !string.IsNullOrWhiteSpace(headerValue))
            return headerValue;

        var jsonPaths = options.CorrelationIdJsonPaths;
        if (jsonPaths is null || jsonPaths.Length == 0 || string.IsNullOrWhiteSpace(messageJson))
            return null;

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(messageJson);
        }
        catch (JsonException)
        {
            return null;
        }

        if (root is null)
            return null;

        foreach (var path in jsonPaths)
        {
            var value = TryReadPath(root, path);
            if (!string.IsNullOrWhiteSpace(value))
                return value;
        }

        return null;
    }

    private static string? TryReadPath(JsonNode root, string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return null;

        var current = root;
        foreach (var segment in path.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            current = UnwrapJsonString(current);
            if (current is not JsonObject obj)
                return null;

            current = TryGetProperty(obj, segment);
            if (current is null)
                return null;
        }

        current = UnwrapJsonString(current);
        return current switch
        {
            JsonValue value when value.TryGetValue<string>(out var s) => s,
            JsonValue value => value.ToString(),
            _ => null
        };
    }

    private static JsonNode? TryGetProperty(JsonObject obj, string name)
    {
        if (obj.TryGetPropertyValue(name, out var exact))
            return exact;

        foreach (var property in obj)
        {
            if (string.Equals(property.Key, name, StringComparison.OrdinalIgnoreCase))
                return property.Value;
        }

        return null;
    }

    private static JsonNode? UnwrapJsonString(JsonNode? node)
    {
        if (node is not JsonValue value || !value.TryGetValue<string>(out var text))
            return node;

        var trimmed = text.AsSpan().TrimStart();
        if (trimmed.Length == 0 || (trimmed[0] != '{' && trimmed[0] != '['))
            return node;

        try
        {
            return JsonNode.Parse(text);
        }
        catch (JsonException)
        {
            return node;
        }
    }
}
