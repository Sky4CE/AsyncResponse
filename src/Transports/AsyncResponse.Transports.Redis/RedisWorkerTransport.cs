using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Diagnostics;
using System.Text.Json;

namespace AsyncResponse.Transports.Redis;

/// <summary>
/// Publishes <see cref="WorkerJobEnvelope"/> messages to a Redis stream.
/// </summary>
/// <remarks>
/// Redis Streams provide durable queueing and consumer-group acknowledgement. Publishing uses XADD
/// with optional approximate trimming, and transient Redis failures are retried with bounded
/// exponential backoff before the exception is returned to the caller.
/// </remarks>
public sealed class RedisWorkerTransport : IWorkerTransport
{
    private readonly RedisAsyncResponseTransportOptions _options;
    private readonly IRedisStreamDatabase _database;
    private readonly RedisTransportKeySchema _keys;

    /// <summary>Runs the RedisWorkerTransport operation.</summary>
    public RedisWorkerTransport(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IConnectionMultiplexer multiplexer)
        : this(
            options,
            new RedisStreamDatabaseAdapter(
                multiplexer.GetDatabase(),
                RedisTransportOptionsValidatorWithValue(options).OperationTimeout))
    {
    }

    internal RedisWorkerTransport(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IRedisStreamDatabase database)
    {
        _options = options.Value;
        RedisTransportOptionsValidator.ValidateCommon(_options);
        ValidatePublishOptions(_options);
        _database = database;
        _keys = new RedisTransportKeySchema(_options);
    }

    private static RedisAsyncResponseTransportOptions RedisTransportOptionsValidatorWithValue(
        IOptions<RedisAsyncResponseTransportOptions> options)
    {
        RedisTransportOptionsValidator.ValidateCommon(options.Value);
        ValidatePublishOptions(options.Value);
        return options.Value;
    }

    private static void ValidatePublishOptions(RedisAsyncResponseTransportOptions options)
    {
        _ = new RedisTransportKeySchema(options).WorkerStream;
        _ = RedisTransportOptionsValidator.Required(options.PayloadField, nameof(options.PayloadField));
    }

    /// <summary>Publishes the supplied message.</summary>
    public async Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);

        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.worker.publish",
            ActivityKind.Producer,
            job.CorrelationId);
        activity?.SetTag("asyncresponse.transport", "redis");
        activity?.SetTag("messaging.system", "redis");
        activity?.SetTag("messaging.destination.name", _keys.WorkerStream.ToString());
        AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
        AsyncResponseDiagnostics.SetWorker(activity, job.Call);

        try
        {
            var fields = CreateMessageFields(
                JsonSerializer.Serialize(job),
                job.CorrelationId,
                _options);
            var messageId = await RedisTransportRetry.ExecuteAsync(
                token => _database.StreamAddAsync(
                    _keys.WorkerStream,
                    fields,
                    _options.StreamMaxLength,
                    _options.UseApproximateStreamTrimming,
                    token),
                _options.PublishMaxAttempts,
                _options.PublishRetryBaseDelay,
                _options.PublishRetryMaxDelay,
                cancellationToken).ConfigureAwait(false);

            activity?.SetTag("messaging.message.id", messageId.ToString());
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    internal static NameValueEntry[] CreateMessageFields(
        string payloadJson,
        string? correlationId,
        RedisAsyncResponseTransportOptions options)
    {
        var payloadField = RedisTransportOptionsValidator.Required(options.PayloadField, nameof(options.PayloadField));
        var correlationField = RedisTransportOptionsValidator.Required(options.CorrelationIdField, nameof(options.CorrelationIdField));

        return string.IsNullOrWhiteSpace(correlationId)
            ? [new NameValueEntry(payloadField, payloadJson)]
            :
            [
                new NameValueEntry(payloadField, payloadJson),
                new NameValueEntry(correlationField, correlationId)
            ];
    }
}
