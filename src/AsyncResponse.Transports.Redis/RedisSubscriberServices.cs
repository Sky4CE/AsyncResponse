using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace AsyncResponse.Transports.Redis;

internal abstract class RedisSubscriberService : BackgroundService
{
    private static readonly string GeneratedConsumerName = CreateGeneratedConsumerName();

    private readonly IRedisStreamDatabase _database;

    protected RedisSubscriberService(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IConnectionMultiplexer multiplexer,
        ILogger logger)
        : this(
            options,
            new RedisStreamDatabaseAdapter(multiplexer.GetDatabase(), options.Value.OperationTimeout),
            logger)
    {
    }

    protected RedisSubscriberService(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IRedisStreamDatabase database,
        ILogger logger)
    {
        Options = options.Value;
        RedisTransportOptionsValidator.ValidateCommon(Options);
        _database = database;
        Logger = logger;
    }

    protected RedisAsyncResponseTransportOptions Options { get; }
    protected ILogger Logger { get; }

    protected abstract RedisKey Stream { get; }
    protected abstract RedisValue ConsumerGroup { get; }
    protected abstract RedisSubscriberOptions SubscriberOptions { get; }
    protected abstract RedisSubscriberRole SubscriberRole { get; }
    protected abstract Task HandleMessageAsync(RedisStreamDelivery delivery, CancellationToken cancellationToken);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        RedisMessageDispatcher.ValidateOptions(Options, SubscriberOptions, SubscriberRole);

        var failures = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunSubscriberAsync(stoppingToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                failures++;
                var retryDelay = RedisTransportRetry.Backoff(
                    failures,
                    Options.SubscriberRetryBaseDelay,
                    Options.SubscriberRetryMaxDelay);
                Logger.LogWarning(
                    ex,
                    "Redis subscriber failed for stream {Stream} ({Role}); retrying in {RetryDelay}.",
                    Stream.ToString(),
                    SubscriberRole,
                    retryDelay);
                await Task.Delay(retryDelay, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private async Task RunSubscriberAsync(CancellationToken stoppingToken)
    {
        if (Options.CreateConsumerGroups)
            await EnsureConsumerGroupAsync(stoppingToken).ConfigureAwait(false);

        var consumerName = ResolveConsumerName(Options, SubscriberRole);
        await using var dispatcher = RedisMessageDispatcher.Create(
            HandleMessageAsync,
            _database,
            Options,
            SubscriberOptions,
            Logger,
            Stream,
            ConsumerGroup,
            SubscriberRole);

        Logger.LogInformation(
            "Redis subscriber started. Stream: {Stream}. Group: {ConsumerGroup}. Consumer: {ConsumerName}. Role: {Role}. AckMode: {AckMode}.",
            Stream.ToString(),
            ConsumerGroup.ToString(),
            consumerName.ToString(),
            SubscriberRole,
            SubscriberOptions.AckMode);

        var nextPendingClaimAt = DateTimeOffset.UtcNow;
        while (!stoppingToken.IsCancellationRequested)
        {
            var processed = 0;

            // When the dispatcher is saturated (ACK-after-enqueue queue full) stop pulling new entries:
            // reading them would only move the backlog into the pending-entry list and spin the loop.
            // The unread entries stay as new messages in the stream until capacity frees.
            if (dispatcher.CanAcceptMore)
            {
                var utcNow = DateTimeOffset.UtcNow;
                if (utcNow >= nextPendingClaimAt)
                {
                    processed += await ClaimPendingAsync(dispatcher, consumerName, stoppingToken).ConfigureAwait(false);
                    nextPendingClaimAt = utcNow + SubscriberOptions.PendingClaimInterval;
                }

                var entries = await _database.StreamReadGroupAsync(
                    Stream,
                    ConsumerGroup,
                    consumerName,
                    SubscriberOptions.BatchSize,
                    stoppingToken).ConfigureAwait(false);

                foreach (var entry in entries)
                {
                    if (await DispatchEntryAsync(dispatcher, entry, attempt: 1, stoppingToken).ConfigureAwait(false)
                        == RedisDispatchOutcome.Processed)
                    {
                        processed++;
                    }
                }
            }

            // Throttle when nothing advanced — an empty stream, or every entry deferred under backpressure.
            if (processed == 0)
                await Task.Delay(SubscriberOptions.EmptyPollDelay, stoppingToken).ConfigureAwait(false);
        }
    }

    private async Task<RedisDispatchOutcome> DispatchEntryAsync(
        RedisMessageDispatcher dispatcher,
        StreamEntry entry,
        int attempt,
        CancellationToken cancellationToken)
    {
        RedisStreamDelivery delivery;
        try
        {
            delivery = CreateDelivery(entry, attempt);
        }
        catch (InvalidDataException ex)
        {
            // A foreign/malformed entry (or a trimmed tombstone) can never be handled; dead-letter and
            // ACK it so it drains instead of poisoning the pending-claim loop forever.
            await dispatcher.DiscardUnprocessableAsync(Stream, ConsumerGroup, entry, ex, cancellationToken).ConfigureAwait(false);
            return RedisDispatchOutcome.Processed;
        }

        return await dispatcher.HandleAsync(delivery, cancellationToken).ConfigureAwait(false);
    }

    private async Task EnsureConsumerGroupAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _database.StreamCreateConsumerGroupAsync(
                Stream,
                ConsumerGroup,
                StreamPosition.Beginning,
                createStream: true,
                cancellationToken).ConfigureAwait(false);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP", StringComparison.OrdinalIgnoreCase))
        {
            // The group already exists; this is the expected path after the first app instance.
        }
    }

    private async Task<int> ClaimPendingAsync(
        RedisMessageDispatcher dispatcher,
        RedisValue consumerName,
        CancellationToken cancellationToken)
    {
        var minIdleMs = ToPositiveMilliseconds(SubscriberOptions.PendingMessageMinIdleTime);
        var pending = await _database.StreamPendingMessagesAsync(
            Stream,
            ConsumerGroup,
            SubscriberOptions.PendingClaimBatchSize,
            RedisValue.Null,
            minId: null,
            maxId: null,
            minIdleMs,
            cancellationToken).ConfigureAwait(false);

        if (pending.Length == 0)
            return 0;

        var pendingById = pending.ToDictionary(
            item => item.MessageId.ToString(),
            StringComparer.Ordinal);
        var claimed = await _database.StreamClaimAsync(
            Stream,
            ConsumerGroup,
            consumerName,
            minIdleMs,
            pending.Select(item => item.MessageId).ToArray(),
            cancellationToken).ConfigureAwait(false);

        var processed = 0;
        foreach (var entry in claimed)
        {
            var priorDeliveries = pendingById.TryGetValue(entry.Id.ToString(), out var info)
                ? info.DeliveryCount
                : 1;
            if (await DispatchEntryAsync(dispatcher, entry, Math.Max(1, priorDeliveries + 1), cancellationToken).ConfigureAwait(false)
                == RedisDispatchOutcome.Processed)
            {
                processed++;
            }
        }

        return processed;
    }

    private RedisStreamDelivery CreateDelivery(StreamEntry entry, int attempt)
    {
        var payload = RedisCorrelationIdExtractor.TryReadField(entry, Options.PayloadField);
        if (string.IsNullOrWhiteSpace(payload))
        {
            throw new InvalidDataException(
                $"Redis stream entry {entry.Id} on {Stream.ToString()} does not contain payload field '{Options.PayloadField}'.");
        }

        var correlationId = SubscriberRole is RedisSubscriberRole.ResponseIngress
            ? RedisCorrelationIdExtractor.Extract(entry, payload, Options)
            : RedisCorrelationIdExtractor.TryReadField(entry, Options.CorrelationIdField);

        return new RedisStreamDelivery(
            Stream,
            ConsumerGroup,
            entry.Id,
            payload,
            correlationId,
            attempt,
            entry);
    }

    private static long ToPositiveMilliseconds(TimeSpan value)
        => Math.Max(1, (long)Math.Ceiling(value.TotalMilliseconds));

    private static RedisValue ResolveConsumerName(
        RedisAsyncResponseTransportOptions options,
        RedisSubscriberRole role)
    {
        // Append the role even to an explicitly configured name so the worker and response subscribers
        // never share a consumer identity (and therefore a pending-entry list) within their groups.
        var baseName = !string.IsNullOrWhiteSpace(options.ConsumerName)
            ? options.ConsumerName
            : GeneratedConsumerName;

        return $"{baseName}-{role.ToString().ToLowerInvariant()}";
    }

    private static string CreateGeneratedConsumerName()
    {
        var name = $"{Environment.MachineName}-{Environment.ProcessId}-{Guid.NewGuid():N}";
        return TrimConsumerName(name);
    }

    internal static string TrimConsumerName(string name)
        => name.Length <= 64 ? name : name[..64];
}

internal sealed class RedisWorkerSubscriber : RedisSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;
    private readonly RedisTransportKeySchema _keys;

    public RedisWorkerSubscriber(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IConnectionMultiplexer multiplexer,
        IAsyncResponseIngress ingress,
        ILogger<RedisWorkerSubscriber> logger)
        : base(options, multiplexer, logger)
    {
        _ingress = ingress;
        _keys = new RedisTransportKeySchema(options.Value);
    }

    internal RedisWorkerSubscriber(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IRedisStreamDatabase database,
        IAsyncResponseIngress ingress,
        ILogger<RedisWorkerSubscriber> logger)
        : base(options, database, logger)
    {
        _ingress = ingress;
        _keys = new RedisTransportKeySchema(options.Value);
    }

    protected override RedisKey Stream => _keys.WorkerStream;
    protected override RedisValue ConsumerGroup => Options.WorkerConsumerGroup;
    protected override RedisSubscriberOptions SubscriberOptions => Options.WorkerSubscriber;
    protected override RedisSubscriberRole SubscriberRole => RedisSubscriberRole.Worker;

    protected override Task HandleMessageAsync(RedisStreamDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleWorkerMessageAsync(delivery.Payload);
}

internal sealed class RedisResponseIngressSubscriber : RedisSubscriberService
{
    private readonly IAsyncResponseIngress _ingress;
    private readonly RedisTransportKeySchema _keys;

    public RedisResponseIngressSubscriber(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IConnectionMultiplexer multiplexer,
        IAsyncResponseIngress ingress,
        ILogger<RedisResponseIngressSubscriber> logger)
        : base(options, multiplexer, logger)
    {
        _ingress = ingress;
        _keys = new RedisTransportKeySchema(options.Value);
    }

    internal RedisResponseIngressSubscriber(
        IOptions<RedisAsyncResponseTransportOptions> options,
        IRedisStreamDatabase database,
        IAsyncResponseIngress ingress,
        ILogger<RedisResponseIngressSubscriber> logger)
        : base(options, database, logger)
    {
        _ingress = ingress;
        _keys = new RedisTransportKeySchema(options.Value);
    }

    protected override RedisKey Stream => _keys.ResponseStream;
    protected override RedisValue ConsumerGroup => Options.ResponseConsumerGroup;
    protected override RedisSubscriberOptions SubscriberOptions => Options.ResponseSubscriber;
    protected override RedisSubscriberRole SubscriberRole => RedisSubscriberRole.ResponseIngress;

    protected override Task HandleMessageAsync(RedisStreamDelivery delivery, CancellationToken cancellationToken)
        => _ingress.HandleResponseMessageAsync(delivery.Payload, delivery.CorrelationId);
}
