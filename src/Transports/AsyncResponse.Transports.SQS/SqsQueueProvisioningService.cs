using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System.Text.Json;

namespace AsyncResponse.Transports.SQS;

/// <summary>
/// Provisions the worker and response queues (each with a dead-letter queue wired through a native
/// redrive policy) when <see cref="SqsAsyncResponseOptions.CreateQueues"/> is enabled. Registered
/// before the subscribers so provisioning completes before consumption starts; queues configured as
/// URLs are assumed to exist and are skipped.
/// </summary>
internal sealed class SqsQueueProvisioningService(
    IOptions<SqsAsyncResponseOptions> options,
    ISqsClient client,
    ILogger<SqsQueueProvisioningService> logger) : IHostedService
{
    private const int MaxAttempts = 40;

    /// <summary>Creates the configured queues and their dead-letter pairs.</summary>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var o = options.Value;
        if (!o.CreateQueues)
            return;

        SqsOptionsValidator.ValidateCommon(o);

        foreach (var queue in new[] { o.WorkerQueue, o.ResponseQueue })
        {
            if (SqsQueueAddress.IsUrl(queue))
            {
                logger.LogInformation("SQS queue {Queue} is configured as a URL; skipping provisioning.", queue);
                continue;
            }

            await EnsureQueueWithDeadLetterAsync(o, queue, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>No-op.</summary>
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    internal async Task EnsureQueueWithDeadLetterAsync(
        SqsAsyncResponseOptions o,
        string queueName,
        CancellationToken cancellationToken)
    {
        var fifo = SqsQueueAddress.IsFifo(queueName);
        // A FIFO queue's dead-letter queue must itself be FIFO, and FIFO names must end in ".fifo".
        var deadLetterQueueName = SqsQueueAddress.DeriveDeadLetterQueueName(queueName, o.DeadLetterQueueSuffix);

        var fifoAttributes = fifo
            ? new Dictionary<string, string>(StringComparer.Ordinal) { [QueueAttributeName.FifoQueue] = "true" }
            : new Dictionary<string, string>(StringComparer.Ordinal);

        var deadLetterQueueUrl = await CreateOrUpdateQueueAsync(deadLetterQueueName, fifoAttributes, cancellationToken)
            .ConfigureAwait(false);
        var deadLetterQueueArn = await RetryAsync(
            () => client.GetQueueArnAsync(deadLetterQueueUrl, cancellationToken),
            cancellationToken).ConfigureAwait(false);

        var attributes = new Dictionary<string, string>(fifoAttributes, StringComparer.Ordinal)
        {
            [QueueAttributeName.RedrivePolicy] = AsyncResponseJson.Serialize(new Dictionary<string, string>
            {
                ["deadLetterTargetArn"] = deadLetterQueueArn,
                ["maxReceiveCount"] = o.MaxReceiveCount.ToString()
            })
        };

        await CreateOrUpdateQueueAsync(queueName, attributes, cancellationToken).ConfigureAwait(false);

        logger.LogInformation(
            "SQS queue {Queue} provisioned with dead-letter queue {DeadLetterQueue} (maxReceiveCount={MaxReceiveCount}).",
            queueName,
            deadLetterQueueName,
            o.MaxReceiveCount);
    }

    private async Task<string> CreateOrUpdateQueueAsync(
        string queueName,
        IReadOnlyDictionary<string, string> attributes,
        CancellationToken cancellationToken)
    {
        return await RetryAsync(CreateAsync, cancellationToken).ConfigureAwait(false);

        async Task<string> CreateAsync()
        {
            try
            {
                return await client.CreateQueueAsync(queueName, attributes, cancellationToken).ConfigureAwait(false);
            }
            catch (QueueNameExistsException)
            {
                // The queue exists with different attributes (for example a redrive policy set by an
                // earlier run against a different DLQ ARN): converge by re-applying the attributes.
                var queueUrl = await client.GetQueueUrlAsync(queueName, cancellationToken).ConfigureAwait(false);
                if (attributes.Count > 0)
                    await client.SetQueueAttributesAsync(queueUrl, attributes, cancellationToken).ConfigureAwait(false);
                return queueUrl;
            }
        }
    }

    private async Task<T> RetryAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken)
    {
        var o = options.Value;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                return await operation().ConfigureAwait(false);
            }
            catch (Exception ex) when (attempt < MaxAttempts && !cancellationToken.IsCancellationRequested)
            {
                // Startup ordering against a fresh endpoint (LocalStack still booting, transient
                // networking) resolves within a few retries; anything persistent still surfaces.
                var delay = AsyncResponseRetry.Backoff(attempt, o.SubscriberRetryBaseDelay, o.SubscriberRetryMaxDelay);
                logger.LogWarning(ex, "SQS queue provisioning attempt {Attempt} failed; retrying in {Delay}.", attempt, delay);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
