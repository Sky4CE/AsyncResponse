using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.PostgreSQL;

internal sealed class PostgreSqlReplyTargetProvider(
    IOptions<PostgreSqlAsyncResponseTransportOptions> _options) : IAsyncResponseReplyTargetProvider
{
    /// <inheritdoc />
    public AsyncResponseReplyTarget GetReplyTarget(string? name = null)
    {
        var options = _options.Value;
        PostgreSqlTransportOptionsValidator.ValidateCommon(options);
        var targetName = string.IsNullOrWhiteSpace(name)
            ? options.DefaultReplyTargetName
            : name;

        var target = ResolveTarget(options, targetName);
        var responseQueue = PostgreSqlTransportOptionsValidator.Required(
            target.ResponseQueue,
            $"{nameof(PostgreSqlReplyTargetOptions)}.{nameof(PostgreSqlReplyTargetOptions.ResponseQueue)}");

        // ValidateCommon enforces three-way distinctness for the transport-wide queues because all
        // logical queues share one table; a NAMED target reaches that same table by another route
        // and must honor the same rule — a target aimed at the worker (or dead-letter) queue lands
        // responses as rows the worker subscriber claims, NAKs to the cap, and dead-letters, while
        // the waiter times out. Matching the transport-wide ResponseQueue is fine: that is
        // literally the default target's destination.
        if (StringComparer.Ordinal.Equals(responseQueue, options.WorkerQueue)
            || StringComparer.Ordinal.Equals(responseQueue, options.DeadLetterQueue))
        {
            throw new InvalidOperationException(
                $"PostgreSQL async-response reply target '{targetName}' uses queue '{responseQueue}', which collides with " +
                $"{nameof(PostgreSqlAsyncResponseTransportOptions.WorkerQueue)} or {nameof(PostgreSqlAsyncResponseTransportOptions.DeadLetterQueue)}; " +
                "all queues share one table, so the target's responses would be consumed as worker jobs (or buried as dead letters).");
        }

        var properties = new Dictionary<string, string>(target.Properties, StringComparer.Ordinal)
        {
            ["schema"] = options.SchemaName,
            ["table"] = options.MessageTable,
            ["queue"] = responseQueue,
            ["notificationChannel"] = options.NotificationChannel,
            ["correlationIdHeader"] = options.CorrelationIdHeader
        };

        return new AsyncResponseReplyTarget
        {
            Name = targetName,
            Transport = PostgreSqlAsyncResponseTransportOptions.TransportName,
            Address = responseQueue,
            Properties = properties
        };
    }

    private static PostgreSqlReplyTargetOptions ResolveTarget(
        PostgreSqlAsyncResponseTransportOptions options,
        string targetName)
    {
        if (options.ReplyTargets.TryGetValue(targetName, out var configured))
            return configured;

        if (StringComparer.Ordinal.Equals(targetName, options.DefaultReplyTargetName))
            return new PostgreSqlReplyTargetOptions { ResponseQueue = options.ResponseQueue };

        throw new InvalidOperationException(
            $"PostgreSQL async-response reply target '{targetName}' is not configured. " +
            $"Configure {nameof(PostgreSqlAsyncResponseTransportOptions.ResponseQueue)} for the default target " +
            $"or add a named target with {nameof(PostgreSqlAsyncResponseTransportOptions.AddReplyTarget)}.");
    }
}
