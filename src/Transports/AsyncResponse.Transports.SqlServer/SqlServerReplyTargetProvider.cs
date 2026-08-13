using Microsoft.Extensions.Options;

namespace AsyncResponse.Transports.SqlServer;

internal sealed class SqlServerReplyTargetProvider(
    IOptions<SqlServerAsyncResponseTransportOptions> _options) : IAsyncResponseReplyTargetProvider
{
    /// <inheritdoc />
    public AsyncResponseReplyTarget GetReplyTarget(string? name = null)
    {
        var options = _options.Value;
        SqlServerTransportOptionsValidator.ValidateCommon(options);
        var targetName = string.IsNullOrWhiteSpace(name)
            ? options.DefaultReplyTargetName
            : name;

        var target = ResolveTarget(options, targetName);
        var queueOptionName = StringComparer.Ordinal.Equals(targetName, options.DefaultReplyTargetName)
            ? $"{nameof(SqlServerReplyTargetOptions)}.{nameof(SqlServerReplyTargetOptions.ResponseQueue)}"
            : $"{nameof(SqlServerAsyncResponseTransportOptions.ReplyTargets)}[\"{targetName}\"]." +
              $"{nameof(SqlServerReplyTargetOptions.ResponseQueue)}";
        var responseQueue = SqlServerTransportOptionsValidator.Required(target.ResponseQueue, queueOptionName);

        // ValidateCommon covers the three transport-wide queues, but a NAMED reply target's queue
        // reaches the same nvarchar(200) column by a different route: it is emitted as the reply
        // address and as the "queue" property a remote publisher inserts with. Unvalidated, an
        // over-long name fails the insert outright and a space-padded one lands a row that the
        // exact-matching claim predicate will never hand to any subscriber. Validated here rather
        // than in ValidateCommon so a target added by mutating the dictionary directly is covered.
        SqlServerTransportOptionsValidator.ValidateQueueName(responseQueue, queueOptionName);

        var properties = new Dictionary<string, string>(target.Properties, StringComparer.Ordinal)
        {
            ["schema"] = options.SchemaName,
            ["table"] = options.MessageTable,
            ["queue"] = responseQueue,
            ["correlationIdHeader"] = options.CorrelationIdHeader
        };

        return new AsyncResponseReplyTarget
        {
            Name = targetName,
            Transport = SqlServerAsyncResponseTransportOptions.TransportName,
            Address = responseQueue,
            Properties = properties
        };
    }

    private static SqlServerReplyTargetOptions ResolveTarget(
        SqlServerAsyncResponseTransportOptions options,
        string targetName)
    {
        if (options.ReplyTargets.TryGetValue(targetName, out var configured))
            return configured;

        if (StringComparer.Ordinal.Equals(targetName, options.DefaultReplyTargetName))
            return new SqlServerReplyTargetOptions { ResponseQueue = options.ResponseQueue };

        throw new InvalidOperationException(
            $"SQL Server async-response reply target '{targetName}' is not configured. " +
            $"Configure {nameof(SqlServerAsyncResponseTransportOptions.ResponseQueue)} for the default target " +
            $"or add a named target with {nameof(SqlServerAsyncResponseTransportOptions.AddReplyTarget)}.");
    }
}
