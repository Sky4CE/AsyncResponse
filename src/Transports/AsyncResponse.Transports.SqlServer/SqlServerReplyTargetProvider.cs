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
        var responseQueue = SqlServerTransportOptionsValidator.Required(
            target.ResponseQueue,
            $"{nameof(SqlServerReplyTargetOptions)}.{nameof(SqlServerReplyTargetOptions.ResponseQueue)}");

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
