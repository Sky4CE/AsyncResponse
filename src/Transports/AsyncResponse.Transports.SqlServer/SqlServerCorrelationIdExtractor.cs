namespace AsyncResponse.Transports.SqlServer;

/// <summary>
/// Extracts the AsyncResponse correlation id from SQL Server row metadata first, then from the
/// JSON response body via configured paths. The provider-named entry point for the shared
/// <see cref="DbCorrelationIdExtractor"/> — the same type name compiles into each database
/// transport assembly, so cross-assembly callers (tests via InternalsVisibleTo) need this
/// unambiguous name.
/// </summary>
internal static class SqlServerCorrelationIdExtractor
{
    public static string? Extract(
        IReadOnlyDictionary<string, string>? headers,
        string messageJson,
        SqlServerAsyncResponseTransportOptions options)
        => DbCorrelationIdExtractor.Extract(headers, messageJson, options);
}
