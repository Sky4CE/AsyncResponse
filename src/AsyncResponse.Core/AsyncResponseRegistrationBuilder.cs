namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Fluent registration builder returned by <c>AddAsyncResponse()</c>. Chain a <em>channel</em>
/// (the response/recovery substrate) and, optionally, a worker <em>transport</em> (background-job
/// dispatch):
/// <code>
/// services.AddAsyncResponse()
///         .WithInMemoryChannel()      // or .WithRedisChannel(...)
///         .WithInMemoryTransport();   // or .WithGooglePubSubTransport(...)
/// </code>
/// Exactly one channel is required — <c>AddAsyncResponse()</c> alone registers no channel, and an
/// app that starts without one fails fast at host startup. Worker transports are optional.
/// </summary>
public sealed class AsyncResponseRegistrationBuilder
{
    internal AsyncResponseRegistrationBuilder(IServiceCollection services) => Services = services;

    /// <summary>
    /// The underlying service collection, exposed for backend packages' <c>With…</c> extensions
    /// and for advanced manual registrations.
    /// </summary>
    public IServiceCollection Services { get; }
}
