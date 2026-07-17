using System.Text.Json.Serialization.Metadata;

namespace AsyncResponse;

/// <summary>
/// Opt-in extensibility for resolving JSON contract metadata for the payload types AsyncResponse
/// serializes on the application's behalf: response payloads inside envelopes, durable-flow inputs,
/// step results and flow values, and literal callback arguments. By default these resolve through
/// the runtime's reflection-based resolver, so apps that serialize with reflection today need to
/// register nothing. Trimmed and Native AOT apps run without that resolver
/// (<c>JsonSerializer.IsReflectionEnabledByDefault</c> is <c>false</c>), so they must register a
/// source-generated <see cref="System.Text.Json.Serialization.JsonSerializerContext"/> covering
/// their payload types at startup:
/// <code>
/// AsyncResponseJsonSerialization.RegisterResolver(MyAppJsonContext.Default);
/// </code>
/// <para>
/// This is process-wide and additive, mirroring <see cref="AsyncResponseTypeResolution"/>:
/// registered resolvers are consulted for types the library's own wire-type metadata does not
/// cover, in registration order, before the reflection fallback. Register at startup, before the
/// host begins waiting on responses — metadata is cached per type on first use.
/// </para>
/// </summary>
public static class AsyncResponseJsonSerialization
{
    private static volatile IJsonTypeInfoResolver[] _resolvers = [];
    private static readonly object _gate = new();

    /// <summary>
    /// Registers a metadata resolver — typically a source-generated
    /// <see cref="System.Text.Json.Serialization.JsonSerializerContext"/> — consulted when
    /// serializing or deserializing application payload types.
    /// </summary>
    public static void RegisterResolver(IJsonTypeInfoResolver resolver)
    {
        ArgumentNullException.ThrowIfNull(resolver);
        lock (_gate)
        {
            _resolvers = [.. _resolvers, resolver];
        }
    }

    /// <summary>Snapshot of the registered resolvers, in registration order.</summary>
    internal static IJsonTypeInfoResolver[] Resolvers => _resolvers;

    /// <summary>Clears all registered resolvers. Test seam only.</summary>
    internal static void Reset()
    {
        lock (_gate)
        {
            _resolvers = [];
        }
    }
}
