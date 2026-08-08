using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace AsyncResponse;

/// <summary>
/// The library's single JSON entry point: every internal serialization site goes through these
/// options and helpers instead of the reflection-based <see cref="JsonSerializer"/> overloads, so
/// the packages carry no trim/AOT warnings (IL2026/IL3050).
/// <para>
/// Metadata resolution order: library wire types (<see cref="AsyncResponseJsonContext"/>, source
/// generated) → user-registered resolvers (<see cref="AsyncResponseJsonSerialization"/>) → the
/// runtime reflection resolver when the app has it enabled
/// (<see cref="JsonSerializer.IsReflectionEnabledByDefault"/>, true for every non-trimmed app).
/// Behavior for existing apps is therefore unchanged; trimmed/AOT apps must register their payload
/// types and otherwise get an actionable error naming the type.
/// </para>
/// </summary>
internal static class AsyncResponseJson
{
    private static readonly IJsonTypeInfoResolver? _reflectionResolver = CreateReflectionResolverIfEnabled();

    /// <summary>The full resolver chain, for options that need to prepend their own metadata.</summary>
    public static IJsonTypeInfoResolver Resolver { get; } = new ChainResolver();

    /// <summary>Serializer-default settings (case-sensitive, write nulls) over the resolver chain.</summary>
    public static JsonSerializerOptions Default { get; } = new() { TypeInfoResolver = Resolver };

    /// <summary>
    /// Case-insensitive property matching, for broker-ingress reads — the historical behavior of
    /// the library's defensive deserialization paths.
    /// </summary>
    public static JsonSerializerOptions CaseInsensitive { get; } = new()
    {
        TypeInfoResolver = Resolver,
        PropertyNameCaseInsensitive = true
    };

    /// <summary>Omits null properties on write; used for the durable-flow ledger.</summary>
    public static JsonSerializerOptions IgnoreNullWrites { get; } = new()
    {
        TypeInfoResolver = Resolver,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>Serializes with default settings, resolving metadata through the chain.</summary>
    public static string Serialize<T>(T value)
        => JsonSerializer.Serialize(value, GetTypeInfo<T>(Default));

    /// <summary>
    /// Serializes by the value's runtime type — the counterpart of the reflection-based
    /// <c>JsonSerializer.Serialize(value, value.GetType())</c> pattern.
    /// </summary>
    public static string Serialize(object value, Type runtimeType)
        => JsonSerializer.Serialize(value, GetTypeInfo(runtimeType, Default));

    /// <summary>
    /// Deserializes with default settings (case-sensitive property matching, like the bare
    /// <c>JsonSerializer.Deserialize&lt;T&gt;(json)</c> these callsites used before).
    /// </summary>
    public static T? Deserialize<T>(string json)
        => JsonSerializer.Deserialize(json, GetTypeInfo<T>(Default));

    /// <summary>Resolves typed metadata for <typeparamref name="T"/> from <paramref name="options"/>.</summary>
    public static JsonTypeInfo<T> GetTypeInfo<T>(JsonSerializerOptions options)
        => (JsonTypeInfo<T>)GetTypeInfo(typeof(T), options);

    /// <summary>
    /// Resolves metadata for <paramref name="type"/> from <paramref name="options"/>, translating
    /// the serializer's "no metadata" failure into guidance to register a context.
    /// </summary>
    public static JsonTypeInfo GetTypeInfo(Type type, JsonSerializerOptions options)
    {
        try
        {
            // Collectible-context (plugin) types are deliberately NOT special-cased here: the
            // runtime's serializer pins a collectible AssemblyLoadContext through process-wide
            // static caches (member accessors) no matter which JsonSerializerOptions instance is
            // used — verified empirically against .NET 10 with a fresh options per call — so a
            // per-call options copy would add cost without restoring unloadability. The library's
            // own type caches skip collectible types (see UnresolvableTypeNames call sites); the
            // supported plugin pattern keeps payload/service CONTRACT types in a non-collectible
            // contracts assembly, under which nothing here ever sees a collectible type. See
            // AsyncResponseTypeResolution docs.
            return options.GetTypeInfo(type);
        }
        catch (NotSupportedException ex)
        {
            throw new NotSupportedException(
                $"No JSON metadata is available for '{type}'. This app runs without reflection-based " +
                "System.Text.Json (trimmed/Native AOT), so payload types must be registered at startup: " +
                $"declare [JsonSerializable(typeof({type.Name}))] on a JsonSerializerContext and call " +
                $"{nameof(AsyncResponseJsonSerialization)}.{nameof(AsyncResponseJsonSerialization.RegisterResolver)}(YourContext.Default).",
                ex);
        }
    }

    private static IJsonTypeInfoResolver? CreateReflectionResolverIfEnabled()
    {
        if (!JsonSerializer.IsReflectionEnabledByDefault)
            return null;

        return CreateReflectionResolver();

        [UnconditionalSuppressMessage("Trimming", "IL2026",
            Justification = "Reachable only when JsonSerializer.IsReflectionEnabledByDefault is true; trimmed and AOT builds substitute that feature switch to false and this branch (and the reflection resolver) is removed.")]
        [UnconditionalSuppressMessage("AOT", "IL3050",
            Justification = "Same guard: the feature switch is false under Native AOT, so the reflection resolver is never constructed there.")]
        static IJsonTypeInfoResolver CreateReflectionResolver() => new DefaultJsonTypeInfoResolver();
    }

    /// <summary>
    /// Library wire types first (their contract is fixed and must not be overridden), then
    /// user-registered resolvers, then the reflection fallback when available. Consulting the
    /// live registration snapshot per lookup lets startup-time registration order be forgiving;
    /// results are cached per options instance by the serializer itself.
    /// </summary>
    private sealed class ChainResolver : IJsonTypeInfoResolver
    {
        public JsonTypeInfo? GetTypeInfo(Type type, JsonSerializerOptions options)
        {
            var info = ((IJsonTypeInfoResolver)AsyncResponseJsonContext.Default).GetTypeInfo(type, options);
            if (info is not null)
                return info;

            foreach (var resolver in AsyncResponseJsonSerialization.Resolvers)
            {
                info = resolver.GetTypeInfo(type, options);
                if (info is not null)
                    return info;
            }

            return _reflectionResolver?.GetTypeInfo(type, options);
        }
    }
}
