using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace AsyncResponse;

/// <summary>
/// Reflection helpers over <see cref="IAsyncResponsePayload"/> implementations. Durable channels
/// use this to fail fast when a recovery-enabled flow's payload has not overridden
/// <see cref="IAsyncResponsePayload.OnRecovery"/> and would otherwise silently take the
/// conservative default (never resume).
/// </summary>
public static class AsyncResponsePayloadReflection
{
    private static readonly ConcurrentDictionary<Type, bool> OverrideCache = new();

    /// <summary>
    /// Returns <c>true</c> when <paramref name="payloadType"/> provides its own implementation of
    /// <see cref="IAsyncResponsePayload.OnRecovery"/> rather than inheriting the interface's
    /// default. The result is cached per type.
    /// </summary>
    public static bool OverridesOnRecovery(Type payloadType)
    {
        ArgumentNullException.ThrowIfNull(payloadType);

        return OverrideCache.GetOrAdd(payloadType, DetectOverride);
    }

    [UnconditionalSuppressMessage("Trimming", "IL2070",
        Justification = "Best-effort diagnostic: payload types reaching this check are statically referenced by the waiter " +
                        "registration that triggers it (typeof(T)), so their methods are preserved; when the runtime still " +
                        "cannot answer, the check fails open rather than failing a correct app.")]
    private static bool DetectOverride(Type type)
    {
        if (type.IsInterface || !typeof(IAsyncResponsePayload).IsAssignableFrom(type))
            return false;

        // Implicit implementations (the overwhelmingly common shape) declare a public
        // OnRecovery on the class itself — detectable without the interface map.
        if (type.GetMethod(nameof(IAsyncResponsePayload.OnRecovery), BindingFlags.Instance | BindingFlags.Public, Type.EmptyTypes) is not null)
            return true;

        try
        {
            var map = type.GetInterfaceMap(typeof(IAsyncResponsePayload));
            var interfaceMethod = typeof(IAsyncResponsePayload).GetMethod(nameof(IAsyncResponsePayload.OnRecovery))!;
            var index = Array.IndexOf(map.InterfaceMethods, interfaceMethod);

            // When the type does not implement the method, the interface map points the target
            // back at the interface's own default implementation.
            return map.TargetMethods[index].DeclaringType != typeof(IAsyncResponsePayload);
        }
        catch (NotSupportedException)
        {
            // Native AOT cannot compute the interface map for interfaces with default
            // implementations. This check exists to fail fast on a payload that silently
            // inherits the conservative default — when the runtime cannot answer, assume the
            // payload is fine instead of breaking a correct app (fail open).
            return true;
        }
    }
}
