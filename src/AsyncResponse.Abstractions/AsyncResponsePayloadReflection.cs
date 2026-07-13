using System.Collections.Concurrent;

namespace AsyncResponse;

/// <summary>
/// Reflection helpers over <see cref="IAsyncResponsePayload"/> implementations. Durable channels
/// use this to fail fast when a recovery-enabled flow's payload has not overridden
/// <see cref="IAsyncResponsePayload.ShouldResumeOnRecovery"/> and would otherwise silently take the
/// conservative default (never resume).
/// </summary>
public static class AsyncResponsePayloadReflection
{
    private static readonly ConcurrentDictionary<Type, bool> OverrideCache = new();

    /// <summary>
    /// Returns <c>true</c> when <paramref name="payloadType"/> provides its own implementation of
    /// <see cref="IAsyncResponsePayload.ShouldResumeOnRecovery"/> rather than inheriting the
    /// interface's default. The result is cached per type.
    /// </summary>
    public static bool OverridesShouldResumeOnRecovery(Type payloadType)
    {
        ArgumentNullException.ThrowIfNull(payloadType);

        return OverrideCache.GetOrAdd(payloadType, DetectOverride);
    }

    private static bool DetectOverride(Type type)
    {
        if (type.IsInterface || !typeof(IAsyncResponsePayload).IsAssignableFrom(type))
            return false;

        var map = type.GetInterfaceMap(typeof(IAsyncResponsePayload));
        var interfaceMethod = typeof(IAsyncResponsePayload).GetMethod(nameof(IAsyncResponsePayload.ShouldResumeOnRecovery))!;
        var index = Array.IndexOf(map.InterfaceMethods, interfaceMethod);

        // When the type does not implement the method, the interface map points the target
        // back at the interface's own default implementation.
        return map.TargetMethods[index].DeclaringType != typeof(IAsyncResponsePayload);
    }
}
