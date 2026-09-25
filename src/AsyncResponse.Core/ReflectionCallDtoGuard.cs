namespace AsyncResponse;

/// <summary>
/// The one shape check for a <see cref="ReflectionCallDto"/>, applied wherever a descriptor
/// crosses a boundary: the producer (<c>EnqueueWorkerAsync(ReflectionCallDto)</c> and the
/// <c>OnLostSubscriber*(ReflectionCallDto)</c> registrations), the worker ingress's parse gate,
/// and the lost-subscriber dispatcher's callback resolution.
/// <para>
/// The descriptor's members are <c>required</c>, which enforces PRESENCE on the wire, not
/// non-null: <c>"Params": null</c>, a null entry, or a null or blank target name all parse, yet
/// none can ever resolve to a callback. Checked at the producer, the caller hears about it in its
/// own stack instead of the consumer silently acknowledging a job that never ran; checked at the
/// consumers, the shape takes their deterministic route (drop-and-acknowledge, or the permanent
/// callback fault) instead of escaping as an <see cref="ArgumentNullException"/> or
/// <see cref="NullReferenceException"/> the transport redelivers forever.
/// </para>
/// </summary>
internal static class ReflectionCallDtoGuard
{
    /// <summary>
    /// Describes the first defect of <paramref name="descriptor"/>, or returns <c>null</c> when it
    /// is well-formed. The description names the defective member, never its value — the
    /// descriptor is untrusted text on the consumer side and its arguments are business data.
    /// </summary>
    internal static string? FindDefect(ReflectionCallDto? descriptor)
    {
        if (descriptor is null)
            return "the call description is null";

        if (string.IsNullOrWhiteSpace(descriptor.ServiceInterfaceFullName))
            return "its service name is null or blank";

        if (string.IsNullOrWhiteSpace(descriptor.MethodName))
            return "its method name is null or blank";

        if (descriptor.Params is null)
            return "its parameter list is null";

        foreach (var parameter in descriptor.Params)
        {
            if (parameter is null)
                return "its parameter list has a null entry";
        }

        return null;
    }

    /// <summary>
    /// Producer-side form of <see cref="FindDefect"/>: throws <see cref="ArgumentException"/> for a
    /// malformed descriptor (and <see cref="ArgumentNullException"/> for a null one).
    /// </summary>
    internal static void ThrowIfMalformed(ReflectionCallDto descriptor, string paramName)
    {
        ArgumentNullException.ThrowIfNull(descriptor, paramName);

        if (FindDefect(descriptor) is { } defect)
            throw new ArgumentException($"The call description is malformed: {defect}.", paramName);
    }
}
