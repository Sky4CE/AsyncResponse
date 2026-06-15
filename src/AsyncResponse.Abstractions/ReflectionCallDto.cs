namespace AsyncResponse;

/// <summary>
/// A serializable description of a method invocation: the target service interface, the method
/// name, and a set of parameters (literal values or runtime placeholders). Because it is plain
/// data, it can be persisted (recovery state in Redis) or transported (worker queues) and
/// resolved against the DI container later — including after a redeploy, by a different process.
/// <para>
/// <b>Contract warning:</b> the service interface full name and method name are persisted as
/// strings and resolved by reflection. Renaming a registered service method is a breaking change
/// for recovery state and in-flight worker jobs created before the rename.
/// </para>
/// </summary>
public sealed class ReflectionCallDto
{
    /// <summary>
    /// The full name of the service interface (including namespace) to resolve from the DI container.
    /// </summary>
    public required string ServiceInterfaceFullName { get; set; }

    /// <summary>The name of the method to invoke on the target service.</summary>
    public required string MethodName { get; set; }

    /// <summary>
    /// Parameters for the method invocation. Each <see cref="CallbackParam"/> may either hold a
    /// literal value or indicate a runtime placeholder to be replaced with the real payload,
    /// exception, or correlation id.
    /// </summary>
    public required CallbackParam[] Params { get; set; }
}

/// <summary>
/// A <see cref="ReflectionCallDto"/> resolved for execution; its <see cref="Params"/> are real
/// CLR objects with all placeholders substituted.
/// </summary>
public sealed class ReflectionInvocationDto
{
    public required string ServiceInterfaceFullName { get; init; }
    public required string MethodName { get; init; }
    public required object?[] Params { get; init; }
}

/// <summary>
/// Enumerates the runtime values that can be injected into a <see cref="CallbackParam"/>
/// when a lost-subscriber callback is invoked.
/// </summary>
public enum PlaceholderType
{
    /// <summary>Replaced by the response payload.</summary>
    Payload,

    /// <summary>Replaced by the exception instance (technical or domain failure).</summary>
    Exception,

    /// <summary>Replaced by the correlation id string.</summary>
    CorrelationId,
}

/// <summary>
/// Describes a single invocation parameter for a <see cref="ReflectionCallDto"/>:
/// either a literal <see cref="Value"/>, or a <see cref="Placeholder"/> token resolved at runtime.
/// </summary>
public sealed class CallbackParam
{
    /// <summary>
    /// If non-null, indicates which runtime value (payload, exception, or correlation id)
    /// should be injected instead of <see cref="Value"/>.
    /// </summary>
    public PlaceholderType? Placeholder { get; set; }

    /// <summary>The literal value to pass when <see cref="Placeholder"/> is <c>null</c>.</summary>
    public object? Value { get; set; }

    /// <summary>Creates a <see cref="CallbackParam"/> holding the given literal value.</summary>
    public static CallbackParam ForValue(object? value)
        => new() { Placeholder = null, Value = value };

    /// <summary>Creates a <see cref="CallbackParam"/> resolved at runtime to the given placeholder.</summary>
    public static CallbackParam ForPlaceholder(PlaceholderType placeholder)
        => new() { Placeholder = placeholder, Value = null };
}
