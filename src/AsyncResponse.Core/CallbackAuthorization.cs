using AsyncResponse;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Builds an allowlist of callback targets for <see cref="IAsyncResponseCallbackAuthorizer"/>.
/// Configure it once, at the type level — no per-method attributes. A target is allowed when its
/// service type is allowed, or when any registered predicate accepts the <c>(type, method)</c> pair.
/// </summary>
public sealed class AsyncResponseCallbackAllowList
{
    private readonly HashSet<string> _allowedTypes = new(StringComparer.Ordinal);
    private readonly List<Func<string, string, bool>> _predicates = [];

    /// <summary>Allows every callback method on the given service type.</summary>
    public AsyncResponseCallbackAllowList Allow(Type serviceType)
    {
        ArgumentNullException.ThrowIfNull(serviceType);
        if (serviceType.FullName is { } name)
            _allowedTypes.Add(name);
        return this;
    }

    /// <summary>Allows every callback method on the given service type.</summary>
    public AsyncResponseCallbackAllowList Allow<TService>() => Allow(typeof(TService));

    /// <summary>Allows a callback by its persisted service full name (for types not referenceable at config time).</summary>
    public AsyncResponseCallbackAllowList Allow(string serviceInterfaceFullName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(serviceInterfaceFullName);
        _allowedTypes.Add(serviceInterfaceFullName);
        return this;
    }

    /// <summary>Allows callbacks matching a custom predicate over the <c>(serviceFullName, methodName)</c> pair.</summary>
    public AsyncResponseCallbackAllowList Allow(Func<string, string, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        _predicates.Add(predicate);
        return this;
    }

    internal IAsyncResponseCallbackAuthorizer Build()
        => new AllowListAuthorizer(_allowedTypes, _predicates);

    private sealed class AllowListAuthorizer(HashSet<string> allowedTypes, List<Func<string, string, bool>> predicates)
        : IAsyncResponseCallbackAuthorizer
    {
        /// <summary>Runs the IsAllowed operation.</summary>
        public bool IsAllowed(string serviceInterfaceFullName, string methodName)
        {
            if (allowedTypes.Contains(serviceInterfaceFullName))
                return true;

            foreach (var predicate in predicates)
            {
                if (predicate(serviceInterfaceFullName, methodName))
                    return true;
            }

            return false;
        }
    }
}

/// <summary>
/// Opt-in registration for callback authorization (review item 1). By default no authorizer is
/// registered and any DI-registered service method may be a callback target — calling these methods
/// is the only thing that turns on the allowlist.
/// </summary>
public static class AsyncResponseCallbackAuthorizationExtensions
{
    /// <summary>Registers an allowlist authorizer configured by <paramref name="configure"/>.</summary>
    public static AsyncResponseRegistrationBuilder AuthorizeCallbacks(
        this AsyncResponseRegistrationBuilder builder,
        Action<AsyncResponseCallbackAllowList> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var allowList = new AsyncResponseCallbackAllowList();
        configure(allowList);
        builder.Services.AddSingleton(allowList.Build());
        return builder;
    }

    /// <summary>Registers a custom <see cref="IAsyncResponseCallbackAuthorizer"/>.</summary>
    public static AsyncResponseRegistrationBuilder AuthorizeCallbacks(
        this AsyncResponseRegistrationBuilder builder,
        IAsyncResponseCallbackAuthorizer authorizer)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(authorizer);
        builder.Services.AddSingleton(authorizer);
        return builder;
    }
}
