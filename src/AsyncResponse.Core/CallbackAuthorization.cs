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

    /// <summary>
    /// Whether the built-in <see cref="IDurableFlowExecutor"/> is allowed as a callback target.
    /// Defaults to <c>true</c>: durable flows persist its methods as their resume/recover/fail
    /// targets, and rejecting them would break flow recovery. Note the security trade-off — an
    /// attacker with write access to the recovery store or worker transport can then invoke
    /// flow-executor methods (bounded to flow ids, terminal failure, and checkpointing a payload
    /// into a flow's ledger; see docs/security.md). Hosts that do not use durable flows, or that
    /// accept re-registering the executor themselves, can set this to <c>false</c>.
    /// </summary>
    public bool AllowDurableFlowExecutor { get; set; } = true;

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
    {
        // Materialized at build time (not special-cased in IsAllowed) so the executor shows up in
        // the same allowlist mechanism as every other target and stays overridable by config.
        if (AllowDurableFlowExecutor && typeof(IDurableFlowExecutor).FullName is { } executorName)
            _allowedTypes.Add(executorName);

        return new AllowListAuthorizer(_allowedTypes, _predicates);
    }

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

    /// <summary>
    /// Registers a custom <see cref="IAsyncResponseCallbackAuthorizer"/>. Unlike the allowlist
    /// overload, nothing is allowed implicitly: when durable flows are enabled the authorizer must
    /// allow <see cref="IDurableFlowExecutor"/>, whose methods are persisted as every flow's
    /// resume/recover/fail targets — rejecting them breaks flow recovery.
    /// </summary>
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
