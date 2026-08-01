using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Opt-in callback authorization (review item 1): when an <see cref="IAsyncResponseCallbackAuthorizer"/>
/// is registered, only allowed (service, method) pairs may be invoked through the reflection
/// machinery. With none registered, behavior is unchanged (allow all) — zero boilerplate by default.
/// </summary>
public class CallbackAuthorizationTests
{
    public interface ICallbackTarget { Task RunAsync(string value); }

    public sealed class CallbackTarget : ICallbackTarget
    {
        public List<string> Calls { get; } = [];
        public Task RunAsync(string value) { Calls.Add(value); return Task.CompletedTask; }
    }

    private static (ServiceProvider Provider, CallbackTarget Target) BuildProvider(
        Action<AsyncResponseRegistrationBuilder>? configure = null)
    {
        var target = new CallbackTarget();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ICallbackTarget>(target);
        var builder = services.AddAsyncResponse();
        configure?.Invoke(builder);
        return (services.BuildServiceProvider(), target);
    }

    private static ReflectionInvocationDto TargetCall()
        => new()
        {
            ServiceInterfaceFullName = typeof(ICallbackTarget).FullName!,
            MethodName = nameof(ICallbackTarget.RunAsync),
            Params = ["hello"]
        };

    [Fact]
    public async Task NoAuthorizer_AllowsAnyCallback()
    {
        var (provider, target) = BuildProvider();
        await provider.InvokeAsync(TargetCall());
        Assert.Equal("hello", Assert.Single(target.Calls));
    }

    [Fact]
    public async Task UnauthorizedTarget_IsRejectedBeforeTypeResolution()
    {
        // The name is deliberately unresolvable: before the ordering fix this path failed with
        // "Type ... not found" — a full assembly scan spent on an attacker-supplied name. The
        // authorization error proves the string-based gate now runs before any resolution.
        var (provider, _) = BuildProvider(b => b.AuthorizeCallbacks(_ => { }));
        var dto = new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = $"Missing.Namespace.IUnresolvable{Guid.NewGuid():N}",
            MethodName = "RunAsync",
            Params = []
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => provider.InvokeAsync(dto));
        Assert.Contains("not authorized", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Allowlist_AllowsRegisteredType()
    {
        var (provider, target) = BuildProvider(b => b.AuthorizeCallbacks(a => a.Allow<ICallbackTarget>()));
        await provider.InvokeAsync(TargetCall());
        Assert.Equal("hello", Assert.Single(target.Calls));
    }

    [Fact]
    public async Task Allowlist_RejectsUnlistedType()
    {
        var (provider, target) = BuildProvider(b => b.AuthorizeCallbacks(a => a.Allow("Some.Other.Service")));
        await Assert.ThrowsAsync<InvalidOperationException>(() => provider.InvokeAsync(TargetCall()));
        Assert.Empty(target.Calls);
    }

    [Fact]
    public async Task Allowlist_PredicateCanAllow()
    {
        var (provider, target) = BuildProvider(b => b.AuthorizeCallbacks(a => a.Allow((_, method) => method == "RunAsync")));
        await provider.InvokeAsync(TargetCall());
        Assert.Equal("hello", Assert.Single(target.Calls));
    }

    [Fact]
    public async Task CustomAuthorizer_OverloadIsRegistered()
    {
        var (provider, target) = BuildProvider(b => b.AuthorizeCallbacks(new StaticAuthorizer(allowed: true)));

        await provider.InvokeAsync(TargetCall());

        Assert.Equal("hello", Assert.Single(target.Calls));
    }

    [Fact]
    public void AuthorizeCallbacks_ValidatesArguments()
    {
        var services = new ServiceCollection();
        var builder = services.AddAsyncResponse();

        Assert.Throws<ArgumentNullException>(() => ((AsyncResponseRegistrationBuilder)null!).AuthorizeCallbacks(_ => { }));
        Assert.Throws<ArgumentNullException>(() => builder.AuthorizeCallbacks((Action<AsyncResponseCallbackAllowList>)null!));
        Assert.Throws<ArgumentNullException>(() => ((AsyncResponseRegistrationBuilder)null!).AuthorizeCallbacks(new StaticAuthorizer(true)));
        Assert.Throws<ArgumentNullException>(() => builder.AuthorizeCallbacks((IAsyncResponseCallbackAuthorizer)null!));
    }

    [Fact]
    public void Allowlist_IncludesDurableFlowExecutorByDefault()
    {
        var services = new ServiceCollection();
        services.AddAsyncResponse().AuthorizeCallbacks(a => a.Allow("Some.Other.Service"));
        using var provider = services.BuildServiceProvider();

        var authorizer = provider.GetRequiredService<IAsyncResponseCallbackAuthorizer>();

        // Durable flows persist executor methods as their recovery targets, so the built allowlist
        // admits them by default — visibly, through the same mechanism as every other entry.
        Assert.True(authorizer.IsAllowed(typeof(IDurableFlowExecutor).FullName!, nameof(IDurableFlowExecutor.ExecuteAsync)));
        Assert.False(authorizer.IsAllowed("Unlisted.Service", "Run"));
    }

    [Fact]
    public void Allowlist_CanExcludeDurableFlowExecutor()
    {
        var services = new ServiceCollection();
        services.AddAsyncResponse().AuthorizeCallbacks(a =>
        {
            a.AllowDurableFlowExecutor = false;
            a.Allow("Some.Other.Service");
        });
        using var provider = services.BuildServiceProvider();

        var authorizer = provider.GetRequiredService<IAsyncResponseCallbackAuthorizer>();

        Assert.False(authorizer.IsAllowed(typeof(IDurableFlowExecutor).FullName!, nameof(IDurableFlowExecutor.RecoverAsync)));
    }

    [Fact]
    public async Task CustomAuthorizer_GatesDurableFlowExecutorTargets()
    {
        // The executor gets no exemption: RecoverAsync carries an attacker-choosable payload, so a
        // custom authorizer that rejects it must win even for the built-in flow executor.
        var (provider, _) = BuildProvider(b => b.AuthorizeCallbacks(new StaticAuthorizer(allowed: false)));

        var executorCall = new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = typeof(IDurableFlowExecutor).FullName!,
            MethodName = nameof(IDurableFlowExecutor.ExecuteAsync),
            Params = ["flow-1"]
        };

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => provider.InvokeAsync(executorCall));
        Assert.Contains("not authorized", ex.Message, StringComparison.Ordinal);
    }

    private sealed class StaticAuthorizer(bool allowed) : IAsyncResponseCallbackAuthorizer
    {
        public bool IsAllowed(string serviceInterfaceFullName, string methodName) => allowed;
    }
}

/// <summary>
/// Opt-in type-resolution hook (review item 8): callback/payload type names that are invisible to the
/// default AssemblyLoadContext (plugin scenarios) can be resolved via a registered resolver, and an
/// unresolvable type name is surfaced via a metric instead of failing silently.
/// </summary>
/// <remarks>
/// Collection-serialized with <c>AsyncResponseTypeResolutionTests</c>: both classes mutate the
/// process-global resolver registry, and that class's per-test <c>Reset()</c> wiped this class's
/// just-registered resolver when xunit ran them in parallel — an intermittent
/// "Type ... not found" failure here.
/// </remarks>
[Collection("AsyncResponseTypeResolutionRegistry")]
public class TypeResolutionTests
{
    public interface IPluginService { Task RunAsync(string value); }

    public sealed class PluginService : IPluginService
    {
        public List<string> Calls { get; } = [];
        public Task RunAsync(string value) { Calls.Add(value); return Task.CompletedTask; }
    }

    [Fact]
    public void ReflectionExtensions_KeepsBeforeFieldInit()
    {
        // Deliberate performance shape, enforced: the AssemblyLoad invalidation hook is
        // first-call-registered precisely so ReflectionExtensions never has an explicit static
        // constructor — one (or a field initializer that forces one) would forfeit
        // beforefieldinit and tax every static access on the hand-tuned ConvertTo/As<T> path
        // with an initialization check. If this fails, someone added a static ctor.
        Assert.True(typeof(ReflectionExtensions).Attributes.HasFlag(System.Reflection.TypeAttributes.BeforeFieldInit));
    }

    [Fact]
    public async Task CustomResolver_ResolvesNameInvisibleToDefaultContext()
    {
        const string aliasName = "Plugin.AsyncResponse.IAliasedFoo.Unique";
        AsyncResponseTypeResolution.RegisterResolver(name => name == aliasName ? typeof(IPluginService) : null);

        var svc = new PluginService();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IPluginService>(svc);
        await using var provider = services.BuildServiceProvider();

        await provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = aliasName,
            MethodName = nameof(IPluginService.RunAsync),
            Params = ["plugin"]
        });

        Assert.Equal("plugin", Assert.Single(svc.Calls));
    }

    [Fact]
    public async Task UnresolvableType_RecordsFailureMetric_AndThrows()
    {
        const string missingName = "Definitely.Missing.Type.Name.Unique4821";
        var captured = new List<Dictionary<string, object?>>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == AsyncResponseDiagnostics.MeterName)
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, _, tags, _) =>
        {
            if (instrument.Name != "asyncresponse.type_resolution.unresolved")
                return;
            var dict = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var tag in tags)
                dict[tag.Key] = tag.Value;
            lock (captured)
                captured.Add(dict);
        });
        listener.Start();

        await using var provider = new ServiceCollection().BuildServiceProvider();

        await Assert.ThrowsAsync<InvalidOperationException>(() => provider.InvokeAsync(new ReflectionInvocationDto
        {
            ServiceInterfaceFullName = missingName,
            MethodName = "X",
            Params = []
        }));

        lock (captured)
            Assert.Contains(captured, d => (string?)d["kind"] == "service");
    }
}
