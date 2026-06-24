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
}

/// <summary>
/// Opt-in type-resolution hook (review item 8): callback/payload type names that are invisible to the
/// default AssemblyLoadContext (plugin scenarios) can be resolved via a registered resolver, and an
/// unresolvable type name is surfaced via a metric instead of failing silently.
/// </summary>
public class TypeResolutionTests
{
    public interface IPluginService { Task RunAsync(string value); }

    public sealed class PluginService : IPluginService
    {
        public List<string> Calls { get; } = [];
        public Task RunAsync(string value) { Calls.Add(value); return Task.CompletedTask; }
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
