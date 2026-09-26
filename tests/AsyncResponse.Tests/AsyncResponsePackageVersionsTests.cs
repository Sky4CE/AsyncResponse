using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Runtime.Loader;
using Xunit;
using static AsyncResponse.AsyncResponsePackageVersions;

namespace AsyncResponse.Tests;

/// <summary>
/// The runtime "all AsyncResponse packages are one version" check: which loaded assemblies it
/// compares, against which baseline, and that it runs before any provider hosted service — the
/// provider code bound to Core internals that a mixed install would otherwise fail inside first.
/// </summary>
public sealed class AsyncResponsePackageVersionsTests
{
    [Fact]
    public void AMismatchedPackage_FailsNamingBothAssemblies()
    {
        var ex = Assert.Throws<InvalidOperationException>(() => EnsureSingleVersion(
            "1.2.0",
            [new LoadedPackage("AsyncResponse.Core", "1.2.0"), new LoadedPackage("AsyncResponse.Transports.Kafka", "1.1.0")]));

        Assert.Contains("AsyncResponse.Transports.Kafka 1.1.0", ex.Message, StringComparison.Ordinal);
        Assert.Contains("AsyncResponse.Core 1.2.0", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OneVersionThroughout_Passes()
        => EnsureSingleVersion(
            "1.2.0-rc.2",
            [new LoadedPackage("AsyncResponse.Core", "1.2.0-rc.2"), new LoadedPackage("AsyncResponse.Channels.Redis", "1.2.0-rc.2")]);

    [Fact]
    public void TheBaselineIsTheExecutingCore_NotWhicheverCoreIsListedFirst()
    {
        // Regression: the baseline was the first AsyncResponse.Core found in the process-wide
        // assembly list, so with a second Core copy loaded the verdict depended on load order.
        var ex = Assert.Throws<InvalidOperationException>(() => EnsureSingleVersion(
            "1.2.0",
            [new LoadedPackage("AsyncResponse.Core", "1.1.0"), new LoadedPackage("AsyncResponse.Transports.Kafka", "1.2.0")]));

        Assert.Contains("AsyncResponse.Core 1.1.0 is loaded next to AsyncResponse.Core 1.2.0", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("1.2.0+abc123", "1.2.0")]
    [InlineData("1.2.0-rc.1+abc123", "1.2.0-rc.1")]
    [InlineData("1.2.0-rc.1", "1.2.0-rc.1")]
    [InlineData(null, "1.2.0.0")]
    [InlineData("", "1.2.0.0")]
    public void ThePackageVersion_IsTheInformationalVersionWithoutBuildMetadata(string? informational, string expected)
        // The metadata suffix is the source revision, which differs between assemblies of one
        // incremental local build; the assembly version cannot tell rc.1 from rc.2.
        => Assert.Equal(expected, PackageVersion(informational, new Version(1, 2, 0, 0)));

    [Fact]
    public void TheRunningProcess_IsOneVersion()
        => EnsureSingleVersion();

    [Fact]
    public void AContextWithItsOwnCoreCopy_IsSkipped_ItsPackagesNeverBindToThisCore()
    {
        // Regression: the scan spans every AssemblyLoadContext, so an isolated plugin that loaded
        // its own copy of the packages — Core included — was compared against the host's Core,
        // and a plugin carrying another AsyncResponse version failed the host start although it
        // never binds to the host's Core.
        var core = typeof(AsyncResponsePackageVersions).Assembly;
        var plugin = new AssemblyLoadContext("isolated-plugin", isCollectible: true);
        try
        {
            var pluginCore = plugin.LoadFromAssemblyPath(core.Location);
            Assert.NotSame(core, pluginCore);

            var packages = Loaded(core, AppDomain.CurrentDomain.GetAssemblies());

            Assert.Single(packages, package => package.Name == "AsyncResponse.Core");
            Assert.Contains(packages, package => package.Name == "AsyncResponse.Abstractions");

            // Fixpoint r2 (GS2#5): the plugin Core's own view. The context loaded only Core, so
            // its Abstractions reference falls back to the default context — the host's copy,
            // which the plugin Core runs on. From this side the default context "holds another
            // Core", and skipping all of it dropped that Abstractions too, so neither gate ever
            // compared the plugin Core with the Abstractions it binds to.
            var pluginView = Loaded(pluginCore, AppDomain.CurrentDomain.GetAssemblies());

            Assert.Single(pluginView, package => package.Name == "AsyncResponse.Core");
            Assert.Single(pluginView, package => package.Name == "AsyncResponse.Abstractions");
        }
        finally
        {
            plugin.Unload();
        }
    }

    [Fact]
    public void TheVersionGate_IsTheFirstHostedServiceTheHostConstructs()
    {
        // Regression: the check ran first thing in the validator's StartAsync — but the host
        // constructs EVERY hosted service (each provider subscriber, and the validator with the
        // provider-built markers it reads) before starting any, and those constructors are
        // provider code bound to Core internals: on a mixed install one of them failed first,
        // with a MissingMethodException instead of the named-version error. Hosted services are
        // constructed in registration order, and the gate is registered ahead of all of them.
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse().WithInMemoryChannel().WithInMemoryTransport().WithInMemoryDurableFlows();
        using var provider = services.BuildServiceProvider();

        var hosted = provider.GetServices<IHostedService>().ToArray();

        Assert.IsType<AsyncResponsePackageVersionGate>(hosted[0]);
        Assert.Single(hosted, service => service is AsyncResponsePackageVersionGate);
        Assert.Contains(hosted, service => service is AsyncResponseStartupValidator);
        Assert.Contains(hosted, service => service is InMemoryWorkerHost);
    }
}
