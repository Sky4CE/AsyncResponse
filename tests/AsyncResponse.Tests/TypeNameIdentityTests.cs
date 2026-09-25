using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Persisted type names are <c>typeof(T).FullName</c>, which embeds every generic argument's
/// assembly version; they are compared by type identity (<see cref="TypeNameIdentity"/>) the way
/// resolution matches them, never rewritten.
/// </summary>
public sealed class TypeNameIdentityTests
{
    /// <summary>The same name as persisted by a build whose argument assemblies had another version.</summary>
    internal static string OtherVersion(string fullName) => Regex.Replace(fullName, "Version=[0-9.]+", "Version=9.9.9.9");

    /// <summary>The same name with every generic argument's assembly signed by another key.</summary>
    private static string OtherKey(string fullName) => Regex.Replace(fullName, "PublicKeyToken=[0-9a-f]+", "PublicKeyToken=0123456789abcdef");

    [Fact]
    public void Normalize_DropsOnlyTheVersionOfGenericArguments_AtAnyDepth_AndKeepsTheRestOfTheAssemblyName()
    {
        var name = typeof(Dictionary<string, List<int[]>>).FullName!;
        Assert.Contains("Version=", name, StringComparison.Ordinal);
        var coreLib = $"System.Private.CoreLib, Culture=neutral, PublicKeyToken={Convert.ToHexString(typeof(object).Assembly.GetName().GetPublicKeyToken()!).ToLowerInvariant()}";

        var normalized = TypeNameIdentity.Normalize(name)!;

        Assert.DoesNotContain("Version=", normalized, StringComparison.Ordinal);
        Assert.Equal(
            $"System.Collections.Generic.Dictionary`2[[System.String, {coreLib}],[System.Collections.Generic.List`1[[System.Int32[], {coreLib}]], {coreLib}]]",
            normalized);
        Assert.True(TypeNameIdentity.Same(name, OtherVersion(name)));
    }

    [Fact]
    public void AnArgumentFromASameNamedAssemblySignedWithAnotherKey_IsADifferentType()
    {
        // Precommit review (A5): the key was stripped along with the version, so a same-named
        // assembly signed with another key — a plugin load context's impostor — normalized to the
        // same identity. A version bump never changes a key; only the version is dropped.
        var name = typeof(List<int>).FullName!;
        Assert.NotEqual(name, OtherKey(name));

        Assert.False(TypeNameIdentity.Same(name, OtherKey(name)));
        Assert.False(TypeNameIdentity.Same(name, OtherKey(OtherVersion(name))));
        Assert.Contains("PublicKeyToken=0123456789abcdef", TypeNameIdentity.Normalize(OtherKey(name)), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Version=1.0.0.0x")]
    [InlineData("Version=abc")]
    [InlineData("Version=1")]
    [InlineData("Version=1.2.3.4.5")]
    [InlineData("Version=1..2")]
    [InlineData("Version=1.2.")]
    [InlineData("Version=123456.0.0.0")]
    [InlineData("Version=")]
    public void AVersionValueThatIsNotDottedDecimal_LeavesTheNameAsItIs(string malformed)
    {
        // Store data a runtime would never write is not rewritten into a name that matches.
        var name = Regex.Replace(typeof(List<int>).FullName!, "Version=[0-9.]+", malformed);

        Assert.Same(name, TypeNameIdentity.Normalize(name));
        Assert.False(TypeNameIdentity.Same(typeof(List<int>).FullName, name));
    }

    [Fact]
    public void DifferentArgumentTypes_AreStillDifferent_AndNonGenericNamesAreUntouched()
    {
        Assert.False(TypeNameIdentity.Same(typeof(List<int>).FullName, OtherVersion(typeof(List<long>).FullName!)));
        Assert.Same(typeof(TypeNameIdentityTests).FullName, TypeNameIdentity.Normalize(typeof(TypeNameIdentityTests).FullName));
        Assert.Null(TypeNameIdentity.Normalize(null));

        // Store data outside the persisted type-name limits is compared verbatim.
        var huge = "Ns.T`1[[" + new string('x', 5000) + ", A, Version=1.0.0.0]]";
        Assert.Same(huge, TypeNameIdentity.Normalize(huge));
    }

    public sealed class ListInputFlow : IDurableFlow<List<int>>
    {
        private int _executions;

        public int Executions => Volatile.Read(ref _executions);

        public Task ExecuteAsync(IDurableFlowContext flow, List<int> input)
        {
            Interlocked.Increment(ref _executions);
            return Task.CompletedTask;
        }
    }

    /// <summary>A run of <see cref="ListInputFlow"/> persisted by a build whose argument assemblies had another version.</summary>
    private static FlowState PersistedByTheOtherBuild(string flowId, Type inputType) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(ListInputFlow).FullName,
        InputTypeName = OtherVersion(inputType.FullName!),
        InputJson = "[1,2]",
        Status = FlowRunStatus.Running,
        CreatedAtUtc = DateTime.UtcNow,
        UpdatedAtUtc = DateTime.UtcNow
    };

    private static DurableFlowRegistration ListInputRegistration() => new()
    {
        FlowTypeFullName = typeof(ListInputFlow).FullName!,
        InputTypeFullName = typeof(List<int>).FullName!,
        FlowType = typeof(ListInputFlow),
        DeserializeInput = static json => JsonSafety.SafeDeserialize<List<int>>(json),
        ExecuteAsync = static (flow, context, input) => ((ListInputFlow)flow).ExecuteAsync(context, (List<int>)input!)
    };

    [Fact]
    public async Task ARegisteredFlowWithAGenericInput_StillExecutesARunPersistedUnderAnotherArgumentVersion()
    {
        // Fixpoint r1 (GS1#2): the registered path compared InputTypeName ordinally, so after a
        // deploy that moved System.Private.CoreLib (a .NET upgrade) or an application assembly,
        // every in-flight run of a flow with a generic input failed "incompatible flow definition"
        // on each redelivery and dead-lettered — while the reflection path, which resolves by
        // simple assembly name, would have run it.
        var store = new InMemoryFlowStateStore();
        await store.TryCreateAsync("generic-input", PersistedByTheOtherBuild("generic-input", typeof(List<int>)), TimeSpan.FromDays(1));
        var flow = new ListInputFlow();
        var services = new ServiceCollection();
        services.AddSingleton<IFlowStateStore>(store);
        services.AddSingleton(flow);
        await using var provider = services.BuildServiceProvider();
        var executor = new DurableFlowExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Moq.Mock.Of<IAsyncResponseBuilder>(),
            Moq.Mock.Of<IAsyncResponseSubscriber>(),
            recoverableSubscriber: null,
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DurableFlowExecutor>.Instance,
            registrations: [ListInputRegistration()]);

        await executor.ExecuteAsync("generic-input");

        Assert.Equal(1, flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync("generic-input"))!.Status);
    }

    [Fact]
    public async Task AnIdempotentRestart_OfARunPersistedUnderAnotherArgumentVersion_IsNotAConflict()
    {
        var store = new InMemoryFlowStateStore();
        await store.TryCreateAsync("generic-restart", PersistedByTheOtherBuild("generic-restart", typeof(List<int>)), TimeSpan.FromDays(1));
        await store.TryCreateAsync("generic-other-type", PersistedByTheOtherBuild("generic-other-type", typeof(List<long>)), TimeSpan.FromDays(1));
        var services = new ServiceCollection();
        services.AddSingleton<IFlowStateStore>(store);
        await using var provider = services.BuildServiceProvider();
        var starter = new DurableFlowService(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Moq.Mock.Of<IAsyncResponseBuilder>(),
            new AsyncResponseContextPropagation([]),
            new DurableFlowOptions(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DurableFlowService>.Instance);

        Assert.Equal("generic-restart", await starter.StartAsync<ListInputFlow, List<int>>([1, 2], "generic-restart"));

        // A different argument type is still different work.
        await Assert.ThrowsAsync<DurableFlowIdConflictException>(
            () => starter.StartAsync<ListInputFlow, List<int>>([1, 2], "generic-other-type"));
    }

    [Fact]
    public void AnAllowlistedClosedGeneric_StillAuthorizesADescriptorPersistedUnderAnotherArgumentVersion()
    {
        // Fixpoint r1 (S3#13): the allowlist matched the ordinal FullName, so a recovery row
        // persisted before a deploy that bumped the argument's assembly (or the runtime) was refused.
        var authorizer = new AsyncResponseCallbackAllowList().Allow<IComparable<int>>().Build();

        Assert.True(authorizer.IsAllowed(typeof(IComparable<int>).FullName!, "CompareTo"));
        Assert.True(authorizer.IsAllowed(OtherVersion(typeof(IComparable<int>).FullName!), "CompareTo"));
        Assert.False(authorizer.IsAllowed(OtherVersion(typeof(IComparable<long>).FullName!), "CompareTo"));

        // Precommit review (A5): the version only — an argument from a same-named assembly signed
        // with another key is not the allowlisted type.
        Assert.False(authorizer.IsAllowed(OtherKey(typeof(IComparable<int>).FullName!), "CompareTo"));
        Assert.False(authorizer.IsAllowed(OtherKey(OtherVersion(typeof(IComparable<int>).FullName!)), "CompareTo"));
    }
}
