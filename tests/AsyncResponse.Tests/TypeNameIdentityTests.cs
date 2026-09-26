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
    private static string OtherKey(string fullName) => WithKey(fullName, "0123456789abcdef");

    /// <summary>The same name with every generic argument's public key token replaced by <paramref name="token"/> (<c>null</c>: not strong-named).</summary>
    private static string WithKey(string fullName, string token) => Regex.Replace(fullName, "PublicKeyToken=[0-9a-f]+", "PublicKeyToken=" + token);

    [Fact]
    public void Normalize_DropsTheVersionCultureAndKeyOfGenericArguments_AtAnyDepth_AndKeepsTheSimpleName()
    {
        var name = typeof(Dictionary<string, List<int[]>>).FullName!;
        Assert.Contains("Version=", name, StringComparison.Ordinal);
        Assert.Contains("Culture=neutral", name, StringComparison.Ordinal);
        Assert.Contains("PublicKeyToken=", name, StringComparison.Ordinal);
        const string coreLib = "System.Private.CoreLib";

        var normalized = TypeNameIdentity.Normalize(name)!;

        Assert.Equal(
            $"System.Collections.Generic.Dictionary`2[[System.String, {coreLib}],[System.Collections.Generic.List`1[[System.Int32[], {coreLib}]], {coreLib}]]",
            normalized);
        Assert.Same(normalized, TypeNameIdentity.Normalize(normalized));
        Assert.True(TypeNameIdentity.Same(name, OtherVersion(name)));
    }

    [Fact]
    public void AnArgumentFromAReSignedOrNewlyStrongNamedAssembly_IsTheSameType_AsResolutionBindsIt()
    {
        // Fixpoint r2 (S3#1), reversing precommit review A5: the key was kept on the premise that a
        // same-named assembly signed with another key is an impostor type. Resolution binds an
        // assembly-qualified argument by SIMPLE name only (AssemblyName.ReferenceMatchesDefinition
        // compares nothing else), so both spellings run the same loaded type — keeping the key only
        // refused, after a deploy that strong-named or re-signed the argument's assembly, the runs
        // and callbacks the reflection path resolves.
        var name = typeof(List<int>).FullName!;
        Assert.NotEqual(name, OtherKey(name));

        Assert.True(TypeNameIdentity.Same(name, OtherKey(name)));
        Assert.True(TypeNameIdentity.Same(name, OtherKey(OtherVersion(name))));
        Assert.True(TypeNameIdentity.Same(name, WithKey(name, "null")));
        Assert.True(TypeNameIdentity.Same(name, name.Replace("Culture=neutral", "Culture=en-US", StringComparison.Ordinal)));
        Assert.Equal("System.Collections.Generic.List`1[[System.Int32, System.Private.CoreLib]]", TypeNameIdentity.Normalize(OtherKey(name)));

        // The simple name still counts: another assembly's type is another type.
        Assert.False(TypeNameIdentity.Same(name, name.Replace("System.Private.CoreLib", "Contoso.CoreLib", StringComparison.Ordinal)));
    }

    [Theory]
    [InlineData("PublicKeyToken=[0-9a-f]+", "PublicKeyToken=0123456789abcde")]
    [InlineData("PublicKeyToken=[0-9a-f]+", "PublicKeyToken=0123456789abcdef0")]
    [InlineData("PublicKeyToken=[0-9a-f]+", "PublicKeyToken=0123456789abcdeg")]
    [InlineData("PublicKeyToken=[0-9a-f]+", "PublicKeyToken=NULL")]
    [InlineData("PublicKeyToken=[0-9a-f]+", "PublicKeyToken=")]
    [InlineData("Culture=neutral", "Culture=")]
    [InlineData("Culture=neutral", "Culture=neu tral")]
    [InlineData("Culture=neutral", "Culture=-neutral")]
    [InlineData("Culture=neutral", "Culture=en--US")]
    [InlineData("Culture=neutral", "Culture=neutral[")]
    public void AKeyOrCultureValueThatIsMalformed_LeavesTheNameAsItIs(string pattern, string malformed)
    {
        // Same rule as a malformed Version: only what a runtime writes is dropped.
        var name = Regex.Replace(typeof(List<int>).FullName!, pattern, malformed);

        Assert.Same(name, TypeNameIdentity.Normalize(name));
        Assert.False(TypeNameIdentity.Same(typeof(List<int>).FullName, name));
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

    /// <summary>
    /// A run of <see cref="ListInputFlow"/> persisted by a build whose argument assemblies had
    /// another version (and, with <paramref name="token"/>, another public key token).
    /// </summary>
    private static FlowState PersistedByTheOtherBuild(string flowId, Type inputType, string? token = null) => new()
    {
        FlowId = flowId,
        FlowTypeName = typeof(ListInputFlow).FullName,
        InputTypeName = token is null ? OtherVersion(inputType.FullName!) : WithKey(OtherVersion(inputType.FullName!), token),
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
        await AssertTheRegisteredFlowRuns(PersistedByTheOtherBuild("generic-input", typeof(List<int>)));
    }

    [Theory]
    [InlineData("0123456789abcdef")]
    [InlineData("null")]
    public async Task ARegisteredFlowWithAGenericInput_StillExecutesARunPersistedUnderAnotherArgumentKey(string token)
    {
        // Fixpoint r2 (S3#1): the identity kept PublicKeyToken, so a run written before a deploy
        // that re-signed the argument's assembly (another token) or strong-named it (token "null"
        // in the ledger) failed "incompatible flow definition" on every redelivery and
        // dead-lettered — while resolution, which binds the argument by simple name, runs it.
        await AssertTheRegisteredFlowRuns(PersistedByTheOtherBuild($"generic-input-key-{token}", typeof(List<int>), token));
    }

    /// <summary>Executes <paramref name="persisted"/> through the statically-typed (registered) path and asserts it ran to success.</summary>
    private static async Task AssertTheRegisteredFlowRuns(FlowState persisted)
    {
        var store = new InMemoryFlowStateStore();
        await store.TryCreateAsync(persisted.FlowId!, persisted, TimeSpan.FromDays(1));
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

        await executor.ExecuteAsync(persisted.FlowId!);

        Assert.Equal(1, flow.Executions);
        Assert.Equal(FlowRunStatus.Succeeded, (await store.LoadAsync(persisted.FlowId!))!.Status);
    }

    [Fact]
    public async Task AnIdempotentRestart_OfARunPersistedUnderAnotherArgumentVersion_IsNotAConflict()
    {
        var store = new InMemoryFlowStateStore();
        await store.TryCreateAsync("generic-restart", PersistedByTheOtherBuild("generic-restart", typeof(List<int>)), TimeSpan.FromDays(1));
        await store.TryCreateAsync("generic-restart-key", PersistedByTheOtherBuild("generic-restart-key", typeof(List<int>), "null"), TimeSpan.FromDays(1));
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
        // Fixpoint r2 (S3#1): nor is a run written before the argument's assembly was strong-named.
        Assert.Equal("generic-restart-key", await starter.StartAsync<ListInputFlow, List<int>>([1, 2], "generic-restart-key"));

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

        // Fixpoint r2 (S3#1), reversing precommit review A5: an argument whose assembly was
        // re-signed resolves to the same type (simple-name binding), so it is the allowlisted type.
        Assert.True(authorizer.IsAllowed(OtherKey(typeof(IComparable<int>).FullName!), "CompareTo"));
        Assert.True(authorizer.IsAllowed(OtherKey(OtherVersion(typeof(IComparable<int>).FullName!)), "CompareTo"));
        Assert.False(authorizer.IsAllowed(OtherKey(typeof(IComparable<long>).FullName!), "CompareTo"));
    }

    [Fact]
    public void TheAllowlist_MatchesADescriptorOfThisBuildVerbatim_WithoutNormalizingItPerCheck()
    {
        // Fixpoint r2 (S3#8): only normalized names were stored, so every check of a generic
        // service name — twice per worker job — scanned it and built a normalized copy (a
        // StringBuilder and a string), although a descriptor persisted by the running build spells
        // the allowlisted name exactly. The configured spelling is now looked up first.
        var authorizer = new AsyncResponseCallbackAllowList().Allow<IComparable<int>>().Build();
        var name = typeof(IComparable<int>).FullName!;
        Assert.True(authorizer.IsAllowed(name, "CompareTo")); // warm-up (JIT, first-call paths)

        const int iterations = 100;
        var before = GC.GetAllocatedBytesForCurrentThread();
        var allowed = true;
        for (var i = 0; i < iterations; i++)
            allowed &= authorizer.IsAllowed(name, "CompareTo");
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(allowed);
        // Normalizing costs a StringBuilder plus a string of the name's length (~300 B) per check.
        Assert.True(allocated < iterations * 16, $"{allocated} bytes allocated over {iterations} exact-match checks.");
    }
}
