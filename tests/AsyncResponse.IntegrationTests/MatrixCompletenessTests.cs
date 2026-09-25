using AsyncResponse.Conformance;
using Microsoft.Extensions.DependencyInjection;
using System.Collections;
using System.Reflection;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// The matrix is only "every combination" if nothing can be added to the library without being added
/// to it. These guards are that tie: they compare the enumerated axes against the registration
/// methods the packages actually expose, and the shards against the cells.
/// <para>
/// A new channel, transport, or store package fails here the day it lands — before anyone can ship it
/// with no cross-product coverage — and the failure names the missing enum member.
/// </para>
/// </summary>
[Trait(Batches.Trait, Batches.None)]
public sealed class MatrixCompletenessTests
{
    /// <summary>
    /// Registration methods are the library's own list of what exists. Anything matching
    /// <c>With…Channel</c> on the registration builder must have a <see cref="MatrixChannel"/>.
    /// </summary>
    [Fact]
    public void EveryChannelRegistration_HasAMatrixAxisMember()
        => AssertAxisCoversRegistrations(
            suffix: "Channel",
            axis: ProviderMatrix.Channels.Select(channel => channel.ToString()),
            axisName: nameof(MatrixChannel));

    [Fact]
    public void EveryTransportRegistration_HasAMatrixAxisMember()
        => AssertAxisCoversRegistrations(
            suffix: "Transport",
            axis: ProviderMatrix.Transports.Select(transport => transport.ToString()),
            axisName: nameof(MatrixTransport));

    [Fact]
    public void EveryDurableFlowStoreRegistration_HasAMatrixAxisMember()
        => AssertAxisCoversRegistrations(
            suffix: "DurableFlows",
            axis: ProviderMatrix.Stores.Select(store => store.ToString()),
            axisName: nameof(MatrixStore));

    /// <summary>The product is complete and nothing is enumerated twice.</summary>
    [Fact]
    public void AllCells_IsTheCompleteCrossProduct()
    {
        var expected = ProviderMatrix.Channels.Count * ProviderMatrix.Transports.Count * ProviderMatrix.Stores.Count;
        Assert.Equal(expected, ProviderMatrix.AllCells.Count);
        Assert.Equal(expected, ProviderMatrix.AllCells.Distinct().Count());
    }

    /// <summary>
    /// Every cell lands in exactly one shard, and the shards partition the product — a cell in no
    /// shard is a combination CI never runs, which is the silent gap this whole file exists to stop.
    /// </summary>
    [Fact]
    public void Shards_PartitionEveryCellExactlyOnce()
    {
        var sharded = Enum.GetValues<MatrixShard>()
            .SelectMany(ProviderMatrix.CellsFor)
            .ToArray();

        Assert.Equal(ProviderMatrix.AllCells.Count, sharded.Length);
        Assert.Equal(ProviderMatrix.AllCells.Count, sharded.Distinct().Count());
        Assert.Empty(ProviderMatrix.AllCells.Except(sharded));
    }

    /// <summary>
    /// Every shard has a test class that runs it. Without this, deleting a class (or forgetting one
    /// for a new shard) removes a fifth of the product from CI while the suite still reports green.
    /// </summary>
    [Fact]
    public void EveryShard_HasATestClassCarryingItsBatchTrait()
    {
        var traitsInUse = BatchAssignmentTests.TestClasses
            .Select(BatchAssignmentTests.BatchTrait)
            .OfType<string>()
            .ToHashSet(StringComparer.Ordinal);

        var missing = Enum.GetValues<MatrixShard>()
            .Select(shard => $"matrix-{ProviderMatrix.TraitValueOf(shard)}")
            .Where(trait => !traitsInUse.Contains(trait))
            .ToArray();

        Assert.True(
            missing.Length == 0,
            "Every cross-product shard needs a test class tagged with its batch trait, or CI's matrix "
            + $"leg for it runs nothing. Missing: {string.Join(", ", missing)}");
    }

    /// <summary>
    /// A shard class picks its cells by hand (<c>MatrixCells.For(MatrixShard.…)</c>) and its CI leg by
    /// its trait, and nothing above ties the two: a class copied from another shard with only the
    /// trait and collection updated runs the OTHER shard's cells — that shard twice, its own never —
    /// while the shards still partition the product and every trait is still carried. This is the
    /// tie: every class tagged with a <c>matrix-*</c> trait that runs theories must feed each of them
    /// from a static <c>Cells</c> member holding exactly the cells of the shard that trait names. (The
    /// transport contract classes share the shards' fleets but run facts, not cells.)
    /// </summary>
    [Fact]
    public void EveryMatrixClass_RunsTheCellsOfTheShardItsTraitNames()
    {
        var shardByTrait = Enum.GetValues<MatrixShard>()
            .ToDictionary(shard => $"matrix-{ProviderMatrix.TraitValueOf(shard)}", StringComparer.Ordinal);

        var checkedClasses = 0;
        var mismatches = new List<string>();
        foreach (var type in BatchAssignmentTests.TestClasses)
        {
            if (BatchAssignmentTests.BatchTrait(type) is not { } trait
                || !shardByTrait.TryGetValue(trait, out var shard)
                || !type.GetMethods().Any(method => method.GetCustomAttributes<TheoryAttribute>().Any()))
            {
                continue;
            }

            checkedClasses++;
            if (type.GetProperty("Cells", BindingFlags.Public | BindingFlags.Static)?.GetValue(null) is not IEnumerable cells)
            {
                mismatches.Add($"{type.Name} (batch={trait}) has no public static Cells member");
                continue;
            }

            // The filter MatrixCells applies (ASYNCRESPONSE_MATRIX_FILTER) applies to both sides alike.
            var actual = CellsIn(cells);
            var expected = CellsIn(MatrixCells.For(shard));
            if (!actual.SetEquals(expected))
            {
                mismatches.Add(
                    $"{type.Name} (batch={trait}) runs {actual.Count} cell(s) that are not {shard}'s {expected.Count}: "
                    + $"{actual.Except(expected).Count()} foreign, {expected.Except(actual).Count()} of its own missing");
            }

            var foreignSources = type.GetMethods()
                .Where(method => method.GetCustomAttributes<TheoryAttribute>().Any())
                .Where(method => !method.GetCustomAttributesData().Any(attribute =>
                    attribute.AttributeType == typeof(MemberDataAttribute)
                    && attribute.ConstructorArguments is [{ Value: "Cells" }, ..]
                    && !attribute.NamedArguments.Any(named => named.MemberName == nameof(MemberDataAttribute.MemberType)
                        && named.TypedValue.Value is Type memberType && memberType != type)))
                .Select(method => method.Name);
            foreach (var method in foreignSources)
                mismatches.Add($"{type.Name}.{method} does not draw its cells from {type.Name}.Cells");
        }

        // A reflection guard that finds nothing passes for the wrong reason.
        Assert.True(
            checkedClasses >= shardByTrait.Count,
            $"Only {checkedClasses} matrix-* classes running cells were found for {shardByTrait.Count} shards; the scan, not the suite, is broken.");
        Assert.True(
            mismatches.Count == 0,
            "Every matrix shard class must run exactly the cells of the shard its batch trait selects, or CI "
            + "runs one shard twice and another never while every other guard stays green. "
            + string.Join("; ", mismatches));
    }

    private static HashSet<MatrixCell> CellsIn(IEnumerable theoryData)
    {
        var cells = new HashSet<MatrixCell>();
        foreach (var row in theoryData)
        {
            var data = row switch
            {
                ITheoryDataRow theoryRow => theoryRow.GetData(),
                object?[] values => values,
                _ => throw new InvalidOperationException($"Unrecognized theory data row type {row?.GetType().FullName ?? "null"}."),
            };
            cells.Add(Assert.IsType<MatrixCell>(Assert.Single(data)));
        }

        return cells;
    }

    /// <summary>
    /// The batch constants are spelled out (trait values must be compile-time constants) while the
    /// shard names are computed. This is the tie that keeps the two from drifting.
    /// </summary>
    [Fact]
    public void BatchConstants_MatchTheShardNames()
    {
        var constants = typeof(Batches)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(field => field.IsLiteral && field.Name.StartsWith("Matrix", StringComparison.Ordinal))
            .Select(field => (string)field.GetRawConstantValue()!)
            .ToHashSet(StringComparer.Ordinal);

        var expected = Enum.GetValues<MatrixShard>()
            .Select(shard => $"matrix-{ProviderMatrix.TraitValueOf(shard)}")
            .ToHashSet(StringComparer.Ordinal);

        Assert.Equal(expected, constants);
    }

    /// <summary>
    /// Reflects over the shipped package assemblies for public <c>With…{suffix}</c> extension methods
    /// on the registration builder and asserts the matrix axis names one per method. Matching on the
    /// method name is deliberate: it is the surface an application actually calls, so the guard tracks
    /// what is shippable rather than what happens to be in an enum.
    /// </summary>
    private static void AssertAxisCoversRegistrations(string suffix, IEnumerable<string> axis, string axisName)
    {
        var registrations = ProviderAssemblies()
            .SelectMany(assembly => assembly.GetTypes())
            .Where(type => type is { IsAbstract: true, IsSealed: true, IsPublic: true }) // static classes
            .SelectMany(type => type.GetMethods(BindingFlags.Public | BindingFlags.Static))
            .Where(method => method.GetParameters() is [{ ParameterType.Name: nameof(AsyncResponseRegistrationBuilder) }, ..])
            .Select(method => method.Name)
            .Where(name => name.StartsWith("With", StringComparison.Ordinal)
                && name.EndsWith(suffix, StringComparison.Ordinal))
            // WithDurableFlow<TFlow, TInput> registers a flow class, not a store; WithDurableFlows<T>
            // is the raw escape hatch for a custom IFlowStateStore. Neither names a shipped provider.
            .Where(name => name != $"With{suffix}" && name != $"With{suffix}s")
            .Select(name => name[4..^suffix.Length])
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        // A reflection guard that finds nothing passes for the wrong reason. Provider assemblies load
        // lazily, so touch one type per package first (see ForceProviderAssemblyLoad) and then insist
        // the scan actually found the shipped registrations.
        Assert.True(
            registrations.Count >= 5,
            $"Only {registrations.Count} With…{suffix} registrations were discovered, which means the "
            + "reflection scan — not the library — is what this guard is measuring.");

        // Every shipped registration must have an axis member. The reverse does not hold: InMemory
        // has no With…Channel/Transport suffix pattern of its own to match on in every case, and is
        // asserted by the containerless cells instead.
        var axisNames = axis.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var uncovered = registrations
            .Where(name => !axisNames.Contains(name) && !axisNames.Contains(Normalize(name)))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.True(
            uncovered.Length == 0,
            $"Every shipped With…{suffix} registration needs a {axisName} member, or its provider is "
            + $"absent from the cross product. Uncovered: {string.Join(", ", uncovered)}");
    }

    /// <summary>
    /// Package names and enum members differ in casing conventions (<c>WithSqsTransport</c> against
    /// <c>Sqs</c>, <c>WithNatsChannel</c> against <c>Nats</c>, <c>WithPostgreSql…</c> against
    /// <c>PostgreSql</c>), so the comparison is case-insensitive with the few spelled-out aliases here.
    /// </summary>
    private static string Normalize(string name) => name switch
    {
        "RabbitMq" => nameof(MatrixTransport.RabbitMq),
        "MongoDb" => nameof(MatrixChannel.MongoDb),
        "DynamoDb" => nameof(MatrixStore.DynamoDb),
        _ => name
    };

    /// <summary>
    /// The shipped provider assemblies, discovered through their registration builder rather than a
    /// hard-coded list — a new package is picked up as soon as the test project references it.
    /// </summary>
    private static IEnumerable<Assembly> ProviderAssemblies()
    {
        ForceProviderAssemblyLoad();
        return AppDomain.CurrentDomain.GetAssemblies()
            .Where(assembly => assembly.GetName().Name?.StartsWith("AsyncResponse.", StringComparison.Ordinal) == true)
            .Where(assembly => !assembly.GetName().Name!.Contains("Tests", StringComparison.Ordinal));
    }

    /// <summary>
    /// The runtime loads an assembly on first use, so a scan of <see cref="AppDomain.GetAssemblies"/>
    /// sees only what earlier code happened to touch. Loading every AsyncResponse assembly from the
    /// build output forces them all in — a hand-maintained anchor list rotted silently instead: a
    /// newly referenced provider package missing from the list stayed unloaded, invisible to every
    /// completeness guard, and the guards stayed green.
    /// </summary>
    private static void ForceProviderAssemblyLoad()
    {
        var loaded = 0;
        foreach (var path in Directory.EnumerateFiles(AppContext.BaseDirectory, "AsyncResponse.*.dll"))
        {
            var name = Path.GetFileNameWithoutExtension(path);
            if (name.Contains("Tests", StringComparison.Ordinal) || name.Contains("Sample", StringComparison.Ordinal))
                continue;

            Assembly.Load(new AssemblyName(name));
            loaded++;
        }

        // A floor pins the mechanism itself: fewer than the currently shipped assembly count means
        // the scan broke (wrong directory, renamed outputs), not that providers went away.
        Assert.True(loaded >= 24, $"expected at least 24 AsyncResponse assemblies in the test output, found {loaded}.");
    }
}
