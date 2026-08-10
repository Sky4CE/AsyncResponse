using System.Reflection;
using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Batch membership is expressed twice per test class — a <c>[Collection]</c> attribute and the
/// fixture type its constructor asks for — and nothing in the compiler ties the two together. A
/// class whose attribute and fixture disagree, or that carries no collection at all, does not fail:
/// it silently runs in the wrong batch or in none, and the suite still reports green. These tests
/// are the tie.
/// </summary>
[Trait(Batches.Trait, Batches.None)]
public sealed class BatchAssignmentTests
{
    private static readonly Dictionary<string, Type> BatchFixtureByCollection = new(StringComparer.Ordinal)
    {
        [DataCollection.Name] = typeof(DataBatchFixture),
        [OracleCosmosCollection.Name] = typeof(OracleCosmosBatchFixture),
        [BrokersCollection.Name] = typeof(BrokersBatchFixture),
        [CloudCollection.Name] = typeof(CloudBatchFixture),
        [MatrixDatabaseLightCollection.Name] = typeof(MatrixDatabaseLightFixture),
        [MatrixBrokerLightCollection.Name] = typeof(MatrixBrokerLightFixture),
        [MatrixCloudLightCollection.Name] = typeof(MatrixCloudLightFixture),
        [MatrixDatabaseOracleCollection.Name] = typeof(MatrixDatabaseOracleFixture),
        [MatrixBrokerOracleCollection.Name] = typeof(MatrixBrokerOracleFixture),
        [MatrixCloudOracleCollection.Name] = typeof(MatrixCloudOracleFixture),
        [MatrixDatabaseCosmosCollection.Name] = typeof(MatrixDatabaseCosmosFixture),
        [MatrixBrokerCosmosCollection.Name] = typeof(MatrixBrokerCosmosFixture),
        [MatrixCloudCosmosCollection.Name] = typeof(MatrixCloudCosmosFixture),
    };

    private static IEnumerable<Type> TestClasses => typeof(BatchAssignmentTests).Assembly
        .GetTypes()
        .Where(type => type is { IsAbstract: false, IsPublic: true })
        .Where(type => type.GetMethods()
            .Any(method => method.GetCustomAttributes<FactAttribute>().Any()
                || method.GetCustomAttributes<TheoryAttribute>().Any()));

    [Fact]
    public void EveryClassThatNeedsAnAppHost_BelongsToAKnownBatch()
    {
        // Classes that need no AppHost at all (in-memory transport, the AOT publish gate) correctly
        // belong to no batch — they boot nothing, which is the cheapest outcome available. The drift
        // that matters is the reverse: asking for a fixture without saying which batch supplies it.
        var unassigned = TestClasses
            .Where(FixtureParameter)
            .Where(type => type.GetCustomAttribute<CollectionAttribute>() is not { } collection
                || !BatchFixtureByCollection.ContainsKey(collection.Name))
            .Select(type => type.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.True(
            unassigned.Length == 0,
            "A test class taking an IntegrationFixture must declare its batch via [Collection(...)], so "
            + "it runs against an AppHost that actually declares the resources it needs. Unassigned: "
            + string.Join(", ", unassigned));
    }

    private static bool FixtureParameter(Type type) => FixtureParameterType(type) is not null;

    private static Type? FixtureParameterType(Type type) => type.GetConstructors()
        .SelectMany(constructor => constructor.GetParameters())
        .Select(parameter => parameter.ParameterType)
        .FirstOrDefault(parameterType => typeof(IntegrationFixture).IsAssignableFrom(parameterType));

    /// <summary>
    /// CI runs one matrix leg per batch, selected with <c>--filter-trait "batch=&lt;name&gt;"</c>. A class
    /// with no batch trait is in no leg, so CI stops running it and stays green — the most expensive
    /// kind of silent gap. Every test class must carry the trait, including the ones needing no
    /// AppHost (they take <c>batch=none</c>).
    /// </summary>
    [Fact]
    public void EveryTestClass_CarriesABatchTrait()
    {
        var untagged = TestClasses
            .Where(type => BatchTrait(type) is null)
            .Select(type => type.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.True(
            untagged.Length == 0,
            $"Every test class needs [Trait(Batches.Trait, ...)] so CI's per-batch matrix runs it; use "
            + $"Batches.None for classes that need no AppHost. Untagged: {string.Join(", ", untagged)}");
    }

    [Fact]
    public void BatchTrait_AgreesWithTheCollection()
    {
        var expectedTraitByCollection = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [DataCollection.Name] = Batches.Data,
            [OracleCosmosCollection.Name] = Batches.OracleCosmos,
            [BrokersCollection.Name] = Batches.Brokers,
            [CloudCollection.Name] = Batches.Cloud,
            [MatrixDatabaseLightCollection.Name] = Batches.MatrixDatabaseLight,
            [MatrixBrokerLightCollection.Name] = Batches.MatrixBrokerLight,
            [MatrixCloudLightCollection.Name] = Batches.MatrixCloudLight,
            [MatrixDatabaseOracleCollection.Name] = Batches.MatrixDatabaseOracle,
            [MatrixBrokerOracleCollection.Name] = Batches.MatrixBrokerOracle,
            [MatrixCloudOracleCollection.Name] = Batches.MatrixCloudOracle,
            [MatrixDatabaseCosmosCollection.Name] = Batches.MatrixDatabaseCosmos,
            [MatrixBrokerCosmosCollection.Name] = Batches.MatrixBrokerCosmos,
            [MatrixCloudCosmosCollection.Name] = Batches.MatrixCloudCosmos,
        };

        var disagreements = new List<string>();
        foreach (var type in TestClasses)
        {
            var trait = BatchTrait(type);
            var collection = type.GetCustomAttribute<CollectionAttribute>()?.Name;

            if (collection is not null && expectedTraitByCollection.TryGetValue(collection, out var expected))
            {
                if (trait != expected)
                    disagreements.Add($"{type.Name} is in '{collection}' but is tagged batch={trait} (expected {expected})");
            }
            else if (trait != Batches.None)
            {
                disagreements.Add($"{type.Name} declares no batch collection but is tagged batch={trait} (expected {Batches.None})");
            }
        }

        Assert.True(
            disagreements.Count == 0,
            "A class's batch trait selects its CI matrix leg and its collection selects the AppHost that "
            + "leg boots; disagreeing means running against a fleet without the resources it needs. "
            + string.Join("; ", disagreements));
    }

    /// <summary>
    /// Read through <see cref="CustomAttributeData"/> rather than xUnit's trait API, which has moved
    /// between versions; the constructor arguments have not.
    /// </summary>
    private static string? BatchTrait(Type type) => type.GetCustomAttributesData()
        .Where(attribute => attribute.AttributeType.Name == nameof(TraitAttribute))
        .Where(attribute => attribute.ConstructorArguments.Count == 2)
        .Where(attribute => (string?)attribute.ConstructorArguments[0].Value == Batches.Trait)
        .Select(attribute => (string?)attribute.ConstructorArguments[1].Value)
        .FirstOrDefault();

    [Fact]
    public void EveryTestClass_TakesTheFixtureItsBatchProvides()
    {
        var mismatched = new List<string>();

        foreach (var type in TestClasses)
        {
            if (type.GetCustomAttribute<CollectionAttribute>() is not { } collection
                || !BatchFixtureByCollection.TryGetValue(collection.Name, out var expected))
            {
                continue; // covered by EveryTestClass_BelongsToAKnownBatch
            }

            // A class need not take a fixture at all, but if it does it must be its own batch's.
            var actual = FixtureParameterType(type);
            if (actual is not null && actual != expected)
                mismatched.Add($"{type.Name} is in '{collection.Name}' but takes {actual.Name} (expected {expected.Name})");
        }

        Assert.True(
            mismatched.Count == 0,
            "A test class must take the fixture its collection provides; xUnit cannot supply another "
            + "batch's fixture. " + string.Join("; ", mismatched));
    }
}
