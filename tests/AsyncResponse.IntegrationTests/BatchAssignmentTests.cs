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
public sealed class BatchAssignmentTests
{
    private static readonly Dictionary<string, Type> BatchFixtureByCollection = new(StringComparer.Ordinal)
    {
        [DataCollection.Name] = typeof(DataBatchFixture),
        [OracleCosmosCollection.Name] = typeof(OracleCosmosBatchFixture),
        [BrokersCollection.Name] = typeof(BrokersBatchFixture),
        [CloudCollection.Name] = typeof(CloudBatchFixture),
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
