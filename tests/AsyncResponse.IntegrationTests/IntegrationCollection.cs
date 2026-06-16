using Xunit;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// All integration tests share one fixture (the costly containers + host) and therefore run
/// sequentially — which also keeps global operations like Redis <c>UnsubscribeAll</c> from
/// interfering across tests.
/// </summary>
[CollectionDefinition(Name)]
public sealed class IntegrationCollection : ICollectionFixture<IntegrationFixture>
{
    public const string Name = "AsyncResponse integration";
}
