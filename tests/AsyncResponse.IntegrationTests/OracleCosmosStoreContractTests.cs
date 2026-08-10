using AsyncResponse.DurableFlows.Cosmos;
using AsyncResponse.DurableFlows.Oracle;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;
using Xunit;
using static AsyncResponse.IntegrationTests.FlowStoreContract;

namespace AsyncResponse.IntegrationTests;

/// <summary>
/// Oracle and Cosmos durable-flow store contracts. These two live in their own batch because their
/// containers are by far the largest in the suite — measured 2,180 MiB and 1,031 MiB, together more
/// than half of a default Docker VM. Kept alongside the other store contracts they made that batch
/// 5.8 GiB, which is what tipped it over whenever anything else ran at the same time.
/// </summary>
[Collection(OracleCosmosCollection.Name)]
public sealed class OracleCosmosStoreContractTests(OracleCosmosBatchFixture fixture) : IntegrationTestBase(fixture)
{
    /// <summary>Oracle provisions its database on first boot; 30s is not close to enough.</summary>
    private static readonly TimeSpan OracleReadyBudget = TimeSpan.FromMinutes(3);

    [Fact]
    public async Task OraclePackageStore_RoundTrips_Expires_Deletes_WhenConnectionStringIsConfigured()
    {
        var connectionString = Environment.GetEnvironmentVariable("ASYNCRESPONSE_ITEST_ORACLE_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString))
            Assert.Skip("Set ASYNCRESPONSE_ITEST_ORACLE_CONNECTION_STRING to run the Oracle durable-flow store contract test.");

        await WaitForOracleAsync(connectionString);
        var table = NewIdentifier("DF_ORACLE", 18).ToUpperInvariant();
        try
        {
            var store = new OracleFlowStateStore(
                Options.Create(new OracleDurableFlowOptions
                {
                    ConnectionString = connectionString,
                    TableName = table
                }));

            await AssertStoreContractAsync(store);

            // A second process can provision against an already-created schema. Oracle reports
            // both CREATE statements as "already exists"; the store must treat that as success.
            var secondStore = new OracleFlowStateStore(
                Options.Create(new OracleDurableFlowOptions
                {
                    ConnectionString = connectionString,
                    TableName = table
                }));
            Assert.Null(await secondStore.LoadAsync($"missing-{Guid.NewGuid():N}"));

            var mismatchedRevisionFlowId = $"revision-{Guid.NewGuid():N}";
            Assert.True(await store.TryCreateAsync(
                mismatchedRevisionFlowId,
                CreateState(mismatchedRevisionFlowId),
                TimeSpan.FromMinutes(1)));

            await using var revisionConnection = new OracleConnection(connectionString);
            await revisionConnection.OpenAsync();
            await using var revisionCommand = revisionConnection.CreateCommand();
            revisionCommand.BindByName = true;
            revisionCommand.CommandText = $"UPDATE {table} SET revision = revision + 1 WHERE flow_id = :flow_id";
            revisionCommand.Parameters.Add(new OracleParameter("flow_id", mismatchedRevisionFlowId));
            Assert.Equal(1, await revisionCommand.ExecuteNonQueryAsync());
            Assert.Null(await store.LoadAsync(mismatchedRevisionFlowId));
        }
        finally
        {
            await using var connection = new OracleConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE {table} PURGE";
            try
            {
                await command.ExecuteNonQueryAsync();
            }
            catch (OracleException ex) when (ex.Number == 942)
            {
            }
        }
    }

    [Fact]
    public async Task CosmosPackageStore_RoundTrips_Expires_Deletes_WhenConnectionStringIsConfigured()
    {
        var connectionString = Environment.GetEnvironmentVariable("ASYNCRESPONSE_ITEST_COSMOS_CONNECTION_STRING");
        if (string.IsNullOrWhiteSpace(connectionString))
            Assert.Skip("Set ASYNCRESPONSE_ITEST_COSMOS_CONNECTION_STRING to run the Cosmos DB durable-flow store contract test.");

        var databaseName = NewIdentifier("df_cosmos", 63);
        using var client = new CosmosClient(connectionString, GetCosmosClientOptions(connectionString));
        await WaitForCosmosAsync(client);
        try
        {
            var store = new CosmosFlowStateStore(
                client,
                Options.Create(new CosmosDurableFlowOptions
                {
                    DatabaseName = databaseName,
                    ContainerName = "flow_state"
                }));

            await AssertStoreContractAsync(store);

            await client.GetDatabase(databaseName).CreateContainerAsync(
                new ContainerProperties("flow_state_without_ttl", "/flowId"));
            var manualStore = new CosmosFlowStateStore(
                client,
                Options.Create(new CosmosDurableFlowOptions
                {
                    DatabaseName = databaseName,
                    ContainerName = "flow_state_without_ttl",
                    AutoCreateContainer = false
                }));
            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => manualStore.LoadAsync("flow"));
            Assert.Contains("TTL", exception.Message, StringComparison.Ordinal);
        }
        finally
        {
            try
            {
                await client.GetDatabase(databaseName).DeleteAsync();
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
            }
        }
    }

    private static async Task WaitForOracleAsync(string connectionString)
        // Oracle creates its database on first boot and needs far longer than the 30s default.
        => await EventuallyAsync(async () =>
        {
            await using var connection = new OracleConnection(connectionString);
            await connection.OpenAsync();
        }, OracleReadyBudget);

    private static async Task WaitForCosmosAsync(CosmosClient client)
        => await EventuallyAsync(async () => _ = await client.ReadAccountAsync());

    private static CosmosClientOptions GetCosmosClientOptions(string connectionString)
    {
        var options = new CosmosClientOptions();
        if (!IsLocalCosmosEndpoint(connectionString))
            return options;

        options.ConnectionMode = ConnectionMode.Gateway;
        options.LimitToEndpoint = true;
        options.HttpClientFactory = () => new HttpClient(new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator
        });
        return options;
    }

    private static bool IsLocalCosmosEndpoint(string connectionString)
    {
        foreach (var segment in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var pair = segment.Split('=', 2, StringSplitOptions.TrimEntries);
            if (pair.Length == 2 &&
                pair[0].Equals("AccountEndpoint", StringComparison.OrdinalIgnoreCase) &&
                Uri.TryCreate(pair[1], UriKind.Absolute, out var endpoint))
            {
                return endpoint.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase) ||
                       endpoint.Host is "127.0.0.1" or "::1";
            }
        }

        return false;
    }
}
