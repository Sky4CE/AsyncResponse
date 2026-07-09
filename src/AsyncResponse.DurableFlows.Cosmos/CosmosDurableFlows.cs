using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.Cosmos;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System.Net;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>DI registration for the Azure Cosmos DB durable-flow state store.</summary>
    public static class CosmosDurableFlowServiceCollectionExtensions
    {
        /// <summary>
        /// Stores durable-flow state in Azure Cosmos DB. Hosts may either register a
        /// <see cref="CosmosClient"/> singleton or set connection options here.
        /// </summary>
        public static AsyncResponseRegistrationBuilder WithCosmosDurableFlows(
            this AsyncResponseRegistrationBuilder builder,
            Action<CosmosDurableFlowOptions>? configure = null)
        {
            builder.Services.AddOptions();
            if (configure is not null)
                builder.Services.Configure(configure);

            builder.Services.TryAddSingleton(provider =>
            {
                var options = provider.GetRequiredService<IOptions<CosmosDurableFlowOptions>>().Value;
                if (string.IsNullOrWhiteSpace(options.ConnectionString))
                    throw new InvalidOperationException($"{nameof(CosmosDurableFlowOptions)}.{nameof(CosmosDurableFlowOptions.ConnectionString)} must be configured when no CosmosClient is registered.");
                return new CosmosClient(options.ConnectionString);
            });

            builder.Services.TryAddScoped<CosmosFlowStateStore>();
            return builder.WithCustomDurableFlows<CosmosFlowStateStore>();
        }
    }
}

namespace AsyncResponse.DurableFlows.Cosmos
{
/// <summary>Options for the Azure Cosmos DB durable-flow state store.</summary>
public sealed class CosmosDurableFlowOptions
{
    /// <summary>Optional Cosmos DB connection string used when no <see cref="CosmosClient"/> is registered.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Cosmos database name. Required.</summary>
    public string? DatabaseName { get; set; }

    /// <summary>Container storing one durable-flow ledger document per flow id.</summary>
    public string ContainerName { get; set; } = "asyncresponse_flow_state";

    /// <summary>Partition-key path for the container. Default: <c>/flowId</c>.</summary>
    public string PartitionKeyPath { get; set; } = "/flowId";

    /// <summary>Creates the database and container on first use.</summary>
    public bool AutoCreateContainer { get; set; } = true;

    /// <summary>Optional throughput used when auto-creating the container.</summary>
    public int? Throughput { get; set; }

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(DatabaseName))
            throw new InvalidOperationException($"{nameof(CosmosDurableFlowOptions)}.{nameof(DatabaseName)} must be configured.");
        if (string.IsNullOrWhiteSpace(ContainerName))
            throw new InvalidOperationException($"{nameof(CosmosDurableFlowOptions)}.{nameof(ContainerName)} must be configured.");
        if (string.IsNullOrWhiteSpace(PartitionKeyPath) || !PartitionKeyPath.StartsWith("/", StringComparison.Ordinal))
            throw new InvalidOperationException($"{nameof(CosmosDurableFlowOptions)}.{nameof(PartitionKeyPath)} must start with '/'.");
        if (Throughput is <= 0)
            throw new InvalidOperationException($"{nameof(CosmosDurableFlowOptions)}.{nameof(Throughput)} must be positive when configured.");
    }
}

/// <summary>Azure Cosmos DB implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class CosmosFlowStateStore : IFlowStateStore
{
    private readonly CosmosClient _client;
    private readonly CosmosDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private bool _created;

    public CosmosFlowStateStore(CosmosClient client, IOptions<CosmosDurableFlowOptions> options)
    {
        _client = client;
        _options = options.Value;
        _options.Validate();
    }

    public async Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateSave(flowId, state, ttl);
        var container = await GetContainerAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTime.UtcNow;
        await container.UpsertItemAsync(
            new CosmosFlowStateDocument
            {
                Id = flowId,
                FlowId = flowId,
                StateJson = DurableFlowStoreShared.Serialize(state),
                ExpiresAtUtc = now.Add(ttl),
                UpdatedAtUtc = now
            },
            new PartitionKey(flowId),
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        var container = await GetContainerAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            var response = await container.ReadItemAsync<CosmosFlowStateDocument>(
                flowId,
                new PartitionKey(flowId),
                cancellationToken: cancellationToken).ConfigureAwait(false);
            var document = response.Resource;
            return document.ExpiresAtUtc > DateTime.UtcNow
                ? DurableFlowStoreShared.Deserialize(document.StateJson)
                : null;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        var container = await GetContainerAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            await container.DeleteItemAsync<CosmosFlowStateDocument>(
                flowId,
                new PartitionKey(flowId),
                cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }
    }

    private async Task<Container> GetContainerAsync(CancellationToken cancellationToken)
    {
        if (!_created && _options.AutoCreateContainer)
            await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        return _client.GetContainer(_options.DatabaseName, _options.ContainerName);
    }

    private async Task EnsureCreatedAsync(CancellationToken cancellationToken)
    {
        if (_created)
            return;

        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            var database = await _client.CreateDatabaseIfNotExistsAsync(
                _options.DatabaseName,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            await database.Database.CreateContainerIfNotExistsAsync(
                new ContainerProperties(_options.ContainerName, _options.PartitionKeyPath),
                _options.Throughput,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }
}

internal sealed class CosmosFlowStateDocument
{
    [JsonProperty("id")]
    public string Id { get; set; } = "";

    [JsonProperty("flowId")]
    public string FlowId { get; set; } = "";

    [JsonProperty("stateJson")]
    public string StateJson { get; set; } = "";

    [JsonProperty("expiresAtUtc")]
    public DateTime ExpiresAtUtc { get; set; }

    [JsonProperty("updatedAtUtc")]
    public DateTime UpdatedAtUtc { get; set; }
}
}
