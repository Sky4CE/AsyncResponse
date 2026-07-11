using AsyncResponse;
using AsyncResponse.DurableFlows.Internal;
using AsyncResponse.DurableFlows.MongoDB;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>DI registration for the MongoDB durable-flow state store.</summary>
    public static class MongoDurableFlowServiceCollectionExtensions
    {
        /// <summary>
        /// Stores durable-flow state in MongoDB. Hosts may either register an
        /// <see cref="IMongoDatabase"/> singleton or set connection options here.
        /// </summary>
        public static AsyncResponseRegistrationBuilder WithMongoDbDurableFlows(
            this AsyncResponseRegistrationBuilder builder,
            Action<MongoDbDurableFlowOptions>? configure = null)
        {
            builder.Services.AddOptions();
            if (configure is not null)
                builder.Services.Configure(configure);

            // Singleton on purpose: index provisioning is cached per store instance, and the
            // executor resolves the store from a fresh scope per flow execution. Host-registered
            // IMongoDatabase / IMongoClient services are reused when present; otherwise the store
            // creates and owns a client from the options. Nothing is registered as a bare
            // IMongoClient/IMongoDatabase service, so unrelated resolutions of those types are
            // never answered — or broken — by this package.
            builder.Services.TryAddSingleton(provider =>
            {
                var options = provider.GetRequiredService<IOptions<MongoDbDurableFlowOptions>>();

                var database = provider.GetService<IMongoDatabase>();
                if (database is not null)
                    return new MongoDbFlowStateStore(database, options);

                if (string.IsNullOrWhiteSpace(options.Value.DatabaseName))
                    throw new InvalidOperationException($"{nameof(MongoDbDurableFlowOptions)}.{nameof(MongoDbDurableFlowOptions.DatabaseName)} must be configured when no IMongoDatabase is registered.");

                var sharedClient = provider.GetService<IMongoClient>();
                if (sharedClient is not null)
                    return new MongoDbFlowStateStore(sharedClient.GetDatabase(options.Value.DatabaseName), options);

                if (string.IsNullOrWhiteSpace(options.Value.ConnectionString))
                    throw new InvalidOperationException($"{nameof(MongoDbDurableFlowOptions)}.{nameof(MongoDbDurableFlowOptions.ConnectionString)} must be configured when no IMongoDatabase or IMongoClient is registered.");

                var ownedClient = new MongoClient(options.Value.ConnectionString);
                return new MongoDbFlowStateStore(ownedClient.GetDatabase(options.Value.DatabaseName), options, ownedClient);
            });
            return builder.WithCustomDurableFlows<MongoDbFlowStateStore>();
        }
    }
}

namespace AsyncResponse.DurableFlows.MongoDB
{
/// <summary>Options for the MongoDB durable-flow state store.</summary>
public sealed class MongoDbDurableFlowOptions
{
    /// <summary>Optional MongoDB connection string used when no <see cref="IMongoDatabase"/> is registered.</summary>
    public string? ConnectionString { get; set; }

    /// <summary>Optional database name used when no <see cref="IMongoDatabase"/> is registered.</summary>
    public string? DatabaseName { get; set; }

    /// <summary>Collection storing one durable-flow ledger document per flow id.</summary>
    public string CollectionName { get; set; } = "asyncresponse_flow_state";

    /// <summary>Creates the expiry index on first use.</summary>
    public bool AutoCreateIndexes { get; set; } = true;

    /// <summary>Validates option values and throws on misconfiguration.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(CollectionName))
            throw new InvalidOperationException($"{nameof(MongoDbDurableFlowOptions)}.{nameof(CollectionName)} must be configured.");
    }
}

/// <summary>MongoDB implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class MongoDbFlowStateStore : IFlowStateStore, IDisposable
{
    private readonly IMongoCollection<MongoFlowStateDocument> _collection;
    private readonly MongoDbDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly IMongoClient? _ownedClient;
    private bool _created;

    public MongoDbFlowStateStore(IMongoDatabase database, IOptions<MongoDbDurableFlowOptions> options, IMongoClient? ownedClient = null)
    {
        _options = options.Value;
        _options.Validate();
        _collection = database.GetCollection<MongoFlowStateDocument>(_options.CollectionName);
        _ownedClient = ownedClient;
    }

    public async Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateSave(flowId, state, ttl);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTime.UtcNow;
        var document = new MongoFlowStateDocument
        {
            FlowId = flowId,
            StateJson = DurableFlowStoreShared.Serialize(state),
            ExpiresAtUtc = now.Add(ttl),
            UpdatedAtUtc = now
        };
        await _collection.ReplaceOneAsync(
            Builders<MongoFlowStateDocument>.Filter.Eq(item => item.FlowId, flowId),
            document,
            new ReplaceOptions { IsUpsert = true },
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTime.UtcNow;
        var filter = Builders<MongoFlowStateDocument>.Filter.Eq(item => item.FlowId, flowId)
                     & Builders<MongoFlowStateDocument>.Filter.Gt(item => item.ExpiresAtUtc, now);
        var document = await _collection.Find(filter).FirstOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        return document is null ? null : DurableFlowStoreShared.Deserialize(document.StateJson);
    }

    public async Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await EnsureCreatedAsync(cancellationToken).ConfigureAwait(false);

        var result = await _collection.DeleteOneAsync(
            Builders<MongoFlowStateDocument>.Filter.Eq(item => item.FlowId, flowId),
            cancellationToken).ConfigureAwait(false);
        return result.DeletedCount > 0;
    }

    private async Task EnsureCreatedAsync(CancellationToken cancellationToken)
    {
        if (_created || !_options.AutoCreateIndexes)
            return;

        await _ensureGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_created)
                return;

            // A TTL index (expireAfterSeconds = 0 on the expiry timestamp) makes MongoDB itself
            // reap expired ledgers — no application-side pruning needed. Loads still filter on
            // ExpiresAtUtc because the TTL monitor only runs periodically (~60s).
            var indexName = $"{_options.CollectionName}_expires_idx";
            var model = new CreateIndexModel<MongoFlowStateDocument>(
                Builders<MongoFlowStateDocument>.IndexKeys.Ascending(item => item.ExpiresAtUtc),
                new CreateIndexOptions { Name = indexName, ExpireAfter = TimeSpan.Zero });
            try
            {
                await _collection.Indexes.CreateOneAsync(model, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (MongoCommandException ex) when (ex.Code is 85 or 86)
            {
                // IndexOptionsConflict/IndexKeySpecsConflict: an earlier package version created the
                // same-named index without the TTL option. Replace it in place.
                await _collection.Indexes.DropOneAsync(indexName, cancellationToken).ConfigureAwait(false);
                await _collection.Indexes.CreateOneAsync(model, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    /// <summary>Disposes the Mongo client when the store created (and therefore owns) it.</summary>
    public void Dispose()
    {
        _ensureGate.Dispose();
        (_ownedClient as IDisposable)?.Dispose();
    }
}

internal sealed class MongoFlowStateDocument
{
    [BsonId]
    [BsonElement("_id")]
    public string FlowId { get; set; } = "";

    [BsonElement("state_json")]
    public string StateJson { get; set; } = "";

    [BsonElement("expires_at_utc")]
    public DateTime ExpiresAtUtc { get; set; }

    [BsonElement("updated_at_utc")]
    public DateTime UpdatedAtUtc { get; set; }
}
}
