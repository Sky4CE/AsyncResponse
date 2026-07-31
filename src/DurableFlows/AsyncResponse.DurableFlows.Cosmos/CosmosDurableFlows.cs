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
            // Singleton on purpose: database/container provisioning is cached per store instance
            // and Cosmos metadata operations are RU-charged and rate-limited — a scoped store would
            // re-issue them on every flow execution. A host-registered CosmosClient is reused when
            // present; otherwise the store creates and owns one from ConnectionString. Nothing is
            // registered as a bare CosmosClient service, so unrelated resolutions of that type are
            // never answered — or broken — by this package.
            builder.Services.TryAddSingleton(provider =>
            {
                var options = provider.GetRequiredService<IOptions<CosmosDurableFlowOptions>>();

                var shared = provider.GetService<CosmosClient>();
                if (shared is not null)
                    return new CosmosFlowStateStore(shared, options);

                if (string.IsNullOrWhiteSpace(options.Value.ConnectionString))
                    throw new InvalidOperationException($"{nameof(CosmosDurableFlowOptions)}.{nameof(CosmosDurableFlowOptions.ConnectionString)} must be configured when no CosmosClient is registered.");
                return new CosmosFlowStateStore(new CosmosClient(options.Value.ConnectionString), options, ownsClient: true);
            });
            return builder.WithDurableFlows<CosmosFlowStateStore, CosmosDurableFlowOptions>(configure);
        }
    }
}

namespace AsyncResponse.DurableFlows.Cosmos
{
/// <summary>Options for the Azure Cosmos DB durable-flow state store.</summary>
public sealed class CosmosDurableFlowOptions : DurableFlowOptions
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

    /// <summary>
    /// Maximum serialized flow-state size in bytes accepted by writes; oversized ledgers fail fast
    /// with an actionable error instead of the raw Cosmos 413 the executor would retry into the
    /// dead-letter queue. Default: 1.9 MB (headroom under Cosmos's 2 MB item cap for the sibling
    /// fields); <c>null</c> disables the guard.
    /// </summary>
    public long? MaxStateBytes { get; set; } = 1_900_000;

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
        if (MaxStateBytes is <= 0)
            throw new InvalidOperationException($"{nameof(CosmosDurableFlowOptions)}.{nameof(MaxStateBytes)} must be positive when configured.");
    }
}

/// <summary>Azure Cosmos DB implementation of <see cref="IFlowStateStore"/>.</summary>
public sealed class CosmosFlowStateStore : IFlowStateStore, IDisposable
{
    // Time authority: this store keeps the app clock (DateTime.UtcNow) for expiry and lease
    // comparisons. Cosmos conditional writes (ETag preconditions) evaluate client-supplied
    // values only — there is no server-clock expression usable inside a point write — so the
    // read-check-replace cycles below compare against the app clock and rely on the ETag fence
    // for atomicity. Multi-node deployments should keep worker clocks synchronized well inside
    // the lease window. (The server's own TTL sweep, by contrast, runs on the service clock.)
    private readonly CosmosClient _client;
    private readonly CosmosDurableFlowOptions _options;
    private readonly SemaphoreSlim _ensureGate = new(1, 1);
    private readonly bool _ownsClient;
    private bool _created;

    public CosmosFlowStateStore(CosmosClient client, IOptions<CosmosDurableFlowOptions> options, bool ownsClient = false)
    {
        _client = client;
        _options = options.Value;
        _options.Validate();
        _ownsClient = ownsClient;
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
            // Point reads have no predicate, so the expiry check happens client-side on the app
            // clock — see the time-authority note on this class.
            if (document.ExpiresAtUtc <= DateTime.UtcNow)
                return null;

            return document.Revision is { } revision
                ? DurableFlowStoreShared.ReadState(flowId, document.StateJson, revision)
                : null;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }

    public async Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "Cosmos DB");
        var container = await GetContainerAsync(cancellationToken).ConfigureAwait(false);

        // Bounded retry: documents carry native Cosmos TTL alongside the logical ExpiresAtUtc, so
        // the server's TTL sweep can physically purge an expired document between our create's
        // 409 and the follow-up read (or the conditional replace). A vanished conflicting
        // document means the slot is free — create again — not that a live competitor won.
        // The short growing delay lets one purge cycle finish instead of burning all attempts
        // inside the same inconsistency window (the Linux emulator is markedly worse here than
        // the service, but the race itself is real on both).
        for (var attempt = 0; attempt < 4; attempt++)
        {
            if (attempt > 0)
                await Task.Delay(TimeSpan.FromMilliseconds(50 * attempt), cancellationToken).ConfigureAwait(false);

            var now = DateTime.UtcNow;
            var document = CreateDocument(flowId, stateJson, state.Revision, ttl, now);
            try
            {
                await container.CreateItemAsync(document, new PartitionKey(flowId), cancellationToken: cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
                try
                {
                    var current = await container.ReadItemAsync<CosmosFlowStateDocument>(
                        flowId,
                        new PartitionKey(flowId),
                        cancellationToken: cancellationToken).ConfigureAwait(false);
                    if (current.Resource.ExpiresAtUtc > now)
                        return false;

                    await container.ReplaceItemAsync(
                        document,
                        flowId,
                        new PartitionKey(flowId),
                        new ItemRequestOptions { IfMatchEtag = current.ETag },
                        cancellationToken).ConfigureAwait(false);
                    return true;
                }
                catch (CosmosException retryEx) when (retryEx.StatusCode == HttpStatusCode.PreconditionFailed)
                {
                    // The ETag moved under us. That is either a live writer winning the id or the
                    // TTL purge touching the expired document; only the next read can tell them
                    // apart, so retry instead of conceding — a live occupant surfaces as an
                    // unexpired read (false) on the next attempt, a purged one as a clean create.
                    continue;
                }
                catch (CosmosException retryEx) when (retryEx.StatusCode == HttpStatusCode.NotFound)
                {
                    // The expired document was TTL-purged after our 409 — the id is free again.
                    continue;
                }
            }
        }

        return false;
    }

    public async Task<bool> TryUpdateAsync(
        string flowId,
        FlowState state,
        long expectedRevision,
        TimeSpan ttl,
        string? leaseId = null,
        CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateUpdate(flowId, state, expectedRevision, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "Cosmos DB");
        var container = await GetContainerAsync(cancellationToken).ConfigureAwait(false);
        for (var attempt = 0; attempt < 4; attempt++)
        {
            var now = DateTime.UtcNow;
            try
            {
                var current = await container.ReadItemAsync<CosmosFlowStateDocument>(
                    flowId,
                    new PartitionKey(flowId),
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                var document = current.Resource;
                if (document.ExpiresAtUtc <= now || document.Revision != expectedRevision)
                    return false;
                if (leaseId is not null && (document.LeaseId != leaseId || document.LeaseExpiresAtUtc <= now))
                    return false;

                document.StateJson = stateJson;
                document.ExpiresAtUtc = DurableFlowStoreShared.AddSaturating(now, ttl);
                document.UpdatedAtUtc = now;
                document.Revision = state.Revision;
                document.Ttl = CosmosTtlSeconds(ttl);
                await container.ReplaceItemAsync(
                    document,
                    flowId,
                    new PartitionKey(flowId),
                    new ItemRequestOptions { IfMatchEtag = current.ETag },
                    cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
            {
                // Lease renewal also replaces the document and changes its ETag without changing
                // the ledger revision. Re-read and retry so that benign race is not reported as a
                // lost execution lease; a real state race fails the revision check above.
            }
        }

        return false;
    }

    public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: true, cancellationToken);

    public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: false, cancellationToken);

    public async Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
    {
        var container = await GetContainerAsync(cancellationToken).ConfigureAwait(false);
        for (var attempt = 0; attempt < 4; attempt++)
        {
            try
            {
                var current = await container.ReadItemAsync<CosmosFlowStateDocument>(
                    flowId,
                    new PartitionKey(flowId),
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                if (current.Resource.LeaseId != leaseId)
                    return;

                current.Resource.LeaseId = null;
                current.Resource.LeaseExpiresAtUtc = null;
                // Every replace refreshes _ts — the anchor the server-side TTL counts from — so
                // re-persisting the stored full-window ttl would restart the physical-retention
                // countdown and decouple it from the logical ExpiresAtUtc. Rewrite it from the
                // remaining logical window instead. (Checkpoints recompute both together in
                // TryUpdateAsync; only the lease paths replace without moving ExpiresAtUtc.)
                current.Resource.Ttl = CosmosTtlSeconds(current.Resource.ExpiresAtUtc, DateTime.UtcNow);
                await container.ReplaceItemAsync(
                    current.Resource,
                    flowId,
                    new PartitionKey(flowId),
                    new ItemRequestOptions { IfMatchEtag = current.ETag },
                    cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
            {
            }
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
        if (!_created)
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

            ContainerResponse container;
            if (_options.AutoCreateContainer)
            {
                var database = await _client.CreateDatabaseIfNotExistsAsync(
                    _options.DatabaseName,
                    cancellationToken: cancellationToken).ConfigureAwait(false);

                // DefaultTimeToLive = -1 enables per-item TTL without a container-wide default.
                var properties = new ContainerProperties(_options.ContainerName, _options.PartitionKeyPath)
                {
                    DefaultTimeToLive = -1
                };
                container = await database.Database.CreateContainerIfNotExistsAsync(
                    properties,
                    _options.Throughput,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            else
            {
                container = await _client
                    .GetContainer(_options.DatabaseName, _options.ContainerName)
                    .ReadContainerAsync(cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
            }

            if (!string.Equals(container.Resource.PartitionKeyPath, _options.PartitionKeyPath, StringComparison.Ordinal))
                throw new InvalidOperationException(
                    $"Cosmos container '{_options.ContainerName}' uses partition key '{container.Resource.PartitionKeyPath}', " +
                    $"but '{_options.PartitionKeyPath}' is required.");
            if (container.Resource.DefaultTimeToLive is null)
                throw new InvalidOperationException(
                    $"Cosmos container '{_options.ContainerName}' does not have TTL enabled. " +
                    "Enable container TTL (DefaultTimeToLive = -1) before using it for durable flows.");

            ValidateHostSerializer();
            _created = true;
        }
        finally
        {
            _ensureGate.Release();
        }
    }

    /// <summary>
    /// Fails provisioning fast when a host-registered <see cref="CosmosClient"/> carries a custom
    /// serializer that does not honor the flow-state document's JSON property names.
    /// <see cref="CosmosFlowStateDocument"/> is attributed for both Newtonsoft.Json
    /// (<c>[JsonProperty]</c>) and System.Text.Json (<c>[JsonPropertyName]</c>), so the SDK's
    /// default serializer and STJ-based serializers both map correctly; a serializer honoring
    /// neither would write documents whose <c>id</c> Cosmos rejects — or, worse, whose fields
    /// silently round-trip as nulls. The probe serializes a sentinel document through the host's
    /// serializer and verifies the wire property names survive.
    /// </summary>
    private void ValidateHostSerializer()
    {
        if (_client.ClientOptions?.Serializer is not { } serializer)
            return; // The SDK default serializer honors [JsonProperty]; nothing to probe.

        var now = DateTime.UtcNow;
        using var stream = serializer.ToStream(new CosmosFlowStateDocument
        {
            Id = "asyncresponse-serializer-probe",
            FlowId = "asyncresponse-serializer-probe",
            StateJson = "{}",
            ExpiresAtUtc = now,
            UpdatedAtUtc = now,
            Revision = 0,
            LeaseId = "probe",
            LeaseExpiresAtUtc = now,
            Ttl = 1
        });
        using var probe = System.Text.Json.JsonDocument.Parse(stream);
        if (!probe.RootElement.TryGetProperty("id", out _) || !probe.RootElement.TryGetProperty("leaseExpiresAtUtc", out _))
        {
            throw new InvalidOperationException(
                $"The registered CosmosClient's serializer ({serializer.GetType().Name}) does not honor the durable-flow document's " +
                "JSON property names ('id', 'flowId', 'leaseExpiresAtUtc', ...). Flow-state documents would be written with wrong " +
                "property names and could not be read back. Configure the serializer to honor Newtonsoft.Json [JsonProperty] or " +
                "System.Text.Json [JsonPropertyName] attributes, or let the SDK use its default serializer.");
        }
    }

    private async Task<bool> UpdateLeaseAsync(
        string flowId,
        string leaseId,
        TimeSpan leaseDuration,
        bool acquire,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentException.ThrowIfNullOrWhiteSpace(leaseId);
        if (leaseDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(leaseDuration));

        var container = await GetContainerAsync(cancellationToken).ConfigureAwait(false);
        for (var attempt = 0; attempt < 4; attempt++)
        {
            var now = DateTime.UtcNow;
            try
            {
                var current = await container.ReadItemAsync<CosmosFlowStateDocument>(
                    flowId,
                    new PartitionKey(flowId),
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                var document = current.Resource;
                if (document.ExpiresAtUtc <= now || document.Revision is null)
                    return false;
                if (acquire)
                {
                    if (document.LeaseId is not null && document.LeaseId != leaseId && document.LeaseExpiresAtUtc > now)
                        return false;
                }
                else if (document.LeaseId != leaseId || document.LeaseExpiresAtUtc <= now)
                {
                    return false;
                }

                document.LeaseId = leaseId;
                document.LeaseExpiresAtUtc = DurableFlowStoreShared.AddSaturating(now, leaseDuration);
                // Same _ts realignment as ReleaseLeaseAsync: a lease heartbeat replaces the
                // document without moving ExpiresAtUtc, so it must not restart the server TTL's
                // full retention window.
                document.Ttl = CosmosTtlSeconds(document.ExpiresAtUtc, now);
                await container.ReplaceItemAsync(
                    document,
                    flowId,
                    new PartitionKey(flowId),
                    new ItemRequestOptions { IfMatchEtag = current.ETag },
                    cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
            {
            }
        }

        return false;
    }

    private static CosmosFlowStateDocument CreateDocument(string flowId, string stateJson, long revision, TimeSpan ttl, DateTime now)
        => new()
        {
            Id = flowId,
            FlowId = flowId,
            StateJson = stateJson,
            ExpiresAtUtc = DurableFlowStoreShared.AddSaturating(now, ttl),
            UpdatedAtUtc = now,
            Revision = revision,
            // Cosmos reaps the item itself once container TTL is enabled. Ceiling keeps the
            // server-side TTL from being shorter than the requested duration.
            Ttl = CosmosTtlSeconds(ttl)
        };

    /// <summary>Per-item TTL in whole seconds, rounded up and saturated at int.MaxValue (~68 years) for absurd expiries.</summary>
    private static int CosmosTtlSeconds(TimeSpan ttl)
        => (int)Math.Min(Math.Ceiling(ttl.TotalSeconds), int.MaxValue);

    /// <summary>
    /// Remaining per-item TTL in whole seconds until <paramref name="expiresAtUtc"/>, rounded up
    /// and floored at 1 (Cosmos rejects 0). Used by replaces that keep the logical expiry in place:
    /// an already-due document collapses to the shortest legal TTL so the next sweep purges it
    /// instead of the replace granting it a fresh retention window.
    /// </summary>
    private static int CosmosTtlSeconds(DateTime expiresAtUtc, DateTime now)
        => (int)Math.Min(Math.Max(Math.Ceiling((expiresAtUtc - now).TotalSeconds), 1), int.MaxValue);

    /// <summary>Disposes the Cosmos client when the store created (and therefore owns) it.</summary>
    public void Dispose()
    {
        _ensureGate.Dispose();
        if (_ownsClient)
            _client.Dispose();
    }
}

/// <summary>
/// One durable-flow ledger document. Attributed for BOTH Newtonsoft.Json and System.Text.Json:
/// the Cosmos SDK's default serializer is Newtonsoft-based, but hosts may register a
/// <see cref="CosmosClient"/> with an STJ-based serializer — with single-stack attributes such a
/// client would silently write PascalCase property names (breaking <c>id</c> and every read
/// back). <see cref="CosmosFlowStateStore"/> additionally probes custom serializers at
/// provisioning time and fails fast when neither attribute set is honored.
/// </summary>
internal sealed class CosmosFlowStateDocument
{
    [JsonProperty("id")]
    [System.Text.Json.Serialization.JsonPropertyName("id")]
    public string Id { get; set; } = "";

    [JsonProperty("flowId")]
    [System.Text.Json.Serialization.JsonPropertyName("flowId")]
    public string FlowId { get; set; } = "";

    [JsonProperty("stateJson")]
    [System.Text.Json.Serialization.JsonPropertyName("stateJson")]
    public string StateJson { get; set; } = "";

    [JsonProperty("expiresAtUtc")]
    [System.Text.Json.Serialization.JsonPropertyName("expiresAtUtc")]
    public DateTime ExpiresAtUtc { get; set; }

    [JsonProperty("updatedAtUtc")]
    [System.Text.Json.Serialization.JsonPropertyName("updatedAtUtc")]
    public DateTime UpdatedAtUtc { get; set; }

    [JsonProperty("revision")]
    [System.Text.Json.Serialization.JsonPropertyName("revision")]
    public long? Revision { get; set; }

    [JsonProperty("leaseId", NullValueHandling = NullValueHandling.Ignore)]
    [System.Text.Json.Serialization.JsonPropertyName("leaseId")]
    [System.Text.Json.Serialization.JsonIgnore(Condition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull)]
    public string? LeaseId { get; set; }

    [JsonProperty("leaseExpiresAtUtc", NullValueHandling = NullValueHandling.Ignore)]
    [System.Text.Json.Serialization.JsonPropertyName("leaseExpiresAtUtc")]
    [System.Text.Json.Serialization.JsonIgnore(Condition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull)]
    public DateTime? LeaseExpiresAtUtc { get; set; }

    /// <summary>Cosmos per-item TTL in seconds; honored once the container enables TTL.</summary>
    [JsonProperty("ttl", NullValueHandling = NullValueHandling.Ignore)]
    [System.Text.Json.Serialization.JsonPropertyName("ttl")]
    [System.Text.Json.Serialization.JsonIgnore(Condition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull)]
    public int? Ttl { get; set; }
}
}
