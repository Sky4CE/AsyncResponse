using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Text.Json;

namespace AsyncResponse;

/// <summary>
/// Default <see cref="IFlowStateStore"/>: persists flow state through the configured channel's
/// <see cref="IRecoveryStateStore"/>. Useful for tests, development, and migration, but production
/// durable flows should use a DurableFlows.* package or application-owned storage via
/// <c>WithCustomDurableFlows&lt;TStore&gt;()</c>.
/// <para>
/// Each flow run is stored as one recovery entry under the flow id, with a fixed registration id
/// (every save replaces the previous checkpoint — the per-registration replace semantics all
/// stores already guarantee) and a sentinel <see cref="RecoveryState.PayloadTypeFullName"/> so the
/// watchdog can tell flow ledgers apart from stale waiter registrations. The state itself rides in
/// the entry's <see cref="RecoveryState.Context"/> bag, which is an arbitrary string map by
/// contract — no recovery-state schema change is involved.
/// </para>
/// </summary>
internal sealed class RecoveryBackedFlowStateStore : IFlowStateStore
{
    private readonly IRecoveryStateStore _recoveryStore;
    private readonly ILogger<RecoveryBackedFlowStateStore> _logger;
    private int _warned;

    /// <summary>Creates the default recovery-backed flow-state store.</summary>
    public RecoveryBackedFlowStateStore(
        IRecoveryStateStore recoveryStore,
        ILogger<RecoveryBackedFlowStateStore>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(recoveryStore);
        _recoveryStore = recoveryStore;
        _logger = logger ?? NullLogger<RecoveryBackedFlowStateStore>.Instance;
    }

    /// <summary>Sentinel payload-type marker identifying a recovery entry as a durable-flow ledger.</summary>
    internal const string LedgerPayloadTypeMarker = "asyncresponse/durable-flow-state";

    /// <summary>Fixed registration id: one ledger entry per flow id, replaced on every checkpoint.</summary>
    internal static readonly Guid LedgerRegistrationId = new("5df1a4a7-0d3c-4c1b-9f2e-6b7a1c0d2e3f");

    private const string StateContextKey = "asyncresponse.flow-state";

    /// <summary>Returns whether a scanned recovery entry is a durable-flow ledger.</summary>
    internal static bool IsFlowLedger(RecoveryState entry)
        => entry.PayloadTypeFullName == LedgerPayloadTypeMarker;

    /// <inheritdoc />
    public Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        ArgumentNullException.ThrowIfNull(state);
        WarnOnce(ttl);

        var entry = new RecoveryState
        {
            RegistrationId = LedgerRegistrationId,
            CorrelationId = flowId,
            PayloadTypeFullName = LedgerPayloadTypeMarker,
            RegisteredAtUtc = state.CreatedAtUtc ?? DateTime.UtcNow,
            Context = new Dictionary<string, string>(1, StringComparer.Ordinal)
            {
                [StateContextKey] = JsonSerializer.Serialize(state)
            }
        };

        return _recoveryStore.SaveAsync(flowId, entry, ttl, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);

        var entries = await _recoveryStore.GetAllAsync(flowId, cancellationToken).ConfigureAwait(false);
        foreach (var entry in entries)
        {
            if (entry is null || !IsFlowLedger(entry))
                continue;

            if (entry.Context is null || !entry.Context.TryGetValue(StateContextKey, out var json))
                return null;

            var state = JsonSafety.SafeDeserialize<FlowState>(json);
            if (state is null || !FlowStateSchema.IsReadable(state.SchemaVersion))
                return null; // unreadable or written by a newer schema: reject rather than misinterpret

            return state;
        }

        return null;
    }

    /// <inheritdoc />
    public Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        return _recoveryStore.TryDeleteAsync(flowId, LedgerRegistrationId, cancellationToken);
    }

    private void WarnOnce(TimeSpan ttl)
    {
        if (Interlocked.Exchange(ref _warned, 1) != 0)
            return;

        _logger.LogWarning(
            "Durable flows are using the default RecoveryBackedFlowStateStore. It stores flow state in the configured channel recovery store with idle TTL {StateTtl}. This is useful for tests, development, and migration, but production flows should use an AsyncResponse.DurableFlows.* package or app-owned durable storage via AddAsyncResponse().WithCustomDurableFlows<TFlowStateStore>().",
            ttl);
    }
}
