using AsyncResponse;
using AsyncResponse.DurableFlows.EFCore;
using AsyncResponse.DurableFlows.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace Microsoft.Extensions.DependencyInjection
{
    /// <summary>DI registration for the Entity Framework Core durable-flow state store.</summary>
    public static class EFCoreDurableFlowServiceCollectionExtensions
    {
        /// <summary>
        /// Stores durable-flow state in a table hosted by the application's own
        /// <typeparamref name="TContext"/>. Map the table into the context's model with
        /// <see cref="EFCoreDurableFlowModelBuilderExtensions.ConfigureAsyncResponseDurableFlows"/>
        /// so it rides the application's migration pipeline; the store itself never runs DDL.
        /// <para>
        /// Each operation resolves a fresh context: from <see cref="IDbContextFactory{TContext}"/>
        /// when one is registered (<c>AddDbContextFactory</c>), otherwise the scoped
        /// <typeparamref name="TContext"/> from a new service scope (<c>AddDbContext</c>). Parallel
        /// flow executions therefore never share a <see cref="DbContext"/> instance.
        /// </para>
        /// </summary>
        public static AsyncResponseRegistrationBuilder WithEFCoreDurableFlows<TContext>(
            this AsyncResponseRegistrationBuilder builder,
            Action<EFCoreDurableFlowOptions>? configure = null)
            where TContext : DbContext
        {
            builder.Services.AddOptions();
            if (configure is not null)
                builder.Services.Configure(configure);

            // Singleton on purpose: the store holds no DbContext (each operation leases one, see
            // above), and the executor resolves the store from a fresh scope per flow execution —
            // a scoped store would redo the mapped-model check on every run.
            builder.Services.TryAddSingleton<EFCoreFlowStateStore<TContext>>();
            return builder.WithCustomDurableFlows<EFCoreFlowStateStore<TContext>>();
        }
    }
}

namespace AsyncResponse.DurableFlows.EFCore
{
/// <summary>Options for the Entity Framework Core durable-flow state store.</summary>
public sealed class EFCoreDurableFlowOptions
{
    /// <summary>
    /// How often <see cref="EFCoreFlowStateStore{TContext}.SaveAsync"/> opportunistically deletes
    /// expired rows (loads already treat expired state as absent; pruning bounds table growth).
    /// Zero or negative prunes on every save. Default: 5 minutes.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromMinutes(5);
}

/// <summary>
/// One durable-flow ledger row, mapped into the application's <see cref="DbContext"/> by
/// <see cref="EFCoreDurableFlowModelBuilderExtensions.ConfigureAsyncResponseDurableFlows"/>.
/// Column names match the other AsyncResponse.DurableFlows.* relational packages, so the table is
/// interchangeable with theirs.
/// </summary>
public sealed class DurableFlowStateRecord
{
    /// <summary>The flow run id (primary key).</summary>
    public string FlowId { get; set; } = string.Empty;

    /// <summary>The serialized <see cref="FlowState"/> ledger.</summary>
    public string StateJson { get; set; } = string.Empty;

    /// <summary>UTC instant after which the row is treated as absent and eligible for pruning.</summary>
    public DateTime ExpiresAtUtc { get; set; }

    /// <summary>UTC instant of the last save.</summary>
    public DateTime UpdatedAtUtc { get; set; }
}

/// <summary>Maps the durable-flow state table into an application model.</summary>
public static class EFCoreDurableFlowModelBuilderExtensions
{
    /// <summary>Default durable-flow state table name (shared with the other relational packages).</summary>
    public const string DefaultTableName = "asyncresponse_flow_state";

    /// <summary>
    /// Maps <see cref="DurableFlowStateRecord"/> to the durable-flow state table. Call from
    /// <c>OnModelCreating</c>; the table then flows through the application's normal EF Core
    /// migrations (or <c>EnsureCreated</c>) like any other entity.
    /// </summary>
    /// <param name="modelBuilder">The application model builder.</param>
    /// <param name="tableName">Table name. Default: <see cref="DefaultTableName"/>.</param>
    /// <param name="schema">Optional schema; <c>null</c> uses the provider default.</param>
    public static ModelBuilder ConfigureAsyncResponseDurableFlows(
        this ModelBuilder modelBuilder,
        string tableName = DefaultTableName,
        string? schema = null)
    {
        ArgumentNullException.ThrowIfNull(modelBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        modelBuilder.Entity<DurableFlowStateRecord>(entity =>
        {
            entity.ToTable(tableName, schema);
            entity.HasKey(r => r.FlowId);
            // 400 matches the sibling packages' key column and stays inside every mainstream
            // provider's index-key size limit (SQL Server 900 bytes, MySQL 3072 bytes).
            entity.Property(r => r.FlowId).HasColumnName("flow_id").HasMaxLength(400);
            entity.Property(r => r.StateJson).HasColumnName("state_json").IsRequired();
            entity.Property(r => r.ExpiresAtUtc).HasColumnName("expires_at_utc");
            entity.Property(r => r.UpdatedAtUtc).HasColumnName("updated_at_utc");
            entity.HasIndex(r => r.ExpiresAtUtc).HasDatabaseName($"{tableName}_expires_idx");
        });

        return modelBuilder;
    }
}

/// <summary>
/// Entity Framework Core implementation of <see cref="IFlowStateStore"/> over an
/// application-owned <typeparamref name="TContext"/>. Requires a relational provider
/// (deletes and updates use <c>ExecuteDeleteAsync</c>/<c>ExecuteUpdateAsync</c>).
/// </summary>
public sealed class EFCoreFlowStateStore<TContext> : IFlowStateStore
    where TContext : DbContext
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly EFCoreDurableFlowOptions _options;
    private long _lastPruneTicks;
    private volatile bool _modelChecked;

    public EFCoreFlowStateStore(IServiceScopeFactory scopeFactory, IOptions<EFCoreDurableFlowOptions> options)
    {
        _scopeFactory = scopeFactory;
        _options = options.Value;
    }

    public async Task SaveAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateSave(flowId, state, ttl);
        await using var lease = await LeaseContextAsync(cancellationToken).ConfigureAwait(false);
        var db = lease.Context;

        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await PruneExpiredAsync(db, cancellationToken).ConfigureAwait(false);

        var now = DateTime.UtcNow;
        var expiresAt = now.Add(ttl);
        var stateJson = DurableFlowStoreShared.Serialize(state);

        // Portable upsert: update-first (bulk, no tracking), insert on miss, and on a lost insert
        // race (concurrent save of the same new flow id) loop back to update the winner's row.
        for (var attempt = 0; ; attempt++)
        {
            var updated = await Records(db)
                .Where(r => r.FlowId == flowId)
                .ExecuteUpdateAsync(setters => setters
                    .SetProperty(r => r.StateJson, stateJson)
                    .SetProperty(r => r.ExpiresAtUtc, expiresAt)
                    .SetProperty(r => r.UpdatedAtUtc, now), cancellationToken)
                .ConfigureAwait(false);
            if (updated > 0)
                return;

            db.Add(new DurableFlowStateRecord
            {
                FlowId = flowId,
                StateJson = stateJson,
                ExpiresAtUtc = expiresAt,
                UpdatedAtUtc = now
            });
            try
            {
                await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (DbUpdateException) when (attempt < 2)
            {
                db.ChangeTracker.Clear();
            }
        }
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await using var lease = await LeaseContextAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTime.UtcNow;
        var stateJson = await Records(lease.Context)
            .AsNoTracking()
            .Where(r => r.FlowId == flowId && r.ExpiresAtUtc > now)
            .Select(r => r.StateJson)
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        return stateJson is null ? null : DurableFlowStoreShared.Deserialize(stateJson);
    }

    public async Task<bool> TryDeleteAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await using var lease = await LeaseContextAsync(cancellationToken).ConfigureAwait(false);

        var deleted = await Records(lease.Context)
            .Where(r => r.FlowId == flowId)
            .ExecuteDeleteAsync(cancellationToken)
            .ConfigureAwait(false);
        return deleted > 0;
    }

    private static async Task PruneExpiredAsync(TContext db, CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;
        await Records(db)
            .Where(r => r.ExpiresAtUtc <= now)
            .ExecuteDeleteAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private static DbSet<DurableFlowStateRecord> Records(TContext db) => db.Set<DurableFlowStateRecord>();

    /// <summary>
    /// Leases a context for one operation: an <see cref="IDbContextFactory{TContext}"/>-created
    /// context when a factory is registered, otherwise the scoped <typeparamref name="TContext"/>
    /// owned by a fresh scope. Never caches a context — <see cref="DbContext"/> is not thread-safe
    /// and this store is a singleton used by parallel flow executions.
    /// </summary>
    private async ValueTask<ContextLease> LeaseContextAsync(CancellationToken cancellationToken)
    {
        var scope = _scopeFactory.CreateAsyncScope();
        try
        {
            var factory = scope.ServiceProvider.GetService<IDbContextFactory<TContext>>();
            var ownsContext = factory is not null;
            var context = ownsContext
                ? await factory!.CreateDbContextAsync(cancellationToken).ConfigureAwait(false)
                : scope.ServiceProvider.GetRequiredService<TContext>();
            try
            {
                EnsureMapped(context);
                return new ContextLease(context, scope, ownsContext);
            }
            catch
            {
                if (ownsContext)
                    await context.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }
        catch
        {
            await scope.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private void EnsureMapped(TContext context)
    {
        if (_modelChecked)
            return;

        if (context.Model.FindEntityType(typeof(DurableFlowStateRecord)) is null)
            throw new InvalidOperationException(
                $"'{typeof(TContext).Name}' does not map {nameof(DurableFlowStateRecord)}. Call " +
                $"modelBuilder.{nameof(EFCoreDurableFlowModelBuilderExtensions.ConfigureAsyncResponseDurableFlows)}() " +
                "in OnModelCreating and add a migration for the durable-flow state table.");

        _modelChecked = true;
    }

    private readonly struct ContextLease : IAsyncDisposable
    {
        private readonly AsyncServiceScope _scope;
        private readonly bool _ownsContext;

        public ContextLease(TContext context, AsyncServiceScope scope, bool ownsContext)
        {
            Context = context;
            _scope = scope;
            _ownsContext = ownsContext;
        }

        public TContext Context { get; }

        public async ValueTask DisposeAsync()
        {
            // Factory-created contexts are not owned by the scope; scoped ones are disposed with it.
            if (_ownsContext)
                await Context.DisposeAsync().ConfigureAwait(false);
            await _scope.DisposeAsync().ConfigureAwait(false);
        }
    }
}
}
