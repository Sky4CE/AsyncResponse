using System.Diagnostics.CodeAnalysis;
using AsyncResponse;
using AsyncResponse.DurableFlows.EFCore;
using AsyncResponse.DurableFlows.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
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
        public static AsyncResponseRegistrationBuilder WithEFCoreDurableFlows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.NonPublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties)] TContext>(
            this AsyncResponseRegistrationBuilder builder,
            Action<EFCoreDurableFlowOptions>? configure = null)
            where TContext : DbContext
        {
            // Singleton on purpose: the store holds no DbContext (each operation leases one, see
            // above), and the executor resolves the store from a fresh scope per flow execution —
            // a scoped store would redo the mapped-model check on every run.
            builder.Services.TryAddSingleton<EFCoreFlowStateStore<TContext>>();
            return builder.WithDurableFlows<EFCoreFlowStateStore<TContext>, EFCoreDurableFlowOptions>(configure);
        }
    }
}

namespace AsyncResponse.DurableFlows.EFCore
{
/// <summary>Options for the Entity Framework Core durable-flow state store.</summary>
public sealed class EFCoreDurableFlowOptions : DurableFlowOptions
{
    /// <summary>
    /// How often <see cref="EFCoreFlowStateStore{TContext}.TryCreateAsync"/> opportunistically deletes
    /// one bounded batch (1000 rows) of expired rows (loads already treat expired state as absent;
    /// pruning bounds table growth). Zero or negative prunes on every save. Default: 5 minutes.
    /// </summary>
    public TimeSpan PruneInterval { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Maximum serialized flow-state size in bytes accepted by writes; oversized ledgers fail fast
    /// with an actionable error instead of an opaque provider error. Default: <c>null</c>
    /// (unlimited — relational text/blob columns are effectively unbounded), settable as an
    /// operator budget.
    /// </summary>
    public long? MaxStateBytes { get; set; }
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

    /// <summary>Optimistic-concurrency revision of the durable ledger.</summary>
    public long Revision { get; set; }

    /// <summary>Current execution-lease owner, when a worker is running the flow.</summary>
    public string? LeaseId { get; set; }

    /// <summary>UTC expiry of the current execution lease.</summary>
    public DateTime? LeaseExpiresAtUtc { get; set; }
}

/// <summary>
/// What a provider needs from the <c>flow_id</c> collation, when it needs anything at all. Kept
/// apart from the store so the rules are one table rather than a chain of conditions inside a
/// generic type — and so both branches can be exercised without dragging in every EF Core provider.
/// </summary>
internal static class FlowIdCollationRules
{
    /// <summary>
    /// The rules for a provider whose DEFAULT collation folds case, or <c>null</c> when the default
    /// is already ordinal (PostgreSQL and SQLite compare byte-wise) or the provider is unknown — a
    /// third-party provider gets the benefit of the doubt rather than a startup failure it has no
    /// documented way to satisfy.
    /// </summary>
    internal static CaseFoldingProviderRules? CaseFoldingProvider(string? providerName) => providerName switch
    {
        not null when providerName.Contains("SqlServer", StringComparison.OrdinalIgnoreCase) => new(
            "SQL Server",
            nameof(AsyncResponseFlowIdCollations.SqlServer),
            AsyncResponseFlowIdCollations.SqlServer,
            "_BIN2 collation",
            static c => c.Contains("_BIN", StringComparison.OrdinalIgnoreCase)),
        not null when providerName.Contains("MySql", StringComparison.OrdinalIgnoreCase)
            || providerName.Contains("Pomelo", StringComparison.OrdinalIgnoreCase) => new(
            "MySQL",
            nameof(AsyncResponseFlowIdCollations.MySql),
            AsyncResponseFlowIdCollations.MySql,
            "_bin collation",
            static c => c.EndsWith("_bin", StringComparison.OrdinalIgnoreCase)),
        _ => null
    };

    /// <summary>One provider's answer to "which collations compare byte-wise, and what to suggest".</summary>
    internal sealed record CaseFoldingProviderRules(
        string Name,
        string ConstantName,
        string Recommended,
        string OrdinalDescription,
        Func<string, bool> IsOrdinal);
}

/// <summary>
/// Well-known case-sensitive collations for the <c>flow_id</c> key column, one per mainstream
/// provider. Pass one to <see cref="EFCoreDurableFlowModelBuilderExtensions.ConfigureAsyncResponseDurableFlows"/>
/// when the database's own collation is case-insensitive — the SQL Server and MySQL defaults are,
/// and under them two flow ids differing only in case collide on the primary key while the engine
/// treats them as two different runs.
/// </summary>
public static class AsyncResponseFlowIdCollations
{
    /// <summary>SQL Server: binary, code-point ordered.</summary>
    public const string SqlServer = "Latin1_General_100_BIN2";

    /// <summary>MySQL / MariaDB: binary comparison over utf8mb4.</summary>
    public const string MySql = "utf8mb4_bin";

    /// <summary>PostgreSQL: the C locale, which compares byte-wise.</summary>
    public const string PostgreSql = "C";

    /// <summary>SQLite: the default, already case-sensitive — named for completeness.</summary>
    public const string Sqlite = "BINARY";
}

/// <summary>Maps the durable-flow state table into an application model.</summary>
public static class EFCoreDurableFlowModelBuilderExtensions
{
    /// <summary>Default durable-flow state table name (shared with the other relational packages).</summary>
    public const string DefaultTableName = "asyncresponse_flow_state";

    /// <summary>
    /// Model annotation recording the <c>flowIdCollation</c> the mapping was configured with. The
    /// store reads it at startup to refuse a case-folding provider left on its default; the
    /// property's own collation cannot be used for that, because EF Core strips relational
    /// configuration the runtime never reads out of the runtime model.
    /// </summary>
    internal const string FlowIdCollationAnnotation = "AsyncResponse:FlowIdCollation";

    /// <summary>
    /// Maps <see cref="DurableFlowStateRecord"/> to the durable-flow state table. Call from
    /// <c>OnModelCreating</c>; the table then flows through the application's normal EF Core
    /// migrations (or <c>EnsureCreated</c>) like any other entity.
    /// </summary>
    /// <param name="modelBuilder">The application model builder.</param>
    /// <param name="tableName">Table name. Default: <see cref="DefaultTableName"/>.</param>
    /// <param name="schema">Optional schema; <c>null</c> uses the provider default.</param>
    /// <param name="flowIdCollation">
    /// Collation for the <c>flow_id</c> key column. Flow ids are compared ORDINALLY by the engine,
    /// so the column must be case-sensitive — but this package runs no DDL and cannot know which
    /// provider the application points at, and both the SQL Server and MySQL defaults are
    /// case-INSENSITIVE, which makes <c>flow-a</c> and <c>FLOW-A</c> one key: the second create
    /// fails as a duplicate and a load returns the other run's state. Pass the matching
    /// <see cref="AsyncResponseFlowIdCollations"/> constant (the sibling PostgreSQL, SQL Server,
    /// and MySQL packages pin this in their own DDL). <c>null</c> keeps the database default.
    /// </param>
    public static ModelBuilder ConfigureAsyncResponseDurableFlows(
        this ModelBuilder modelBuilder,
        string tableName = DefaultTableName,
        string? schema = null,
        string? flowIdCollation = null)
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
            if (!string.IsNullOrWhiteSpace(flowIdCollation))
            {
                entity.Property(r => r.FlowId).UseCollation(flowIdCollation);
                // Also recorded as a model annotation, which survives into the runtime model the
                // store can actually read at startup.
                entity.HasAnnotation(FlowIdCollationAnnotation, flowIdCollation);
            }
            entity.Property(r => r.StateJson).HasColumnName("state_json").IsRequired();
            entity.Property(r => r.ExpiresAtUtc).HasColumnName("expires_at_utc");
            entity.Property(r => r.UpdatedAtUtc).HasColumnName("updated_at_utc");
            entity.Property(r => r.Revision).HasColumnName("revision").HasDefaultValue(0L);
            entity.Property(r => r.LeaseId).HasColumnName("lease_id").HasMaxLength(64);
            entity.Property(r => r.LeaseExpiresAtUtc).HasColumnName("lease_expires_at_utc");
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
public sealed class EFCoreFlowStateStore<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.NonPublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties)] TContext> : IFlowStateStore
    where TContext : DbContext
{
    private const int PruneBatchSize = 1000;

    // Time authority: this store deliberately keeps the app clock (DateTime.UtcNow) for expiry
    // and lease comparisons. It is provider-agnostic LINQ — there is no portable way to reference
    // the database server's clock in a translated expression — so multi-node deployments should
    // either keep worker clocks synchronized well inside the lease window or use one of the
    // provider-specific relational stores, which run all time math on the database clock.
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly EFCoreDurableFlowOptions _options;
    private long _lastPruneTicks;
    private volatile bool _modelChecked;

    public EFCoreFlowStateStore(IServiceScopeFactory scopeFactory, IOptions<EFCoreDurableFlowOptions> options)
    {
        _scopeFactory = scopeFactory;
        _options = options.Value;
        DurableFlowStoreShared.ValidateMaxStateBytes(_options.MaxStateBytes, nameof(EFCoreDurableFlowOptions));
    }

    public async Task<FlowState?> LoadAsync(string flowId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(flowId);
        await using var lease = await LeaseContextAsync(cancellationToken).ConfigureAwait(false);

        var now = DateTime.UtcNow;
        // Named-record projection, not an anonymous type: anonymous projections lower to the
        // RequiresUnreferencedCode Expression.New(ctor, args, members) overload, which ILC trim
        // analysis rejects in Native AOT publishes even though the Roslyn analyzer stays quiet.
        var record = await Records(lease.Context)
            .AsNoTracking()
            .Where(r => r.FlowId == flowId && r.ExpiresAtUtc > now)
            .Select(r => new StateRow(r.StateJson, r.Revision))
            .FirstOrDefaultAsync(cancellationToken)
            .ConfigureAwait(false);

        if (record is null)
            return null;

        return DurableFlowStoreShared.ReadState(flowId, record.StateJson, record.Revision);
    }

    public async Task<bool> TryCreateAsync(string flowId, FlowState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        DurableFlowStoreShared.ValidateCreate(flowId, state, ttl);
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "EF Core");
        await using var lease = await LeaseContextAsync(cancellationToken).ConfigureAwait(false);
        var db = lease.Context;
        var now = DateTime.UtcNow;

        if (DurableFlowStoreShared.ShouldPrune(ref _lastPruneTicks, _options.PruneInterval))
            await PruneExpiredAsync(db, cancellationToken).ConfigureAwait(false);

        // Replace an expired ledger IN PLACE, in one statement (sibling parity: PostgreSQL
        // `ON CONFLICT ... DO UPDATE ... WHERE expired`, SQL Server/Oracle `MERGE ... WHEN MATCHED
        // ... WHERE`). Delete-then-insert spanned two transactions, and a failure between them
        // destroyed the expired row with no replacement. The lease columns are cleared as the
        // siblings clear them: the replaced ledger is a fresh, unleased run.
        var expiresAtUtc = DurableFlowStoreShared.AddSaturating(now, ttl);
        var revision = state.Revision;
        var replaced = await Records(db)
            .Where(r => r.FlowId == flowId && r.ExpiresAtUtc <= now)
            .ExecuteUpdateAsync(setters => setters
                .SetProperty(r => r.StateJson, stateJson)
                .SetProperty(r => r.ExpiresAtUtc, expiresAtUtc)
                .SetProperty(r => r.UpdatedAtUtc, now)
                .SetProperty(r => r.Revision, revision)
                .SetProperty(r => r.LeaseId, (string?)null)
                .SetProperty(r => r.LeaseExpiresAtUtc, (DateTime?)null), cancellationToken)
            .ConfigureAwait(false);
        if (replaced > 0)
            return true;

        db.Add(new DurableFlowStateRecord
        {
            FlowId = flowId,
            StateJson = stateJson,
            ExpiresAtUtc = expiresAtUtc,
            UpdatedAtUtc = now,
            Revision = revision
        });
        try
        {
            await db.SaveChangesAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (DbUpdateException)
        {
            db.ChangeTracker.Clear();
            // Provider-agnostic duplicate-key detection is not reliable. Verify that another
            // creator actually owns this id; otherwise preserve the real database failure instead
            // of misreporting truncation, trigger, permission, or schema errors as "already exists".
            if (await Records(db)
                    .AsNoTracking()
                    .AnyAsync(r => r.FlowId == flowId, cancellationToken)
                    .ConfigureAwait(false))
                return false;

            throw;
        }
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
        var stateJson = DurableFlowStoreShared.SerializeBounded(flowId, state, _options.MaxStateBytes, "EF Core");
        await using var lease = await LeaseContextAsync(cancellationToken).ConfigureAwait(false);
        var now = DateTime.UtcNow;
        var expiresAtUtc = DurableFlowStoreShared.AddSaturating(now, ttl);
        var query = Records(lease.Context).Where(r =>
            r.FlowId == flowId
            && r.Revision == expectedRevision
            && r.ExpiresAtUtc > now
            && (leaseId == null || (r.LeaseId == leaseId && r.LeaseExpiresAtUtc > now)));
        var updated = await query.ExecuteUpdateAsync(setters => setters
                .SetProperty(r => r.StateJson, stateJson)
                .SetProperty(r => r.ExpiresAtUtc, expiresAtUtc)
                .SetProperty(r => r.UpdatedAtUtc, now)
                .SetProperty(r => r.Revision, state.Revision), cancellationToken)
            .ConfigureAwait(false);
        return updated > 0;
    }

    public Task<bool> TryAcquireLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: true, cancellationToken);

    public Task<bool> TryRenewLeaseAsync(string flowId, string leaseId, TimeSpan leaseDuration, CancellationToken cancellationToken = default)
        => UpdateLeaseAsync(flowId, leaseId, leaseDuration, acquire: false, cancellationToken);

    public async Task ReleaseLeaseAsync(string flowId, string leaseId, CancellationToken cancellationToken = default)
    {
        await using var lease = await LeaseContextAsync(cancellationToken).ConfigureAwait(false);
        await Records(lease.Context)
            .Where(r => r.FlowId == flowId && r.LeaseId == leaseId)
            .ExecuteUpdateAsync(setters => setters
                .SetProperty(r => r.LeaseId, (string?)null)
                .SetProperty(r => r.LeaseExpiresAtUtc, (DateTime?)null), cancellationToken)
            .ConfigureAwait(false);
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
        // One bounded batch per prune interval (policy shared by all relational stores): an
        // unbatched delete over a large expired backlog holds row locks and bloats one
        // transaction for the unlucky create that triggered the prune. Loads already filter on
        // expiry, so any backlog beyond the batch just waits for the next interval. The OrderBy
        // makes the row-limited delete deterministic (and keeps providers from warning about an
        // unordered Take).
        var now = DateTime.UtcNow;
        await Records(db)
            .Where(r => r.ExpiresAtUtc <= now)
            .OrderBy(r => r.FlowId)
            .Take(PruneBatchSize)
            .ExecuteDeleteAsync(cancellationToken)
            .ConfigureAwait(false);
    }

    private static DbSet<DurableFlowStateRecord> Records(TContext db) => db.Set<DurableFlowStateRecord>();

    private async Task<bool> UpdateLeaseAsync(
        string flowId,
        string leaseId,
        TimeSpan leaseDuration,
        bool acquire,
        CancellationToken cancellationToken)
    {
        DurableFlowStoreShared.ValidateLeaseArgs(flowId, leaseId, leaseDuration);

        await using var contextLease = await LeaseContextAsync(cancellationToken).ConfigureAwait(false);
        var now = DateTime.UtcNow;
        var leaseExpiresAtUtc = DurableFlowStoreShared.AddSaturating(now, leaseDuration);
        var query = Records(contextLease.Context).Where(r =>
            r.FlowId == flowId
            && r.ExpiresAtUtc > now
            && (acquire
                ? r.LeaseId == null || r.LeaseExpiresAtUtc <= now || r.LeaseId == leaseId
                : r.LeaseId == leaseId && r.LeaseExpiresAtUtc > now));
        var updated = await query.ExecuteUpdateAsync(setters => setters
                .SetProperty(r => r.LeaseId, leaseId)
                .SetProperty(r => r.LeaseExpiresAtUtc, leaseExpiresAtUtc), cancellationToken)
            .ConfigureAwait(false);
        return updated > 0;
    }

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

        var entity = context.Model.FindEntityType(typeof(DurableFlowStateRecord))
            ?? throw new InvalidOperationException(
                $"'{typeof(TContext).Name}' does not map {nameof(DurableFlowStateRecord)}. Call " +
                $"modelBuilder.{nameof(EFCoreDurableFlowModelBuilderExtensions.ConfigureAsyncResponseDurableFlows)}() " +
                "in OnModelCreating and add a migration for the durable-flow state table.");

        // The flow_id column is a KEY the engine compares ordinally. This package owns no DDL, so
        // it cannot pin the collation itself — but it can refuse to run against a mapping that
        // leaves it to a provider whose default folds case. On SQL Server and MySQL that default
        // makes 'flow-a' and 'FLOW-A' one primary key: the second StartAsync fails as a duplicate
        // and a load returns the other run's state. Silence there is not an acceptable default.
        // Read the decision from the annotation the mapping records, not from the property's
        // collation: EF Core strips relational configuration the runtime never reads out of
        // context.Model, and asking a runtime property for its collation throws outright.
        if (FlowIdCollationRules.CaseFoldingProvider(context.Database.ProviderName) is not { } provider)
        {
            _modelChecked = true;
            return;
        }

        var collation = entity.FindAnnotation(EFCoreDurableFlowModelBuilderExtensions.FlowIdCollationAnnotation)?.Value as string;
        if (string.IsNullOrWhiteSpace(collation))
        {
            throw new InvalidOperationException(
                $"'{typeof(TContext).Name}' maps {nameof(DurableFlowStateRecord)}.{nameof(DurableFlowStateRecord.FlowId)} without a " +
                $"collation, and {provider.Name} defaults to a case-insensitive one. Flow ids are compared ordinally, so two ids " +
                "differing only in case would collide on the primary key — the second flow fails to start and a load returns the " +
                $"other run's state. Pass {nameof(AsyncResponseFlowIdCollations)}.{provider.ConstantName} to " +
                $"{nameof(EFCoreDurableFlowModelBuilderExtensions.ConfigureAsyncResponseDurableFlows)}(flowIdCollation: …) and add a " +
                "migration.");
        }

        // A declared collation is a claim, not a proof: "I chose one" and "I chose an ordinal one"
        // are different statements, and only the second is what the primary key needs. On these
        // providers the difference is namable, so name it — Latin1_General_100_CS_AS is a perfectly
        // valid SQL Server collation that still folds full-width forms, and every _CS_AI collation
        // folds accents. Only _BIN/_BIN2 (SQL Server) and _bin (MySQL) compare byte-wise.
        if (!provider.IsOrdinal(collation))
        {
            throw new InvalidOperationException(
                $"'{typeof(TContext).Name}' maps {nameof(DurableFlowStateRecord)}.{nameof(DurableFlowStateRecord.FlowId)} with the " +
                $"collation '{collation}', which {provider.Name} does not compare byte-wise. Case sensitivity alone is not enough: a " +
                "case-sensitive collation still folds accents or full-width forms, so two flow ids the library treats as distinct " +
                "collide on the primary key — the second flow fails to start and a load returns the other run's state. Pass " +
                $"{nameof(AsyncResponseFlowIdCollations)}.{provider.ConstantName} ('{provider.Recommended}') instead, or another " +
                $"{provider.OrdinalDescription}, and add a migration.");
        }

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

    /// <summary>
    /// Ledger-row projection for <see cref="LoadAsync"/>. A named type keeps the LINQ projection
    /// off the anonymous-type Expression.New overload that Native AOT trim analysis rejects.
    /// </summary>
    private sealed record StateRow(string StateJson, long Revision);
}
}
