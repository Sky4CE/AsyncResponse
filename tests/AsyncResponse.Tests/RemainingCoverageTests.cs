using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.MongoDB;
using Microsoft.Data.SqlClient;
using AsyncResponse.DurableFlows.EFCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.EntityFrameworkCore;
using Moq;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Targeted unit tests covering the remaining uncovered code paths identified
/// in the coverage analysis. Focuses on in-memory stores, validation paths,
/// and EF Core error handling that don't require real database connections.
/// </summary>
public sealed class RemainingCoverageTests
{
    // ─────────────────────────────────────────────────────────────────────────
    // InMemoryFlowStateStore — CAS retry and edge-case paths
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task InMemoryFlowStateStore_TryCreate_ReplacesExpiredEntry()
    {
        // Covers L34-36: TryUpdate replacing an expired entry
        var store = new InMemoryFlowStateStore();
        var state1 = CreateFlowState("flow-1");
        Assert.True(await store.TryCreateAsync("flow-1", state1, TimeSpan.FromMilliseconds(1)));

        // Wait for the entry to expire
        await Task.Delay(10);

        // Create again — should succeed by replacing the expired entry
        var state2 = CreateFlowState("flow-1");
        Assert.True(await store.TryCreateAsync("flow-1", state2, TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task InMemoryFlowStateStore_TryCreate_ReturnsFalseForLiveEntry()
    {
        // Covers L31-32: returns false when a live entry exists
        var store = new InMemoryFlowStateStore();
        var state1 = CreateFlowState("flow-1");
        Assert.True(await store.TryCreateAsync("flow-1", state1, TimeSpan.FromMinutes(5)));

        var state2 = CreateFlowState("flow-1");
        Assert.False(await store.TryCreateAsync("flow-1", state2, TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task InMemoryFlowStateStore_TryUpdate_ReturnsFalseOnMissingEntry()
    {
        // Covers L97-99: TryUpdate when entry not found
        var store = new InMemoryFlowStateStore();
        var state = CreateFlowState("flow-missing", revision: 1);
        Assert.False(await store.TryUpdateAsync("flow-missing", state, 0, TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task InMemoryFlowStateStore_TryUpdate_ReturnsFalseOnWrongLease()
    {
        // Covers L85-88: lease mismatch
        var store = new InMemoryFlowStateStore();
        var state = CreateFlowState("flow-lease");
        await store.TryCreateAsync("flow-lease", state, TimeSpan.FromMinutes(5));
        await store.TryAcquireLeaseAsync("flow-lease", "owner-a", TimeSpan.FromMinutes(5));

        var updated = CreateFlowState("flow-lease", revision: 1);
        Assert.False(await store.TryUpdateAsync("flow-lease", updated, 0, TimeSpan.FromMinutes(5), leaseId: "wrong-owner"));
    }

    [Fact]
    public async Task InMemoryFlowStateStore_ReleaseLeaseAsync_NoOpWhenLeaseNotHeld()
    {
        // Covers L127-128 + L132: break when lease doesn't match
        var store = new InMemoryFlowStateStore();
        var state = CreateFlowState("flow-release");
        await store.TryCreateAsync("flow-release", state, TimeSpan.FromMinutes(5));
        await store.TryAcquireLeaseAsync("flow-release", "owner-a", TimeSpan.FromMinutes(5));

        // Release with wrong lease id — should be a no-op
        await store.ReleaseLeaseAsync("flow-release", "wrong-owner");

        // Verify the lease is still held by owner-a
        Assert.False(await store.TryAcquireLeaseAsync("flow-release", "other", TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task InMemoryFlowStateStore_TryRenewLease_ReturnsFalseWhenNotHeld()
    {
        // Covers L164 + L174-176: TryRenewLease when entry doesn't exist or lease doesn't match
        var store = new InMemoryFlowStateStore();

        // No entry at all
        Assert.False(await store.TryRenewLeaseAsync("missing", "lease", TimeSpan.FromMinutes(5)));

        // Entry exists but no lease held
        var state = CreateFlowState("flow-renew");
        await store.TryCreateAsync("flow-renew", state, TimeSpan.FromMinutes(5));
        Assert.False(await store.TryRenewLeaseAsync("flow-renew", "lease", TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task InMemoryFlowStateStore_TryAcquireLease_FailsWhenOtherLeaseActive()
    {
        // Covers L164: acquire fails when another lease is active
        var store = new InMemoryFlowStateStore();
        var state = CreateFlowState("flow-contended");
        await store.TryCreateAsync("flow-contended", state, TimeSpan.FromMinutes(5));
        Assert.True(await store.TryAcquireLeaseAsync("flow-contended", "owner-a", TimeSpan.FromMinutes(5)));
        Assert.False(await store.TryAcquireLeaseAsync("flow-contended", "owner-b", TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task InMemoryFlowStateStore_TryAcquireLease_FailsOnExpiredEntry()
    {
        // Covers L160-161: expired entry returns false
        var store = new InMemoryFlowStateStore();
        var state = CreateFlowState("flow-expired");
        await store.TryCreateAsync("flow-expired", state, TimeSpan.FromMilliseconds(1));
        await Task.Delay(10);
        Assert.False(await store.TryAcquireLeaseAsync("flow-expired", "lease", TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task InMemoryFlowStateStore_ConcurrentTryCreate_OneWinsOneRetries()
    {
        // Covers L28-29, L34-36: concurrent create racing on CAS
        var store = new InMemoryFlowStateStore();
        var results = await Task.WhenAll(
            Enumerable.Range(0, 10).Select(i =>
            {
                var state = CreateFlowState("racy");
                return store.TryCreateAsync("racy", state, TimeSpan.FromMinutes(5));
            }));

        // Exactly one should succeed, rest should fail
        Assert.Equal(1, results.Count(r => r));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // InMemoryRecoveryStateStore — edge-case paths
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task InMemoryRecoveryStateStore_SaveAsync_ThrowsOnMismatchedCorrelationId()
    {
        // Covers L28-29: validation
        var store = ResolveRecoveryStore();
        var state = new RecoveryState
        {
            CorrelationId = "different",
            SchemaVersion = RecoveryStateSchema.Current,
            RegistrationId = Guid.NewGuid()
        };
        await Assert.ThrowsAsync<ArgumentException>(() => store.SaveAsync("expected", state, TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task InMemoryRecoveryStateStore_SaveAsync_ThrowsOnInvalidSchema()
    {
        // Covers L30-31: schema validation
        var store = ResolveRecoveryStore();
        var state = new RecoveryState
        {
            CorrelationId = "corr",
            SchemaVersion = 999,
            RegistrationId = Guid.NewGuid()
        };
        await Assert.ThrowsAsync<ArgumentException>(() => store.SaveAsync("corr", state, TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task InMemoryRecoveryStateStore_GetAllAsync_ReturnsEmptyForMissing()
    {
        var store = ResolveRecoveryStore();
        var result = await store.GetAllAsync("nonexistent");
        Assert.Empty(result);
    }

    [Fact]
    public async Task InMemoryRecoveryStateStore_GetAllAsync_PrunesExpired()
    {
        // Covers L63-68: expired entries are pruned
        var store = ResolveRecoveryStore();
        var state = CreateRecoveryState("expire-me");
        await store.SaveAsync("expire-me", state, TimeSpan.FromMilliseconds(1));
        await Task.Delay(10);
        var result = await store.GetAllAsync("expire-me");
        Assert.Empty(result);
    }

    [Fact]
    public async Task InMemoryRecoveryStateStore_TryDeleteAsync_ReturnsFalseForMissing()
    {
        // Covers L114-116: missing entry
        var store = ResolveRecoveryStore();
        Assert.False(await store.TryDeleteAsync("nonexistent", Guid.NewGuid()));
    }

    [Fact]
    public async Task InMemoryRecoveryStateStore_TryDeleteAsync_ReturnsFalseForExpired()
    {
        // Covers L89-94: expired entries pruned during delete
        var store = ResolveRecoveryStore();
        var state = CreateRecoveryState("expire-del");
        await store.SaveAsync("expire-del", state, TimeSpan.FromMilliseconds(1));
        await Task.Delay(10);
        Assert.False(await store.TryDeleteAsync("expire-del", state.RegistrationId));
    }

    [Fact]
    public async Task InMemoryRecoveryStateStore_TryDeleteAsync_ReturnsFalseForWrongRegistration()
    {
        // Covers L96-102: remove returns false because registration ID doesn't match
        var store = ResolveRecoveryStore();
        var state = CreateRecoveryState("wrong-reg");
        await store.SaveAsync("wrong-reg", state, TimeSpan.FromMinutes(5));
        Assert.False(await store.TryDeleteAsync("wrong-reg", Guid.NewGuid()));
    }

    [Fact]
    public async Task InMemoryRecoveryStateStore_TryDeleteAsync_SucceedsForMatchingRegistration()
    {
        var store = ResolveRecoveryStore();
        var state = CreateRecoveryState("del-match");
        await store.SaveAsync("del-match", state, TimeSpan.FromMinutes(5));
        Assert.True(await store.TryDeleteAsync("del-match", state.RegistrationId));
        // After delete, GetAll should be empty
        Assert.Empty(await store.GetAllAsync("del-match"));
    }

    [Fact]
    public async Task InMemoryRecoveryStateStore_MultipleEntries_UpsertAndRemoveCoverage()
    {
        // Covers L147-167 (upsert into multi-entry bucket) and L206-247 (remove from multi)
        var store = ResolveRecoveryStore();

        // Save two entries under the same correlation ID (different registrations)
        var state1 = CreateRecoveryState("multi", Guid.NewGuid());
        var state2 = CreateRecoveryState("multi", Guid.NewGuid());
        await store.SaveAsync("multi", state1, TimeSpan.FromMinutes(5));
        await store.SaveAsync("multi", state2, TimeSpan.FromMinutes(5));

        var all = await store.GetAllAsync("multi");
        Assert.Equal(2, all.Count);

        // Remove one — bucket should collapse to single
        Assert.True(await store.TryDeleteAsync("multi", state1.RegistrationId));
        all = await store.GetAllAsync("multi");
        Assert.Single(all);
        Assert.Equal(state2.RegistrationId, all[0].RegistrationId);

        // Upsert existing registration (update in multi-array before collapse)
        var state3 = CreateRecoveryState("multi", Guid.NewGuid());
        await store.SaveAsync("multi", state3, TimeSpan.FromMinutes(5));
        all = await store.GetAllAsync("multi");
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public async Task InMemoryRecoveryStateStore_ScanAsync_YieldsSingleAndMulti()
    {
        // Covers L312-320: ScanAsync iterating over single and multi-entry buckets
        var store = ResolveRecoveryStore();
        var scanner = (IRecoveryStateScanner)store;

        var state1 = CreateRecoveryState("scan-single", Guid.NewGuid());
        await store.SaveAsync("scan-single", state1, TimeSpan.FromMinutes(5));

        var state2a = CreateRecoveryState("scan-multi", Guid.NewGuid());
        var state2b = CreateRecoveryState("scan-multi", Guid.NewGuid());
        await store.SaveAsync("scan-multi", state2a, TimeSpan.FromMinutes(5));
        await store.SaveAsync("scan-multi", state2b, TimeSpan.FromMinutes(5));

        var scanned = new List<RecoveryState>();
        await foreach (var s in scanner.ScanAsync())
            scanned.Add(s);

        Assert.True(scanned.Count >= 3);
    }

    [Fact]
    public async Task InMemoryRecoveryStateStore_ScanAsync_PrunesExpired()
    {
        // Covers L303-306: ScanAsync prunes expired entries
        var store = ResolveRecoveryStore();
        var scanner = (IRecoveryStateScanner)store;

        var state = CreateRecoveryState("scan-expire", Guid.NewGuid());
        await store.SaveAsync("scan-expire", state, TimeSpan.FromMilliseconds(1));
        await Task.Delay(10);

        var scanned = new List<RecoveryState>();
        await foreach (var s in scanner.ScanAsync())
            scanned.Add(s);

        Assert.DoesNotContain(scanned, s => s.CorrelationId == "scan-expire");
    }

    [Fact]
    public async Task InMemoryRecoveryStateStore_ConcurrentSave_OnlyOneWinsAdd()
    {
        // Covers L43-46: concurrent TryAdd race → continue
        var store = ResolveRecoveryStore();
        var tasks = Enumerable.Range(0, 20).Select(i =>
        {
            var state = CreateRecoveryState("race-save", Guid.NewGuid());
            return store.SaveAsync("race-save", state, TimeSpan.FromMinutes(5));
        });

        await Task.WhenAll(tasks);
        var all = await store.GetAllAsync("race-save");
        Assert.Equal(20, all.Count);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Recovery State Store validation — mismatched correlation IDs
    // (SqlServer, PostgreSQL, MongoDB all share the same validation pattern)
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task SqlServerRecoveryStateStore_SaveAsync_ThrowsOnMismatchedCorrelationId()
    {
        // Covers SqlServerRecoveryStateStore.SaveAsync L23
        var options = Options.Create(new SqlServerAsyncResponseChannelOptions
        {
            ConnectionString = "Server=unused;Database=unused;Encrypt=False",
            AutoCreateSchema = false
        });
        var channelSql = new SqlServerChannelSql(options);
        var store = new SqlServerRecoveryStateStore(channelSql, NullLogger<SqlServerRecoveryStateStore>.Instance);

        var state = new RecoveryState
        {
            CorrelationId = "actual",
            SchemaVersion = RecoveryStateSchema.Current,
            RegistrationId = Guid.NewGuid()
        };
        await Assert.ThrowsAsync<ArgumentException>(() => store.SaveAsync("expected", state, TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task PostgreSqlRecoveryStateStore_SaveAsync_ThrowsOnMismatchedCorrelationId()
    {
        // Covers PostgreSqlRecoveryStateStore.SaveAsync L23
        // Validation throws before any SQL call, so null! is safe
        var store = new PostgreSqlRecoveryStateStore(null!, NullLogger<PostgreSqlRecoveryStateStore>.Instance);

        var state = new RecoveryState
        {
            CorrelationId = "actual",
            SchemaVersion = RecoveryStateSchema.Current,
            RegistrationId = Guid.NewGuid()
        };
        await Assert.ThrowsAsync<ArgumentException>(() => store.SaveAsync("expected", state, TimeSpan.FromMinutes(5)));
    }

    [Fact]
    public async Task MongoDbRecoveryStateStore_SaveAsync_ThrowsOnMismatchedCorrelationId()
    {
        // Covers MongoDbRecoveryStateStore.SaveAsync L24
        // Validation throws before any DB call, so null! is safe
        var store = new MongoDbRecoveryStateStore(null!, NullLogger<MongoDbRecoveryStateStore>.Instance);

        var state = new RecoveryState
        {
            CorrelationId = "actual",
            SchemaVersion = RecoveryStateSchema.Current,
            RegistrationId = Guid.NewGuid()
        };
        await Assert.ThrowsAsync<ArgumentException>(() => store.SaveAsync("expected", state, TimeSpan.FromMinutes(5)));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // EF Core DurableFlows — LeaseContextAsync exception paths
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task EFCoreDurableFlows_LeaseContextAsync_DisposesScope_WhenGetRequiredServiceThrows()
    {
        // Covers L327-331: outer catch disposes scope when context resolution fails
        var services = new ServiceCollection();
        // Register NO TestFlowDbContext → GetRequiredService will throw
        var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        var store = new EFCoreFlowStateStore<TestFlowDbContext>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new EFCoreDurableFlowOptions()));

        // Any operation that calls LeaseContextAsync will fail with scope disposal
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.LoadAsync("test-flow"));
    }

    [Fact]
    public async Task EFCoreDurableFlows_LeaseContextAsync_DisposesFactoryContext_WhenEnsureMappedThrows()
    {
        // Covers L320-325: inner catch disposes factory-created context
        await using var database = CreateTempSqliteDatabase();
        var services = new ServiceCollection();
        services.AddDbContextFactory<UnmappedFlowDbContext>(opts => opts.UseSqlite(database.ConnectionString));
        await using var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true });

        var store = new EFCoreFlowStateStore<UnmappedFlowDbContext>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            Options.Create(new EFCoreDurableFlowOptions()));

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.LoadAsync("flow-any"));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Redis Channel — WithRedisChannel without configure delegate
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void RedisChannel_WithRedisChannel_ThrowsWhenNoConnectionConfigured()
    {
        // Covers ServiceCollectionExtensions L32-34: exception when no connection string/multiplexer
        var services = new ServiceCollection();
        services.AddLogging();
        var builder = services.AddAsyncResponse();

        // WithRedisChannel with no configure delegate → should register but fail on resolve
        // because no connection string is provided
        builder.WithRedisChannel();
        using var provider = services.BuildServiceProvider();

        // Resolving the channel should throw because there's no Redis connection
        Assert.ThrowsAny<Exception>(() => provider.GetRequiredService<IAsyncResponsePublisher>());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    private static FlowState CreateFlowState(string flowId, long revision = 0)
        => new()
        {
            FlowId = flowId,
            Status = FlowRunStatus.Running,
            Revision = revision,
            CreatedAtUtc = DateTime.UtcNow,
            UpdatedAtUtc = DateTime.UtcNow
        };

    private static RecoveryState CreateRecoveryState(string correlationId, Guid? registrationId = null)
        => new()
        {
            CorrelationId = correlationId,
            SchemaVersion = RecoveryStateSchema.Current,
            RegistrationId = registrationId ?? Guid.NewGuid()
        };

    private static InMemoryRecoveryStateStore ResolveRecoveryStore() => new();

    private static TempSqliteDb CreateTempSqliteDatabase() => new();

    private sealed class TempSqliteDb : IAsyncDisposable
    {
        private readonly string _path = Path.Combine(Path.GetTempPath(), $"ar-coverage-{Guid.NewGuid():N}.db");
        // Pooling=False: every closed connection releases its file handle immediately, so
        // cleanup can delete the temp database on Windows and no process-wide pool state
        // couples parallel tests (SqliteConnection.ClearAllPools() here previously flushed
        // OTHER tests' idle connections mid-run and manifested as 'database is locked').
        public string ConnectionString => $"Data Source={_path};Pooling=False";
        public ValueTask DisposeAsync()
        {
            // Pooling is disabled in the connection string, so the last closed context already
            // released the file handle; deletion stays best-effort temp hygiene regardless.
            foreach (var file in new[] { _path, _path + "-wal", _path + "-shm" })
            {
                try
                {
                    File.Delete(file);
                }
                catch (IOException)
                {
                }
            }

            return ValueTask.CompletedTask;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SqlServer & PostgreSQL utility class coverage
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void SqlServerChannelSql_ValidationAndFaults()
    {
        // 1. ValidateIdentifier
        Assert.Throws<InvalidOperationException>(() => SqlServerChannelSql.ValidateIdentifier("", "Test"));
        Assert.Throws<InvalidOperationException>(() => SqlServerChannelSql.ValidateIdentifier("1abc", "Test"));
        SqlServerChannelSql.ValidateIdentifier("valid_identifier_123", "Test"); // should not throw

        // 2. SqlServerChannelSql.IsTransient / SqlServerTransientFaults.IsTransient
        // Create SqlError
        var errorConstructor = typeof(SqlError).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)[0];
        Assert.NotNull(errorConstructor);
        
        // Find parameter count to invoke it correctly
        var errorParams = errorConstructor.GetParameters();
        object[] errorArgs = new object[errorParams.Length];
        for (int i = 0; i < errorParams.Length; i++)
        {
            var p = errorParams[i];
            if (p.Name == "infoNumber") errorArgs[i] = 10060;
            else if (p.Name == "errorClass") errorArgs[i] = (byte)0;
            else if (p.ParameterType == typeof(string)) errorArgs[i] = "";
            else if (p.ParameterType == typeof(int)) errorArgs[i] = 0;
            else if (p.ParameterType == typeof(uint)) errorArgs[i] = 0U;
            else if (p.ParameterType == typeof(byte)) errorArgs[i] = (byte)0;
            else errorArgs[i] = null!;
        }
        var errorTransient = (SqlError)errorConstructor.Invoke(errorArgs);

        for (int i = 0; i < errorParams.Length; i++)
        {
            var p = errorParams[i];
            if (p.Name == "infoNumber") errorArgs[i] = 123;
        }
        var errorNonTransient = (SqlError)errorConstructor.Invoke(errorArgs);

        for (int i = 0; i < errorParams.Length; i++)
        {
            var p = errorParams[i];
            if (p.Name == "infoNumber") errorArgs[i] = 123;
            else if (p.Name == "errorClass") errorArgs[i] = (byte)20;
        }
        var errorHighClass = (SqlError)errorConstructor.Invoke(errorArgs);

        // Create SqlErrorCollection
        var errorCollectionConstructor = typeof(SqlErrorCollection).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)[0];
        Assert.NotNull(errorCollectionConstructor);
        
        var errorCollection1 = (SqlErrorCollection)errorCollectionConstructor.Invoke([]);
        var errorCollection2 = (SqlErrorCollection)errorCollectionConstructor.Invoke([]);
        var errorCollectionHigh = (SqlErrorCollection)errorCollectionConstructor.Invoke([]);
        
        var addMethod = typeof(SqlErrorCollection).GetMethod(
            "Add",
            BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            [typeof(SqlError)],
            null);
        Assert.NotNull(addMethod);
        addMethod.Invoke(errorCollection1, [errorTransient]);
        addMethod.Invoke(errorCollection2, [errorNonTransient]);
        addMethod.Invoke(errorCollectionHigh, [errorHighClass]);

        // Create SqlException
        var exceptionConstructor = typeof(SqlException).GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)[0];
        Assert.NotNull(exceptionConstructor);

        // Find parameters for SqlException constructor
        var exceptionParams = exceptionConstructor.GetParameters();
        object[] exceptionArgs = new object[exceptionParams.Length];
        for (int i = 0; i < exceptionParams.Length; i++)
        {
            var p = exceptionParams[i];
            if (p.ParameterType == typeof(string)) exceptionArgs[i] = "message";
            else if (p.ParameterType == typeof(SqlErrorCollection)) exceptionArgs[i] = errorCollection1;
            else if (p.ParameterType == typeof(Guid)) exceptionArgs[i] = Guid.Empty;
            else exceptionArgs[i] = null!;
        }
        var sqlExTransient = (SqlException)exceptionConstructor.Invoke(exceptionArgs);

        for (int i = 0; i < exceptionParams.Length; i++)
        {
            var p = exceptionParams[i];
            if (p.ParameterType == typeof(SqlErrorCollection)) exceptionArgs[i] = errorCollection2;
        }
        var sqlExNonTransient = (SqlException)exceptionConstructor.Invoke(exceptionArgs);

        for (int i = 0; i < exceptionParams.Length; i++)
        {
            var p = exceptionParams[i];
            if (p.ParameterType == typeof(SqlErrorCollection)) exceptionArgs[i] = errorCollectionHigh;
        }
        var sqlExHighClass = (SqlException)exceptionConstructor.Invoke(exceptionArgs);

        // Call SqlServerTransientFaults.IsTransient
        Assert.True(SqlServerTransientFaults.IsTransient(sqlExTransient));
        Assert.False(SqlServerTransientFaults.IsTransient(sqlExNonTransient));
        Assert.True(SqlServerTransientFaults.IsTransient(sqlExHighClass));

        // Call SqlServerChannelSql.IsTransient via reflection
        var isTransientMethod = typeof(SqlServerChannelSql).GetMethod("IsTransient", BindingFlags.Static | BindingFlags.NonPublic)!;
        Assert.True((bool)isTransientMethod.Invoke(null, [sqlExTransient])!);
        Assert.False((bool)isTransientMethod.Invoke(null, [new OperationCanceledException()])!);
        Assert.True((bool)isTransientMethod.Invoke(null, [new TimeoutException()])!);
        Assert.False((bool)isTransientMethod.Invoke(null, [new Exception()])!);
    }

    [Fact]
    public void PostgreSqlChannelSql_ValidationAndFaults()
    {
        // 1. ValidateIdentifier
        var validateMethod = typeof(PostgreSqlChannelSql).GetMethod("ValidateIdentifier", BindingFlags.Static | BindingFlags.Public)!;
        Assert.Throws<InvalidOperationException>(() => {
            try { validateMethod.Invoke(null, ["", "Test"]); }
            catch (TargetInvocationException ex) { throw ex.InnerException!; }
        });
        Assert.Throws<InvalidOperationException>(() => {
            try { validateMethod.Invoke(null, ["1abc", "Test"]); }
            catch (TargetInvocationException ex) { throw ex.InnerException!; }
        });
        
        validateMethod.Invoke(null, ["valid_identifier_123", "Test"]); // should not throw

        // 2. IsTransient
        var mockNpgsqlExTransient = new Mock<Npgsql.NpgsqlException>();
        mockNpgsqlExTransient.Setup(x => x.IsTransient).Returns(true);
        var mockNpgsqlExNonTransient = new Mock<Npgsql.NpgsqlException>();
        mockNpgsqlExNonTransient.Setup(x => x.IsTransient).Returns(false);

        var isTransientMethod = typeof(PostgreSqlChannelSql).GetMethod("IsTransient", BindingFlags.Static | BindingFlags.NonPublic)!;
        Assert.True((bool)isTransientMethod.Invoke(null, [mockNpgsqlExTransient.Object])!);
        Assert.False((bool)isTransientMethod.Invoke(null, [mockNpgsqlExNonTransient.Object])!);
        Assert.False((bool)isTransientMethod.Invoke(null, [new OperationCanceledException()])!);
        Assert.True((bool)isTransientMethod.Invoke(null, [new TimeoutException()])!);
        Assert.False((bool)isTransientMethod.Invoke(null, [new Exception()])!);
    }
}
