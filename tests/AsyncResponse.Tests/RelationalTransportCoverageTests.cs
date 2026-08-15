using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Npgsql;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class RelationalTransportCoverageTests
{
    [Fact]
    public async Task SqlServerTransport_PublishDeadLetterAndSubscriberFailuresAreContainedOrPropagated()
    {
        var configured = Options.Create(new SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = "Server=tcp:127.0.0.1,1;Database=none;User ID=sa;Password=unused;Encrypt=False;Connect Timeout=1",
            AutoCreateSchema = false,
            PublishMaxAttempts = 1
        });
        var store = new SqlServerTransportStore(configured);
        var transport = new SqlServerWorkerTransport(configured, store);

        await Assert.ThrowsAnyAsync<Exception>(() => transport.PublishAsync(Job()));
        Assert.False(await InvokeDeadLetterAsync(store, deleteOriginal: false));
        Assert.False(await InvokeDeadLetterAsync(store, deleteOriginal: true));

        var ingress = new Mock<IAsyncResponseIngress>();
        ingress.Setup(instance => instance.HandleWorkerMessageAsync(It.IsAny<string>()))
            .ThrowsAsync(new InvalidOperationException("worker failed"));
        ingress.Setup(instance => instance.HandleResponseMessageAsync(It.IsAny<string>(), It.IsAny<string?>()))
            .ThrowsAsync(new InvalidOperationException("response failed"));
        var delivery = new SqlServerTransportDelivery(
            Guid.NewGuid(),
            "worker",
            "{}",
            new Dictionary<string, string> { [configured.Value.CorrelationIdHeader] = "corr" },
            1,
            () => ValueTask.CompletedTask,
            _ => ValueTask.CompletedTask,
            (_, _, _) => new ValueTask<bool>(true),
            () => new ValueTask<bool>(true));
        var worker = new SqlServerWorkerSubscriber(
            configured,
            store,
            ingress.Object,
            NullLogger<SqlServerWorkerSubscriber>.Instance);
        var response = new SqlServerResponseIngressSubscriber(
            configured,
            store,
            ingress.Object,
            NullLogger<SqlServerResponseIngressSubscriber>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeHandleAsync(worker, delivery));
        await Assert.ThrowsAsync<InvalidOperationException>(() => InvokeHandleAsync(response, delivery));
    }

    [Fact]
    public async Task PostgreSqlTransport_PublishAndDeadLetterFailuresAreContainedOrPropagated()
    {
        await using var dataSource = NpgsqlDataSource.Create(
            "Host=127.0.0.1;Port=1;Username=unused;Password=unused;Database=none;Timeout=1");
        var configured = Options.Create(new PostgreSqlAsyncResponseTransportOptions
        {
            AutoCreateSchema = false,
            PublishMaxAttempts = 1
        });
        var store = new PostgreSqlTransportStore(dataSource, configured);
        var transport = new PostgreSqlWorkerTransport(configured, store);

        await Assert.ThrowsAnyAsync<Exception>(() => transport.PublishAsync(Job()));
        Assert.False(await InvokeDeadLetterAsync(store, deleteOriginal: false));
        Assert.False(await InvokeDeadLetterAsync(store, deleteOriginal: true));
    }

    private static async Task<bool> InvokeDeadLetterAsync(object store, bool deleteOriginal)
    {
        var method = store.GetType().GetMethod("DeadLetterAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        return await (ValueTask<bool>)method.Invoke(
            store,
            [
                Guid.NewGuid(),
                Guid.NewGuid(),
                "worker",
                "{}",
                new Dictionary<string, string>(),
                new InvalidOperationException("dead letter failed"),
                deleteOriginal,
                CancellationToken.None
            ])!;
    }

    private static Task InvokeHandleAsync(object subscriber, SqlServerTransportDelivery delivery)
        => (Task)subscriber.GetType()
            .GetMethod("HandleMessageAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .Invoke(subscriber, [delivery, CancellationToken.None])!;

    private static WorkerJobEnvelope Job() => new()
    {
        Call = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "Service",
            MethodName = "Method",
            Params = []
        }
    };

    [Fact]
    public async Task RelationalTransport_ExceptionCoverage()
    {
        // 1. SQL Server Transport Store EnsureCreatedAsync and InsertAsync exceptions
        var sqlOptions = Options.Create(new SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = "Server=localhost,1;Database=unused;User Id=sa;Password=unused;Encrypt=False;Connect Timeout=1",
            AutoCreateSchema = true
        });
        var sqlStore = new SqlServerTransportStore(sqlOptions);
        await Assert.ThrowsAnyAsync<Exception>(() => sqlStore.EnsureCreatedAsync());

        sqlStore.GetType().GetField("_created", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(sqlStore, true);
        var insertMethod = sqlStore.GetType().GetMethod("InsertAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await Assert.ThrowsAnyAsync<Exception>(() => (Task)insertMethod.Invoke(sqlStore, [Guid.NewGuid(), "worker", "{}", new Dictionary<string, string>(), null, false, CancellationToken.None])!);

        // 2. PostgreSQL Transport Store EnsureCreatedAsync exception
        await using var dataSource = NpgsqlDataSource.Create(
            "Host=localhost;Port=1;Database=unused;Username=unused;Password=unused;Timeout=1;Pooling=false");
        var pgOptions = Options.Create(new PostgreSqlAsyncResponseTransportOptions
        {
            AutoCreateSchema = true
        });
        var pgStore = new PostgreSqlTransportStore(dataSource, pgOptions);
        await Assert.ThrowsAnyAsync<Exception>(() => pgStore.EnsureCreatedAsync());
    }

    /// <summary>
    /// AutoCreateSchema=false is a managed-schema contract, not a no-op: the store must verify the
    /// operator-provisioned queue table (its channel siblings always did) instead of trusting it
    /// blind. Against an unreachable server that verification now surfaces the connection failure —
    /// previously EnsureCreated returned silently having checked nothing — and it must NOT latch,
    /// so the next call re-verifies once the server (or the migration) is there.
    /// </summary>
    [Fact]
    public async Task ManagedSchema_TransportStores_VerifyInsteadOfSilentlySkipping()
    {
        var sqlOptions = Options.Create(new SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = "Server=tcp:127.0.0.1,1;Database=none;User ID=sa;Password=unused;Encrypt=False;Connect Timeout=1",
            AutoCreateSchema = false
        });
        var sqlStore = new SqlServerTransportStore(sqlOptions);
        await Assert.ThrowsAnyAsync<Exception>(() => sqlStore.EnsureCreatedAsync());
        Assert.False(Created(sqlStore));
        await Assert.ThrowsAnyAsync<Exception>(() => sqlStore.EnsureCreatedAsync());

        await using var dataSource = NpgsqlDataSource.Create(
            "Host=127.0.0.1;Port=1;Username=unused;Password=unused;Database=none;Timeout=1;Pooling=false");
        var pgOptions = Options.Create(new PostgreSqlAsyncResponseTransportOptions { AutoCreateSchema = false });
        var pgStore = new PostgreSqlTransportStore(dataSource, pgOptions);
        await Assert.ThrowsAnyAsync<Exception>(() => pgStore.EnsureCreatedAsync());
        Assert.False(Created(pgStore));
        await Assert.ThrowsAnyAsync<Exception>(() => pgStore.EnsureCreatedAsync());
    }

    private static bool Created(object store)
        => (bool)store.GetType().GetField("_created", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(store)!;

    [Fact]
    public async Task PostgreSqlWorkerSubscriber_InvalidOptions_FailHostStartupSynchronously()
    {
        // Red-on-old (Hosting 10.0.10+): validation used to sit at the top of ExecuteAsync, which
        // BackgroundService.StartAsync no longer runs inline — StartAsync returned without
        // throwing and the misconfiguration surfaced late or never. Validation now runs in
        // StartAsync so a misconfigured subscriber fails host startup synchronously.
        var options = Options.Create(new PostgreSqlAsyncResponseTransportOptions
        {
            WorkerSubscriber = { AckMode = PostgreSqlAckMode.AckAfterEnqueue }
        });
        // Creating a data source opens no connection; validation throws before the store is touched.
        await using var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=unused;Password=unused;Database=unused");
        var store = new PostgreSqlTransportStore(dataSource, options);
        var subscriber = new PostgreSqlWorkerSubscriber(options, store, Mock.Of<IAsyncResponseIngress>(), NullLogger<PostgreSqlWorkerSubscriber>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.StartAsync(CancellationToken.None));
        Assert.Contains("BackgroundWorkerCount", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SqlServerWorkerSubscriber_InvalidOptions_FailHostStartupSynchronously()
    {
        // Same contract as the PostgreSQL fact above: a misconfigured subscriber must fail host
        // startup synchronously from StartAsync.
        var options = Options.Create(new SqlServerAsyncResponseTransportOptions
        {
            // Validated as present but never opened: validation throws before the store is touched.
            ConnectionString = "Server=localhost;Database=unused;User Id=unused;Password=unused;TrustServerCertificate=true",
            WorkerSubscriber = { AckMode = SqlServerAckMode.AckAfterEnqueue }
        });
        var store = new SqlServerTransportStore(options);
        var subscriber = new SqlServerWorkerSubscriber(options, store, Mock.Of<IAsyncResponseIngress>(), NullLogger<SqlServerWorkerSubscriber>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.StartAsync(CancellationToken.None));
        Assert.Contains("BackgroundWorkerCount", ex.Message, StringComparison.Ordinal);
    }
}
