using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Moq;
using Npgsql;
using System.Reflection;
using System.Threading.Channels;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The polling DB transports' empty-queue wait (<c>WaitForSignalOrDelayAsync</c>) races a
/// <c>Task.Delay</c> against a <c>WaitToReadAsync</c> on the private bounded(1) wake-signal
/// channel and must cancel the WhenAny loser via its per-iteration linked token source.
/// Regression (review fix): the loser was simply abandoned, so every empty poll on an idle queue
/// parked one more reader in the signal channel's waiting-reader list — unbounded growth for as
/// long as the queue stayed empty. The wait method is invoked here exactly as the subscriber loop
/// invokes it, once per empty poll (the loop-to-wait mapping itself is pinned by
/// <see cref="MongoDbSubscriberServiceTests"/>); the PostgreSQL/SQL Server stores are sealed over
/// real connections, so driving the claim loop end-to-end is a container test, not a unit test.
/// The parked-reader count is read via reflection over the BCL channel internals — verified
/// against both net8.0 (<c>_waitingReadersTail</c>, circular singly-linked via <c>Next</c>) and
/// net10.0 (<c>_waitingReadersHead</c>, doubly-linked, cancelled nodes unlinked eagerly) — and the
/// helpers fail loudly with the discovered member names if a future BCL rename breaks the probe,
/// so it can never silently pass.
/// </summary>
public sealed class TransportSubscriberSignalWaiterTests
{
    private const int EmptyPolls = 45;
    private static readonly TimeSpan EmptyPollDelay = TimeSpan.FromMilliseconds(5);

    [Theory]
    [InlineData("postgresql")]
    [InlineData("sqlserver")]
    [InlineData("mongodb")]
    public async Task EmptyPollWait_DoesNotAccumulateParkedSignalReaders(string transport)
    {
        // The subscriber's stopping token: alive for the whole test, exactly like a hosted
        // service's token across empty polls. Cancelled in the finally so any waiters a broken
        // implementation leaked complete before disposal instead of outliving the test.
        using var lifetime = new CancellationTokenSource();
        var (service, resource) = CreateWorkerSubscriber(transport);
        try
        {
            var wait = ResolveWaitMethod(service);
            for (var i = 0; i < EmptyPolls; i++)
                await (Task)wait.Invoke(service.Instance, [lifetime.Token])!;

            var parked = CountParkedSignalReaders(service);
            Assert.True(
                parked <= 2,
                $"{transport}: {parked} WaitToReadAsync readers are parked in the subscriber's signal channel " +
                $"after {EmptyPolls} empty polls — the WhenAny loser is being abandoned instead of cancelled.");
        }
        finally
        {
            lifetime.Cancel();
            service.Dispose();
            resource?.Dispose();
        }
    }

    // ---------------------------------------------------------------------------------------

    private static (BackgroundServiceHandle Service, IDisposable? Resource) CreateWorkerSubscriber(string transport)
    {
        switch (transport)
        {
            case "postgresql":
            {
                var options = Options.Create(new PostgreSqlAsyncResponseTransportOptions
                {
                    WorkerSubscriber = { EmptyPollDelay = EmptyPollDelay }
                });
                // Creating a data source opens no connection; the wait method never touches the store.
                var dataSource = NpgsqlDataSource.Create("Host=localhost;Username=unused;Password=unused;Database=unused");
                var store = new PostgreSqlTransportStore(dataSource, options);
                var service = new PostgreSqlWorkerSubscriber(
                    options, store, Mock.Of<IAsyncResponseIngress>(), NullLogger<PostgreSqlWorkerSubscriber>.Instance);
                return (new BackgroundServiceHandle(service), dataSource);
            }

            case "sqlserver":
            {
                var options = Options.Create(new SqlServerAsyncResponseTransportOptions
                {
                    // Validated as present but never opened: the wait method never touches the store.
                    ConnectionString = "Server=localhost;Database=unused;User Id=unused;Password=unused;TrustServerCertificate=true",
                    WorkerSubscriber = { EmptyPollDelay = EmptyPollDelay }
                });
                var store = new SqlServerTransportStore(options);
                var service = new SqlServerWorkerSubscriber(
                    options, store, Mock.Of<IAsyncResponseIngress>(), NullLogger<SqlServerWorkerSubscriber>.Instance);
                return (new BackgroundServiceHandle(service), null);
            }

            case "mongodb":
            {
                var options = Options.Create(new MongoDbAsyncResponseTransportOptions
                {
                    AutoCreateIndexes = false,
                    WorkerSubscriber = { EmptyPollDelay = EmptyPollDelay }
                });
                var database = new Mock<IMongoDatabase>(MockBehavior.Loose);
                database
                    .Setup(d => d.GetCollection<MongoTransportMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
                    .Returns(new Mock<IMongoCollection<MongoTransportMessageDocument>>(MockBehavior.Loose).Object);
                database.WithTestNamespace();
                var store = new MongoDbTransportStore(database.Object, options);
                var service = new MongoDbWorkerSubscriber(
                    options, store, Mock.Of<IAsyncResponseIngress>(), NullLogger<MongoDbWorkerSubscriber>.Instance);
                return (new BackgroundServiceHandle(service), null);
            }

            default:
                Assert.Fail($"Unknown transport case '{transport}'.");
                throw new InvalidOperationException(); // unreachable
        }
    }

    /// <summary>Erases the three unrelated subscriber base types behind one dispose/reflect surface.</summary>
    private sealed record BackgroundServiceHandle(object Instance)
    {
        public void Dispose() => (Instance as IDisposable)?.Dispose();
    }

    private static MethodInfo ResolveWaitMethod(BackgroundServiceHandle service)
    {
        for (var type = service.Instance.GetType(); type is not null; type = type.BaseType)
        {
            var method = type.GetMethod(
                "WaitForSignalOrDelayAsync",
                BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly);
            if (method is not null)
                return method;
        }

        Assert.Fail(
            $"No private WaitForSignalOrDelayAsync on {service.Instance.GetType()} or its bases — " +
            "the subscriber's empty-poll wait moved; update this test alongside it.");
        throw new InvalidOperationException(); // unreachable
    }

    /// <summary>
    /// Counts the still-PENDING readers parked in the service's private <c>_signals</c> channel.
    /// Completed nodes are excluded deliberately: on net8.0 a cancelled waiter stays linked until
    /// the next wake (the BCL unlinks lazily), so the raw chain length does not distinguish the
    /// fixed code from the leak — only operations still awaiting a signal do.
    /// </summary>
    private static int CountParkedSignalReaders(BackgroundServiceHandle service)
    {
        var signals = ReadSignalsChannel(service);
        var channelType = signals.GetType();
        var waitingField = channelType
            .GetFields(BindingFlags.NonPublic | BindingFlags.Instance)
            .FirstOrDefault(field => field.Name.Contains("waitingReaders", StringComparison.OrdinalIgnoreCase));
        if (waitingField is null)
        {
            Assert.Fail(
                $"No *waitingReaders* field on {channelType} — the BCL bounded-channel internals changed. " +
                $"Fields found: {string.Join(", ", channelType.GetFields(BindingFlags.NonPublic | BindingFlags.Instance).Select(field => field.Name))}.");
            throw new InvalidOperationException(); // unreachable
        }

        var entry = waitingField.GetValue(signals);
        if (entry is null)
            return 0;

        // net8.0: a circular singly-linked list entered at its tail. net10.0: a doubly-linked
        // list entered at its head. Walking Next from the entry node until null or a revisit
        // counts every node exactly once in either shape.
        var seen = new HashSet<object>(ReferenceEqualityComparer.Instance);
        var parked = 0;
        for (var node = entry; node is not null && seen.Add(node); node = ReadMember(node, "Next"))
        {
            if (!(bool)ReadRequiredMember(node, "IsCompleted"))
                parked++;
        }

        return parked;
    }

    private static Channel<bool> ReadSignalsChannel(BackgroundServiceHandle service)
    {
        for (var type = service.Instance.GetType(); type is not null; type = type.BaseType)
        {
            var field = type.GetField("_signals", BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly);
            if (field is not null)
                return Assert.IsAssignableFrom<Channel<bool>>(field.GetValue(service.Instance));
        }

        Assert.Fail(
            $"No private _signals field on {service.Instance.GetType()} or its bases — " +
            "the subscriber's wake-signal channel moved; update this test alongside it.");
        throw new InvalidOperationException(); // unreachable
    }

    private static object? ReadMember(object instance, string name)
    {
        for (var type = instance.GetType(); type is not null; type = type.BaseType)
        {
            var property = type.GetProperty(name, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
            if (property is not null)
                return property.GetValue(instance);
        }

        return null;
    }

    private static object ReadRequiredMember(object instance, string name)
    {
        for (var type = instance.GetType(); type is not null; type = type.BaseType)
        {
            var property = type.GetProperty(name, BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
            if (property is not null)
                return property.GetValue(instance)!;
        }

        Assert.Fail(
            $"No {name} member on {instance.GetType()} — the BCL async-operation internals changed. " +
            $"Properties found: {string.Join(", ", instance.GetType().GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance).Select(property => property.Name))}.");
        throw new InvalidOperationException(); // unreachable
    }
}
