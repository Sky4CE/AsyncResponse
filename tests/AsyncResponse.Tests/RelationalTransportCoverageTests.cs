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
            (_, _, _) => new ValueTask<bool>(true));
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
}
