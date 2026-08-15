using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression (r24): a NAMED reply target's queue was only checked for non-emptiness, although
/// the transport-wide validator enforces three-way distinctness precisely because all logical
/// queues share one table/collection. AddReplyTarget("billing", options.WorkerQueue) passed every
/// check and stamped the WORKER queue as the reply address — every response addressed to the
/// target landed as a row the worker subscriber claimed, NAK-cycled to the cap and dead-lettered,
/// while the waiter timed out. The named-target path now honors the same distinctness rule.
/// </summary>
public sealed class DbTransportReplyTargetCollisionTests
{
    [Theory]
    [InlineData("worker")]
    [InlineData("deadletter")]
    public void PostgreSql_NamedTargetCollidingWithWorkerOrDeadLetterQueue_IsRejected(string queue)
    {
        var options = new PostgreSqlAsyncResponseTransportOptions();
        options.AddReplyTarget("billing", queue);
        var provider = new PostgreSqlReplyTargetProvider(Options.Create(options));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("billing"));
        Assert.Contains("billing", exception.Message);
        Assert.Contains(queue, exception.Message);
    }

    [Fact]
    public void PostgreSql_NamedTargetMatchingTheResponseQueue_IsAllowed()
    {
        // The transport-wide ResponseQueue IS the default target's destination, so a named target
        // pointing at it is legitimate.
        var options = new PostgreSqlAsyncResponseTransportOptions();
        options.AddReplyTarget("billing", options.ResponseQueue);
        var provider = new PostgreSqlReplyTargetProvider(Options.Create(options));

        Assert.Equal(options.ResponseQueue, provider.GetReplyTarget("billing").Address);
    }

    [Theory]
    [InlineData("worker")]
    [InlineData("deadletter")]
    public void SqlServer_NamedTargetCollidingWithWorkerOrDeadLetterQueue_IsRejected(string queue)
    {
        var options = new SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = "Server=localhost;Database=unused;Integrated Security=true"
        };
        options.AddReplyTarget("billing", queue);
        var provider = new SqlServerReplyTargetProvider(Options.Create(options));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("billing"));
        Assert.Contains("billing", exception.Message);
        Assert.Contains(queue, exception.Message);
    }

    [Theory]
    [InlineData("worker")]
    [InlineData("deadletter")]
    public void MongoDb_NamedTargetCollidingWithWorkerOrDeadLetterQueue_IsRejected(string queue)
    {
        var options = new MongoDbAsyncResponseTransportOptions();
        options.AddReplyTarget("billing", queue);
        var provider = new MongoDbReplyTargetProvider(Options.Create(options));

        var exception = Assert.Throws<InvalidOperationException>(() => provider.GetReplyTarget("billing"));
        Assert.Contains("billing", exception.Message);
        Assert.Contains(queue, exception.Message);
    }
}
