using AsyncResponse.Transports.Redis;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using StackExchange.Redis;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 65, Redis Streams transport: the worker stream's capacity refuses instead of evicting
/// (F-01), both early-ACK drains are summed against the host budget (L-04), and a dead-letter
/// entry no longer depends on the handler exception's Message getter (L-02). The Redis-server
/// half of F-01 (the Lua script's trim and refusal) is pinned against real servers by
/// RedisTransportRound65Tests in the integration suite.
/// </summary>
public sealed class Round65RedisTransportTests
{
    private const string FullReply = "ASYNCRESPONSE_STREAM_FULL the worker stream holds 3 entries its consumer group has not settled (capacity 3); the publish was refused rather than evicting unprocessed jobs";

    // ------------------------------------------------------------------ F-01 publish

    /// <summary>
    /// The capped worker append used to ride `XADD … MAXLEN ~ N`, which trims by length alone:
    /// past the cap it deleted entries the worker group had never read and entries pending in a
    /// handler. A capped append now carries no trim mode at all — only the capacity and the
    /// group whose settled entries a full stream may drop.
    /// </summary>
    [Fact]
    public async Task CappedPublish_SendsACapacityAndTheWorkerGroup_NotALengthTrim()
    {
        var (multiplexer, _, evaluations) = Multiplexer(() => Task.FromResult(RedisResult.Create((RedisValue)"7-0")));
        var transport = Transport(multiplexer, maxAttempts: 1);

        await transport.PublishAsync(Job("corr-1"));

        var (script, args) = Assert.Single(evaluations);
        Assert.DoesNotContain("MAXLEN", script, StringComparison.Ordinal);
        Assert.Equal("3", args[1].ToString());           // capacity
        Assert.Equal(string.Empty, args[2].ToString());  // no settled bound on the first attempt
        Assert.Equal("workers", args[3].ToString());     // the group whose settled entries may go
    }

    /// <summary>
    /// A full stream answers the append with ASYNCRESPONSE_STREAM_FULL. The adapter then reads
    /// the worker group's last-delivered id (XINFO GROUPS, outside the script) and lets the script
    /// drop what that group has settled before it decides again. Before the fix the refusal did
    /// not exist — the old script trimmed instead — and nothing ever consulted the group.
    /// </summary>
    [Fact]
    public async Task FullStream_ReadsTheGroupsLastDeliveredId_AndRetriesTheAppendWithIt()
    {
        var (multiplexer, database, evaluations) = Multiplexer(
            () => Task.FromException<RedisResult>(FullError()),
            () => Task.FromResult(RedisResult.Create((RedisValue)"9-0")));
        database
            .Setup(db => db.StreamGroupInfoAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync([GroupInfo("other", "8-0"), GroupInfo("workers", "4-0")]);
        var transport = Transport(multiplexer, maxAttempts: 1);

        await transport.PublishAsync(Job("corr-1"));

        Assert.Equal(2, evaluations.Count);
        Assert.Equal(string.Empty, evaluations[0].Args[2].ToString());
        Assert.Equal("4-0", evaluations[1].Args[2].ToString()); // the WORKER group's, not another group's
        // The same publish identity: the dedup marker key is unchanged, so a lost reply stays idempotent.
        database.Verify(db => db.StreamGroupInfoAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Once);
    }

    /// <summary>
    /// Nothing settled — the worker group does not exist yet, or has delivered nothing — means
    /// nothing may be dropped: the refusal stands, is retried within the publish budget (NATS
    /// parity: a full Discard=New work queue answers 503, which its publish retries), and then
    /// fails the publish. A wake-up that cannot be queued fails loudly instead of being
    /// trimmed away later.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FullStream_WithNothingSettled_IsRetriedWithinTheBudget_ThenThrown(bool groupExists)
    {
        var (multiplexer, database, evaluations) = Multiplexer(() => Task.FromException<RedisResult>(FullError()));
        database
            .Setup(db => db.StreamGroupInfoAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(groupExists ? [GroupInfo("workers", "0-0")] : []);
        var transport = Transport(multiplexer, maxAttempts: 2);

        var ex = await Assert.ThrowsAsync<RedisServerException>(() => transport.PublishAsync(Job("corr-1")));

        Assert.StartsWith("ASYNCRESPONSE_STREAM_FULL", ex.Message, StringComparison.Ordinal);
        Assert.Equal(2, evaluations.Count); // one per attempt: no bound to retry the script with
        Assert.All(evaluations, evaluation => Assert.Equal(string.Empty, evaluation.Args[2].ToString()));
    }

    // ------------------------------------------------------------------ F-01 settlement

    /// <summary>
    /// A settled worker entry must leave the stream: the capacity counts entries, and an ACKed
    /// entry left behind (XACK only, as before) held room until a trim reached it — behind one
    /// long-pending entry (a multi-day in-process timer) it never would, and every publish was
    /// refused. Both settlement paths of the worker role delete.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WorkerSettlement_AcknowledgesAndDeletes(bool earlyAck)
    {
        var database = new RecordingStreamDatabase();
        var subscriber = earlyAck ? new RedisSubscriberOptions().UseAckAfterEnqueue(1, 4, TimeSpan.FromSeconds(5)) : new RedisSubscriberOptions();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using (var dispatcher = RedisMessageDispatcher.Create(
            (_, _) =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            },
            database,
            new RedisAsyncResponseTransportOptions(),
            subscriber,
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker))
        {
            await dispatcher.HandleAsync(Delivery("1-0"), CancellationToken.None);
            await handled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        }

        Assert.Equal("1-0", Assert.Single(database.AckDeletes));
        Assert.Empty(database.AcksOnly);
    }

    /// <summary>
    /// Critic follow-up: without a capacity nothing needs the room, and another consumer group on
    /// the worker stream must keep entries it has not read yet — the worker only ACKs, as before.
    /// </summary>
    [Fact]
    public async Task WorkerSettlement_WithoutACapacity_OnlyAcknowledges()
    {
        var database = new RecordingStreamDatabase();
        var handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using (var dispatcher = RedisMessageDispatcher.Create(
            (_, _) =>
            {
                handled.TrySetResult();
                return Task.CompletedTask;
            },
            database,
            new RedisAsyncResponseTransportOptions { StreamMaxLength = null },
            new RedisSubscriberOptions(),
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker))
        {
            await dispatcher.HandleAsync(Delivery("1-0"), CancellationToken.None);
            await handled.Task.WaitAsync(TimeSpan.FromSeconds(10));
        }

        Assert.Equal("1-0", Assert.Single(database.AcksOnly));
        Assert.Empty(database.AckDeletes);
    }

    /// <summary>
    /// The response stream belongs to the remote producers (and may be read by groups of theirs):
    /// the ingress only ACKs it, never deletes.
    /// </summary>
    [Fact]
    public async Task ResponseSettlement_OnlyAcknowledges()
    {
        var database = new RecordingStreamDatabase();
        await using (var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => Task.CompletedTask,
            database,
            new RedisAsyncResponseTransportOptions(),
            new RedisSubscriberOptions(),
            NullLogger.Instance,
            "response-stream",
            "response-group",
            RedisSubscriberRole.ResponseIngress))
        {
            await dispatcher.HandleAsync(Delivery("1-0"), CancellationToken.None);
        }

        Assert.Equal("1-0", Assert.Single(database.AcksOnly));
        Assert.Empty(database.AckDeletes);
    }

    // ------------------------------------------------------------------ L-04

    /// <summary>
    /// The worker and response subscribers are two hosted services the host stops one after the
    /// other inside one shutdown budget. Each drain was validated alone, so 20 s + 20 s passed
    /// against 30 s and the second drain was cut off with its already-ACKed entries still queued
    /// (NATS has summed them since round 31).
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void BothRolesEarlyAck_TheirDrainsAreSummedAgainstTheHostBudget(bool validateWorker)
    {
        var options = new RedisAsyncResponseTransportOptions { HostShutdownTimeout = TimeSpan.FromSeconds(30) };
        options.WorkerSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(20));
        options.ResponseSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(20));

        var ex = Assert.Throws<InvalidOperationException>(() => RedisMessageDispatcher.ValidateOptions(
            options,
            validateWorker ? options.WorkerSubscriber : options.ResponseSubscriber,
            validateWorker ? RedisSubscriberRole.Worker : RedisSubscriberRole.ResponseIngress));

        Assert.Contains("00:00:40", ex.Message, StringComparison.Ordinal);
        Assert.Contains("WorkerSubscriber.BackgroundDrainTimeout", ex.Message, StringComparison.Ordinal);
        Assert.Contains("ResponseSubscriber.BackgroundDrainTimeout", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void BothRolesEarlyAck_WithinTheBudgetTogether_IsAccepted_AndOneRoleAloneIsNotSummed()
    {
        var both = new RedisAsyncResponseTransportOptions { HostShutdownTimeout = TimeSpan.FromSeconds(30) };
        both.WorkerSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(15));
        both.ResponseSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(15));
        RedisMessageDispatcher.ValidateOptions(both, both.WorkerSubscriber, RedisSubscriberRole.Worker);
        RedisMessageDispatcher.ValidateOptions(both, both.ResponseSubscriber, RedisSubscriberRole.ResponseIngress);

        var one = new RedisAsyncResponseTransportOptions { HostShutdownTimeout = TimeSpan.FromSeconds(30) };
        one.WorkerSubscriber.UseAckAfterEnqueue(4, 100, TimeSpan.FromSeconds(25));
        RedisMessageDispatcher.ValidateOptions(one, one.WorkerSubscriber, RedisSubscriberRole.Worker);
    }

    // ------------------------------------------------------------------ L-02

    /// <summary>
    /// The early-ACK burial built the dead-letter entry from exception.Message, which is user
    /// code: a throwing getter failed the write, and the entry — ACKed at enqueue, never
    /// redelivered — lost its only durable record (NATS was fixed in round 55).
    /// </summary>
    [Fact]
    public async Task EarlyAckBurial_SurvivesAThrowingMessageGetter()
    {
        var (database, reported) = await RunFailingEarlyAckHandlerAsync(new ThrowingMessageException());

        var dead = Assert.Single(database.DeadLetters);
        Assert.Equal(nameof(ThrowingMessageException), RedisTransportTests.Field(dead, "exceptionMessage"));
        Assert.Equal(typeof(ThrowingMessageException).FullName, RedisTransportTests.Field(dead, "exceptionType"));
        Assert.IsType<ThrowingMessageException>(reported);
    }

    [Fact]
    public async Task EarlyAckBurial_CapsTheExceptionMessage()
    {
        var (database, _) = await RunFailingEarlyAckHandlerAsync(new InvalidOperationException(new string('x', 20_000)));

        var dead = Assert.Single(database.DeadLetters);
        Assert.Equal(4096, RedisTransportTests.Field(dead, "exceptionMessage").Length); // MaxDeadLetterExceptionMessageLength
    }

    private static async Task<(RecordingStreamDatabase Database, Exception Reported)> RunFailingEarlyAckHandlerAsync(Exception failure)
    {
        var database = new RecordingStreamDatabase();
        var reported = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscriber = new RedisSubscriberOptions().UseAckAfterEnqueue(1, 4, TimeSpan.FromSeconds(5));
        subscriber.OnBackgroundFailure = context =>
        {
            reported.TrySetResult(context.Exception);
            return ValueTask.CompletedTask;
        };
        await using var dispatcher = RedisMessageDispatcher.Create(
            (_, _) => throw failure,
            database,
            new RedisAsyncResponseTransportOptions { DeadLetterStream = "dead" },
            subscriber,
            NullLogger.Instance,
            "worker-stream",
            "worker-group",
            RedisSubscriberRole.Worker);

        await dispatcher.HandleAsync(Delivery("1-0"), CancellationToken.None);
        // OnBackgroundFailure runs after the burial attempt, so the dead-letter outcome is final.
        return (database, await reported.Task.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    // ------------------------------------------------------------------ helpers

    // The obsolete message-only overload on purpose (RedisSubscriberTests' BUSYGROUP double has
    // the reasoning): the kind-taking one is experimental, and the refusal is recognised by its
    // error code, never by a kind.
#pragma warning disable CS0618 // Type or member is obsolete
    private static RedisServerException FullError() => new(FullReply);
#pragma warning restore CS0618

    private static RedisWorkerTransport Transport(IConnectionMultiplexer multiplexer, int maxAttempts)
        => new(
            Options.Create(new RedisAsyncResponseTransportOptions
            {
                WorkerConsumerGroup = "workers",
                StreamMaxLength = 3,
                PublishMaxAttempts = maxAttempts,
                PublishRetryBaseDelay = TimeSpan.FromMilliseconds(1),
                PublishRetryMaxDelay = TimeSpan.FromMilliseconds(1)
            }),
            multiplexer);

    /// <summary>
    /// A multiplexer whose database answers each script evaluation with the next of
    /// <paramref name="replies"/> (the last one repeating), recording the script and a copy of its
    /// arguments.
    /// </summary>
    private static (IConnectionMultiplexer Multiplexer, Mock<IDatabase> Database, List<(string Script, RedisValue[] Args)> Evaluations) Multiplexer(params Func<Task<RedisResult>>[] replies)
    {
        var evaluations = new List<(string Script, RedisValue[] Args)>();
        var database = new Mock<IDatabase>();
        database
            .Setup(db => db.ScriptEvaluateAsync(It.IsAny<string>(), It.IsAny<RedisKey[]>(), It.IsAny<RedisValue[]>(), It.IsAny<CommandFlags>()))
            .Returns<string, RedisKey[], RedisValue[], CommandFlags>((script, _, args, _) =>
            {
                evaluations.Add((script, (RedisValue[])args.Clone()));
                return replies[Math.Min(evaluations.Count, replies.Length) - 1]();
            });
        var multiplexer = new Mock<IConnectionMultiplexer>();
        multiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(database.Object);
        return (multiplexer.Object, database, evaluations);
    }

    /// <summary>
    /// StreamGroupInfo has no public constructor; build one by parameter name so the test does not
    /// depend on the constructor's parameter order.
    /// </summary>
    private static StreamGroupInfo GroupInfo(string name, string lastDeliveredId)
    {
        var constructor = typeof(StreamGroupInfo)
            .GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .OrderByDescending(candidate => candidate.GetParameters().Length)
            .First();
        var values = constructor.GetParameters()
            .Select(parameter => parameter.Name switch
            {
                "name" => name,
                "lastDeliveredId" => lastDeliveredId,
                _ => parameter.ParameterType.IsValueType ? Activator.CreateInstance(parameter.ParameterType) : null
            })
            .ToArray();
        var info = (StreamGroupInfo)constructor.Invoke(values);
        Assert.Equal(name, info.Name);
        Assert.Equal(lastDeliveredId, info.LastDeliveredId);
        return info;
    }

    private static WorkerJobEnvelope Job(string correlationId)
        => new()
        {
            CorrelationId = correlationId,
            Call = new ReflectionCallDto { ServiceInterfaceFullName = "T", MethodName = "M", Params = [] }
        };

    private static RedisStreamDelivery Delivery(string id)
        => new(
            "worker-stream",
            "worker-group",
            id,
            "payload-json",
            "corr",
            1,
            RedisTransportTests.Entry(id, ("payload", "payload-json"), ("correlationId", "corr")));

    private sealed class ThrowingMessageException : Exception
    {
        public override string Message => throw new FormatException("a formatting bug in a custom exception");
    }

    /// <summary>
    /// Records ACK-only and ACK-and-delete settlements apart. <c>StreamAcknowledgeAndDeleteAsync</c>
    /// implements the interface member where it exists; without it (the code before the fix) the
    /// worker role's XACK lands in <see cref="AcksOnly"/>.
    /// </summary>
    private sealed class RecordingStreamDatabase : IRedisStreamDatabase
    {
        private readonly object _gate = new();

        public List<string> AcksOnly { get; } = [];
        public List<string> AckDeletes { get; } = [];
        public List<NameValueEntry[]> DeadLetters { get; } = [];

        public Task<RedisValue> StreamAddAsync(RedisKey stream, NameValueEntry[] values, long? maxLength, bool useApproximateMaxLength, CancellationToken cancellationToken)
        {
            lock (_gate)
                DeadLetters.Add(values);
            return Task.FromResult<RedisValue>("dl-1");
        }

        public Task<bool> StreamCreateConsumerGroupAsync(RedisKey stream, RedisValue groupName, RedisValue position, bool createStream, CancellationToken cancellationToken)
            => Task.FromResult(true);

        public Task<StreamEntry[]> StreamReadGroupAsync(RedisKey stream, RedisValue groupName, RedisValue consumerName, int count, CancellationToken cancellationToken)
            => Task.FromResult(Array.Empty<StreamEntry>());

        public Task<long> StreamAcknowledgeAsync(RedisKey stream, RedisValue groupName, RedisValue messageId, CancellationToken cancellationToken)
        {
            lock (_gate)
                AcksOnly.Add(messageId.ToString());
            return Task.FromResult(1L);
        }

        public Task<long> StreamAcknowledgeAndDeleteAsync(RedisKey stream, RedisValue groupName, RedisValue messageId, CancellationToken cancellationToken)
        {
            lock (_gate)
                AckDeletes.Add(messageId.ToString());
            return Task.FromResult(1L);
        }

        public Task<StreamPendingMessageInfo[]> StreamPendingMessagesAsync(RedisKey stream, RedisValue groupName, int count, RedisValue consumerName, RedisValue? minId, RedisValue? maxId, long minIdleTimeInMilliseconds, CancellationToken cancellationToken)
            => Task.FromResult(Array.Empty<StreamPendingMessageInfo>());

        public Task<StreamEntry[]> StreamClaimAsync(RedisKey stream, RedisValue groupName, RedisValue consumerName, long minIdleTimeInMilliseconds, RedisValue[] messageIds, CancellationToken cancellationToken)
            => Task.FromResult(Array.Empty<StreamEntry>());
    }
}
