using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Npgsql;
using System.Reflection;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class RelationalChannelSubscriptionCoverageTests
{
    [Fact]
    public async Task SqlServerSubscription_CoversSeenSetAndEveryEnvelopeOutcome()
    {
        var options = Options.Create(new SqlServerAsyncResponseChannelOptions
        {
            ConnectionString = "Server=localhost,1;Database=unused;User Id=unused;Password=unused;TrustServerCertificate=true;Connect Timeout=1",
            AutoCreateSchema = false
        });
        var sql = new SqlServerChannelSql(options);
        using var provider = new ServiceCollection().BuildServiceProvider();
        var channel = new SqlServerAsyncResponseChannel(
            provider.GetRequiredService<IServiceScopeFactory>(),
            sql,
            MockRecoveryStore(),
            options,
            new AsyncResponseContextPropagation([]),
            NullLogger<SqlServerAsyncResponseChannel>.Instance);
        var recoveryStore = new SqlServerRecoveryStateStore(
            sql,
            NullLogger<SqlServerRecoveryStateStore>.Instance);
        ExerciseRecoveryDeserializer(recoveryStore);
        await Assert.ThrowsAsync<ArgumentException>(() => recoveryStore.SaveAsync(
            "corr",
            new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "different" },
            TimeSpan.FromMinutes(1)));
        await sql.HeartbeatSubscribersAsync(
            "instance",
            [],
            TimeSpan.FromMinutes(1),
            CancellationToken.None);

        await ExerciseAsync(
            typeof(SqlServerAsyncResponseChannel),
            "SqlServerSubscription`1",
            channel,
            json => new SqlServerChannelMessage(Guid.NewGuid(), "corr", json, DateTimeOffset.UtcNow));

        await channel.DisposeAsync();
    }

    [Fact]
    public async Task PostgreSqlSubscription_CoversSeenSetAndEveryEnvelopeOutcome()
    {
        await using var dataSource = NpgsqlDataSource.Create(
            "Host=localhost;Port=1;Database=unused;Username=unused;Password=unused;Timeout=1;Pooling=false");
        var options = Options.Create(new PostgreSqlAsyncResponseChannelOptions { AutoCreateSchema = false });
        var sql = new PostgreSqlChannelSql(dataSource, options);
        using var provider = new ServiceCollection().BuildServiceProvider();
        var channel = new PostgreSqlAsyncResponseChannel(
            provider.GetRequiredService<IServiceScopeFactory>(),
            sql,
            MockRecoveryStore(),
            options,
            new AsyncResponseContextPropagation([]),
            NullLogger<PostgreSqlAsyncResponseChannel>.Instance);
        ExerciseRecoveryDeserializer(
            new PostgreSqlRecoveryStateStore(sql, NullLogger<PostgreSqlRecoveryStateStore>.Instance));

        await ExerciseAsync(
            typeof(PostgreSqlAsyncResponseChannel),
            "PostgreSqlSubscription`1",
            channel,
            json => new PostgreSqlChannelMessage(Guid.NewGuid(), "corr", json, DateTimeOffset.UtcNow));

        await channel.DisposeAsync();
    }

    private static async Task ExerciseAsync(
        Type channelType,
        string subscriptionTypeName,
        object channel,
        Func<string, object> message)
    {
        var success = Subscription(channelType, subscriptionTypeName, channel, payload =>
            new ValueTask<bool>(payload.Status == OperationStatus.Completed));
        var seenId = Guid.NewGuid();
        Assert.True(Invoke<bool>(success.Instance, "MarkSeen", seenId));
        Assert.True(Invoke<bool>(success.Instance, "HasSeen", seenId));
        Assert.False(Invoke<bool>(success.Instance, "MarkSeen", seenId));
        Invoke(success.Instance, "PruneSeen", DateTimeOffset.UtcNow.AddMinutes(1));
        Assert.False(Invoke<bool>(success.Instance, "HasSeen", seenId));
        await ProcessAsync(success.Instance, message(SuccessEnvelope(OperationStatus.Running, "progress")));
        Assert.False(success.Completion.Task.IsCompleted);
        await ProcessAsync(success.Instance, message(SuccessEnvelope(OperationStatus.Completed, "done")));
        Assert.Equal("done", (await success.Completion.Task).Message);

        var nullEnvelope = Subscription(channelType, subscriptionTypeName, channel, _ => new ValueTask<bool>(true));
        await ProcessAsync(nullEnvelope.Instance, message("null"));
        await Assert.ThrowsAsync<JsonException>(() => nullEnvelope.Completion.Task);

        var futureEnvelope = Subscription(channelType, subscriptionTypeName, channel, _ => new ValueTask<bool>(true));
        await ProcessAsync(futureEnvelope.Instance, message(
            """{"SchemaVersion":999,"Success":true,"Payload":{"Status":2}}"""));
        await Assert.ThrowsAsync<InvalidOperationException>(() => futureEnvelope.Completion.Task);

        var remoteFailure = Subscription(channelType, subscriptionTypeName, channel, _ => new ValueTask<bool>(true));
        await ProcessAsync(remoteFailure.Instance, message(
            """{"SchemaVersion":1,"Success":false,"ExceptionMessage":"remote","ExceptionStackTrace":"stack"}"""));
        var remote = await Assert.ThrowsAsync<Exception>(() => remoteFailure.Completion.Task);
        Assert.Equal("stack", remote.Data["RemoteStackTrace"]);

        var malformed = Subscription(channelType, subscriptionTypeName, channel, _ => new ValueTask<bool>(true));
        await ProcessAsync(malformed.Instance, message("{not-json"));
        await Assert.ThrowsAsync<JsonException>(() => malformed.Completion.Task);

        var dropped = Subscription(channelType, subscriptionTypeName, channel, _ => new ValueTask<bool>(true));
        SetField(dropped.Instance, "_dropped", true);
        await ProcessAsync(dropped.Instance, message("null"));
        Assert.False(dropped.Completion.Task.IsCompleted);
    }

    private static (object Instance, TaskCompletionSource<OperationResult> Completion) Subscription(
        Type channelType,
        string nestedTypeName,
        object channel,
        Func<OperationResult, ValueTask<bool>> predicate)
    {
        var completion = new TaskCompletionSource<OperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var type = channelType.GetNestedType(nestedTypeName, BindingFlags.NonPublic)!
            .MakeGenericType(typeof(OperationResult));
        var instance = Activator.CreateInstance(
            type,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            [channel, "corr", Guid.NewGuid(), DateTimeOffset.UtcNow, predicate, completion, null, null],
            culture: null)!;
        // Processing terminal envelopes normally performs database cleanup. Other integration tests
        // cover that real cleanup; this focused test keeps the branch exercise isolated from a server.
        SetField(instance, "_cleanupStarted", 1);
        return (instance, completion);
    }

    private static IRecoveryStateStore MockRecoveryStore()
    {
        var store = new Moq.Mock<IRecoveryStateStore>();
        store.Setup(instance => instance.TryDeleteAsync(
                Moq.It.IsAny<string>(),
                Moq.It.IsAny<Guid>(),
                Moq.It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        return store.Object;
    }

    private static void ExerciseRecoveryDeserializer(object store)
    {
        var method = store.GetType().GetMethod("DeserializeState", BindingFlags.Instance | BindingFlags.NonPublic)!;
        object? Deserialize(string json, string? correlationId)
            => method.Invoke(store, [json, correlationId]);

        Assert.Null(Deserialize("null", "corr"));
        Assert.Null(Deserialize(
            JsonSerializer.Serialize(new RecoveryState { CorrelationId = "corr" }),
            "corr"));
        Assert.Null(Deserialize(
            JsonSerializer.Serialize(new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = "different"
            }),
            "corr"));
        Assert.Null(Deserialize(
            JsonSerializer.Serialize(new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = "corr",
                SchemaVersion = RecoveryStateSchema.Current + 1
            }),
            "corr"));
        Assert.Null(Deserialize("{not-json", "corr"));

        var valid = new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr" };
        Assert.IsType<RecoveryState>(Deserialize(JsonSerializer.Serialize(valid), "corr"));
        Assert.IsType<RecoveryState>(Deserialize(JsonSerializer.Serialize(valid), null));
    }

    private static string SuccessEnvelope(OperationStatus status, string message)
        => JsonSerializer.Serialize(new AsyncResponseEnvelope<OperationResult>
        {
            Success = true,
            Payload = new OperationResult { Status = status, Message = message }
        }, AsyncResponseEnvelopeOptions<OperationResult>.Instance);

    private static Task ProcessAsync(object subscription, object message)
        => (Task)subscription.GetType().GetMethod("ProcessAsync")!.Invoke(subscription, [message])!;

    private static T Invoke<T>(object target, string method, params object?[] arguments)
        => (T)target.GetType().GetMethod(method)!.Invoke(target, arguments)!;

    private static void Invoke(object target, string method, params object?[] arguments)
        => target.GetType().GetMethod(method)!.Invoke(target, arguments);

    private static void SetField(object target, string name, object value)
        => target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(target, value);
}
