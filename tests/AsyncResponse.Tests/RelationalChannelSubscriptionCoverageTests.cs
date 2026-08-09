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
            new FakeDebugLogger<SqlServerAsyncResponseChannel>());
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
            "DbSubscription`1",
            channel,
            json => new SqlServerChannelMessage(Guid.NewGuid(), "corr", json, DateTimeOffset.UtcNow));

        var nestedType = typeof(SqlServerAsyncResponseChannel).BaseType!.GetNestedType("PendingConfirmation", BindingFlags.NonPublic)!;
        var confirmation = Activator.CreateInstance(nestedType, [channel, Guid.NewGuid(), new TaskCompletionSource<bool>()])!;
        ((IDisposable)confirmation).Dispose();

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
            new FakeDebugLogger<PostgreSqlAsyncResponseChannel>());
        ExerciseRecoveryDeserializer(
            new PostgreSqlRecoveryStateStore(sql, NullLogger<PostgreSqlRecoveryStateStore>.Instance));

        await ExerciseAsync(
            typeof(PostgreSqlAsyncResponseChannel),
            "DbSubscription`1",
            channel,
            json => new PostgreSqlChannelMessage(Guid.NewGuid(), "corr", json, DateTimeOffset.UtcNow));

        var nestedType = typeof(PostgreSqlAsyncResponseChannel).BaseType!.GetNestedType("PendingConfirmation", BindingFlags.NonPublic)!;
        var confirmation = Activator.CreateInstance(nestedType, [channel, Guid.NewGuid(), new TaskCompletionSource<bool>()])!;
        ((IDisposable)confirmation).Dispose();

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

    [Fact]
    public void SequenceNames_ReserveSuffixSpaceAtTheIdentifierCaps()
    {
        // A maximum-length message-table name used to truncate "{table}_ack_seq" back to the
        // table's own name: the sequence then collided with the table (they share a namespace) —
        // PostgreSQL silently skipped creation and failed at the first nextval; SQL Server failed
        // at CREATE SEQUENCE.
        var pgOptions = new PostgreSqlAsyncResponseChannelOptions { MessageTable = new string('m', 63) };
        using var dataSource = NpgsqlDataSource.Create(
            "Host=localhost;Port=1;Database=unused;Username=unused;Password=unused;Timeout=1;Pooling=false");
        var pg = new PostgreSqlChannelSql(dataSource, Options.Create(pgOptions));
        Assert.EndsWith("_ack_seq", pg.AckSequenceName, StringComparison.Ordinal);
        Assert.True(pg.AckSequenceName.Length <= 63);
        Assert.NotEqual(pgOptions.MessageTable, pg.AckSequenceName);

        var sqlOptions = new SqlServerAsyncResponseChannelOptions
        {
            ConnectionString = "Server=localhost,1;Database=unused;User Id=sa;Password=unused;Encrypt=False;Connect Timeout=1",
            MessageTable = new string('m', 128)
        };
        var sqlServer = new SqlServerChannelSql(Options.Create(sqlOptions));
        Assert.EndsWith("_ack_seq]", sqlServer.AckSequence, StringComparison.Ordinal);
        Assert.NotEqual($"{sqlServer.Schema}.[{sqlOptions.MessageTable}]", sqlServer.AckSequence);
    }

    private static (object Instance, TaskCompletionSource<OperationResult> Completion) Subscription(
        Type channelType,
        string nestedTypeName,
        object channel,
        Func<OperationResult, ValueTask<bool>> predicate)
    {
        var completion = new TaskCompletionSource<OperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var type = channelType.BaseType!.GetNestedType(nestedTypeName, BindingFlags.NonPublic)!
            .MakeGenericType(typeof(OperationResult));
        var instance = Activator.CreateInstance(
            type,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            [channel, "corr", Guid.NewGuid(), DateTimeOffset.UtcNow, 0L, predicate, completion, null],
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

    private sealed class FakeDebugLogger<T> : Microsoft.Extensions.Logging.ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => logLevel == Microsoft.Extensions.Logging.LogLevel.Debug;
        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) {}
    }

    [Fact]
    public async Task SqlServerChannelSql_ExceptionCoverage()
    {
        var options = Options.Create(new SqlServerAsyncResponseChannelOptions
        {
            ConnectionString = "Server=localhost,1;Database=unused;User Id=sa;Password=unused;Encrypt=False;Connect Timeout=1",
            AutoCreateSchema = true
        });
        var sql = new SqlServerChannelSql(options);

        // 1. EnsureCreatedAsync throws when AutoCreateSchema = true
        await Assert.ThrowsAnyAsync<Exception>(() => sql.EnsureCreatedAsync());

        // 2. Set _created to true to bypass DDL, so other methods fail on their actual commands
        SetField(sql, "_created", true);

        await Assert.ThrowsAnyAsync<Exception>(() => sql.GetServerTimeUtcAsync(CancellationToken.None));
        await Assert.ThrowsAnyAsync<Exception>(() => sql.IsMessageAcknowledgedAsync(Guid.NewGuid(), CancellationToken.None));
        await Assert.ThrowsAnyAsync<Exception>(() => sql.LoadMessagesAsync("corr", DateTimeOffset.UtcNow, 10, null, null, CancellationToken.None));
        await Assert.ThrowsAnyAsync<Exception>(() => sql.HeartbeatSubscribersAsync("instance", [("corr", Guid.NewGuid())], TimeSpan.FromMinutes(1), CancellationToken.None));
        await Assert.ThrowsAnyAsync<Exception>(() => sql.CountActiveSubscribersAsync("corr", CancellationToken.None));
        await Assert.ThrowsAnyAsync<Exception>(() => sql.SaveRecoveryStateAsync("corr", new RecoveryState { RegistrationId = Guid.NewGuid(), CorrelationId = "corr" }, TimeSpan.FromMinutes(1), CancellationToken.None));

        var pruneRecovery = typeof(SqlServerChannelSql).GetMethod("PruneExpiredRecoveryAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await Assert.ThrowsAnyAsync<Exception>(() => (Task)pruneRecovery.Invoke(sql, [null, CancellationToken.None])!);

        var pruneMessages = typeof(SqlServerChannelSql).GetMethod("PruneExpiredMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await Assert.ThrowsAnyAsync<Exception>(() => (Task)pruneMessages.Invoke(sql, [CancellationToken.None])!);

        var pruneSubscribers = typeof(SqlServerChannelSql).GetMethod("PruneExpiredSubscribersAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await Assert.ThrowsAnyAsync<Exception>(() => (Task)pruneSubscribers.Invoke(sql, [null, CancellationToken.None])!);
    }

    [Fact]
    public async Task PostgreSqlChannelSql_ExceptionCoverage()
    {
        await using var dataSource = NpgsqlDataSource.Create(
            "Host=localhost;Port=1;Database=unused;Username=unused;Password=unused;Timeout=1;Pooling=false");
        var options = Options.Create(new PostgreSqlAsyncResponseChannelOptions { AutoCreateSchema = true });
        var sql = new PostgreSqlChannelSql(dataSource, options);

        // 1. EnsureCreatedAsync throws when AutoCreateSchema = true
        await Assert.ThrowsAnyAsync<Exception>(() => sql.EnsureCreatedAsync());

        // 2. Set _created to true
        SetField(sql, "_created", true);

        await Assert.ThrowsAnyAsync<Exception>(() => sql.GetServerTimeUtcAsync(CancellationToken.None));
        await Assert.ThrowsAnyAsync<Exception>(() => sql.IsMessageAcknowledgedAsync(Guid.NewGuid(), CancellationToken.None));
        await Assert.ThrowsAnyAsync<Exception>(() => sql.LoadMessagesAsync("corr", DateTimeOffset.UtcNow, 10, null, null, CancellationToken.None));
        await Assert.ThrowsAnyAsync<Exception>(() => sql.HeartbeatSubscribersAsync("instance", [("corr", Guid.NewGuid())], TimeSpan.FromMinutes(1), CancellationToken.None));
        await Assert.ThrowsAnyAsync<Exception>(() => sql.CountActiveSubscribersAsync("corr", CancellationToken.None));

        var pgPruneSubscribers = typeof(PostgreSqlChannelSql).GetMethod("PruneExpiredSubscribersAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        await Assert.ThrowsAnyAsync<Exception>(() => (Task)pgPruneSubscribers.Invoke(sql, [null, CancellationToken.None])!);

        var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<Exception>(() => sql.ExecuteListenAsync(_ => Task.CompletedTask, cts.Token));
    }

    [Fact]
    public async Task SqlServerAsyncResponseChannel_InternalCoverage()
    {
        var options = Options.Create(new SqlServerAsyncResponseChannelOptions
        {
            ConnectionString = "Server=localhost,1;Database=unused;User Id=sa;Password=unused;Encrypt=False;Connect Timeout=1",
            AutoCreateSchema = false,
            DeliveryConfirmationTimeout = TimeSpan.FromMilliseconds(2),
            DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(1)
        });
        var sql = new SqlServerChannelSql(options);
        using var provider = new ServiceCollection().BuildServiceProvider();
        var channel = new SqlServerAsyncResponseChannel(
            provider.GetRequiredService<IServiceScopeFactory>(),
            sql,
            MockRecoveryStore(),
            options,
            new AsyncResponseContextPropagation([]),
            new FakeDebugLogger<SqlServerAsyncResponseChannel>());

        // 1. Test DispatchMessageToSubscribersAsync with subscription marked dropped or seen
        var subscription = Subscription(typeof(SqlServerAsyncResponseChannel), "DbSubscription`1", channel, _ => new ValueTask<bool>(true));
        SetField(subscription.Instance, "_dropped", true);

        var message = new SqlServerChannelMessage(Guid.NewGuid(), "corr", "null", DateTimeOffset.UtcNow);
        var dispatchMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("DispatchMessageToSubscribersAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

        var subInterfaceType = typeof(SqlServerAsyncResponseChannel).BaseType!.GetNestedType("IDbSubscription", BindingFlags.NonPublic)!;
        var subArray = Array.CreateInstance(subInterfaceType, 1);
        subArray.SetValue(subscription.Instance, 0);

        await (Task)dispatchMethod.Invoke(channel, [message, subArray, CancellationToken.None])!;

        // 2. Test DispatchPendingMessagesAsync with scope filtering
        var addSubMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("AddSubscription", BindingFlags.Instance | BindingFlags.NonPublic)!;
        addSubMethod.Invoke(channel, ["corr", subscription.Instance]);

        var dispatchPendingMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("DispatchPendingMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var scope = new HashSet<string> { "other-corr" };
        await (Task)dispatchPendingMethod.Invoke(channel, [scope, CancellationToken.None])!;

        // 3. Test WaitForAcknowledgementAsync timeout path / database failure path
        var beginConfirmationMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("BeginConfirmation", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var tryConfirmMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("TryConfirmDeliveryAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var confirmation = beginConfirmationMethod.Invoke(channel, [message.Id])!;

        await Task.Delay(10);
        await Assert.ThrowsAnyAsync<Exception>(() => (Task)tryConfirmMethod.Invoke(channel, [confirmation, CancellationToken.None])!);

        await channel.DisposeAsync();

        // 4. Disposal is observed under the dispatcher gate: a racing registration must refuse
        // instead of recreating the CTS/loops the teardown just stopped.
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => channel.CreateResponseWaiter<OperationResult>("post-dispose"));
    }

    [Fact]
    public async Task PostgreSqlAsyncResponseChannel_DisposedChannelRefusesNewWaiters()
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
            new FakeDebugLogger<PostgreSqlAsyncResponseChannel>());

        await channel.DisposeAsync();

        // Disposal is observed under the listener gate: a racing registration must refuse instead
        // of recreating the CTS/loops the teardown just stopped.
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => channel.CreateResponseWaiter<OperationResult>("post-dispose"));
    }

    [Fact]
    public async Task PostgreSqlAsyncResponseChannel_InternalCoverage()
    {
        await using var dataSource = NpgsqlDataSource.Create(
            "Host=localhost;Port=1;Database=unused;Username=unused;Password=unused;Timeout=1;Pooling=false");
        var options = Options.Create(new PostgreSqlAsyncResponseChannelOptions
        {
            AutoCreateSchema = false,
            DeliveryConfirmationTimeout = TimeSpan.FromMilliseconds(2),
            DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(1)
        });
        var sql = new PostgreSqlChannelSql(dataSource, options);
        using var provider = new ServiceCollection().BuildServiceProvider();
        var channel = new PostgreSqlAsyncResponseChannel(
            provider.GetRequiredService<IServiceScopeFactory>(),
            sql,
            MockRecoveryStore(),
            options,
            new AsyncResponseContextPropagation([]),
            new FakeDebugLogger<PostgreSqlAsyncResponseChannel>());

        // 1. Test DispatchMessageToSubscribersAsync with subscription marked dropped or seen
        var subscription = Subscription(typeof(PostgreSqlAsyncResponseChannel), "DbSubscription`1", channel, _ => new ValueTask<bool>(true));
        SetField(subscription.Instance, "_dropped", true);

        var message = new PostgreSqlChannelMessage(Guid.NewGuid(), "corr", "null", DateTimeOffset.UtcNow);
        var dispatchMethod = typeof(PostgreSqlAsyncResponseChannel).GetMethod("DispatchMessageToSubscribersAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;

        var subInterfaceType = typeof(PostgreSqlAsyncResponseChannel).BaseType!.GetNestedType("IDbSubscription", BindingFlags.NonPublic)!;
        var subArray = Array.CreateInstance(subInterfaceType, 1);
        subArray.SetValue(subscription.Instance, 0);

        await (Task)dispatchMethod.Invoke(channel, [message, subArray, CancellationToken.None])!;

        // 2. Test DispatchPendingMessagesAsync with scope filtering
        var addSubMethod = typeof(PostgreSqlAsyncResponseChannel).GetMethod("AddSubscription", BindingFlags.Instance | BindingFlags.NonPublic)!;
        addSubMethod.Invoke(channel, ["corr", subscription.Instance]);

        var dispatchPendingMethod = typeof(PostgreSqlAsyncResponseChannel).GetMethod("DispatchPendingMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var scope = new HashSet<string> { "other-corr" };
        await (Task)dispatchPendingMethod.Invoke(channel, [scope, CancellationToken.None])!;

        // 3. Test WaitForAcknowledgementAsync timeout path / database failure path
        var beginConfirmationMethod = typeof(PostgreSqlAsyncResponseChannel).GetMethod("BeginConfirmation", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var tryConfirmMethod = typeof(PostgreSqlAsyncResponseChannel).GetMethod("TryConfirmDeliveryAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var confirmation = beginConfirmationMethod.Invoke(channel, [message.Id])!;

        await Task.Delay(10);
        await Assert.ThrowsAnyAsync<Exception>(() => (Task)tryConfirmMethod.Invoke(channel, [confirmation, CancellationToken.None])!);

        await channel.DisposeAsync();
    }
}
