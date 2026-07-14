using System.Reflection;
using AsyncResponse.Channels.SqlServer;
using AsyncResponse.Sample;
using AsyncResponse.Transports.SqlServer;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.IntegrationTests;

[Collection(IntegrationCollection.Name)]
public sealed class SqlServerDirectIntegrationTests(IntegrationFixture fixture) : IntegrationTestBase(fixture)
{
    [Fact]
    public async Task ChannelSql_RoundTripsRecoverySubscribersMessagesAndClaims()
    {
        await WithSchemaAsync("channel_sql", async schema =>
        {
            var options = ChannelOptions(schema);
            var sql = new SqlServerChannelSql(Options.Create(options));
            var store = new SqlServerRecoveryStateStore(sql, NullLogger<SqlServerRecoveryStateStore>.Instance);
            await sql.EnsureCreatedAsync();

            Assert.Equal(Quote(schema), sql.Schema);
            Assert.Contains(Quote(options.MessageTable), sql.MessageTable, StringComparison.Ordinal);
            Assert.Equal(
                SqlServerTransportStore.SchemaLockResource(schema),
                SqlServerChannelSql.SchemaLockResource(schema));

            var correlationId = NewId("direct-recovery");
            var state = new RecoveryState
            {
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(OperationResult).FullName
            };

            await store.SaveAsync(correlationId, state, TimeSpan.FromSeconds(30));
            Assert.NotEqual(Guid.Empty, state.RegistrationId);

            var stored = Assert.Single(await store.GetAllAsync(correlationId));
            Assert.Equal(correlationId, stored.CorrelationId);
            Assert.Equal(state.RegistrationId, stored.RegistrationId);

            var scanned = new List<RecoveryState>();
            await foreach (var scannedState in store.ScanAsync())
                scanned.Add(scannedState);
            Assert.Contains(scanned, item => item.CorrelationId == correlationId);

            var newerState = new RecoveryState
            {
                CorrelationId = "future-state",
                RegistrationId = Guid.NewGuid(),
                PayloadTypeFullName = typeof(OperationResult).FullName,
                SchemaVersion = RecoveryStateSchema.Current + 1
            };
            await sql.SaveRecoveryStateAsync("future-state", newerState, TimeSpan.FromSeconds(30), CancellationToken.None);
            Assert.Empty(await store.GetAllAsync("future-state"));

            await InsertUnreadableRecoveryStateAsync(schema, options.RecoveryStateTable, "bad-state");
            Assert.Empty(await store.GetAllAsync("bad-state"));

            Assert.False(await store.TryDeleteAsync(correlationId, Guid.NewGuid()));
            Assert.True(await store.TryDeleteAsync(correlationId, state.RegistrationId));
            Assert.Empty(await store.GetAllAsync(correlationId));

            var deleteState = new RecoveryState { CorrelationId = correlationId };
            await store.SaveAsync(correlationId, deleteState, TimeSpan.FromSeconds(30));
            Assert.True(await store.TryDeleteAsync(correlationId, deleteState.RegistrationId));
            Assert.Empty(await store.GetAllAsync(correlationId));

            await store.SaveAsync("expired-state", new RecoveryState { CorrelationId = "expired-state" }, TimeSpan.FromMilliseconds(1));
            await Task.Delay(40);
            Assert.Empty(await store.GetAllAsync("expired-state"));

            var subscriberId = Guid.NewGuid();
            await sql.UpsertSubscriberAsync("subscribed", subscriberId, "test-instance", TimeSpan.FromSeconds(30), CancellationToken.None);
            await sql.UpsertSubscriberAsync("expired-subscriber", Guid.NewGuid(), "test-instance", TimeSpan.FromMilliseconds(1), CancellationToken.None);
            await Task.Delay(40);
            Assert.Equal(1, await sql.CountActiveSubscribersAsync("subscribed", CancellationToken.None));
            Assert.Equal(0, await sql.CountActiveSubscribersAsync("expired-subscriber", CancellationToken.None));
            await sql.DeleteSubscriberAsync("subscribed", subscriberId, CancellationToken.None);
            Assert.Equal(0, await sql.CountActiveSubscribersAsync("subscribed", CancellationToken.None));

            var heartbeatA = Guid.NewGuid();
            var heartbeatB = Guid.NewGuid();
            var staleHeartbeat = Guid.NewGuid();
            await sql.UpsertSubscriberAsync("heartbeat-a", heartbeatA, "heartbeat-instance", TimeSpan.FromMilliseconds(100), CancellationToken.None);
            await sql.UpsertSubscriberAsync("heartbeat-b", heartbeatB, "heartbeat-instance", TimeSpan.FromMilliseconds(100), CancellationToken.None);
            await sql.UpsertSubscriberAsync("heartbeat-stale", staleHeartbeat, "heartbeat-instance", TimeSpan.FromMilliseconds(100), CancellationToken.None);
            await Task.Delay(50);
            await sql.HeartbeatSubscribersAsync("heartbeat-instance", [heartbeatA, heartbeatB], TimeSpan.FromSeconds(2), CancellationToken.None);
            await Task.Delay(100);
            Assert.Equal(1, await sql.CountActiveSubscribersAsync("heartbeat-a", CancellationToken.None));
            Assert.Equal(1, await sql.CountActiveSubscribersAsync("heartbeat-b", CancellationToken.None));
            Assert.Equal(0, await sql.CountActiveSubscribersAsync("heartbeat-stale", CancellationToken.None));

            var startedAt = await sql.GetServerTimeUtcAsync(CancellationToken.None);
            var messageId = Guid.NewGuid();
            await sql.InsertMessageAsync(messageId, "message-correlation", SuccessEnvelope("first"), TimeSpan.FromSeconds(30), CancellationToken.None);
            await sql.InsertMessageAsync(messageId, "message-correlation", SuccessEnvelope("duplicate"), TimeSpan.FromSeconds(30), CancellationToken.None);

            var messages = await sql.LoadMessagesAsync("message-correlation", startedAt.AddSeconds(-5), 10, null, null, CancellationToken.None);
            var message = Assert.Single(messages);
            Assert.Equal(messageId, message.Id);
            Assert.Equal("message-correlation", message.CorrelationId);
            Assert.False(await sql.IsMessageAcknowledgedAsync(messageId, CancellationToken.None));
            Assert.True(await sql.TryClaimForDeliveryAsync(messageId, CancellationToken.None));
            Assert.True(await sql.IsMessageAcknowledgedAsync(messageId, CancellationToken.None));
            Assert.False(await sql.TryClaimForRecoveryAsync(messageId, CancellationToken.None));

            var recoveryMessageId = Guid.NewGuid();
            await sql.InsertMessageAsync(recoveryMessageId, "recovery-correlation", SuccessEnvelope("late"), TimeSpan.FromSeconds(30), CancellationToken.None);
            Assert.True(await sql.TryClaimForRecoveryAsync(recoveryMessageId, CancellationToken.None));
            Assert.False(await sql.TryClaimForDeliveryAsync(recoveryMessageId, CancellationToken.None));

            await sql.InsertMessageAsync(Guid.NewGuid(), "expired-message", SuccessEnvelope("old"), TimeSpan.FromMilliseconds(1), CancellationToken.None);
            await Task.Delay(40);
            await sql.InsertMessageAsync(Guid.NewGuid(), "fresh-message", SuccessEnvelope("new"), TimeSpan.FromSeconds(30), CancellationToken.None);
            Assert.Empty(await sql.LoadMessagesAsync("expired-message", startedAt.AddSeconds(-5), 10, null, null, CancellationToken.None));

            const int pagedCount = 70;
            var pagedCorrelation = NewId("paged-messages");
            for (var index = 0; index < pagedCount; index++)
                await sql.InsertMessageAsync(Guid.NewGuid(), pagedCorrelation, SuccessEnvelope($"page-{index}"), TimeSpan.FromSeconds(30), CancellationToken.None);
            var paged = new List<SqlServerChannelMessage>();
            DateTimeOffset? afterCreatedAtUtc = null;
            Guid? afterId = null;
            while (true)
            {
                var page = await sql.LoadMessagesAsync(pagedCorrelation, startedAt.AddSeconds(-5), 16, afterCreatedAtUtc, afterId, CancellationToken.None);
                paged.AddRange(page);
                if (page.Count < 16)
                    break;
                afterCreatedAtUtc = page[^1].CreatedAtUtc;
                afterId = page[^1].Id;
            }
            Assert.Equal(pagedCount, paged.Count);
            Assert.Equal(pagedCount, paged.Select(item => item.Id).Distinct().Count());
        });
    }

    [Fact]
    public async Task Channel_DeliversLiveResponsesAndLostSubscriberCallbacks()
    {
        var schema = NewSchema("channel");
        ServiceProvider? provider = null;
        try
        {
            provider = BuildProvider(schema);
            var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
            var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
            var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
            var recoverable = provider.GetRequiredService<IRecoverableAsyncResponseSubscriber>();
            var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
            var channel = provider.GetRequiredService<SqlServerAsyncResponseChannel>();
            var flow = provider.GetRequiredService<DirectRecoveryFlow>();

            var correlationId = NewId("live");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
                correlationId,
                payload => new ValueTask<bool>(payload.Status == OperationStatus.Completed),
                TimeSpan.FromSeconds(5)))
            {
                Assert.Equal(1, await probe.CountActiveSubscribersAsync(correlationId));
                await publisher.SetResponse(new OperationResult { Status = OperationStatus.Running, Message = "progress" }, correlationId);
                await Task.Delay(100);
                Assert.False(waiter.ResponseTask.IsCompleted);

                await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "done" }, correlationId);
                var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Equal("done", result.Message);
            }

            var rawCorrelationId = NewId("raw");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(rawCorrelationId, timeout: TimeSpan.FromSeconds(5)))
            {
                await rawPublisher.SetRawResponseJson("""{"Status":2,"Message":"raw"}""", rawCorrelationId);
                var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Equal("raw", result.Message);
            }

            var rawObjectCorrelationId = NewId("raw-object");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(rawObjectCorrelationId, timeout: TimeSpan.FromSeconds(5)))
            {
                await rawPublisher.SetRawResponse(new OperationResult { Status = OperationStatus.Completed, Message = "raw-object" }, rawObjectCorrelationId);
                var result = await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Equal("raw-object", result.Message);
            }

            var exceptionCorrelationId = NewId("exception");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(exceptionCorrelationId, timeout: TimeSpan.FromSeconds(5)))
            {
                await publisher.SetException(new InvalidOperationException("remote boom"), exceptionCorrelationId);
                var ex = await Assert.ThrowsAnyAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
                Assert.Equal("remote boom", ex.Message);
            }

            await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, " ");
            await rawPublisher.SetRawResponseJson("""{"Status":2}""", " ");
            await publisher.SetException(new InvalidOperationException("blank"), " ");
            Assert.Equal(0, await probe.CountActiveSubscribersAsync(" "));

            var resumeCorrelationId = NewId("lost-resume");
            await using (var waiter = await recoverable.CreateRecoverableResponseWaiter<OperationResult>(
                resumeCorrelationId,
                ResumeCallback(),
                FailureCallback(),
                timeout: TimeSpan.FromSeconds(5)))
            {
                await channel.DropLocalSubscriptionsAsync();
                await publisher.SetResponse(
                    new OperationResult { Status = OperationStatus.Completed, Message = "late" },
                    resumeCorrelationId);
                var resumed = await flow.WaitResumeAsync(resumeCorrelationId).WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Equal("late", resumed.Message);
            }

            var rawLostCorrelationId = NewId("lost-raw");
            await using (var waiter = await recoverable.CreateRecoverableResponseWaiter<OperationResult>(
                rawLostCorrelationId,
                ResumeCallback(),
                FailureCallback(),
                timeout: TimeSpan.FromSeconds(5)))
            {
                await channel.DropLocalSubscriptionsAsync();
                await rawPublisher.SetRawResponseJson("""{"Status":2,"Message":"late raw"}""", rawLostCorrelationId);
                var resumed = await flow.WaitResumeAsync(rawLostCorrelationId).WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Equal("late raw", resumed.Message);
            }

            var failedCorrelationId = NewId("lost-failed");
            await using (var waiter = await recoverable.CreateRecoverableResponseWaiter<OperationResult>(
                failedCorrelationId,
                ResumeCallback(),
                FailureCallback(),
                timeout: TimeSpan.FromSeconds(5)))
            {
                await channel.DropLocalSubscriptionsAsync();
                await publisher.SetResponse(
                    new OperationResult { Status = OperationStatus.Failed, Message = "domain failed" },
                    failedCorrelationId);
                var failure = await flow.WaitFailureAsync(failedCorrelationId).WaitAsync(TimeSpan.FromSeconds(5));
                var domainFailure = Assert.IsType<AsyncResponseDomainFailureException>(failure);
                Assert.Contains("domain failed", domainFailure.PayloadJson, StringComparison.Ordinal);
            }

            var lostExceptionCorrelationId = NewId("lost-exception");
            await using (var waiter = await recoverable.CreateRecoverableResponseWaiter<OperationResult>(
                lostExceptionCorrelationId,
                ResumeCallback(),
                FailureCallback(),
                timeout: TimeSpan.FromSeconds(5)))
            {
                await channel.DropLocalSubscriptionsAsync();
                await publisher.SetException(new InvalidOperationException("late exception"), lostExceptionCorrelationId);
                var failure = await flow.WaitFailureAsync(lostExceptionCorrelationId).WaitAsync(TimeSpan.FromSeconds(5));
                Assert.Equal("late exception", failure.Message);
            }

            var callback = ResumeCallback();
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                recoverable.CreateRecoverableResponseWaiter<DefaultRecoveryPayload>(
                    NewId("default-recovery"),
                    callback,
                    timeout: TimeSpan.FromSeconds(5)));
        }
        finally
        {
            if (provider is not null)
                await provider.DisposeAsync();
            await DropSchemaAsync(schema);
        }
    }

    [Fact]
    public async Task Channel_DeliversLocalResponsesWithoutWaitingForSweepBacklog()
    {
        var schema = NewSchema("fast");
        ServiceProvider? provider = null;
        var waiters = new List<IAsyncResponseWaiter<OperationResult>>();
        try
        {
            provider = BuildProvider(schema, options =>
            {
                // A deliberately glacial sweep (30s): local deliveries must complete through the
                // same-process fast path without ever waiting for the polling loop. The assertion
                // window (20s) stays below the sweep so a pass can only mean in-process delivery.
                // DeliveryConfirmationTimeout is generous (5s) on purpose: it is the budget after which
                // the publisher gives a response up to recovery, so it must comfortably exceed a claim
                // round-trip under a loaded CI database. A too-tight value (the previous 20ms is below a
                // CI round-trip) makes the publisher steal a live-but-slow local delivery to recovery,
                // starving the waiter — which is exactly what flaked under the heavier integration fixture.
                options.DeliveryConfirmationTimeout = TimeSpan.FromSeconds(5);
                options.ActivePollInterval = TimeSpan.FromSeconds(30);
                options.IdlePollInterval = TimeSpan.FromSeconds(30);
            });
            var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
            var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();

            var correlationIds = Enumerable.Range(0, 32)
                .Select(_ => NewId("local-fast"))
                .ToArray();

            foreach (var correlationId in correlationIds)
                waiters.Add(await subscriber.CreateResponseWaiter<OperationResult>(
                    correlationId,
                    timeout: TimeSpan.FromSeconds(20)));

            await Task.WhenAll(correlationIds.Select(correlationId =>
                publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, correlationId)))
                .WaitAsync(TimeSpan.FromSeconds(20));

            var results = await Task.WhenAll(waiters.Select(waiter => waiter.ResponseTask))
                .WaitAsync(TimeSpan.FromSeconds(20));
            Assert.All(results, result => Assert.Equal(OperationStatus.Completed, result.Status));
        }
        finally
        {
            foreach (var waiter in waiters)
                await waiter.DisposeAsync();
            if (provider is not null)
                await provider.DisposeAsync();
            await DropSchemaAsync(schema);
        }
    }

    [Fact]
    public async Task Channel_RegressionEdges_HandleFallbacksFaultedEnvelopesAndSetupFailures()
    {
        var schema = NewSchema("channel_edges");
        ServiceProvider? provider = null;
        try
        {
            provider = BuildProvider(schema);
            var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
            var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
            var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
            var recoverableStore = provider.GetRequiredService<IRecoveryStateStore>();
            var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
            var sql = provider.GetRequiredService<SqlServerChannelSql>();
            var channel = provider.GetRequiredService<SqlServerAsyncResponseChannel>();
            var flow = provider.GetRequiredService<DirectRecoveryFlow>();

            await Assert.ThrowsAsync<ArgumentNullException>(() =>
                subscriber.CreateResponseWaiter<OperationResult>(" "));
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
                subscriber.CreateResponseWaiter<OperationResult>(NewId("bad-timeout"), timeout: TimeSpan.Zero));

            var recoveryClaimedCorrelationId = NewId("recovery-claimed-before-waiter");
            var recoveryClaimedMessageId = Guid.NewGuid();
            await sql.InsertMessageAsync(
                recoveryClaimedMessageId,
                recoveryClaimedCorrelationId,
                SuccessEnvelope("already-recovered"),
                TimeSpan.FromSeconds(30),
                CancellationToken.None);
            Assert.True(await sql.TryClaimForRecoveryAsync(recoveryClaimedMessageId, CancellationToken.None));
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
                recoveryClaimedCorrelationId,
                timeout: TimeSpan.FromSeconds(1)))
            {
                await Assert.ThrowsAsync<TimeoutException>(() =>
                    waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(3)));
            }

            var malformedCorrelationId = NewId("malformed");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(malformedCorrelationId, timeout: TimeSpan.FromSeconds(5)))
            {
                await sql.InsertMessageAsync(Guid.NewGuid(), malformedCorrelationId, "null", TimeSpan.FromSeconds(30), CancellationToken.None);
                await Assert.ThrowsAsync<JsonException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
            }

            var futureSchemaCorrelationId = NewId("future-envelope");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(futureSchemaCorrelationId, timeout: TimeSpan.FromSeconds(5)))
            {
                await sql.InsertMessageAsync(
                    Guid.NewGuid(),
                    futureSchemaCorrelationId,
                    """{"SchemaVersion":999,"Success":true,"Payload":{"Status":2},"ExceptionMessage":null,"ExceptionStackTrace":null}""",
                    TimeSpan.FromSeconds(30),
                    CancellationToken.None);
                var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
                Assert.Contains("schema version", ex.Message, StringComparison.Ordinal);
            }

            var predicateCorrelationId = NewId("predicate");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
                predicateCorrelationId,
                _ => throw new InvalidOperationException("predicate boom"),
                TimeSpan.FromSeconds(5)))
            {
                await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, predicateCorrelationId);
                var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
                Assert.Equal("predicate boom", ex.Message);
            }

            var stackCorrelationId = NewId("remote-stack");
            await using (var waiter = await subscriber.CreateResponseWaiter<OperationResult>(stackCorrelationId, timeout: TimeSpan.FromSeconds(5)))
            {
                Exception captured;
                try
                {
                    throw new InvalidOperationException("with stack");
                }
                catch (Exception thrown)
                {
                    captured = thrown;
                }

                await publisher.SetException(captured, stackCorrelationId);
                var remoteFailure = await Assert.ThrowsAnyAsync<Exception>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
                Assert.Equal("with stack", remoteFailure.Message);
                Assert.True(remoteFailure.Data.Contains("RemoteStackTrace"));
            }

            var noLocalResponse = NewId("no-local-response");
            await ArmRecoveryStateAsync(recoverableStore, noLocalResponse);
            await sql.UpsertSubscriberAsync(noLocalResponse, Guid.NewGuid(), "remote-instance", TimeSpan.FromSeconds(1), CancellationToken.None);
            await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "confirmed late" }, noLocalResponse);
            var resumed = await flow.WaitResumeAsync(noLocalResponse).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("confirmed late", resumed.Message);

            var noLocalRaw = NewId("no-local-raw");
            await ArmRecoveryStateAsync(recoverableStore, noLocalRaw);
            await sql.UpsertSubscriberAsync(noLocalRaw, Guid.NewGuid(), "remote-instance", TimeSpan.FromSeconds(1), CancellationToken.None);
            await rawPublisher.SetRawResponseJson("""{"Status":2,"Message":"confirmed raw"}""", noLocalRaw);
            var rawResumed = await flow.WaitResumeAsync(noLocalRaw).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("confirmed raw", rawResumed.Message);

            var noLocalException = NewId("no-local-exception");
            await ArmRecoveryStateAsync(recoverableStore, noLocalException);
            await sql.UpsertSubscriberAsync(noLocalException, Guid.NewGuid(), "remote-instance", TimeSpan.FromSeconds(1), CancellationToken.None);
            await publisher.SetException(new InvalidOperationException("confirmed exception"), noLocalException);
            var failure = await flow.WaitFailureAsync(noLocalException).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal("confirmed exception", failure.Message);

            Assert.Equal(0, await probe.CountActiveSubscribersAsync(" "));

            var lingering = await subscriber.CreateResponseWaiter<OperationResult>(NewId("dispose-active"), timeout: TimeSpan.FromSeconds(30));
            await using (lingering)
            {
                await channel.DisposeAsync();
                await Assert.ThrowsAsync<ObjectDisposedException>(() =>
                    subscriber.CreateResponseWaiter<OperationResult>(NewId("disposed"), timeout: TimeSpan.FromSeconds(5)));
            }
        }
        finally
        {
            if (provider is not null)
                await provider.DisposeAsync();
            await DropSchemaAsync(schema);
        }

        var badSchema = NewSchema("missing_channel");
        await using var badProvider = BuildProvider(badSchema, options => options.AutoCreateSchema = false);
        var badSubscriber = badProvider.GetRequiredService<IAsyncResponseSubscriber>();
        var badPublisher = badProvider.GetRequiredService<IAsyncResponsePublisher>();
        var badRawPublisher = badProvider.GetRequiredService<IRawAsyncResponsePublisher>();
        var badProbe = badProvider.GetRequiredService<IActiveSubscriberProbe>();

        await using (var faulted = await badSubscriber.CreateResponseWaiter<OperationResult>(NewId("missing"), timeout: TimeSpan.FromSeconds(5)))
        {
            await Assert.ThrowsAnyAsync<Exception>(() => faulted.ResponseTask.WaitAsync(TimeSpan.FromSeconds(5)));
        }

        Assert.Equal(0, await badProbe.CountActiveSubscribersAsync(NewId("missing-count")));
        await Assert.ThrowsAnyAsync<Exception>(() =>
            badPublisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, NewId("missing-response")));
        await Assert.ThrowsAnyAsync<Exception>(() =>
            badRawPublisher.SetRawResponseJson("""{"Status":2}""", NewId("missing-raw")));
        await Assert.ThrowsAnyAsync<Exception>(() =>
            badPublisher.SetException(new InvalidOperationException("missing"), NewId("missing-exception")));
    }

    [Fact]
    public async Task TransportStore_WorkerTransportSubscribersAndDeliveryStatesRoundTrip()
    {
        await WithSchemaAsync("transport", async schema =>
        {
            var options = TransportOptions(schema);
            var optionsAccessor = Options.Create(options);
            var store = new SqlServerTransportStore(optionsAccessor);
            await store.EnsureCreatedAsync();

            Assert.Equal(Quote(schema), store.Schema);
            Assert.Contains(Quote(options.MessageTable), store.MessageTable, StringComparison.Ordinal);

            // Same-process wake: publishes raise the in-process event that replaces LISTEN/NOTIFY.
            var published = new List<string?>();
            store.MessagePublished += published.Add;

            var id = Guid.NewGuid();
            await store.PublishAsync(
                id,
                options.WorkerQueue,
                """{"kind":"ack"}""",
                new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase) { ["X-Trace"] = "trace-1" },
                CancellationToken.None);
            Assert.Contains(options.WorkerQueue, published);

            // Publishing the same id again is a no-op (idempotent publish).
            await store.PublishAsync(id, options.WorkerQueue, """{"kind":"ack-duplicate"}""", null, CancellationToken.None);

            var delivery = await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None);
            Assert.NotNull(delivery);
            Assert.Equal(id, delivery.Id);
            Assert.Equal(1, delivery.Attempt);
            Assert.Equal("""{"kind":"ack"}""", delivery.Payload);
            Assert.Equal("trace-1", delivery.Headers["x-trace"]);
            await delivery.AckAsync();
            Assert.Null(await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None));

            published.Clear();
            await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, """{"kind":"nak"}""", null, CancellationToken.None);
            var retry = (await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None))!;
            await retry.NakAsync(TimeSpan.FromMilliseconds(30));
            Assert.Contains(null, published); // a NAK release wakes every queue's subscriber
            Assert.Null(await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None));
            await EventuallyAsync(async () =>
                (await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None)) is { } retried
                && await AckAndMatchAttemptAsync(retried, 2));

            await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, """{"kind":"deadletter"}""", null, CancellationToken.None);
            var poison = (await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None))!;
            Assert.True(await poison.DeadLetterAsync(new InvalidOperationException("line1\nline2"), true, CancellationToken.None));
            Assert.Null(await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None));
            var deadLetter = (await store.TryClaimAsync(options.DeadLetterQueue, options.LockTimeout, CancellationToken.None))!;
            Assert.Equal(options.DeadLetterQueue, deadLetter.Queue);
            Assert.Equal("line1 line2", deadLetter.Headers["AR-DeadLetter-Reason"]);
            Assert.Equal(options.WorkerQueue, deadLetter.Headers["AR-DeadLetter-Source-Queue"]);
            await deadLetter.AckAsync();

            var disabledOptions = TransportOptions(schema);
            disabledOptions.DeadLetterEnabled = false;
            var disabledStore = new SqlServerTransportStore(Options.Create(disabledOptions));
            await disabledStore.PublishAsync(Guid.NewGuid(), disabledOptions.WorkerQueue, """{"kind":"disabled"}""", null, CancellationToken.None);
            var disabled = (await disabledStore.TryClaimAsync(disabledOptions.WorkerQueue, disabledOptions.LockTimeout, CancellationToken.None))!;
            Assert.True(await disabled.DeadLetterAsync(new InvalidOperationException("no dlq"), true, CancellationToken.None));
            Assert.Null(await disabledStore.TryClaimAsync(disabledOptions.WorkerQueue, disabledOptions.LockTimeout, CancellationToken.None));

            for (var i = 0; i < 3; i++)
                await store.PublishAsync(Guid.NewGuid(), "batch", $$"""{"index":{{i}}}""", null, CancellationToken.None);

            var batch = new List<SqlServerTransportDelivery>();
            await foreach (var item in store.ClaimBatchAsync("batch", 2, options.LockTimeout, CancellationToken.None))
                batch.Add(item);
            Assert.Equal(2, batch.Count);
            foreach (var item in batch)
                await item.AckAsync();
            await DrainQueueAsync(store, "batch", options.LockTimeout);

            var transport = new SqlServerWorkerTransport(optionsAccessor, store);
            await transport.PublishAsync(new WorkerJobEnvelope
            {
                CorrelationId = "corr-worker",
                Call = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IDirectSqlServerRecoveryFlow).FullName!,
                    MethodName = nameof(IDirectSqlServerRecoveryFlow.ResumeAsync),
                    Params = []
                },
                ReplyTarget = new AsyncResponseReplyTarget
                {
                    Name = "default",
                    Transport = SqlServerAsyncResponseTransportOptions.TransportName,
                    Address = options.ResponseQueue
                }
            });

            var jobDelivery = (await store.TryClaimAsync(options.WorkerQueue, options.LockTimeout, CancellationToken.None))!;
            Assert.Equal("corr-worker", jobDelivery.Headers[options.CorrelationIdHeader]);
            var job = JsonSerializer.Deserialize<WorkerJobEnvelope>(jobDelivery.Payload);
            Assert.NotNull(job);
            Assert.Equal("corr-worker", job.CorrelationId);
            Assert.Equal(nameof(IDirectSqlServerRecoveryFlow.ResumeAsync), job.Call.MethodName);
            await jobDelivery.AckAsync();

            var ingress = new RecordingIngress();
            var workerSubscriber = new SqlServerWorkerSubscriber(
                optionsAccessor,
                store,
                ingress,
                NullLogger<SqlServerWorkerSubscriber>.Instance);
            var responseSubscriber = new SqlServerResponseIngressSubscriber(
                optionsAccessor,
                store,
                ingress,
                NullLogger<SqlServerResponseIngressSubscriber>.Instance);

            await workerSubscriber.StartAsync(CancellationToken.None);
            await responseSubscriber.StartAsync(CancellationToken.None);
            try
            {
                await store.PublishAsync(Guid.NewGuid(), options.WorkerQueue, """{"worker":true}""", null, CancellationToken.None);
                await store.PublishAsync(Guid.NewGuid(), options.ResponseQueue, """{"CorrelationId":"corr-response","Status":2}""", null, CancellationToken.None);

                using (var workerJson = JsonDocument.Parse(await ingress.WorkerReceived.Task.WaitAsync(TimeSpan.FromSeconds(5))))
                    Assert.True(workerJson.RootElement.GetProperty("worker").GetBoolean());

                var response = await ingress.ResponseReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
                using (var responseJson = JsonDocument.Parse(response.Json))
                    Assert.Equal(2, responseJson.RootElement.GetProperty("Status").GetInt32());
                Assert.Equal("corr-response", response.CorrelationId);
            }
            finally
            {
                await workerSubscriber.StopAsync(CancellationToken.None);
                await responseSubscriber.StopAsync(CancellationToken.None);
            }
        });
    }

    private ServiceProvider BuildProvider(
        string schema,
        Action<SqlServerAsyncResponseChannelOptions>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(EnabledLogger<>));
        services.AddSingleton<IDirectSqlServerRecoveryFlow, DirectRecoveryFlow>();
        services.AddSingleton(provider => (DirectRecoveryFlow)provider.GetRequiredService<IDirectSqlServerRecoveryFlow>());
        services.AddAsyncResponse().WithSqlServerChannel(options =>
        {
            ApplyChannelOptions(options, schema);
            configure?.Invoke(options);
        });
        return services.BuildServiceProvider();
    }

    private async Task WithSchemaAsync(string prefix, Func<string, Task> body)
    {
        var schema = NewSchema(prefix);
        try
        {
            await body(schema);
        }
        finally
        {
            await DropSchemaAsync(schema);
        }
    }

    private SqlServerAsyncResponseChannelOptions ChannelOptions(string schema)
    {
        var options = new SqlServerAsyncResponseChannelOptions();
        ApplyChannelOptions(options, schema);
        return options;
    }

    private void ApplyChannelOptions(SqlServerAsyncResponseChannelOptions options, string schema)
    {
        options.ConnectionString = Fixture.SqlServerConnectionString;
        options.SchemaName = schema;
        options.DefaultTimeout = TimeSpan.FromSeconds(5);
        options.RecoveryStateExpiry = TimeSpan.FromSeconds(30);
        options.MessageRetention = TimeSpan.FromSeconds(30);
        options.DeliveryConfirmationTimeout = TimeSpan.FromMilliseconds(250);
        options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(10);
        options.ActivePollInterval = TimeSpan.FromMilliseconds(25);
        options.IdlePollInterval = TimeSpan.FromMilliseconds(100);
        options.PendingMessageBatchSize = 32;
        options.SubscriberHeartbeatInterval = TimeSpan.FromMilliseconds(50);
        options.SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(1);
        options.PruneInterval = TimeSpan.Zero;
    }

    private SqlServerAsyncResponseTransportOptions TransportOptions(string schema)
    {
        var options = new SqlServerAsyncResponseTransportOptions
        {
            ConnectionString = Fixture.SqlServerConnectionString,
            SchemaName = schema,
            WorkerQueue = $"{schema}_worker",
            ResponseQueue = $"{schema}_response",
            DeadLetterQueue = $"{schema}_deadletter",
            LockTimeout = TimeSpan.FromMilliseconds(200),
            DeadLetterRetention = TimeSpan.FromSeconds(30),
            SubscriberRetryBaseDelay = TimeSpan.FromMilliseconds(10),
            SubscriberRetryMaxDelay = TimeSpan.FromMilliseconds(50)
        };

        options.WorkerSubscriber.BatchSize = 4;
        options.WorkerSubscriber.EmptyPollDelay = TimeSpan.FromMilliseconds(25);
        options.WorkerSubscriber.RedeliveryDelay = TimeSpan.FromMilliseconds(25);
        options.WorkerSubscriber.MaxDeliveryAttempts = 2;
        options.ResponseSubscriber.BatchSize = 4;
        options.ResponseSubscriber.EmptyPollDelay = TimeSpan.FromMilliseconds(25);
        options.ResponseSubscriber.RedeliveryDelay = TimeSpan.FromMilliseconds(25);
        options.ResponseSubscriber.MaxDeliveryAttempts = 2;
        return options;
    }

    private async Task DropSchemaAsync(string schema)
    {
        // SQL Server has no DROP SCHEMA ... CASCADE: drop the schema's tables first, then the schema.
        await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            """
            DECLARE @drop nvarchar(max) = N'';
            SELECT @drop += N'DROP TABLE ' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name) + N';'
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schema;
            IF SCHEMA_ID(@schema) IS NOT NULL
                SET @drop += N'DROP SCHEMA ' + QUOTENAME(@schema) + N';';
            EXEC sp_executesql @drop;
            """;
        command.Parameters.AddWithValue("@schema", schema);
        await command.ExecuteNonQueryAsync();
    }

    private async Task InsertUnreadableRecoveryStateAsync(string schema, string recoveryTable, string correlationId)
    {
        await using var connection = new SqlConnection(Fixture.SqlServerConnectionString);
        await connection.OpenAsync();
        await using var command = connection.CreateCommand();
        command.CommandText =
            $"""
            INSERT INTO {Quote(schema)}.{Quote(recoveryTable)}
                (correlation_id, registration_id, state_json, expires_at, registered_at)
            VALUES (@correlation_id, @registration_id, N'"bad-json-string"', DATEADD(SECOND, 30, SYSUTCDATETIME()), SYSUTCDATETIME());
            """;
        command.Parameters.AddWithValue("@correlation_id", correlationId);
        command.Parameters.AddWithValue("@registration_id", Guid.NewGuid());
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<bool> AckAndMatchAttemptAsync(SqlServerTransportDelivery delivery, int attempt)
    {
        var matched = delivery.Attempt == attempt;
        await delivery.AckAsync();
        return matched;
    }

    private static async Task DrainQueueAsync(SqlServerTransportStore store, string queue, TimeSpan lockTimeout)
    {
        while (await store.TryClaimAsync(queue, lockTimeout, CancellationToken.None) is { } delivery)
            await delivery.AckAsync();
    }

    private static Task ArmRecoveryStateAsync(IRecoveryStateStore store, string correlationId)
        => store.SaveAsync(correlationId, new RecoveryState
        {
            CorrelationId = correlationId,
            PayloadTypeFullName = typeof(OperationResult).FullName,
            ResumeCallback = ResumeCallback(),
            FailureCallback = FailureCallback()
        }, TimeSpan.FromSeconds(30));

    private static async Task EventuallyAsync(Func<Task<bool>> condition)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (!await condition())
        {
            await Task.Delay(25, cts.Token);
            cts.Token.ThrowIfCancellationRequested();
        }
    }

    private static ReflectionCallDto ResumeCallback() => new()
    {
        ServiceInterfaceFullName = typeof(IDirectSqlServerRecoveryFlow).FullName!,
        MethodName = nameof(IDirectSqlServerRecoveryFlow.ResumeAsync),
        Params =
        [
            CallbackParam.ForPlaceholder(PlaceholderType.Payload),
            CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId)
        ]
    };

    private static ReflectionCallDto FailureCallback() => new()
    {
        ServiceInterfaceFullName = typeof(IDirectSqlServerRecoveryFlow).FullName!,
        MethodName = nameof(IDirectSqlServerRecoveryFlow.FailAsync),
        Params =
        [
            CallbackParam.ForPlaceholder(PlaceholderType.Exception),
            CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId)
        ]
    };

    private static string NewSchema(string prefix)
        => $"ar_{prefix}_{Guid.NewGuid():N}";

    private static string Quote(string identifier) => "[" + identifier + "]";

    private static string SuccessEnvelope(string message)
        => $$"""{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"{{message}}"},"ExceptionMessage":null,"ExceptionStackTrace":null}""";

    [Fact]
    public async Task SqlServerAsyncResponseChannel_CoverInternalEdgeCases()
    {
        await WithSchemaAsync("channel_edges", async schema =>
        {
            var services = new ServiceCollection();
            services.AddLogging();
            var options = ChannelOptions(schema);
            options.DeliveryConfirmationTimeout = TimeSpan.FromMilliseconds(10);
            options.DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(5);
            
            services.AddSingleton(Options.Create(options));
            var sql = new SqlServerChannelSql(Options.Create(options));
            services.AddSingleton(sql);
            services.AddSingleton(MockRecoveryStore());
            services.AddSingleton(new AsyncResponseContextPropagation([]));
            services.AddSingleton<SqlServerAsyncResponseChannel>();
            
            await using var provider = services.BuildServiceProvider();
            var channel = provider.GetRequiredService<SqlServerAsyncResponseChannel>();
            
            // Cover EnsureCreatedAsync double call (first/second return)
            await sql.EnsureCreatedAsync();
            await sql.EnsureCreatedAsync();

            var subscription1 = Subscription(typeof(SqlServerAsyncResponseChannel), "SqlServerSubscription`1", channel, _ => new ValueTask<bool>(true));
            var subscription2 = Subscription(typeof(SqlServerAsyncResponseChannel), "SqlServerSubscription`1", channel, _ => new ValueTask<bool>(true));
            var subscription3 = Subscription(typeof(SqlServerAsyncResponseChannel), "SqlServerSubscription`1", channel, _ => new ValueTask<bool>(true));
            
            SetField(subscription1.Instance, "_dropped", true);

            var addSubMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("AddSubscription", BindingFlags.Instance | BindingFlags.NonPublic)!;
            addSubMethod.Invoke(channel, ["corr", subscription1.Instance]);
            addSubMethod.Invoke(channel, ["corr", subscription2.Instance]);
            addSubMethod.Invoke(channel, ["corr", subscription3.Instance]);

            // 1. Cover DispatchPendingMessagesAsync where subscriptions.Count == 0
            var channelClean = provider.GetRequiredService<SqlServerAsyncResponseChannel>();
            var subsField = typeof(SqlServerAsyncResponseChannel).GetField("_subscriptions", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var subsDict = (System.Collections.IDictionary)subsField.GetValue(channelClean)!;
            subsDict.Clear();
            
            addSubMethod.Invoke(channelClean, ["corr-dropped-only", subscription1.Instance]);
            
            var dispatchPendingMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("DispatchPendingMessagesAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var scope = new HashSet<string> { "corr-dropped-only" };
            await (Task)dispatchPendingMethod.Invoke(channelClean, [scope, CancellationToken.None])!;

            // 2. Cover WaitForAcknowledgementAsync break branch and pollDelay branches
            var beginConfirmationMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("BeginConfirmation", BindingFlags.Instance | BindingFlags.NonPublic)!;
            var tryConfirmMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("TryConfirmDeliveryAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
            
            var messageId = Guid.NewGuid();
            await sql.InsertMessageAsync(messageId, "corr", "{}", TimeSpan.FromMinutes(1), CancellationToken.None);
            
            var confirmation = beginConfirmationMethod.Invoke(channel, [messageId])!;
            await (Task)tryConfirmMethod.Invoke(channel, [confirmation, CancellationToken.None])!;

            // 3. Cover DispatchMessageToSubscribersAsync continue branch (dropped & seen & live)
            var messageId2 = Guid.NewGuid();
            // Cover PK violation unique constraint catch block
            await sql.InsertMessageAsync(messageId2, "corr", "{}", TimeSpan.FromMinutes(1), CancellationToken.None);
            await sql.InsertMessageAsync(messageId2, "corr", "{}", TimeSpan.FromMinutes(1), CancellationToken.None);

            subscription2.Instance.GetType().GetMethod("MarkSeen", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!
                .Invoke(subscription2.Instance, [messageId2]);

            var message2 = new SqlServerChannelMessage(messageId2, "corr", "{}", DateTimeOffset.UtcNow);
            var dispatchMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("DispatchMessageToSubscribersAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
            
            var subInterfaceType = typeof(SqlServerAsyncResponseChannel).GetNestedType("ISqlServerSubscription", BindingFlags.NonPublic)!;
            var subArray = Array.CreateInstance(subInterfaceType, 3);
            subArray.SetValue(subscription1.Instance, 0); // Dropped
            subArray.SetValue(subscription2.Instance, 1); // Already seen
            subArray.SetValue(subscription3.Instance, 2); // Live (covers ProcessUnderCapturedContextAsync)
            
            await (Task)dispatchMethod.Invoke(channel, [message2, subArray, CancellationToken.None])!;

            // Start dispatcher to cover its background task dispose/cancellation
            var ensureDispatcherStartedMethod = typeof(SqlServerAsyncResponseChannel).GetMethod("EnsureDispatcherStarted", BindingFlags.Instance | BindingFlags.NonPublic)!;
            ensureDispatcherStartedMethod.Invoke(channel, null);

            await channel.DisposeAsync();
            await channelClean.DisposeAsync();
        });
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
        SetField(instance, "_cleanupStarted", 1);
        return (instance, completion);
    }

    private static void SetField(object target, string name, object value)
        => target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(target, value);

    private static IRecoveryStateStore MockRecoveryStore() => new FakeRecoveryStore();

    private sealed class FakeRecoveryStore : IRecoveryStateStore
    {
        public Task SaveAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default) => Task.FromResult<IReadOnlyList<RecoveryState>>([]);
        public Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default) => Task.FromResult(true);
    }

    private interface IDirectSqlServerRecoveryFlow
    {
        Task ResumeAsync(OperationResult payload, string correlationId);
        Task FailAsync(Exception exception, string correlationId);
    }

    private sealed class DirectRecoveryFlow : IDirectSqlServerRecoveryFlow
    {
        private readonly ConcurrentDictionary<string, TaskCompletionSource<OperationResult>> _resumes = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<string, TaskCompletionSource<Exception>> _failures = new(StringComparer.Ordinal);

        public Task<OperationResult> WaitResumeAsync(string correlationId)
            => _resumes.GetOrAdd(correlationId, _ => NewSource<OperationResult>()).Task;

        public Task<Exception> WaitFailureAsync(string correlationId)
            => _failures.GetOrAdd(correlationId, _ => NewSource<Exception>()).Task;

        public Task ResumeAsync(OperationResult payload, string correlationId)
        {
            _resumes.GetOrAdd(correlationId, _ => NewSource<OperationResult>()).TrySetResult(payload);
            return Task.CompletedTask;
        }

        public Task FailAsync(Exception exception, string correlationId)
        {
            _failures.GetOrAdd(correlationId, _ => NewSource<Exception>()).TrySetResult(exception);
            return Task.CompletedTask;
        }

        private static TaskCompletionSource<T> NewSource<T>()
            => new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class RecordingIngress : IAsyncResponseIngress
    {
        public TaskCompletionSource<string> WorkerReceived { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource<(string Json, string? CorrelationId)> ResponseReceived { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task HandleResponseMessageAsync(string messageJson, string? correlationId = null)
        {
            ResponseReceived.TrySetResult((messageJson, correlationId));
            return Task.CompletedTask;
        }

        public Task HandleWorkerMessageAsync(string messageJson)
        {
            WorkerReceived.TrySetResult(messageJson);
            return Task.CompletedTask;
        }
    }

    private sealed class DefaultRecoveryPayload : IAsyncResponsePayload
    {
    }

    private sealed class EnabledLogger<T> : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
        }
    }
}
