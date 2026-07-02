using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Verifies the default no-Redis setup: process-local response channel plus process-local
/// recovery store.
/// </summary>
public class InMemoryAsyncResponseTests
{
    private const string CorrelationId = "in-memory-correlation-id";

    [Fact]
    public async Task AddAsyncResponse_CompletesWaiterThroughTransportNeutralIngress()
    {
        var provider = CreateProvider();
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        var result = await asyncResponse
            .For<OperationResult>()
            .WithTimeout(TimeSpan.FromSeconds(5))
            .Until(response => response.Status != OperationStatus.Running)
            .WaitAsync(async context =>
            {
                await ingress.HandleResponseMessageAsync(
                    JsonSerializer.Serialize(new OperationResult { Status = OperationStatus.Running }),
                    context.CorrelationId);

                await ingress.HandleResponseMessageAsync(
                    JsonSerializer.Serialize(new OperationResult { Status = OperationStatus.Completed, Message = "done" }),
                    context.CorrelationId);
            });

        Assert.Equal(OperationStatus.Completed, result.Status);
        Assert.Equal("done", result.Message);
    }

    [Fact]
    public async Task AddAsyncResponse_NoSubscriber_UsesRecoveryStoreCallbacks()
    {
        var spy = new RecoverySpy();
        var provider = CreateProvider(services => services.AddSingleton<IRecoverySpy>(spy));
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        await store.SaveAsync(
            CorrelationId,
            new RecoveryState
            {
                CorrelationId = CorrelationId,
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                ResumeCallback = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
                    MethodName = nameof(IRecoverySpy.OnResume),
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
                },
                FailureCallback = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
                    MethodName = nameof(IRecoverySpy.OnFailure),
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
                }
            },
            TimeSpan.FromMinutes(5));

        await publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "recovered" },
            CorrelationId);

        var resumed = Assert.IsType<OperationResult>(Assert.Single(spy.ResumedPayloads));
        Assert.Equal("recovered", resumed.Message);
        Assert.Empty(spy.Failures);
        Assert.Null(await store.GetAsync(CorrelationId));
    }

    [Fact]
    public async Task CreateResponseWaiter_TinyTimeout_DoesNotLeakCleanedSubscription()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var correlationId = $"{CorrelationId}-tiny-timeout";

        var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            timeout: TimeSpan.FromTicks(1));

        try
        {
            await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask);
        }
        finally
        {
            await waiter.DisposeAsync();
        }

        Assert.Equal(0, await probe.CountActiveSubscribersAsync(correlationId));
        Assert.Null(await store.GetAsync(correlationId));
    }

    [Fact]
    public async Task MultipleWaiters_SameCorrelation_AllCompleteAndCleanup()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var correlationId = $"{CorrelationId}-fanout";

        await using var first = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            timeout: TimeSpan.FromSeconds(5));
        await using var second = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            timeout: TimeSpan.FromSeconds(5));

        Assert.Equal(2, await probe.CountActiveSubscribersAsync(correlationId));

        await publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "done" },
            correlationId);

        Assert.Equal("done", (await first.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
        Assert.Equal("done", (await second.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
        Assert.Equal(0, await probe.CountActiveSubscribersAsync(correlationId));
        Assert.Null(await store.GetAsync(correlationId));
    }

    [Fact]
    public async Task SetResponse_WhenSubscriberAppearsDuringRecoveryRead_DeliversLive()
    {
        var services = new ServiceCollection().BuildServiceProvider();
        var store = new RetryLiveRecoveryStore();
        var channel = new InMemoryAsyncResponseChannel(
            services.GetRequiredService<IServiceScopeFactory>(),
            store,
            Options.Create(new InMemoryAsyncResponseOptions
            {
                DefaultTimeout = TimeSpan.FromSeconds(5),
                RecoveryStateExpiry = TimeSpan.FromMinutes(5)
            }),
            new AsyncResponseContextPropagation([]),
            NullLogger<InMemoryAsyncResponseChannel>.Instance);
        IAsyncResponseWaiter<OperationResult>? waiter = null;
        store.BeforeGetAllAsync = async () =>
        {
            waiter ??= await channel.CreateResponseWaiter<OperationResult>(
                CorrelationId,
                timeout: TimeSpan.FromSeconds(5));
        };

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "live" }, CorrelationId);

        var liveWaiter = waiter ?? throw new InvalidOperationException("Retry-live waiter was not created.");
        await using (liveWaiter)
        {
            Assert.Equal("live", (await liveWaiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
        }
    }

    [Fact]
    public async Task RawJsonResponse_WhenSubscriberAppearsDuringRecoveryRead_DeliversLive()
    {
        var services = new ServiceCollection().BuildServiceProvider();
        var store = new RetryLiveRecoveryStore();
        var channel = new InMemoryAsyncResponseChannel(
            services.GetRequiredService<IServiceScopeFactory>(),
            store,
            Options.Create(new InMemoryAsyncResponseOptions
            {
                DefaultTimeout = TimeSpan.FromSeconds(5),
                RecoveryStateExpiry = TimeSpan.FromMinutes(5)
            }),
            new AsyncResponseContextPropagation([]),
            NullLogger<InMemoryAsyncResponseChannel>.Instance);
        var rawPublisher = (IRawAsyncResponsePublisher)channel;
        IAsyncResponseWaiter<OperationResult>? waiter = null;
        store.BeforeGetAllAsync = async () =>
        {
            waiter ??= await channel.CreateResponseWaiter<OperationResult>(
                CorrelationId,
                timeout: TimeSpan.FromSeconds(5));
        };

        await rawPublisher.SetRawResponseJson("""{"Status":2,"Message":"live raw"}""", CorrelationId);

        var liveWaiter = waiter ?? throw new InvalidOperationException("Retry-live waiter was not created.");
        await using (liveWaiter)
        {
            Assert.Equal("live raw", (await liveWaiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
        }
    }

    [Fact]
    public async Task CompletingOneWaiter_RemovesOnlyItsRecoveryRegistration()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var correlationId = $"{CorrelationId}-partial-cleanup";

        await using var first = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            completionPredicate: payload => new ValueTask<bool>(payload.Status == OperationStatus.Running),
            timeout: TimeSpan.FromSeconds(5));
        await using var second = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            completionPredicate: payload => new ValueTask<bool>(payload.Status == OperationStatus.Completed),
            timeout: TimeSpan.FromSeconds(5));

        Assert.Equal(2, (await store.GetAllAsync(correlationId)).Count);

        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Running }, correlationId);
        Assert.Equal(OperationStatus.Running, (await first.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Status);
        await Eventually(async () => (await store.GetAllAsync(correlationId)).Count == 1);

        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, correlationId);
        Assert.Equal(OperationStatus.Completed, (await second.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Status);
        await Eventually(async () => (await store.GetAllAsync(correlationId)).Count == 0);
    }

    [Fact]
    public async Task AsyncCompletionPredicate_FalseThenTrue_CompletesOnLaterResponse()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var correlationId = $"{CorrelationId}-async-predicate";

        await using var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            completionPredicate: async payload =>
            {
                await Task.Yield();
                return payload.Status == OperationStatus.Completed;
            },
            timeout: TimeSpan.FromSeconds(5));

        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Running }, correlationId);
        await Task.Delay(50);
        Assert.False(waiter.ResponseTask.IsCompleted);

        await publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "done" },
            correlationId);

        Assert.Equal("done", (await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
    }

    [Fact]
    public async Task AsyncCompletionPredicate_WhenItThrows_FaultsWaiterAndCleansUp()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var correlationId = $"{CorrelationId}-predicate-throws";

        await using var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            completionPredicate: async _ =>
            {
                await Task.Yield();
                throw new InvalidOperationException("predicate failed");
            },
            timeout: TimeSpan.FromSeconds(5));

        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, correlationId);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal("predicate failed", ex.Message);
        Assert.Equal(0, await probe.CountActiveSubscribersAsync(correlationId));
    }

    [Fact]
    public async Task RawObjectResponse_WhenMaterializationFails_FaultsWaiterAndCleansUp()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var correlationId = $"{CorrelationId}-raw-object-invalid";

        await using var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            timeout: TimeSpan.FromSeconds(5));

        await rawPublisher.SetRawResponse("not-json", correlationId);

        await Assert.ThrowsAsync<JsonException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal(0, await probe.CountActiveSubscribersAsync(correlationId));
    }

    [Fact]
    public async Task RawJsonResponse_AsyncCompletionPredicate_CompletesFromRawPublisher()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
        var correlationId = $"{CorrelationId}-raw";

        await using var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            completionPredicate: async payload =>
            {
                await Task.Yield();
                return payload.Status == OperationStatus.Completed;
            },
            timeout: TimeSpan.FromSeconds(5));

        await rawPublisher.SetRawResponseJson("""{"Status":2,"Message":"done"}""", correlationId);

        Assert.Equal("done", (await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
    }

    [Fact]
    public async Task RawJsonResponse_WhenCompletionPredicateThrows_FaultsWaiter()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var correlationId = $"{CorrelationId}-raw-predicate-throws";

        await using var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            completionPredicate: _ => throw new InvalidOperationException("raw predicate failed"),
            timeout: TimeSpan.FromSeconds(5));

        await rawPublisher.SetRawResponseJson("""{"Status":2,"Message":"done"}""", correlationId);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal("raw predicate failed", ex.Message);
        Assert.Equal(0, await probe.CountActiveSubscribersAsync(correlationId));
    }

    [Fact]
    public async Task RawObjectResponse_CompletesWaiterThroughRawPublisher()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var correlationId = $"{CorrelationId}-raw-object";

        await using var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            timeout: TimeSpan.FromSeconds(5));

        await rawPublisher.SetRawResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "raw object" },
            correlationId);

        Assert.Equal("raw object", (await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
        Assert.Equal(0, await probe.CountActiveSubscribersAsync(correlationId));
    }

    [Fact]
    public async Task RawJsonResponse_WithMultipleAsyncWaiters_FansOutAndWaitsForAll()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var correlationId = $"{CorrelationId}-raw-fanout";
        var predicateCalls = 0;

        async ValueTask<bool> IsCompleteAsync(OperationResult payload)
        {
            Interlocked.Increment(ref predicateCalls);
            await Task.Yield();
            return payload.Status == OperationStatus.Completed;
        }

        await using var first = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            completionPredicate: IsCompleteAsync,
            timeout: TimeSpan.FromSeconds(5));
        await using var second = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            completionPredicate: IsCompleteAsync,
            timeout: TimeSpan.FromSeconds(5));

        Assert.Equal(2, await probe.CountActiveSubscribersAsync(correlationId));

        await rawPublisher.SetRawResponseJson("""{"Status":2,"Message":"fanout"}""", correlationId);

        Assert.Equal("fanout", (await first.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
        Assert.Equal("fanout", (await second.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
        Assert.Equal(2, predicateCalls);
        Assert.Equal(0, await probe.CountActiveSubscribersAsync(correlationId));
    }

    [Fact]
    public async Task LivePublishBranches_LogAndDeliverResponseRawAndException()
    {
        var provider = CreateProvider(services => services.AddSingleton(typeof(ILogger<>), typeof(TestLogger<>)));
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();

        await using var responseWaiter = await subscriber.CreateResponseWaiter<OperationResult>(
            $"{CorrelationId}-logged-response",
            timeout: TimeSpan.FromSeconds(5));
        await publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "logged" },
            $"{CorrelationId}-logged-response");
        Assert.Equal("logged", (await responseWaiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);

        await using var rawWaiter = await subscriber.CreateResponseWaiter<OperationResult>(
            $"{CorrelationId}-logged-raw",
            timeout: TimeSpan.FromSeconds(5));
        await rawPublisher.SetRawResponseJson("""{"Status":2,"Message":"logged raw"}""", $"{CorrelationId}-logged-raw");
        Assert.Equal("logged raw", (await rawWaiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);

        await using var exceptionWaiter = await subscriber.CreateResponseWaiter<OperationResult>(
            $"{CorrelationId}-logged-exception",
            timeout: TimeSpan.FromSeconds(5));
        await publisher.SetException(new InvalidOperationException("logged exception"), $"{CorrelationId}-logged-exception");
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            exceptionWaiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal("logged exception", ex.Message);
    }

    [Fact]
    public async Task RawJsonResponse_WhenMaterializationFails_FaultsWaiterAndCleansUp()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var correlationId = $"{CorrelationId}-raw-invalid";

        await using var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            timeout: TimeSpan.FromSeconds(5));

        await rawPublisher.SetRawResponseJson("""{"Status": }""", correlationId);

        await Assert.ThrowsAsync<InvalidDataException>(() => waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
        Assert.Equal(0, await probe.CountActiveSubscribersAsync(correlationId));
    }

    [Fact]
    public async Task SetResponse_AfterWaiterCleanup_DropsLateResponseWhenRecoveryStateIsGone()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var correlationId = $"{CorrelationId}-late-after-cleanup";

        await using var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            timeout: TimeSpan.FromSeconds(5));

        await publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "first" },
            correlationId);

        Assert.Equal("first", (await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
        Assert.Equal(0, await probe.CountActiveSubscribersAsync(correlationId));
        Assert.Null(await store.GetAsync(correlationId));

        await publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Failed, Message = "late" },
            correlationId);

        Assert.Null(await store.GetAsync(correlationId));
    }

    [Fact]
    public async Task SetResponse_NoSubscriberWhenPayloadJsonCannotBeSerialized_StillInvokesFailureCallback()
    {
        var spy = new RecoverySpy();
        var provider = CreateProvider(services => services.AddSingleton<IRecoverySpy>(spy));
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var correlationId = $"{CorrelationId}-unserializable-domain-failure";
        var payload = new SelfReferencingFailurePayload();
        payload.Self = payload;

        await store.SaveAsync(
            correlationId,
            new RecoveryState
            {
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(SelfReferencingFailurePayload).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                FailureCallback = FailureCallback()
            },
            TimeSpan.FromMinutes(5));

        await publisher.SetResponse(payload, correlationId);

        var failure = Assert.IsType<AsyncResponseDomainFailureException>(Assert.Single(spy.Failures));
        Assert.Equal(correlationId, failure.CorrelationId);
        Assert.Equal(typeof(SelfReferencingFailurePayload).FullName, failure.PayloadTypeFullName);
        Assert.Null(failure.PayloadJson);
        Assert.Null(await store.GetAsync(correlationId));
    }

    [Fact]
    public async Task RawObjectResponse_NoSubscriberWithNullPayload_InvokesFailureCallback()
    {
        var spy = new RecoverySpy();
        var provider = CreateProvider(services => services.AddSingleton<IRecoverySpy>(spy));
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
        var correlationId = $"{CorrelationId}-raw-null";

        await store.SaveAsync(
            correlationId,
            new RecoveryState
            {
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                FailureCallback = FailureCallback()
            },
            TimeSpan.FromMinutes(5));

        await rawPublisher.SetRawResponse(null, correlationId);

        var failure = Assert.IsType<AsyncResponseDomainFailureException>(Assert.Single(spy.Failures));
        Assert.Equal(correlationId, failure.CorrelationId);
        Assert.Equal("null", failure.PayloadJson);
        Assert.Null(await store.GetAsync(correlationId));
    }

    [Fact]
    public async Task Waiter_DisposeAsync_CleansRecoveryStateAndSubscription()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var correlationId = $"{CorrelationId}-async-dispose";

        var waiter = await subscriber.CreateResponseWaiter<OperationResult>(
            correlationId,
            timeout: TimeSpan.FromSeconds(5));

        await waiter.DisposeAsync();

        Assert.Equal(0, await probe.CountActiveSubscribersAsync(correlationId));
        Assert.Null(await store.GetAsync(correlationId));
    }

    [Fact]
    public async Task Waiter_DisposeAsync_RemovesSubscriptionsFromManyGroup()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var correlationId = $"{CorrelationId}-many-dispose";

        await using var first = await subscriber.CreateResponseWaiter<OperationResult>(correlationId, timeout: TimeSpan.FromSeconds(5));
        await using var second = await subscriber.CreateResponseWaiter<OperationResult>(correlationId, timeout: TimeSpan.FromSeconds(5));
        await using var third = await subscriber.CreateResponseWaiter<OperationResult>(correlationId, timeout: TimeSpan.FromSeconds(5));

        Assert.Equal(3, await probe.CountActiveSubscribersAsync(correlationId));

        await second.DisposeAsync();
        Assert.Equal(2, await probe.CountActiveSubscribersAsync(correlationId));

        await first.DisposeAsync();
        Assert.Equal(1, await probe.CountActiveSubscribersAsync(correlationId));

        await third.DisposeAsync();
        Assert.Equal(0, await probe.CountActiveSubscribersAsync(correlationId));
    }

    [Fact]
    public async Task CreateResponseWaiter_RejectsInvalidCorrelationAndTimeout()
    {
        var provider = CreateProvider();
        var subscriber = provider.GetRequiredService<IAsyncResponseSubscriber>();

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            subscriber.CreateResponseWaiter<OperationResult>(" "));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            subscriber.CreateResponseWaiter<OperationResult>(CorrelationId, timeout: TimeSpan.Zero));
    }

    [Fact]
    public async Task Publishers_WithBlankCorrelationId_AreNoops()
    {
        var provider = CreateProvider();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();

        await publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, " ");
        await rawPublisher.SetRawResponseJson("""{"Status":2}""", " ");
        await publisher.SetException(new InvalidOperationException("missing correlation"), " ");

        Assert.Equal(0, await probe.CountActiveSubscribersAsync(" "));
    }

    [Fact]
    public async Task CreateResponseWaiter_WhenRecoverySaveFails_ThrowsAndCleansSubscription()
    {
        var services = new ServiceCollection().BuildServiceProvider();
        var store = new Mock<IRecoveryStateStore>();
        var failure = new InvalidOperationException("store unavailable");
        store
            .Setup(s => s.SaveAsync(
                It.IsAny<string>(),
                It.IsAny<RecoveryState>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(failure);
        store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var channel = new InMemoryAsyncResponseChannel(
            services.GetRequiredService<IServiceScopeFactory>(),
            store.Object,
            Options.Create(new InMemoryAsyncResponseOptions
            {
                DefaultTimeout = TimeSpan.FromSeconds(5),
                RecoveryStateExpiry = TimeSpan.FromMinutes(5)
            }),
            new AsyncResponseContextPropagation([]),
            NullLogger<InMemoryAsyncResponseChannel>.Instance);

        // Must throw rather than return a pre-faulted waiter: the builder's contract is that the
        // trigger only runs once the subscription AND recovery state exist, so a save failure has
        // to surface before any trigger could fire the remote operation.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            channel.CreateResponseWaiter<OperationResult>(CorrelationId, timeout: TimeSpan.FromSeconds(5)));
        Assert.Same(failure, ex);
        Assert.Equal(0, await channel.CountActiveSubscribersAsync(CorrelationId));
    }

    [Fact]
    public async Task CreateResponseWaiter_WhenResponseCompletesBeforeRecoverySaveFinishes_DeletesRegistrationAfterSave()
    {
        var services = new ServiceCollection().BuildServiceProvider();
        var saveStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseSave = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new Mock<IRecoveryStateStore>();
        store
            .Setup(s => s.SaveAsync(
                It.IsAny<string>(),
                It.IsAny<RecoveryState>(),
                It.IsAny<TimeSpan>(),
                It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                saveStarted.TrySetResult();
                await releaseSave.Task.ConfigureAwait(false);
            });
        store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        store
            .Setup(s => s.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var channel = new InMemoryAsyncResponseChannel(
            services.GetRequiredService<IServiceScopeFactory>(),
            store.Object,
            Options.Create(new InMemoryAsyncResponseOptions
            {
                DefaultTimeout = TimeSpan.FromSeconds(5),
                RecoveryStateExpiry = TimeSpan.FromMinutes(5)
            }),
            new AsyncResponseContextPropagation([]),
            NullLogger<InMemoryAsyncResponseChannel>.Instance);

        var waiterTask = channel.CreateResponseWaiter<OperationResult>(
            CorrelationId,
            timeout: TimeSpan.FromSeconds(5));
        await saveStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

        await channel.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "early" }, CorrelationId);
        releaseSave.TrySetResult();

        await using var waiter = await waiterTask.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("early", (await waiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2))).Message);
        store.Verify(s => s.TryDeleteAsync(CorrelationId, It.IsAny<Guid>(), It.IsAny<CancellationToken>()), Times.AtLeast(2));
    }

    [Fact]
    public async Task Publishers_WhenRecoveryReadFails_Propagate()
    {
        var services = new ServiceCollection().BuildServiceProvider();
        var failure = new InvalidOperationException("recovery read failed");
        var store = new Mock<IRecoveryStateStore>();
        store
            .Setup(s => s.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(failure);
        var channel = new InMemoryAsyncResponseChannel(
            services.GetRequiredService<IServiceScopeFactory>(),
            store.Object,
            Options.Create(new InMemoryAsyncResponseOptions
            {
                DefaultTimeout = TimeSpan.FromSeconds(5),
                RecoveryStateExpiry = TimeSpan.FromMinutes(5)
            }),
            new AsyncResponseContextPropagation([]),
            NullLogger<InMemoryAsyncResponseChannel>.Instance);
        var rawPublisher = (IRawAsyncResponsePublisher)channel;

        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() =>
            channel.SetResponse(new OperationResult { Status = OperationStatus.Completed }, $"{CorrelationId}-response")));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() =>
            rawPublisher.SetRawResponseJson("""{"Status":2}""", $"{CorrelationId}-raw")));
        Assert.Same(failure, await Assert.ThrowsAsync<InvalidOperationException>(() =>
            channel.SetException(new InvalidOperationException("remote failure"), $"{CorrelationId}-exception")));
    }

    [Fact]
    public async Task NoSubscriber_CompletedPayloadWithoutResumeCallback_DoesNotInvokeFailure()
    {
        var spy = new RecoverySpy();
        var provider = CreateProvider(services => services.AddSingleton<IRecoverySpy>(spy));
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var correlationId = $"{CorrelationId}-no-resume-callback";

        await store.SaveAsync(
            correlationId,
            new RecoveryState
            {
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow
            },
            TimeSpan.FromMinutes(5));

        await publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "done" },
            correlationId);

        Assert.Empty(spy.ResumedPayloads);
        Assert.Empty(spy.Failures);
        Assert.NotNull(await store.GetAsync(correlationId));
    }

    [Fact]
    public async Task NoSubscriber_ExceptionWithoutRecoveryState_IsDropped()
    {
        var provider = CreateProvider();
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var correlationId = $"{CorrelationId}-missing-exception-state";

        await publisher.SetException(new InvalidOperationException("remote failure"), correlationId);

        Assert.Null(await store.GetAsync(correlationId));
    }

    [Fact]
    public async Task NoSubscriber_MixedRecoveryRoutes_DispatchesBothCallbacks()
    {
        var spy = new RecoverySpy();
        var provider = CreateProvider(services => services.AddSingleton<IRecoverySpy>(spy));
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var rawPublisher = provider.GetRequiredService<IRawAsyncResponsePublisher>();
        var correlationId = $"{CorrelationId}-mixed-routes";

        await store.SaveAsync(
            correlationId,
            new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                ResumeCallback = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
                    MethodName = nameof(IRecoverySpy.OnResume),
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
                }
            },
            TimeSpan.FromMinutes(5));
        await store.SaveAsync(
            correlationId,
            new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = correlationId,
                PayloadTypeFullName = "Missing.Payload.Type",
                RegisteredAtUtc = DateTime.UtcNow,
                FailureCallback = FailureCallback()
            },
            TimeSpan.FromMinutes(5));

        await rawPublisher.SetRawResponseJson("""{"Status":2,"Message":"done"}""", correlationId);

        Assert.Single(spy.ResumedPayloads);
        Assert.Single(spy.Failures);
        Assert.Empty(await store.GetAllAsync(correlationId));
    }

    private static ServiceProvider CreateProvider(Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse()
            .WithInMemoryChannel(options =>
            {
                options.DefaultTimeout = TimeSpan.FromSeconds(5);
                options.RecoveryStateExpiry = TimeSpan.FromMinutes(5);
            });
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    private static ReflectionCallDto FailureCallback() => new()
    {
        ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
        MethodName = nameof(IRecoverySpy.OnFailure),
        Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
    };

    private static async Task Eventually(Func<Task<bool>> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
                return;

            await Task.Delay(10);
        }

        Assert.True(await condition());
    }

    private sealed class SelfReferencingFailurePayload : IAsyncResponsePayload
    {
        public SelfReferencingFailurePayload? Self { get; set; }

        public bool ShouldResumeOnRecovery() => false;
    }

    private sealed class RetryLiveRecoveryStore : IRecoveryStateStore
    {
        public Func<Task>? BeforeGetAllAsync { get; set; }

        public Task SaveAsync(
            string correlationId,
            RecoveryState state,
            TimeSpan ttl,
            CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<RecoveryState?> GetAsync(string correlationId, CancellationToken cancellationToken = default)
            => Task.FromResult<RecoveryState?>(null);

        public async Task<IReadOnlyList<RecoveryState>> GetAllAsync(
            string correlationId,
            CancellationToken cancellationToken = default)
        {
            if (BeforeGetAllAsync is not null)
            {
                var callback = BeforeGetAllAsync;
                BeforeGetAllAsync = null;
                await callback().ConfigureAwait(false);
            }

            return
            [
                new RecoveryState
                {
                    RegistrationId = Guid.NewGuid(),
                    CorrelationId = correlationId,
                    PayloadTypeFullName = typeof(OperationResult).FullName
                }
            ];
        }

        public Task<bool> TryDeleteAsync(string correlationId, CancellationToken cancellationToken = default)
            => Task.FromResult(true);

        public Task<bool> TryDeleteAsync(
            string correlationId,
            Guid registrationId,
            CancellationToken cancellationToken = default)
            => Task.FromResult(true);
    }
}
