using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Focused coverage for <see cref="IAsyncResponsePublisher.SetException"/> on the process-local
/// channel: live subscribers, ambient correlation fallback, fan-out, and lost-subscriber recovery.
/// </summary>
public sealed class InMemorySetExceptionTests
{
    [Fact]
    public async Task SetException_WithExplicitCorrelation_FaultsActiveWaiterWithWireParityException()
    {
        await using var provider = CreateProvider();
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        InvalidOperationException expected;
        try
        {
            throw new InvalidOperationException("remote technical error");
        }
        catch (InvalidOperationException thrownSource)
        {
            expected = thrownSource;
        }

        // Wire parity: every durable channel serializes only the message (+ capped stack trace in
        // Data["RemoteStackTrace"]) and faults the waiter with a plain Exception — the concrete
        // type never crosses the wire. Handing the publisher's live instance through let a typed
        // `catch` pass against this channel that can never match in production.
        var thrown = await Assert.ThrowsAsync<Exception>(() =>
            asyncResponse
                .For<OperationResult>()
                .WithTimeout(TimeSpan.FromSeconds(5))
                .WaitAsync(ctx => publisher.SetException(expected, ctx.CorrelationId)));

        Assert.NotSame(expected, thrown);
        Assert.Equal(expected.Message, thrown.Message);
        Assert.NotNull(thrown.Data["RemoteStackTrace"]);
    }

    [Fact]
    public async Task SetException_PublishesToExplicitCorrelationId()
    {
        await using var provider = CreateProvider();
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var correlationId = $"ambient-{Guid.NewGuid():N}";
        var expected = new InvalidOperationException("ambient technical error");

        var waitTask = asyncResponse
            .For<OperationResult>(correlationId)
            .WithTimeout(TimeSpan.FromSeconds(5))
            .WaitAsync();

        Assert.True(await WaitForSubscriberCountAsync(probe, correlationId, expected: 1));

        await publisher.SetException(expected, correlationId);

        // Wire parity: the waiter observes the type-erased failure shape production channels
        // produce — a plain Exception carrying only the message.
        var thrown = await Assert.ThrowsAsync<Exception>(async () => await waitTask);
        Assert.Equal(expected.Message, thrown.Message);
    }

    [Fact]
    public async Task SetException_WithMultipleWaitersForSameCorrelation_FaultsAllWaiters()
    {
        await using var provider = CreateProvider();
        var asyncResponse = provider.GetRequiredService<IAsyncResponseBuilder>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var probe = provider.GetRequiredService<IActiveSubscriberProbe>();
        var correlationId = $"shared-{Guid.NewGuid():N}";
        var expected = new ApplicationException("shared failure");

        var first = asyncResponse.For<OperationResult>(correlationId).WithTimeout(TimeSpan.FromSeconds(5)).WaitAsync();
        var second = asyncResponse.For<OperationResult>(correlationId).WithTimeout(TimeSpan.FromSeconds(5)).WaitAsync();

        Assert.True(await WaitForSubscriberCountAsync(probe, correlationId, expected: 2));

        await publisher.SetException(expected, correlationId);

        // Wire parity: each waiter observes its own type-erased failure, exactly as it would
        // materializing a failure envelope off a durable channel.
        var firstException = await Assert.ThrowsAsync<Exception>(async () => await first);
        var secondException = await Assert.ThrowsAsync<Exception>(async () => await second);
        Assert.Equal(expected.Message, firstException.Message);
        Assert.Equal(expected.Message, secondException.Message);
        Assert.True(await WaitForSubscriberCountAsync(probe, correlationId, expected: 0));
    }

    [Fact]
    public async Task SetException_NoSubscriber_InvokesFailureCallbackAndDeletesRecoveryState()
    {
        var spy = new RecoverySpy();
        await using var provider = CreateProvider(services => services.AddSingleton<IRecoverySpy>(spy));
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var correlationId = $"lost-{Guid.NewGuid():N}";
        var expected = new InvalidOperationException("late technical error");

        await store.SaveAsync(
            correlationId,
            new RecoveryState
            {
                CorrelationId = correlationId,
                RegisteredAtUtc = DateTime.UtcNow,
                FailureCallback = FailureCallback()
            },
            TimeSpan.FromMinutes(5));

        await publisher.SetException(expected, correlationId);

        var failure = Assert.Single(spy.Failures);
        Assert.Same(expected, failure);
        Assert.Empty(await store.GetAllAsync(correlationId));
    }

    [Fact]
    public async Task SetException_NoSubscriberWithoutFailureCallback_KeepsRecoveryState()
    {
        await using var provider = CreateProvider();
        var store = provider.GetRequiredService<IRecoveryStateStore>();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var correlationId = $"lost-no-callback-{Guid.NewGuid():N}";

        await store.SaveAsync(
            correlationId,
            new RecoveryState
            {
                CorrelationId = correlationId,
                RegisteredAtUtc = DateTime.UtcNow
            },
            TimeSpan.FromMinutes(5));

        await publisher.SetException(new InvalidOperationException("late technical error"), correlationId);

        Assert.NotEmpty(await store.GetAllAsync(correlationId));
    }

    [Fact]
    public async Task SetException_NullException_ThrowsBeforePublishing()
    {
        await using var provider = CreateProvider();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        await Assert.ThrowsAsync<ArgumentNullException>(() => publisher.SetException(null!, "cid"));
    }

    [Fact]
    public async Task SetException_WhenSubscriberAppearsDuringRecoveryRead_DeliversLive()
    {
        // Same snapshot race the response path re-checks: a waiter registers between the empty
        // subscriber snapshot and the recovery-state read. The exception must be delivered live
        // instead of consuming the registration (or, with no failure callback, silently dropping).
        var services = new ServiceCollection().BuildServiceProvider();
        var correlationId = $"retry-live-exception-{Guid.NewGuid():N}";
        var store = new RetryLiveExceptionRecoveryStore(correlationId);
        var channel = new InMemoryAsyncResponseChannel(
            services.GetRequiredService<IServiceScopeFactory>(),
            store,
            Microsoft.Extensions.Options.Options.Create(new InMemoryAsyncResponseOptions
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
                correlationId,
                timeout: TimeSpan.FromSeconds(5));
        };
        var expected = new InvalidOperationException("late but live");

        await channel.SetException(expected, correlationId);

        var liveWaiter = waiter ?? throw new InvalidOperationException("Retry-live waiter was not created.");
        await using (liveWaiter)
        {
            // Wire parity: live delivery faults the waiter with the type-erased failure shape.
            var thrown = await Assert.ThrowsAsync<Exception>(() =>
                liveWaiter.ResponseTask.WaitAsync(TimeSpan.FromSeconds(2)));
            Assert.Equal(expected.Message, thrown.Message);
        }
    }

    private sealed class RetryLiveExceptionRecoveryStore(string _correlationId) : IRecoveryStateStore
    {
        public Func<Task>? BeforeGetAllAsync { get; set; }

        public Task SaveAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public async Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default)
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
                    CorrelationId = _correlationId,
                    PayloadTypeFullName = typeof(OperationResult).FullName
                }
            ];
        }

        public Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default)
            => Task.FromResult(true);
    }

    private static ServiceProvider CreateProvider(Action<IServiceCollection>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse(options => options.Watchdog.Enabled = false)
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

    private static async Task<bool> WaitForSubscriberCountAsync(
        IActiveSubscriberProbe probe,
        string correlationId,
        long expected)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            var count = await probe.CountActiveSubscribersAsync(correlationId);
            if (expected == 0 ? count == 0 : count >= expected)
                return true;

            await Task.Delay(25);
        }

        return false;
    }
}
