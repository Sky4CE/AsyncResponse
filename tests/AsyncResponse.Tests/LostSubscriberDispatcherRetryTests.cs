using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The lost-subscriber dispatcher's failure-callback retry must run on the host's registered
/// <see cref="TimeProvider"/>. Regression (review fix): the dispatcher armed its transient-retry
/// backoff on the system clock even when the host registered a TimeProvider — the ingress half of
/// the same finding is pinned by
/// <c>AsyncResponseIngressErrorTests.HandleResponseMessageAsync_RetryBackoff_RunsOnTheInjectedTimeProvider</c>;
/// this is the dispatcher half, driven through the in-memory channel, which forwards its own
/// engine clock into the dispatcher it constructs.
/// </summary>
public sealed class LostSubscriberDispatcherRetryTests
{
    private const string CorrelationId = "dispatcher-retry-clock-correlation-id";

    [Fact]
    public async Task FailureCallbackRetryBackoff_RunsOnTheInjectedTimeProvider()
    {
        var time = new VirtualTimeProvider();
        var spy = new ThrowOnceFailureSpy();

        // Built through DI rather than direct construction because the fix under test is precisely
        // that the channel forwards the registered TimeProvider into its dispatcher.
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        // Registered BEFORE AddAsyncResponse: the library's TryAdd registrations yield to these.
        services.AddSingleton<TimeProvider>(time);
        services.AddSingleton<IThrowOnceFailureSpy>(spy);
        services.AddAsyncResponse().WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();

        // The registration a recoverable waiter persisted before its process died: no resume
        // callback needed — a Failed payload routes to the failure callback, whose first
        // invocation fails transiently and whose second succeeds.
        var recoveryStateStore = provider.GetRequiredService<IRecoveryStateStore>();
        await recoveryStateStore.SaveAsync(
            CorrelationId,
            new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = CorrelationId,
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                FailureCallback = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IThrowOnceFailureSpy).FullName!,
                    MethodName = nameof(IThrowOnceFailureSpy.OnFailure),
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
                }
            },
            TimeSpan.FromMinutes(5));

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var dispatching = publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Failed, Message = "remote step failed" },
            CorrelationId);

        // The first attempt failed and the retry is parked on the VIRTUAL clock: ample real time
        // passes and the dispatch must still be pending (the old code's 125-250ms system-clock
        // backoff would long since have fired and completed it).
        await Task.Delay(TimeSpan.FromMilliseconds(600));
        Assert.False(dispatching.IsCompleted, "the failure-callback retry backoff ran on the system clock instead of the injected TimeProvider");
        Assert.Equal(1, spy.Calls);

        // The backoff timer is armed on the virtual provider (nothing else arms timers here — no
        // waiters exist); advancing past the 2s backoff ceiling fires it deterministically.
        Assert.NotNull(time.NextTimerDueAt);
        time.Advance(TimeSpan.FromSeconds(2));
        await dispatching.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(2, spy.Calls);
        // The successful retry consumed the registration.
        Assert.Empty(await recoveryStateStore.GetAllAsync(CorrelationId));
    }

    [Fact]
    public async Task AFailureCallbackNamingATypeTheRuntimeRefusesToBuild_IsADeterministicFault_AcknowledgedWithoutRetry()
    {
        // Fixpoint r2 (S3#4): the default scan let the runtime's ArgumentException out for a name
        // it parses but cannot instantiate (Nullable<string>: a constraint violation), and the
        // dispatcher classified that as TRANSIENT — four in-process attempts on the backoff
        // ladder, then RecoveryCallbackFailedException and a transport redelivery, for a callback
        // that can never be wired up. It is now an unresolved type: acknowledged on the first
        // attempt, the registration kept for the watchdog.
        const string correlationId = "dispatcher-unbuildable-service-type";
        var time = new VirtualTimeProvider();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<TimeProvider>(time);
        services.AddAsyncResponse().WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();

        var recoveryStateStore = provider.GetRequiredService<IRecoveryStateStore>();
        await recoveryStateStore.SaveAsync(
            correlationId,
            new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(OperationResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                FailureCallback = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = "System.Nullable`1[[System.String, System.Private.CoreLib]]",
                    MethodName = "OnFailure",
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
                }
            },
            TimeSpan.FromMinutes(5));

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var dispatching = publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Failed, Message = "remote step failed" },
            correlationId);

        // No virtual time is advanced: a transient classification parks the second attempt on the
        // virtual clock and this never completes (the bound is only the red-path hang guard).
        await dispatching.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Null(time.NextTimerDueAt);
        Assert.NotEmpty(await recoveryStateStore.GetAllAsync(correlationId));
    }
}

/// <summary>
/// Stand-in for a flow's failure handler: resolved from DI by interface full name and invoked via
/// reflection, exactly like production failure callbacks. Fails its first invocation transiently.
/// </summary>
public interface IThrowOnceFailureSpy
{
    Task OnFailure(Exception exception);
}

public sealed class ThrowOnceFailureSpy : IThrowOnceFailureSpy
{
    private int _calls;

    public int Calls => Volatile.Read(ref _calls);

    public Task OnFailure(Exception exception)
        => Interlocked.Increment(ref _calls) == 1
            ? Task.FromException(new InvalidOperationException("transient dependency blip"))
            : Task.CompletedTask;
}
