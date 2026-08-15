using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression (r23): with two recovery registrations sharing one correlation id (the expected
/// shape when a worker dies mid-await and its replacement re-attaches), a failure in ONE
/// registration's resume callback was rethrown even though the OTHER registration had already
/// consumed the response and resumed its flow. The ingress escalated that throw through its
/// retry loop into SetException, terminally failing a flow that was correctly recovered moments
/// earlier. A partial success now completes the dispatch: the residual failure is logged and the
/// failed registration stays registered for redelivery/watchdog visibility.
/// </summary>
public sealed class LostSubscriberPartialSuccessTests
{
    private const string CorrelationId = "partial-success-correlation-id";

    public interface IPartialResumeSpy
    {
        Task ResumeOk(OperationResult payload);
        Task ResumeBoom(OperationResult payload);
    }

    private sealed class PartialResumeSpy : IPartialResumeSpy
    {
        private int _ok;
        private int _boom;

        public int Ok => Volatile.Read(ref _ok);
        public int Boom => Volatile.Read(ref _boom);

        public Task ResumeOk(OperationResult payload)
        {
            Interlocked.Increment(ref _ok);
            return Task.CompletedTask;
        }

        public Task ResumeBoom(OperationResult payload)
        {
            Interlocked.Increment(ref _boom);
            throw new InvalidOperationException("re-enqueue failed on a publish-blocked broker");
        }
    }

    [Fact]
    public async Task DispatchLostResponses_SiblingFailureAfterASuccessfulCallback_DoesNotEscalate()
    {
        var spy = new PartialResumeSpy();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IPartialResumeSpy>(spy);
        services.AddAsyncResponse().WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();

        var recoveryStateStore = provider.GetRequiredService<IRecoveryStateStore>();
        var okRegistration = Guid.NewGuid();
        var boomRegistration = Guid.NewGuid();
        await recoveryStateStore.SaveAsync(CorrelationId, Registration(okRegistration, nameof(IPartialResumeSpy.ResumeOk)), TimeSpan.FromMinutes(5));
        await recoveryStateStore.SaveAsync(CorrelationId, Registration(boomRegistration, nameof(IPartialResumeSpy.ResumeBoom)), TimeSpan.FromMinutes(5));

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        // On the old code this task faulted with the sibling's InvalidOperationException even
        // though the response WAS delivered — and the ingress path escalated exactly that throw
        // into SetException/FailAsync.
        await publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "late response" },
            CorrelationId);

        Assert.Equal(1, spy.Ok);
        Assert.Equal(1, spy.Boom);

        // The successful registration was consumed; the failed one stays registered so a later
        // redelivery can retry it and the watchdog can surface it.
        var remaining = await recoveryStateStore.GetAllAsync(CorrelationId);
        var leftover = Assert.Single(remaining);
        Assert.Equal(boomRegistration, leftover.RegistrationId);
    }

    private static RecoveryState Registration(Guid registrationId, string methodName)
        => new()
        {
            RegistrationId = registrationId,
            CorrelationId = CorrelationId,
            PayloadTypeFullName = typeof(OperationResult).FullName,
            RegisteredAtUtc = DateTime.UtcNow,
            ResumeCallback = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IPartialResumeSpy).FullName!,
                MethodName = methodName,
                Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
            }
        };

    public interface IPartialFailSpy
    {
        Task FailOk(Exception exception);
        Task FailBoom(Exception exception);
    }

    private sealed class PartialFailSpy : IPartialFailSpy
    {
        private int _ok;
        private int _boom;

        public int Ok => Volatile.Read(ref _ok);
        public int Boom => Volatile.Read(ref _boom);

        public Task FailOk(Exception exception)
        {
            Interlocked.Increment(ref _ok);
            return Task.CompletedTask;
        }

        public Task FailBoom(Exception exception)
        {
            Interlocked.Increment(ref _boom);
            throw new InvalidOperationException("failure callback hit an unregistered service");
        }
    }

    [Fact]
    public async Task DispatchLostExceptions_SiblingFailureAfterASuccessfulCallback_DoesNotEscalate()
    {
        // Regression (r24): DispatchLostResponses received this partial-failure guard in r23, but
        // its exception-path twin still rethrew unconditionally — a SetException whose FIRST
        // registration's failure callback succeeded (and was consumed) faulted on the SECOND
        // registration's throw, so the ingress redelivered the whole message forever (the consumed
        // registration is gone, the failing one keeps failing; the delivery never settles).
        const string correlationId = "partial-failure-correlation-id";
        var spy = new PartialFailSpy();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IPartialFailSpy>(spy);
        services.AddAsyncResponse().WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();

        var recoveryStateStore = provider.GetRequiredService<IRecoveryStateStore>();
        var okRegistration = Guid.NewGuid();
        var boomRegistration = Guid.NewGuid();
        await recoveryStateStore.SaveAsync(correlationId, FailureRegistration(correlationId, okRegistration, nameof(IPartialFailSpy.FailOk)), TimeSpan.FromMinutes(5));
        await recoveryStateStore.SaveAsync(correlationId, FailureRegistration(correlationId, boomRegistration, nameof(IPartialFailSpy.FailBoom)), TimeSpan.FromMinutes(5));

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        // On the old code this faulted with the sibling's InvalidOperationException even though
        // the exception WAS delivered to (and consumed by) the first registration.
        await publisher.SetException(new InvalidOperationException("remote boom"), correlationId);

        Assert.Equal(1, spy.Ok);
        Assert.Equal(1, spy.Boom);

        // The successful registration was consumed; the failed one stays registered so a later
        // redelivery can retry it and the watchdog can surface it.
        var remaining = await recoveryStateStore.GetAllAsync(correlationId);
        var leftover = Assert.Single(remaining);
        Assert.Equal(boomRegistration, leftover.RegistrationId);
    }

    private static RecoveryState FailureRegistration(string correlationId, Guid registrationId, string methodName)
        => new()
        {
            RegistrationId = registrationId,
            CorrelationId = correlationId,
            PayloadTypeFullName = typeof(OperationResult).FullName,
            RegisteredAtUtc = DateTime.UtcNow,
            FailureCallback = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IPartialFailSpy).FullName!,
                MethodName = methodName,
                Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
            }
        };
}
