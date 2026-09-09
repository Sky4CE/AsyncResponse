using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Two recovery registrations sharing one correlation id (the expected shape when a worker dies
/// mid-await and its replacement re-attaches), where one registration's callback succeeds and the
/// other's fails.
/// <para>
/// History: r23/r24 made a partial success complete the dispatch — the residual failure was logged
/// and swallowed so the ingress would not escalate a delivered response through
/// <c>SetException</c>. Round 35 found the other half of that trade: swallowing returned success to
/// the broker for a payload the FAILED registration never received, so the broker acknowledged its
/// only copy, the registration stayed armed with nothing left to replay it, and the watchdog could
/// only report the stale row. A <b>transient</b> residual failure now propagates as
/// <see cref="RecoveryCallbackFailedException"/> (the ingress passes it through untouched, so the
/// transport redelivers to the one registration still armed); a <b>deterministic</b> one keeps the
/// swallow, because redelivery cannot fix it.
/// </para>
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

        /// <summary>The dependency behind ResumeBoom: down until a test brings it back.</summary>
        public volatile bool DependencyUp;

        public Task ResumeOk(OperationResult payload)
        {
            Interlocked.Increment(ref _ok);
            return Task.CompletedTask;
        }

        public Task ResumeBoom(OperationResult payload)
        {
            Interlocked.Increment(ref _boom);
            if (!DependencyUp)
                throw new InvalidOperationException("re-enqueue failed on a publish-blocked broker");
            return Task.CompletedTask;
        }
    }

    /// <summary>A target interface no service implements: its callback fails deterministically at wire-up.</summary>
    public interface IUnregisteredSpy
    {
        Task Resume(OperationResult payload);
    }

    private static ServiceProvider BuildProvider(PartialResumeSpy spy)
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IPartialResumeSpy>(spy);
        services.AddAsyncResponse().WithInMemoryChannel();
        return services.BuildServiceProvider();
    }

    /// <summary>
    /// Round 35: pre-fix, this publish completed normally — the broker would have acknowledged the
    /// response the failed registration never got. It now propagates for redelivery, with the
    /// successful registration consumed and the failed one still armed.
    /// </summary>
    [Fact]
    public async Task DispatchLostResponses_TransientSiblingFailureAfterASuccessfulCallback_PropagatesForRedelivery()
    {
        var spy = new PartialResumeSpy();
        await using var provider = BuildProvider(spy);

        var recoveryStateStore = provider.GetRequiredService<IRecoveryStateStore>();
        var okRegistration = Guid.NewGuid();
        var boomRegistration = Guid.NewGuid();
        await recoveryStateStore.SaveAsync(CorrelationId, Registration(okRegistration, nameof(IPartialResumeSpy.ResumeOk)), TimeSpan.FromMinutes(5));
        await recoveryStateStore.SaveAsync(CorrelationId, Registration(boomRegistration, nameof(IPartialResumeSpy.ResumeBoom)), TimeSpan.FromMinutes(5));

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        var ex = await Assert.ThrowsAsync<RecoveryCallbackFailedException>(() => publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "late response" },
            CorrelationId));

        Assert.Equal(CorrelationId, ex.CorrelationId);
        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(1, spy.Ok);
        Assert.Equal(1, spy.Boom);

        // The successful registration was consumed; the failed one stays armed for the redelivery.
        var remaining = await recoveryStateStore.GetAllAsync(CorrelationId);
        var leftover = Assert.Single(remaining);
        Assert.Equal(boomRegistration, leftover.RegistrationId);
    }

    /// <summary>
    /// Eventual completion: once the dependency is back, the redelivery (here: the publisher's
    /// caller retrying the publish) reaches only the registration that failed — the consumed one
    /// is not re-invoked — and settles with nothing left armed.
    /// </summary>
    [Fact]
    public async Task DispatchLostResponses_RedeliveryAfterTheDependencyRecovers_CompletesOnlyTheFailedRegistration()
    {
        var spy = new PartialResumeSpy();
        await using var provider = BuildProvider(spy);

        var recoveryStateStore = provider.GetRequiredService<IRecoveryStateStore>();
        await recoveryStateStore.SaveAsync(CorrelationId, Registration(Guid.NewGuid(), nameof(IPartialResumeSpy.ResumeOk)), TimeSpan.FromMinutes(5));
        await recoveryStateStore.SaveAsync(CorrelationId, Registration(Guid.NewGuid(), nameof(IPartialResumeSpy.ResumeBoom)), TimeSpan.FromMinutes(5));

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();
        var response = new OperationResult { Status = OperationStatus.Completed, Message = "late response" };

        await Assert.ThrowsAsync<RecoveryCallbackFailedException>(() => publisher.SetResponse(response, CorrelationId));

        spy.DependencyUp = true;
        await publisher.SetResponse(response, CorrelationId);

        Assert.Equal(1, spy.Ok);
        Assert.Equal(2, spy.Boom);
        Assert.Empty(await recoveryStateStore.GetAllAsync(CorrelationId));
    }

    /// <summary>
    /// Pin (unchanged): a DETERMINISTIC residual failure — the sibling's target service is not
    /// registered — is still swallowed. Redelivery cannot fix it; the message is acknowledged and
    /// the failed registration stays for the watchdog to surface.
    /// </summary>
    [Fact]
    public async Task DispatchLostResponses_DeterministicSiblingFailureAfterASuccessfulCallback_IsStillSwallowed()
    {
        var spy = new PartialResumeSpy();
        await using var provider = BuildProvider(spy);

        var recoveryStateStore = provider.GetRequiredService<IRecoveryStateStore>();
        var unresolvable = Guid.NewGuid();
        await recoveryStateStore.SaveAsync(CorrelationId, Registration(Guid.NewGuid(), nameof(IPartialResumeSpy.ResumeOk)), TimeSpan.FromMinutes(5));
        await recoveryStateStore.SaveAsync(CorrelationId, new RecoveryState
        {
            RegistrationId = unresolvable,
            CorrelationId = CorrelationId,
            PayloadTypeFullName = typeof(OperationResult).FullName,
            RegisteredAtUtc = DateTime.UtcNow,
            ResumeCallback = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IUnregisteredSpy).FullName!,
                MethodName = nameof(IUnregisteredSpy.Resume),
                Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
            }
        }, TimeSpan.FromMinutes(5));

        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        await publisher.SetResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "late response" },
            CorrelationId);

        Assert.Equal(1, spy.Ok);
        var leftover = Assert.Single(await recoveryStateStore.GetAllAsync(CorrelationId));
        Assert.Equal(unresolvable, leftover.RegistrationId);
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
            throw new InvalidOperationException("failure callback hit a dependency that is down");
        }
    }

    /// <summary>
    /// The exception-path twin (r24 made it a swallow; round 35 turns the transient case into a
    /// propagation for redelivery, exactly like the response path). Pre-fix: the publish
    /// completed normally with one registration's callback never having run.
    /// </summary>
    [Fact]
    public async Task DispatchLostExceptions_TransientSiblingFailureAfterASuccessfulCallback_PropagatesForRedelivery()
    {
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

        var ex = await Assert.ThrowsAsync<RecoveryCallbackFailedException>(
            () => publisher.SetException(new InvalidOperationException("remote boom"), correlationId));

        Assert.Equal(correlationId, ex.CorrelationId);
        Assert.Equal(1, spy.Ok);
        Assert.Equal(1, spy.Boom);

        // The successful registration was consumed; the failed one stays armed for the redelivery.
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
