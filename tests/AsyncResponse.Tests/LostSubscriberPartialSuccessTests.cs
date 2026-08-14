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
}
