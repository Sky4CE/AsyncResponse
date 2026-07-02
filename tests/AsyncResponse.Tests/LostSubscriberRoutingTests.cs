using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using StackExchange.Redis;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// No-subscriber behavior of the Redis response channel (the lost-subscriber fallback after a
/// redeploy/restart), exercised through the real implementation resolved from DI with Redis
/// mocked. Verifies that the payload's ShouldResumeOnRecovery — not the transport envelope — decides
/// between the resume callback and the failure callback, and that the broker ingress delivers
/// failed-but-valid payloads through the raw response path instead of converting them at ingress.
/// </summary>
public class LostSubscriberRoutingTests
{
    private const string CorrelationId = "test-correlation-id";

    private readonly Mock<IConnectionMultiplexer> _multiplexer = new();
    private readonly Mock<ISubscriber> _subscriber = new();
    private readonly Mock<IDatabase> _database = new();
    private readonly RecoverySpy _spy = new();
    private readonly IServiceProvider _provider;

    public LostSubscriberRoutingTests()
    {
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(0); // no active subscribers: the lost-subscriber fallback kicks in
        _database
            .Setup(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);
        _database
            .Setup(d => d.KeyTimeToLiveAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(TimeSpan.FromMinutes(5));
        _database
            .Setup(d => d.StringSetAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<RedisValue>(),
                It.IsAny<Expiration>(),
                It.IsAny<ValueCondition>(),
                It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);
        _database
            .Setup(d => d.StringSetAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<RedisValue>(),
                It.IsAny<TimeSpan?>(),
                It.IsAny<When>(),
                It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);
        _database
            .Setup(d => d.StringSetAsync(
                It.IsAny<RedisKey>(),
                It.IsAny<RedisValue>(),
                It.IsAny<TimeSpan?>(),
                It.IsAny<bool>(),
                It.IsAny<When>(),
                It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);
        // Recovery-store writes/deletes now run through a conditioned transaction (optimistic
        // concurrency). Forward the transaction's operations to the database mock so the existing
        // Times.Once/Never verifications keep observing them, and commit unconditionally — these
        // tests exercise routing, not CAS conflicts.
        var transaction = new Mock<ITransaction>();
        transaction
            .Setup(t => t.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .Returns<RedisKey, CommandFlags>((key, flags) => _database.Object.KeyDeleteAsync(key, flags));
        transaction
            .Setup(t => t.StringSetAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<Expiration>(), It.IsAny<ValueCondition>(), It.IsAny<CommandFlags>()))
            .Returns<RedisKey, RedisValue, Expiration, ValueCondition, CommandFlags>(
                (key, value, expiration, condition, flags) => _database.Object.StringSetAsync(key, value, expiration, condition, flags));
        transaction
            .Setup(t => t.StringSetAsync(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(), It.IsAny<TimeSpan?>(), It.IsAny<When>(), It.IsAny<CommandFlags>()))
            .Returns<RedisKey, RedisValue, TimeSpan?, When, CommandFlags>(
                (key, value, expiry, when, flags) => _database.Object.StringSetAsync(key, value, expiry, when, flags));
        transaction
            .Setup(t => t.ExecuteAsync(It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);
        _database
            .Setup(d => d.CreateTransaction(It.IsAny<object?>()))
            .Returns(transaction.Object);

        _multiplexer.Setup(m => m.GetSubscriber(It.IsAny<object?>())).Returns(_subscriber.Object);
        _multiplexer.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object?>())).Returns(_database.Object);

        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton(_multiplexer.Object);
        services.AddSingleton<IRecoverySpy>(_spy);
        services.AddAsyncResponse().WithRedisChannel();

        _provider = services.BuildServiceProvider();
    }

    private IAsyncResponsePublisher Publisher => _provider.GetRequiredService<IAsyncResponsePublisher>();
    private IAsyncResponseIngress Ingress => _provider.GetRequiredService<IAsyncResponseIngress>();

    // ----- Domain-state-aware routing through SetResponse -----

    [Fact]
    public async Task SetResponse_FailedPayload_InvokesFailureCallbackInsteadOfResume()
    {
        ArmRecoveryState();
        var payload = new OperationResult { Status = OperationStatus.Failed, Message = "remote step failed" };

        await Publisher.SetResponse(payload, CorrelationId);

        Assert.Empty(_spy.ResumedPayloads);
        var failure = Assert.IsType<AsyncResponseDomainFailureException>(Assert.Single(_spy.Failures));
        Assert.Equal(CorrelationId, failure.CorrelationId);
        Assert.Equal(typeof(OperationResult).FullName, failure.PayloadTypeFullName);
        Assert.Contains("remote step failed", failure.PayloadJson);
        _database.Verify(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Once);
    }

    [Fact]
    public async Task Ingress_FailedPayload_AsRawJson_InvokesFailureCallback()
    {
        // The realistic redeploy scenario: the broker ingress delivers the response as an
        // untyped JsonElement; the payload type is only known from the recovery state.
        ArmRecoveryState();

        await Ingress.HandleResponseMessageAsync("""{"Status":3,"Message":"remote step failed"}""", CorrelationId);

        Assert.Empty(_spy.ResumedPayloads);
        Assert.IsType<AsyncResponseDomainFailureException>(Assert.Single(_spy.Failures));
    }

    [Fact]
    public async Task Ingress_CompletedPayload_AsRawJson_InvokesResumeCallback()
    {
        ArmRecoveryState();

        await Ingress.HandleResponseMessageAsync("""{"Status":2,"Message":"done"}""", CorrelationId);

        Assert.Single(_spy.ResumedPayloads);
        Assert.Empty(_spy.Failures);
        _database.Verify(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Once);
    }

    [Fact]
    public async Task SetResponse_NoSubscribers_DispatchesAllRecoveryRegistrationsForCorrelationId()
    {
        ArmRecoveryStates(NewRecoveryState(), NewRecoveryState());

        await Publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed, Message = "done" }, CorrelationId);

        Assert.Equal(2, _spy.ResumedPayloads.Count);
        Assert.Empty(_spy.Failures);
    }

    [Fact]
    public async Task SetResponse_RunningPayload_InvokesResumeCallback()
    {
        ArmRecoveryState();

        await Publisher.SetResponse(new OperationResult { Status = OperationStatus.Running }, CorrelationId);

        Assert.Single(_spy.ResumedPayloads);
        Assert.Empty(_spy.Failures);
    }

    [Fact]
    public async Task SetResponse_UnknownPayload_InvokesFailureCallbackConservatively()
    {
        ArmRecoveryState();

        await Publisher.SetResponse(new OperationResult { Status = OperationStatus.Unknown }, CorrelationId);

        Assert.Empty(_spy.ResumedPayloads);
        Assert.IsType<AsyncResponseDomainFailureException>(Assert.Single(_spy.Failures));
    }

    [Fact]
    public async Task Ingress_UnclassifiableRawJsonWithoutStoredType_FailsConservatively()
    {
        // No stored payload type → the payload cannot be asked whether to resume, so it takes the
        // failure path rather than resume something the recovery process cannot understand.
        ArmRecoveryState(payloadTypeFullName: null);

        await Ingress.HandleResponseMessageAsync("""{"Status":3,"Message":"unclassifiable without a stored type"}""", CorrelationId);

        Assert.Empty(_spy.ResumedPayloads);
        Assert.IsType<AsyncResponseDomainFailureException>(Assert.Single(_spy.Failures));
    }

    [Fact]
    public async Task SetResponse_FailedPayload_WithoutFailureCallback_DoesNotInvokeResume()
    {
        ArmRecoveryState(includeFailureCallback: false);

        await Publisher.SetResponse(new OperationResult { Status = OperationStatus.Failed }, CorrelationId);

        Assert.Empty(_spy.ResumedPayloads);
        Assert.Empty(_spy.Failures);
        // Nothing was dispatched: the recovery state is kept.
        _database.Verify(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task SetResponse_FailureCallbackThrows_IsSwallowedAndRecoveryStateIsKept()
    {
        ArmRecoveryState();
        _spy.FailureCallbackError = new InvalidOperationException("handler exploded");

        // Must not throw: rethrowing would loop back through the ingress's SetException safety
        // net and invoke the same failure callback a second time.
        await Publisher.SetResponse(new OperationResult { Status = OperationStatus.Failed }, CorrelationId);

        Assert.Single(_spy.Failures);
        _database.Verify(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task SetResponse_WithActiveSubscribers_NeverTouchesRecoveryState()
    {
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(1);

        await Publisher.SetResponse(new OperationResult { Status = OperationStatus.Failed }, CorrelationId);

        _database.Verify(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Never);
        Assert.Empty(_spy.ResumedPayloads);
        Assert.Empty(_spy.Failures);
    }

    [Fact]
    public async Task SetException_NoSubscriber_InvokesFailureCallbackWithOriginalException()
    {
        ArmRecoveryState();
        var original = new InvalidOperationException("technical error");

        await Publisher.SetException(original, CorrelationId);

        Assert.Empty(_spy.ResumedPayloads);
        var failure = Assert.Single(_spy.Failures);
        Assert.IsType<InvalidOperationException>(failure);
        Assert.Equal("technical error", failure.Message);
    }

    [Fact]
    public async Task SetException_NoSubscribers_DispatchesAllRecoveryRegistrationsForCorrelationId()
    {
        ArmRecoveryStates(NewRecoveryState(), NewRecoveryState());
        var original = new InvalidOperationException("technical error");

        await Publisher.SetException(original, CorrelationId);

        Assert.Empty(_spy.ResumedPayloads);
        Assert.Equal(2, _spy.Failures.Count);
        Assert.All(_spy.Failures, failure => Assert.Same(original, failure));
    }

    [Fact]
    public async Task SetException_WithActiveSubscribers_PublishesFailureEnvelopeAndDoesNotReadRecoveryState()
    {
        RedisChannel publishedChannel = default;
        RedisValue publishedValue = default;
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .Callback<RedisChannel, RedisValue, CommandFlags>((channel, value, _) =>
            {
                publishedChannel = channel;
                publishedValue = value;
            })
            .ReturnsAsync(2);

        await Publisher.SetException(new InvalidOperationException("technical error"), CorrelationId);

        Assert.Equal($"asyncresponse:response:{CorrelationId}", publishedChannel.ToString());
        using var document = JsonDocument.Parse(publishedValue.ToString());
        var root = document.RootElement;
        Assert.False(root.GetProperty("Success").GetBoolean());
        Assert.Equal("technical error", root.GetProperty("ExceptionMessage").GetString());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("Payload").ValueKind);
        _database.Verify(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Never);
        _database.Verify(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task SetException_PublishesToExplicitCorrelationChannel()
    {
        RedisChannel publishedChannel = default;
        _subscriber
            .Setup(s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()))
            .Callback<RedisChannel, RedisValue, CommandFlags>((channel, _, _) => publishedChannel = channel)
            .ReturnsAsync(1);

        await Publisher.SetException(new InvalidOperationException("technical error"), CorrelationId);

        Assert.Equal($"asyncresponse:response:{CorrelationId}", publishedChannel.ToString());
    }

    [Fact]
    public async Task SetException_NoSubscriberWithoutFailureCallback_DoesNotDeleteRecoveryState()
    {
        ArmRecoveryState(includeFailureCallback: false);

        await Publisher.SetException(new InvalidOperationException("technical error"), CorrelationId);

        Assert.Empty(_spy.Failures);
        _database.Verify(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task SetException_FailureCallbackThrows_PropagatesAndKeepsRecoveryState()
    {
        ArmRecoveryState();
        _spy.FailureCallbackError = new InvalidOperationException("handler exploded");

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Publisher.SetException(new InvalidOperationException("technical error"), CorrelationId));

        Assert.Equal("handler exploded", thrown.Message);
        Assert.Single(_spy.Failures);
        _database.Verify(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task SetException_FailureCallbackThrows_PreservesOriginalStackTrace()
    {
        // Regression guard: the multi-registration dispatcher captures the first callback exception
        // and re-throws it after dispatching the rest. It must do so with ExceptionDispatchInfo so
        // the original throw site survives; a bare `throw capturedVariable;` would reset the stack
        // trace to the dispatcher and drop the throwing frame.
        ArmRecoveryState();
        _spy.FailureCallbackError = CaptureExceptionThrownAt();

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Publisher.SetException(new InvalidOperationException("technical error"), CorrelationId));

        Assert.NotNull(thrown.StackTrace);
        Assert.Contains(nameof(ThrowMarkerSite), thrown.StackTrace);
    }

    [Fact]
    public async Task SetResponse_ResumeCallbackThrows_PreservesOriginalStackTrace()
    {
        // Same regression guard for the response-dispatch path: a resuming payload whose resume
        // callback throws must propagate with the original throw site intact.
        ArmRecoveryState();
        _spy.ResumeCallbackError = CaptureExceptionThrownAt();

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Publisher.SetResponse(new OperationResult { Status = OperationStatus.Completed }, CorrelationId));

        Assert.NotNull(thrown.StackTrace);
        Assert.Contains(nameof(ThrowMarkerSite), thrown.StackTrace);
    }

    [Fact]
    public async Task SetException_NullException_ThrowsBeforePublishing()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => Publisher.SetException(null!, CorrelationId));

        _subscriber.Verify(
            s => s.PublishAsync(It.IsAny<RedisChannel>(), It.IsAny<RedisValue>(), It.IsAny<CommandFlags>()),
            Times.Never);
    }

    // ----- Broker ingress -----

    [Fact]
    public async Task Ingress_FailedPayload_IsDeliveredThroughSetResponse_NotConvertedAtIngress()
    {
        // Pins the ingress contract: a failed-state response is valid JSON, so the untyped
        // ingress deserialization cannot fail on it — it flows through SetResponse and only the
        // lost-subscriber dispatcher decides whether it resumes.
        ArmRecoveryState();

        await Ingress.HandleResponseMessageAsync("""{"Status":3,"Message":"remote step failed"}""", CorrelationId);

        Assert.Empty(_spy.ResumedPayloads);
        Assert.IsType<AsyncResponseDomainFailureException>(Assert.Single(_spy.Failures));
    }

    [Fact]
    public async Task Ingress_UnparseablePayload_IsTheOnlyIngressPathToFailure()
    {
        ArmRecoveryState();

        await Ingress.HandleResponseMessageAsync("<html>502 Bad Gateway</html>", CorrelationId);

        Assert.Empty(_spy.ResumedPayloads);
        var failure = Assert.Single(_spy.Failures);
        Assert.IsType<InvalidDataException>(failure);
    }

    [Fact]
    public async Task Ingress_WorkerMessage_ExecutesJobWithRestoredCorrelationId()
    {
        var job = new WorkerJobEnvelope
        {
            CorrelationId = CorrelationId,
            ReplyTarget = new AsyncResponseReplyTarget
            {
                Name = "default",
                Transport = "test",
                Address = "test://reply"
            },
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
                MethodName = nameof(IRecoverySpy.OnWorkerJob),
                Params = [CallbackParam.ForValue(42)]
            }
        };

        await Ingress.HandleWorkerMessageAsync(JsonSerializer.Serialize(job));

        var (orderId, observedCorrelationId, observedReplyTarget) = Assert.Single(_spy.WorkerJobs);
        Assert.Equal(42, orderId);
        Assert.Equal(CorrelationId, observedCorrelationId);
        Assert.Equal("default", observedReplyTarget);
    }

    [Fact]
    public async Task Ingress_WorkerMessage_WithoutContext_ClearsAmbientOnlyForJob()
    {
        AsyncResponseContext.SetCorrelationId("outer-correlation-id");
        AsyncResponseContext.SetReplyTarget(new AsyncResponseReplyTarget
        {
            Name = "outer",
            Transport = "test",
            Address = "test://outer"
        });
        try
        {
            var job = new WorkerJobEnvelope
            {
                CorrelationId = null,
                Call = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
                    MethodName = nameof(IRecoverySpy.OnWorkerJob),
                    Params = [CallbackParam.ForValue(42)]
                }
            };

            await Ingress.HandleWorkerMessageAsync(JsonSerializer.Serialize(job));

            var (orderId, observedCorrelationId, observedReplyTarget) = Assert.Single(_spy.WorkerJobs);
            Assert.Equal(42, orderId);
            Assert.Null(observedCorrelationId);
            Assert.Null(observedReplyTarget);
            Assert.Equal("outer-correlation-id", AsyncResponseContext.CorrelationId);
            Assert.Equal("outer", AsyncResponseContext.ReplyTarget?.Name);
        }
        finally
        {
            AsyncResponseContext.ClearCorrelationId();
            AsyncResponseContext.ClearReplyTarget();
        }
    }

    // ----- helpers -----

    // Produces an exception carrying a real, test-controlled throw-site frame (ThrowMarkerSite),
    // so a stack-trace assertion can prove the original trace survived dispatch. NoInlining keeps
    // the frame present in Release builds.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static InvalidOperationException CaptureExceptionThrownAt()
    {
        try
        {
            ThrowMarkerSite();
            throw new InvalidOperationException("unreachable");
        }
        catch (InvalidOperationException ex)
        {
            return ex;
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowMarkerSite()
        => throw new InvalidOperationException("handler exploded");

    private void ArmRecoveryState(string? payloadTypeFullName = "default", bool includeFailureCallback = true)
        => ArmRecoveryStates(NewRecoveryState(payloadTypeFullName, includeFailureCallback));

    private void ArmRecoveryStates(params RecoveryState[] states)
        => _database
            .Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(states.Length == 1 ? JsonSerializer.Serialize(states[0]) : JsonSerializer.Serialize(states));

    private static RecoveryState NewRecoveryState(string? payloadTypeFullName = "default", bool includeFailureCallback = true)
    {
        return new RecoveryState
        {
            RegistrationId = Guid.NewGuid(),
            CorrelationId = CorrelationId,
            PayloadTypeFullName = payloadTypeFullName == "default" ? typeof(OperationResult).FullName : payloadTypeFullName,
            RegisteredAtUtc = DateTime.UtcNow,
            ResumeCallback = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
                MethodName = nameof(IRecoverySpy.OnResume),
                Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
            },
            FailureCallback = includeFailureCallback
                ? new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IRecoverySpy).FullName!,
                    MethodName = nameof(IRecoverySpy.OnFailure),
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
                }
                : null
        };
    }
}

/// <summary>
/// Stand-in for a flow service: lost-subscriber callbacks and worker jobs resolve it from DI by
/// full name and invoke it via reflection, exactly like production resume/fail handlers.
/// </summary>
public interface IRecoverySpy
{
    Task OnResume(object payload);
    Task OnFailure(Exception exception);
    Task OnWorkerJob(int orderId);
}

public sealed class RecoverySpy : IRecoverySpy
{
    public List<object> ResumedPayloads { get; } = [];
    public List<Exception> Failures { get; } = [];
    public List<(int OrderId, string? CorrelationId, string? ReplyTarget)> WorkerJobs { get; } = [];
    public Exception? FailureCallbackError { get; set; }
    public Exception? ResumeCallbackError { get; set; }

    public Task OnResume(object payload)
    {
        ResumedPayloads.Add(payload);
        return ResumeCallbackError is null ? Task.CompletedTask : Task.FromException(ResumeCallbackError);
    }

    public Task OnFailure(Exception exception)
    {
        Failures.Add(exception);
        return FailureCallbackError is null ? Task.CompletedTask : Task.FromException(FailureCallbackError);
    }

    public Task OnWorkerJob(int orderId)
    {
        WorkerJobs.Add((orderId, AsyncResponseContext.CorrelationId, AsyncResponseContext.ReplyTarget?.Name));
        return Task.CompletedTask;
    }
}
