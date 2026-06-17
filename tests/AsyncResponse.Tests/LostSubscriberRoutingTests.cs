using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using StackExchange.Redis;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// No-subscriber behavior of the Redis response channel (the lost-subscriber fallback after a
/// redeploy/restart), exercised through the real implementation resolved from DI with Redis
/// mocked. Verifies that the payload's ShouldResumeOnRecovery — not the transport envelope — decides
/// between the resume callback and the failure callback, and that the broker ingress delivers
/// failed-but-valid payloads through <c>SetResponse</c> instead of converting them at ingress.
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
    public async Task SetResponse_FailedPayload_AsRawJson_InvokesFailureCallback()
    {
        // The realistic redeploy scenario: the broker ingress delivers the response as an
        // untyped JsonElement; the payload type is only known from the recovery state.
        ArmRecoveryState();
        var payload = JsonSerializer.Deserialize<object>("""{"Status":3,"Message":"remote step failed"}""");

        await Publisher.SetResponse(payload, CorrelationId);

        Assert.Empty(_spy.ResumedPayloads);
        Assert.IsType<AsyncResponseDomainFailureException>(Assert.Single(_spy.Failures));
    }

    [Fact]
    public async Task SetResponse_CompletedPayload_AsRawJson_InvokesResumeCallback()
    {
        ArmRecoveryState();
        var payload = JsonSerializer.Deserialize<object>("""{"Status":2,"Message":"done"}""");

        await Publisher.SetResponse(payload, CorrelationId);

        Assert.Single(_spy.ResumedPayloads);
        Assert.Empty(_spy.Failures);
        _database.Verify(d => d.KeyDeleteAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Once);
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
    public async Task SetResponse_UnclassifiableRawJsonWithoutStoredType_FailsConservatively()
    {
        // No stored payload type → the payload cannot be asked whether to resume, so it takes the
        // failure path rather than resume something the recovery process cannot understand.
        ArmRecoveryState(payloadTypeFullName: null);
        var payload = JsonSerializer.Deserialize<object>("""{"Status":3,"Message":"unclassifiable without a stored type"}""");

        await Publisher.SetResponse(payload, CorrelationId);

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

    private void ArmRecoveryState(string? payloadTypeFullName = "default", bool includeFailureCallback = true)
    {
        var state = new RecoveryState
        {
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

        _database
            .Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(JsonSerializer.Serialize(state));
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

    public Task OnResume(object payload)
    {
        ResumedPayloads.Add(payload);
        return Task.CompletedTask;
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
