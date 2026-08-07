using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression suite for the production flow-deadlock shape (Optimatic incident 292332 / tech-debt
/// 292683): a deploy killed the process holding the waiter mid-step; the remote system then
/// published two in-progress checkpoints and a terminal success through the broker ingress. The
/// lost-subscriber recovery path classified them correctly by type name but invoked the resume
/// callback with the <em>raw JSON</em> payload — the callback's <c>object</c>-declared parameter
/// received a <c>JsonElement</c>, every <c>is</c>-guard in the flow silently failed, three duplicate
/// workers were spawned, the terminal success was never persisted, and the flow re-attached to a
/// consumed correlation id and hung.
/// <para>
/// These tests drive the REAL ingress (<see cref="IAsyncResponseIngress"/> — the funnel every
/// broker transport delivers response messages through) against the real in-memory channel, with
/// recovery registrations armed through <see cref="IRecoveryStateStore"/> exactly as a recoverable
/// waiter persists them (and exactly what survives when its process dies). The channel-generic
/// equivalents run against Redis / NATS / PostgreSQL / SQL Server / MongoDB via
/// <c>ChannelConformanceSuite</c>.
/// </para>
/// </summary>
public class LostSubscriberRecoveryRegressionTests
{
    private const string CorrelationId = "incident-292332-correlation-id";
    private static readonly TimeSpan RecoveryTtl = TimeSpan.FromMinutes(2);

    // ----- the incident payload sequence, end to end through the ingress -----

    [Fact]
    public async Task Ingress_TerminalRawJson_ObjectParameterResumeCallback_ReceivesMaterializedPayload()
    {
        await using var harness = Harness.Create();
        await harness.ArmIncidentRegistrationAsync();

        await harness.Ingress.HandleResponseMessageAsync(
            """{"Status":2,"Message":"pipeline succeeded"}""", CorrelationId);

        // The callback parameter is declared `object` (one flow callback shared across payload
        // types — the production shape that made a concrete parameter type impossible). It must
        // receive the payload type recorded in the recovery registration, not raw JSON.
        var (payload, correlationId) = Assert.Single(harness.Spy.Resumed);
        var typed = Assert.IsType<IncidentStepResult>(payload);
        Assert.Equal(IncidentStepStatus.Succeeded, typed.Status);
        Assert.Equal("pipeline succeeded", typed.Message);
        Assert.Equal(CorrelationId, correlationId);
        Assert.Empty(harness.Spy.Failed);
    }

    [Fact]
    public async Task Ingress_IncidentSequence_TwoCheckpointsThenTerminal_ResumesExactlyOnceWithTerminalPayload()
    {
        await using var harness = Harness.Create();
        await harness.ArmIncidentRegistrationAsync();

        // 08:34:29 and 08:34:29 — in-progress checkpoints land with no live waiter. They are
        // neither a result to resume from nor a failure: the registration must stay armed and no
        // callback may fire. (The in-memory channel dispatches inline, so each await below is a
        // completed lost-subscriber dispatch — the mid-sequence asserts are deterministic.)
        await harness.Ingress.HandleResponseMessageAsync("""{"Status":1,"Message":"checkpoint-1"}""", CorrelationId);
        await harness.Ingress.HandleResponseMessageAsync("""{"Status":1,"Message":"checkpoint-2"}""", CorrelationId);

        Assert.Empty(harness.Spy.Resumed);
        Assert.Empty(harness.Spy.Failed);
        Assert.NotEmpty(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));

        // 08:34:31 — the terminal success. Exactly one resume, with the materialized terminal
        // payload; the registration is consumed. (The incident produced THREE resumed workers
        // holding a seven-day wait on a consumed correlation id, and the success was never
        // persisted because the untyped payload failed every guard.)
        await harness.Ingress.HandleResponseMessageAsync("""{"Status":2,"Message":"pipeline succeeded"}""", CorrelationId);

        var (payload, _) = Assert.Single(harness.Spy.Resumed);
        var typed = Assert.IsType<IncidentStepResult>(payload);
        Assert.Equal(IncidentStepStatus.Succeeded, typed.Status);
        Assert.Equal("pipeline succeeded", typed.Message);
        Assert.Empty(harness.Spy.Failed);
        Assert.Empty(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
    }

    [Fact]
    public async Task Ingress_NonTerminalCheckpoint_InvokesNothingAndRetainsRegistration()
    {
        await using var harness = Harness.Create();
        await harness.ArmIncidentRegistrationAsync();

        await harness.Ingress.HandleResponseMessageAsync("""{"Status":1,"Message":"still running"}""", CorrelationId);

        Assert.Empty(harness.Spy.Resumed);
        Assert.Empty(harness.Spy.Failed);
        var registration = Assert.Single(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
        Assert.Equal(typeof(IncidentStepResult).FullName, registration.PayloadTypeFullName);
    }

    [Fact]
    public async Task Ingress_FailedRawJson_FailureCallbackReceivesMaterializedPayloadAndDomainException()
    {
        await using var harness = Harness.Create();
        await harness.ArmIncidentRegistrationAsync();

        await harness.Ingress.HandleResponseMessageAsync(
            """{"Status":3,"Message":"pipeline failed"}""", CorrelationId);

        Assert.Empty(harness.Spy.Resumed);
        var (payload, exception) = Assert.Single(harness.Spy.Failed);
        var typed = Assert.IsType<IncidentStepResult>(payload);
        Assert.Equal(IncidentStepStatus.Failed, typed.Status);
        var domain = Assert.IsType<AsyncResponseDomainFailureException>(exception);
        Assert.Equal(CorrelationId, domain.CorrelationId);
        Assert.Equal(typeof(IncidentStepResult).FullName, domain.PayloadTypeFullName);
        Assert.Empty(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
    }

    // ----- callback parameter shapes beyond `object` -----

    [Fact]
    public async Task Ingress_TerminalRawJson_InterfaceParameterResumeCallback_ReceivesMaterializedPayload()
    {
        await using var harness = Harness.Create();
        await harness.ArmRegistrationAsync(
            typeof(BaseStepResult).FullName,
            resume: new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(ITypedParameterFlowSpy).FullName!,
                MethodName = nameof(ITypedParameterFlowSpy.ResumeTyped),
                Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
            });

        // Before the fix this path could not even invoke: deserializing a JsonElement into the
        // DECLARED parameter type (an interface) throws in System.Text.Json, the resume dispatch
        // faults, and the ingress escalates through SetException — the flow never resumes.
        await harness.Ingress.HandleResponseMessageAsync(
            """{"Status":2,"Message":"typed-parameter resume"}""", CorrelationId);

        var payload = Assert.Single(harness.Spy.ResumedTyped);
        var typed = Assert.IsType<BaseStepResult>(payload, exactMatch: false);
        Assert.Equal(IncidentStepStatus.Succeeded, typed.Status);
        Assert.Equal("typed-parameter resume", typed.Message);
    }

    [Fact]
    public async Task Ingress_TerminalRawJson_BaseClassParameterResumeCallback_ReceivesRegisteredDerivedType()
    {
        await using var harness = Harness.Create();
        await harness.ArmRegistrationAsync(
            typeof(DerivedStepResult).FullName,
            resume: new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IBaseParameterFlowSpy).FullName!,
                MethodName = nameof(IBaseParameterFlowSpy.ResumeBase),
                Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
            });

        // The registration recorded the DERIVED payload type; the callback declares the BASE. The
        // callback must receive the derived instance — deserializing into the declared base type
        // silently sliced off the derived state before the fix.
        await harness.Ingress.HandleResponseMessageAsync(
            """{"Status":2,"Message":"derived resume","Extra":"must survive"}""", CorrelationId);

        var payload = Assert.Single(harness.Spy.ResumedBase);
        var derived = Assert.IsType<DerivedStepResult>(payload);
        Assert.Equal("must survive", derived.Extra);
        Assert.Equal(IncidentStepStatus.Succeeded, derived.Status);
    }

    // ----- conservative paths that must keep working exactly as before -----

    [Fact]
    public async Task Ingress_UnresolvablePayloadType_StillRoutesToFailureWithRawPayload()
    {
        await using var harness = Harness.Create();
        await harness.ArmRegistrationAsync(
            "Missing.Assembly.After.Redeploy.StepResult",
            resume: IncidentResumeCallback(),
            failure: IncidentFailureCallback());

        // A payload type that no longer resolves (renamed/removed across the redeploy) cannot be
        // materialized. The conservative contract is unchanged: never resume what cannot be
        // understood — route to the failure callback, with the raw payload attached as-is.
        await harness.Ingress.HandleResponseMessageAsync("""{"Status":2,"Message":"who am I"}""", CorrelationId);

        Assert.Empty(harness.Spy.Resumed);
        var (payload, exception) = Assert.Single(harness.Spy.Failed);
        Assert.NotNull(payload);
        Assert.IsNotType<IncidentStepResult>(payload);
        var domain = Assert.IsType<AsyncResponseDomainFailureException>(exception);
        Assert.Contains("who am I", domain.PayloadJson);
    }

    [Fact]
    public async Task Ingress_MixedRegistrations_KeepWaitingSiblingRetainedWhileResumeSiblingConsumed()
    {
        await using var harness = Harness.Create();
        var keepWaitingRegistration = Guid.NewGuid();
        await harness.ArmRegistrationAsync(typeof(IncidentStepResult).FullName, resume: IncidentResumeCallback());
        await harness.ArmRegistrationAsync(
            typeof(AlwaysCheckpointProbe).FullName,
            resume: IncidentResumeCallback(),
            registrationId: keepWaitingRegistration);

        // Two waiters shared the correlation id with different payload types. The same raw JSON
        // classifies per registration: the incident payload resumes (and its registration is
        // consumed), while the checkpoint-only sibling keeps waiting (and must survive).
        await harness.Ingress.HandleResponseMessageAsync("""{"Status":2,"Message":"done"}""", CorrelationId);

        var (payload, _) = Assert.Single(harness.Spy.Resumed);
        Assert.IsType<IncidentStepResult>(payload);
        var remaining = Assert.Single(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
        Assert.Equal(keepWaitingRegistration, remaining.RegistrationId);
    }

    // ----- harness -----

    private static ReflectionCallDto IncidentResumeCallback() => new()
    {
        ServiceInterfaceFullName = typeof(IIncidentFlowSpy).FullName!,
        MethodName = nameof(IIncidentFlowSpy.ResumeStep),
        Params =
        [
            CallbackParam.ForPlaceholder(PlaceholderType.Payload),
            CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId)
        ]
    };

    private static ReflectionCallDto IncidentFailureCallback() => new()
    {
        ServiceInterfaceFullName = typeof(IIncidentFlowSpy).FullName!,
        MethodName = nameof(IIncidentFlowSpy.FailStep),
        Params =
        [
            CallbackParam.ForPlaceholder(PlaceholderType.Payload),
            CallbackParam.ForPlaceholder(PlaceholderType.Exception)
        ]
    };

    private sealed class Harness : IAsyncDisposable
    {
        private readonly ServiceProvider _provider;

        private Harness(ServiceProvider provider, IncidentFlowSpy spy)
        {
            _provider = provider;
            Spy = spy;
        }

        public IncidentFlowSpy Spy { get; }
        public IAsyncResponseIngress Ingress => _provider.GetRequiredService<IAsyncResponseIngress>();
        public IRecoveryStateStore RecoveryStateStore => _provider.GetRequiredService<IRecoveryStateStore>();

        public static Harness Create()
        {
            var services = new ServiceCollection();
            services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
            var spy = new IncidentFlowSpy();
            services.AddSingleton<IIncidentFlowSpy>(spy);
            services.AddSingleton<ITypedParameterFlowSpy>(spy);
            services.AddSingleton<IBaseParameterFlowSpy>(spy);
            services.AddAsyncResponse().WithInMemoryChannel();
            return new Harness(services.BuildServiceProvider(), spy);
        }

        /// <summary>The registration the incident's waiter persisted before its process died.</summary>
        public Task ArmIncidentRegistrationAsync()
            => ArmRegistrationAsync(
                typeof(IncidentStepResult).FullName,
                resume: IncidentResumeCallback(),
                failure: IncidentFailureCallback());

        public Task ArmRegistrationAsync(
            string? payloadTypeFullName,
            ReflectionCallDto resume,
            ReflectionCallDto? failure = null,
            Guid? registrationId = null)
            => RecoveryStateStore.SaveAsync(
                CorrelationId,
                new RecoveryState
                {
                    RegistrationId = registrationId ?? Guid.NewGuid(),
                    CorrelationId = CorrelationId,
                    PayloadTypeFullName = payloadTypeFullName,
                    RegisteredAtUtc = DateTime.UtcNow,
                    ResumeCallback = resume,
                    FailureCallback = failure
                },
                RecoveryTtl);

        public ValueTask DisposeAsync() => _provider.DisposeAsync();
    }
}

public enum IncidentStepStatus
{
    Unknown = 0,
    InProgress = 1,
    Succeeded = 2,
    Failed = 3
}

/// <summary>
/// The incident's step-result shape: the remote pipeline reports non-terminal checkpoints before
/// its terminal outcome, so recovery classification is tri-state — a checkpoint must keep the
/// registration armed instead of consuming it.
/// </summary>
public sealed class IncidentStepResult : IAsyncResponsePayload
{
    public IncidentStepStatus Status { get; set; }
    public string? Message { get; set; }

    public RecoveryAction OnRecovery() => Status switch
    {
        IncidentStepStatus.Succeeded => RecoveryAction.Resume,
        IncidentStepStatus.InProgress => RecoveryAction.KeepWaiting,
        _ => RecoveryAction.Fail,
    };
}

/// <summary>A base-class payload; <see cref="DerivedStepResult"/> extends it with extra state.</summary>
public class BaseStepResult : IAsyncResponsePayload
{
    public IncidentStepStatus Status { get; set; }
    public string? Message { get; set; }

    public RecoveryAction OnRecovery() => Status is not IncidentStepStatus.Failed ? RecoveryAction.Resume : RecoveryAction.Fail;
}

/// <summary>Derived payload used to prove the callback receives the REGISTERED type, unsliced.</summary>
public sealed class DerivedStepResult : BaseStepResult
{
    public string? Extra { get; set; }
}

/// <summary>Classifies every response as a checkpoint — its registration must never be consumed.</summary>
public sealed class AlwaysCheckpointProbe : IAsyncResponsePayload
{
    public IncidentStepStatus Status { get; set; }
    public string? Message { get; set; }

    public RecoveryAction OnRecovery() => RecoveryAction.KeepWaiting;
}

public interface IIncidentFlowSpy
{
    Task ResumeStep(object payload, string correlationId);
    Task FailStep(object payload, Exception exception);
}

public interface ITypedParameterFlowSpy
{
    Task ResumeTyped(IAsyncResponsePayload payload);
}

public interface IBaseParameterFlowSpy
{
    Task ResumeBase(BaseStepResult payload);
}

public sealed class IncidentFlowSpy : IIncidentFlowSpy, ITypedParameterFlowSpy, IBaseParameterFlowSpy
{
    private readonly object _gate = new();
    private readonly List<(object Payload, string CorrelationId)> _resumed = [];
    private readonly List<(object Payload, Exception Exception)> _failed = [];
    private readonly List<IAsyncResponsePayload> _resumedTyped = [];
    private readonly List<BaseStepResult> _resumedBase = [];

    public IReadOnlyList<(object Payload, string CorrelationId)> Resumed
    {
        get { lock (_gate) return [.. _resumed]; }
    }

    public IReadOnlyList<(object Payload, Exception Exception)> Failed
    {
        get { lock (_gate) return [.. _failed]; }
    }

    public IReadOnlyList<IAsyncResponsePayload> ResumedTyped
    {
        get { lock (_gate) return [.. _resumedTyped]; }
    }

    public IReadOnlyList<BaseStepResult> ResumedBase
    {
        get { lock (_gate) return [.. _resumedBase]; }
    }

    public Task ResumeStep(object payload, string correlationId)
    {
        lock (_gate) _resumed.Add((payload, correlationId));
        return Task.CompletedTask;
    }

    public Task FailStep(object payload, Exception exception)
    {
        lock (_gate) _failed.Add((payload, exception));
        return Task.CompletedTask;
    }

    public Task ResumeTyped(IAsyncResponsePayload payload)
    {
        lock (_gate) _resumedTyped.Add(payload);
        return Task.CompletedTask;
    }

    public Task ResumeBase(BaseStepResult payload)
    {
        lock (_gate) _resumedBase.Add(payload);
        return Task.CompletedTask;
    }
}
