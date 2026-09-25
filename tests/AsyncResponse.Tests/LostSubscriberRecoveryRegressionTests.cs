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

    // ----- classification must see the payload exactly as the wire would carry it -----

    [Fact]
    public async Task TypedPublish_PolymorphicDeclaredBase_RoutesLikeTheBrokerWould()
    {
        // A [JsonPolymorphic] base published in-process: the broker envelope serializes the
        // payload as the DECLARED base, emitting the type discriminator, so broker-side recovery
        // re-materializes the DERIVED payload and takes its verdict (Resume). Runtime-type
        // serialization dropped the discriminator, failed to materialize the abstract base, and
        // sent the same logical response down the failure route.
        await using var harness = Harness.Create();
        await harness.ArmRegistrationAsync(
            typeof(PolyStepBase).FullName,
            resume: IncidentResumeCallback(),
            failure: IncidentFailureCallback());

        await harness.Publisher.SetResponse<PolyStepBase>(
            new PolyStepCompleted { Message = "poly done" }, CorrelationId);

        var (payload, _) = Assert.Single(harness.Spy.Resumed);
        var typed = Assert.IsType<PolyStepCompleted>(payload);
        Assert.Equal("poly done", typed.Message);
        Assert.Empty(harness.Spy.Failed);
    }

    [Fact]
    public async Task TypedPublish_JsonIgnoredState_CannotInfluenceRecoveryRouting()
    {
        // Exact-type instance reuse leaked in-process-only state into the verdict: a [JsonIgnore]d
        // property said Resume while the wire representation (which never carries it) says Fail.
        // The same payload must route identically whether or not it crossed a broker.
        await using var harness = Harness.Create();
        await harness.ArmRegistrationAsync(
            typeof(IgnoredStatePayload).FullName,
            resume: IncidentResumeCallback(),
            failure: IncidentFailureCallback());

        await harness.Publisher.SetResponse(
            new IgnoredStatePayload { Message = "hint set", ResumeHint = true }, CorrelationId);

        Assert.Empty(harness.Spy.Resumed);
        var (payload, exception) = Assert.Single(harness.Spy.Failed);
        var typed = Assert.IsType<IgnoredStatePayload>(payload);
        Assert.False(typed.ResumeHint);
        Assert.IsType<AsyncResponseDomainFailureException>(exception);
    }

    [Fact]
    public async Task Ingress_ResumableResponseWithOnlyFailureCallback_EngagesTheFailureCallback()
    {
        // "Tell me if my flow dies" registrations arm only the failure callback. A resumable
        // terminal response used to be discarded with a warning on every redelivery until the
        // registration's TTL; the flow cannot proceed without a resume callback, so the failure
        // route must fire instead.
        await using var harness = Harness.Create();
        await harness.ArmRegistrationAsync(
            typeof(IncidentStepResult).FullName,
            resume: null,
            failure: IncidentFailureCallback());

        await harness.Ingress.HandleResponseMessageAsync(
            """{"Status":2,"Message":"pipeline succeeded"}""", CorrelationId);

        Assert.Empty(harness.Spy.Resumed);
        var (payload, exception) = Assert.Single(harness.Spy.Failed);
        var typed = Assert.IsType<IncidentStepResult>(payload);
        Assert.Equal(IncidentStepStatus.Succeeded, typed.Status);
        Assert.IsType<AsyncResponseDomainFailureException>(exception);
        Assert.Empty(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
    }

    // ----- cleanup failures must never be reinterpreted as a failed response -----

    [Fact]
    public async Task Ingress_DeleteFailsAfterSuccessfulResume_DoesNotEscalateToFailureCallback()
    {
        // The registration delete that follows a SUCCESSFUL resume is bookkeeping, not the
        // outcome. When it throws, the dispatch must not rethrow: rethrowing made the ingress
        // retry the whole delivery four times (re-invoking the resume each time) and then publish
        // the CLEANUP exception through SetException — invoking the failure callback for a flow
        // whose resume had already succeeded.
        await using var harness = Harness.Create(failDeletes: true);
        await harness.ArmIncidentRegistrationAsync();

        await harness.Ingress.HandleResponseMessageAsync(
            """{"Status":2,"Message":"pipeline succeeded"}""", CorrelationId);

        var (payload, _) = Assert.Single(harness.Spy.Resumed);
        Assert.IsType<IncidentStepResult>(payload);
        Assert.Empty(harness.Spy.Failed);
        // The failed delete leaves the registration for its TTL — at-least-once, watchdog-visible.
        Assert.NotEmpty(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
    }

    [Fact]
    public async Task SetException_DeleteFailsAfterSuccessfulFailureCallback_DoesNotThrow()
    {
        await using var harness = Harness.Create(failDeletes: true);
        await harness.ArmIncidentRegistrationAsync();

        // Same rule on the exception route: the failure callback ran; a failed cleanup afterwards
        // must not surface as a publish failure (which would loop the failure path).
        await harness.Publisher.SetException(new InvalidOperationException("remote boom"), CorrelationId);

        var (_, exception) = Assert.Single(harness.Spy.Failed);
        Assert.Equal("remote boom", exception.Message);
        Assert.Empty(harness.Spy.Resumed);
        Assert.NotEmpty(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
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
    public async Task TypedPublish_MixedRegistrations_ClassifiesEachRegistrationByItsOwnRegisteredType()
    {
        await using var harness = Harness.Create();
        var keepWaitingRegistration = Guid.NewGuid();
        await harness.ArmRegistrationAsync(typeof(IncidentStepResult).FullName, resume: IncidentResumeCallback());
        await harness.ArmRegistrationAsync(
            typeof(AlwaysCheckpointProbe).FullName,
            resume: IncidentResumeCallback(),
            registrationId: keepWaitingRegistration);

        // The typed-publish sibling of the raw-JSON fact below: an in-process publish carries a
        // live instance, but each registration must STILL be classified as the type IT registered
        // — the publisher's runtime type must not speak for a sibling that registered a different
        // type (that consumed the checkpoint sibling's registration and resumed it spuriously).
        var published = new IncidentStepResult { Status = IncidentStepStatus.Succeeded, Message = "done" };
        await harness.Publisher.SetResponse(published, CorrelationId);

        var (payload, _) = Assert.Single(harness.Spy.Resumed);
        var typed = Assert.IsType<IncidentStepResult>(payload);
        // Wire parity: the callback receives a materialization of the wire representation, never
        // the publisher's live instance.
        Assert.NotSame(published, typed);
        Assert.Equal(IncidentStepStatus.Succeeded, typed.Status);
        Assert.Equal("done", typed.Message);
        Assert.Empty(harness.Spy.Failed);
        var remaining = Assert.Single(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
        Assert.Equal(keepWaitingRegistration, remaining.RegistrationId);
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

    // ----- deterministic failure-callback faults are acknowledged on BOTH failure routes -----

    [Fact]
    public async Task SetException_FailureCallbackCannotBeWiredUp_IsAcknowledgedAndKeepsTheRegistration()
    {
        // The exception route IS the SetException escalation, and it rethrew a deterministic
        // failure-callback fault raw: a direct caller got an internal exception type, and through
        // the ingress the transport redelivered the same fault forever (RabbitMQ's default requeue
        // has no cap). The Fail route already logged and acknowledged the identical fault; so does
        // this one now, keeping the registration for the watchdog.
        await using var harness = Harness.Create();
        await harness.ArmRegistrationAsync(
            typeof(IncidentStepResult).FullName,
            resume: IncidentResumeCallback(),
            failure: UnregisteredFailureCallback());

        await harness.Publisher.SetException(new InvalidOperationException("remote boom"), CorrelationId);

        Assert.Empty(harness.Spy.Failed);
        Assert.Single(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
    }

    [Fact]
    public async Task Ingress_UnparseableBody_WithAnUnresolvableFailureCallback_IsAcknowledgedInsteadOfRedelivered()
    {
        // An unparseable body escalates straight to SetException; with a failure callback nothing
        // can wire up, that escalation used to throw, so the ingress rethrew and the transport
        // redelivered a message no attempt could ever handle.
        await using var harness = Harness.Create();
        await harness.ArmRegistrationAsync(
            typeof(IncidentStepResult).FullName,
            resume: IncidentResumeCallback(),
            failure: UnregisteredFailureCallback());

        await harness.Ingress.HandleResponseMessageAsync("<html>bad gateway</html>", CorrelationId);

        Assert.Empty(harness.Spy.Failed);
        Assert.Single(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
    }

    [Theory]
    [InlineData(nameof(ITypedFailureFlowSpy.FailTyped))]
    [InlineData(nameof(ITypedFailureFlowSpy.FailViaInterface))]
    public async Task Ingress_UnmaterializablePayload_WithATypedFailureCallback_IsAcknowledgedNotRetriedForever(string method)
    {
        // The payload cannot be materialized as the registered type (an enum value this build does
        // not know), so the failure route carries the raw JSON — and a failure callback whose
        // payload parameter is the registered type (or IAsyncResponsePayload) re-ran the same
        // failed conversion. Classified TRANSIENT, it burned the 4-attempt ladder and threw
        // RecoveryCallbackFailedException, which the transport redelivered forever. A persisted
        // argument that no longer converts is a wiring fault: deterministic, logged, acknowledged.
        await using var harness = Harness.Create();
        await harness.ArmRegistrationAsync(
            typeof(IncidentStepResult).FullName,
            resume: IncidentResumeCallback(),
            failure: new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(ITypedFailureFlowSpy).FullName!,
                MethodName = method,
                Params =
                [
                    CallbackParam.ForPlaceholder(PlaceholderType.Payload),
                    CallbackParam.ForPlaceholder(PlaceholderType.Exception)
                ]
            });

        await harness.Ingress.HandleResponseMessageAsync("""{"Status":"NotAStatusThisBuildKnows"}""", CorrelationId);

        Assert.Equal(0, harness.Spy.TypedFailures);
        Assert.Empty(harness.Spy.Resumed);
        Assert.Single(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
    }

    public static TheoryData<string> MalformedDescriptorShapes => ["null-params", "null-entry", "null-method", "blank-service"];

    [Theory]
    [MemberData(nameof(MalformedDescriptorShapes))]
    public async Task SetException_MalformedFailureDescriptor_IsAcknowledgedAndKeepsTheRegistration(string shape)
    {
        // `required` enforces presence on the wire, not non-null: a DTO registration, a foreign
        // producer or a store writer can persist "Params": null, a null entry, or a null name.
        // Resolving one threw ArgumentNullException / NullReferenceException — read as TRANSIENT,
        // wrapped for redelivery, and redelivered forever. It is a wiring fault like any other.
        await using var harness = Harness.Create();
        await harness.ArmRegistrationAsync(
            typeof(IncidentStepResult).FullName,
            resume: IncidentResumeCallback(),
            failure: Malformed(IncidentFailureCallback(), shape));

        await harness.Publisher.SetException(new InvalidOperationException("remote boom"), CorrelationId);

        Assert.Empty(harness.Spy.Failed);
        Assert.Single(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
    }

    [Theory]
    [MemberData(nameof(MalformedDescriptorShapes))]
    public async Task Ingress_FailClassifiedResponse_MalformedFailureDescriptor_IsAcknowledged(string shape)
    {
        // The response route's failure callback resolved the descriptor OUTSIDE its settlement, so
        // even a deterministic fault there skipped the log-and-acknowledge branch.
        await using var harness = Harness.Create();
        await harness.ArmRegistrationAsync(
            typeof(IncidentStepResult).FullName,
            resume: IncidentResumeCallback(),
            failure: Malformed(IncidentFailureCallback(), shape));

        await harness.Ingress.HandleResponseMessageAsync("""{"Status":3,"Message":"pipeline failed"}""", CorrelationId);

        Assert.Empty(harness.Spy.Failed);
        Assert.Single(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
    }

    [Theory]
    [MemberData(nameof(MalformedDescriptorShapes))]
    public async Task Ingress_ResumableResponse_MalformedResumeDescriptor_EscalatesToTheFailureCallback(string shape)
    {
        // A resume that can never be wired up is deterministic: the response route rethrows it
        // raw, and the ingress escalates it through SetException — the flow is failed through its
        // (well-formed) failure callback instead of the message being redelivered forever.
        await using var harness = Harness.Create();
        await harness.ArmRegistrationAsync(
            typeof(IncidentStepResult).FullName,
            resume: Malformed(IncidentResumeCallback(), shape),
            failure: IncidentFailureCallback());

        await harness.Ingress.HandleResponseMessageAsync("""{"Status":2,"Message":"pipeline succeeded"}""", CorrelationId);

        Assert.Empty(harness.Spy.Resumed);
        var (_, exception) = Assert.Single(harness.Spy.Failed);
        Assert.Contains("malformed", exception.Message, StringComparison.Ordinal);
    }

    // ----- the consumed registration's delete is bookkeeping, not the publisher's I/O -----

    [Fact]
    public async Task SetResponse_PublisherCancelsAfterTheCallbackRan_StillDeletesTheConsumedRegistration()
    {
        // The publisher's token scopes its own I/O. Pre-fix it also scoped the delete that follows
        // a SUCCESSFUL resume, so a caller cancelling late (an HTTP RequestAborted while the resume
        // ran) left the consumed registration armed: the watchdog then flagged a resumed flow as
        // stuck, and a retried publish re-invoked the callback.
        await using var harness = Harness.Create();
        await harness.ArmIncidentRegistrationAsync();
        using var publish = new CancellationTokenSource();
        harness.Spy.OnResume = publish.Cancel;

        await harness.Publisher.SetResponse(
            new IncidentStepResult { Status = IncidentStepStatus.Succeeded, Message = "done" },
            CorrelationId,
            publish.Token);

        Assert.Single(harness.Spy.Resumed);
        Assert.Empty(await harness.RecoveryStateStore.GetAllAsync(CorrelationId));
    }

    private static ReflectionCallDto UnregisteredFailureCallback() => new()
    {
        ServiceInterfaceFullName = typeof(IUnregisteredIncidentFlowSpy).FullName!,
        MethodName = nameof(IUnregisteredIncidentFlowSpy.FailStep),
        Params =
        [
            CallbackParam.ForPlaceholder(PlaceholderType.Payload),
            CallbackParam.ForPlaceholder(PlaceholderType.Exception)
        ]
    };

    private static ReflectionCallDto Malformed(ReflectionCallDto descriptor, string shape)
    {
        switch (shape)
        {
            case "null-params":
                descriptor.Params = null!;
                break;
            case "null-entry":
                descriptor.Params = [descriptor.Params[0], null!];
                break;
            case "null-method":
                descriptor.MethodName = null!;
                break;
            case "blank-service":
                descriptor.ServiceInterfaceFullName = "  ";
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(shape), shape, null);
        }

        return descriptor;
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
        public IAsyncResponsePublisher Publisher => _provider.GetRequiredService<IAsyncResponsePublisher>();
        public IRecoveryStateStore RecoveryStateStore => _provider.GetRequiredService<IRecoveryStateStore>();

        public static Harness Create(bool failDeletes = false)
        {
            var services = new ServiceCollection();
            services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
            var spy = new IncidentFlowSpy();
            services.AddSingleton<IIncidentFlowSpy>(spy);
            services.AddSingleton<ITypedParameterFlowSpy>(spy);
            services.AddSingleton<IBaseParameterFlowSpy>(spy);
            services.AddSingleton<ITypedFailureFlowSpy>(spy);
            if (failDeletes)
            {
                // Registered BEFORE AddAsyncResponse: the library's TryAddSingleton yields to it.
                services.AddSingleton<IRecoveryStateStore>(new DeleteFailingRecoveryStateStore());
            }

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
            ReflectionCallDto? resume,
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

/// <summary>
/// A polymorphic payload contract: publishers declare the base, the wire carries the
/// discriminator, and recovery must re-materialize the derived type exactly as a broker would.
/// </summary>
[System.Text.Json.Serialization.JsonPolymorphic(TypeDiscriminatorPropertyName = "$kind")]
[System.Text.Json.Serialization.JsonDerivedType(typeof(PolyStepCompleted), "completed")]
public abstract class PolyStepBase : IAsyncResponsePayload
{
    public string? Message { get; set; }

    // Virtual so the derived override genuinely participates in interface dispatch (a new-slot
    // method on the derived type would NOT re-implement the interface member).
    public virtual RecoveryAction OnRecovery() => RecoveryAction.Fail;
}

public sealed class PolyStepCompleted : PolyStepBase
{
    public override RecoveryAction OnRecovery() => RecoveryAction.Resume;
}

/// <summary>In-process state that never crosses the wire must never decide recovery routing.</summary>
public sealed class IgnoredStatePayload : IAsyncResponsePayload
{
    public string? Message { get; set; }

    [System.Text.Json.Serialization.JsonIgnore]
    public bool ResumeHint { get; set; }

    public RecoveryAction OnRecovery() => ResumeHint ? RecoveryAction.Resume : RecoveryAction.Fail;
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

/// <summary>Failure callbacks whose payload parameter is typed — the registered type, or the marker interface.</summary>
public interface ITypedFailureFlowSpy
{
    Task FailTyped(IncidentStepResult payload, Exception exception);
    Task FailViaInterface(IAsyncResponsePayload payload, Exception exception);
}

/// <summary>A callback target no test registers in DI — a failure callback nothing can wire up.</summary>
public interface IUnregisteredIncidentFlowSpy
{
    Task FailStep(object payload, Exception exception);
}

/// <summary>
/// A working in-memory recovery store whose deletes always fail — the post-callback cleanup
/// fault the dispatcher must treat as bookkeeping, never as a failed response.
/// </summary>
internal sealed class DeleteFailingRecoveryStateStore : IRecoveryStateStore
{
    private readonly Dictionary<string, List<RecoveryState>> _states = [];
    private readonly object _gate = new();

    public Task SaveAsync(string correlationId, RecoveryState state, TimeSpan ttl, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_states.TryGetValue(correlationId, out var list))
                _states[correlationId] = list = [];
            list.Add(state);
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<RecoveryState>> GetAllAsync(string correlationId, CancellationToken cancellationToken = default)
    {
        lock (_gate)
            return Task.FromResult<IReadOnlyList<RecoveryState>>(
                _states.TryGetValue(correlationId, out var list) ? [.. list] : []);
    }

    public Task<bool> TryDeleteAsync(string correlationId, Guid registrationId, CancellationToken cancellationToken = default)
        => throw new InvalidOperationException("recovery store rejected the delete");
}

public sealed class IncidentFlowSpy : IIncidentFlowSpy, ITypedParameterFlowSpy, IBaseParameterFlowSpy, ITypedFailureFlowSpy
{
    private readonly object _gate = new();
    private int _typedFailures;

    /// <summary>Runs inside every resume, after it is recorded.</summary>
    public Action? OnResume { get; set; }

    public int TypedFailures => Volatile.Read(ref _typedFailures);
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
        OnResume?.Invoke();
        return Task.CompletedTask;
    }

    public Task FailTyped(IncidentStepResult payload, Exception exception)
    {
        Interlocked.Increment(ref _typedFailures);
        return Task.CompletedTask;
    }

    public Task FailViaInterface(IAsyncResponsePayload payload, Exception exception)
    {
        Interlocked.Increment(ref _typedFailures);
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
