using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Loader;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Coverage-gap pins for the long tail of small AsyncResponse.Core / Abstractions helpers: each
/// fact drives one branch the rest of the suite never reached and asserts what that branch
/// decides, not merely that it ran.
/// </summary>
public sealed class CoreTailCoverageGapTests
{
    // ---------------------------------------------------------------------------------------
    // CurrentReadFlowStateStore: every member forwards to the inner store unchanged.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task CurrentReadFlowStateStore_ForwardsEveryMember_AndReadsThroughTheCurrentLoad()
    {
        var state = new FlowState { FlowId = "f", Revision = 3 };
        var observation = new FlowLeaseObservation("other-lease", DateTime.UtcNow.AddMinutes(1));
        var inner = new Mock<IFlowStateStore>(MockBehavior.Strict);
        inner.Setup(s => s.ValidateCreate("f", state, TimeSpan.FromMinutes(1)));
        inner.Setup(s => s.TryCreateAsync("f", state, TimeSpan.FromMinutes(1), It.IsAny<CancellationToken>())).ReturnsAsync(true);
        inner.Setup(s => s.LoadCurrentAsync("f", It.IsAny<CancellationToken>())).ReturnsAsync(state);
        inner.Setup(s => s.TryAcquireLeaseAsync("f", "l", TimeSpan.FromSeconds(5), It.IsAny<CancellationToken>())).ReturnsAsync(true);
        inner.Setup(s => s.TryRenewLeaseAsync("f", "l", TimeSpan.FromSeconds(6), It.IsAny<CancellationToken>())).ReturnsAsync(false);
        inner.Setup(s => s.ReleaseLeaseAsync("f", "l", It.IsAny<CancellationToken>())).Returns(Task.CompletedTask);
        inner.Setup(s => s.ObserveLeaseAsync("f", It.IsAny<CancellationToken>())).ReturnsAsync(observation);
        inner.Setup(s => s.TryDeleteAsync("f", It.IsAny<CancellationToken>())).ReturnsAsync(true);
        inner.Setup(s => s.TryUpdateAsync("f", state, 3, TimeSpan.FromMinutes(1), "l", It.IsAny<CancellationToken>())).ReturnsAsync(true);

        IFlowStateStore store = new CurrentReadFlowStateStore(inner.Object);

        store.ValidateCreate("f", state, TimeSpan.FromMinutes(1));
        Assert.True(await store.TryCreateAsync("f", state, TimeSpan.FromMinutes(1)));
        Assert.Same(state, await store.LoadAsync("f"));
        Assert.Same(state, await store.LoadCurrentAsync("f"));
        Assert.True(await store.TryAcquireLeaseAsync("f", "l", TimeSpan.FromSeconds(5)));
        Assert.False(await store.TryRenewLeaseAsync("f", "l", TimeSpan.FromSeconds(6)));
        await store.ReleaseLeaseAsync("f", "l");
        Assert.Same(observation, await store.ObserveLeaseAsync("f"));
        Assert.True(await store.TryDeleteAsync("f"));
        Assert.True(await store.TryUpdateAsync("f", state, 3, TimeSpan.FromMinutes(1), "l"));

        // The plain load is the CURRENT load: the inner LoadAsync was never consulted (strict mock).
        inner.Verify(s => s.LoadCurrentAsync("f", It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    // ---------------------------------------------------------------------------------------
    // AsyncLocal scopes: disposal is idempotent and restores what was ambient before.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void WorkerJobScope_DisposedTwice_RestoresThePreviousJobOnce()
    {
        var outer = new WorkerJobEnvelope { Call = new ReflectionCallDto { ServiceInterfaceFullName = "S", MethodName = "Outer", Params = [] } };
        var inner = new WorkerJobEnvelope { Call = new ReflectionCallDto { ServiceInterfaceFullName = "S", MethodName = "Inner", Params = [] } };

        using var outerScope = WorkerJobScope.Enter(outer);
        var innerScope = WorkerJobScope.Enter(inner);
        Assert.Same(inner, WorkerJobScope.Current);

        innerScope.Dispose();
        Assert.Same(outer, WorkerJobScope.Current);

        // A later job entered after the first disposal is not clobbered by a second one.
        using var later = WorkerJobScope.Enter(inner);
        innerScope.Dispose();
        Assert.Same(inner, WorkerJobScope.Current);
    }

    [Fact]
    public void WorkerJobSkewScope_UnmarkedScopeDisposedTwice_RestoresTheMarkerOnce()
    {
        using var marked = WorkerJobSkewScope.Enter();
        Assert.True(WorkerJobSkewScope.IsForcedEarlyExecution);

        var unmarked = WorkerJobSkewScope.EnterUnmarked();
        Assert.False(WorkerJobSkewScope.IsForcedEarlyExecution);

        unmarked.Dispose();
        Assert.True(WorkerJobSkewScope.IsForcedEarlyExecution);

        // The second disposal must not re-apply the stale marker over a newer unmarked scope.
        using var again = WorkerJobSkewScope.EnterUnmarked();
        unmarked.Dispose();
        Assert.False(WorkerJobSkewScope.IsForcedEarlyExecution);
    }

    // ---------------------------------------------------------------------------------------
    // Input equivalence: a null persisted side and an unreadable side are mismatches, not throws.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void DurableFlowRegistration_InputEquivalent_NullPersistedOrUnreadableSide_IsAMismatch()
    {
        var throwing = Registration(_ => throw new FormatException("cannot read"));

        Assert.False(throwing.InputEquivalent(null, "{\"A\":1}"));
        Assert.False(throwing.InputEquivalent("{\"A\":1}", "{\"A\":2}"));
        Assert.True(throwing.InputEquivalent("{\"A\":1}", "{ \"A\" : 1 }"));
    }

    [Fact]
    public void FlowStateJson_InputEquivalent_NullPersisted_IsAMismatch()
    {
        Assert.False(FlowStateJson.InputEquivalent<Dictionary<string, int>>(null, "{\"A\":1}"));
        Assert.True(FlowStateJson.InputEquivalent<Dictionary<string, int>>("{\"A\":1}", "{\"A\":1}"));
    }

    private static DurableFlowRegistration Registration(Func<string, object?> deserialize) => new()
    {
        FlowTypeFullName = "Flow",
        InputTypeFullName = "Input",
        FlowType = typeof(object),
        DeserializeInput = deserialize,
        ExecuteAsync = (_, _, _) => Task.CompletedTask
    };

    // ---------------------------------------------------------------------------------------
    // Small guards.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void ReflectionCallDtoGuard_NullDescriptor_IsNamedAsTheDefect()
        => Assert.Equal("the call description is null", ReflectionCallDtoGuard.FindDefect(null));

    [Fact]
    public void CorrelationIdGuard_DroppedContractViolation_LogsAnError_WithTheDroppedExceptionWhenThereIsOne()
    {
        var logger = new CollectingLogger();

        Assert.True(CorrelationIdGuard.IsUnpublishable(" padded", logger, activity: null, "a response", dropped: null, dropContractViolations: true));
        Assert.True(CorrelationIdGuard.IsUnpublishable(" padded", logger, activity: null, "an exception", dropped: new InvalidOperationException("boom"), dropContractViolations: true));

        Assert.Collection(
            logger.Messages,
            plain =>
            {
                Assert.Contains("outside the portable contract", plain, StringComparison.Ordinal);
                Assert.DoesNotContain("Exception:", plain, StringComparison.Ordinal);
            },
            withException =>
            {
                Assert.Contains("outside the portable contract", withException, StringComparison.Ordinal);
                Assert.Contains("Exception: boom", withException, StringComparison.Ordinal);
            });
    }

    public interface IPlaceholderTarget
    {
        Task Handle(Exception error);
    }

    [Fact]
    public void CallbackExpressionConverter_UnknownPlaceholderMarker_IsRejected()
    {
        // Only a hand-built expression can call a Placeholder member other than the three markers.
        var markerInvoked = typeof(Placeholder).GetMethod("MarkerInvoked", BindingFlags.NonPublic | BindingFlags.Static)!;
        var service = Expression.Parameter(typeof(IPlaceholderTarget), "svc");
        var body = Expression.Call(
            service,
            typeof(IPlaceholderTarget).GetMethod(nameof(IPlaceholderTarget.Handle))!,
            Expression.Convert(Expression.Call(markerInvoked), typeof(Exception)));
        var lambda = Expression.Lambda<Func<IPlaceholderTarget, Task>>(body, service);

        var ex = Assert.Throws<NotSupportedException>(() => CallbackExpressionConverter.ToReflectionCall(lambda));
        Assert.Contains("MarkerInvoked", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void JsonSafety_Utf8Guard_RejectsBlankAndHtmlBodies_AndPassesJson()
    {
        var blank = Assert.Throws<InvalidDataException>(() => JsonSafety.ThrowIfClearlyNotJson(" \r\n\t"u8));
        Assert.Contains("Empty message body", blank.Message, StringComparison.Ordinal);

        var html = Assert.Throws<InvalidDataException>(() => JsonSafety.ThrowIfClearlyNotJson("  <html>secret banner</html>"u8));
        Assert.Contains("UTF-8 bytes", html.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("secret", html.Message, StringComparison.Ordinal);

        JsonSafety.ThrowIfClearlyNotJson(" {\"a\":1}"u8);
    }

    public sealed class UnsupportedMember
    {
        public Type Kind { get; set; } = typeof(string);
    }

    [Fact]
    public void AsyncResponseJson_SerializeToUtf8Bytes_UnsupportedMember_RaisesRegistrationGuidance()
    {
        var ex = Assert.Throws<NotSupportedException>(() => AsyncResponseJson.SerializeToUtf8Bytes(new UnsupportedMember()));

        // The guidance names the root type, and keeps the serializer's own failure as the cause.
        Assert.Contains(nameof(UnsupportedMember), ex.Message, StringComparison.Ordinal);
        Assert.IsType<NotSupportedException>(ex.InnerException);
    }

    [Fact]
    public void DiagnosticText_EscapedExcerpt_KeepsASurrogatePairThatFollowsAnEscapedUnit()
    {
        // The first unsafe unit starts the escaping loop; the well-formed pair after it is text.
        var escaped = DiagnosticText.EscapedExcerpt("a\n\U0001F600b", 40);

        Assert.Equal("a\\u000a\U0001F600b", escaped);
    }

    [Fact]
    public void AsyncResponseTypeResolution_DescribeForDiagnostics_Null_IsEmpty()
        => Assert.Equal(string.Empty, AsyncResponseTypeResolution.DescribeForDiagnostics(null));

    [Fact]
    public void AsyncResponseTypeResolution_ResolveLoaded_NameOutsideTheLimits_IsUnresolvable()
    {
        // A pointer decoration is refused by the limits backstop before the CLR parser sees it.
        Assert.Null(AsyncResponseTypeResolution.ResolveLoaded("System.Int32*"));
        Assert.Null(AsyncResponseTypeResolution.ResolveLoaded(new string('A', 100_000)));
        Assert.Equal(typeof(int), AsyncResponseTypeResolution.ResolveLoaded("System.Int32"));
    }

    // ---------------------------------------------------------------------------------------
    // Payload override detection fails open when the runtime cannot build the interface map.
    // ---------------------------------------------------------------------------------------

    public sealed class DefaultRecoveryPayload : IAsyncResponsePayload;

    private sealed class NoInterfaceMapType(Type delegatingType) : TypeDelegator(delegatingType)
    {
        public override InterfaceMapping GetInterfaceMap(Type interfaceType)
            => throw new NotSupportedException("no interface map on this runtime");
    }

    [Fact]
    public void AsyncResponsePayloadReflection_InterfaceMapUnavailable_FailsOpen()
    {
        // The real type inherits the conservative default, which the interface map proves...
        Assert.False(AsyncResponsePayloadReflection.OverridesOnRecovery(typeof(DefaultRecoveryPayload)));

        // ...but a runtime that cannot answer (Native AOT) must not break a correct app.
        Assert.True(AsyncResponsePayloadReflection.OverridesOnRecovery(new NoInterfaceMapType(typeof(DefaultRecoveryPayload))));
    }

    // ---------------------------------------------------------------------------------------
    // Diagnostics: a span whose started AND stopped callbacks throw still leaves the ambient span.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void StartActivity_WhenBothTheStartedAndStoppedCallbacksThrow_RestoresTheAmbientSpan()
    {
        var thisFlow = new AsyncLocal<bool> { Value = true };
        var stopAttempts = 0;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == AsyncResponseDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                thisFlow.Value ? ActivitySamplingResult.AllData : ActivitySamplingResult.None,
            ActivityStarted = activity =>
            {
                if (thisFlow.Value && activity.OperationName == "asyncresponse.test.gap_started_and_stopped")
                    throw new InvalidOperationException("exporter start failed");
            },
            ActivityStopped = activity =>
            {
                if (thisFlow.Value && activity.OperationName == "asyncresponse.test.gap_started_and_stopped")
                {
                    stopAttempts++;
                    throw new InvalidOperationException("exporter stop failed");
                }
            }
        };
        ActivitySource.AddActivityListener(listener);
        using var ambient = new Activity("gap-ambient").Start();

        var span = AsyncResponseDiagnostics.StartActivity("asyncresponse.test.gap_started_and_stopped");

        Assert.Null(span);
        Assert.Equal(1, stopAttempts);
        // The throwing stop never got to restore the ambient span; the fallback did.
        Assert.Same(ambient, Activity.Current);
    }

    // ---------------------------------------------------------------------------------------
    // In-memory flow-state store records the checkpoint size when the instrument is observed.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task InMemoryFlowStateStore_WithTheCheckpointInstrumentObserved_RecordsEachLedgerSize()
    {
        var thisFlow = new AsyncLocal<bool> { Value = true };
        var sizes = new List<long>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == AsyncResponseDiagnostics.MeterName && instrument.Name == "asyncresponse.flow_state.checkpoint.size")
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            if (!thisFlow.Value)
                return;
            foreach (var tag in tags)
            {
                if (tag.Key == "provider" && Equals(tag.Value, "InMemory"))
                    lock (sizes) sizes.Add(value);
            }
        });
        listener.Start();

        var store = new InMemoryFlowStateStore();
        var state = new FlowState { FlowId = "gap-checkpoint-size", Status = FlowRunStatus.Running };
        Assert.True(await store.TryCreateAsync(state.FlowId, state, TimeSpan.FromMinutes(5)));

        long[] recorded;
        lock (sizes) recorded = [.. sizes];
        var size = Assert.Single(recorded);
        Assert.Equal(System.Text.Encoding.UTF8.GetByteCount(FlowStateJson.Serialize(state)), size);
    }

    // ---------------------------------------------------------------------------------------
    // Startup validation.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void DurableFlowStoreMarker_ValidatesItsForwardLifetimeOnce_ThenReleasesTheCollection()
    {
        var services = new ServiceCollection();
        services.AddSingleton<InMemoryFlowStateStore>();
        var marker = new AsyncResponseDurableFlowStoreMarker(typeof(InMemoryFlowStateStore), ServiceLifetime.Scoped, services);

        Assert.Throws<InvalidOperationException>(marker.ValidateForwardLifetime);

        // The collection was released by the first check: a second one has nothing to judge.
        marker.ValidateForwardLifetime();
    }

    [Fact]
    public void ObserverLifetimeAudit_ValidatesOnce_ThenReleasesTheCollection()
    {
        var services = new ServiceCollection();
        services.AddScoped<IDurableFlowExecutionObserver>(_ => Mock.Of<IDurableFlowExecutionObserver>());
        var audit = new DurableFlowObserverLifetimeAudit(services);

        Assert.Throws<InvalidOperationException>(audit.Validate);
        audit.Validate();
    }

    [Fact]
    public async Task StartupValidator_NonPositiveWatchdogScanCap_FailsStartup()
    {
        var validator = new AsyncResponseStartupValidator(
            [],
            [],
            [],
            Microsoft.Extensions.Options.Options.Create(new AsyncResponseOptions
            {
                Watchdog = new AsyncResponseWatchdogOptions { MaxScanEntries = 0 }
            }));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));
        Assert.Contains(nameof(AsyncResponseWatchdogOptions.MaxScanEntries), ex.Message, StringComparison.Ordinal);
    }

    /// <summary>An <see cref="Assembly"/> that belongs to no load context (not a runtime assembly).</summary>
    private sealed class ContextlessAssembly : Assembly
    {
        public override AssemblyName GetName() => new("AsyncResponse.Core.Contextless");
    }

    [Fact]
    public void PackageVersions_ACoreWithoutALoadContext_BindsNoAbstractions_AndSkipsEveryOtherCoreContext()
    {
        var hostCore = typeof(AsyncResponsePackageVersions).Assembly;
        var hostAbstractions = typeof(IAsyncResponsePayload).Assembly;

        var packages = AsyncResponsePackageVersions.Loaded(new ContextlessAssembly(), [hostCore, hostAbstractions]);

        // The host's context holds "another" Core, and nothing is exempt from that skip.
        Assert.Empty(packages);
    }

    private sealed class AbstractionsRefusingContext() : AssemblyLoadContext("gap-abstractions-refusing", isCollectible: true)
    {
        protected override Assembly? Load(AssemblyName assemblyName)
            => assemblyName.Name == "AsyncResponse.Abstractions"
                ? throw new FileNotFoundException("Abstractions is not deployed next to this plugin.")
                : null;
    }

    [Fact]
    public void PackageVersions_ACoreWhoseContextCannotLoadAbstractions_BindsNone()
    {
        var hostCore = typeof(AsyncResponsePackageVersions).Assembly;
        var hostAbstractions = typeof(IAsyncResponsePayload).Assembly;
        var plugin = new AbstractionsRefusingContext();
        try
        {
            var pluginCore = plugin.LoadFromAssemblyPath(hostCore.Location);

            var packages = AsyncResponsePackageVersions.Loaded(pluginCore, [hostCore, hostAbstractions, pluginCore]);

            // Only the plugin's own Core is reported: its bound Abstractions could not be
            // resolved, so the host context's Abstractions is skipped with the host Core.
            var core = Assert.Single(packages);
            Assert.Equal("AsyncResponse.Core", core.Name);
        }
        finally
        {
            plugin.Unload();
        }
    }

    // ---------------------------------------------------------------------------------------
    // Cron.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void CronSchedule_ExposesItsExpression_AndRejectsAnEmptyListEntry()
    {
        Assert.Equal("0 9 * * MON", CronSchedule.Parse("0 9 * * MON").Expression);

        var ex = Assert.Throws<FormatException>(() => CronSchedule.Parse("1,,2 * * * *"));
        Assert.Contains("empty list entry", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CronSchedule_FromTheRepeatedFallBackHour_SkipsWallMinutesWhoseFirstPassIsAlreadyPast()
    {
        var newYork = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        // US DST 2030 ends Sunday November 3. 06:30 UTC is 01:30 EST — the SECOND pass of the
        // repeated hour. Every following 01:xx wall minute maps to its FIRST (EDT) pass, which is
        // already behind the cursor, so the next every-minute occurrence is 02:00 EST.
        var next = CronSchedule.Parse("* * * * *", newYork).GetNextOccurrence(new DateTimeOffset(2030, 11, 3, 6, 30, 0, TimeSpan.Zero));

        Assert.Equal(new DateTimeOffset(2030, 11, 3, 7, 0, 0, TimeSpan.Zero), next);
    }

    // ---------------------------------------------------------------------------------------
    // Builder: the delayed Action / ValueTask overloads publish through the delayed transport.
    // ---------------------------------------------------------------------------------------

    public interface IDelayedGapWorker
    {
        void Run(int value);

        ValueTask RunAsync(int value);
    }

    private sealed class CapturingDelayedTransport : IDelayedWorkerTransport
    {
        public List<(WorkerJobEnvelope Job, TimeSpan? Delay)> Published { get; } = [];

        public TimeSpan MaxPublishDelay => TimeSpan.FromDays(1);

        public Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
        {
            Published.Add((job, null));
            return Task.CompletedTask;
        }

        public Task PublishAsync(WorkerJobEnvelope job, TimeSpan delay, CancellationToken cancellationToken = default)
        {
            Published.Add((job, delay));
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task Builder_DelayedActionAndValueTaskOverloads_PublishDelayedJobs_AndHonorCancellation()
    {
        var transport = new CapturingDelayedTransport();
        var builder = new AsyncResponseBuilder(Mock.Of<IAsyncResponseSubscriber>(), transport);

        await builder.EnqueueWorkerAsync<IDelayedGapWorker>(worker => worker.Run(1), TimeSpan.FromMinutes(5));
        await builder.EnqueueWorkerAsync<IDelayedGapWorker>(worker => worker.RunAsync(2), TimeSpan.FromMinutes(6));

        Assert.Collection(
            transport.Published,
            first =>
            {
                Assert.Equal(nameof(IDelayedGapWorker.Run), first.Job.Call.MethodName);
                Assert.Equal(TimeSpan.FromMinutes(5), first.Delay);
            },
            second =>
            {
                Assert.Equal(nameof(IDelayedGapWorker.RunAsync), second.Job.Call.MethodName);
                Assert.Equal(TimeSpan.FromMinutes(6), second.Delay);
            });

        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        // Refused synchronously, before anything is converted or published.
        Assert.Throws<OperationCanceledException>(() => { _ = builder.EnqueueWorkerAsync<IDelayedGapWorker>(worker => worker.Run(3), TimeSpan.FromMinutes(1), cancelled.Token); });
        Assert.Throws<OperationCanceledException>(() => { _ = builder.EnqueueWorkerAsync<IDelayedGapWorker>(worker => worker.RunAsync(4), TimeSpan.FromMinutes(1), cancelled.Token); });
        Assert.Equal(2, transport.Published.Count);
    }

    // ---------------------------------------------------------------------------------------
    // Worker executor due-time guard.
    // ---------------------------------------------------------------------------------------

    public interface IDueGapWork
    {
        Task RunAsync();
    }

    private static ReflectionCallDto DueWork() => new()
    {
        ServiceInterfaceFullName = typeof(IDueGapWork).FullName!,
        MethodName = nameof(IDueGapWork.RunAsync),
        Params = []
    };

    [Fact]
    public async Task WorkerJobExecutor_EarlyJobOnATransportThatCannotReDelay_ThrowsInsteadOfRunningEarly()
    {
        var work = new Mock<IDueGapWork>();
        await using var provider = new ServiceCollection().AddSingleton(work.Object).BuildServiceProvider();
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<WorkerJobExecutor>.Instance,
            Mock.Of<IWorkerTransport>(),
            clock);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => executor.ExecuteAsync(new WorkerJobEnvelope
        {
            Call = DueWork(),
            CorrelationId = "due-cid",
            NotBeforeUtc = clock.GetUtcNow().UtcDateTime.AddHours(1)
        }));

        Assert.Contains("does not support delayed delivery", ex.Message, StringComparison.Ordinal);
        work.Verify(w => w.RunAsync(), Times.Never);
    }

    [Fact]
    public async Task WorkerJobExecutor_EarlyJob_WithDebugLogging_LogsTheHop_AndRepublishesTheRemainder()
    {
        var work = new Mock<IDueGapWork>();
        await using var provider = new ServiceCollection().AddSingleton(work.Object).BuildServiceProvider();
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        var transport = new CapturingDelayedTransport();
        var logger = new CollectingLogger();
        var executor = new WorkerJobExecutor(
            provider.GetRequiredService<IServiceScopeFactory>(),
            logger.For<WorkerJobExecutor>(),
            transport,
            clock);

        // Unspecified kind: the wire value is read as UTC (AsUtc), not as local time.
        var due = DateTime.SpecifyKind(clock.GetUtcNow().UtcDateTime.AddHours(1), DateTimeKind.Unspecified);
        await executor.ExecuteAsync(new WorkerJobEnvelope { Call = DueWork(), NotBeforeUtc = due });

        var (hop, delay) = Assert.Single(transport.Published);
        Assert.Equal(TimeSpan.FromHours(1), delay);
        Assert.Equal(DateTimeKind.Utc, WorkerJobExecutor.AsUtc(due).Kind);
        Assert.Contains(logger.Messages, message => message.Contains("re-publishing the next hop", StringComparison.Ordinal));
        work.Verify(w => w.RunAsync(), Times.Never);
        Assert.NotNull(hop.NotBeforeUtc);
    }

    // ---------------------------------------------------------------------------------------
    // Async-void guard on a class-typed service.
    // ---------------------------------------------------------------------------------------

    public class GapBaseWorker
    {
        public int Runs;

        public virtual void Run(int step) => Runs += step;
    }

    public sealed class GapAsyncVoidOverride : GapBaseWorker
    {
        public readonly TaskCompletionSource Gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public int Started;

        public override async void Run(int step)
        {
            Interlocked.Increment(ref Started);
            await Gate.Task;
        }
    }

    public sealed class GapSyncOverride : GapBaseWorker
    {
        public override void Run(int step) => Runs += 10 * step;
    }

    /// <summary>A derived registration whose generic sibling makes the name lookup ambiguous.</summary>
    public sealed class GapAmbiguousOverride : GapBaseWorker
    {
        public override void Run(int step) => Runs += 100 * step;

        public void Run<T>(int step) => Runs += 1000 * step;
    }

    private static ReflectionInvocationDto BaseRun() => new()
    {
        ServiceInterfaceFullName = typeof(GapBaseWorker).FullName!,
        MethodName = nameof(GapBaseWorker.Run),
        Params = [1]
    };

    [Fact]
    public async Task AsyncVoidOverride_OfAClassTypedService_IsRejectedBeforeItStarts()
    {
        var worker = new GapAsyncVoidOverride();
        await using var provider = new ServiceCollection().AddSingleton<GapBaseWorker>(worker).BuildServiceProvider();

        var ex = await Assert.ThrowsAsync<CallbackTargetUnresolvableException>(() => provider.InvokeAsync(BaseRun()));

        Assert.Contains("async void", ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(GapAsyncVoidOverride), ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, Volatile.Read(ref worker.Started));
        worker.Gate.SetResult();
    }

    [Fact]
    public async Task SynchronousOverrides_OfAClassTypedService_StillDispatch_EvenWhenTheLookupIsAmbiguous()
    {
        var sync = new GapSyncOverride();
        await using (var provider = new ServiceCollection().AddSingleton<GapBaseWorker>(sync).BuildServiceProvider())
            await provider.InvokeAsync(BaseRun());
        Assert.Equal(10, sync.Runs);

        var ambiguous = new GapAmbiguousOverride();
        await using (var provider = new ServiceCollection().AddSingleton<GapBaseWorker>(ambiguous).BuildServiceProvider())
            await provider.InvokeAsync(BaseRun());
        Assert.Equal(100, ambiguous.Runs);
    }

    // ---------------------------------------------------------------------------------------
    // Watchdog scan: dedupe keeps the OLDEST registration, and the cap also gates ungrouped entries.
    // ---------------------------------------------------------------------------------------

    private sealed class ListScanner(params RecoveryState[] states) : IRecoveryStateScanner
    {
        public async IAsyncEnumerable<RecoveryState> ScanAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            foreach (var state in states)
                yield return state;
            await Task.CompletedTask;
        }
    }

    private sealed class NoSubscribers : IActiveSubscriberProbe
    {
        public ValueTask<long> CountActiveSubscribersAsync(string correlationId, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(0L);
    }

    [Fact]
    public async Task WatchdogScan_KeepsTheOldestSibling_AndTruncatesAtTheCapOnUngroupedEntries()
    {
        var now = DateTime.UtcNow;
        var scanner = new ListScanner(
            new RecoveryState { CorrelationId = "gap-dup", RegisteredAtUtc = now.AddHours(-1), PayloadTypeFullName = "Newer" },
            new RecoveryState { CorrelationId = "gap-dup", RegisteredAtUtc = now.AddHours(-3), PayloadTypeFullName = "Older" },
            new RecoveryState { CorrelationId = null, RegisteredAtUtc = now.AddHours(-2) },
            new RecoveryState { CorrelationId = "", RegisteredAtUtc = now.AddHours(-2) });
        var options = Microsoft.Extensions.Options.Options.Create(new AsyncResponseOptions
        {
            Watchdog = new AsyncResponseWatchdogOptions
            {
                MaxScanEntries = 2,
                StaleAfter = TimeSpan.FromMinutes(1),
                StartupDelay = TimeSpan.Zero,
                IntervalJitter = TimeSpan.Zero
            }
        });
        using var watchdog = new AsyncResponseWatchdog(
            [scanner],
            [new NoSubscribers()],
            new AsyncResponseWatchdogState(),
            options,
            NullLogger<AsyncResponseWatchdog>.Instance);

        var scan = typeof(AsyncResponseWatchdog).GetMethod("ScanOnceAsync", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var report = await (Task<AsyncResponseWatchdogReport>)scan.Invoke(watchdog, [CancellationToken.None])!;

        Assert.True(report.Truncated);
        Assert.Equal(2, report.TotalEntries);
        var dup = Assert.Single(report.StaleEntries, entry => entry.CorrelationId == "gap-dup");
        Assert.Equal("Older", dup.PayloadTypeFullName);
    }
}
