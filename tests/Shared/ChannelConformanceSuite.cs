using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using Xunit;

namespace AsyncResponse.Conformance;

/// <summary>
/// The channel behavioral contract: one shared suite of <c>Contract_*</c> facts run against every
/// response-channel implementation, so the in-memory channel stops being a de-facto spec and
/// becomes an enforced one. The suite drives channels only through the public consumption surface
/// an application gets from DI — <c>AddAsyncResponse().With…Channel(…)</c>, then
/// <see cref="IAsyncResponseSubscriber"/> (subscribe + <c>Until</c> predicate + timeout +
/// awaiting the waiter), <see cref="IAsyncResponsePublisher"/>, <see cref="IRawAsyncResponsePublisher"/>,
/// <see cref="IActiveSubscriberProbe"/>, and recovery-callback registration through
/// <see cref="IRecoveryStateStore"/> (the one recovery-arming API every channel shares — the
/// in-memory channel has no <c>IRecoverableAsyncResponseSubscriber</c>).
/// <para>
/// This file is <c>&lt;Compile Include&gt;</c>-linked into both the unit-test project (the in-memory
/// derivation — the spec) and the integration-test project (the Redis / NATS / PostgreSQL /
/// SQL Server / MongoDB derivations against real containers), mirroring the
/// <c>src/Channels/Shared</c> pattern. Behavior was calibrated against the in-memory channel first;
/// where a durable channel legitimately cannot match it (the wire envelope carries no CLR exception
/// type), the assertion pins the common contract and the comment explains the boundary.
/// </para>
/// </summary>
public abstract class ChannelConformanceSuite
{
    /// <summary>How often the eventually-loops re-check their condition.</summary>
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(25);

    /// <summary>
    /// A deliberately short waiter timeout for the facts that assert timeout behavior. Kept well
    /// below every channel's <see cref="Generous"/> so the surrounding wait budget can always
    /// observe the fault.
    /// </summary>
    private static readonly TimeSpan ShortTimeout = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Grace period after which "nothing happened" assertions (a waiter that must still be pending)
    /// are checked. Not a delivery bound — only a settle window for negative assertions.
    /// </summary>
    private static readonly TimeSpan SettleDelay = TimeSpan.FromMilliseconds(150);

    /// <summary>TTL for recovery registrations armed by the suite; only needs to outlive one fact.</summary>
    private static readonly TimeSpan RecoveryTtl = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Per-channel latency budget. The in-memory channel completes synchronously (~2s is already
    /// generous); polling database channels need ~15s under a loaded CI fixture. Assertions never
    /// require exact timing — the budget only bounds waits and eventually-loops.
    /// </summary>
    protected abstract TimeSpan Generous { get; }

    /// <summary>
    /// Builds a fully wired provider with the channel under test registered via its public
    /// <c>AddAsyncResponse().With…Channel(…)</c> registration. Implementations start from
    /// <see cref="NewConformanceServices"/> (which registers the null logger and the recovery spy),
    /// add whatever client singletons the channel requires, and hand any external teardown
    /// (dropping a schema/database, disposing a connection) to the harness. Called once per fact —
    /// every fact runs against a fresh, isolated channel. <paramref name="configureChannel"/> is
    /// applied LAST inside the channel's options lambda, so a fact can tighten a shared knob (e.g.
    /// <see cref="AsyncResponseChannelOptions.DisposalDrainTimeout"/>) on any derivation.
    /// </summary>
    protected abstract Task<ChannelConformanceHarness> CreateHarnessAsync(
        Action<AsyncResponseChannelOptions>? configureChannel = null);

    /// <summary>Bound applied when awaiting a response that must arrive.</summary>
    private TimeSpan WaitBudget => Generous * 2;

    /// <summary>
    /// Timeout given to waiters that must not time out during the fact. Large enough that even a
    /// slow polling channel completes the wait first.
    /// </summary>
    private TimeSpan WaiterTimeout => Generous * 4;

    /// <summary>
    /// Delay used by the delayed correlation-id-reuse variant: scaled with the channel budget so a
    /// polling channel gets a proportional gap, floored at the ~50 ms the in-memory spec pins.
    /// </summary>
    private TimeSpan ReuseDelay => TimeSpan.FromMilliseconds(Math.Max(50, Generous.TotalMilliseconds / 40));

    // ----- a. Live delivery -----

    [Fact]
    public async Task Contract_ResponseCompletesLiveWaiter()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("live");

        await using var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout);
        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "done"), correlationId);

        var result = await waiter.ResponseTask.WaitAsync(WaitBudget);
        Assert.Equal(ConformanceStatus.Completed, result.Status);
        Assert.Equal("done", result.Message);
    }

    // ----- b. Until predicate -----

    [Fact]
    public async Task Contract_UntilPredicateHoldsWaiterUntilMatchingResponse()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("until");
        var observed = new ConcurrentQueue<string?>();

        await using var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId,
            payload =>
            {
                observed.Enqueue(payload.Message);
                return new ValueTask<bool>(payload.Status == ConformanceStatus.Completed);
            },
            WaiterTimeout);

        // The non-matching progress response is delivered (the predicate sees it) but must not
        // complete the wait.
        await harness.Publisher.SetResponse(Result(ConformanceStatus.Running, "progress"), correlationId);
        await EventuallyAsync(
            () => Task.FromResult(observed.Contains("progress")),
            "the non-matching response reaches the Until predicate");
        Assert.False(waiter.ResponseTask.IsCompleted);

        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "final"), correlationId);
        var result = await waiter.ResponseTask.WaitAsync(WaitBudget);

        Assert.Equal("final", result.Message);
        Assert.Contains("progress", observed);
        Assert.Contains("final", observed);
    }

    // ----- c. SetException faults the waiter -----

    [Fact]
    public async Task Contract_SetExceptionFaultsWaiterWithRemoteError()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("exception");

        await using var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout);
        await harness.Publisher.SetException(new InvalidOperationException("remote boom"), correlationId);

        var exception = await Assert.ThrowsAnyAsync<Exception>(() => waiter.ResponseTask.WaitAsync(WaitBudget));

        // The preserved message is the cross-channel contract. The concrete exception type is
        // representational: the in-memory channel faults the waiter with the original instance,
        // while wire channels rehydrate the failure envelope — which intentionally carries only
        // the message and an optional remote stack trace, never the CLR type — as a base
        // System.Exception. Pinning the type would freeze a serialization-boundary artifact,
        // not behavior.
        Assert.Equal("remote boom", exception.Message);
        Assert.IsNotType<TimeoutException>(exception);
        Assert.IsNotType<OperationCanceledException>(exception);
    }

    // ----- d. Timeout -----

    [Fact]
    public async Task Contract_TimeoutFaultsWaiterWithTimeoutException()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("timeout");

        await using var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: ShortTimeout);

        var exception = await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask.WaitAsync(WaitBudget));

        // Every channel faults with the in-memory channel's TimeoutException, whose message names
        // the correlation id. Asserting the id also proves the fault came from the channel's own
        // timeout, not from this test's outer WaitAsync bound.
        Assert.Contains(correlationId, exception.Message, StringComparison.Ordinal);
    }

    // ----- e. Correlation isolation -----

    [Fact]
    public async Task Contract_ResponseCompletesOnlyItsOwnCorrelationId()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationIdA = NewCorrelationId("isolation-a");
        var correlationIdB = NewCorrelationId("isolation-b");

        await using var waiterA = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationIdA, timeout: WaiterTimeout);
        await using var waiterB = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationIdB, timeout: WaiterTimeout);

        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "for-a"), correlationIdA);

        var resultA = await waiterA.ResponseTask.WaitAsync(WaitBudget);
        Assert.Equal("for-a", resultA.Message);

        // B must remain pending: give the channel a settle window, then assert nothing crossed over.
        await Task.Delay(SettleDelay);
        Assert.False(waiterB.ResponseTask.IsCompleted);
    }

    // ----- f/g/h. Lost-subscriber routing -----

    [Fact]
    public async Task Contract_NoSubscriber_ResumablePayloadInvokesResumeCallback()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("lost-resume");
        await ArmRecoveryCallbacksAsync(harness, correlationId);

        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "late resume"), correlationId);

        await EventuallyAsync(
            () => Task.FromResult(harness.Spy.Resumed.Count == 1),
            "the resumable payload routes to the resume callback");
        var resumed = Assert.IsType<ConformanceResult>(Assert.Single(harness.Spy.Resumed));
        Assert.Equal("late resume", resumed.Message);
        Assert.Empty(harness.Spy.Failures);
    }

    [Fact]
    public async Task Contract_NoSubscriber_NonResumablePayloadInvokesFailureCallback()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("lost-fail");
        await ArmRecoveryCallbacksAsync(harness, correlationId);

        await harness.Publisher.SetResponse(Result(ConformanceStatus.Failed, "domain failed"), correlationId);

        await EventuallyAsync(
            () => Task.FromResult(harness.Spy.Failures.Count == 1),
            "the non-resumable payload routes to the failure callback");
        // A domain failure surfaces as the wrapped AsyncResponseDomainFailureException so flow code
        // can distinguish it from a technical SetException while sharing one failure path.
        var failure = Assert.IsType<AsyncResponseDomainFailureException>(Assert.Single(harness.Spy.Failures));
        Assert.Equal(correlationId, failure.CorrelationId);
        Assert.Equal(typeof(ConformanceResult).FullName, failure.PayloadTypeFullName);
        Assert.NotNull(failure.PayloadJson);
        Assert.Contains("domain failed", failure.PayloadJson, StringComparison.Ordinal);
        Assert.Empty(harness.Spy.Resumed);
    }

    [Fact]
    public async Task Contract_NoSubscriber_SetExceptionInvokesFailureCallback()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("lost-exception");
        await ArmRecoveryCallbacksAsync(harness, correlationId);

        await harness.Publisher.SetException(new InvalidOperationException("late exception"), correlationId);

        await EventuallyAsync(
            () => Task.FromResult(harness.Spy.Failures.Count == 1),
            "the exception routes to the failure callback");
        // The publisher process dispatches the lost-subscriber callbacks in-process, so the
        // original exception instance (type and message) reaches the callback on every channel.
        var failure = Assert.IsType<InvalidOperationException>(Assert.Single(harness.Spy.Failures));
        Assert.Equal("late exception", failure.Message);
        Assert.Empty(harness.Spy.Resumed);
    }

    // ----- f2/g2/h2. Lost-subscriber recovery over the raw ingress (incident 292332 regression) -----
    //
    // The production shape of the lost-subscriber path: the waiter's process died (redeploy), the
    // response arrives through a broker ingress as raw JSON, and the payload type is known only
    // from the persisted recovery registration. These facts pin the two guarantees whose absence
    // deadlocked a real flow (Optimatic 292332/292683): the recovery callback must receive the
    // MATERIALIZED payload (an object-typed callback parameter received a raw JsonElement, so
    // every `is` guard in the flow silently failed), and a non-terminal checkpoint must not
    // consume the recovery registration out from under the terminal response that follows it.

    [Fact]
    public async Task Contract_NoSubscriber_RawJsonResumablePayload_ResumeCallbackReceivesMaterializedPayload()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("lost-raw-typed");
        await ArmRecoveryCallbacksAsync(harness, correlationId);

        await harness.RawPublisher.SetRawResponseJson("""{"Status":2,"Message":"late raw resume"}""", correlationId);

        await EventuallyAsync(
            () => Task.FromResult(harness.Spy.Resumed.Count == 1),
            "the raw-JSON resumable payload routes to the resume callback");

        // The callback parameter is declared `object` (the spy mirrors production flow services
        // that share one callback across payload types). It must still receive the materialized
        // payload type recorded in the recovery registration — not the ingress's raw JsonElement.
        var resumed = Assert.IsType<ConformanceResult>(Assert.Single(harness.Spy.Resumed));
        Assert.Equal(ConformanceStatus.Completed, resumed.Status);
        Assert.Equal("late raw resume", resumed.Message);
        Assert.Empty(harness.Spy.Failures);
    }

    [Fact]
    public async Task Contract_NoSubscriber_NonTerminalCheckpoint_RetainsRegistrationForLaterTerminal()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("keep-waiting");
        await ArmRecoveryCallbacksAsync(harness, correlationId);

        // A non-terminal checkpoint (progress report) arrives after the waiter died. It must not
        // resume, must not fail, and must NOT consume the registration — the flow's real outcome
        // is still in flight.
        await harness.RawPublisher.SetRawResponseJson("""{"Status":1,"Message":"still running"}""", correlationId);

        await Task.Delay(SettleDelay);
        Assert.Empty(harness.Spy.Resumed);
        Assert.Empty(harness.Spy.Failures);
        Assert.NotEmpty(await harness.RecoveryStateStore.GetAllAsync(correlationId));

        // The retained registration is live, not a leftover: the terminal response that follows
        // must route through it with the materialized payload. (Timing-proof: even if the
        // checkpoint's dispatch were still in flight above, a consumed registration here would
        // drop the terminal response and this wait would fail.)
        await harness.RawPublisher.SetRawResponseJson("""{"Status":2,"Message":"terminal after checkpoint"}""", correlationId);

        await EventuallyAsync(
            () => Task.FromResult(harness.Spy.Resumed.Count == 1),
            "the terminal response routes through the registration the checkpoint left armed");
        var resumed = Assert.IsType<ConformanceResult>(Assert.Single(harness.Spy.Resumed));
        Assert.Equal("terminal after checkpoint", resumed.Message);
        Assert.Empty(harness.Spy.Failures);
    }

    [Fact]
    public async Task Contract_NoSubscriber_CheckpointsThenTerminal_ResumesExactlyOnceWithTerminalPayload()
    {
        // The full 292332 message sequence: the waiter died mid-step; the remote side then
        // publishes two in-progress checkpoints and the terminal success a few seconds later.
        // Contract: exactly ONE resume, carrying the MATERIALIZED terminal payload — never three
        // resumed workers, never a checkpoint payload, never a dropped terminal response.
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("incident-replay");
        await ArmRecoveryCallbacksAsync(harness, correlationId);

        await harness.RawPublisher.SetRawResponseJson("""{"Status":1,"Message":"checkpoint-1"}""", correlationId);
        await harness.RawPublisher.SetRawResponseJson("""{"Status":1,"Message":"checkpoint-2"}""", correlationId);
        await harness.RawPublisher.SetRawResponseJson("""{"Status":2,"Message":"pipeline succeeded"}""", correlationId);

        await EventuallyAsync(
            () => Task.FromResult(harness.Spy.Resumed.Count >= 1),
            "the terminal response resumes the flow");

        var resumed = Assert.IsType<ConformanceResult>(Assert.Single(harness.Spy.Resumed));
        Assert.Equal(ConformanceStatus.Completed, resumed.Status);
        Assert.Equal("pipeline succeeded", resumed.Message);
        Assert.Empty(harness.Spy.Failures);

        // The terminal dispatch consumed the registration; nothing is left to fire again.
        await EventuallyAsync(
            async () => (await harness.RecoveryStateStore.GetAllAsync(correlationId)).Count == 0,
            "the consumed registration is deleted");
    }

    // ----- i. Raw JSON ingress path -----

    [Fact]
    public async Task Contract_RawJsonResponseCompletesLiveWaiter()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("raw");

        await using var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout);
        await harness.RawPublisher.SetRawResponseJson("""{"Status":2,"Message":"raw"}""", correlationId);

        var result = await waiter.ResponseTask.WaitAsync(WaitBudget);
        Assert.Equal(ConformanceStatus.Completed, result.Status);
        Assert.Equal("raw", result.Message);
    }

    // ----- j. Correlation-id reuse after completion -----

    [Fact]
    public Task Contract_CorrelationIdReuseAfterCompletion_ImmediateResubscribe()
        => RunCorrelationIdReuseAsync(delayBeforeResubscribe: TimeSpan.Zero);

    [Fact]
    public Task Contract_CorrelationIdReuseAfterCompletion_DelayedResubscribe()
        => RunCorrelationIdReuseAsync(delayBeforeResubscribe: ReuseDelay);

    /// <summary>
    /// Pins the tombstone/registration-order behavior: after waiter #1 on a correlation id
    /// completes, waiter #2 registered on the same id must receive the <em>next</em> response —
    /// never the already-delivered one (durable channels keep the first message row around) and
    /// never nothing (a stale unsubscribe from waiter #1's async cleanup must not tear down
    /// waiter #2's fresh registration).
    /// </summary>
    private async Task RunCorrelationIdReuseAsync(TimeSpan delayBeforeResubscribe)
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("reuse");

        await using (var first = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout))
        {
            await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "first"), correlationId);
            var firstResult = await first.ResponseTask.WaitAsync(WaitBudget);
            Assert.Equal("first", firstResult.Message);
        }

        if (delayBeforeResubscribe > TimeSpan.Zero)
            await Task.Delay(delayBeforeResubscribe);

        await using var second = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout);
        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "second"), correlationId);

        var secondResult = await second.ResponseTask.WaitAsync(WaitBudget);
        Assert.Equal("second", secondResult.Message);
    }

    // ----- k. Active-subscriber probe -----

    [Fact]
    public async Task Contract_ProbeCountsLiveWaiterAndReturnsToZeroAfterCompletion()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("probe");

        var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout);
        try
        {
            // ≥ 1, not == 1: the NATS probe reports presence (0 or 1) by design, and heartbeat-based
            // probes may briefly report more during refresh. "Somebody is listening" is the contract.
            await EventuallyAsync(
                async () => await harness.Probe.CountActiveSubscribersAsync(correlationId) >= 1,
                "the probe reports a live waiter");

            await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "done"), correlationId);
            await waiter.ResponseTask.WaitAsync(WaitBudget);
        }
        finally
        {
            await waiter.DisposeAsync();
        }

        // Heartbeat/cleanup timing differs per channel — poll within the budget for the count to drop.
        await EventuallyAsync(
            async () => await harness.Probe.CountActiveSubscribersAsync(correlationId) == 0,
            "the probe returns to zero after completion and cleanup");
    }

    // ----- l. Late response after waiter timeout -----

    [Fact]
    public async Task Contract_LateResponseAfterWaiterTimeout_RoutesToRecoveryCallback()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("late");
        await ArmRecoveryCallbacksAsync(harness, correlationId);

        await using (var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: ShortTimeout))
        {
            var timeout = await Assert.ThrowsAsync<TimeoutException>(() => waiter.ResponseTask.WaitAsync(WaitBudget));
            Assert.Contains(correlationId, timeout.Message, StringComparison.Ordinal);
        }

        // The waiter's timeout cleanup removes only its own registration and live subscription; the
        // independently armed recovery registration must survive. Wait for the subscription to be
        // fully gone before publishing — this models the real scenario (the waiting process is
        // dead by the time the late response arrives) instead of racing the async unsubscribe.
        await EventuallyAsync(
            async () => await harness.Probe.CountActiveSubscribersAsync(correlationId) == 0,
            "the timed-out waiter's subscription is fully cleaned up");

        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "late"), correlationId);

        await EventuallyAsync(
            () => Task.FromResult(harness.Spy.Resumed.Count == 1),
            "the late response routes to the resume callback instead of being lost");
        var resumed = Assert.IsType<ConformanceResult>(Assert.Single(harness.Spy.Resumed));
        Assert.Equal("late", resumed.Message);
        Assert.Empty(harness.Spy.Failures);
    }

    // ----- m. Manual waiter disposal -----

    [Fact]
    public async Task Contract_DisposeBeforeTerminalSignal_CancelsResponseTask()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("dispose");

        var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout);
        var responseTask = waiter.ResponseTask;

        await waiter.DisposeAsync();

        // Disposal tears down the subscription AND the timeout, so nothing else could ever
        // complete the task — a caller holding ResponseTask directly (manual lifetime control,
        // or any in-flight wait when the channel itself is disposed at host shutdown) must
        // observe cancellation instead of hanging forever.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => responseTask.WaitAsync(WaitBudget));
        Assert.True(responseTask.IsCanceled);
    }

    [Fact]
    public async Task Contract_DisposeDuringInFlightDelivery_SettlesAsDelivered()
    {
        // Disposal must DRAIN an in-flight delivery before settling: the channel has already
        // claimed the terminal message when the Until predicate runs, so a dispose racing that
        // delivery must observe the delivered payload — cancelling would make the consumed
        // response vanish (the strand a re-attaching flow then waits a full step timeout for).
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("drain");
        using var predicateEntered = new SemaphoreSlim(0);
        using var releasePredicate = new SemaphoreSlim(0);

        var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId,
            async _ =>
            {
                predicateEntered.Release();
                // Asserted, not ignored: a silently-lapsed wait once let the predicate finish
                // early and the fact "passed" against an already-settled waiter.
                Assert.True(await releasePredicate.WaitAsync(WaitBudget), "the blocked predicate was never released");
                return true;
            },
            timeout: WaiterTimeout);

        // Publish WITHOUT awaiting: the in-memory publisher dispatches inline, so awaiting it
        // would serialize the whole delivery (blocked predicate included) BEFORE the dispose and
        // the fact would exercise no overlap at all.
        var publish = harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "drained"), correlationId);
        Assert.True(await predicateEntered.WaitAsync(WaitBudget), "the delivery never reached the Until predicate");

        // Dispose directly on this thread — a Task.Run lambda is not guaranteed to have started
        // by the time the predicate is released, which would reorder dispose after delivery.
        var disposal = waiter.DisposeAsync().AsTask();

        // The claimed delivery still holds the dispatch pipeline, so a DRAINING disposal cannot
        // have finished yet; completing here means it settled without waiting for the in-flight
        // message.
        await Task.Delay(SettleDelay);
        Assert.False(disposal.IsCompleted, "disposal completed while a claimed delivery was still in flight — it did not drain");

        releasePredicate.Release();
        await disposal.WaitAsync(WaitBudget);

        var result = await waiter.ResponseTask.WaitAsync(WaitBudget);
        Assert.Equal("drained", result.Message);
        await publish.WaitAsync(WaitBudget);
    }

    [Fact]
    public async Task Contract_DisposeDrainBudgetExhausted_FaultsAsIndeterminate()
    {
        // When the in-flight delivery outlives DisposalDrainTimeout, disposal must NOT fall back
        // to cancellation: the wedged predicate holds a message the channel already claimed, and
        // a canceled task tells a re-attaching caller "nothing was delivered". The explicit
        // indeterminate fault routes durable flows to a fresh idempotent restart instead.
        await using var harness = await CreateHarnessAsync(
            options => options.DisposalDrainTimeout = TimeSpan.FromSeconds(1));
        var correlationId = NewCorrelationId("indeterminate");
        using var predicateEntered = new SemaphoreSlim(0);
        using var releasePredicate = new SemaphoreSlim(0);

        var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId,
            async _ =>
            {
                predicateEntered.Release();
                // Wedged until the fact releases it — well past the 1s drain budget. Returns
                // terminal only when genuinely released; the late TrySetResult must lose to the
                // indeterminate fault and be dropped.
                return await releasePredicate.WaitAsync(WaitBudget);
            },
            timeout: WaiterTimeout);

        var publish = harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "wedged"), correlationId);
        Assert.True(await predicateEntered.WaitAsync(WaitBudget), "the delivery never reached the Until predicate");

        // Disposal must RETURN once the budget lapses (a wedged predicate cannot hold host
        // shutdown hostage) — but with the indeterminate fault, never a cancellation.
        var disposal = waiter.DisposeAsync().AsTask();
        await disposal.WaitAsync(WaitBudget);

        var fault = await Assert.ThrowsAsync<AsyncResponseIndeterminateDeliveryException>(
            () => waiter.ResponseTask.WaitAsync(WaitBudget));
        Assert.Equal(correlationId, fault.CorrelationId);
        Assert.False(waiter.ResponseTask.IsCanceled);

        releasePredicate.Release();
        await publish.WaitAsync(WaitBudget);
    }

    [Fact]
    public async Task Contract_UnsupportedTimeout_FailsFastWithoutSideEffects()
    {
        // One timeout rule on every channel: positive (zero used to be accepted by some channels
        // — subscribing, persisting recovery state, then insta-timing-out — and rejected by
        // others) and at most ~49.7 days (uint.MaxValue - 1 ms, the ceiling of every BCL timer
        // the waiters arm — the runtime rejects longer only at arming, which used to surface
        // AFTER the subscription and recovery state existed, leaking both).
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("ceiling");

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            harness.Subscriber.CreateResponseWaiter<ConformanceResult>(correlationId, timeout: TimeSpan.FromDays(50)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            harness.Subscriber.CreateResponseWaiter<ConformanceResult>(correlationId, timeout: TimeSpan.Zero));

        Assert.Empty(await harness.RecoveryStateStore.GetAllAsync(correlationId));

        // The correlation id is fully usable afterwards — nothing half-registered lingers.
        await using var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout);
        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "after"), correlationId);
        Assert.Equal("after", (await waiter.ResponseTask.WaitAsync(WaitBudget)).Message);
    }

    // ----- helpers -----

    /// <summary>
    /// Baseline service collection every harness starts from: a silent logger and the recovery spy
    /// the lost-subscriber callbacks resolve from DI by interface full name.
    /// </summary>
    protected static IServiceCollection NewConformanceServices()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IConformanceRecoverySpy>(new ConformanceRecoverySpy());
        return services;
    }

    private static string NewCorrelationId(string prefix) => $"conformance-{prefix}-{Guid.NewGuid():N}";

    private static ConformanceResult Result(ConformanceStatus status, string? message = null)
        => new() { Status = status, Message = message };

    /// <summary>
    /// Registers durable recovery callbacks for <paramref name="correlationId"/> through the same
    /// <see cref="IRecoveryStateStore"/> every channel package registers — the uniform public
    /// recovery-arming surface (and the exact state a recoverable waiter would persist).
    /// </summary>
    private static Task ArmRecoveryCallbacksAsync(ChannelConformanceHarness harness, string correlationId)
        => harness.RecoveryStateStore.SaveAsync(
            correlationId,
            new RecoveryState
            {
                RegistrationId = Guid.NewGuid(),
                CorrelationId = correlationId,
                PayloadTypeFullName = typeof(ConformanceResult).FullName,
                RegisteredAtUtc = DateTime.UtcNow,
                ResumeCallback = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IConformanceRecoverySpy).FullName!,
                    MethodName = nameof(IConformanceRecoverySpy.OnResume),
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
                },
                FailureCallback = new ReflectionCallDto
                {
                    ServiceInterfaceFullName = typeof(IConformanceRecoverySpy).FullName!,
                    MethodName = nameof(IConformanceRecoverySpy.OnFailure),
                    Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception)]
                }
            },
            RecoveryTtl);

    /// <summary>Polls <paramref name="condition"/> until it holds or the wait budget elapses.</summary>
    private async Task EventuallyAsync(Func<Task<bool>> condition, string description)
    {
        var deadline = DateTime.UtcNow + WaitBudget;
        while (true)
        {
            if (await condition())
                return;

            if (DateTime.UtcNow >= deadline)
                Assert.Fail($"Timed out after {WaitBudget} waiting until {description}.");

            await Task.Delay(PollInterval);
        }
    }
}

/// <summary>
/// One fully wired channel under test: the provider built by the per-channel factory plus any
/// external teardown (drop schema/database, dispose client connections). Disposing the harness
/// disposes the provider first (stopping the channel) and then runs the teardown.
/// </summary>
public sealed class ChannelConformanceHarness(ServiceProvider provider, Func<ValueTask>? teardownAsync = null)
    : IAsyncDisposable
{
    public ServiceProvider Provider { get; } = provider;

    public IAsyncResponsePublisher Publisher => Provider.GetRequiredService<IAsyncResponsePublisher>();
    internal IRawAsyncResponsePublisher RawPublisher => Provider.GetRequiredService<IRawAsyncResponsePublisher>();
    public IAsyncResponseSubscriber Subscriber => Provider.GetRequiredService<IAsyncResponseSubscriber>();
    public IActiveSubscriberProbe Probe => Provider.GetRequiredService<IActiveSubscriberProbe>();
    public IRecoveryStateStore RecoveryStateStore => Provider.GetRequiredService<IRecoveryStateStore>();
    public ConformanceRecoverySpy Spy => (ConformanceRecoverySpy)Provider.GetRequiredService<IConformanceRecoverySpy>();

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try
        {
            await Provider.DisposeAsync();
        }
        finally
        {
            if (teardownAsync is not null)
                await teardownAsync();
        }
    }
}

/// <summary>Status model for <see cref="ConformanceResult"/>; values match the raw-JSON fixture literals.</summary>
public enum ConformanceStatus
{
    Unknown = 0,
    Running = 1,
    Completed = 2,
    Failed = 3
}

/// <summary>
/// The suite's payload: a typical remote-operation result with explicit lost-subscriber recovery
/// semantics. Self-contained (with conformance-specific names) because this shared source compiles
/// into both test assemblies, each of which already owns an unrelated <c>OperationResult</c>.
/// </summary>
public sealed class ConformanceResult : IAsyncResponsePayload
{
    public ConformanceStatus Status { get; set; }
    public string? Message { get; set; }

    /// <summary>
    /// Recovery routing only: resume on success, keep the registration armed on a non-terminal
    /// checkpoint (<see cref="ConformanceStatus.Running"/> — the response stream reports progress
    /// before its terminal message), fail otherwise. Classifying Running as resume would consume
    /// the registration on a progress message and let the real terminal response arrive with
    /// nothing to route against — the 292332 flow-deadlock shape.
    /// </summary>
    public RecoveryAction OnRecovery() => Status switch
    {
        ConformanceStatus.Completed => RecoveryAction.Resume,
        ConformanceStatus.Running => RecoveryAction.KeepWaiting,
        _ => RecoveryAction.Fail,
    };
}

/// <summary>
/// Stand-in for a flow service: lost-subscriber callbacks resolve it from DI by interface full name
/// and invoke it via reflection, exactly like production resume/fail handlers.
/// </summary>
public interface IConformanceRecoverySpy
{
    Task OnResume(object payload);
    Task OnFailure(Exception exception);
}

/// <summary>Thread-safe recording implementation of <see cref="IConformanceRecoverySpy"/>.</summary>
public sealed class ConformanceRecoverySpy : IConformanceRecoverySpy
{
    private readonly object _gate = new();
    private readonly List<object> _resumed = [];
    private readonly List<Exception> _failures = [];

    public IReadOnlyList<object> Resumed
    {
        get
        {
            lock (_gate)
                return [.. _resumed];
        }
    }

    public IReadOnlyList<Exception> Failures
    {
        get
        {
            lock (_gate)
                return [.. _failures];
        }
    }

    /// <inheritdoc />
    public Task OnResume(object payload)
    {
        lock (_gate)
            _resumed.Add(payload);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task OnFailure(Exception exception)
    {
        lock (_gate)
            _failures.Add(exception);
        return Task.CompletedTask;
    }
}
