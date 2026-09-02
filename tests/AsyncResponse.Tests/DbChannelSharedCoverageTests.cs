using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Moq;
using Npgsql;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The background loops, dispatch-scope collection and cleanup fallbacks of
/// <c>src/Channels/Shared/DbChannelShared.cs</c>. That file is <c>&lt;Compile Include&gt;</c>-linked into
/// the MongoDB, PostgreSQL and SQL Server channel packages, so the base class compiles separately
/// into each assembly and a path exercised through only one provider stays uncovered in the other
/// two. Every fact here therefore runs against all three.
/// <para>
/// <b>Why this harness drives internals instead of the public conformance surface</b> (evaluated
/// against a complexity-review proposal to replace it with <c>ChannelConformanceSuite</c> facts):
/// the conformance suite's unit-side derivation is the in-memory channel, which never executes
/// this file — its facts cover <c>DbChannelShared</c> only in the container-backed integration
/// run. Every fact here exists for a branch the public surface cannot reach deterministically:
/// </para>
/// <list type="bullet">
/// <item><description><b>Store-fault seams</b> (loop retry/teardown, cleanup double-fault): a
/// failing store rejects <c>CreateResponseWaiter</c> at registration, so a live subscription can
/// only exist alongside a faulted store by fabricating it — and integration containers never
/// fail on cue.</description></item>
/// <item><description><b>Race states</b> (all-dropped local subscriptions, the delivery
/// watermark's skew window): reachable publicly only by losing a race; fabricated state makes the
/// branch deterministic. The watermark BEHAVIOR is separately pinned on real containers by the
/// conformance correlation-id-reuse facts.</description></item>
/// <item><description><b>Signal-scope logic</b> (full sweep outranks targeted ids): observable
/// publicly only through sweep timing side effects, which no assertion can await
/// deterministically.</description></item>
/// <item><description><b>Activity tagging</b> is already public-surface (no reflection) and lives
/// here only for the ×3-assembly matrix.</description></item>
/// </list>
/// <para>
/// The ×3 provider matrix itself is also load-bearing, not ceremony: this shared source compiles
/// separately into the MongoDB, PostgreSQL and SQL Server assemblies, and coverage is counted
/// per assembly — running a seam against one provider leaves the same lines uncovered in the
/// other two, which the integration suite cannot make up (real containers never fault on cue).
/// Collapsing the matrix to one provider trades those covered lines away in the two dropped
/// assemblies for nothing in return.
/// </para>
/// </summary>
public sealed class DbChannelSharedCoverageTests
{
    /// <summary>
    /// Both background loops swallow-and-retry a store failure rather than dying, and the teardown
    /// absorbs the cancellation that tears them out of their retry delay.
    /// </summary>
    /// <remarks>
    /// The poll interval is long and the dispatcher pre-signalled so the dispatch loop is parked in
    /// the retry <c>Task.Delay</c> of its catch block when disposal cancels — that is the path where
    /// the cancellation escapes the loop and has to be absorbed by <c>DisposeAsync</c>.
    /// </remarks>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task BackgroundLoops_RetryStoreFailuresAndAbsorbTeardownCancellation(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: true, pollInterval: TimeSpan.FromSeconds(30));
        var logger = harness.Logger;

        // A live registration is what gives the heartbeat something to write and the sweep a
        // correlation id to load messages for; without one both loops idle without touching the store.
        harness.AddSubscription("corr", harness.Subscription("corr").Instance);
        harness.Invoke("SignalDispatcher", "corr");
        harness.Invoke("EnsureListenerStarted");

        await logger.WaitForAsync("subscriber heartbeat failed");
        await logger.WaitForAsync("response dispatch loop failed");

        // Parked in the catch-block retry delay: disposal cancels it, and the teardown must absorb
        // the resulting OperationCanceledException instead of surfacing it to the host.
        await harness.Channel.DisposeAsync();
    }

    /// <summary>
    /// The dispatch loop keeps sweeping after a failure: it serves out its retry delay and comes
    /// back round rather than exiting on the first fault.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task DispatchLoop_ResumesSweepingAfterItsRetryDelay(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: true, pollInterval: TimeSpan.FromMilliseconds(20));
        harness.AddSubscription("corr", harness.Subscription("corr").Instance);
        harness.Invoke("EnsureListenerStarted");

        // Two failures means the first retry delay ran to completion and the loop iterated.
        await harness.Logger.WaitForAsync("response dispatch loop failed", occurrences: 2);
    }

    // Not covered here: the catch around the executor retirement in CleanupOnceAsync. It runs on a
    // detached Task.Run, touches no store, and the only fault injection point — the registry field —
    // is also used by RemoveSubscription earlier in the same finally block, so breaking it faults
    // before reaching the catch. There is no reachable trigger for it from a test.

    /// <summary>
    /// A full-sweep signal (null correlation id) beats any targeted ids queued alongside it, and an
    /// empty batch also degrades to the full sweep.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task CollectDispatchScope_FullSweepSignalOutranksTargetedIds(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: false, pollInterval: TimeSpan.FromSeconds(30));

        // Targeted only: scan exactly the signalled ids.
        harness.Invoke("SignalDispatcher", "corr-a");
        harness.Invoke("SignalDispatcher", "corr-b");
        var targeted = Assert.IsType<HashSet<string>>(await harness.CollectDispatchScopeAsync());
        Assert.Equal(["corr-a", "corr-b"], targeted.OrderBy(id => id, StringComparer.Ordinal));

        // A null signal mixed in means "scan everything" — the targeted ids are discarded.
        harness.Invoke("SignalDispatcher", "corr-a");
        harness.Invoke("SignalDispatcher", (string?)null);
        Assert.Null(await harness.CollectDispatchScopeAsync());
    }

    /// <summary>
    /// FullSweepInterval gates the timer-driven safety-net sweep (which costs one store query per
    /// subscribed correlation id per tick): the first poll tick sweeps and stamps, a second tick
    /// inside the interval scans NOTHING (a non-null empty scope), and a targeted signal still
    /// flows during the suppression window.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    public async Task CollectDispatchScope_SuppressesPollSweepsInsideFullSweepInterval(Provider provider)
    {
        await using var harness = Harness.Create(
            provider, failing: false, pollInterval: TimeSpan.FromMilliseconds(20), fullSweepInterval: TimeSpan.FromMinutes(10));

        // First delay-win: no sweep has run yet — full sweep (null scope), stamping the interval.
        Assert.Null(await harness.CollectDispatchScopeAsync());

        // Second delay-win, milliseconds later: the 10-minute sweep is not due — scan nothing.
        var suppressed = Assert.IsType<HashSet<string>>(await harness.CollectDispatchScopeAsync());
        Assert.Empty(suppressed);

        // Signalled scans are unaffected by the gate: a targeted id still dispatches on its signal.
        harness.Invoke("SignalDispatcher", "corr-signal");
        var targeted = Assert.IsType<HashSet<string>>(await harness.CollectDispatchScopeAsync());
        Assert.Single(targeted, "corr-signal");
    }

    /// <summary>
    /// MongoDB honours the throttle only while change streams carry delivery: with them on (and
    /// not reported unsupported) a not-yet-due tick scans nothing, exactly as the relational
    /// providers above.
    /// </summary>
    [Fact]
    public async Task CollectDispatchScope_MongoDbWithChangeStreams_SuppressesPollSweepsInsideFullSweepInterval()
    {
        await using var harness = Harness.Create(
            Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromMilliseconds(20), fullSweepInterval: TimeSpan.FromMinutes(10), useChangeStreams: true);

        Assert.Null(await harness.CollectDispatchScopeAsync());
        var suppressed = Assert.IsType<HashSet<string>>(await harness.CollectDispatchScopeAsync());
        Assert.Empty(suppressed);
    }

    /// <summary>
    /// With change streams off, MongoDB has no push wake: the sweep IS cross-process delivery,
    /// and the 5s default throttle equalled DeliveryConfirmationTimeout — the publisher gave up,
    /// claimed the message for recovery and fired the lost-subscriber callback a beat before the
    /// healthy waiter's throttled sweep found it. The option is ignored in that mode: every poll
    /// tick is a full sweep, as the UseChangeStreams doc promises.
    /// </summary>
    [Fact]
    public async Task CollectDispatchScope_MongoDbWithoutChangeStreams_IgnoresFullSweepInterval()
    {
        await using var harness = Harness.Create(
            Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromMilliseconds(20), fullSweepInterval: TimeSpan.FromMinutes(10));

        Assert.Null(await harness.CollectDispatchScopeAsync());
        Assert.Null(await harness.CollectDispatchScopeAsync());
    }

    /// <summary>Null (the default) keeps the pre-option behavior: every poll tick is a full sweep.</summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task CollectDispatchScope_NullFullSweepInterval_SweepsOnEveryPollTick(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: false, pollInterval: TimeSpan.FromMilliseconds(20));

        Assert.Null(await harness.CollectDispatchScopeAsync());
        Assert.Null(await harness.CollectDispatchScopeAsync());
    }

    /// <summary>
    /// A targeted dispatch scan touches exactly the signaled correlation ids — a publish signals
    /// one id, so the scan must cost O(scope), not O(live waiters). Ids outside the scope, ids
    /// with no live subscription, and the not-yet-due poll tick's empty scope never reach the
    /// store; a signaled id with a live waiter and the null-scope full sweep still do. The
    /// failing store makes "reached the store" observable as the arranged fault.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task DispatchPendingMessages_ScansOnlyTheSignaledScope(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: true, pollInterval: TimeSpan.FromSeconds(30));
        harness.AddSubscription("corr-live", harness.Subscription("corr-live").Instance);

        // Out-of-scope subscription, unknown signaled id, and the empty (suppressed-tick) scope:
        // no store read, so the arranged fault never surfaces.
        await harness.InvokeAsync(
            "DispatchPendingMessagesAsync", new HashSet<string>(StringComparer.Ordinal) { "corr-other" }, CancellationToken.None);
        await harness.InvokeAsync(
            "DispatchPendingMessagesAsync", new HashSet<string>(StringComparer.Ordinal), CancellationToken.None);

        // The signaled id has a live waiter: the scan loads its messages and hits the fault.
        await Assert.ThrowsAnyAsync<Exception>(() => harness.InvokeAsync(
            "DispatchPendingMessagesAsync", new HashSet<string>(StringComparer.Ordinal) { "corr-live" }, CancellationToken.None));

        // The full sweep (null scope) still scans every subscribed id.
        await Assert.ThrowsAnyAsync<Exception>(() => harness.InvokeAsync(
            "DispatchPendingMessagesAsync", null, CancellationToken.None));
    }

    /// <summary>
    /// The heartbeat round's compensation: a registration dropped (or removed) after the round's
    /// snapshot was taken gets a compensating subscriber delete — the round's upsert may have
    /// resurrected the row the cleanup had just deleted, which would count a phantom live waiter
    /// and suppress lost-subscriber recovery until the heartbeat timeout. Live registrations are
    /// skipped, and a failing store delete stays best-effort (both asserted through the logs; the
    /// relational harnesses' closed-port stores make the delete itself throw, which also covers
    /// the catch).
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task Heartbeat_CompensatesRegistrationsDroppedDuringTheRound(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: true, pollInterval: TimeSpan.FromSeconds(30));

        var live = harness.Subscription("corr-live").Instance;
        var droppedDuringRound = harness.Subscription("corr-dropped").Instance;
        harness.AddSubscription("corr-live", live);
        harness.AddSubscription("corr-dropped", droppedDuringRound);

        // The round snapshotted all three pairs; afterwards one subscription dropped and one was
        // removed from the map entirely (its cleanup finished).
        var heartbeaten = new List<(string CorrelationId, Guid RegistrationId)>
        {
            ("corr-live", SubscriptionId(live)),
            ("corr-dropped", SubscriptionId(droppedDuringRound)),
            ("corr-ghost", Guid.NewGuid())
        };
        SetField(droppedDuringRound, "_dropped", true);

        await harness.InvokeAsync("DeleteRegistrationsDroppedDuringHeartbeatAsync", heartbeaten, CancellationToken.None);

        Assert.Contains(harness.Logger.Messages, m => m.Contains("dropped while a heartbeat round was in flight") && m.Contains("corr-dropped"));
        Assert.Contains(harness.Logger.Messages, m => m.Contains("dropped while a heartbeat round was in flight") && m.Contains("corr-ghost"));
        Assert.DoesNotContain(harness.Logger.Messages, m => m.Contains("corr-live"));
    }

    /// <summary>
    /// The compensation must run even when the heartbeat round FAILS: SQL Server commits
    /// per-batch, MongoDB bulk-writes unordered, and any provider can fail after some upserts
    /// landed — so a registration dropped mid-round may already be resurrected when the round
    /// throws. Deterministic on the Mongo harness (the bulk-write mock "lands" the upsert, drops
    /// the registration, then fails the round); the loop's failure-path ordering is shared
    /// source, and the compensation logic itself is pinned per-assembly by the ×3 test above.
    /// </summary>
    /// <summary>
    /// Regression (review fix): <c>DropLocalSubscriptionsAsync</c> must retire each dropped
    /// subscription's executor-registry registration, exactly as <c>RemoveSubscription</c> does.
    /// A leftover refcount defeated the tombstone its own <c>RemoveAsync</c> sets, so a later
    /// delivery for the correlation id recreated an executor nothing ever retired — one leaked
    /// drain loop per dropped id. Mongo harness only: its store is fully mocked, so the drop's
    /// subscriber delete succeeds; the closed-port SQL harnesses fault that call before the
    /// retire logic runs (the logic itself is shared source, identical in all three assemblies).
    /// </summary>
    [Fact]
    public async Task DropLocalSubscriptions_RetiresExecutorRegistrations()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        var subscription = harness.Subscription("corr-drop").Instance;
        harness.AddSubscription("corr-drop", subscription);
        Assert.Single(harness.ExecutorRegistrations);

        await harness.InvokeAsync("DropLocalSubscriptionsAsync", CancellationToken.None);

        Assert.Empty(harness.ExecutorRegistrations);
        Assert.Empty(harness.Subscriptions);
    }

    [Fact]
    public async Task Heartbeat_CompensatesEvenWhenTheRoundFails()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: true, pollInterval: TimeSpan.FromSeconds(30));
        var subscription = harness.Subscription("corr-mid-round").Instance;
        harness.AddSubscription("corr-mid-round", subscription);

        harness.MongoSubscribers!
            .Setup(c => c.BulkWriteAsync(
                It.IsAny<IEnumerable<WriteModel<MongoChannelSubscriberDocument>>>(),
                It.IsAny<BulkWriteOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback(() => SetField(subscription, "_dropped", true))
            .ThrowsAsync(new MongoException("partial bulk failure"));

        harness.Invoke("EnsureListenerStarted");

        await harness.Logger.WaitForAsync("subscriber heartbeat failed");
        await harness.Logger.WaitForAsync("dropped while a heartbeat round was in flight");
    }

    private static Guid SubscriptionId(object subscription)
        => (Guid)subscription.GetType().GetProperty("Id", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!
            .GetValue(subscription)!;

    /// <summary>
    /// The same-process fast path stops before claiming the executor when every local subscription
    /// for the correlation id has already been dropped.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task LocalDispatch_SkipsCorrelationWhoseSubscriptionsAreAllDropped(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: true, pollInterval: TimeSpan.FromSeconds(30));
        var subscription = harness.Subscription("corr").Instance;
        SetField(subscription, "_dropped", true);
        harness.AddSubscription("corr", subscription);

        // A failing store proves the early return: reaching the executor would hit it and throw.
        await harness.InvokeAsync("TryDispatchLocalSubscribersAsync", harness.Message("null"), CancellationToken.None);

        // An unknown correlation id takes the other early return, before the group lookup succeeds.
        await harness.InvokeAsync("TryDispatchLocalSubscribersAsync", harness.Message("null", "unknown"), CancellationToken.None);
    }

    /// <summary>
    /// The delivery watermark excludes a message acknowledged at or before the subscription started
    /// — the guard that stops a correlation id reused inside the 1s skew window from replaying its
    /// predecessor's response — while still admitting one acknowledged after it.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task DeliveryWatermark_ExcludesMessagesAckedBeforeTheSubscriptionStarted(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: true, pollInterval: TimeSpan.FromSeconds(30));
        var startedAt = DateTimeOffset.UtcNow;
        var subscription = harness.Subscription("corr", startedAt).Instance;

        var subscriptions = harness.SubscriptionArray(subscription);
        var dispatch = "DispatchMessageToSubscribersAsync";

        // Acked before this subscription existed: already consumed, so there is nothing to target
        // and the failing store is never reached.
        await harness.InvokeAsync(dispatch, harness.Message("null", ackedAtUtc: startedAt.AddSeconds(-5)), subscriptions, CancellationToken.None);

        // Created before the watermark's 1s tolerance: likewise out of range.
        await harness.InvokeAsync(dispatch, harness.Message("null", createdAtUtc: startedAt.AddMinutes(-5)), subscriptions, CancellationToken.None);

        // In range and acked after the start: a real target, so the claim runs and the store throws.
        await Assert.ThrowsAnyAsync<Exception>(() => harness.InvokeAsync(
            dispatch, harness.Message("null", ackedAtUtc: startedAt.AddSeconds(5)), subscriptions, CancellationToken.None));
    }

    /// <summary>
    /// The same-tick photo-finish that timestamps cannot arbitrate: <c>acked_at</c> equal to the
    /// subscription's start is EITHER history (a predecessor consumed it just before this waiter
    /// registered) OR a cross-process fan-out delivery to a group including this waiter. The
    /// monotonic ack sequence breaks exactly that tie — and ONLY that tie: sequence values are
    /// drawn before claims become visible, so a claim can draw early, stall past a registration,
    /// and land in a later tick — where the truthful timestamp must win (ranking the sequence
    /// above timestamps excluded precisely that delivery as history). Excluded messages never
    /// touch the store; included ones run the claim and the failing store throws.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task DeliveryWatermark_SameTickTie_IsResolvedByTheAckSequence_AndTimestampsStayPrimary(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: true, pollInterval: TimeSpan.FromSeconds(30));
        var startedAt = DateTimeOffset.UtcNow;
        var subscription = harness.Subscription("corr", startedAt, startedSeq: 100).Instance;
        var subscriptions = harness.SubscriptionArray(subscription);
        var dispatch = "DispatchMessageToSubscribersAsync";

        // Identical timestamps; the claim drew BEFORE this waiter registered → history, excluded.
        await harness.InvokeAsync(
            dispatch, harness.Message("null", ackedAtUtc: startedAt, ackedSeq: 99), subscriptions, CancellationToken.None);

        // Identical timestamps; the claim drew AFTER this waiter registered → fan-out, included.
        await Assert.ThrowsAnyAsync<Exception>(() => harness.InvokeAsync(
            dispatch, harness.Message("null", ackedAtUtc: startedAt, ackedSeq: 101), subscriptions, CancellationToken.None));

        // Identical timestamps, no sequence stamp (row acked by a pre-sequence build): the legacy
        // conservative tie resolution — history, excluded.
        await harness.InvokeAsync(
            dispatch, harness.Message("null", ackedAtUtc: startedAt), subscriptions, CancellationToken.None);

        // A claim that DREW before this waiter registered (99 < 100) but STALLED and landed in a
        // strictly later tick is a delivery this waiter is part of — the truthful timestamp wins
        // over the stale draw order. Ranking the sequence first wrongly excluded exactly this.
        await Assert.ThrowsAnyAsync<Exception>(() => harness.InvokeAsync(
            dispatch, harness.Message("null", ackedAtUtc: startedAt.AddSeconds(5), ackedSeq: 99), subscriptions, CancellationToken.None));

        // And a claim acked in a strictly EARLIER tick is history regardless of its sequence
        // value — a late-stamped high draw must not resurrect a consumed response.
        await harness.InvokeAsync(
            dispatch, harness.Message("null", ackedAtUtc: startedAt.AddSeconds(-5), ackedSeq: 101), subscriptions, CancellationToken.None);
    }

    /// <summary>
    /// Regression (round 29): the waiter timeout faulted the TaskCompletionSource BEFORE draining
    /// the per-correlation executor. A delivery already inside that executor may hold a message the
    /// claim ACKED — the publisher was told "delivered" and the watermark excludes it from every
    /// later sweep, so it exists nowhere else — and the timeout beat it, reporting a consumed
    /// response as a timeout. The response was then unrecoverable: gone from the store, and never
    /// handed to the caller. The terminal exception is now applied after the drain, where TrySet
    /// loses to the delivery that won.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task WaiterTimeout_DrainsTheExecutorBeforeFaulting_SoAnInFlightDeliveryStillWins(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: true, pollInterval: TimeSpan.FromSeconds(30));
        var (subscription, completion) = harness.Subscription("corr", cleanupStarted: false);
        harness.AddSubscription("corr", subscription);

        // Occupy the executor the way an in-flight delivery mid-Until-predicate does: the drain's
        // marker item cannot run until this one returns.
        var inFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseInFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Assert.True(await harness.Executors.EnqueueAsync(harness.ChannelName("corr"), async () =>
        {
            inFlight.TrySetResult();
            await releaseInFlight.Task;
        }));
        await inFlight.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // The timeout fires while that delivery is still in flight.
        var timedOut = ((ValueTask)subscription.GetType()
            .GetMethod("DrainThenCleanupAsync")!
            .Invoke(subscription, [true, new TimeoutException("timed out")])!).AsTask();

        // Red on the old code: it faulted here, before the drain could prove anything.
        await Task.Delay(100);
        Assert.False(completion.Task.IsCompleted, "the waiter was settled before the in-flight delivery had drained");

        // The delivery finishes and hands over the response it was holding...
        completion.TrySetResult(new OperationResult { Status = OperationStatus.Completed });
        releaseInFlight.TrySetResult();
        await timedOut.WaitAsync(TimeSpan.FromSeconds(10));

        // ...and the timeout does not overwrite it.
        Assert.Equal(OperationStatus.Completed, (await completion.Task).Status);
    }

    /// <summary>
    /// The counterpart: with nothing in flight the drain completes immediately and the timeout is
    /// still what the caller sees.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task WaiterTimeout_WithNothingInFlight_StillFaultsWithTheTimeout(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: true, pollInterval: TimeSpan.FromSeconds(30));
        var (subscription, completion) = harness.Subscription("corr", cleanupStarted: false);
        harness.AddSubscription("corr", subscription);

        await ((ValueTask)subscription.GetType()
            .GetMethod("DrainThenCleanupAsync")!
            .Invoke(subscription, [true, new TimeoutException("timed out")])!).AsTask()
            .WaitAsync(TimeSpan.FromSeconds(10));

        await Assert.ThrowsAsync<TimeoutException>(() => completion.Task);
    }

    /// <summary>
    /// Regression (round 31): round 30's shared publish-with-recovery helper erased the
    /// publisher's declared <c>T</c> to <c>object</c>, so the three DB channels serialized the
    /// recovery payload by its RUNTIME type while the durable envelope (and Redis/NATS/in-memory
    /// recovery) used the declared contract — leaking derived-only members into the recovery wire
    /// form and breaking "in-process and broker deliveries of the same response classify
    /// identically". The recovery wire form must be the declared-type serialization.
    /// </summary>
    [Fact]
    public async Task TypedPublish_RecoveryPayload_UsesTheDeclaredTypeWireForm()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        var correlationId = $"declared-{Guid.NewGuid():N}";
        harness.RecoveryState
            .Setup(store => store.GetAllAsync(correlationId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(
            [
                new RecoveryState
                {
                    CorrelationId = correlationId,
                    RegistrationId = Guid.NewGuid(),
                    RegisteredAtUtc = DateTime.UtcNow,
                    PayloadTypeFullName = typeof(SlicedDerivedPayload).FullName
                }
            ]);

        SlicedBasePayload.LastRecovered = null;
        var publisher = (IAsyncResponsePublisher)harness.Channel;
        await publisher.SetResponse<SlicedBasePayload>(
            new SlicedDerivedPayload { Message = "m", Extra = "x" },
            correlationId);

        var recovered = Assert.IsType<SlicedDerivedPayload>(SlicedBasePayload.LastRecovered);
        Assert.Equal("m", recovered.Message);
        // The declared-type wire form carries only the contract's members: a runtime-only
        // property must NOT survive the round trip, exactly as a live envelope would slice it.
        Assert.Null(recovered.Extra);
    }

    public class SlicedBasePayload : IAsyncResponsePayload
    {
        public static volatile SlicedBasePayload? LastRecovered;

        public string? Message { get; set; }

        public RecoveryAction OnRecovery()
        {
            LastRecovered = this;
            return RecoveryAction.Fail;
        }
    }

    public sealed class SlicedDerivedPayload : SlicedBasePayload
    {
        public string? Extra { get; set; }
    }

    /// <summary>
    /// Cleanup is best-effort on both network calls: a recovery-state delete and a subscriber delete
    /// that both fail are logged, and the purely local teardown still runs.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task Cleanup_LogsAndContinuesWhenBothStoreDeletesFail(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: true, pollInterval: TimeSpan.FromSeconds(30));
        harness.RecoveryState
            .Setup(store => store.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("recovery store offline"));

        var subscription = harness.Subscription("corr", cleanupStarted: false).Instance;
        harness.AddSubscription("corr", subscription);

        await (ValueTask)subscription.GetType()
            .GetMethod("CleanupOnceAsync")!
            .Invoke(subscription, [true])!;

        // Both best-effort deletes failed and were logged rather than thrown.
        Assert.Equal(2, harness.Logger.Messages.Count(
            message => message.StartsWith("Failed to delete", StringComparison.Ordinal)));
        // Local teardown ran regardless of the two failures: the subscription is deregistered.
        Assert.Empty(harness.Subscriptions);
    }

    /// <summary>
    /// Every publish path opens an activity and tags it. Nothing else in the unit suite drives these
    /// three entry points with a listener attached, so without this the tagging is only ever compiled,
    /// never run — the store failure afterwards is incidental, the tags precede it.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task PublishPaths_TagTheirActivityWhenAListenerIsAttached(Provider provider)
    {
        using var activities = new AsyncResponseActivityCollector();
        await using var harness = Harness.Create(provider, failing: true, pollInterval: TimeSpan.FromSeconds(30));
        var publisher = (IRawAsyncResponsePublisher)harness.Channel;

        // Whether the store failure surfaces or the publish falls through to the lost-subscriber
        // path differs per provider mock; the tagging under test happens before either.
        await Ignoring(() => ((IAsyncResponsePublisher)harness.Channel)
            .SetResponse(new OperationResult { Status = OperationStatus.Completed }, "corr"));
        await Ignoring(() => publisher.SetRawResponseJson("""{"Status":2}""", "corr"));
        await Ignoring(() => ((IAsyncResponsePublisher)harness.Channel)
            .SetException(new InvalidOperationException("boom"), "corr"));

        var tag = provider switch
        {
            Provider.SqlServer => "sqlserver",
            Provider.PostgreSql => "postgresql",
            _ => "mongodb"
        };
        foreach (var name in new[] { "asyncresponse.set_response", "asyncresponse.ingress.raw_response", "asyncresponse.set_exception" })
            activities.Single(name, "asyncresponse.channel", tag);
    }

    /// <summary>
    /// Regression (round 33): the same-process fast path built its dispatch message with a
    /// fabricated <c>AckedAtUtc = null</c>, so a publish RETRY — the same message id landing as an
    /// idempotent duplicate after another process had already claimed and acked the first attempt
    /// — bypassed <c>IsWithinWatermark</c>'s acked-history exclusion (<c>AckedAtUtc is null</c>
    /// admits) and replayed the consumed response to a waiter registered AFTER the ack; the
    /// delivery claim gates only on <c>recovery_claimed</c>, so it won again. The store now returns
    /// the ORIGINAL row, settlement columns included, and the fast path dispatches exactly that.
    /// Mongo harness only: its store is the real <c>MongoDbChannelStore</c> over a mocked
    /// collection, so the duplicate's acked document is arranged directly on the upsert (the
    /// relational harnesses' closed-port stores cannot answer an insert); the fast path itself is
    /// shared source. Pre-fix: the delivery claim ran and the late waiter completed with the stale
    /// response.
    /// </summary>
    [Fact]
    public async Task FastPath_PublishRetryOfAnAckedMessage_DoesNotReplayItToAWaiterRegisteredAfterTheAck()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        var startedAt = DateTimeOffset.UtcNow;
        var (late, completion) = harness.Subscription("corr", startedAt, startedSeq: 100);
        harness.AddSubscription("corr", late);

        // The idempotent duplicate: the upsert returns the ORIGINAL document — created inside the
        // watermark's 1s tolerance, but claimed and acked BEFORE this waiter registered.
        var messageId = Guid.NewGuid();
        harness.MongoMessages!
            .Setup(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.Is<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(options => options != null && options.IsUpsert),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MongoChannelMessageDocument
            {
                Id = messageId,
                CorrelationId = "corr",
                EnvelopeJson = StaleEnvelope,
                CreatedAtUtc = startedAt.AddMilliseconds(-500).UtcDateTime,
                ExpiresAtUtc = startedAt.AddMinutes(5).UtcDateTime,
                AckedAtUtc = startedAt.AddMilliseconds(-200).UtcDateTime,
                AckedSeq = 7
            });
        // The delivery claim (no upsert) would win again: it gates only on recovery_claimed.
        harness.MongoMessages
            .Setup(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.Is<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(options => options == null || !options.IsUpsert),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(new MongoChannelMessageDocument
            {
                Id = messageId,
                CorrelationId = "corr",
                EnvelopeJson = StaleEnvelope,
                CreatedAtUtc = startedAt.AddMilliseconds(-500).UtcDateTime,
                ExpiresAtUtc = startedAt.AddMinutes(5).UtcDateTime,
                AckedAtUtc = startedAt.AddMilliseconds(-200).UtcDateTime,
                AckedSeq = 7
            });

        // The retry lands through the publish path with the SAME message id...
        await harness.InvokeAsync("PublishMessageAsync", messageId, "corr", StaleEnvelope, CancellationToken.None);
        // ...whose fast path dispatches on the per-correlation executor; a marker queued behind it
        // proves that dispatch ran to completion before anything is asserted.
        var dispatched = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Assert.True(await harness.Executors.EnqueueAsync(harness.ChannelName("corr"), () =>
        {
            dispatched.TrySetResult();
            return Task.CompletedTask;
        }));
        await dispatched.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.False(completion.Task.IsCompleted, "the consumed response was replayed to a waiter registered after its ack");
        harness.MongoMessages.Verify(
            collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.Is<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(options => options == null || !options.IsUpsert),
                It.IsAny<CancellationToken>()),
            Times.Never);
    }

    private const string StaleEnvelope =
        """{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"stale"},"ExceptionMessage":null,"ExceptionStackTrace":null}""";

    // ---------------------------------------------------------------------------------------
    // Harness
    // ---------------------------------------------------------------------------------------

    public enum Provider
    {
        SqlServer,
        PostgreSql,
        MongoDb
    }

    /// <summary>
    /// One constructed channel per provider assembly plus the reflection needed to drive the shared
    /// base. "Failing" points the relational providers at a closed port and arms the Mongo mocks to
    /// throw, so every store call faults deterministically without a container.
    /// </summary>
    private sealed class Harness : IAsyncDisposable
    {
        private readonly Type _channelType;
        private readonly Func<string, string, DateTimeOffset?, DateTimeOffset?, long?, object> _message;
        private readonly NpgsqlDataSource? _dataSource;

        private Harness(
            object channel,
            Type channelType,
            Func<string, string, DateTimeOffset?, DateTimeOffset?, long?, object> message,
            CollectingLogger logger,
            Mock<IRecoveryStateStore> recoveryState,
            NpgsqlDataSource? dataSource)
        {
            Channel = (IAsyncDisposable)channel;
            _channelType = channelType;
            _message = message;
            Logger = logger;
            RecoveryState = recoveryState;
            _dataSource = dataSource;
        }

        public IAsyncDisposable Channel { get; }
        public CollectingLogger Logger { get; }
        public Mock<IRecoveryStateStore> RecoveryState { get; }

        /// <summary>Mongo harness only: the subscribers-collection mock, for heartbeat fault injection.</summary>
        public Mock<IMongoCollection<MongoChannelSubscriberDocument>>? MongoSubscribers { get; private set; }

        /// <summary>Mongo harness only: the messages-collection mock, for arranging what the store's upsert and claim return.</summary>
        public Mock<IMongoCollection<MongoChannelMessageDocument>>? MongoMessages { get; private set; }

        public static Harness Create(Provider provider, bool failing, TimeSpan pollInterval, TimeSpan? fullSweepInterval = null, bool useChangeStreams = false)
        {
            var logger = new CollectingLogger();
            var recoveryState = new Mock<IRecoveryStateStore>();
            recoveryState
                .Setup(store => store.TryDeleteAsync(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(true);
            recoveryState
                .Setup(store => store.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
                .Returns(Task.CompletedTask);
            recoveryState
                .Setup(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync([]);

            var scopeFactory = new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();
            var heartbeat = TimeSpan.FromMilliseconds(15);

            switch (provider)
            {
                case Provider.SqlServer:
                {
                    var options = Options.Create(new SqlServerAsyncResponseChannelOptions
                    {
                        // Port 1 is closed, so every command faults fast without a container.
                        ConnectionString = "Server=localhost,1;Database=unused;User Id=sa;Password=unused;Encrypt=False;Connect Timeout=1",
                        AutoCreateSchema = false,
                        ActivePollInterval = pollInterval,
                        IdlePollInterval = pollInterval,
                        FullSweepInterval = fullSweepInterval,
                        SubscriberHeartbeatInterval = heartbeat,
                        SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(5),
                        DeliveryConfirmationTimeout = TimeSpan.FromMilliseconds(2),
                        DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(1)
                    });
                    var channel = new SqlServerAsyncResponseChannel(
                        scopeFactory,
                        new SqlServerChannelSql(options),
                        recoveryState.Object,
                        options,
                        new AsyncResponseContextPropagation([]),
                        logger.For<SqlServerAsyncResponseChannel>());
                    return new Harness(
                        channel,
                        typeof(SqlServerAsyncResponseChannel),
                        (json, correlationId, created, acked, ackedSeq) => new SqlServerChannelMessage(
                            Guid.NewGuid(), correlationId, json, created ?? DateTimeOffset.UtcNow, acked, ackedSeq),
                        logger,
                        recoveryState,
                        dataSource: null);
                }

                case Provider.PostgreSql:
                {
                    var dataSource = NpgsqlDataSource.Create(
                        "Host=localhost;Port=1;Database=unused;Username=unused;Password=unused;Timeout=1;Pooling=false");
                    var options = Options.Create(new PostgreSqlAsyncResponseChannelOptions
                    {
                        AutoCreateSchema = false,
                        ListenerPollInterval = pollInterval,
                        FullSweepInterval = fullSweepInterval,
                        SubscriberHeartbeatInterval = heartbeat,
                        SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(5),
                        DeliveryConfirmationTimeout = TimeSpan.FromMilliseconds(2),
                        DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(1)
                    });
                    var channel = new PostgreSqlAsyncResponseChannel(
                        scopeFactory,
                        new PostgreSqlChannelSql(dataSource, options),
                        recoveryState.Object,
                        options,
                        new AsyncResponseContextPropagation([]),
                        logger.For<PostgreSqlAsyncResponseChannel>());
                    return new Harness(
                        channel,
                        typeof(PostgreSqlAsyncResponseChannel),
                        (json, correlationId, created, acked, ackedSeq) => new PostgreSqlChannelMessage(
                            Guid.NewGuid(), correlationId, json, created ?? DateTimeOffset.UtcNow, acked, ackedSeq),
                        logger,
                        recoveryState,
                        dataSource);
                }

                default:
                {
                    var options = Options.Create(new MongoDbAsyncResponseChannelOptions
                    {
                        AutoCreateIndexes = false,
                        // Keep EnsureCreatedAsync off the ledger: the ledger's collection is not
                        // mocked, and a store call routed through it would fault on the loose mock
                        // instead of the fault (or success) the test actually arranged. (The
                        // read-only index check on this path swallows the loose mocks' faults, so
                        // it cannot interfere.)
                        UseOwnershipLedger = false,
                        // Off by default: the mocked collection cannot serve a change stream, and
                        // no harness test starts the wake listener with it on. It only decides
                        // whether the FullSweepInterval throttle applies (see the Mongo-specific
                        // CollectDispatchScope tests).
                        UseChangeStreams = useChangeStreams,
                        ListenerPollInterval = pollInterval,
                        FullSweepInterval = fullSweepInterval,
                        SubscriberHeartbeatInterval = heartbeat,
                        SubscriberHeartbeatTimeout = TimeSpan.FromSeconds(5),
                        DeliveryConfirmationTimeout = TimeSpan.FromMilliseconds(2),
                        DeliveryConfirmationPollInterval = TimeSpan.FromMilliseconds(1)
                    });
                    var database = new Mock<IMongoDatabase>(MockBehavior.Loose);
                    var messages = new Mock<IMongoCollection<MongoChannelMessageDocument>>(MockBehavior.Loose).SelfPinning();
                    var subscribers = new Mock<IMongoCollection<MongoChannelSubscriberDocument>>(MockBehavior.Loose).SelfPinning();
                    database
                        .Setup(db => db.GetCollection<MongoChannelMessageDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
                        .Returns(messages.Object);
                    database
                        .Setup(db => db.GetCollection<MongoChannelSubscriberDocument>(It.IsAny<string>(), It.IsAny<MongoCollectionSettings>()))
                        .Returns(subscribers.Object);
                    database.WithLooseCollection<MongoRecoveryStateDocument>();
                    database.WithCounters();

                    if (failing)
                    {
                        // The Mongo equivalent of the closed port: the sweep's Find and the
                        // heartbeat's BulkWrite both fault.
                        messages
                            .Setup(collection => collection.FindAsync(
                                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                                It.IsAny<CancellationToken>()))
                            .ThrowsAsync(new InvalidOperationException("mongo offline"));
                        messages
                            .Setup(collection => collection.FindOneAndUpdateAsync(
                                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                                It.IsAny<CancellationToken>()))
                            .ThrowsAsync(new InvalidOperationException("mongo offline"));
                        subscribers
                            .Setup(collection => collection.BulkWriteAsync(
                                It.IsAny<IEnumerable<WriteModel<MongoChannelSubscriberDocument>>>(),
                                It.IsAny<BulkWriteOptions>(),
                                It.IsAny<CancellationToken>()))
                            .ThrowsAsync(new InvalidOperationException("mongo offline"));
                        subscribers
                            .Setup(collection => collection.DeleteOneAsync(
                                It.IsAny<FilterDefinition<MongoChannelSubscriberDocument>>(),
                                It.IsAny<CancellationToken>()))
                            .ThrowsAsync(new InvalidOperationException("mongo offline"));
                    }

                    var channel = new MongoDbAsyncResponseChannel(
                        scopeFactory,
                        new MongoDbChannelStore(database.Object, options),
                        recoveryState.Object,
                        options,
                        new AsyncResponseContextPropagation([]),
                        logger.For<MongoDbAsyncResponseChannel>());
                    var harness = new Harness(
                        channel,
                        typeof(MongoDbAsyncResponseChannel),
                        (json, correlationId, created, acked, ackedSeq) => new MongoDbChannelMessage(
                            Guid.NewGuid(), correlationId, json, created ?? DateTimeOffset.UtcNow, acked, ackedSeq),
                        logger,
                        recoveryState,
                        dataSource: null);
                    harness.MongoSubscribers = subscribers;
                    harness.MongoMessages = messages;
                    return harness;
                }
            }
        }

        public object Message(string json, string correlationId = "corr", DateTimeOffset? createdAtUtc = null, DateTimeOffset? ackedAtUtc = null, long? ackedSeq = null)
            => _message(json, correlationId, createdAtUtc, ackedAtUtc, ackedSeq);

        /// <summary>The shared base's per-provider subscription map, for asserting local teardown.</summary>
        public System.Collections.IDictionary Subscriptions
            => (System.Collections.IDictionary)Field("_subscriptions").GetValue(Channel)!;

        /// <summary>The serial-executor registry's per-channel registration refcounts, for asserting retirement.</summary>
        public System.Collections.IDictionary ExecutorRegistrations
        {
            get
            {
                var registry = Field("_executors").GetValue(Channel)!;
                return (System.Collections.IDictionary)registry.GetType()
                    .GetField("_registrations", BindingFlags.Instance | BindingFlags.NonPublic)!
                    .GetValue(registry)!;
            }
        }

        public (object Instance, TaskCompletionSource<OperationResult> Completion) Subscription(
            string correlationId,
            DateTimeOffset? startedAtUtc = null,
            bool cleanupStarted = true,
            long startedSeq = 0)
        {
            var completion = new TaskCompletionSource<OperationResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            var type = _channelType.BaseType!.GetNestedType("DbSubscription`1", BindingFlags.NonPublic)!
                .MakeGenericType(typeof(OperationResult));
            var instance = Activator.CreateInstance(
                type,
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                binder: null,
                [
                    Channel,
                    correlationId,
                    Guid.NewGuid(),
                    startedAtUtc ?? DateTimeOffset.UtcNow,
                    startedSeq,
                    (Func<OperationResult, ValueTask<bool>>)(_ => new ValueTask<bool>(true)),
                    completion,
                    null
                ],
                culture: null)!;
            if (cleanupStarted)
                SetField(instance, "_cleanupStarted", 1);
            return (instance, completion);
        }

        /// <summary>A one-element <c>IDbSubscription[]</c>, which only exists inside the provider assembly.</summary>
        public Array SubscriptionArray(object subscription)
        {
            var elementType = _channelType.BaseType!.GetNestedType("IDbSubscription", BindingFlags.NonPublic)!;
            var array = Array.CreateInstance(elementType, 1);
            array.SetValue(subscription, 0);
            return array;
        }

        public void AddSubscription(string correlationId, object subscription)
            => Method("AddSubscription").Invoke(Channel, [correlationId, subscription]);

        /// <summary>The per-correlation serial executor the disposal drain has to get through.</summary>
        public SerialExecutorRegistry Executors => (SerialExecutorRegistry)Field("_executors").GetValue(Channel)!;

        public string ChannelName(string correlationId) => (string)Method("ChannelName").Invoke(Channel, [correlationId])!;


        public void Invoke(string name, params object?[] arguments) => Method(name).Invoke(Channel, arguments);

        public Task InvokeAsync(string name, params object?[] arguments)
            => (Task)Method(name).Invoke(Channel, arguments)!;

        public async Task<object?> CollectDispatchScopeAsync()
            => await (dynamic)Method("CollectDispatchScopeAsync").Invoke(Channel, [CancellationToken.None])!;

        private MethodInfo Method(string name)
        {
            for (var type = _channelType; type is not null; type = type.BaseType)
            {
                var method = type.GetMethod(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                if (method is not null)
                    return method;
            }

            throw new MissingMethodException(_channelType.FullName, name);
        }

        private FieldInfo Field(string name)
        {
            for (var type = _channelType; type is not null; type = type.BaseType)
            {
                var field = type.GetField(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                if (field is not null)
                    return field;
            }

            throw new MissingFieldException(_channelType.FullName, name);
        }

        public async ValueTask DisposeAsync()
        {
            await Channel.DisposeAsync();
            if (_dataSource is not null)
                await _dataSource.DisposeAsync();
        }
    }

    private static async Task Ignoring(Func<Task> action)
    {
        try
        {
            await action();
        }
        catch (Exception)
        {
            // The store fault is the point of the failing harness, not of this assertion.
        }
    }

    private static void SetField(object target, string name, object value)
        => target.GetType().GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(target, value);

}
