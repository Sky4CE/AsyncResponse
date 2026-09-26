using AsyncResponse.Channels.MongoDB;
using AsyncResponse.Channels.PostgreSQL;
using AsyncResponse.Channels.SqlServer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
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
        // Wakes for ids with no local waiter are dropped at the source (fixpoint r1 S5#4).
        harness.AddWaiters("corr-a", "corr-b");

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
        harness.AddWaiters("corr-signal");
        // PostgreSQL throttles only while its LISTEN is up (fixpoint r1 S5#3); the full sweep the
        // (re)established listen requests is the first scope below.
        harness.MarkWakeListenerEstablished();

        // First pass: no sweep has run yet — full sweep (null scope), stamping the interval.
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
    /// Round 40 (HIGH): the poll deadline is absolute. Each pass used to arm a FRESH poll delay
    /// and only honoured it when the delay beat the signal channel, so a dispatcher that always
    /// had a targeted signal waiting never ran a full sweep — the only thing that delivers a
    /// response nobody signalled (every cross-process response on SQL Server; a missed or dropped
    /// notification on PostgreSQL and MongoDB). Proven red on ca58de0: zero full sweeps in every
    /// row below, however long the traffic ran.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer, null)]
    [InlineData(Provider.PostgreSql, null)]
    [InlineData(Provider.MongoDb, null)]
    [InlineData(Provider.SqlServer, 100)]
    [InlineData(Provider.PostgreSql, 100)]
    public async Task CollectDispatchScope_SustainedTargetedSignals_DoNotStarveTheFullSweep(Provider provider, int? fullSweepIntervalMs)
    {
        await using var harness = Harness.Create(
            provider,
            failing: false,
            pollInterval: TimeSpan.FromMilliseconds(40),
            fullSweepInterval: fullSweepIntervalMs is { } ms ? TimeSpan.FromMilliseconds(ms) : null);
        harness.AddWaiters("corr-busy");
        harness.MarkWakeListenerEstablished();

        var fullSweeps = 0;
        var targetedPasses = 0;
        var traffic = System.Diagnostics.Stopwatch.StartNew();
        while (traffic.Elapsed < TimeSpan.FromMilliseconds(800))
        {
            // Unrelated local traffic: a targeted signal is already queued before every pass.
            harness.Invoke("SignalDispatcher", "corr-busy");
            if (await harness.CollectDispatchScopeAsync() is null)
                fullSweeps++;
            else
                targetedPasses++;

            // The dispatch a real pass performs between two collects.
            await Task.Delay(5);
        }

        Assert.True(targetedPasses > 0, "The signals must still be served as targeted scans between sweeps.");
        Assert.True(
            fullSweeps >= 2,
            $"{fullSweeps} full sweep(s) in {traffic.ElapsedMilliseconds} ms of sustained targeted signals ({targetedPasses} targeted passes); the 40 ms poll and its sweep were starved.");
    }

    /// <summary>
    /// The signals a due sweep leaves queued are not lost: the sweep covered their correlation
    /// ids, and the next pass still serves them as a targeted scope.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task CollectDispatchScope_ADueSweepOutranksQueuedSignals_AndLeavesThemForTheNextPass(Provider provider)
    {
        // 250 ms: long enough that the third collect below lands inside the re-armed interval
        // even on a stalled runner, short enough to lapse on purpose.
        await using var harness = Harness.Create(provider, failing: false, pollInterval: TimeSpan.FromMilliseconds(250));
        harness.AddWaiters("corr-arm", "corr-late");

        // Arms the poll deadline (the loop starts lazily), then lets it lapse with a signal queued.
        harness.Invoke("SignalDispatcher", "corr-arm");
        Assert.IsType<HashSet<string>>(await harness.CollectDispatchScopeAsync());
        harness.Invoke("SignalDispatcher", "corr-late");
        await Task.Delay(TimeSpan.FromMilliseconds(350));

        Assert.Null(await harness.CollectDispatchScopeAsync());
        var targeted = Assert.IsType<HashSet<string>>(await harness.CollectDispatchScopeAsync());
        Assert.Single(targeted, "corr-late");
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
        // The throttle holds only while a stream is actually open (fixpoint r1 S5#3).
        harness.MarkWakeListenerEstablished();

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

        // The early return never reaches the registry: no executor is created for the id. (The
        // fast path's admission is non-blocking since fixpoint r1 S5#15, so the failing store can
        // no longer surface to this caller; the executor map is what shows the early return.)
        harness.Invoke("TryDispatchLocalSubscribers", harness.Message("null"));
        Assert.Empty(harness.LiveExecutors);

        // An unknown correlation id takes the other early return, before the group lookup succeeds.
        harness.Invoke("TryDispatchLocalSubscribers", harness.Message("null", "unknown"));
        Assert.Empty(harness.LiveExecutors);
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
    /// Round 35 (P1): the process-wide dispatch sweep visited correlation ids sequentially and
    /// AWAITED each id's serial-executor capacity. One waiter wedged in a slow completion predicate
    /// (its executor's single reader blocked on the first message) plus a backlog of NEW progress
    /// messages for that id filled the 1024-slot queue, and the sweep then parked on slot 1025
    /// without ever querying the next correlation id — every other waiter in the process stopped
    /// receiving. The sweep now admits work without waiting: at capacity it leaves the rest of that
    /// id's messages unclaimed in the store, schedules a rescan of that id alone, and moves on.
    /// Mongo harness only: its store is the real <c>MongoDbChannelStore</c> over a mocked
    /// collection, so the backlog can be arranged (the relational harnesses' closed-port stores
    /// cannot answer a query); the sweep itself is shared source, identical in all three
    /// assemblies. The targeted scope is a <c>HashSet</c> whose enumeration follows insertion order
    /// for a small, removal-free set, so the wedged id is visited first — the shape that hung.
    /// Pre-fix failure: the live waiter's delivery never arrives (the sweep is parked), and the
    /// sweep task never completes.
    /// </summary>
    [Fact]
    public async Task DispatchSweep_ASaturatedCorrelationExecutor_DoesNotBlockDeliveryToOtherCorrelations()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30), pendingMessageBatchSize: 4096);
        var startedAt = DateTimeOffset.UtcNow;

        // The wedged waiter: its dispatch hook never completes, so the first message parks its
        // executor's reader and every later message for the id queues behind it.
        var (blocked, _) = harness.Subscription("corr-blocked", startedAt);
        var wedged = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.SetProcessHook(blocked, () => wedged.Task);
        harness.AddSubscription("corr-blocked", blocked);

        // The unrelated waiter whose delivery must not wait behind it.
        var (live, _) = harness.Subscription("corr-live", startedAt);
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.SetProcessHook(live, () =>
        {
            delivered.TrySetResult();
            return Task.CompletedTask;
        });
        harness.AddSubscription("corr-live", live);

        // The store: 1100 distinct pending progress messages for the wedged id (more than the
        // executor's capacity), one for the live id — all unacked and inside both watermarks.
        MongoChannelMessageDocument Pending(string correlationId, int i) => new()
        {
            Id = Guid.NewGuid(),
            CorrelationId = correlationId,
            EnvelopeJson = StaleEnvelope,
            CreatedAtUtc = startedAt.AddMilliseconds(i).UtcDateTime,
            ExpiresAtUtc = startedAt.AddMinutes(5).UtcDateTime
        };
        var backlog = Enumerable.Range(0, ChannelSerialExecutor.DefaultCapacity + 76).Select(i => Pending("corr-blocked", i)).ToList();
        var single = new List<MongoChannelMessageDocument> { Pending("corr-live", 0) };
        harness.MongoMessages!
            .Setup(collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns((FilterDefinition<MongoChannelMessageDocument> filter, FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument> _, CancellationToken _) =>
                Task.FromResult(Cursor(CorrelationIdOf(filter) == "corr-blocked" ? backlog : single)));
        // Every delivery claim wins (the claim gates only on recovery_claimed).
        harness.MongoMessages
            .Setup(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Pending("claimed", 0));

        using var sweepCancellation = new CancellationTokenSource();
        var sweep = harness.InvokeAsync(
            "DispatchPendingMessagesAsync",
            new HashSet<string>(StringComparer.Ordinal) { "corr-blocked", "corr-live" },
            sweepCancellation.Token);

        try
        {
            await delivered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            await sweep.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            // On the pre-fix build the sweep is parked on the wedged id's 1025th enqueue; unpark
            // it so the harness can dispose.
            sweepCancellation.Cancel();
            wedged.TrySetResult();
        }
    }

    [Fact]
    public async Task DispatchSweep_IdleAndNewMessages_DoNotReloadConsumedHistory()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30), timeProvider: clock);
        var started = clock.GetUtcNow();
        var rows = Enumerable.Range(0, 200).Select(i => HistoryRow(started, i)).ToList();
        var delivered = 0;
        var subscription = harness.Subscription("corr", started).Instance;
        harness.SetProcessHook(subscription, () => { Interlocked.Increment(ref delivered); return Task.CompletedTask; });
        harness.AddSubscription("corr", subscription);
        var reads = new List<int>();
        ServeHistory(harness, rows, reads);
        await SweepAndDrainAsync(harness);
        Assert.Equal(200, delivered);
        reads.Clear();
        for (var i = 0; i < 20; i++) await SweepAndDrainAsync(harness);
        Assert.Equal(20, reads.Sum()); // Only the last database-clock tick, not 200 rows per poll.
        Assert.Equal(20, reads.Count);

        rows.Add(HistoryRow(started, 200));
        reads.Clear();
        await SweepAndDrainAsync(harness);
        Assert.Equal(2, reads.Sum()); // Last tick plus the new row.
        Assert.Equal(201, delivered);
        var sameTick = HistoryRow(started, 201);
        sameTick.Id = Guid.Empty;
        sameTick.CreatedAtUtc = rows[^1].CreatedAtUtc;
        rows.Add(sameTick);
        await SweepAndDrainAsync(harness);
        Assert.Equal(202, delivered); // No reconciliation delay for a lower random id in the last tick.
    }

    /// <summary>
    /// History reconciliation finds a commit that landed behind the forward cursor (outside the
    /// late-commit lookback window: the harness's 2 ms confirmation budget makes that window 1 ms)
    /// and one page at a time. Fixpoint r1 S5#10: the interval runs on the REAL monotonic clock —
    /// it paces discovery of other processes' commits, which land in real time — so the virtual
    /// clock below is never advanced. The pre-fix deadline was drawn from the injected clock: a
    /// virtual clock nobody advanced (or a wall clock stepped back) suspended reconciliation, and
    /// this fact then never delivered the late row.
    /// </summary>
    [Fact]
    public async Task DispatchSweep_ReconcilesLateAcknowledgedCommit_AndResetsForNewSubscriber()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30), pendingMessageBatchSize: 2, timeProvider: clock);
        // Held off first by an interval nothing reaches, then made due below by shortening it.
        harness.SetOption("HistoryReconciliationInterval", TimeSpan.FromHours(1));
        var started = clock.GetUtcNow();
        var rows = Enumerable.Range(1, 5).Select(i => HistoryRow(started, i)).ToList();
        var delivered = 0;
        var subscription = harness.Subscription("corr", started).Instance;
        harness.SetProcessHook(subscription, () => { Interlocked.Increment(ref delivered); return Task.CompletedTask; });
        harness.AddSubscription("corr", subscription);
        var reads = new List<int>();
        ServeHistory(harness, rows, reads);
        await SweepAndDrainAsync(harness);
        Assert.Equal(5, delivered);

        // An insert committed behind the cursor and was already ACKed by another process.
        var late = HistoryRow(started, 0);
        late.Id = Guid.Empty;
        late.CreatedAtUtc = rows[0].CreatedAtUtc; // Historical timestamp, lower id than its existing peer.
        rows.Add(late);
        await SweepAndDrainAsync(harness);
        Assert.Equal(5, delivered);

        // Due once REAL time past the interval has elapsed since the last reconciliation — the
        // virtual clock stays where it is.
        harness.SetOption("HistoryReconciliationInterval", TimeSpan.FromMilliseconds(1));
        await Task.Delay(TimeSpan.FromMilliseconds(20));
        reads.Clear();
        await SweepAndDrainAsync(harness);
        Assert.Equal(6, delivered);
        Assert.Equal(3, reads.Sum()); // Last tick plus one history page, not a scan to exhaustion.
        // The pass in progress pages on to the end; the next one is an hour away again.
        harness.SetOption("HistoryReconciliationInterval", TimeSpan.FromHours(1));
        await SweepAndDrainAsync(harness);
        await SweepAndDrainAsync(harness);
        Assert.Equal(6, delivered);
        reads.Clear();
        await SweepAndDrainAsync(harness);
        Assert.Equal(1, reads.Sum());

        // A second local fan-out registration has its OWN watermark and no seen history.
        var joined = 0;
        var other = harness.Subscription("corr", started, startedSeq: 0).Instance;
        harness.SetProcessHook(other, () => { Interlocked.Increment(ref joined); return Task.CompletedTask; });
        harness.AddSubscription("corr", other);
        await SweepAndDrainAsync(harness);
        Assert.Equal(6, joined);
        Assert.Equal(6, delivered);
    }

    [Fact]
    public async Task DispatchSweep_ClaimFailure_RewindsWithoutWaitingForReconciliation()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30), timeProvider: clock);
        var rows = new List<MongoChannelMessageDocument> { HistoryRow(clock.GetUtcNow(), 1) };
        var delivered = 0;
        var subscription = harness.Subscription("corr", clock.GetUtcNow()).Instance;
        harness.SetProcessHook(subscription, () => { Interlocked.Increment(ref delivered); return Task.CompletedTask; });
        harness.AddSubscription("corr", subscription);
        ServeHistory(harness, rows, []);
        harness.MongoMessages!.SetupSequence(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("claim unavailable"))
            .ReturnsAsync(rows[0]);
        await SweepAndDrainAsync(harness);
        Assert.Equal(0, delivered);
        await SweepAndDrainAsync(harness);
        Assert.Equal(1, delivered);
    }

    [Fact]
    public async Task DispatchSweep_OneTimestampAcrossManyPages_ContinuesPastThePerPassBudget()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30), timeProvider: clock);
        var rows = Enumerable.Range(0, 1100).Select(i => HistoryRow(clock.GetUtcNow(), i)).ToList();
        foreach (var row in rows) row.CreatedAtUtc = clock.GetUtcNow().UtcDateTime;
        var delivered = 0;
        var subscription = harness.Subscription("corr", clock.GetUtcNow()).Instance;
        harness.SetProcessHook(subscription, () => { Interlocked.Increment(ref delivered); return Task.CompletedTask; });
        harness.AddSubscription("corr", subscription);
        var reads = new List<int>();
        ServeHistory(harness, rows, reads);
        await SweepAndDrainAsync(harness);
        Assert.InRange(delivered, 1, 1024);
        Assert.Equal(16, reads.Count);
        await SweepAndDrainAsync(harness);
        Assert.Equal(1100, delivered);
    }

    [Fact]
    public async Task DispatchSweep_PrunedTail_DoesNotWalkBackwardThroughHistory()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30), timeProvider: clock);
        var rows = Enumerable.Range(1, 5).Select(i => HistoryRow(clock.GetUtcNow(), i)).ToList();
        var subscription = harness.Subscription("corr", clock.GetUtcNow()).Instance;
        harness.SetProcessHook(subscription, () => Task.CompletedTask);
        harness.AddSubscription("corr", subscription);
        var reads = new List<int>();
        ServeHistory(harness, rows, reads);
        await SweepAndDrainAsync(harness);
        rows.RemoveAt(rows.Count - 1);
        reads.Clear();
        for (var i = 0; i < 20; i++) await SweepAndDrainAsync(harness);
        Assert.Equal(0, reads.Sum());
    }

    // Review 2026-09-21 F3. The delivery-confirmation budget is an interval, and it was a wall-clock
    // deadline (GetUtcNow() + timeout). A system clock stepped FORWARD while a publish waited — an
    // NTP correction, a resumed or migrated VM — made the remaining budget negative before the
    // first wait: the publisher skipped the wait outright and went on to claim the message for
    // lost-subscriber recovery under a live waiter that was about to be handed it. The budget now
    // runs on the injected clock's MONOTONIC timestamp, the rule the poll deadlines already follow.
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task DeliveryConfirmation_SurvivesAForwardWallClockStep(Provider provider)
    {
        var clock = new ForwardSteppingClock();
        await using var harness = Harness.Create(provider, false, TimeSpan.FromSeconds(30), timeProvider: clock);
        // One poll interval as long as the whole budget: the only thing that can end the wait
        // early is the in-process delivery below, never a store round trip.
        harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromSeconds(20), pollInterval: TimeSpan.FromSeconds(20));

        var (acknowledged, delivered) = harness.BeginWaitForAcknowledgement(clock);

        // The local dispatch loop delivers a moment later — well inside the 20 s budget.
        await Task.Delay(TimeSpan.FromMilliseconds(100));
        Assert.False(acknowledged.IsCompleted, "The wait ended before the delivery: the stepped wall clock was read as an exhausted budget.");
        delivered.TrySetResult(true);

        Assert.True(await acknowledged.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    /// <summary>
    /// A system clock that is stepped a day forward after its first reading, as an NTP correction
    /// or a VM resume steps it; the monotonic timestamp (the base implementation's Stopwatch) and
    /// the timers are untouched, exactly as on a real host.
    /// </summary>
    private sealed class ForwardSteppingClock : TimeProvider
    {
        private int _step;

        /// <summary>Arms the step: the NEXT reading is the last one before the clock jumps.</summary>
        public void StepAfterNextReading() => Volatile.Write(ref _step, 1);

        public override DateTimeOffset GetUtcNow()
        {
            var now = base.GetUtcNow();
            return Interlocked.CompareExchange(ref _step, 2, 1) switch
            {
                1 => now,
                2 => now + TimeSpan.FromDays(1),
                _ => now
            };
        }
    }


    [Theory]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.MongoDb)]
    public void HistoryReconciliationInterval_RejectsZero(Provider provider)
    {
        Assert.Throws<InvalidOperationException>(() =>
        {
            switch (provider)
            {
                case Provider.PostgreSql:
                    new PostgreSqlAsyncResponseChannelOptions { HistoryReconciliationInterval = TimeSpan.Zero }.Validate();
                    break;
                case Provider.SqlServer:
                    new SqlServerAsyncResponseChannelOptions { ConnectionString = "Server=unused", HistoryReconciliationInterval = TimeSpan.Zero }.Validate();
                    break;
                default:
                    new MongoDbAsyncResponseChannelOptions { HistoryReconciliationInterval = TimeSpan.Zero }.Validate();
                    break;
            }
        });
    }

    private static MongoChannelMessageDocument HistoryRow(DateTimeOffset started, int index) => new()
    {
        Id = Guid.NewGuid(), CorrelationId = "corr", EnvelopeJson = StaleEnvelope,
        CreatedAtUtc = started.AddMilliseconds(index).UtcDateTime,
        AckedAtUtc = started.AddSeconds(1).UtcDateTime,
        ExpiresAtUtc = started.AddHours(1).UtcDateTime
    };

    private static async Task SweepAndDrainAsync(Harness harness)
    {
        await harness.InvokeAsync("DispatchPendingMessagesAsync", new HashSet<string> { "corr" }, CancellationToken.None);
        var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await harness.Executors.EnqueueAsync(harness.ChannelName("corr"), () => { drained.TrySetResult(); return Task.CompletedTask; });
        await drained.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }

    private static void ServeHistory(Harness harness, List<MongoChannelMessageDocument> rows, List<int> reads)
    {
        harness.MongoMessages!.Setup(collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns((FilterDefinition<MongoChannelMessageDocument> filter, FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument> options, CancellationToken _) =>
            {
                var bson = filter.Render(new RenderArgs<MongoChannelMessageDocument>(BsonSerializer.LookupSerializer<MongoChannelMessageDocument>(), BsonSerializer.SerializerRegistry));
                var found = rows.Where(row => Matches(row.ToBsonDocument(), bson)).OrderBy(row => row.CreatedAtUtc).ThenBy(row => row.ToBsonDocument()["_id"]).Take(options.Limit ?? int.MaxValue).ToList();
                reads.Add(found.Count);
                return Task.FromResult(Cursor(found));
            });
        harness.MongoMessages.Setup(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>())).ReturnsAsync(rows[0]);

        static bool Matches(BsonDocument row, BsonDocument filter)
        {
            foreach (var entry in filter)
            {
                if (entry.Name == "$expr") continue; // Every fixture row is unexpired.
                if (entry.Name == "$and") { if (!entry.Value.AsBsonArray.All(item => Matches(row, item.AsBsonDocument))) return false; continue; }
                if (entry.Name == "$or") { if (!entry.Value.AsBsonArray.Any(item => Matches(row, item.AsBsonDocument))) return false; continue; }
                var value = row[entry.Name];
                if (entry.Value is not BsonDocument conditions) { if (!value.Equals(entry.Value)) return false; continue; }
                foreach (var condition in conditions)
                {
                    var comparison = value.CompareTo(condition.Value);
                    if (condition.Name == "$gt" && comparison <= 0 || condition.Name == "$gte" && comparison < 0) return false;
                }
            }
            return true;
        }
    }

    /// <summary>A one-batch cursor over <paramref name="items"/>, for the mocked collection's <c>FindAsync</c>.</summary>
    private static IAsyncCursor<T> Cursor<T>(IReadOnlyList<T> items)
    {
        var cursor = new Mock<IAsyncCursor<T>>();
        var moved = false;
        cursor.Setup(c => c.MoveNextAsync(It.IsAny<CancellationToken>())).ReturnsAsync(() => !moved && (moved = true));
        cursor.Setup(c => c.MoveNext(It.IsAny<CancellationToken>())).Returns(() => !moved && (moved = true));
        cursor.SetupGet(c => c.Current).Returns(items);
        return cursor.Object;
    }

    /// <summary>The <c>correlation_id</c> a rendered message filter asks for, or <c>null</c>.</summary>
    private static string? CorrelationIdOf(FilterDefinition<MongoChannelMessageDocument> filter)
    {
        var rendered = filter.Render(new RenderArgs<MongoChannelMessageDocument>(
            BsonSerializer.LookupSerializer<MongoChannelMessageDocument>(),
            BsonSerializer.SerializerRegistry));
        return FindCorrelationId(rendered);

        static string? FindCorrelationId(BsonValue value)
        {
            switch (value)
            {
                case BsonDocument document:
                    if (document.TryGetValue("correlation_id", out var id) && id.IsString)
                        return id.AsString;
                    foreach (var element in document)
                    {
                        if (FindCorrelationId(element.Value) is { } nested)
                            return nested;
                    }
                    return null;
                case BsonArray array:
                    foreach (var item in array)
                    {
                        if (FindCorrelationId(item) is { } nested)
                            return nested;
                    }
                    return null;
                default:
                    return null;
            }
        }
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

    // ---------------------------------------------------------------------------------------
    // Fixpoint round 1 (G5): database-channel dispatch regressions
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// Fixpoint r1 S5#2: a response whose INSERT was stamped before a row the scan already read,
    /// but whose transaction committed after that read, lands BEHIND the forward cursor. Only
    /// history reconciliation (one page per pass, a whole interval after the last one) used to
    /// find it, and on defaults that came after the publisher's 5 s confirmation budget — the
    /// publisher then claimed it for lost-subscriber recovery under the live waiter. A fresh
    /// cursor now revisits a lookback window behind itself (half the confirmation budget, at most
    /// 2 s), so the late commit's own wake delivers it. Reconciliation is held an hour away to
    /// prove it is not what delivers the row. Pre-fix: the late row stayed undelivered (5, not 6).
    /// The window is held open explicitly: it is open for twice the lookback of REAL time after
    /// the cursor moved, and this fact used to pass only if the two sweeps ran less than 4 s apart.
    /// </summary>
    [Fact]
    public async Task DispatchSweep_ALateCommitBehindTheCursor_IsDeliveredByTheNextPass_WithoutWaitingForReconciliation()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30), timeProvider: clock);
        harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromSeconds(5), pollInterval: TimeSpan.FromMilliseconds(50));
        harness.SetOption("HistoryReconciliationInterval", TimeSpan.FromHours(1));
        var started = clock.GetUtcNow();
        var rows = Enumerable.Range(1, 5).Select(i => HistoryRow(started, i)).ToList();
        var delivered = 0;
        var subscription = harness.Subscription("corr", started).Instance;
        harness.SetProcessHook(subscription, () => { Interlocked.Increment(ref delivered); return Task.CompletedTask; });
        harness.AddSubscription("corr", subscription);
        ServeHistory(harness, rows, []);
        await SweepAndDrainAsync(harness);
        Assert.Equal(5, delivered);

        // Stamped with the oldest row's tick (and a lower id), committed only now — nobody has
        // acknowledged it yet.
        var late = HistoryRow(started, 1);
        late.Id = Guid.Empty;
        late.AckedAtUtc = null;
        rows.Add(late);
        harness.HoldLookbackWindowOpen("corr");
        await SweepAndDrainAsync(harness);
        Assert.Equal(6, delivered);

        // Seen rows in the window are screened, never delivered twice.
        harness.HoldLookbackWindowOpen("corr");
        await SweepAndDrainAsync(harness);
        Assert.Equal(6, delivered);
    }

    /// <summary>
    /// Pre-commit fix (fixpoint r1 D2): a page the executor refused ended the pass before the
    /// cursor was put back, so a refused revisit left it at the lookback window's start and no
    /// longer caught up — every rescan then re-read the whole window from there, open or not, and
    /// re-offered the same rows to the executor that had just refused them. It now goes back to
    /// where the pass found it, caught up, and the late row is still delivered while the window
    /// is open. Red on the round's code: the cursor stayed 2 s behind, not caught up.
    /// </summary>
    [Fact]
    public async Task DispatchSweep_ARefusedLookbackPage_PutsTheCursorBackWhereThePassFoundIt()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30), timeProvider: clock);
        harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromSeconds(5), pollInterval: TimeSpan.FromMilliseconds(50));
        harness.SetOption("HistoryReconciliationInterval", TimeSpan.FromHours(1));
        var started = clock.GetUtcNow();
        var rows = Enumerable.Range(1, 5).Select(i => HistoryRow(started, i)).ToList();
        var delivered = 0;
        var subscription = harness.Subscription("corr", started).Instance;
        harness.SetProcessHook(subscription, () => { Interlocked.Increment(ref delivered); return Task.CompletedTask; });
        harness.AddSubscription("corr", subscription);
        ServeHistory(harness, rows, []);
        await SweepAndDrainAsync(harness);
        Assert.Equal(5, delivered);
        var caughtUp = harness.ForwardCursor("corr");
        Assert.True(caughtUp.CaughtUp);

        // A late commit behind the cursor, and an executor with no room left for it.
        var late = HistoryRow(started, 1);
        late.Id = Guid.Empty;
        late.AckedAtUtc = null;
        rows.Add(late);
        var release = await OccupyExecutorAsync(harness, "corr");
        try
        {
            for (var i = 0; i < ChannelSerialExecutor.DefaultCapacity; i++)
                Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Accepted, harness.Executors.TryEnqueue(harness.ChannelName("corr"), () => Task.CompletedTask));
            harness.HoldLookbackWindowOpen("corr");

            await harness.InvokeAsync("DispatchPendingMessagesAsync", new HashSet<string> { "corr" }, CancellationToken.None);

            Assert.Equal(caughtUp, harness.ForwardCursor("corr"));
        }
        finally
        {
            // Released on failure too: a parked executor would otherwise hold the harness's disposal.
            release.TrySetResult();
        }

        // Once the executor has room, the next pass inside the window delivers the late row.
        await DrainAsync(harness, "corr");
        harness.HoldLookbackWindowOpen("corr");
        await SweepAndDrainAsync(harness);
        Assert.Equal(6, delivered);
    }

    /// <summary>
    /// Pre-commit fix (fixpoint r1 pass 2): the restored cursor does not advance, so while
    /// refusals lasted the lookback window aged out behind it — an executor full for longer than
    /// twice the lookback (a slow Until predicate with 1,024 queued) — and the refused late row,
    /// now behind a caught-up cursor, was left to reconciliation. A refusal now restarts the
    /// window, so it stays open while refusals last. The window runs on the real monotonic clock,
    /// so it is observed through its stamp: held an hour ahead before the refused pass, it must
    /// now run from the refusal. Red on the round's code: the stamp was left an hour ahead.
    /// </summary>
    [Fact]
    public async Task DispatchSweep_ARefusedLookbackPage_KeepsTheLookbackWindowOpenWhileRefusalsLast()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30), timeProvider: clock);
        harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromSeconds(5), pollInterval: TimeSpan.FromMilliseconds(50));
        harness.SetOption("HistoryReconciliationInterval", TimeSpan.FromHours(1));
        var started = clock.GetUtcNow();
        var rows = Enumerable.Range(1, 5).Select(i => HistoryRow(started, i)).ToList();
        var subscription = harness.Subscription("corr", started).Instance;
        harness.SetProcessHook(subscription, () => Task.CompletedTask);
        harness.AddSubscription("corr", subscription);
        ServeHistory(harness, rows, []);
        await SweepAndDrainAsync(harness);

        // A late commit behind the cursor, and an executor with no room left for it.
        var late = HistoryRow(started, 1);
        late.Id = Guid.Empty;
        late.AckedAtUtc = null;
        rows.Add(late);
        var release = await OccupyExecutorAsync(harness, "corr");
        try
        {
            for (var i = 0; i < ChannelSerialExecutor.DefaultCapacity; i++)
                Assert.Equal(SerialExecutorRegistry.TryEnqueueOutcome.Accepted, harness.Executors.TryEnqueue(harness.ChannelName("corr"), () => Task.CompletedTask));
            harness.HoldLookbackWindowOpen("corr");
            var before = System.Diagnostics.Stopwatch.GetTimestamp();

            await harness.InvokeAsync("DispatchPendingMessagesAsync", new HashSet<string> { "corr" }, CancellationToken.None);

            Assert.True(harness.ForwardCursor("corr").CaughtUp);
            Assert.InRange(harness.ForwardAdvancedAt("corr")!.Value, before, System.Diagnostics.Stopwatch.GetTimestamp());
        }
        finally
        {
            // Released on failure too: a parked executor would otherwise hold the harness's disposal.
            release.TrySetResult();
        }
    }

    /// <summary>
    /// Pre-commit fix (fixpoint r1 D2): rows are marked seen only when their work item runs, so a
    /// row admitted by one pass but still queued behind a slow item was re-read by the next pass's
    /// lookback window (or last-tick revisit) and queued again — on every pass, each duplicate
    /// taking an executor slot ahead of new rows. The scan now screens rows it has already queued.
    /// Red on the round's code: 10 queued items for 5 rows.
    /// </summary>
    [Fact]
    public async Task DispatchSweep_ARowStillQueuedFromAnEarlierPass_IsNotQueuedAgain()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30), timeProvider: clock);
        harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromSeconds(5), pollInterval: TimeSpan.FromMilliseconds(50));
        harness.SetOption("HistoryReconciliationInterval", TimeSpan.FromHours(1));
        var started = clock.GetUtcNow();
        var rows = Enumerable.Range(1, 5).Select(i => HistoryRow(started, i)).ToList();
        var delivered = 0;
        var subscription = harness.Subscription("corr", started).Instance;
        harness.SetProcessHook(subscription, () => { Interlocked.Increment(ref delivered); return Task.CompletedTask; });
        harness.AddSubscription("corr", subscription);
        ServeHistory(harness, rows, []);

        // Admitted behind a busy item, not processed yet.
        var release = await OccupyExecutorAsync(harness, "corr");
        try
        {
            await harness.InvokeAsync("DispatchPendingMessagesAsync", new HashSet<string> { "corr" }, CancellationToken.None);
            Assert.Equal(5, harness.PendingDispatches("corr"));

            // The next pass revisits the window and reads all five again.
            harness.HoldLookbackWindowOpen("corr");
            await harness.InvokeAsync("DispatchPendingMessagesAsync", new HashSet<string> { "corr" }, CancellationToken.None);
            Assert.Equal(5, harness.PendingDispatches("corr"));
        }
        finally
        {
            // Released on failure too: a parked executor would otherwise hold the harness's disposal.
            release.TrySetResult();
        }

        await DrainAsync(harness, "corr");
        Assert.Equal(5, delivered);
    }

    /// <summary>
    /// Fixpoint r1 S5#11: seen-set entries were stamped and aged on the injected clock, while the
    /// rows they stand in for expire on the SERVER's clock. A clock jumped past
    /// <c>MessageRetention</c> + 1 min (a virtual clock a test advanced, a host clock stepped
    /// forward) emptied a live waiter's seen set while the rows were still retained, and the
    /// last-tick overlap and reconciliation then handed them to it again. The fixture treats every
    /// row as unexpired, which models the server still retaining them. Pre-fix: 4 deliveries.
    /// </summary>
    [Fact]
    public async Task DispatchSweep_AClockJumpPastMessageRetention_DoesNotRedeliverWhatAWaiterAlreadyProcessed()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30), timeProvider: clock);
        var started = clock.GetUtcNow();
        // Acked after the waiter started: fan-out-eligible, so every re-read passes the watermark
        // and only the seen set keeps it from being delivered again.
        var rows = Enumerable.Range(0, 3).Select(i => HistoryRow(started, i)).ToList();
        var delivered = 0;
        var subscription = harness.Subscription("corr", started).Instance;
        harness.SetProcessHook(subscription, () => { Interlocked.Increment(ref delivered); return Task.CompletedTask; });
        harness.AddSubscription("corr", subscription);
        ServeHistory(harness, rows, []);
        await SweepAndDrainAsync(harness);
        Assert.Equal(3, delivered);

        clock.Advance(TimeSpan.FromHours(2));
        await SweepAndDrainAsync(harness);
        await SweepAndDrainAsync(harness);

        Assert.Equal(3, delivered);
    }

    /// <summary>
    /// Fixpoint r1 S5#3: PostgreSQL never lifted the full-sweep throttle, so while its LISTEN was
    /// down — before the first one, across every reconnect, or forever on a half-open socket —
    /// cross-process responses waited for the 5 s throttled sweep, which equals the publisher's
    /// confirmation budget, and were claimed for lost-subscriber recovery under live waiters. The
    /// throttle now applies only while a LISTEN is established; without one the sweep runs at the
    /// wake-down floor, a quarter of the confirmation budget (the harness's 2 ms budget makes that
    /// every 20 ms tick here). Pre-fix: the configured 10 minutes was returned throughout, and the
    /// second pass below scanned nothing.
    /// <para>
    /// Fixpoint r2 S11#5: this fact never had a LISTEN established, so the listen loop's
    /// <c>_listening = false</c> reset was unpinned (deleting it stayed green). Like its MongoDB
    /// twin it now establishes one first: the throttle applies, then the loop fails and lifts it.
    /// </para>
    /// </summary>
    [Fact]
    public async Task FullSweepThrottle_PostgreSql_IsLiftedUntilListenIsEstablished_AndAfterEveryListenFailure()
    {
        await using var harness = Harness.Create(
            Provider.PostgreSql, failing: true, pollInterval: TimeSpan.FromMilliseconds(20), fullSweepInterval: TimeSpan.FromMinutes(10));

        // Nothing is listening yet: every poll tick is a full sweep.
        Assert.Equal(HarnessWakeDownInterval, harness.CurrentFullSweepInterval());
        Assert.Null(await harness.CollectDispatchScopeAsync());
        Assert.Null(await harness.CollectDispatchScopeAsync());

        // An established LISTEN carries delivery: the configured throttle applies.
        harness.MarkWakeListenerEstablished();
        Assert.Equal(TimeSpan.FromMinutes(10), harness.CurrentFullSweepInterval());

        // The listen loop fails (closed port) and keeps retrying: no push wake from then on.
        harness.Invoke("EnsureListenerStarted");
        await harness.Logger.WaitForAsync("PostgreSQL LISTEN loop failed");
        Assert.Equal(HarnessWakeDownInterval, harness.CurrentFullSweepInterval());
    }

    /// <summary>The wake-down sweep interval on the harness's defaults: a quarter of its 2 ms confirmation budget.</summary>
    private static readonly TimeSpan HarnessWakeDownInterval = TimeSpan.FromMilliseconds(2) / 4;

    /// <summary>
    /// Pre-commit fix (fixpoint r1 D3): the round lifted the throttle to EVERY tick while the push
    /// wake was down, so an outage or an exhausted pool — the very conditions that drop LISTEN —
    /// got up to eight pooled connections held back to back, and the reconnecting listener
    /// competed with them. The sweep now runs at <c>min(FullSweepInterval,
    /// DeliveryConfirmationTimeout / 4)</c> while the wake is down: a tick inside that floor scans
    /// nothing. A shorter configured throttle still wins, and polling for good (MongoDB without
    /// change streams) still sweeps every tick. Red on the round's code: no interval, and the
    /// second tick swept again.
    /// </summary>
    [Theory]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task FullSweepThrottle_WhileTheWakeIsDown_SweepsAtAQuarterOfTheConfirmationBudget_NotEveryTick(Provider provider)
    {
        await using var harness = Harness.Create(
            provider, failing: false, pollInterval: TimeSpan.FromMilliseconds(20), fullSweepInterval: TimeSpan.FromMinutes(10), useChangeStreams: true);
        harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromMinutes(20), pollInterval: TimeSpan.FromMilliseconds(50));

        Assert.Equal(TimeSpan.FromMinutes(5), harness.CurrentFullSweepInterval());
        Assert.Null(await harness.CollectDispatchScopeAsync());
        var suppressed = Assert.IsType<HashSet<string>>(await harness.CollectDispatchScopeAsync());
        Assert.Empty(suppressed);

        harness.SetOption("FullSweepInterval", TimeSpan.FromMinutes(1));
        Assert.Equal(TimeSpan.FromMinutes(1), harness.CurrentFullSweepInterval());
        harness.SetOption("FullSweepInterval", null!);
        Assert.Null(harness.CurrentFullSweepInterval());
    }

    /// <summary>
    /// Pre-commit fix (fixpoint r1 D3): the PostgreSQL listen loop reset its failure count only
    /// when the listen method returned — which it does only on shutdown — so every LISTEN that
    /// was established and then dropped escalated the next reconnect's backoff, toward a 5 s gap
    /// without a push wake on every flap. A failure after a healthy run now starts the count over
    /// (pass 2: only after one — see the flap test below); the healthy-run threshold is shortened
    /// to zero here so every established listener counts as one. Each listener connects and is
    /// dropped straight away. Backoff(1) is 50–100 ms (half-jitter); the third failure of an
    /// unreset run is at least 200 ms. Red on the round's code: the third delay was 200–400 ms.
    /// </summary>
    [Theory]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task WakeListener_AFailureAfterAHealthyRun_StartsTheReconnectBackoffOver(Provider provider)
    {
        await using var server = new FakePostgresWireServer();
        await using var harness = FlappingWakeListener(provider, server);
        harness.WakeListenerHealthyRun = TimeSpan.Zero;

        harness.Invoke("EnsureListenerStarted");
        var delays = await WakeListenerRetryDelaysAsync(harness, provider, count: 3);

        Assert.All(delays, delay => Assert.InRange(delay, TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(100)));
    }

    /// <summary>
    /// Pre-commit fix (fixpoint r1 pass 2): pass 1 reset the backoff on every established LISTEN
    /// or opened change stream, which removed its bound on a server or proxy that accepts the
    /// session and then drops it (a crash-looping server, a short <c>idle_session_timeout</c>, a
    /// stream whose first read fails): it reconnected every 50–100 ms for good, each time
    /// requesting an immediate full sweep and logging a warning. The count now starts over only
    /// after the listener stayed up the backoff's 5 s cap (SubscriberSupervisor's healthy-run
    /// rule), so a connect-then-drop flap escalates like any failure run: the third delay is
    /// Backoff(3), 200–400 ms. The threshold is raised to an hour here so no run, however slow the
    /// runner, counts as healthy. Red on pass 1's code: every delay was 50–100 ms.
    /// </summary>
    [Theory]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task WakeListener_AConnectThenDropFlap_KeepsEscalatingTheReconnectBackoff(Provider provider)
    {
        await using var server = new FakePostgresWireServer();
        await using var harness = FlappingWakeListener(provider, server);
        Assert.Equal(TimeSpan.FromSeconds(5), harness.WakeListenerHealthyRun);
        harness.WakeListenerHealthyRun = TimeSpan.FromHours(1);

        harness.Invoke("EnsureListenerStarted");
        var delays = await WakeListenerRetryDelaysAsync(harness, provider, count: 3);

        Assert.InRange(delays[2], TimeSpan.FromMilliseconds(200), TimeSpan.FromMilliseconds(400));
    }

    /// <summary>
    /// A channel whose wake listener connects and is dropped straight away, every time: a
    /// <see cref="FakePostgresWireServer"/> that answers the LISTEN and its delivery probe and then
    /// closes the socket, or a change stream that opens and then fails its first read. (The server
    /// is unused for MongoDB.)
    /// </summary>
    private static Harness FlappingWakeListener(Provider provider, FakePostgresWireServer server)
    {
        if (provider == Provider.PostgreSql)
        {
            server.Respond = (_, sql) => Task.FromResult(
                FakePostgresWireServer.Reply.CompleteEchoingNotify(sql, closeAfter: sql.StartsWith("NOTIFY", StringComparison.Ordinal)));
            var postgres = Harness.Create(
                Provider.PostgreSql, failing: false, pollInterval: TimeSpan.FromSeconds(30), postgreSqlConnectionString: server.ConnectionString());
            postgres.MarkStoreCreated();
            return postgres;
        }

        var mongo = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30), useChangeStreams: true);
        var dropped = new Mock<IChangeStreamCursor<ChangeStreamDocument<MongoChannelMessageDocument>>>();
        dropped.Setup(cursor => cursor.MoveNextAsync(It.IsAny<CancellationToken>())).ThrowsAsync(new MongoException("stream dropped"));
        mongo.MongoMessages!
            .Setup(collection => collection.WatchAsync(
                It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoChannelMessageDocument>, ChangeStreamDocument<MongoChannelMessageDocument>>>(),
                It.IsAny<ChangeStreamOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(dropped.Object);
        return mongo;
    }

    /// <summary>The reconnect delays the wake listener's first <paramref name="count"/> failures logged.</summary>
    private static async Task<List<TimeSpan>> WakeListenerRetryDelaysAsync(Harness harness, Provider provider, int count)
    {
        var fragment = provider == Provider.PostgreSql ? "PostgreSQL LISTEN loop failed" : "MongoDB change-stream loop failed";
        await harness.Logger.WaitForAsync(fragment, occurrences: count);
        var delays = harness.Logger.Messages
            .Where(message => message.Contains(fragment, StringComparison.Ordinal))
            .Take(count)
            .Select(message => TimeSpan.Parse(
                System.Text.RegularExpressions.Regex.Match(message, @"retrying in (?<delay>[0-9:.]+)\.$").Groups["delay"].Value,
                System.Globalization.CultureInfo.InvariantCulture))
            .ToList();
        Assert.Equal(count, delays.Count);
        return delays;
    }

    /// <summary>
    /// The counterpart: once a LISTEN (or a Mongo change stream) is established, the configured
    /// throttle applies again, and one full sweep is requested at once — the wakes published while
    /// nothing was listening are gone, and a re-opened change stream has no resume point.
    /// </summary>
    [Theory]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task FullSweepThrottle_AnEstablishedWakeListener_RestoresTheThrottle_AndRequestsOneFullSweep(Provider provider)
    {
        await using var harness = Harness.Create(
            provider, failing: false, pollInterval: TimeSpan.FromSeconds(30), fullSweepInterval: TimeSpan.FromMinutes(10), useChangeStreams: true);
        Assert.Equal(HarnessWakeDownInterval, harness.CurrentFullSweepInterval());

        harness.MarkWakeListenerEstablished();

        Assert.Equal(TimeSpan.FromMinutes(10), harness.CurrentFullSweepInterval());
        // The 30 s poll is nowhere near due: only the requested sweep can produce this scope.
        Assert.Null(await harness.CollectDispatchScopeAsync().WaitAsync(TimeSpan.FromSeconds(10)));
    }

    /// <summary>
    /// MongoDB lifted the throttle only when the server reported change streams unsupported, not
    /// while a stream was failing transiently. Pre-fix: the configured interval was returned with
    /// the stream down.
    /// </summary>
    [Fact]
    public async Task FullSweepThrottle_MongoDb_IsLiftedWhileTheChangeStreamIsDown()
    {
        await using var harness = Harness.Create(
            Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30), fullSweepInterval: TimeSpan.FromMinutes(10), useChangeStreams: true);
        harness.MongoMessages!
            .Setup(collection => collection.WatchAsync(
                It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoChannelMessageDocument>, ChangeStreamDocument<MongoChannelMessageDocument>>>(),
                It.IsAny<ChangeStreamOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new MongoException("stream down"));
        harness.MarkWakeListenerEstablished();
        Assert.Equal(TimeSpan.FromMinutes(10), harness.CurrentFullSweepInterval());

        harness.Invoke("EnsureListenerStarted");
        await harness.Logger.WaitForAsync("change-stream loop failed");

        Assert.Equal(HarnessWakeDownInterval, harness.CurrentFullSweepInterval());
    }

    /// <summary>
    /// Fixpoint r2 precommit J7: the change-stream loop's failure warning ran unguarded, so a
    /// logging provider that throws (Microsoft.Extensions.Logging rethrows a provider's failure)
    /// ended the loop for good at its first stream failure — no stream was ever re-opened, and the
    /// waiters were left on the wake-down sweep for the rest of the process's uptime. The loop now
    /// survives its own log line. Red on the old code: the stream was never re-opened.
    /// </summary>
    [Fact]
    public async Task ChangeStreamLoop_ALoggerThatThrowsOnTheFailureWarning_KeepsReopeningTheStream()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30), useChangeStreams: true);
        harness.Logger.ThrowOnMessageContaining = "change-stream loop failed";
        var watches = 0;
        var reopened = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.MongoMessages!
            .Setup(collection => collection.WatchAsync(
                It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoChannelMessageDocument>, ChangeStreamDocument<MongoChannelMessageDocument>>>(),
                It.IsAny<ChangeStreamOptions>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (Interlocked.Increment(ref watches) >= 2)
                    reopened.TrySetResult();
                return Task.FromException<IChangeStreamCursor<ChangeStreamDocument<MongoChannelMessageDocument>>>(new MongoException("stream down"));
            });

        harness.Invoke("EnsureListenerStarted");

        await reopened.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Contains(harness.Logger.Messages, message => message.Contains("change-stream loop failed", StringComparison.Ordinal));
    }

    /// <summary>
    /// The wake-down floor (fixpoint r1 D3) is for a stream that is only down until it re-opens.
    /// A server that reports change streams unsupported (a standalone) is polled for good, like
    /// <c>UseChangeStreams = false</c>, and keeps its full sweep on every tick.
    /// </summary>
    [Fact]
    public async Task FullSweepThrottle_MongoDb_AServerWithoutChangeStreams_SweepsOnEveryTick()
    {
        await using var harness = Harness.Create(
            Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30), fullSweepInterval: TimeSpan.FromMinutes(10), useChangeStreams: true);
        var connectionId = new MongoDB.Driver.Core.Connections.ConnectionId(
            new MongoDB.Driver.Core.Servers.ServerId(new MongoDB.Driver.Core.Clusters.ClusterId(), new System.Net.DnsEndPoint("localhost", 27017)));
        harness.MongoMessages!
            .Setup(collection => collection.WatchAsync(
                It.IsAny<PipelineDefinition<ChangeStreamDocument<MongoChannelMessageDocument>, ChangeStreamDocument<MongoChannelMessageDocument>>>(),
                It.IsAny<ChangeStreamOptions>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new MongoCommandException(
                connectionId, "aggregate", new BsonDocument(), new BsonDocument { ["ok"] = 0, ["code"] = 40573, ["errmsg"] = "unsupported" }));

        harness.Invoke("EnsureListenerStarted");
        await harness.Logger.WaitForAsync("MongoDB change streams are unavailable");

        Assert.Null(harness.CurrentFullSweepInterval());
    }

    /// <summary>
    /// Fixpoint r1 S5#4: every process received every wake on the database and wrote each one to
    /// its bounded (1024, DropOldest) signal channel, local waiter or not. A burst during one long
    /// pass evicted the earliest signals — including the one for the waiter this process actually
    /// holds — silently. Wakes for ids with no local waiter are now dropped at the source.
    /// Pre-fix: the scope held 1024 remote ids and not <c>corr-local</c>.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task SignalDispatcher_DropsWakesForCorrelationIdsWithNoLocalWaiter(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        harness.AddWaiters("corr-local");

        harness.Invoke("SignalDispatcher", "corr-local");
        for (var i = 0; i < 1100; i++)
            harness.Invoke("SignalDispatcher", $"corr-remote-{i}");

        var scope = Assert.IsType<HashSet<string>>(await harness.CollectDispatchScopeAsync());
        Assert.Single(scope, "corr-local");
    }

    /// <summary>
    /// The same bound with relevant wakes only: a signal the full channel refuses is no longer
    /// dropped — it requests a full sweep, which covers every id a refused signal could have
    /// named. Pre-fix: DropOldest kept the last 1024 ids and the pass scanned only those.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task SignalDispatcher_AnOverflowOfRelevantWakes_RequestsAFullSweepInsteadOfDroppingAny(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        var ids = Enumerable.Range(0, 1100).Select(i => $"corr-{i}").ToArray();
        harness.AddWaiters(ids);

        foreach (var id in ids)
            harness.Invoke("SignalDispatcher", id);

        Assert.Null(await harness.CollectDispatchScopeAsync());
        // Consumed with that sweep, not requested again.
        harness.Invoke("SignalDispatcher", "corr-0");
        var next = Assert.IsType<HashSet<string>>(await harness.CollectDispatchScopeAsync());
        Assert.Contains("corr-0", next);
    }

    /// <summary>
    /// Fixpoint r1 S5#5: a waiter's subscriber row lapses once heartbeats have failed for longer
    /// than the heartbeat timeout (a database outage). A publish in that window counted no
    /// subscriber and went to lost-subscriber recovery — even from the waiter's own process, where
    /// the live subscription sat in the local map. A live local subscription now counts as live.
    /// Pre-fix: the recovery path ran (no registration was found) and the waiter never completed.
    /// </summary>
    [Fact]
    public async Task Publish_ALiveLocalWaiter_IsDeliveredEvenWhenItsSubscriberRowHasLapsed()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromSeconds(5), pollInterval: TimeSpan.FromMilliseconds(50));
        var startedAt = DateTimeOffset.UtcNow;
        var (waiter, completion) = harness.Subscription("corr", startedAt);
        harness.AddSubscription("corr", waiter);
        harness.MongoSubscribers!
            .Setup(collection => collection.CountDocumentsAsync(
                It.IsAny<FilterDefinition<MongoChannelSubscriberDocument>>(),
                It.IsAny<CountOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(0L);
        ArrangeMongoInsertAndClaim(harness, Stored(Guid.NewGuid(), "corr", startedAt));

        await ((IAsyncResponsePublisher)harness.Channel).SetResponse(
            new OperationResult { Status = OperationStatus.Completed, Message = "live" }, "corr");

        Assert.Equal("live", (await completion.Task.WaitAsync(TimeSpan.FromSeconds(10))).Message);
        harness.RecoveryState.Verify(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    /// <summary>
    /// The same rule on the exception path.
    /// </summary>
    [Fact]
    public async Task SetException_ALiveLocalWaiter_IsFailedLiveEvenWhenItsSubscriberRowHasLapsed()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromSeconds(5), pollInterval: TimeSpan.FromMilliseconds(50));
        var startedAt = DateTimeOffset.UtcNow;
        var (waiter, completion) = harness.Subscription("corr", startedAt);
        harness.AddSubscription("corr", waiter);
        ArrangeMongoInsertAndClaim(harness, Stored(Guid.NewGuid(), "corr", startedAt));

        await ((IAsyncResponsePublisher)harness.Channel).SetException(new InvalidOperationException("remote boom"), "corr");

        var failure = await Assert.ThrowsAnyAsync<Exception>(() => completion.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal("remote boom", failure.Message);
        harness.RecoveryState.Verify(store => store.GetAllAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    /// <summary>
    /// Fixpoint r1 S5#5, the heartbeat half: a failed round waited a full
    /// <c>SubscriberHeartbeatInterval</c> before the next try, so after an outage longer than the
    /// heartbeat timeout the rows stayed lapsed for up to a whole interval after the database came
    /// back. A failed round is now retried on a short backoff. The first failure stretches the
    /// interval to an hour, so only the retry can bring the second round. Pre-fix: no second round.
    /// </summary>
    [Fact]
    public async Task Heartbeat_AFailedRound_IsRetriedOnAShortBackoff_NotAfterAFullInterval()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        harness.AddWaiters("corr");
        var rounds = 0;
        var retried = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.MongoSubscribers!
            .Setup(collection => collection.BulkWriteAsync(
                It.IsAny<IEnumerable<WriteModel<MongoChannelSubscriberDocument>>>(),
                It.IsAny<BulkWriteOptions>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (Interlocked.Increment(ref rounds) == 1)
                {
                    harness.SetOption("SubscriberHeartbeatInterval", TimeSpan.FromHours(1));
                    return Task.FromException<BulkWriteResult<MongoChannelSubscriberDocument>>(new MongoException("database failing over"));
                }

                retried.TrySetResult();
                return Task.FromResult<BulkWriteResult<MongoChannelSubscriberDocument>>(null!);
            });

        harness.Invoke("EnsureListenerStarted");

        await retried.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// Pre-commit fix (fixpoint r1 D6): the short retry logged a Warning on every failed round —
    /// about one a second through an outage, ten times the old full-interval volume. The first
    /// failure of a run warns; later ones at most once per heartbeat interval (an hour here, from
    /// the first failure on), the rest at Debug. The fourth round starting means the first three
    /// failures were all logged. Red on the round's code: three warnings.
    /// </summary>
    [Fact]
    public async Task Heartbeat_ARunOfFailedRetries_WarnsOnceNotOncePerRetry()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        harness.AddWaiters("corr");
        var rounds = 0;
        var fourthRound = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.MongoSubscribers!
            .Setup(collection => collection.BulkWriteAsync(
                It.IsAny<IEnumerable<WriteModel<MongoChannelSubscriberDocument>>>(),
                It.IsAny<BulkWriteOptions>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                var round = Interlocked.Increment(ref rounds);
                if (round == 1)
                    harness.SetOption("SubscriberHeartbeatInterval", TimeSpan.FromHours(1));
                if (round == 4)
                    fourthRound.TrySetResult();
                return Task.FromException<BulkWriteResult<MongoChannelSubscriberDocument>>(new MongoException("database down"));
            });

        harness.Invoke("EnsureListenerStarted");
        await fourthRound.Task.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Single(harness.Logger.Messages, message => message.Contains("subscriber heartbeat failed", StringComparison.Ordinal));
        // Rounds 2 and 3 at Debug (round 4's may land either side of this read).
        Assert.InRange(harness.Logger.Messages.Count(message => message.Contains("subscriber heartbeat retry", StringComparison.Ordinal)), 2, 3);
    }

    /// <summary>
    /// Fixpoint r1 S5#6: the full sweep awaited one store query per subscribed correlation id in
    /// turn, so its duration grew as waiters × round trip and crossed the 5 s confirmation budget
    /// at a few thousand waiters. Ids are now dispatched with bounded concurrency. Every query
    /// waits at a barrier until eight are in flight at once — the whole first wave — so only a
    /// concurrent sweep can get past it; a sequential one parks its first query there for good.
    /// The 30 s wait is a hang guard for that case, not an assertion window (it used to be a 5 s
    /// in-mock delay that a starved thread pool could turn into a false pass or fail). Pre-fix:
    /// the sweep never got past the barrier.
    /// </summary>
    [Fact]
    public async Task FullSweep_DispatchesCorrelationIdsConcurrently()
    {
        const int firstWave = 8;
        await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30));
        harness.AddWaiters(Enumerable.Range(0, 2 * firstWave).Select(i => $"corr-{i}").ToArray());
        var arrived = 0;
        var inFlight = 0;
        var peak = 0;
        var wholeWaveInFlight = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task<IAsyncCursor<MongoChannelMessageDocument>> HoldAtTheBarrierAsync(CancellationToken token)
        {
            var now = Interlocked.Increment(ref inFlight);
            for (var seen = Volatile.Read(ref peak); now > seen; seen = Volatile.Read(ref peak))
            {
                if (Interlocked.CompareExchange(ref peak, now, seen) == seen)
                    break;
            }

            if (Interlocked.Increment(ref arrived) >= firstWave)
                wholeWaveInFlight.TrySetResult();
            await wholeWaveInFlight.Task.WaitAsync(token);
            Interlocked.Decrement(ref inFlight);
            return Cursor(new List<MongoChannelMessageDocument>());
        }

        harness.MongoMessages!
            .Setup(collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns((FilterDefinition<MongoChannelMessageDocument> _, FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument> _, CancellationToken token) =>
                HoldAtTheBarrierAsync(token));

        using var abandon = new CancellationTokenSource();
        var sweep = harness.InvokeAsync("DispatchPendingMessagesAsync", null, abandon.Token);
        try
        {
            await sweep.WaitAsync(TimeSpan.FromSeconds(30));
        }
        finally
        {
            await abandon.CancelAsync();
        }

        Assert.Equal(firstWave, Volatile.Read(ref peak));
    }

    /// <summary>
    /// Pre-commit fix (fixpoint r1 D1): with every id dispatched in isolation, a database outage
    /// cost one failing store call per subscribed correlation id per pass, and the loop's Warning
    /// carried one exception per id — at 1,000 waiters, megabytes of stack traces every poll tick.
    /// Once the first wave of ids to settle has failed transiently the breaker skips the rest of
    /// the pass, and the exception carries the first three failures and counts the others.
    /// Settle order and the trip are decided under one lock, so at most one id per other worker
    /// (seven) can still be in flight when it trips. Red on the round's code: all 40 ids were
    /// loaded and the exception carried 40 inner exceptions.
    /// </summary>
    [Fact]
    public async Task FullSweep_AStoreOutage_TripsTheBreakerAfterTheFirstWave_AndCapsTheReportedFailures()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, TimeSpan.FromSeconds(30));
        harness.AddWaiters(Enumerable.Range(0, 40).Select(i => $"corr-{i}").ToArray());
        var loaded = new System.Collections.Concurrent.ConcurrentDictionary<string, byte>();
        harness.MongoMessages!
            .Setup(collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns((FilterDefinition<MongoChannelMessageDocument> filter, FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument> _, CancellationToken _) =>
            {
                loaded.TryAdd(CorrelationIdOf(filter)!, 0);
                return Task.FromException<IAsyncCursor<MongoChannelMessageDocument>>(new TimeoutException("server selection timed out"));
            });

        var failure = await Assert.ThrowsAsync<AggregateException>(
            () => harness.InvokeAsync("DispatchPendingMessagesAsync", null, CancellationToken.None));

        Assert.InRange(loaded.Count, 8, 15);
        Assert.Equal(3, failure.InnerExceptions.Count);
        Assert.All(failure.InnerExceptions, inner => Assert.IsType<TimeoutException>(inner));
        Assert.Contains($"failed for {loaded.Count} correlation ids (the first 3 attached, and {loaded.Count - 3} more)", failure.Message, StringComparison.Ordinal);
        Assert.Contains($"the remaining {40 - loaded.Count} were left to a later pass", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The targeted pass (a burst of signals, or rescans, during an outage) walks its ids in turn,
    /// so it stops after exactly the first wave; the ids it skips had their signals consumed, so
    /// the next pass is a full sweep. Red on the round's code: all 20 ids were loaded.
    /// </summary>
    [Fact]
    public async Task TargetedPass_AStoreOutage_StopsAfterTheFirstWave_AndRequestsAFullSweep()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, TimeSpan.FromSeconds(30));
        var ids = Enumerable.Range(0, 20).Select(i => $"corr-{i}").ToArray();
        harness.AddWaiters(ids);
        var loads = 0;
        harness.MongoMessages!
            .Setup(collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                Interlocked.Increment(ref loads);
                return Task.FromException<IAsyncCursor<MongoChannelMessageDocument>>(new TimeoutException("server selection timed out"));
            });

        await Assert.ThrowsAsync<AggregateException>(
            () => harness.InvokeAsync("DispatchPendingMessagesAsync", new HashSet<string>(ids, StringComparer.Ordinal), CancellationToken.None));

        Assert.Equal(8, loads);
        harness.Invoke("SignalDispatcher", "corr-0");
        Assert.Null(await harness.CollectDispatchScopeAsync());
    }

    /// <summary>
    /// Fixpoint r1 S5#6 / S5#12: one correlation id's failure (a transient fault, a poisoned row,
    /// SQL Server's 2,100-parameter error on an oversized hydration) aborted the whole pass at that
    /// id — every id after it lost its delivery on every pass. The targeted scope enumerates in
    /// insertion order, so the failing id comes first, the order the pre-fix loop stopped at. The
    /// pass still reports the failure (the loop logs it and backs off). Pre-fix: the healthy id
    /// was never dispatched.
    /// </summary>
    [Fact]
    public async Task DispatchPass_ACorrelationIdWhoseLoadFails_DoesNotStopTheOthers()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, TimeSpan.FromSeconds(30));
        var startedAt = DateTimeOffset.UtcNow;
        harness.AddSubscription("corr-poisoned", harness.Subscription("corr-poisoned", startedAt).Instance);
        var (healthy, _) = harness.Subscription("corr-healthy", startedAt);
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.SetProcessHook(healthy, () => { delivered.TrySetResult(); return Task.CompletedTask; });
        harness.AddSubscription("corr-healthy", healthy);
        ServePoisonedLoads(harness, "corr-poisoned", Stored(Guid.NewGuid(), "corr-healthy", startedAt));

        var pass = harness.InvokeAsync(
            "DispatchPendingMessagesAsync",
            new HashSet<string>(StringComparer.Ordinal) { "corr-poisoned", "corr-healthy" },
            CancellationToken.None);

        await Assert.ThrowsAnyAsync<Exception>(() => pass);
        await delivered.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// The full-sweep counterpart: however the dictionary enumerates, the healthy id is dispatched.
    /// Poisoned rows are not transient faults, so the outage breaker (fixpoint r1 D1) never trips
    /// on them; the reported exception carries the first three and counts the rest.
    /// </summary>
    [Fact]
    public async Task FullSweep_CorrelationIdsWhoseLoadsFail_DoNotStopTheOthers()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, TimeSpan.FromSeconds(30));
        var startedAt = DateTimeOffset.UtcNow;
        harness.AddWaiters(Enumerable.Range(0, 20).Select(i => $"corr-poisoned-{i}").ToArray());
        var (healthy, _) = harness.Subscription("corr-healthy", startedAt);
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.SetProcessHook(healthy, () => { delivered.TrySetResult(); return Task.CompletedTask; });
        harness.AddSubscription("corr-healthy", healthy);
        ServePoisonedLoads(harness, "corr-poisoned", Stored(Guid.NewGuid(), "corr-healthy", startedAt));

        var failure = await Assert.ThrowsAsync<AggregateException>(
            () => harness.InvokeAsync("DispatchPendingMessagesAsync", null, CancellationToken.None));

        Assert.Equal(3, failure.InnerExceptions.Count);
        Assert.Contains("failed for 20 correlation ids (the first 3 attached, and 17 more); every other id was still dispatched", failure.Message, StringComparison.Ordinal);
        await delivered.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// Fixpoint r1 S5#14: the Mongo channel counted subscribers under the collection's DEFAULT
    /// collation. On an operator-provisioned case-folding collection a count for "ABC" counted
    /// "abc"'s live waiter, so publishes to "ABC" took the live route and sat out the whole
    /// confirmation budget. The count pins the simple (binary) collation, as the Mongo transport
    /// does. Pre-fix: no options were passed.
    /// </summary>
    [Fact]
    public async Task CountActiveSubscribers_MongoDb_CountsUnderTheSimpleCollation()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        CountOptions? used = null;
        harness.MongoSubscribers!
            .Setup(collection => collection.CountDocumentsAsync(
                It.IsAny<FilterDefinition<MongoChannelSubscriberDocument>>(),
                It.IsAny<CountOptions>(),
                It.IsAny<CancellationToken>()))
            .Callback((FilterDefinition<MongoChannelSubscriberDocument> _, CountOptions options, CancellationToken _) => used = options)
            .ReturnsAsync(1L);

        Assert.Equal(1L, await ((IActiveSubscriberProbe)harness.Channel).CountActiveSubscribersAsync("ABC"));

        Assert.Equal(Collation.Simple, used?.Collation);
    }

    /// <summary>
    /// Fixpoint r1 S5#15 (a): the same-process fast path ran its work item on the PUBLISHER's
    /// token. A publisher that gave up after its response was stored (an aborted request) aborted
    /// the queued local delivery — its claim threw — and the executor logged the cancellation as
    /// an error. The item now runs on the channel's own dispatch token. Pre-fix: "Channel executor
    /// error" was logged and the waiter never received the response.
    /// </summary>
    [Fact]
    public async Task FastPath_APublisherCancellingAfterItsResponseIsStored_DoesNotAbortTheQueuedLocalDelivery()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        var startedAt = DateTimeOffset.UtcNow;
        var (waiter, completion) = harness.Subscription("corr", startedAt);
        harness.AddSubscription("corr", waiter);
        var release = await OccupyExecutorAsync(harness, "corr");
        ArrangeMongoInsertAndClaim(harness, Stored(Guid.NewGuid(), "corr", startedAt), claimHonoursCancellation: true);

        using var publisher = new CancellationTokenSource();
        await harness.InvokeAsync("PublishMessageAsync", Guid.NewGuid(), "corr", LiveEnvelope, publisher.Token);
        await publisher.CancelAsync();
        release.TrySetResult();

        Assert.Equal("live", (await completion.Task.WaitAsync(TimeSpan.FromSeconds(10))).Message);
        Assert.DoesNotContain(harness.Logger.Messages, message => message.Contains("Channel executor error", StringComparison.Ordinal));
    }

    /// <summary>
    /// The channel's own token is cancelled at disposal: a delivery still queued then stops
    /// quietly (Debug), instead of every such item being logged as an executor error on each
    /// graceful shutdown.
    /// </summary>
    [Fact]
    public async Task FastPath_ADeliveryStillQueuedWhenTheChannelStops_IsNotLoggedAsAnExecutorError()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        var startedAt = DateTimeOffset.UtcNow;
        harness.AddSubscription("corr", harness.Subscription("corr", startedAt).Instance);
        harness.Invoke("EnsureListenerStarted");
        var release = await OccupyExecutorAsync(harness, "corr");
        ArrangeMongoInsertAndClaim(harness, Stored(Guid.NewGuid(), "corr", startedAt), claimHonoursCancellation: true);

        await harness.InvokeAsync("PublishMessageAsync", Guid.NewGuid(), "corr", LiveEnvelope, CancellationToken.None);
        await harness.ListenerCancellation!.CancelAsync();
        release.TrySetResult();
        await DrainAsync(harness, "corr");

        Assert.DoesNotContain(harness.Logger.Messages, message => message.Contains("Channel executor error", StringComparison.Ordinal));
        Assert.Contains(harness.Logger.Messages, message => message.Contains("stopped with the channel", StringComparison.Ordinal));
    }

    /// <summary>
    /// Fixpoint r1 S5#15 (b): the fast path AWAITED executor admission, and behind an executor
    /// mid-retirement (a sibling waiter's cleanup draining a slow predicate) a same-process publish
    /// sat out the whole drain — up to 30 s + 30 s — stalling a serial ingress. Admission is now
    /// non-blocking: the row is stored, and the targeted signal hands it to the sweep. Pre-fix:
    /// the publish threw OperationCanceledException when its 5 s token lapsed.
    /// </summary>
    [Fact]
    public async Task FastPath_DoesNotWaitOutAnExecutorRetirement()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        var startedAt = DateTimeOffset.UtcNow;
        harness.AddSubscription("corr", harness.Subscription("corr", startedAt).Instance);
        var release = await OccupyExecutorAsync(harness, "corr");
        var retirement = harness.Executors.RemoveAsync(harness.ChannelName("corr")).AsTask();
        ArrangeMongoInsertAndClaim(harness, Stored(Guid.NewGuid(), "corr", startedAt));

        try
        {
            using var publisher = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await harness.InvokeAsync("PublishMessageAsync", Guid.NewGuid(), "corr", LiveEnvelope, publisher.Token);

            // Stored and handed to the sweep, which admits it once the retirement is over.
            var scope = Assert.IsType<HashSet<string>>(await harness.CollectDispatchScopeAsync());
            Assert.Contains("corr", scope);
        }
        finally
        {
            release.TrySetResult();
            await retirement.WaitAsync(TimeSpan.FromSeconds(10));
        }
    }

    /// <summary>
    /// Fixpoint r1 S5#16: the delivery-confirmation poll runs after the response row is stored,
    /// and one transient fault among its polls failed the whole publish — the ingress then
    /// re-published under a NEW message id, a duplicate for an Until waiter. A transient poll
    /// failure now reads as "not yet acknowledged". Pre-fix: the TimeoutException escaped.
    /// </summary>
    [Fact]
    public async Task DeliveryConfirmation_ATransientFaultInTheAcknowledgementPoll_KeepsPolling()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromSeconds(10), pollInterval: TimeSpan.FromMilliseconds(1));
        var polls = 0;
        harness.MongoMessages!
            .Setup(collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, DateTime?>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() => Interlocked.Increment(ref polls) == 1
                ? Task.FromException<IAsyncCursor<DateTime?>>(new TimeoutException("connection reset during the poll"))
                : Task.FromResult(Cursor<DateTime?>([DateTime.UtcNow])));

        Assert.True(await harness.WaitForAcknowledgementAsync(Guid.NewGuid()).WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.True(polls >= 2);
    }

    /// <summary>
    /// The recovery claim at the deadline is retried on the insert's policy (it is idempotent):
    /// one transient fault there failed the publish after its row was stored. Pre-fix: the
    /// TimeoutException escaped.
    /// </summary>
    [Fact]
    public async Task DeliveryConfirmation_ATransientFaultInTheRecoveryClaim_IsRetried()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromMilliseconds(20), pollInterval: TimeSpan.FromMilliseconds(5));
        harness.MongoMessages!
            .Setup(collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, DateTime?>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() => Task.FromResult(Cursor<DateTime?>([])));
        var claims = 0;
        harness.MongoMessages
            .Setup(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() => Interlocked.Increment(ref claims) == 1
                ? Task.FromException<MongoChannelMessageDocument>(new TimeoutException("claim reply lost"))
                : Task.FromResult(Stored(Guid.NewGuid(), "corr", DateTimeOffset.UtcNow)));

        // The recovery claim won on its retry: not delivered live.
        Assert.False(await harness.TryConfirmDeliveryAsync(Guid.NewGuid()).WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(2, claims);
    }

    /// <summary>
    /// Fixpoint r1 S3#8: the waiter's activity status copied the REMOTE failure message verbatim —
    /// up to the inbound size limit, CR/LF included — while the remote stack trace was already
    /// capped for exactly that exposure. The status now quotes a capped, escaped excerpt; the
    /// waiter's exception keeps the message verbatim. Pre-fix: raw CR/LF and 100 KB in the status.
    /// </summary>
    [Theory]
    [InlineData(Provider.SqlServer)]
    [InlineData(Provider.PostgreSql)]
    [InlineData(Provider.MongoDb)]
    public async Task RemoteFailure_TheActivityStatusQuotesAnEscapedBoundedExcerpt(Provider provider)
    {
        await using var harness = Harness.Create(provider, failing: true, pollInterval: TimeSpan.FromSeconds(30));
        using var activity = new System.Diagnostics.Activity("asyncresponse.wait").Start();
        var (subscription, completion) = harness.Subscription("corr", activity: activity);
        var hostile = "boom\r\nFORGED log entry" + new string('x', 100_000);
        var envelope = System.Text.Json.JsonSerializer.Serialize(new
        {
            SchemaVersion = 1,
            Success = false,
            Payload = (object?)null,
            ExceptionMessage = hostile,
            ExceptionStackTrace = (string?)null
        });

        await (Task)subscription.GetType().GetMethod("ProcessAsync")!.Invoke(subscription, [harness.Message(envelope)])!;

        Assert.Equal(System.Diagnostics.ActivityStatusCode.Error, activity.Status);
        var status = activity.StatusDescription!;
        Assert.DoesNotContain('\r', status);
        Assert.DoesNotContain('\n', status);
        Assert.StartsWith("boom\\u000d\\u000aFORGED", status, StringComparison.Ordinal);
        Assert.True(status.Length < 1024, $"status is {status.Length} characters");
        Assert.Equal(hostile, (await Assert.ThrowsAnyAsync<Exception>(() => completion.Task)).Message);
    }

    // ---------------------------------------------------------------------------------------
    // Fixpoint round 2 (G5): database-channel dispatch, sweep and listen loop
    // ---------------------------------------------------------------------------------------

    /// <summary>
    /// Fixpoint r2 S5#3: a pass whose load failed transiently had already consumed its id's wake
    /// (a NOTIFY, a change event, a local publish), and unless the breaker tripped nothing came
    /// back for the id but the next full sweep — with the push wake up, <c>FullSweepInterval</c>
    /// away (5 s by default, the publisher's whole confirmation budget), so a response behind one
    /// dead pooled connection after a failover was claimed for lost-subscriber recovery under its
    /// live waiter. The id is now rescanned on its own after the wake-down floor (a quarter of the
    /// confirmation budget; 50 ms here) — never the poll interval, which would retry every failed
    /// id on every tick. The poll is 30 s and the full sweep 10 minutes away here, so only that
    /// rescan can produce the targeted scope. Red on the old code: nothing signalled the id.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DispatchPass_ATransientLoadFailureBelowTheBreaker_IsRescannedAfterTheWakeDownFloor(bool fullSweep)
    {
        var floor = TimeSpan.FromMilliseconds(50);
        await using var harness = Harness.Create(
            Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30), fullSweepInterval: TimeSpan.FromMinutes(10), useChangeStreams: true);
        harness.ConfigureDeliveryConfirmation(timeout: floor * 4, pollInterval: TimeSpan.FromMilliseconds(5));
        // A change stream carrying delivery (so the throttle is the 10 minutes); its "sweep once
        // now" request is taken off the queue first.
        harness.MarkWakeListenerEstablished();
        Assert.Null(await harness.CollectDispatchScopeAsync().WaitAsync(TimeSpan.FromSeconds(10)));

        var startedAt = DateTimeOffset.UtcNow;
        var (waiter, _) = harness.Subscription("corr", startedAt);
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.SetProcessHook(waiter, () => { delivered.TrySetResult(); return Task.CompletedTask; });
        harness.AddSubscription("corr", waiter);
        var pending = Stored(Guid.NewGuid(), "corr", startedAt);
        var loads = 0;
        harness.MongoMessages!
            .Setup(collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() => Interlocked.Increment(ref loads) == 1
                ? Task.FromException<IAsyncCursor<MongoChannelMessageDocument>>(new TimeoutException("a pooled connection died in the failover"))
                : Task.FromResult(Cursor(new List<MongoChannelMessageDocument> { pending })));
        harness.MongoMessages
            .Setup(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(pending);

        var failedAt = System.Diagnostics.Stopwatch.GetTimestamp();
        await Assert.ThrowsAsync<TimeoutException>(() => harness.InvokeAsync(
            "DispatchPendingMessagesAsync",
            fullSweep ? null : new HashSet<string>(StringComparer.Ordinal) { "corr" },
            CancellationToken.None));

        var scope = Assert.IsType<HashSet<string>>(await harness.CollectDispatchScopeAsync().WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Single(scope, "corr");
        // The floor, not an immediate retry (the timer behind it has a millisecond or so of slack).
        Assert.True(System.Diagnostics.Stopwatch.GetElapsedTime(failedAt) >= floor - TimeSpan.FromMilliseconds(15));

        await harness.InvokeAsync("DispatchPendingMessagesAsync", scope, CancellationToken.None);
        await delivered.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// Fixpoint r2 S5#3 (the round-1 pre-commit B-p2 #1 redo): a REQUESTED full sweep — here the
    /// one an established change stream asks for — carries work nothing else comes back for, and
    /// when the outage breaker cut it short its unvisited ids waited for the regular throttle (10
    /// minutes here, 5 s by default). Round 1 re-requested it outright, which made every poll tick
    /// sweep in full while the outage lasted, past both throttles, so it was taken out. The retry
    /// now waits for the wake-down floor (500 ms here) after the tripped sweep — never less — while
    /// a timer sweep that trips is left to the next scheduled one. Red with the re-arm removed: the
    /// retry never came.
    /// </summary>
    [Fact]
    public async Task FullSweep_ARequestedSweepTheBreakerCutShort_IsRetriedAtTheWakeDownFloor_NeverSooner()
    {
        var floor = TimeSpan.FromMilliseconds(500);
        await using var harness = Harness.Create(
            Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromMilliseconds(20), fullSweepInterval: TimeSpan.FromMinutes(10), useChangeStreams: true);
        harness.ConfigureDeliveryConfirmation(timeout: floor * 4, pollInterval: TimeSpan.FromMilliseconds(5));
        harness.AddWaiters(Enumerable.Range(0, 40).Select(i => $"corr-{i}").ToArray());
        harness.MongoMessages!
            .Setup(collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() => Task.FromException<IAsyncCursor<MongoChannelMessageDocument>>(new TimeoutException("server selection timed out")));

        // A timer sweep that trips leaves its unvisited ids to the next scheduled sweep.
        await Assert.ThrowsAsync<AggregateException>(() => harness.InvokeAsync("DispatchPendingMessagesAsync", null, CancellationToken.None));
        Assert.False(harness.RequestedSweepRetryArmed);

        // A requested one is retried.
        harness.MarkWakeListenerEstablished();
        var requestedAt = System.Diagnostics.Stopwatch.GetTimestamp();
        Assert.Null(await harness.CollectDispatchScopeAsync().WaitAsync(TimeSpan.FromSeconds(10)));
        var tripped = await Assert.ThrowsAsync<AggregateException>(() => harness.InvokeAsync("DispatchPendingMessagesAsync", null, CancellationToken.None));
        Assert.Contains("the store looks unavailable", tripped.Message, StringComparison.Ordinal);
        Assert.True(harness.RequestedSweepRetryArmed);

        await RetryIsDueAsync().WaitAsync(TimeSpan.FromSeconds(10));
        // The poll ticks every 20 ms; the retry waited for the floor, measured from the stamp the
        // requested sweep took (which is after requestedAt).
        Assert.True(System.Diagnostics.Stopwatch.GetElapsedTime(requestedAt) >= floor);
        Assert.False(harness.RequestedSweepRetryArmed);

        async Task RetryIsDueAsync()
        {
            while (await harness.CollectDispatchScopeAsync() is not null)
            {
            }
        }
    }

    /// <summary>
    /// Fixpoint r2 S5#4: an id whose subscriptions are all dropped (waiters mid-cleanup, their
    /// store deletes still riding the connect timeout of the outage) returns without a store call,
    /// and it counted as a success: settling at once, it took a place in the breaker's first wave
    /// and kept it from ever tripping — every waiter's failing call then ran on every pass. Such
    /// ids no longer count. The targeted scope enumerates in insertion order, so the dropped id
    /// settles first. Red on the old code: all 20 failing ids were loaded.
    /// </summary>
    [Fact]
    public async Task TargetedPass_AStoreOutage_StillTripsWhenAWaiterIsMidCleanup()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, TimeSpan.FromSeconds(30));
        var ids = Enumerable.Range(0, 20).Select(i => $"corr-{i}").ToArray();
        AddMidCleanupWaiter(harness, "corr-dropped");
        harness.AddWaiters(ids);
        var loads = 0;
        harness.MongoMessages!
            .Setup(collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                Interlocked.Increment(ref loads);
                return Task.FromException<IAsyncCursor<MongoChannelMessageDocument>>(new TimeoutException("server selection timed out"));
            });
        var scope = new HashSet<string>(StringComparer.Ordinal) { "corr-dropped" };
        scope.UnionWith(ids);

        await Assert.ThrowsAsync<AggregateException>(() => harness.InvokeAsync("DispatchPendingMessagesAsync", scope, CancellationToken.None));

        Assert.Equal(8, loads);
        harness.Invoke("SignalDispatcher", "corr-0");
        Assert.Null(await harness.CollectDispatchScopeAsync());
    }

    /// <summary>
    /// The full-sweep counterpart. The map enumerates in hash order, so the dropped ids are the
    /// majority: on the old code the first eight to settle were all failures well under one run
    /// in a thousand, and the sweep visited all 40 failing ids.
    /// </summary>
    [Fact]
    public async Task FullSweep_AStoreOutage_StillTripsWhenWaitersAreMidCleanup()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, TimeSpan.FromSeconds(30));
        harness.AddWaiters(Enumerable.Range(0, 40).Select(i => $"corr-{i}").ToArray());
        for (var i = 0; i < 120; i++)
            AddMidCleanupWaiter(harness, $"corr-dropped-{i}");
        var loaded = new System.Collections.Concurrent.ConcurrentDictionary<string, byte>();
        harness.MongoMessages!
            .Setup(collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns((FilterDefinition<MongoChannelMessageDocument> filter, FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument> _, CancellationToken _) =>
            {
                loaded.TryAdd(CorrelationIdOf(filter)!, 0);
                return Task.FromException<IAsyncCursor<MongoChannelMessageDocument>>(new TimeoutException("server selection timed out"));
            });

        var failure = await Assert.ThrowsAsync<AggregateException>(
            () => harness.InvokeAsync("DispatchPendingMessagesAsync", null, CancellationToken.None));

        Assert.InRange(loaded.Count, 8, 15);
        Assert.DoesNotContain(loaded.Keys, id => id.StartsWith("corr-dropped", StringComparison.Ordinal));
        Assert.Contains("the store looks unavailable", failure.Message, StringComparison.Ordinal);
    }

    /// <summary>A registered waiter whose only subscription is already dropped: one whose cleanup is still running.</summary>
    private static void AddMidCleanupWaiter(Harness harness, string correlationId)
    {
        var (subscription, _) = harness.Subscription(correlationId);
        SetField(subscription, "_dropped", true);
        harness.AddSubscription(correlationId, subscription);
    }

    /// <summary>
    /// Fixpoint r2 S5#5: while a cursor was fresh every pass re-read the whole late-commit
    /// lookback window, and a steady stream on one id (an Until progress stream) keeps it fresh —
    /// with a pass per publish that was about 2R² rows a second for R responses a second. The
    /// window is now revisited at most once per poll interval (30 s here — capped at the lookback,
    /// see the precommit D2 test below); the passes in between read the last tick only and schedule
    /// a rescan for when the throttle is over. Two seconds of window here (a 4 s confirmation
    /// budget), 200 rows inside it. Red on the old code: all five passes read the 200 rows (1,000).
    /// (Each throttled pass is stamped as right after the revisit, so the 2 s throttle cannot run
    /// out on a slow runner between two sweeps.)
    /// </summary>
    [Fact]
    public async Task DispatchSweep_TheLookbackWindow_IsRevisitedAtMostOncePerPollInterval()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30), timeProvider: clock);
        harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromSeconds(4), pollInterval: TimeSpan.FromMilliseconds(50));
        harness.SetOption("HistoryReconciliationInterval", TimeSpan.FromHours(1));
        var started = clock.GetUtcNow();
        var rows = Enumerable.Range(0, 200).Select(i => HistoryRow(started, i)).ToList();
        var delivered = 0;
        var subscription = harness.Subscription("corr", started).Instance;
        harness.SetProcessHook(subscription, () => { Interlocked.Increment(ref delivered); return Task.CompletedTask; });
        harness.AddSubscription("corr", subscription);
        var reads = new List<int>();
        ServeHistory(harness, rows, reads);
        await SweepAndDrainAsync(harness);
        Assert.Equal(200, delivered);
        Assert.Empty(harness.BackpressureRescans);
        Assert.Empty(harness.LookbackRescans);

        reads.Clear();
        harness.HoldLookbackWindowOpen("corr");
        await SweepAndDrainAsync(harness);
        for (var pass = 1; pass < 5; pass++)
        {
            harness.StampWindowRevisit("corr", TimeSpan.Zero);
            await SweepAndDrainAsync(harness);
        }

        // One window revisit (200 rows), then the last tick (one row) four times.
        Assert.Equal(204, reads.Sum());
        Assert.Equal(200, delivered);
        Assert.Contains("corr", harness.LookbackRescans.Keys.Cast<string>());
    }

    /// <summary>
    /// Fixpoint r2 precommit D2: the revisit throttle was the poll interval, and the window is open
    /// for only twice the lookback (at most 4 s). With a poll interval of a few seconds (allowed;
    /// nothing ties it to the confirmation budget the lookback derives from) a late commit whose
    /// wake pass was throttled got its rescan a whole interval later — past the window's close — and
    /// waited for history reconciliation while its publisher's confirmation lapsed into
    /// lost-subscriber recovery under a live waiter. The throttle is now at most the lookback, and
    /// the rescan is due when it ends. Here: a 3 s poll interval, a 2 s lookback. Red on the old
    /// code: the pass 2.2 s after the revisit read the last tick only (still inside the 3 s
    /// interval), and the throttled pass logged no lookback rescan (a backpressure rescan a full
    /// 3 s out instead).
    /// </summary>
    [Fact]
    public async Task DispatchSweep_ALongPollInterval_ThrottlesTheLookbackRevisitToTheLookback()
    {
        var clock = new AsyncResponse.Testing.VirtualTimeProvider();
        await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(3), timeProvider: clock);
        harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromSeconds(4), pollInterval: TimeSpan.FromMilliseconds(50));
        harness.SetOption("HistoryReconciliationInterval", TimeSpan.FromHours(1));
        var started = clock.GetUtcNow();
        var rows = Enumerable.Range(0, 200).Select(i => HistoryRow(started, i)).ToList();
        var subscription = harness.Subscription("corr", started).Instance;
        harness.SetProcessHook(subscription, () => Task.CompletedTask);
        harness.AddSubscription("corr", subscription);
        var reads = new List<int>();
        ServeHistory(harness, rows, reads);
        await SweepAndDrainAsync(harness);

        // Past the 2 s lookback, still inside the 3 s poll interval: the window is revisited.
        harness.HoldLookbackWindowOpen("corr");
        harness.StampWindowRevisit("corr", TimeSpan.FromSeconds(2.2));
        reads.Clear();
        await SweepAndDrainAsync(harness);
        Assert.Equal(200, reads.Sum());

        // Inside the throttle: the last tick only, and a rescan due when the throttle ends — within
        // the lookback, not a whole poll interval out.
        harness.StampWindowRevisit("corr", TimeSpan.FromSeconds(0.1));
        reads.Clear();
        await SweepAndDrainAsync(harness);
        Assert.Equal(1, reads.Sum());
        Assert.Contains("corr", harness.LookbackRescans.Keys.Cast<string>());
        var scheduled = harness.Logger.Messages.Single(message => message.Contains("the window is revisited again in", StringComparison.Ordinal));
        var delay = TimeSpan.Parse(
            System.Text.RegularExpressions.Regex.Match(scheduled, @"revisited again in (?<delay>[0-9:.]+)\.$").Groups["delay"].Value,
            System.Globalization.CultureInfo.InvariantCulture);
        Assert.InRange(delay, TimeSpan.FromTicks(1), TimeSpan.FromSeconds(1.9));
    }

    /// <summary>
    /// Fixpoint r2 precommit D2, the throttle itself: the poll interval capped at the lookback, and
    /// the rescan due when it ends. Red on the old rule (throttle = poll interval, rescan a whole
    /// interval out): the 3 s rows.
    /// </summary>
    [Theory]
    [InlineData(3000, 2000, 500, 1500)]
    [InlineData(3000, 2000, 2100, null)]
    [InlineData(250, 2000, 100, 150)]
    [InlineData(250, 2000, 300, null)]
    public void LookbackRescanDelay_IsThePollIntervalCappedAtTheLookback_LessTheTimeSinceTheRevisit(int pollMs, int lookbackMs, int sinceMs, int? expectedMs)
    {
        var helper = typeof(MongoDbAsyncResponseChannel).BaseType!.GetMethod("LookbackRescanDelay", BindingFlags.Static | BindingFlags.NonPublic)!;

        var delay = (TimeSpan?)helper.Invoke(null, [TimeSpan.FromMilliseconds(pollMs), TimeSpan.FromMilliseconds(lookbackMs), TimeSpan.FromMilliseconds(sinceMs)]);

        Assert.Equal(expectedMs is { } ms ? TimeSpan.FromMilliseconds(ms) : null, delay);
    }

    /// <summary>
    /// Fixpoint r2 GS3#3: the dispatch loop serves every correlation id of the channel, and a
    /// logging provider that throws (Microsoft.Extensions.Logging rethrows a provider's failure)
    /// ended it at its failure warning — every live waiter then timed out. The loop now survives
    /// its own log line. Red on the old code: the response was never delivered.
    /// </summary>
    [Fact]
    public async Task DispatchLoop_ALoggerThatThrowsOnTheFailureWarning_DoesNotEndTheLoop()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromMilliseconds(20));
        harness.Logger.ThrowOnMessageContaining = "response dispatch loop failed";
        var startedAt = DateTimeOffset.UtcNow;
        var (waiter, _) = harness.Subscription("corr", startedAt);
        var delivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.SetProcessHook(waiter, () => { delivered.TrySetResult(); return Task.CompletedTask; });
        harness.AddSubscription("corr", waiter);
        var pending = Stored(Guid.NewGuid(), "corr", startedAt);
        var loads = 0;
        harness.MongoMessages!
            .Setup(collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns(() => Interlocked.Increment(ref loads) == 1
                ? Task.FromException<IAsyncCursor<MongoChannelMessageDocument>>(new InvalidOperationException("one failed pass"))
                : Task.FromResult(Cursor(new List<MongoChannelMessageDocument> { pending })));
        harness.MongoMessages
            .Setup(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(pending);

        harness.Invoke("EnsureListenerStarted");

        await harness.Logger.WaitForAsync("response dispatch loop failed");
        await delivered.Task.WaitAsync(TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// Fixpoint r2 GS3#3: the heartbeat's failure warning threw, skipped the drop compensation,
    /// and the outer catch's own warning threw too — ending the loop, so every live waiter's
    /// subscriber row lapsed and their responses routed to lost-subscriber recovery until the
    /// process restarted. Red on the old code: no second round ever ran.
    /// </summary>
    [Fact]
    public async Task Heartbeat_ALoggerThatThrowsOnItsFailureWarnings_DoesNotEndTheLoop()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        harness.Logger.ThrowOnMessageContaining = "subscriber heartbeat";
        harness.AddWaiters("corr");
        var rounds = 0;
        var secondRound = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.MongoSubscribers!
            .Setup(collection => collection.BulkWriteAsync(
                It.IsAny<IEnumerable<WriteModel<MongoChannelSubscriberDocument>>>(),
                It.IsAny<BulkWriteOptions>(),
                It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                if (Interlocked.Increment(ref rounds) == 1)
                    return Task.FromException<BulkWriteResult<MongoChannelSubscriberDocument>>(new TimeoutException("heartbeat round failed"));
                secondRound.TrySetResult();
                return Task.FromResult<BulkWriteResult<MongoChannelSubscriberDocument>>(null!);
            });

        harness.Invoke("EnsureListenerStarted");

        await secondRound.Task.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Contains(harness.Logger.Messages, message => message.Contains("subscriber heartbeat failed", StringComparison.Ordinal));
    }

    /// <summary>
    /// Fixpoint r2 GS3#3: disposal absorbed only a loop's cancellation. A loop that had died of
    /// anything else rethrew here and skipped every waiter's cleanup, so their response tasks
    /// never settled and their executors were never retired. Red on the old code: disposal threw
    /// the loop's fault.
    /// </summary>
    [Fact]
    public async Task Dispose_ABackgroundLoopThatFaulted_StillCleansUpEveryWaiter()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        var (waiter, completion) = harness.Subscription("corr", cleanupStarted: false);
        harness.AddSubscription("corr", waiter);
        harness.Invoke("EnsureListenerStarted");
        harness.SetChannelField("_heartbeatTask", Task.FromException(new InvalidOperationException("the heartbeat loop died")));

        await harness.Channel.DisposeAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => completion.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Empty(harness.Subscriptions);
        Assert.Contains(harness.Logger.Messages, message => message.Contains("background loop had failed before disposal", StringComparison.Ordinal));
    }

    /// <summary>
    /// Fixpoint r2 S2#13: a registration that failed (here the recovery-state save) logged before
    /// cleaning up, so a logging provider that throws skipped the cleanup — the subscriber row
    /// stayed for publishers to count — and replaced the failure the caller received. Cleanup
    /// runs first now, and the original failure is rethrown. Red on the old code: the caller got
    /// the logger's exception and the subscriber row was never deleted.
    /// </summary>
    [Fact]
    public async Task CreateWaiter_AFailedRegistration_IsCleanedUpAndRethrown_EvenWhenTheLoggerThrows()
    {
        await using var harness = Harness.Create(Provider.MongoDb, failing: false, pollInterval: TimeSpan.FromSeconds(30));
        harness.Logger.ThrowOnMessageContaining = "Failed to create";
        harness.RecoveryState
            .Setup(store => store.SaveAsync(It.IsAny<string>(), It.IsAny<RecoveryState>(), It.IsAny<TimeSpan>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new TimeoutException("recovery store unreachable"));

        var failure = await Assert.ThrowsAsync<TimeoutException>(
            () => ((IAsyncResponseSubscriber)harness.Channel).CreateResponseWaiter<OperationResult>("corr"));

        Assert.Equal("recovery store unreachable", failure.Message);
        harness.MongoSubscribers!.Verify(
            collection => collection.DeleteOneAsync(It.IsAny<FilterDefinition<MongoChannelSubscriberDocument>>(), It.IsAny<CancellationToken>()),
            Times.Once);
        Assert.Contains(harness.Logger.Messages, message => message.Contains("Failed to create", StringComparison.Ordinal));
    }

    /// <summary>
    /// Fixpoint r2 S5#6: a LISTEN that succeeded was taken as proof NOTIFY works, but behind a
    /// transaction-mode pooler (PgBouncer pool_mode=transaction) the LISTEN runs on a server
    /// connection that goes straight back to the pool, and nothing reaches the channel's — while
    /// <c>SELECT 1</c> pings kept succeeding, so the channel kept its full-sweep throttle for good.
    /// A delivery probe (a NOTIFY the listen connection sends itself) must now come back before
    /// the LISTEN counts as established. Here the server completes the NOTIFY but never delivers
    /// it. Red on the old code: the listener reported itself established straight after LISTEN.
    /// </summary>
    [Fact]
    public async Task PostgreSqlListen_AListenWhoseNotificationsNeverArrive_IsNotEstablished()
    {
        await using var server = new FakePostgresWireServer();
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var sql = ListenOnlyStore(dataSource);
        sql.ListenProbeTimeout = TimeSpan.FromMilliseconds(200);
        var established = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stop = new CancellationTokenSource();

        var listen = sql.ExecuteListenAsync(_ => Task.CompletedTask, stop.Token, () => established.TrySetResult());
        try
        {
            Assert.Same(listen, await Task.WhenAny(listen, established.Task).WaitAsync(TimeSpan.FromSeconds(30)));
            var failure = await Assert.ThrowsAsync<TimeoutException>(() => listen);
            Assert.Contains("delivery-probe notification", failure.Message, StringComparison.Ordinal);
            Assert.Contains(server.Statements, statement => statement.Sql.StartsWith("NOTIFY", StringComparison.Ordinal));
        }
        finally
        {
            await stop.CancelAsync();
        }
    }

    /// <summary>
    /// Fixpoint r2 precommit D3: the probe waited <c>connection.WaitAsync(remaining)</c> once any
    /// budget was left, but Npgsql truncates the TimeSpan to whole milliseconds and treats 0 ms as
    /// an INFINITE wait — so a notification-free final fraction of a millisecond on a half-open
    /// socket blocked the listener forever, the hang the bounded wait exists to prevent. Under a
    /// millisecond now counts as spent. (The sub-millisecond window cannot be hit reliably through
    /// the wire, so the budget rule is pinned directly, plus the probe's use of it.) Red on the old
    /// rule (<c>remaining &lt;= 0</c>): the sub-millisecond rows.
    /// </summary>
    [Theory]
    [InlineData(-10_000L, true)]
    [InlineData(0L, true)]
    [InlineData(5_000L, true)]
    [InlineData(9_999L, true)]
    [InlineData(10_000L, false)]
    [InlineData(50_000_000L, false)]
    public void PostgreSqlListenProbe_ABudgetUnderAMillisecond_IsSpent(long remainingTicks, bool spent)
    {
        Assert.Equal(spent, PostgreSqlChannelSql.ProbeBudgetSpent(TimeSpan.FromTicks(remainingTicks)));

        var calls = SqlServerTransportStorePruneTests.Decode(SqlServerTransportStorePruneTests.AsyncBody(typeof(PostgreSqlChannelSql), "ProbeListenDeliveryAsync"))
            .Select(instruction => instruction.Operand)
            .OfType<MethodBase>()
            .Select(method => method.Name);
        Assert.Contains(nameof(PostgreSqlChannelSql.ProbeBudgetSpent), calls);
    }

    /// <summary>
    /// The counterpart: a probe that comes back makes the LISTEN established, and its own
    /// notification is consumed — never handed on as a wake. Other payloads still are.
    /// </summary>
    [Fact]
    public async Task PostgreSqlListen_ADeliveredProbe_EstablishesTheListen_AndIsNotHandedOnAsAWake()
    {
        await using var server = new FakePostgresWireServer();
        server.Respond = (_, statement) => Task.FromResult(FakePostgresWireServer.Reply.CompleteEchoingNotify(statement));
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var sql = ListenOnlyStore(dataSource);
        var established = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var payloads = new System.Collections.Concurrent.ConcurrentQueue<string?>();
        using var stop = new CancellationTokenSource();

        var listen = sql.ExecuteListenAsync(payload => { payloads.Enqueue(payload); return Task.CompletedTask; }, stop.Token, () => established.TrySetResult());
        try
        {
            await established.Task.WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Empty(payloads);
        }
        finally
        {
            await stop.CancelAsync();
            await Ignoring(() => listen);
        }
    }

    /// <summary>
    /// The liveness check on a quiet listen connection is the same probe: a connection that stops
    /// delivering (the server here echoes only the first NOTIFY) fails into the reconnect path
    /// after one liveness interval. Red with the old <c>SELECT 1</c> ping: the listener never
    /// noticed.
    /// </summary>
    [Fact]
    public async Task PostgreSqlListen_AQuietConnectionThatStopsDelivering_FailsIntoTheReconnectPath()
    {
        await using var server = new FakePostgresWireServer();
        var notifies = 0;
        server.Respond = (_, statement) => Task.FromResult(
            statement.StartsWith("NOTIFY", StringComparison.Ordinal) && Interlocked.Increment(ref notifies) > 1
                ? FakePostgresWireServer.Reply.Complete(statement)
                : FakePostgresWireServer.Reply.CompleteEchoingNotify(statement));
        await using var dataSource = NpgsqlDataSource.Create(server.ConnectionString());
        var sql = ListenOnlyStore(dataSource);
        sql.ListenLivenessInterval = TimeSpan.FromMilliseconds(100);
        sql.ListenProbeTimeout = TimeSpan.FromMilliseconds(200);
        var established = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stop = new CancellationTokenSource();

        var listen = sql.ExecuteListenAsync(_ => Task.CompletedTask, stop.Token, () => established.TrySetResult());
        try
        {
            await established.Task.WaitAsync(TimeSpan.FromSeconds(30));
            // The message tells the probe's timeout apart from the hang guard's.
            var failure = await Assert.ThrowsAsync<TimeoutException>(() => listen.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Contains("delivery-probe notification", failure.Message, StringComparison.Ordinal);
            Assert.True(Volatile.Read(ref notifies) >= 2);
        }
        finally
        {
            await stop.CancelAsync();
        }
    }

    /// <summary>
    /// The channel-level consequence: behind a server whose NOTIFYs never come back, the channel
    /// never treats its push wake as up — it keeps sweeping at the wake-down floor instead of the
    /// configured throttle. Red on the old code: the LISTEN counted as established (the 10 minute
    /// throttle applied) and the loop never failed.
    /// </summary>
    [Fact]
    public async Task FullSweepThrottle_PostgreSql_AListenThatDeliversNothing_KeepsTheThrottleLifted()
    {
        await using var server = new FakePostgresWireServer();
        await using var harness = Harness.Create(
            Provider.PostgreSql, failing: false, pollInterval: TimeSpan.FromSeconds(30), fullSweepInterval: TimeSpan.FromMinutes(10),
            postgreSqlConnectionString: server.ConnectionString());
        harness.MarkStoreCreated();
        harness.PostgreSqlStore.ListenProbeTimeout = TimeSpan.FromMilliseconds(200);

        harness.Invoke("EnsureListenerStarted");

        await harness.Logger.WaitForAsync("PostgreSQL LISTEN loop failed");
        Assert.Contains(server.Statements, statement => statement.Sql.StartsWith("LISTEN", StringComparison.Ordinal));
        Assert.Equal(HarnessWakeDownInterval, harness.CurrentFullSweepInterval());
    }

    /// <summary>A PostgreSQL channel store over <paramref name="dataSource"/> whose schema check is skipped (a fake server cannot answer the catalog).</summary>
    private static PostgreSqlChannelSql ListenOnlyStore(NpgsqlDataSource dataSource)
    {
        var sql = new PostgreSqlChannelSql(dataSource, Options.Create(new PostgreSqlAsyncResponseChannelOptions { AutoCreateSchema = false }));
        SetField(sql, "_created", true);
        return sql;
    }

    private const string LiveEnvelope =
        """{"SchemaVersion":1,"Success":true,"Payload":{"Status":2,"Message":"live"},"ExceptionMessage":null,"ExceptionStackTrace":null}""";

    /// <summary>An unacked stored response document, created at <paramref name="createdAt"/>.</summary>
    private static MongoChannelMessageDocument Stored(Guid id, string correlationId, DateTimeOffset createdAt) => new()
    {
        Id = id,
        CorrelationId = correlationId,
        EnvelopeJson = StaleEnvelope,
        CreatedAtUtc = createdAt.UtcDateTime,
        ExpiresAtUtc = createdAt.AddMinutes(5).UtcDateTime
    };

    /// <summary>
    /// The insert's upsert returns <paramref name="stored"/>; every delivery claim wins — or, with
    /// <paramref name="claimHonoursCancellation"/>, throws when its token is already cancelled, as
    /// the driver does.
    /// </summary>
    private static void ArrangeMongoInsertAndClaim(Harness harness, MongoChannelMessageDocument stored, bool claimHonoursCancellation = false)
    {
        harness.MongoMessages!
            .Setup(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.Is<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(options => options != null && options.IsUpsert),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(stored);
        harness.MongoMessages
            .Setup(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.Is<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(options => options == null || !options.IsUpsert),
                It.IsAny<CancellationToken>()))
            .Returns((FilterDefinition<MongoChannelMessageDocument> _, UpdateDefinition<MongoChannelMessageDocument> _, FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument> _, CancellationToken token) =>
                claimHonoursCancellation && token.IsCancellationRequested
                    ? Task.FromCanceled<MongoChannelMessageDocument>(token)
                    : Task.FromResult(stored));
    }

    /// <summary>Loads for ids starting with <paramref name="poisonedPrefix"/> throw; every other id reads <paramref name="pending"/>.</summary>
    private static void ServePoisonedLoads(Harness harness, string poisonedPrefix, MongoChannelMessageDocument pending)
    {
        harness.MongoMessages!
            .Setup(collection => collection.FindAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .Returns((FilterDefinition<MongoChannelMessageDocument> filter, FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument> _, CancellationToken _) =>
                CorrelationIdOf(filter) is { } id && id.StartsWith(poisonedPrefix, StringComparison.Ordinal)
                    ? Task.FromException<IAsyncCursor<MongoChannelMessageDocument>>(new InvalidOperationException($"poisoned row for {id}"))
                    : Task.FromResult(Cursor(new List<MongoChannelMessageDocument> { pending })));
        harness.MongoMessages
            .Setup(collection => collection.FindOneAndUpdateAsync(
                It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<UpdateDefinition<MongoChannelMessageDocument>>(),
                It.IsAny<FindOneAndUpdateOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(pending);
    }

    /// <summary>Parks the id's executor on an in-flight item; completing the returned source releases it.</summary>
    private static async Task<TaskCompletionSource> OccupyExecutorAsync(Harness harness, string correlationId)
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Assert.True(await harness.Executors.EnqueueAsync(harness.ChannelName(correlationId), async () =>
        {
            entered.TrySetResult();
            await release.Task;
        }));
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        return release;
    }

    /// <summary>Waits until everything queued on the id's executor so far has run.</summary>
    private static async Task DrainAsync(Harness harness, string correlationId)
    {
        var drained = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Assert.True(await harness.Executors.EnqueueAsync(harness.ChannelName(correlationId), () => { drained.TrySetResult(); return Task.CompletedTask; }));
        await drained.Task.WaitAsync(TimeSpan.FromSeconds(10));
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
    /// <summary>Target of <see cref="Harness.SetProcessHook"/>: bound to <paramref name="body"/>, ignores the message.</summary>
    private static Task InvokeProcessHook<TMessage>(Func<Task> body, TMessage _) => body();

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

        public static Harness Create(Provider provider, bool failing, TimeSpan pollInterval, TimeSpan? fullSweepInterval = null, bool useChangeStreams = false, int? pendingMessageBatchSize = null, TimeProvider? timeProvider = null, string? postgreSqlConnectionString = null)
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
                        logger.For<SqlServerAsyncResponseChannel>(), timeProvider: timeProvider);
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
                        postgreSqlConnectionString ?? "Host=localhost;Port=1;Database=unused;Username=unused;Password=unused;Timeout=1;Pooling=false");
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
                        logger.For<PostgreSqlAsyncResponseChannel>(), timeProvider: timeProvider);
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
                        PendingMessageBatchSize = pendingMessageBatchSize ?? 64,
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
                        logger.For<MongoDbAsyncResponseChannel>(), timeProvider: timeProvider);
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
            long startedSeq = 0,
            System.Diagnostics.Activity? activity = null)
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
                    activity
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

        /// <summary>A live local waiter per id — what a wake for the id needs to be kept at all.</summary>
        public void AddWaiters(params string[] correlationIds)
        {
            foreach (var correlationId in correlationIds)
                AddSubscription(correlationId, Subscription(correlationId).Instance);
        }

        /// <summary>
        /// What a successful LISTEN / change-stream open does (PostgreSQL and MongoDB only): the
        /// push wake carries delivery again, so the full-sweep throttle applies, and one full
        /// sweep is requested for whatever the gap hid.
        /// </summary>
        public void MarkWakeListenerEstablished()
        {
            if (_channelType != typeof(SqlServerAsyncResponseChannel))
                Invoke("OnWakeListenerEstablished");
        }

        /// <summary>The provider's resolved full-sweep throttle (<c>null</c> = sweep every tick).</summary>
        public TimeSpan? CurrentFullSweepInterval()
            => (TimeSpan?)Method("CurrentFullSweepInterval").Invoke(Channel, []);

        /// <summary>Skips the store's one-time schema validation, whose catalog queries a fake server cannot answer.</summary>
        public void MarkStoreCreated() => SetField(Field("_store").GetValue(Channel)!, "_created", true);

        /// <summary>PostgreSQL harness only: the channel's store, for its listen-loop timing seams.</summary>
        public PostgreSqlChannelSql PostgreSqlStore => (PostgreSqlChannelSql)Field("_store").GetValue(Channel)!;

        /// <summary>Whether a requested full sweep the outage breaker cut short is waiting for its retry.</summary>
        public bool RequestedSweepRetryArmed => (bool)Field("_requestedSweepRetryArmed").GetValue(Channel)!;

        /// <summary>The correlation ids with a backpressure rescan pending.</summary>
        public System.Collections.IDictionary BackpressureRescans
            => (System.Collections.IDictionary)Field("_backpressureRescans").GetValue(Channel)!;

        /// <summary>Overwrites one of the shared base's fields (a background loop's task, say).</summary>
        public void SetChannelField(string name, object? value) => Field(name).SetValue(Channel, value);

        /// <summary>
        /// Holds the correlation id's late-commit lookback window open for the next hour, as if
        /// its cursor had just moved, and lets the next pass revisit the whole window (a pass
        /// otherwise does at most once per poll interval — the harness's is 30 s). The window
        /// runs on the real monotonic clock (twice the lookback after the cursor last advanced),
        /// so a test that needs it open must not depend on how much real time passes between two
        /// sweeps on a loaded runner.
        /// </summary>
        public void HoldLookbackWindowOpen(string correlationId)
        {
            var scan = Scan(correlationId);
            scan.GetType().GetField("ForwardAdvancedAt")!.SetValue(
                scan,
                (long?)(System.Diagnostics.Stopwatch.GetTimestamp() + System.Diagnostics.Stopwatch.Frequency * 3600));
            scan.GetType().GetField("WindowRevisitedAt")!.SetValue(scan, null);
        }

        /// <summary>The correlation ids with a lookback-window rescan pending.</summary>
        public System.Collections.IDictionary LookbackRescans
            => (System.Collections.IDictionary)Field("_lookbackRescans").GetValue(Channel)!;

        /// <summary>
        /// Stamps the correlation id's last full lookback revisit <paramref name="ago"/> in the
        /// past (real monotonic clock), so the next pass's revisit throttle is decided by the stamp,
        /// not by how much real time the runner took between two sweeps.
        /// </summary>
        public void StampWindowRevisit(string correlationId, TimeSpan ago)
        {
            var scan = Scan(correlationId);
            scan.GetType().GetField("WindowRevisitedAt")!.SetValue(
                scan,
                (long?)(System.Diagnostics.Stopwatch.GetTimestamp() - (long)(ago.TotalSeconds * System.Diagnostics.Stopwatch.Frequency)));
        }

        /// <summary>The correlation id's lookback-window stamp: the Stopwatch timestamp the late-commit window runs from.</summary>
        public long? ForwardAdvancedAt(string correlationId)
        {
            var scan = Scan(correlationId);
            return (long?)scan.GetType().GetField("ForwardAdvancedAt")!.GetValue(scan);
        }

        /// <summary>How long an established wake listener must stay up before its failure starts the reconnect backoff over.</summary>
        public TimeSpan WakeListenerHealthyRun
        {
            get => (TimeSpan)Field("_wakeListenerHealthyRun").GetValue(Channel)!;
            set => Field("_wakeListenerHealthyRun").SetValue(Channel, value);
        }

        /// <summary>The correlation id's forward cursor: where the next pass continues from, and whether it had caught up.</summary>
        public (DateTimeOffset? CreatedAtUtc, Guid? Id, bool CaughtUp) ForwardCursor(string correlationId)
        {
            var scan = Scan(correlationId);
            var cursor = scan.GetType().GetField("Forward")!.GetValue(scan)!;
            return (
                (DateTimeOffset?)cursor.GetType().GetField("CreatedAtUtc")!.GetValue(cursor),
                (Guid?)cursor.GetType().GetField("Id")!.GetValue(cursor),
                (bool)scan.GetType().GetField("ForwardCaughtUp")!.GetValue(scan)!);
        }

        /// <summary>Work items waiting in the correlation id's executor (the one running excluded).</summary>
        public int PendingDispatches(string correlationId)
        {
            var entry = LiveExecutors[ChannelName(correlationId)]!;
            var executor = entry.GetType().GetProperty("Executor")!.GetValue(entry)!;
            return (int)executor.GetType().GetField("_pending", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(executor)!;
        }

        /// <summary>The correlation id's dispatch scan (its cursor state), which exists once a sweep has visited it.</summary>
        private object Scan(string correlationId)
        {
            var group = Subscriptions[correlationId]!;
            var table = Field("_dispatchScans").GetValue(Channel)!;
            var arguments = new object?[] { group, null };
            Assert.True((bool)table.GetType().GetMethod("TryGetValue")!.Invoke(table, arguments)!, $"No dispatch scan for {correlationId} yet.");
            return arguments[1]!;
        }

        /// <summary>Overwrites one option on the channel's live options object (read on every use).</summary>
        public void SetOption(string name, object value)
        {
            var options = Field("_options").GetValue(Channel)!;
            options.GetType().GetProperty(name)!.SetValue(options, value);
        }

        /// <summary>The serial-executor registry's live executors, keyed by channel name.</summary>
        public System.Collections.IDictionary LiveExecutors
        {
            get
            {
                var registry = Field("_executors").GetValue(Channel)!;
                return (System.Collections.IDictionary)registry.GetType()
                    .GetField("_executors", BindingFlags.Instance | BindingFlags.NonPublic)!
                    .GetValue(registry)!;
            }
        }

        /// <summary>The channel's listener cancellation source (non-null once the loops are started).</summary>
        public CancellationTokenSource? ListenerCancellation
            => (CancellationTokenSource?)Field("_listenerCts").GetValue(Channel);

        /// <summary>Overrides the harness's test-sized (2 ms) delivery-confirmation budget.</summary>
        public void ConfigureDeliveryConfirmation(TimeSpan timeout, TimeSpan pollInterval)
        {
            var options = Field("_options").GetValue(Channel)!;
            options.GetType().GetProperty("DeliveryConfirmationTimeout")!.SetValue(options, timeout);
            options.GetType().GetProperty("DeliveryConfirmationPollInterval")!.SetValue(options, pollInterval);
        }

        /// <summary>
        /// Starts the publish path's wait for a delivery confirmation and hands back the wait
        /// together with the completion the local dispatch loop would trip. The clock is stepped
        /// right after the wait reads "now" for the first time — the window a real step lands in.
        /// </summary>
        public (Task<bool> Acknowledged, TaskCompletionSource<bool> Delivered) BeginWaitForAcknowledgement(ForwardSteppingClock steppingClock)
        {
            var messageId = Guid.NewGuid();
            var confirmation = Method("BeginConfirmation").Invoke(Channel, [messageId])!;
            var pending = (System.Collections.Concurrent.ConcurrentDictionary<Guid, TaskCompletionSource<bool>>)Field("_pendingConfirmations").GetValue(Channel)!;
            steppingClock.StepAfterNextReading();
            var acknowledged = (Task<bool>)Method("WaitForAcknowledgementAsync").Invoke(Channel, [confirmation, CancellationToken.None])!;
            return (acknowledged, pending[messageId]);
        }

        /// <summary>
        /// Replaces the subscription's per-message dispatch delegate (<c>ProcessUnderContextAsync</c>,
        /// a <c>Func&lt;DbChannelMessage, Task&gt;</c> over the provider assembly's message type) with
        /// <paramref name="body"/>, so a test can wedge or observe delivery without an envelope.
        /// </summary>
        public void SetProcessHook(object subscription, Func<Task> body)
        {
            var property = subscription.GetType().GetProperty("ProcessUnderContextAsync", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!;
            var messageType = property.PropertyType.GetGenericArguments()[0];
            var hook = typeof(DbChannelSharedCoverageTests)
                .GetMethod(nameof(InvokeProcessHook), BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(messageType)
                .CreateDelegate(property.PropertyType, body);
            property.SetValue(subscription, hook);
        }

        /// <summary>The per-correlation serial executor the disposal drain has to get through.</summary>
        public SerialExecutorRegistry Executors => (SerialExecutorRegistry)Field("_executors").GetValue(Channel)!;

        public string ChannelName(string correlationId) => (string)Method("ChannelName").Invoke(Channel, [correlationId])!;


        public void Invoke(string name, params object?[] arguments) => Method(name).Invoke(Channel, arguments);

        /// <summary>The publish path's confirmation wait for <paramref name="messageId"/> (no local delivery ever trips it).</summary>
        public Task<bool> WaitForAcknowledgementAsync(Guid messageId)
        {
            var confirmation = Method("BeginConfirmation").Invoke(Channel, [messageId])!;
            return (Task<bool>)Method("WaitForAcknowledgementAsync").Invoke(Channel, [confirmation, CancellationToken.None])!;
        }

        /// <summary>The confirmation wait plus the recovery claim at its deadline; <c>false</c> = recovery won.</summary>
        public Task<bool> TryConfirmDeliveryAsync(Guid messageId)
        {
            var confirmation = Method("BeginConfirmation").Invoke(Channel, [messageId])!;
            return (Task<bool>)Method("TryConfirmDeliveryAsync").Invoke(Channel, [confirmation, CancellationToken.None])!;
        }

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

    /// <summary>
    /// The sweep-duration histogram facts. Serialized against the rest of the suite (fixpoint r2
    /// S11#16 h): the <see cref="System.Diagnostics.Metrics.MeterListener"/> they read is
    /// process-wide and filtered only by the channel tag, so the sweeps of MongoDB harnesses in
    /// parallel classes landed in it too — enough to satisfy a count, or to fail an absence.
    /// Nested only to reach the shared harness.
    /// </summary>
    [Collection(nameof(DbChannelSweepMetricsCollection))]
    public sealed class SweepMetrics
    {
        /// <summary>
        /// Fixpoint r1 S5#6: nothing measured the sweep, so the cliff past the confirmation budget
        /// was invisible. Each full sweep now records its duration on the AsyncResponse meter, and a
        /// sweep longer than half the confirmation budget logs a warning — at most once a minute.
        /// </summary>
        [Fact]
        public async Task FullSweep_RecordsItsDuration_AndWarnsOnceAMinuteWhenItNearsTheConfirmationBudget()
        {
            using var listener = SweepDurationListener(out var durations);

            await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30));
            harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromMilliseconds(40), pollInterval: TimeSpan.FromMilliseconds(1));
            harness.AddWaiters("corr");
            harness.MongoMessages!
                .Setup(collection => collection.FindAsync(
                    It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                    It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                    It.IsAny<CancellationToken>()))
                .Returns(async () =>
                {
                    // A slow store round trip, so the sweep outlasts half the 40 ms budget.
                    await Task.Delay(TimeSpan.FromMilliseconds(60));
                    return Cursor(new List<MongoChannelMessageDocument>());
                });

            await harness.InvokeAsync("DispatchPendingMessagesAsync", null, CancellationToken.None);
            await harness.InvokeAsync("DispatchPendingMessagesAsync", null, CancellationToken.None);

            Assert.Single(harness.Logger.Messages, message => message.Contains("full dispatch sweep over 1 correlation ids took", StringComparison.Ordinal));
            lock (durations)
                Assert.True(durations.Count(value => value >= 0.05) >= 2, $"Expected both sweeps' durations recorded; saw [{string.Join(", ", durations)}].");
        }

        /// <summary>
        /// Fixpoint r2 S5#12: only a tripped sweep was left out of the histogram and the
        /// slow-sweep warning, yet any failing sweep measured the failure — a connect or
        /// server-selection timeout per failed id — not the sweep: fewer than eight waiters never
        /// trip, and the warning then told an operator in a database outage to reduce the number
        /// of waiters. A sweep with any failure is no longer recorded. Three waiters here, each
        /// load failing transiently after more than half the budget. Red on the old code: the
        /// sweep was recorded and the warning logged.
        /// </summary>
        [Fact]
        public async Task FullSweep_ThatFailsWithoutTripping_IsNeitherRecordedNorReportedAsSlow()
        {
            using var listener = SweepDurationListener(out var durations);

            await using var harness = Harness.Create(Provider.MongoDb, false, TimeSpan.FromSeconds(30));
            harness.ConfigureDeliveryConfirmation(timeout: TimeSpan.FromMilliseconds(40), pollInterval: TimeSpan.FromMilliseconds(1));
            harness.AddWaiters("corr-0", "corr-1", "corr-2");
            harness.MongoMessages!
                .Setup(collection => collection.FindAsync(
                    It.IsAny<FilterDefinition<MongoChannelMessageDocument>>(),
                    It.IsAny<FindOptions<MongoChannelMessageDocument, MongoChannelMessageDocument>>(),
                    It.IsAny<CancellationToken>()))
                .Returns(async () =>
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(60));
                    throw new TimeoutException("server selection timed out");
                });

            var failure = await Assert.ThrowsAsync<AggregateException>(() => harness.InvokeAsync("DispatchPendingMessagesAsync", null, CancellationToken.None));

            Assert.Contains("every other id was still dispatched", failure.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(harness.Logger.Messages, message => message.Contains("full dispatch sweep over", StringComparison.Ordinal));
            lock (durations)
                Assert.Empty(durations);
        }

        /// <summary>Collects the MongoDB channel's <c>asyncresponse.channel.sweep.duration</c> measurements while it lives.</summary>
        private static System.Diagnostics.Metrics.MeterListener SweepDurationListener(out List<double> durations)
        {
            var collected = durations = [];
            var listener = new System.Diagnostics.Metrics.MeterListener
            {
                InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Meter.Name == AsyncResponseDiagnostics.MeterName && instrument.Name == "asyncresponse.channel.sweep.duration")
                        l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == "asyncresponse.channel" && Equals(tag.Value, "mongodb"))
                    {
                        lock (collected)
                            collected.Add(value);
                    }
                }
            });
            listener.Start();
            return listener;
        }
    }
}

/// <summary>Serializes <see cref="DbChannelSharedCoverageTests.SweepMetrics"/> against the rest of the suite.</summary>
[CollectionDefinition(nameof(DbChannelSweepMetricsCollection), DisableParallelization = true)]
public sealed class DbChannelSweepMetricsCollection;
