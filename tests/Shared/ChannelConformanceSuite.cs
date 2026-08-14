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

    [Fact]
    public async Task Contract_CorrelationIdsDifferingOnlyInCaseAreDistinctWaiters()
    {
        // Correlation ids are compared ORDINALLY by contract, so 'x' and 'X' are two conversations.
        // A store that disagrees delivers one waiter's response to the other: SQL Server columns
        // inherit the database collation, and the common default is case-INSENSITIVE, so a query
        // for the upper-case id returns the lower-case row. Both waiters then completed with the
        // same payload — one of them belonging to somebody else's request.
        await using var harness = await CreateHarnessAsync();
        var lower = NewCorrelationId("case-x").ToLowerInvariant();
        var upper = lower.ToUpperInvariant();
        Assert.NotEqual(lower, upper, StringComparer.Ordinal);

        await using var lowerWaiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(lower, timeout: WaiterTimeout);
        await using var upperWaiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(upper, timeout: WaiterTimeout);

        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "for-lower"), lower);

        var lowerResult = await lowerWaiter.ResponseTask.WaitAsync(WaitBudget);
        Assert.Equal("for-lower", lowerResult.Message);

        await Task.Delay(SettleDelay);
        Assert.False(upperWaiter.ResponseTask.IsCompleted);

        // …and the reverse direction, so a case-folding store cannot pass by answering only one.
        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "for-upper"), upper);
        var upperResult = await upperWaiter.ResponseTask.WaitAsync(WaitBudget);
        Assert.Equal("for-upper", upperResult.Message);
    }

    [Theory]
    // Over the portable length: rejected or silently truncated at the first relational write.
    [InlineData("length")]
    // Surrounding spaces: SQL Server pads the shorter operand of `=` under EVERY collation, binary
    // ones included, so this id and its trimmed form are ONE key to the database while the library
    // compares them ordinally — the wrong waiter can be handed the response.
    [InlineData("trailing-space")]
    [InlineData("leading-space")]
    // An unpaired surrogate: every UTF-8 encoder substitutes U+FFFD for it rather than failing, so
    // this id and one carrying a literal U+FFFD produce identical bytes — one NATS subject, one
    // recovery key, one stored value — for two conversations the engine treats as different.
    // A SHAPE, not a literal: xUnit serializes theory arguments and that round trip would repair
    // the string before the test saw it.
    [InlineData("unpaired-surrogate")]
    public async Task Contract_ANonPortableCorrelationIdIsRefusedWhenSubscribing(string violation)
    {
        // Every channel applies the same contract at the same boundary, whatever its own storage
        // could tolerate: an id the SQL Server channel cannot key on must not be accepted by the
        // Redis one either, or the same application code stops working when the channel changes.
        // Subscribing takes the id from the application, so a violation is an argument error —
        // thrown before any subscription or recovery state exists to leak.
        await using var harness = await CreateHarnessAsync();
        var valid = NewCorrelationId("portable");
        var correlationId = violation switch
        {
            "length" => valid + new string('x', AsyncResponseChannelOptions.MaxCorrelationIdLength),
            "trailing-space" => valid + " ",
            "unpaired-surrogate" => valid + '\ud800',
            _ => " " + valid
        };

        await Assert.ThrowsAnyAsync<ArgumentException>(
            () => harness.Subscriber.CreateResponseWaiter<ConformanceResult>(correlationId, timeout: WaiterTimeout));
    }

    [Fact]
    public async Task Contract_EveryPublishEntryPointRefusesANonPortableCorrelationId()
    {
        // To a database 'abc ' and 'abc' are ONE key, while the library compares them ordinally, so
        // a stored padded row is a response sitting in another conversation's mailbox. Every publish
        // entry point refuses to write one — but the PUBLIC ones throw, because they are called by
        // application code and swallowing a typo there just leaves a waiter to time out later with
        // nothing at the call site to explain why. Only the raw path, which exists to serve inbound
        // broker messages, drops instead: throwing there fails the delivery and the transport
        // redelivers an id that can never become valid.
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("padded");
        var padded = correlationId + " ";
        var overlong = correlationId + new string('x', AsyncResponseChannelOptions.MaxCorrelationIdLength);
        var illFormed = correlationId + '\ud800';

        await using var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout);

        await Assert.ThrowsAnyAsync<ArgumentException>(
            () => harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "smuggled"), padded));
        await Assert.ThrowsAnyAsync<ArgumentException>(
            () => harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "too long"), overlong));
        await Assert.ThrowsAnyAsync<ArgumentException>(
            () => harness.Publisher.SetException(new InvalidOperationException("smuggled failure"), padded));
        await Assert.ThrowsAnyAsync<ArgumentException>(
            () => harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "ill-formed"), illFormed));

        // The raw path takes the ingress's answer: acknowledged, logged, never written.
        await harness.RawPublisher.SetRawResponseJson("""{"Status":2,"Message":"smuggled raw"}""", padded);

        await Task.Delay(SettleDelay);
        Assert.False(waiter.ResponseTask.IsCompleted);

        // The waiter is still armed for its own id — the refusals cost it nothing.
        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "mine"), correlationId);
        var result = await waiter.ResponseTask.WaitAsync(WaitBudget);
        Assert.Equal("mine", result.Message);
    }

    [Fact]
    public async Task Contract_ACorrelationIdWithASupplementaryCharacterRoundTrips()
    {
        // The false-positive guard for the rule above, and the reason it is worded in terms of
        // WELL-FORMEDNESS rather than "no surrogates": a real supplementary character — an emoji,
        // most CJK extension text — IS a surrogate pair, and must survive subscribe, publish, and
        // whatever subject or key the channel derives from it.
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("astral") + "-\U0001F600";

        await using var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout);
        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "astral"), correlationId);

        var result = await waiter.ResponseTask.WaitAsync(WaitBudget);
        Assert.Equal("astral", result.Message);
    }

    [Fact]
    public async Task Contract_ABlankCorrelationIdPublishIsANoOp_NotAThrow()
    {
        // The deliberate carve-out beside the fact above, and the reason the two are not one rule.
        // A blank id is not a caller's typo to hand back — there is no id to complain about and
        // nothing to route — so it keeps the log-and-skip it has always had, on every channel.
        // Without this the natural simplification is "any bad id throws", which would break every
        // adapter that legitimately hands the publisher whatever it failed to extract.
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("blank-sibling");
        await using var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout);

        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "nowhere"), " ");
        await harness.RawPublisher.SetRawResponseJson("""{"Status":2,"Message":"nowhere raw"}""", " ");
        await harness.Publisher.SetException(new InvalidOperationException("nowhere failure"), " ");

        await Task.Delay(SettleDelay);
        Assert.False(waiter.ResponseTask.IsCompleted);
    }

    [Fact]
    public async Task Contract_ACorrelationIdExactlyAtThePortableLimitIsAccepted()
    {
        // The other half of a bound: an id AT the documented maximum has to work, on both sides of
        // the channel. An off-by-one here would reject ids the contract promises to carry, and
        // every test above uses short ids, so nothing else would catch it.
        await using var harness = await CreateHarnessAsync();
        var prefix = NewCorrelationId("at-cap");
        var correlationId = prefix + new string('c', AsyncResponseChannelOptions.MaxCorrelationIdLength - prefix.Length);
        Assert.Equal(AsyncResponseChannelOptions.MaxCorrelationIdLength, correlationId.Length);

        await using var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout);
        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "at the cap"), correlationId);

        var result = await waiter.ResponseTask.WaitAsync(WaitBudget);
        Assert.Equal("at the cap", result.Message);
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

    // ----- f3/g3/h3. Multi-response streams -----
    //
    // The library's core scenario: the remote side reports several progress checkpoints on one
    // correlation id before its terminal result. These facts pin the stream behaviors around that
    // shape — a live waiter riding out a whole checkpoint stream, stragglers (duplicate terminals,
    // late checkpoints) after the wait already ended on either path, and the crash interleaving
    // where the stream is split across the waiter's death.

    [Fact]
    public async Task Contract_ProgressStream_MultipleCheckpointsThenTerminal_CompleteLiveWaiter()
    {
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("stream");
        var observed = new ConcurrentQueue<string?>();

        await using var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId,
            payload =>
            {
                observed.Enqueue(payload.Message);
                return new ValueTask<bool>(payload.Status == ConformanceStatus.Completed);
            },
            WaiterTimeout);

        // Every checkpoint is delivered to the SAME waiter and none may end the wait — a
        // non-terminal delivery must leave the subscription fully armed for the next message.
        await harness.Publisher.SetResponse(Result(ConformanceStatus.Running, "checkpoint-1"), correlationId);
        await harness.Publisher.SetResponse(Result(ConformanceStatus.Running, "checkpoint-2"), correlationId);
        await harness.Publisher.SetResponse(Result(ConformanceStatus.Running, "checkpoint-3"), correlationId);
        await EventuallyAsync(
            () => Task.FromResult(observed.Contains("checkpoint-3")),
            "the checkpoint stream reaches the Until predicate");
        Assert.False(waiter.ResponseTask.IsCompleted);

        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "final"), correlationId);
        var result = await waiter.ResponseTask.WaitAsync(WaitBudget);

        Assert.Equal("final", result.Message);
        Assert.Contains("checkpoint-1", observed);
        Assert.Contains("checkpoint-2", observed);
        Assert.Contains("checkpoint-3", observed);
    }

    [Fact]
    public async Task Contract_LiveFanout_MixedPayloadTypes_EachWaiterReceivesItsDeclaredType()
    {
        // Shared-correlation fan-out with DIFFERENT declared payload types: broker channels give
        // each waiter its own JSON materialization of the envelope, so a waiter never faults just
        // because the publisher's CLR type differs from its own. The in-memory channel must match
        // that contract instead of casting the publisher's instance across unrelated DTO types.
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("mixed-fanout");

        await using var resultWaiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout);
        await using var mirrorWaiter = await harness.Subscriber.CreateResponseWaiter<ConformanceMirrorResult>(
            correlationId, timeout: WaiterTimeout);

        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "both"), correlationId);

        var result = await resultWaiter.ResponseTask.WaitAsync(WaitBudget);
        Assert.Equal(ConformanceStatus.Completed, result.Status);
        Assert.Equal("both", result.Message);

        var mirror = await mirrorWaiter.ResponseTask.WaitAsync(WaitBudget);
        Assert.Equal(ConformanceStatus.Completed, mirror.Status);
        Assert.Equal("both", mirror.Message);
    }

    [Fact]
    public async Task Contract_LiveFanout_SameType_EachWaiterGetsItsOwnWireMaterialization()
    {
        // Same-type fan-out must be wire-true too: every waiter receives its OWN materialization
        // of the declared-type wire JSON — never the publisher's live instance. Sharing the
        // instance aliases one mutable object across the fan-out and leaks [JsonIgnore] state
        // that no broker-backed channel can deliver; the in-memory channel must not diverge from
        // what Redis/NATS/database waiters receive byte-for-byte.
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("same-type-fanout");

        await using var firstWaiter = await harness.Subscriber.CreateResponseWaiter<ConformanceLocalStateResult>(
            correlationId, timeout: WaiterTimeout);
        await using var secondWaiter = await harness.Subscriber.CreateResponseWaiter<ConformanceLocalStateResult>(
            correlationId, timeout: WaiterTimeout);

        var published = new ConformanceLocalStateResult { Marker = 7, LocalHint = "in-process-only" };
        await harness.Publisher.SetResponse(published, correlationId);

        var first = await firstWaiter.ResponseTask.WaitAsync(WaitBudget);
        var second = await secondWaiter.ResponseTask.WaitAsync(WaitBudget);

        Assert.Equal(7, first.Marker);
        Assert.Equal(7, second.Marker);
        // Wire-excluded state never crosses, and no waiter aliases the publisher's (or another
        // waiter's) instance.
        Assert.Null(first.LocalHint);
        Assert.Null(second.LocalHint);
        Assert.NotSame(published, first);
        Assert.NotSame(published, second);
        Assert.NotSame(first, second);
    }

    [Fact]
    public async Task Contract_LiveFanout_PolymorphicDeclaredBase_PreservesDiscriminatorForEveryWaiter()
    {
        // The polymorphic sharpening of the mixed-type fan-out contract: the publisher declares a
        // [JsonPolymorphic] base, so the wire representation carries the type discriminator. Every
        // waiter — including one bound to a DIFFERENT compatible polymorphic contract — must
        // receive its own materialization of that discriminated payload, exactly as broker
        // envelopes (which serialize the declared base) deliver it.
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("poly-fanout");

        await using var ownWaiter = await harness.Subscriber.CreateResponseWaiter<ConformancePolyBase>(
            correlationId, timeout: WaiterTimeout);
        await using var mirrorWaiter = await harness.Subscriber.CreateResponseWaiter<ConformanceMirrorPolyBase>(
            correlationId, timeout: WaiterTimeout);

        await harness.Publisher.SetResponse<ConformancePolyBase>(
            new ConformancePolyResult { Message = "poly-both" }, correlationId);

        var own = await ownWaiter.ResponseTask.WaitAsync(WaitBudget);
        var ownResult = Assert.IsType<ConformancePolyResult>(own);
        Assert.Equal("poly-both", ownResult.Message);

        var mirror = await mirrorWaiter.ResponseTask.WaitAsync(WaitBudget);
        var mirrorResult = Assert.IsType<ConformanceMirrorPolyResult>(mirror);
        Assert.Equal("poly-both", mirrorResult.Message);
    }

    [Fact]
    public async Task Contract_StragglersAfterLiveCompletion_AreDroppedAndCorrelationIdStaysReusable()
    {
        // Broker redelivery and slow remotes produce stragglers: a duplicate of the terminal
        // response, or a progress checkpoint that was in flight when the wait completed. After a
        // live completion the waiter's own recovery registration is consumed, so both stragglers
        // must be dropped benignly — no recovery dispatch, no exception, and the correlation id
        // remains fully usable for the next operation.
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("stragglers");

        await using (var first = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout))
        {
            await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "first"), correlationId);
            Assert.Equal("first", (await first.ResponseTask.WaitAsync(WaitBudget)).Message);
        }

        await EventuallyAsync(
            async () => await harness.Probe.CountActiveSubscribersAsync(correlationId) == 0,
            "the completed waiter's subscription is fully cleaned up");
        await EventuallyAsync(
            async () => (await harness.RecoveryStateStore.GetAllAsync(correlationId)).Count == 0,
            "the completed waiter's recovery registration is consumed");

        // The stragglers: a redelivered terminal and a late checkpoint. Nothing is listening and
        // no registration exists — both must be dropped without dispatching anything.
        await harness.RawPublisher.SetRawResponseJson("""{"Status":2,"Message":"first"}""", correlationId);
        await harness.RawPublisher.SetRawResponseJson("""{"Status":1,"Message":"late checkpoint"}""", correlationId);
        await Task.Delay(SettleDelay);
        Assert.Empty(harness.Spy.Resumed);
        Assert.Empty(harness.Spy.Failures);

        // The correlation id is not poisoned: a fresh waiter receives the NEXT response — never a
        // straggler that predates it.
        await using var second = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId, timeout: WaiterTimeout);
        await harness.Publisher.SetResponse(Result(ConformanceStatus.Completed, "second"), correlationId);
        Assert.Equal("second", (await second.ResponseTask.WaitAsync(WaitBudget)).Message);
    }

    [Fact]
    public async Task Contract_CrashBetweenProgressAndTerminal_RecoveryResumesWithTerminalPayload()
    {
        // The full crash interleaving: the stream starts against a LIVE waiter (a checkpoint is
        // observed by its Until predicate), the waiting process then dies mid-stream, and the rest
        // of the stream — another checkpoint, then the terminal success — arrives on the lost
        // path. The armed recovery registration must ride out the post-crash checkpoint and route
        // the terminal response, exactly once, with the materialized payload.
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("crash-mid-stream");
        await ArmRecoveryCallbacksAsync(harness, correlationId);
        var observed = new ConcurrentQueue<string?>();

        var waiter = await harness.Subscriber.CreateResponseWaiter<ConformanceResult>(
            correlationId,
            payload =>
            {
                observed.Enqueue(payload.Message);
                return new ValueTask<bool>(payload.Status == ConformanceStatus.Completed);
            },
            WaiterTimeout);

        // Live phase: the checkpoint reaches the waiter, not the recovery spy.
        await harness.Publisher.SetResponse(Result(ConformanceStatus.Running, "progress-live"), correlationId);
        await EventuallyAsync(
            () => Task.FromResult(observed.Contains("progress-live")),
            "the live checkpoint reaches the Until predicate");
        Assert.Empty(harness.Spy.Resumed);
        Assert.Empty(harness.Spy.Failures);

        // The crash: the waiter's subscription dies mid-wait. (Its own registration goes with it;
        // the independently armed registration — the flow's recovery arm — survives, exactly like
        // a real crashed process whose persisted registration outlives it.)
        var responseTask = waiter.ResponseTask;
        await waiter.DisposeAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => responseTask.WaitAsync(WaitBudget));
        await EventuallyAsync(
            async () => await harness.Probe.CountActiveSubscribersAsync(correlationId) == 0,
            "the crashed waiter's subscription is fully gone");

        // Post-crash phase, via the raw ingress like a real broker delivery: the checkpoint must
        // keep the registration armed, and the terminal must resume exactly once, typed.
        await harness.RawPublisher.SetRawResponseJson("""{"Status":1,"Message":"progress-lost"}""", correlationId);
        await Task.Delay(SettleDelay);
        Assert.Empty(harness.Spy.Resumed);
        Assert.Empty(harness.Spy.Failures);
        Assert.NotEmpty(await harness.RecoveryStateStore.GetAllAsync(correlationId));

        await harness.RawPublisher.SetRawResponseJson("""{"Status":2,"Message":"terminal-lost"}""", correlationId);
        await EventuallyAsync(
            () => Task.FromResult(harness.Spy.Resumed.Count == 1),
            "the terminal response routes through the surviving registration");
        var resumed = Assert.IsType<ConformanceResult>(Assert.Single(harness.Spy.Resumed));
        Assert.Equal(ConformanceStatus.Completed, resumed.Status);
        Assert.Equal("terminal-lost", resumed.Message);
        Assert.Empty(harness.Spy.Failures);
    }

    [Fact]
    public async Task Contract_MessagesAfterConsumedTerminal_AreDroppedWithoutCallbacks()
    {
        // Once the terminal response consumed the recovery registration, redelivered terminals and
        // late checkpoints have nothing to route against — recovery is at-least-once, and once the
        // registration is gone the contract is a benign drop: never a second resume, never a
        // failure dispatch.
        await using var harness = await CreateHarnessAsync();
        var correlationId = NewCorrelationId("post-consume");
        await ArmRecoveryCallbacksAsync(harness, correlationId);

        await harness.RawPublisher.SetRawResponseJson("""{"Status":2,"Message":"terminal"}""", correlationId);
        await EventuallyAsync(
            () => Task.FromResult(harness.Spy.Resumed.Count == 1),
            "the terminal response resumes the flow");
        await EventuallyAsync(
            async () => (await harness.RecoveryStateStore.GetAllAsync(correlationId)).Count == 0,
            "the consumed registration is deleted");

        await harness.RawPublisher.SetRawResponseJson("""{"Status":2,"Message":"terminal"}""", correlationId);
        await harness.RawPublisher.SetRawResponseJson("""{"Status":1,"Message":"late checkpoint"}""", correlationId);

        await Task.Delay(SettleDelay);
        var resumed = Assert.IsType<ConformanceResult>(Assert.Single(harness.Spy.Resumed));
        Assert.Equal("terminal", resumed.Message);
        Assert.Empty(harness.Spy.Failures);
        Assert.Empty(await harness.RecoveryStateStore.GetAllAsync(correlationId));
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
/// A second payload type sharing <see cref="ConformanceResult"/>'s wire shape: what a sibling
/// service waiting on the same correlation id would declare. Lets the suite pin mixed-type
/// shared-correlation fan-out — every waiter receives its own declared type, however the
/// publisher spelled the payload.
/// </summary>
public sealed class ConformanceMirrorResult : IAsyncResponsePayload
{
    public ConformanceStatus Status { get; set; }
    public string? Message { get; set; }

    public RecoveryAction OnRecovery() => Status switch
    {
        ConformanceStatus.Completed => RecoveryAction.Resume,
        ConformanceStatus.Running => RecoveryAction.KeepWaiting,
        _ => RecoveryAction.Fail,
    };
}

/// <summary>
/// Same-type fan-out probe: wire-carried state plus in-process-only (<c>[JsonIgnore]</c>) state.
/// Pins that every waiter gets its own wire materialization — the ignored member must never
/// cross, and no waiter may alias the publisher's live instance.
/// </summary>
public sealed class ConformanceLocalStateResult : IAsyncResponsePayload
{
    public int Marker { get; set; }

    [System.Text.Json.Serialization.JsonIgnore]
    public string? LocalHint { get; set; }

    public RecoveryAction OnRecovery() => RecoveryAction.Resume;
}

/// <summary>
/// A polymorphic payload contract published by its declared base: the wire carries the
/// discriminator, and every waiter must get a faithful materialization of the derived type.
/// </summary>
[System.Text.Json.Serialization.JsonPolymorphic(TypeDiscriminatorPropertyName = "$kind")]
[System.Text.Json.Serialization.JsonDerivedType(typeof(ConformancePolyResult), "result")]
public abstract class ConformancePolyBase : IAsyncResponsePayload
{
    public string? Message { get; set; }

    // Virtual so the derived override genuinely participates in interface dispatch.
    public virtual RecoveryAction OnRecovery() => RecoveryAction.Fail;
}

public sealed class ConformancePolyResult : ConformancePolyBase
{
    public override RecoveryAction OnRecovery() => RecoveryAction.Resume;
}

/// <summary>
/// A second, compatible polymorphic contract (same discriminator name and value) — what a sibling
/// service waiting on the same correlation id would declare.
/// </summary>
[System.Text.Json.Serialization.JsonPolymorphic(TypeDiscriminatorPropertyName = "$kind")]
[System.Text.Json.Serialization.JsonDerivedType(typeof(ConformanceMirrorPolyResult), "result")]
public abstract class ConformanceMirrorPolyBase : IAsyncResponsePayload
{
    public string? Message { get; set; }

    // Virtual so the derived override genuinely participates in interface dispatch.
    public virtual RecoveryAction OnRecovery() => RecoveryAction.Fail;
}

public sealed class ConformanceMirrorPolyResult : ConformanceMirrorPolyBase
{
    public override RecoveryAction OnRecovery() => RecoveryAction.Resume;
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
