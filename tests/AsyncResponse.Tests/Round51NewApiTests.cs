using AsyncResponse.Channels.NATS;
using AsyncResponse.Channels.Redis;
using AsyncResponse.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round 51 pins over the option it introduced, <c>MaxRecoveryRegistrationsPerCorrelationId</c>
/// on the Redis and NATS channel options (the behavior pins that also compile against the pre-fix
/// tree live in <see cref="Round51RegressionTests"/>).
/// </summary>
public sealed class Round51NewApiTests
{
    private static RecoveryState Registration(string correlationId) => new() { CorrelationId = correlationId, RegistrationId = Guid.NewGuid() };

    [Fact]
    public void FanOutLimit_DefaultsTo64_AndABoundBelowOneFailsValidation()
    {
        Assert.Equal(64, new NatsAsyncResponseChannelOptions().MaxRecoveryRegistrationsPerCorrelationId);
        Assert.Equal(64, new RedisAsyncResponseOptions().MaxRecoveryRegistrationsPerCorrelationId);

        foreach (var bound in new[] { 0, -1 })
        {
            var nats = Assert.Throws<InvalidOperationException>(() => new NatsAsyncResponseChannelOptions { MaxRecoveryRegistrationsPerCorrelationId = bound }.Validate());
            Assert.Contains("NatsAsyncResponseChannelOptions.MaxRecoveryRegistrationsPerCorrelationId", nats.Message, StringComparison.Ordinal);
            var redis = Assert.Throws<InvalidOperationException>(() => new RedisAsyncResponseOptions { MaxRecoveryRegistrationsPerCorrelationId = bound }.Validate());
            Assert.Contains("RedisAsyncResponseOptions.MaxRecoveryRegistrationsPerCorrelationId", redis.Message, StringComparison.Ordinal);
        }

        new NatsAsyncResponseChannelOptions { MaxRecoveryRegistrationsPerCorrelationId = 1 }.Validate();
        new RedisAsyncResponseOptions { MaxRecoveryRegistrationsPerCorrelationId = 1 }.Validate();
    }

    public static TheoryData<string> LookupSteps => ["session", "barrier", "majority read"];

    /// <summary>
    /// Whichever step of the MongoDB lookup fails — the session start, the barrier, or the majority
    /// read behind an acknowledged barrier (a step-down in between) — surfaces as
    /// <see cref="RecoveryStateUnconfirmedException"/>, carrying the correlation id and the
    /// driver's error. A raw driver error from any of them would be escalated by the ingress (the
    /// critic's second-pass catch: the first cut wrapped the barrier only).
    /// </summary>
    [Theory]
    [MemberData(nameof(LookupSteps))]
    public async Task MongoLookup_AnyUnconfirmedStep_SurfacesAsRecoveryStateUnconfirmed(string step)
    {
        await using var harness = new Round51RegressionTests.MongoConsistencyHarness();
        harness.CurrentView.Add(JsonSerializer.Serialize(Round51RegressionTests.Registration("r51-unconfirmed")));
        Exception failure = step switch
        {
            "session" => new TimeoutException("A timeout occurred after 30000ms selecting a server."),
            "barrier" => MongoReplicationTimeouts.Write(),
            _ => new MongoDB.Driver.MongoNotPrimaryException(MongoReplicationTimeouts.Connection, new MongoDB.Bson.BsonDocument("find", "recovery"), new MongoDB.Bson.BsonDocument { ["ok"] = 0, ["code"] = 10107, ["errmsg"] = "not primary" })
        };
        switch (step)
        {
            case "session":
                harness.SessionFailure = failure;
                break;
            case "barrier":
                harness.BarrierFailure = failure;
                break;
            default:
                harness.MajorityReadFailure = failure;
                break;
        }

        var thrown = await Assert.ThrowsAsync<RecoveryStateUnconfirmedException>(() => harness.RecoveryStore.GetAllAsync("r51-unconfirmed"));

        Assert.Equal("r51-unconfirmed", thrown.CorrelationId);
        Assert.Same(failure, thrown.InnerException);
        Assert.Equal(step == "session" ? 0 : 1, harness.Barriers.Count);
    }

    /// <summary>
    /// The pre-commit critic's scenario, end to end through the broker ingress: the barrier fails
    /// for the whole in-process ladder (four attempts), and the replica set recovers just after.
    /// The ingress must propagate — not escalate through <c>SetException</c>, whose own lookup would
    /// then succeed and run the FAILURE callback, with the driver's error, for a response the
    /// worker produced successfully — so the redelivery resumes the flow with the real response.
    /// </summary>
    [Fact]
    public async Task IngressRedelivery_AfterAnUnconfirmedLookup_ResumesWithTheRealResponse_NotTheFailureCallback()
    {
        await using var harness = new Round51RegressionTests.MongoConsistencyHarness();
        harness.CurrentView.Add(JsonSerializer.Serialize(Round51RegressionTests.Registration("r51-ingress")));
        for (var i = 0; i < 4; i++)
            harness.NextBarrierFailures.Enqueue(MongoReplicationTimeouts.Write());
        var time = new VirtualTimeProvider();
        var ingress = new AsyncResponseIngress(
            harness.Channel,
            harness.Channel,
            new WorkerJobExecutor(new ServiceCollection().BuildServiceProvider().GetRequiredService<IServiceScopeFactory>(), NullLogger<WorkerJobExecutor>.Instance),
            new AsyncResponseContextPropagation([]),
            NullLogger<AsyncResponseIngress>.Instance,
            _timeProvider: time);

        var delivery = ingress.HandleResponseMessageAsync("""{"Status":2}""", "r51-ingress");
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while (!delivery.IsCompleted && DateTime.UtcNow < deadline)
        {
            if (time.NextTimerDueAt is not null)
                time.Advance(TimeSpan.FromSeconds(2));
            else
                await Task.Delay(5);
        }

        // Bounded, so a regression that leaves the delivery pending fails here instead of hanging.
        await Assert.ThrowsAsync<RecoveryStateUnconfirmedException>(() => delivery.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Equal(4, harness.Barriers.Count);
        Assert.Equal(0, harness.Spy.Failed + harness.Spy.Resumed);
        Assert.Equal(0, harness.Deletes);

        await ingress.HandleResponseMessageAsync("""{"Status":2}""", "r51-ingress");

        Assert.Equal(1, harness.Spy.Resumed);
        Assert.Equal(0, harness.Spy.Failed);
        Assert.Equal(1, harness.Deletes);
    }

    /// <summary>
    /// The pacing's shape: the first pause is drawn under 2 ms, each loss doubles the ceiling, and
    /// the ceiling stops at 250 ms; the budget is 30 attempts, so an operation that loses every
    /// attempt pauses for at most about 5.75 s in total.
    /// </summary>
    [Fact]
    public void ContentionPauses_DoubleFrom2Milliseconds_UpTo250Milliseconds()
    {
        Assert.Equal(30, RecoveryStateContention.MaxAttempts);
        for (var sample = 0; sample < 200; sample++)
        {
            Assert.InRange(RecoveryStateContention.Pause(1), TimeSpan.Zero, TimeSpan.FromMilliseconds(2));
            Assert.InRange(RecoveryStateContention.Pause(4), TimeSpan.Zero, TimeSpan.FromMilliseconds(16));
            Assert.InRange(RecoveryStateContention.Pause(29), TimeSpan.Zero, TimeSpan.FromMilliseconds(250));
        }

        Assert.Contains(Enumerable.Range(0, 200).Select(_ => RecoveryStateContention.Pause(29)), pause => pause > TimeSpan.FromMilliseconds(128));
        var worstCase = Enumerable.Range(1, RecoveryStateContention.MaxAttempts - 1)
            .Sum(attempt => Math.Min(RecoveryStateContention.MaxDelay.TotalMilliseconds, RecoveryStateContention.BaseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1)));
        Assert.InRange(worstCase, 5_000, 6_000);
    }

    [Fact]
    public async Task FanOutLimit_AConfiguredBound_IsTheOneEnforced()
    {
        var kv = new FakeNatsKvStore();
        var nats = new NatsRecoveryStateStore(
            kv,
            Options.Create(new NatsAsyncResponseChannelOptions { MaxRecoveryRegistrationsPerCorrelationId = 2 }),
            NullLogger<NatsRecoveryStateStore>.Instance,
            new TestTimeProvider());
        var redis = new StatefulRedis();
        var redisStore = redis.CreateStore(new TestTimeProvider(), options => options.MaxRecoveryRegistrationsPerCorrelationId = 2);

        foreach (var store in new IRecoveryStateStore[] { nats, redisStore })
        {
            await store.SaveAsync("r51-custom", Registration("r51-custom"), TimeSpan.FromMinutes(5));
            await store.SaveAsync("r51-custom", Registration("r51-custom"), TimeSpan.FromMinutes(5));

            var refused = await Assert.ThrowsAsync<InvalidOperationException>(
                () => store.SaveAsync("r51-custom", Registration("r51-custom"), TimeSpan.FromMinutes(5)));

            Assert.Contains("already has 2 live recovery registration(s)", refused.Message, StringComparison.Ordinal);
            Assert.Equal(2, (await store.GetAllAsync("r51-custom")).Count);
        }
    }
}
