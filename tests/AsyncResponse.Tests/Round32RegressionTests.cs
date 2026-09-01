using AsyncResponse.DurableFlows.SqlServer;
using AsyncResponse.Testing;
using AsyncResponse.Transports.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>Regression pins for the round-32 review's core findings.</summary>
public sealed class Round32RegressionTests
{
    [Theory]
    [InlineData("{app}")]           // the idiomatic Redis Cluster co-location prefix
    [InlineData("{tenant-a}:jobs")]
    [InlineData("plain")]
    public void RedisWorkerPublishDedupKey_SharesTheWorkerStreamsClusterSlot(string keyPrefix)
    {
        // Regression: the marker wrapped the WHOLE stream name in braces, so a name that already
        // carried a hash tag nested them and Redis read `{app` as the marker's tag — a different
        // slot from the stream's `app`, and CROSSSLOT on every publish. The rule is written out
        // here independently of the schema's own helper so the fact holds the code to the server's
        // behaviour rather than to itself.
        var schema = new RedisTransportKeySchema(new RedisAsyncResponseTransportOptions { KeyPrefix = keyPrefix });
        var stream = schema.WorkerStream.ToString()!;
        var marker = schema.WorkerPublishDedupKey("publish-1").ToString()!;

        Assert.Equal(RedisSlotKey(stream), RedisSlotKey(marker));
    }

    /// <summary>Redis Cluster's hash-tag rule: first <c>{</c>, first <c>}</c> after it, non-empty — else the whole key.</summary>
    private static string RedisSlotKey(string key)
    {
        var open = key.IndexOf('{');
        if (open < 0)
            return key;
        var close = key.IndexOf('}', open + 1);
        return close > open + 1 ? key.Substring(open + 1, close - open - 1) : key;
    }

    [Theory]
    [InlineData("a{}b")] // an empty tag: Redis hashes the whole key, so no marker key can share its slot
    [InlineData("a}b")]  // a stray closing brace: any wrapped marker hashes on `a`
    public void RedisValidator_RejectsAWorkerStreamWhoseBracesFormNoHashTag(string workerStream)
    {
        var options = new RedisAsyncResponseTransportOptions { WorkerStream = workerStream };

        var ex = Assert.Throws<InvalidOperationException>(() => RedisTransportOptionsValidator.ValidateCommon(options));

        Assert.Contains("hash tag", ex.Message, StringComparison.Ordinal);
        // A well-formed tag stays accepted.
        RedisTransportOptionsValidator.ValidateCommon(new RedisAsyncResponseTransportOptions { WorkerStream = "{tenant}:jobs" });
    }

    [Fact]
    public async Task StartupValidator_RejectsASecondDurableFlowOptionsRegistration()
    {
        // Regression: the engine resolves DurableFlowOptions through GetRequiredService — the LAST
        // registration — while the validator enumerated them and judged the FIRST. A provider
        // registration (its own options type) followed by the generic overload (the base type, to
        // adjust a common setting) registered two forwards resolving two DIFFERENT instances for
        // the same store type, which the store-count check collapsed into one — so the validator
        // green-lit a StateExpiry that was not the one in effect. (Two registrations of the SAME
        // options type converge on one IOptions instance and are not the hazard.)
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddAsyncResponse()
            .WithInMemoryChannel()
            .WithInMemoryTransport()
            .WithSqlServerDurableFlows(options =>
            {
                options.ConnectionString = "Server=localhost;Database=unused;User ID=sa;Password=unused;TrustServerCertificate=True";
                options.StateExpiry = TimeSpan.FromDays(7);
            })
            .WithDurableFlows<SqlServerFlowStateStore>(options => options.StateExpiry = TimeSpan.FromMinutes(1));
        await using var provider = services.BuildServiceProvider();
        var validator = provider.GetServices<IHostedService>().OfType<AsyncResponseStartupValidator>().Single();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => validator.StartAsync(CancellationToken.None));

        Assert.Contains("registered 2 times", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void InMemoryChannel_RejectsANegativeRemoteStackTraceCap()
    {
        // Regression: the in-memory channel inherited MaxRemoteStackTraceLength and consumed it,
        // but validated only the shared knobs — RemoteStackTrace.Cap treats a non-positive cap as
        // "no cap", so the bound was silently disabled here while all five wire channels reject
        // the same configuration at startup. A config green on the test harness then failed the
        // real deployment.
        using var services = new ServiceCollection().BuildServiceProvider();

        var ex = Assert.Throws<InvalidOperationException>(() => new InMemoryAsyncResponseChannel(
            services.GetRequiredService<IServiceScopeFactory>(),
            new InMemoryRecoveryStateStore(),
            Options.Create(new InMemoryAsyncResponseOptions { MaxRemoteStackTraceLength = -1 }),
            new AsyncResponseContextPropagation([]),
            NullLogger<InMemoryAsyncResponseChannel>.Instance));

        Assert.Contains(nameof(InMemoryAsyncResponseOptions.MaxRemoteStackTraceLength), ex.Message, StringComparison.Ordinal);
        // Zero (uncapped, the documented escape hatch) stays accepted.
        new InMemoryAsyncResponseOptions { MaxRemoteStackTraceLength = 0 }.Validate();
    }

    [Fact]
    public async Task SimulateRestart_DisposingTheAbandonedWaiter_KeepsTheRecoveryRegistration()
    {
        // Regression: AbandonAsync set the cleanup-started flag but never latched the cleanup
        // task, so the zombie waiter's LATER disposal — a flow disposing its waiter after the
        // cancelled wait, or any `await using` caller — still ran the cleanup that deletes the
        // recovery registration the simulated crash exists to leave behind. The late response
        // then found no waiter and no registration and was dropped.
        var audit = new RecordingRecoveryAudit();
        await using var harness = await AsyncResponseTestHarness.StartAsync(options =>
            options.ConfigureServices = services => services.AddSingleton<IRecoveryAudit>(audit));

        var subscriber = harness.Services.GetRequiredService<IRecoverableAsyncResponseSubscriber>();
        const string correlationId = "order-9";
        var waiter = await subscriber.CreateRecoverableResponseWaiter<OperationResult>(
            correlationId,
            resumeCallback: CallbackExpressionConverter.ToReflectionCall<IRecoveryAudit>(
                target => target.ResumedAsync("order-9", Placeholder.Payload<OperationResult>()!, Placeholder.CorrelationId())));

        await harness.SimulateRestartAsync();
        await waiter.DisposeAsync();

        await harness.PublishAsync(new OperationResult { Status = OperationStatus.Completed, Message = "late" }, correlationId);

        Assert.Equal([$"order-9:{OperationStatus.Completed}:order-9"], audit.Resumed);
    }
}
