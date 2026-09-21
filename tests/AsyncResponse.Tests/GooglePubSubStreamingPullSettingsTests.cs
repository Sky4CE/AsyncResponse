using AsyncResponse.Transports.GooglePubSub;
using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The Pub/Sub streaming-pull knobs the package used to leave to the SDK: the ack-extension
/// ceiling (the transport's in-flight ceiling, advertised through
/// <see cref="IWorkerTransportInFlightLimit"/>), the connection count and the flow-control limits —
/// their defaults, their validation, and that they actually reach the SDK client.
/// </summary>
public class GooglePubSubStreamingPullSettingsTests
{
    private static readonly SubscriptionName Subscription = SubscriptionName.FromProjectSubscription("project-a", "workers");

    [Fact]
    public void SubscriberBuilder_AppliesTheConfiguredCeiling_ConnectionCount_AndFlowControl()
    {
        var builder = GooglePubSubSubscriberService.CreateSubscriberBuilder(
            Subscription,
            new GooglePubSubSubscriberOptions
            {
                MaxTotalAckExtension = TimeSpan.FromHours(6),
                ClientCount = 3,
                MaxOutstandingMessages = 25,
                MaxOutstandingBytes = 5_000_000
            });

        Assert.Equal(Subscription, builder.SubscriptionName);
        Assert.Equal(EmulatorDetection.EmulatorOrProduction, builder.EmulatorDetection);
        Assert.Equal(3, builder.ClientCount);
        Assert.Equal(TimeSpan.FromHours(6), builder.Settings.MaxTotalAckExtension);
        Assert.Equal(25, builder.Settings.FlowControlSettings.MaxOutstandingElementCount);
        Assert.Equal(5_000_000, builder.Settings.FlowControlSettings.MaxOutstandingByteCount);
    }

    [Fact]
    public void SubscriberBuilder_Defaults_AreExplicit_OneConnection_AndTheSdkLimits()
    {
        var options = new GooglePubSubSubscriberOptions();

        var builder = GooglePubSubSubscriberService.CreateSubscriberBuilder(Subscription, options);

        // Nothing is left to an SDK default the package does not know about: the SDK's own
        // ClientCount default is the CPU count, and it applies the flow-control limits per
        // connection — N connections lease N × 1000 jobs while running 1000.
        Assert.Equal(1, builder.ClientCount);
        Assert.Equal(TimeSpan.FromMinutes(60), builder.Settings.MaxTotalAckExtension);
        Assert.Equal(1000, builder.Settings.FlowControlSettings.MaxOutstandingElementCount);
        Assert.Equal(100_000_000, builder.Settings.FlowControlSettings.MaxOutstandingByteCount);

        // The documented "SDK default" values really are the pinned SDK's: a package bump that
        // moves them must be noticed here, not by an operator.
        Assert.Equal(SubscriberClient.DefaultMaxTotalAckExtension, options.MaxTotalAckExtension);
        Assert.Equal(SubscriberClient.DefaultFlowControlSettings.MaxOutstandingElementCount, options.MaxOutstandingMessages);
        Assert.Equal(SubscriberClient.DefaultFlowControlSettings.MaxOutstandingByteCount, options.MaxOutstandingBytes);
    }

    [Fact]
    public void SubscriberBuilder_AckAfterEnqueue_StaysBoundedToTheBackgroundQueue_AndStillGetsCeilingAndConnectionCount()
    {
        var options = new GooglePubSubSubscriberOptions
        {
            MaxTotalAckExtension = TimeSpan.FromHours(2),
            ClientCount = 2,
            MaxOutstandingMessages = 25
        }.UseAckAfterEnqueue(backgroundWorkerCount: 4, backgroundQueueCapacity: 64);

        var builder = GooglePubSubSubscriberService.CreateSubscriberBuilder(Subscription, options);

        // Early ACK keeps its documented bound — the background queue capacity, no byte limit —
        // and ignores the ack-after-handler flow-control options.
        Assert.Equal(64, builder.Settings.FlowControlSettings.MaxOutstandingElementCount);
        Assert.Null(builder.Settings.FlowControlSettings.MaxOutstandingByteCount);
        Assert.Equal(2, builder.ClientCount);
        Assert.Equal(TimeSpan.FromHours(2), builder.Settings.MaxTotalAckExtension);
    }

    [Theory]
    [MemberData(nameof(BoundaryOptions))]
    public async Task SubscriberBuilder_BoundaryValues_AreAcceptedByTheRealSdkClient(GooglePubSubSubscriberOptions options)
    {
        // Whatever the package's validator lets through must also pass the SDK's own build-time
        // validation: a value only the SDK rejects fails inside the supervised retry loop — an
        // endless rebuild-and-fail cycle — instead of at startup. Built against an insecure dummy
        // endpoint: gRPC channels connect lazily, so nothing leaves the process.
        GooglePubSubMessageDispatcher.ValidateOptions(
            new GooglePubSubAsyncResponseOptions(),
            options,
            GooglePubSubSubscriberRole.Worker);
        var builder = GooglePubSubSubscriberService.CreateSubscriberBuilder(Subscription, options);
        builder.EmulatorDetection = EmulatorDetection.None;
        builder.Endpoint = "localhost:1";
        builder.ChannelCredentials = ChannelCredentials.Insecure;

        var client = await builder.BuildAsync(TestContext.Current.CancellationToken);

        await client.DisposeAsync();
    }

    public static TheoryData<GooglePubSubSubscriberOptions> BoundaryOptions()
        => new()
        {
            new GooglePubSubSubscriberOptions(),
            new GooglePubSubSubscriberOptions
            {
                MaxTotalAckExtension = GooglePubSubOptionsValidator.MinimumMaxTotalAckExtension,
                ClientCount = 1,
                MaxOutstandingMessages = 1,
                MaxOutstandingBytes = 1
            },
            new GooglePubSubSubscriberOptions
            {
                MaxTotalAckExtension = AsyncResponseChannelOptions.MaxTimerBackedTimeout,
                ClientCount = 2,
                MaxOutstandingMessages = int.MaxValue,
                MaxOutstandingBytes = long.MaxValue
            },
            new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(1, 1)
        };

    [Theory]
    [MemberData(nameof(InvalidStreamingPullOptions))]
    public void ValidateOptions_RejectsOutOfRangeStreamingPullSettings_InBothAckModes(
        Action<GooglePubSubSubscriberOptions> misconfigure,
        string expectedKnob)
    {
        foreach (var options in new[]
                 {
                     new GooglePubSubSubscriberOptions(),
                     new GooglePubSubSubscriberOptions().UseAckAfterEnqueue(1, 8)
                 })
        {
            misconfigure(options);

            var ex = Assert.Throws<InvalidOperationException>(() => GooglePubSubMessageDispatcher.ValidateOptions(
                new GooglePubSubAsyncResponseOptions(),
                options,
                GooglePubSubSubscriberRole.Worker));

            Assert.Contains($"WorkerSubscriber.{expectedKnob}", ex.Message, StringComparison.Ordinal);
        }
    }

    public static TheoryData<Action<GooglePubSubSubscriberOptions>, string> InvalidStreamingPullOptions()
        => new()
        {
            { o => o.MaxTotalAckExtension = TimeSpan.Zero, nameof(GooglePubSubSubscriberOptions.MaxTotalAckExtension) },
            { o => o.MaxTotalAckExtension = TimeSpan.FromSeconds(-1), nameof(GooglePubSubSubscriberOptions.MaxTotalAckExtension) },
            // Below the client's 60 s ack deadline the setting cannot speed up redelivery; it would
            // only shrink the ceiling durable flows plan their in-process waits against.
            { o => o.MaxTotalAckExtension = TimeSpan.FromSeconds(59), nameof(GooglePubSubSubscriberOptions.MaxTotalAckExtension) },
            // The SDK arms a timer with it per pulled batch.
            { o => o.MaxTotalAckExtension = AsyncResponseChannelOptions.MaxTimerBackedTimeout + TimeSpan.FromMilliseconds(1), nameof(GooglePubSubSubscriberOptions.MaxTotalAckExtension) },
            { o => o.ClientCount = 0, nameof(GooglePubSubSubscriberOptions.ClientCount) },
            { o => o.ClientCount = 257, nameof(GooglePubSubSubscriberOptions.ClientCount) },
            { o => o.MaxOutstandingMessages = 0, nameof(GooglePubSubSubscriberOptions.MaxOutstandingMessages) },
            { o => o.MaxOutstandingBytes = 0, nameof(GooglePubSubSubscriberOptions.MaxOutstandingBytes) }
        };

    [Fact]
    public async Task SubscriberService_OutOfRangeStreamingPullSetting_FailsHostStartup_BeforeCreatingAClient()
    {
        var factoryCalled = false;
        var options = new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            ResponseSubscriptionId = "responses"
        };
        options.ResponseSubscriber.ClientCount = 0;
        var subscriber = new GooglePubSubResponseIngressSubscriber(
            Options.Create(options),
            Mock.Of<IAsyncResponseIngress>(),
            NullLogger<GooglePubSubResponseIngressSubscriber>.Instance,
            (_, _) =>
            {
                factoryCalled = true;
                return Task.FromResult<IGooglePubSubSubscriberClient>(null!);
            });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => subscriber.StartAsync(CancellationToken.None));

        Assert.Contains("ResponseSubscriber.ClientCount", ex.Message, StringComparison.Ordinal);
        Assert.False(factoryCalled);
    }

    [Fact]
    public async Task WorkerTransport_AdvertisesTheAckExtensionCeiling_AsItsInFlightLimit()
    {
        // Past MaxTotalAckExtension the client stops extending the ack deadline and Pub/Sub hands
        // the SAME job to another consumer while the first handler is still running. Durable flows
        // park in process on this transport (no native delayed delivery), so the engine has to be
        // able to see the ceiling to keep each in-process wait inside it.
        await using var transport = new GooglePubSubWorkerTransport(Options.Create(new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            WorkerTopicId = "worker-topic"
        }));

        var limit = Assert.IsAssignableFrom<IWorkerTransportInFlightLimit>(transport);
        Assert.Equal(SubscriberClient.DefaultMaxTotalAckExtension, limit.MaxInFlightDuration);
    }

    [Fact]
    public async Task WorkerTransport_InFlightLimit_FollowsTheWorkerSubscriberOption_ThroughTheRegisteredTransport()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddAsyncResponse().WithGooglePubSubTransport(options =>
        {
            options.ProjectId = "project-a";
            options.WorkerTopicId = "worker-topic";
            options.WorkerSubscriber.MaxTotalAckExtension = TimeSpan.FromHours(8);
            // The response subscriber's ceiling is not the worker queue's.
            options.ResponseSubscriber.MaxTotalAckExtension = TimeSpan.FromMinutes(5);
        });
        // The transport is IAsyncDisposable-only, so the container must be disposed asynchronously.
        await using var provider = services.BuildServiceProvider();

        // Resolved the way the flow engine sees it: through IWorkerTransport.
        var limit = Assert.IsAssignableFrom<IWorkerTransportInFlightLimit>(provider.GetRequiredService<IWorkerTransport>());

        Assert.Equal(TimeSpan.FromHours(8), limit.MaxInFlightDuration);
    }

    [Fact]
    public async Task WorkerTransport_InFlightLimit_IsNull_WhenTheWorkerSubscriberAcksAtEnqueue()
    {
        // Early ACK settles the delivery before the handler runs: nothing is held against the
        // ack-extension ceiling, so there is none to plan around.
        var options = new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            WorkerTopicId = "worker-topic"
        };
        options.WorkerSubscriber.UseAckAfterEnqueue(2, 64);
        await using var transport = new GooglePubSubWorkerTransport(Options.Create(options));

        Assert.Null(((IWorkerTransportInFlightLimit)transport).MaxInFlightDuration);
    }

    [Fact]
    public void WorkerTransport_Ctor_RejectsAnAckExtensionCeilingItCouldNotAdvertise()
    {
        var options = new GooglePubSubAsyncResponseOptions
        {
            ProjectId = "project-a",
            WorkerTopicId = "worker-topic"
        };
        options.WorkerSubscriber.MaxTotalAckExtension = TimeSpan.Zero;

        var ex = Assert.Throws<InvalidOperationException>(() => new GooglePubSubWorkerTransport(Options.Create(options)));

        Assert.Contains("WorkerSubscriber.MaxTotalAckExtension", ex.Message, StringComparison.Ordinal);
    }
}
