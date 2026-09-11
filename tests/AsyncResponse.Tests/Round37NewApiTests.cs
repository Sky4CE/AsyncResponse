using AsyncResponse.Transports.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Round-37 pins over API the round introduced — they do not compile against the pre-fix tree, so
/// they live apart from the behavior pins in <see cref="Round37RegressionTests"/>.
/// </summary>
public sealed class Round37NewApiTests
{
    // ---------- F2: WorkerJobTooLargeException and the estimator behind the producer-side check ----------

    [Fact]
    public async Task EnqueueWorkerAsync_OverTheBudget_ThrowsWorkerJobTooLargeException_WithTheMeasurement()
    {
        var transport = new Round37RegressionTests.CapturingWorkerTransport();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IWorkerTransport>(transport);
        services.AddAsyncResponse(o => o.MaxInboundMessageChars = 4096).WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();
        var builder = provider.GetRequiredService<IAsyncResponseBuilder>();

        var ex = await Assert.ThrowsAsync<WorkerJobTooLargeException>(() =>
            builder.EnqueueWorkerAsync<Round37RegressionTests.IR37Probe>(probe => probe.RunAsync(new string('x', 8192))));

        Assert.Equal(4096, ex.Limit);
        Assert.True(ex.SerializedLength > 8192, $"SerializedLength {ex.SerializedLength} should cover the 8192-character argument.");
        Assert.Contains("MaxInboundMessageChars", ex.Message, StringComparison.Ordinal);
        Assert.Empty(transport.Published);
    }

    [Fact]
    public async Task DurableFlowStart_OverTheBudget_SurfacesWorkerJobTooLargeException_Unwrapped()
    {
        var transport = new Round37RegressionTests.CapturingWorkerTransport();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IWorkerTransport>(transport);
        services.AddAsyncResponse(o => o.MaxInboundMessageChars = 4096).WithInMemoryChannel().WithInMemoryDurableFlows();
        await using var provider = services.BuildServiceProvider();

        // Not DurableFlowNotDispatchedException: that one means "retry the start", and this start
        // fails the same way every time.
        await Assert.ThrowsAsync<WorkerJobTooLargeException>(() =>
            provider.GetRequiredService<IDurableFlows>().StartAsync<Round37RegressionTests.R37Flow, Round37RegressionTests.R37Input>(
                new Round37RegressionTests.R37Input(new string('x', 8192))));
        Assert.Equal(0, transport.Attempts);
    }

    [Fact]
    public async Task NoBudget_PublishesAnythingTheTransportTakes()
    {
        var transport = new Round37RegressionTests.CapturingWorkerTransport();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IWorkerTransport>(transport);
        services.AddAsyncResponse(o => o.MaxInboundMessageChars = null).WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();

        await provider.GetRequiredService<IAsyncResponseBuilder>()
            .EnqueueWorkerAsync<Round37RegressionTests.IR37Probe>(probe => probe.RunAsync(new string('x', 9_000_000)));
        Assert.Single(transport.Published);
    }

    public static TheoryData<string, WorkerJobEnvelope> EstimableEnvelopes()
    {
        static WorkerJobEnvelope Envelope(params object?[] values) => new()
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "AsyncResponse.Tests.Round37RegressionTests+IR37Probe",
                MethodName = "RunAsync",
                Params = values.Select(CallbackParam.ForValue).ToArray()
            },
            CorrelationId = "corr-é中\U0001F600"
        };

        return new TheoryData<string, WorkerJobEnvelope>
        {
            { "ascii", Envelope("plain ascii text") },
            { "quotes and backslashes", Envelope(new string('"', 500) + new string('\\', 500)) },
            { "non-ascii and emoji", Envelope(new string('中', 400) + string.Concat(Enumerable.Repeat("\U0001F600", 200))) },
            { "control characters", Envelope(new string((char)1, 300) + "\r\n\t") },
            { "html-sensitive", Envelope(new string('<', 200) + new string('&', 200) + new string('+', 200)) },
            { "scalars", Envelope(int.MaxValue, long.MinValue, double.MaxValue, decimal.MaxValue, Guid.NewGuid(), DateTime.MaxValue, DateTimeOffset.MinValue, TimeSpan.MaxValue, true, 'q', null) },
            {
                "context and reply target",
                new WorkerJobEnvelope
                {
                    Call = new ReflectionCallDto
                    {
                        ServiceInterfaceFullName = "S",
                        MethodName = "M",
                        Params = [CallbackParam.ForPlaceholder(PlaceholderType.CorrelationId), CallbackParam.ForValue("v")]
                    },
                    Context = new Dictionary<string, string> { ["tenant-ü"] = new string('"', 100), ["trace"] = "00-abc-01" },
                    ReplyTarget = new AsyncResponseReplyTarget
                    {
                        Name = "default",
                        Transport = "kafka",
                        Address = new string('中', 50),
                        Properties = { ["group"] = "g\"1" }
                    },
                    NotBeforeUtc = DateTime.UtcNow,
                    LastRedelayRemaining = TimeSpan.FromMinutes(1),
                    RedelayStallCount = 2
                }
            }
        };
    }

    [Theory]
    [MemberData(nameof(EstimableEnvelopes))]
    public void UpperBoundEstimate_NeverUndercountsTheSerializedEnvelope(string label, WorkerJobEnvelope envelope)
    {
        // The estimate is what lets the hot path skip a second serialization; it is only safe if
        // it never says "fits" for an envelope that does not.
        Assert.True(AsyncResponseBuilderBase.TryEstimateUpperBound(envelope, out var upperBound), label);
        var actual = AsyncResponseJson.Serialize(envelope).Length;
        Assert.True(upperBound >= actual, $"{label}: estimate {upperBound} undercounts the serialized {actual}.");
    }

    [Fact]
    public void UpperBoundEstimate_DeclinesAnArgumentItCannotBound()
    {
        var envelope = new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "S",
                MethodName = "M",
                Params = [CallbackParam.ForValue(new Round37RegressionTests.R37Input("x"))]
            }
        };

        Assert.False(AsyncResponseBuilderBase.TryEstimateUpperBound(envelope, out _));
    }

    // ---------- F4: the overload form of the indeterminate contract and its counter ----------

    [Fact]
    public void IndeterminateDeliveryException_OverloadForm_CarriesTheBufferedCount()
    {
        var ex = new AsyncResponseIndeterminateDeliveryException("corr", 1024);

        Assert.Equal("corr", ex.CorrelationId);
        Assert.Equal(1024, ex.BufferedMessages);
        Assert.Contains("1024", ex.Message, StringComparison.Ordinal);
        Assert.Contains("indeterminate", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, new AsyncResponseIndeterminateDeliveryException("corr", TimeSpan.FromSeconds(1)).BufferedMessages);
    }

    [Fact]
    public void RecordWaiterOverload_IncrementsTheOverloadedWaitsCounter_TaggedByChannel()
    {
        var measurements = new List<(string Instrument, long Value, string? Channel)>();
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == AsyncResponseDiagnostics.MeterName && instrument.Name == "asyncresponse.channel.overloaded_waits")
                    l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            string? channel = null;
            foreach (var tag in tags)
            {
                if (tag.Key == "channel")
                    channel = tag.Value?.ToString();
            }

            lock (measurements)
                measurements.Add((instrument.Name, value, channel));
        });
        listener.Start();

        AsyncResponseDiagnostics.RecordWaiterOverload("redis");

        lock (measurements)
        {
            var measurement = Assert.Single(measurements);
            Assert.Equal(("asyncresponse.channel.overloaded_waits", 1L, "redis"), measurement);
        }
    }

    // ---------- F7: the new Kafka knob ----------

    [Fact]
    public void KafkaSubscriberOptions_DetachHandlerAfter_DefaultsToOneSecond()
        => Assert.Equal(TimeSpan.FromSeconds(1), new KafkaSubscriberOptions().DetachHandlerAfter);
}
