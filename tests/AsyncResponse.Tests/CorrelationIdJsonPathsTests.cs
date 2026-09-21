using AsyncResponse.Transports.AzureServiceBus;
using AsyncResponse.Transports.GooglePubSub;
using AsyncResponse.Transports.Kafka;
using AsyncResponse.Transports.MongoDB;
using AsyncResponse.Transports.NATS;
using AsyncResponse.Transports.PostgreSQL;
using AsyncResponse.Transports.RabbitMQ;
using AsyncResponse.Transports.Redis;
using AsyncResponse.Transports.SqlServer;
using AsyncResponse.Transports.SQS;
using System.Reflection;
using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// <c>src/Transports/Shared/CorrelationIdJsonPaths.cs</c> is source-linked into all 10 transport
/// packages, so the body walk is exercised here by reflection against EVERY compiled copy (the
/// type name is identical in each, so it cannot be referenced directly). Each transport's own test
/// class separately pins its provider-specific fast path (header/property/attribute lookup).
/// </summary>
public sealed class CorrelationIdJsonPathsTests
{
    public static TheoryData<Type> TransportAssemblies =>
    [
        typeof(AzureServiceBusAsyncResponseOptions),
        typeof(GooglePubSubAsyncResponseOptions),
        typeof(KafkaAsyncResponseTransportOptions),
        typeof(MongoDbAsyncResponseTransportOptions),
        typeof(NatsAsyncResponseTransportOptions),
        typeof(PostgreSqlAsyncResponseTransportOptions),
        typeof(RabbitMqAsyncResponseOptions),
        typeof(RedisAsyncResponseTransportOptions),
        typeof(SqlServerAsyncResponseTransportOptions),
        typeof(SqsAsyncResponseOptions)
    ];

    /// <summary>
    /// The premise, pinned on this runtime: an ESCAPED lone surrogate is well-formed JSON — Parse
    /// accepts it — and only transcoding it to a string fails, with
    /// <see cref="InvalidOperationException"/> rather than the <see cref="JsonException"/> the
    /// walker already guards.
    /// </summary>
    [Fact]
    public void EscapedLoneSurrogate_ParsesButCannotBeTranscoded()
    {
        using var value = JsonDocument.Parse("""{"CorrelationId":"\ud800"}""");
        Assert.Throws<InvalidOperationException>(() => value.RootElement.GetProperty("CorrelationId").GetString());

        using var name = JsonDocument.Parse("""{"\ud800":1}""");
        Assert.Throws<InvalidOperationException>(() =>
        {
            foreach (var property in name.RootElement.EnumerateObject())
                _ = property.Name;
        });
    }

    /// <summary>
    /// Regression: that InvalidOperationException escaped the extractor — right next to the
    /// duplicate-key case that already reports "not found" — and turned an unroutable inbound
    /// message into a handler failure, which on RabbitMQ's default MaxDeliveryAttempts = 0 requeues
    /// forever. An id that cannot be transcoded is simply not in this body.
    /// </summary>
    [Theory]
    [MemberData(nameof(TransportAssemblies))]
    public void Extract_ReturnsNull_WhenTheIdIsAnEscapedLoneSurrogate(Type marker)
        => Assert.Null(Extract(marker, """{"CorrelationId":"\ud800"}""", ["CorrelationId"]));

    /// <summary>The walk transcodes every property NAME of a walked object, not only the wanted one.</summary>
    [Theory]
    [MemberData(nameof(TransportAssemblies))]
    public void Extract_ReturnsNull_WhenAWalkedObjectHasALoneSurrogatePropertyName(Type marker)
        => Assert.Null(Extract(marker, """{"\udc00":"noise","CorrelationId":"abc"}""", ["CorrelationId"]));

    /// <summary>An embedded-JSON candidate is transcoded before it is sniffed for a leading brace.</summary>
    [Theory]
    [MemberData(nameof(TransportAssemblies))]
    public void Extract_ReturnsNull_WhenAnEmbeddedJsonCandidateHoldsALoneSurrogate(Type marker)
        => Assert.Null(Extract(marker, """{"Envelope":"{\ud800}"}""", ["Envelope.CorrelationId"]));

    /// <summary>
    /// Unresolvable is per PATH, like every other miss: a later configured path that never touches
    /// the untranscodable text still resolves.
    /// </summary>
    [Theory]
    [MemberData(nameof(TransportAssemblies))]
    public void Extract_FallsThroughToALaterPath_PastALoneSurrogate(Type marker)
        => Assert.Equal(
            "from-second-path",
            Extract(
                marker,
                """{"First":{"CorrelationId":"\ud800"},"Second":{"CorrelationId":"from-second-path"}}""",
                ["First.CorrelationId", "Second.CorrelationId"]));

    private static string? Extract(Type marker, string json, string[] paths)
    {
        var method = marker.Assembly
            .GetType("AsyncResponse.Transports.CorrelationIdJsonPaths", throwOnError: true)!
            .GetMethod("Extract", BindingFlags.Public | BindingFlags.Static)!;
        try
        {
            return (string?)method.Invoke(null, [json, paths]);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            // Surface what the extractor actually threw, not reflection's wrapper.
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }
    }
}
