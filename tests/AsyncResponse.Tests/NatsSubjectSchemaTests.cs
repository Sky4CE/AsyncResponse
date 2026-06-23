using AsyncResponse.Channels.NATS;
using Xunit;

namespace AsyncResponse.Tests;

public class NatsSubjectSchemaTests
{
    [Theory]
    [InlineData("8f14e45f- ceea-467a-9575-1234567890ab")]
    [InlineData("simple")]
    [InlineData("with:colon/and+slash=padding")]
    [InlineData("tenant.123|order#42")]
    [InlineData("ünïcödé-Δοκιμή-本")]
    public void Encode_RoundTrips_AndProducesNatsSafeTokens(string correlationId)
    {
        var encoded = NatsSubjectSchema.Encode(correlationId);

        // URL-safe Base64 without padding: legal in both NATS subject tokens and KV keys.
        Assert.DoesNotContain('+', encoded);
        Assert.DoesNotContain('/', encoded);
        Assert.DoesNotContain('=', encoded);
        Assert.DoesNotContain('.', encoded);
        Assert.All(encoded, c => Assert.True(char.IsAsciiLetterOrDigit(c) || c is '-' or '_'));

        Assert.Equal(correlationId, NatsSubjectSchema.Decode(encoded));
        Assert.Equal(correlationId, NatsSubjectSchema.CorrelationIdFromRecoveryKey(encoded));
    }

    [Fact]
    public void RecoveryKey_EqualsEncode()
        => Assert.Equal(NatsSubjectSchema.Encode("corr-a"), NatsSubjectSchema.RecoveryKey("corr-a"));

    [Fact]
    public void ResponseSubject_UsesPrefixAndEncodedCorrelationId()
    {
        var schema = new NatsSubjectSchema("myapp");
        var subject = schema.ResponseSubject("corr-a");

        Assert.StartsWith("myapp.response.", subject);
        Assert.Equal($"myapp.response.{NatsSubjectSchema.Encode("corr-a")}", subject);
    }

    [Fact]
    public void Decode_ReturnsNullForUndecodableToken()
    {
        Assert.Null(NatsSubjectSchema.Decode("!!!not-base64!!!"));
        Assert.Null(NatsSubjectSchema.Decode(""));
    }

    [Fact]
    public void CorrelationIdFromRecoveryKey_ReturnsKeyVerbatimWhenUndecodable()
        => Assert.Equal("!!!", NatsSubjectSchema.CorrelationIdFromRecoveryKey("!!!"));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Encode_Throws_OnNullOrWhitespace(string? value)
        => Assert.ThrowsAny<ArgumentException>(() => NatsSubjectSchema.Encode(value!));
}
