using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The defensive JSON guard used on broker-ingress payloads: empty bodies and HTML error pages are
/// rejected with a diagnosable <see cref="InvalidDataException"/>, and a malformed JSON body is
/// re-wrapped with its offending prefix rather than surfacing a bare <see cref="JsonException"/>.
/// </summary>
public class JsonSafetyTests
{
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\n\t ")]
    public void EmptyOrWhitespaceBody_ThrowsInvalidData(string body)
    {
        var ex = Assert.Throws<InvalidDataException>(() => JsonSafety.SafeDeserialize<OperationResult>(body));
        Assert.Contains("Empty message body", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void HtmlBody_ThrowsInvalidDataWithPrefix()
    {
        var ex = Assert.Throws<InvalidDataException>(
            () => JsonSafety.SafeDeserialize<OperationResult>("  <html><body>502 Bad Gateway</body></html>"));

        Assert.Contains("HTML when JSON was expected", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MalformedJson_ThrowsInvalidDataWrappingTheJsonException()
    {
        var ex = Assert.Throws<InvalidDataException>(
            () => JsonSafety.SafeDeserialize<OperationResult>("""{"Status": }"""));

        Assert.Contains("Failed to parse JSON payload", ex.Message, StringComparison.Ordinal);
        Assert.IsType<JsonException>(ex.InnerException);
    }

    [Fact]
    public void MalformedJson_NonGenericDeserialize_ThrowsInvalidDataWrappingTheJsonException()
    {
        var ex = Assert.Throws<InvalidDataException>(
            () => JsonSafety.SafeDeserialize("""{"Status": }""", typeof(OperationResult)));

        Assert.Contains("Failed to parse JSON payload", ex.Message, StringComparison.Ordinal);
        Assert.IsType<JsonException>(ex.InnerException);
    }

    [Fact]
    public void ValidJson_DeserializesCaseInsensitivelyByDefault()
    {
        // Default options are case-insensitive, mirroring how brokers may emit camelCase.
        var result = JsonSafety.SafeDeserialize<OperationResult>("""{"status":2,"message":"done"}""");

        Assert.NotNull(result);
        Assert.Equal(OperationStatus.Completed, result!.Status);
        Assert.Equal("done", result.Message);
    }

    [Fact]
    public void ProvidedOptions_AreHonored()
    {
        // A caller can opt out of case-insensitivity; "status" then no longer binds to "Status".
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = false };

        var result = JsonSafety.SafeDeserialize<OperationResult>("""{"status":2}""", options);

        Assert.NotNull(result);
        Assert.Equal(OperationStatus.Unknown, result!.Status); // unbound → enum default
    }

    [Fact]
    public void NonGenericDeserialize_UsesDefaultCaseInsensitiveOptions()
    {
        var result = Assert.IsType<OperationResult>(
            JsonSafety.SafeDeserialize("""{"status":2,"message":"done"}""", typeof(OperationResult)));

        Assert.Equal(OperationStatus.Completed, result.Status);
        Assert.Equal("done", result.Message);
    }
}
