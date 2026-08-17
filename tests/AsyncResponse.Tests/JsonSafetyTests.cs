using System.Text.Json;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// The defensive JSON guard used on broker-ingress payloads: empty bodies and HTML error pages are
/// rejected with a diagnosable <see cref="InvalidDataException"/>, and a malformed JSON body is
/// re-wrapped with its size and JSON position — never its content — rather than surfacing a bare
/// <see cref="JsonException"/>.
/// </summary>
public class JsonSafetyTests
{
    // A token that cannot plausibly appear in any framework diagnostic, so finding it anywhere in
    // the thrown exception chain proves the body itself was copied there.
    private const string Secret = "SUPERSECRET-tenant-acme-bearer-9f3c1d2e4b5a6789";
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

    // ---------------------------------------------------------------------------------------
    // docs/security.md: "The library never logs a message body. At every log level, including
    // Debug." These guards throw at the ingress, where AsyncResponseIngress logs the exception
    // and — on the response path — republishes it to the waiter through SetException. A payload
    // prefix in the message defeated that guarantee on both routes at once.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public void MalformedJson_DoesNotEchoTheBodyIntoTheExceptionChain()
    {
        var body = $$"""{"token":"{{Secret}}","status": }""";

        var ex = Assert.Throws<InvalidDataException>(() => JsonSafety.SafeDeserialize<OperationResult>(body));

        AssertNoBodyAnywhereIn(ex);
    }

    [Fact]
    public void MalformedJson_NonGenericDeserialize_DoesNotEchoTheBodyIntoTheExceptionChain()
    {
        var body = $$"""{"token":"{{Secret}}","status": }""";

        var ex = Assert.Throws<InvalidDataException>(() => JsonSafety.SafeDeserialize(body, typeof(OperationResult)));

        AssertNoBodyAnywhereIn(ex);
    }

    [Fact]
    public void HtmlBody_DoesNotEchoTheMarkupIntoTheExceptionChain()
    {
        // The realistic source: a proxy or auth gateway answering in place of the service, with
        // session and infrastructure detail in the page it returns.
        var body = $"<html><body>502 Bad Gateway — upstream session {Secret}</body></html>";

        var ex = Assert.Throws<InvalidDataException>(() => JsonSafety.SafeDeserialize<OperationResult>(body));

        AssertNoBodyAnywhereIn(ex);
        Assert.Contains("HTML when JSON was expected", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ParseFailure_StillReportsSizeAndPositionSoItStaysDiagnosable()
    {
        // Dropping the body must not cost the diagnosis: the fault is still locatable by how big
        // the payload was and where the reader stopped.
        var body = $$"""{"token":"{{Secret}}","status": }""";

        var ex = Assert.Throws<InvalidDataException>(() => JsonSafety.SafeDeserialize<OperationResult>(body));

        Assert.Contains($"{body.Length} UTF-16 code units", ex.Message, StringComparison.Ordinal);
        Assert.Contains("byte position", ex.Message, StringComparison.Ordinal);
        Assert.IsType<JsonException>(ex.InnerException);
    }

    /// <summary>
    /// Walks the whole chain, because the wrapper is what gets logged: <c>LogError(ex, …)</c>
    /// renders inner exceptions too, so a body smuggled into an inner message leaks just as
    /// surely as one in the outer.
    /// </summary>
    private static void AssertNoBodyAnywhereIn(Exception exception)
    {
        for (Exception? current = exception; current is not null; current = current.InnerException)
        {
            Assert.DoesNotContain(Secret, current.Message, StringComparison.Ordinal);
            // The surrounding JSON too, not just the value: a prefix that stopped short of the
            // secret would still have disclosed the envelope's shape.
            Assert.DoesNotContain("\"token\"", current.Message, StringComparison.Ordinal);
        }
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
