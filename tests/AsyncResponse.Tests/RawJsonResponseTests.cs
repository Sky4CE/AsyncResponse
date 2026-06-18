using Xunit;

namespace AsyncResponse.Tests;

public class RawJsonResponseTests
{
    [Fact]
    public void Deserialize_CachesUntypedSingleTypedAndMultipleTypedPayloads()
    {
        var raw = new RawJsonResponse("""{"Status":2,"Message":"done","Value":42}""");

        var untyped = raw.DeserializeUntyped();
        Assert.Same(untyped, raw.DeserializeUntyped());

        var operation = raw.Deserialize<OperationResult>();
        Assert.Same(operation, raw.Deserialize<OperationResult>());
        Assert.Equal("done", operation!.Message);

        var alternate = raw.Deserialize<AlternatePayload>();
        Assert.Equal(42, alternate!.Value);
        Assert.Same(alternate, raw.Deserialize<AlternatePayload>());
        Assert.Same(operation, raw.Deserialize(typeof(OperationResult)));
    }

    private sealed class AlternatePayload
    {
        public int Value { get; set; }
    }
}
