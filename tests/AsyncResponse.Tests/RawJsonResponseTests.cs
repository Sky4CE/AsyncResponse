using Xunit;

namespace AsyncResponse.Tests;

public class RawJsonResponseTests
{
    [Fact]
    public void Deserialize_CachesUntypedSingleTypedAndMultipleTypedPayloads()
    {
        const string json = """{"Status":2,"Message":"done","Value":42}""";
        var raw = new RawJsonResponse(json);

        Assert.Equal(json, raw.Json);
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

    [Fact]
    public async Task Deserialize_IsThreadSafeUnderConcurrentFanOut()
    {
        // One instance is shared across every subscriber of a correlation id, so concurrent
        // materialization from fan-out dispatch must never observe a torn (type, payload) pair or
        // corrupt the memoization dictionary.
        const string json = """{"Status":2,"Message":"done","Value":42}""";
        for (var round = 0; round < 25; round++)
        {
            var raw = new RawJsonResponse(json);
            using var start = new ManualResetEventSlim();
            var tasks = Enumerable.Range(0, 8).Select(index => Task.Run(() =>
            {
                start.Wait();
                for (var i = 0; i < 20; i++)
                {
                    switch ((index + i) % 3)
                    {
                        case 0:
                            Assert.NotNull(raw.Deserialize<OperationResult>());
                            break;
                        case 1:
                            Assert.NotNull(raw.Deserialize<AlternatePayload>());
                            break;
                        default:
                            Assert.NotNull(raw.DeserializeUntyped());
                            break;
                    }
                }
            })).ToArray();

            start.Set();
            await Task.WhenAll(tasks);

            // Memoization stayed consistent: repeated lookups return the cached instances.
            Assert.Same(raw.Deserialize<OperationResult>(), raw.Deserialize<OperationResult>());
            Assert.Same(raw.Deserialize<AlternatePayload>(), raw.Deserialize<AlternatePayload>());
        }
    }

    private sealed class AlternatePayload
    {
        public int Value { get; set; }
    }
}
