using Xunit;

namespace AsyncResponse.Tests;

public class RawJsonResponseTests
{
    [Fact]
    public void Deserialize_MaterializesAFreshTypedInstancePerCall_AndCachesOnlyTheUntypedPayload()
    {
        const string json = """{"Status":2,"Message":"done","Value":42}""";
        var raw = new RawJsonResponse(json);

        Assert.Equal(json, raw.Json);

        // The untyped payload is an immutable JsonElement, so sharing one instance is safe.
        var untyped = raw.DeserializeUntyped();
        Assert.Same(untyped, raw.DeserializeUntyped());

        // Typed payloads are deliberately NOT memoized: one RawJsonResponse fans out to every
        // subscriber of a correlation id, and a shared mutable payload instance would alias user
        // state across concurrently-running predicates and handlers. Every call materializes a
        // private instance — the same per-waiter isolation the durable channels provide.
        var operation = raw.Deserialize<OperationResult>();
        Assert.NotNull(operation);
        Assert.Equal("done", operation!.Message);
        Assert.NotSame(operation, raw.Deserialize<OperationResult>());

        var alternate = raw.Deserialize<AlternatePayload>();
        Assert.Equal(42, alternate!.Value);
        Assert.NotSame(alternate, raw.Deserialize<AlternatePayload>());
        Assert.NotSame(operation, raw.Deserialize(typeof(OperationResult)));
    }

    [Fact]
    public async Task Deserialize_IsThreadSafeUnderConcurrentFanOut()
    {
        // One instance is shared across every subscriber of a correlation id, so concurrent
        // materialization from fan-out dispatch must never observe a torn untyped payload — and
        // every typed materialization must be a private instance, never one aliased across
        // concurrently-running subscribers.
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

            // Typed materialization stays per-call private; the untyped memo stays stable.
            Assert.NotSame(raw.Deserialize<OperationResult>(), raw.Deserialize<OperationResult>());
            Assert.NotSame(raw.Deserialize<AlternatePayload>(), raw.Deserialize<AlternatePayload>());
            Assert.Same(raw.DeserializeUntyped(), raw.DeserializeUntyped());
        }
    }

    private sealed class AlternatePayload
    {
        public int Value { get; set; }
    }
}
