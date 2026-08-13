using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace AsyncResponse.Tests;

public class AsyncResponseIngressErrorTests
{
    [Fact]
    public async Task HandleResponseMessageAsync_ParseFailure_FinalizesViaSetException_WithoutRetrying()
    {
        var original = new InvalidDataException("bad payload");
        var rawPublisher = new ThrowingRawPublisher(original);
        var publisher = new RecordingPublisher();
        var ingress = CreateIngress(rawPublisher, publisher);

        await ingress.HandleResponseMessageAsync("<html>bad gateway</html>", "corr-a");

        // An unparseable message never becomes parseable: exactly one attempt, then escalation.
        Assert.Equal(1, rawPublisher.RawJsonCalls);
        Assert.Same(original, publisher.Exception);
        Assert.Equal("corr-a", publisher.CorrelationId);
    }

    [Fact]
    public async Task HandleResponseMessageAsync_TransientFailure_RetriesInProcess_WithoutFinalizing()
    {
        var rawPublisher = new ThrowingRawPublisher(new TimeoutException("store blip"), _failures: 1);
        var publisher = new RecordingPublisher();
        var ingress = CreateIngress(rawPublisher, publisher);

        await ingress.HandleResponseMessageAsync("""{"Status":2}""", "corr-b");

        // A transient infrastructure fault must not convert the response into a permanent
        // business failure on the first attempt.
        Assert.Equal(2, rawPublisher.RawJsonCalls);
        Assert.Null(publisher.Exception);
    }

    [Fact]
    public async Task HandleResponseMessageAsync_PersistentTransientFailure_FinalizesAfterRetryBudget()
    {
        var original = new TimeoutException("store down");
        var rawPublisher = new ThrowingRawPublisher(original);
        var publisher = new RecordingPublisher();
        var ingress = CreateIngress(rawPublisher, publisher);

        await ingress.HandleResponseMessageAsync("""{"Status":2}""", "corr-c");

        Assert.Equal(4, rawPublisher.RawJsonCalls);
        Assert.Same(original, publisher.Exception);
        Assert.Equal("corr-c", publisher.CorrelationId);
    }

    [Fact]
    public async Task HandleResponseMessageAsync_WhenEscalationAlsoFails_Propagates_SoTransportRedelivers()
    {
        var original = new InvalidDataException("bad payload");
        var fallbackFailure = new InvalidOperationException("fallback failed");
        var rawPublisher = new ThrowingRawPublisher(original);
        var publisher = new RecordingPublisher(fallbackFailure);
        var ingress = CreateIngress(rawPublisher, publisher);

        // Returning normally here would ack a response that now exists nowhere; the double fault
        // must reach the transport so its redelivery/dead-letter policy retries the pipeline.
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ingress.HandleResponseMessageAsync("<html>bad gateway</html>", "corr-d"));

        Assert.Same(fallbackFailure, thrown);
        Assert.Same(original, publisher.Exception);
    }

    [Fact]
    public async Task HandleResponseMessageAsync_BlankCorrelationId_DropsMessage()
    {
        var rawPublisher = new ThrowingRawPublisher();
        var publisher = new RecordingPublisher();
        var ingress = CreateIngress(rawPublisher, publisher);

        await ingress.HandleResponseMessageAsync("""{"Status":2}""", " ");

        Assert.Equal(0, rawPublisher.RawJsonCalls);
        Assert.Null(publisher.Exception);
    }

    [Theory]
    [InlineData("corr-e ")]
    [InlineData(" corr-e")]
    [InlineData("looooong")]
    public async Task HandleResponseMessageAsync_NonPortableCorrelationId_DropsMessage(string correlationId)
    {
        // An id extracted from an untrusted broker message can be unroutable without being blank.
        // A padded one is the SAME key as its trimmed form to a relational store while the library
        // compares ids ordinally, so publishing it could surface this payload at another
        // conversation's waiter; an over-long one is truncated or rejected at that first write.
        // Dropped like a blank id — acknowledged and logged — because throwing would turn one bad
        // producer into an endless redelivery loop.
        if (correlationId == "looooong")
            correlationId = new string('c', AsyncResponseChannelOptions.MaxCorrelationIdLength + 1);

        var rawPublisher = new ThrowingRawPublisher();
        var publisher = new RecordingPublisher();
        var ingress = CreateIngress(rawPublisher, publisher);

        await ingress.HandleResponseMessageAsync("""{"Status":2}""", correlationId);

        Assert.Equal(0, rawPublisher.RawJsonCalls);
        Assert.Null(publisher.Exception);
    }

    [Fact]
    public async Task HandleWorkerMessageAsync_InvalidPayload_PropagatesForTransportRetryOrDeadLetter()
    {
        var ingress = CreateIngress(new ThrowingRawPublisher(), new RecordingPublisher());

        // The transport dispatcher owns the retry/dead-letter decision for worker jobs, so a
        // failing worker message must propagate instead of being acknowledged as a success.
        await Assert.ThrowsAsync<InvalidDataException>(() => ingress.HandleWorkerMessageAsync("null"));
        await Assert.ThrowsAnyAsync<Exception>(() => ingress.HandleWorkerMessageAsync("{not-json"));
    }

    private static AsyncResponseIngress CreateIngress(
        IRawAsyncResponsePublisher rawPublisher,
        IAsyncResponsePublisher publisher)
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        return new AsyncResponseIngress(
            rawPublisher,
            publisher,
            new WorkerJobExecutor(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<WorkerJobExecutor>.Instance),
            new AsyncResponseContextPropagation([]),
            NullLogger<AsyncResponseIngress>.Instance);
    }

    private sealed class ThrowingRawPublisher(Exception? _exception = null, int _failures = int.MaxValue) : IRawAsyncResponsePublisher
    {
        public int RawJsonCalls { get; private set; }

        public Task SetRawResponse(object? response, string? correlationId, CancellationToken cancellationToken = default)
            => _exception is null ? Task.CompletedTask : Task.FromException(_exception);

        public Task SetRawResponseJson(string responseJson, string? correlationId, CancellationToken cancellationToken = default)
        {
            RawJsonCalls++;
            return _exception is null || RawJsonCalls > _failures ? Task.CompletedTask : Task.FromException(_exception);
        }
    }

    private sealed class RecordingPublisher(Exception? _throwOnException = null) : IAsyncResponsePublisher
    {
        public Exception? Exception { get; private set; }
        public string? CorrelationId { get; private set; }

        public Task SetResponse<T>(T response, string? correlationId = null, CancellationToken cancellationToken = default) where T : IAsyncResponsePayload
            => Task.CompletedTask;

        public Task SetException(Exception exception, string? correlationId = null, CancellationToken cancellationToken = default)
        {
            Exception = exception;
            CorrelationId = correlationId;
            return _throwOnException is null ? Task.CompletedTask : Task.FromException(_throwOnException);
        }
    }
}
