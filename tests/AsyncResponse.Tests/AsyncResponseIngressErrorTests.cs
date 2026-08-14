using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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

    [Fact]
    public async Task HandleWorkerMessageAsync_WithAnUnparseableEnvelope_StillNeverLogsIt()
    {
        // The other half of the worker path: a body that never becomes an envelope at all. It takes
        // a different route — the failure is thrown out of deserialization, before any field has
        // been read — so the "log safe metadata once parsed" step never runs and the only thing
        // standing between the body and the log is the size line. A malformed body is exactly the
        // one an operator turns Debug on to inspect, which is when it would have leaked.
        var logger = new CapturingLogger<AsyncResponseIngress>();
        var ingress = CreateIngress(new ThrowingRawPublisher(), new RecordingPublisher(), logger);

        await Assert.ThrowsAnyAsync<Exception>(
            () => ingress.HandleWorkerMessageAsync("""{"Call":{"broken":"card 4111-1111-1111-1111"}"""));

        Assert.DoesNotContain(logger.Entries, entry => entry.Message.Contains("4111", StringComparison.Ordinal));
        AssertNoContentDigest(logger);
        Assert.Contains(logger.Entries, entry => entry.Message.Contains("code units", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("corr-e ")]
    [InlineData(" corr-e")]
    [InlineData("looooong")]
    public async Task HandleResponseMessageAsync_NonPortableCorrelationId_DropsMessageWithoutLoggingIt(string correlationId)
    {
        // The drop branch logs at ERROR — the loudest level in this file, and the one most likely
        // to be on — so it is also the branch where a body would be most exposed. Same rule: a
        // size, never the payload — and never a hash of it, which is a content oracle for a
        // low-entropy body and equality-trackable across messages.
        if (correlationId == "looooong")
            correlationId = new string('c', AsyncResponseChannelOptions.MaxCorrelationIdLength + 1);

        var logger = new CapturingLogger<AsyncResponseIngress>();
        var rawPublisher = new ThrowingRawPublisher();
        var ingress = CreateIngress(rawPublisher, new RecordingPublisher(), logger);

        await ingress.HandleResponseMessageAsync("""{"Status":2,"Message":"card 4111-1111-1111-1111"}""", correlationId);

        Assert.Equal(0, rawPublisher.RawJsonCalls);
        Assert.DoesNotContain(logger.Entries, entry => entry.Message.Contains("4111", StringComparison.Ordinal));
        AssertNoContentDigest(logger);
        Assert.Contains(logger.Entries, entry =>
            entry.Level == LogLevel.Error && entry.Message.Contains("code units", StringComparison.Ordinal));
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

    [Fact]
    public async Task HandleResponseMessageAsync_NeverLogsThePayloadBody()
    {
        // Response payloads carry application data — this project's own security policy keeps them
        // out of logs (docs/security.md), and the ingress states that policy in a comment fifteen
        // lines above where it used to log the entire body at Debug. Debug is on in plenty of
        // production deployments, and this is the first place every inbound response passes.
        var logger = new CapturingLogger<AsyncResponseIngress>();
        var ingress = CreateIngress(new ThrowingRawPublisher(), new RecordingPublisher(), logger);

        await ingress.HandleResponseMessageAsync("""{"Status":2,"Message":"card 4111-1111-1111-1111"}""", "corr-log");

        Assert.DoesNotContain(logger.Entries, entry => entry.Message.Contains("4111", StringComparison.Ordinal));
        AssertNoContentDigest(logger);
        // Still traceable: the correlation id and a size, which is what the digest stood in for.
        Assert.Contains(logger.Entries, entry =>
            entry.Message.Contains("code units", StringComparison.Ordinal)
            && entry.Message.Contains("corr-log", StringComparison.Ordinal));
    }

    [Fact]
    public async Task HandleWorkerMessageAsync_NeverLogsTheEnvelope()
    {
        // Worse than a response body: the worker envelope carries the job's ARGUMENTS and whatever
        // the context propagators captured — tenant, auth, trace baggage. Execution fails here (no
        // such service is registered), which is the point: neither the happy path nor the failure
        // path may put the envelope in the log.
        var logger = new CapturingLogger<AsyncResponseIngress>();
        var ingress = CreateIngress(new ThrowingRawPublisher(), new RecordingPublisher(), logger);
        var envelope = AsyncResponseJson.Serialize(new WorkerJobEnvelope
        {
            CorrelationId = "corr-worker",
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "Contoso.IBilling",
                MethodName = "Charge",
                Params = [CallbackParam.ForValue("card 4111-1111-1111-1111")]
            },
            Context = new Dictionary<string, string> { ["tenant"] = "acme-secret" }
        });

        await Assert.ThrowsAnyAsync<Exception>(() => ingress.HandleWorkerMessageAsync(envelope));

        Assert.DoesNotContain(logger.Entries, entry => entry.Message.Contains("4111", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Entries, entry => entry.Message.Contains("acme-secret", StringComparison.Ordinal));
        AssertNoContentDigest(logger);
        // Safe routing metadata is still logged — the service and method are the point of the line.
        Assert.Contains(logger.Entries, entry => entry.Message.Contains("code units", StringComparison.Ordinal));
        Assert.Contains(logger.Entries, entry => entry.Message.Contains("Contoso.IBilling.Charge", StringComparison.Ordinal));
    }

    /// <summary>
    /// No hash of the body, at any level. A digest reads like harmless metadata and is not: it is
    /// deterministic, so two entries showing the same value prove the two payloads were identical
    /// across messages and hosts, and a payload drawn from a small set (a status enum, an account
    /// id, a yes/no result) is confirmed outright by hashing the candidates. Asserting only that
    /// the payload is absent would let a digest come back unnoticed — which is how it got here.
    /// </summary>
    private static void AssertNoContentDigest(CapturingLogger<AsyncResponseIngress> logger)
    {
        Assert.DoesNotContain(logger.Entries, entry =>
            entry.Message.Contains("sha", StringComparison.OrdinalIgnoreCase)
            || entry.Message.Contains("hash", StringComparison.OrdinalIgnoreCase)
            || entry.Message.Contains("digest", StringComparison.OrdinalIgnoreCase)
            || entry.Message.Contains("fingerprint", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task HandleResponseMessageAsync_IllFormedCorrelationId_DropsMessage()
    {
        // The untrusted edge takes the same ill-formed-UTF-16 rule as the public boundary, with the
        // opposite answer: dropped, not thrown, because a broker message that throws comes straight
        // back on redelivery. An unpaired surrogate encodes to the same bytes as a literal U+FFFD,
        // so routing it would hand this payload to whichever conversation owns that subject.
        var rawPublisher = new ThrowingRawPublisher();
        var ingress = CreateIngress(rawPublisher, new RecordingPublisher());

        await ingress.HandleResponseMessageAsync("""{"Status":2}""", "corr-\ud800");

        Assert.Equal(0, rawPublisher.RawJsonCalls);
    }

    private static AsyncResponseIngress CreateIngress(
        IRawAsyncResponsePublisher rawPublisher,
        IAsyncResponsePublisher publisher,
        ILogger<AsyncResponseIngress>? logger = null)
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        return new AsyncResponseIngress(
            rawPublisher,
            publisher,
            new WorkerJobExecutor(provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<WorkerJobExecutor>.Instance),
            new AsyncResponseContextPropagation([]),
            logger ?? NullLogger<AsyncResponseIngress>.Instance);
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
