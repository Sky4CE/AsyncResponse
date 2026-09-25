using AsyncResponse.Testing;
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
    public async Task HandleResponseMessageAsync_DeterministicCallbackFault_EscalatesWithoutRetrying()
    {
        // A resume callback that can never be wired up (unauthorized, unresolvable, malformed, no
        // longer binding) fails identically on every attempt. Pre-fix the ingress still spent its
        // 4-attempt ladder (~1.75 s of backoff, re-dispatching each time) before escalating — the
        // cost the dispatcher had already removed for failure callbacks. It escalates at once,
        // like a parse failure.
        var original = new CallbackTargetUnresolvableException("resume target is not registered");
        var rawPublisher = new ThrowingRawPublisher(original);
        var publisher = new RecordingPublisher();
        var ingress = CreateIngress(rawPublisher, publisher);

        await ingress.HandleResponseMessageAsync("""{"Status":2}""", "corr-deterministic");

        Assert.Equal(1, rawPublisher.RawJsonCalls);
        Assert.Same(original, publisher.Exception);
        Assert.Equal("corr-deterministic", publisher.CorrelationId);
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
    public async Task HandleResponseMessageAsync_Cancellation_Propagates_WithoutRetryOrSetException()
    {
        // Cancellation is not a handler failure: a durable flow losing its execution lease
        // mid-dispatch surfaces here as an OperationCanceledException. Retrying it burned the
        // in-process budget, and escalating it through SetException terminally failed a waiter
        // whose response was never lost — then the successful publish acked the message away.
        // It must propagate so the transport treats the delivery as canceled (NAK/redeliver).
        var cancellation = new OperationCanceledException("execution lease lost");
        var rawPublisher = new ThrowingRawPublisher(cancellation);
        var publisher = new RecordingPublisher();
        var ingress = CreateIngress(rawPublisher, publisher);

        var thrown = await Assert.ThrowsAsync<OperationCanceledException>(
            () => ingress.HandleResponseMessageAsync("""{"Status":2}""", "corr-oce"));

        Assert.Same(cancellation, thrown);
        Assert.Equal(1, rawPublisher.RawJsonCalls);
        Assert.Null(publisher.Exception);
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

        // Acknowledged now rather than thrown (it can never parse on any build), but the point of
        // this regression is unchanged: nothing of the body may reach the log on the way out.
        await ingress.HandleWorkerMessageAsync("""{"Call":{"broken":"card 4111-1111-1111-1111"}""");

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
    public async Task HandleWorkerMessageAsync_UnparseableEnvelope_IsAcknowledgedInsteadOfRedelivered()
    {
        var ingress = CreateIngress(new ThrowingRawPublisher(), new RecordingPublisher());

        // An envelope NO build can ever parse gets the same answer the response path gives an
        // unroutable id: acknowledged, never thrown. Propagating it made the transport redeliver —
        // and on RabbitMQ's shipped default MaxDeliveryAttempts = 0 that requeued forever, at full
        // CPU, pinning a prefetch slot. Errors that CAN succeed on a retry still propagate (see
        // HandleWorkerMessageAsync_ExecutionFailure_PropagatesForTransportRetryOrDeadLetter).
        await ingress.HandleWorkerMessageAsync("null");
        await ingress.HandleWorkerMessageAsync("{not-json");
    }

    [Fact]
    public async Task HandleWorkerMessageAsync_ExplicitNullCall_IsAcknowledgedInsteadOfRedelivered()
    {
        // Regression: `required` on WorkerJobEnvelope.Call enforces PRESENCE on the wire, not
        // non-null, so {"Call":null} parsed successfully and then hit the first unguarded
        // dereference — a NullReferenceException outside the drop-and-ack filter, so the
        // transport redelivered it forever (RabbitMQ's shipped default MaxDeliveryAttempts = 0
        // requeues without a cap) with no worker-outcome counter moving. It is the same
        // producer-side contract violation as an unparseable envelope and gets the same answer:
        // acknowledged, never thrown.
        var ingress = CreateIngress(new ThrowingRawPublisher(), new RecordingPublisher());

        await ingress.HandleWorkerMessageAsync("""{"SchemaVersion":1,"Call":null,"CorrelationId":"corr-null-call"}""");
    }

    [Theory]
    [InlineData("""{"SchemaVersion":1,"Call":{"ServiceInterfaceFullName":null,"MethodName":"Y","Params":[]},"CorrelationId":"c1"}""")]
    [InlineData("""{"SchemaVersion":1,"Call":{"ServiceInterfaceFullName":"X","MethodName":null,"Params":[]},"CorrelationId":"c1"}""")]
    [InlineData("""{"SchemaVersion":1,"Call":{"ServiceInterfaceFullName":"X","MethodName":"Y","Params":null},"CorrelationId":"c1"}""")]
    [InlineData("""{"SchemaVersion":1,"Call":{"ServiceInterfaceFullName":"X","MethodName":"Y","Params":[null]},"CorrelationId":"c1"}""")]
    [InlineData("""{"SchemaVersion":1,"Call":{"ServiceInterfaceFullName":"  ","MethodName":"Y","Params":[]},"CorrelationId":"c1"}""")]
    [InlineData("""{"SchemaVersion":1,"Call":{"ServiceInterfaceFullName":"X","MethodName":"","Params":[]},"CorrelationId":"c1"}""")]
    public async Task HandleWorkerMessageAsync_NullCallMembers_AreAcknowledgedInsteadOfRedelivered(string json)
    {
        // Regression (round 31): the Call-null guard's own mechanism — `required` enforces
        // presence on the wire, not non-null — applies identically one level down, but
        // ReflectionCallDto's members got no equivalent check. An explicit "Params": null (or a
        // null element, or a null target name) parsed, passed the guard, and threw
        // ArgumentNullException from the first dereference — outside the drop-and-ack filter, so
        // the transport redelivered it forever. Same contract violation, same answer:
        // acknowledged, never thrown.
        var ingress = CreateIngress(new ThrowingRawPublisher(), new RecordingPublisher());

        await ingress.HandleWorkerMessageAsync(json);
    }

    [Theory]
    [InlineData(null, "t", "a")]
    [InlineData("", "t", "a")]
    [InlineData("  ", "t", "a")]
    [InlineData("n", null, "a")]
    [InlineData("n", "t", null)]
    [InlineData("n", "t", " ")]
    public async Task HandleWorkerMessageAsync_MalformedReplyTarget_IsAcknowledgedWithoutRunningTheJob(string? name, string? transport, string? address)
    {
        // Same mechanism beside the call: the reply target's members are `required` strings, which
        // enforces presence on the wire, not non-null. A null or blank member parsed, passed the
        // gate, and threw ArgumentException from the executor's context push — outside the
        // drop-and-ack filter, on every delivery, forever. It is rejected at the gate now.
        var worker = new CountingWorker();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ICountingWorker>(worker);
        services.AddAsyncResponse().WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();
        var replyTarget = new System.Text.Json.Nodes.JsonObject
        {
            ["Name"] = name,
            ["Transport"] = transport,
            ["Address"] = address
        };
        var json = $$"""{"SchemaVersion":1,"Call":{"ServiceInterfaceFullName":"{{typeof(ICountingWorker).FullName}}","MethodName":"{{nameof(ICountingWorker.Run)}}","Params":[]},"CorrelationId":"corr-reply-target","ReplyTarget":{{replyTarget.ToJsonString()}}}""";

        await ingress.HandleWorkerMessageAsync(json);

        Assert.Equal(0, worker.Runs);
    }

    [Fact]
    public async Task HandleWorkerMessageAsync_StreamWrittenNames_AreEscapedInItsLogLines()
    {
        // The routing line is logged before anything has validated or authorized the envelope,
        // and its names are stream-written text: a CR/LF inside one ended the real log entry and
        // started a forged one in every line-oriented sink.
        var logger = new CapturingLogger<AsyncResponseIngress>();
        var ingress = CreateIngress(new ThrowingRawPublisher(), new RecordingPublisher(), logger);
        var envelope = AsyncResponseJson.Serialize(new WorkerJobEnvelope
        {
            CorrelationId = "corr-forged-names",
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = "Contoso.IBilling\r\n[Error] forged service line",
                MethodName = "Charge\r\n[Error] forged method line",
                Params = []
            }
        });

        await Assert.ThrowsAnyAsync<Exception>(() => ingress.HandleWorkerMessageAsync(envelope));

        Assert.Contains(logger.Entries, entry => entry.Message.Contains("Contoso.IBilling", StringComparison.Ordinal));
        Assert.DoesNotContain(logger.Entries, entry => entry.Message.Contains('\r') || entry.Message.Contains('\n'));
    }

    public interface ICountingWorker
    {
        Task Run();
    }

    private sealed class CountingWorker : ICountingWorker
    {
        private int _runs;

        public int Runs => Volatile.Read(ref _runs);

        public Task Run()
        {
            Interlocked.Increment(ref _runs);
            return Task.CompletedTask;
        }
    }

    [Fact]
    public async Task HandleWorkerMessageAsync_JsonExceptionFromTheJobBody_PropagatesForRedelivery()
    {
        // Regression: the "unparseable envelope" catch filter spanned the WHOLE handler, so a
        // JsonException thrown by the job body — a durable flow deserializing a persisted input,
        // a handler parsing a third-party response — was misclassified as a malformed envelope,
        // recorded as rejected, and acknowledged: the transport never redelivered, and a Running
        // flow lost its only wake-up. The filter now covers only the parse; body failures reach
        // the transport's retry/dead-letter policy.
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<IJsonThrowingWorker>(new JsonThrowingWorker());
        services.AddAsyncResponse().WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        var json = AsyncResponseJson.Serialize(new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IJsonThrowingWorker).FullName!,
                MethodName = nameof(IJsonThrowingWorker.Parse),
                Params = [CallbackParam.ForValue(7)]
            },
            CorrelationId = "corr-body-json"
        });

        var exception = await Assert.ThrowsAnyAsync<Exception>(() => ingress.HandleWorkerMessageAsync(json));
        Assert.IsType<System.Text.Json.JsonException>(exception);
    }

    [Fact]
    public async Task HandleWorkerMessageAsync_HostStopHandBack_PropagatesWithoutAnErrorLog()
    {
        // A DurableFlowInterruptedException is the engine handing the delivery back because the
        // host is stopping — not a failure. The executor records it as neither failed nor an error
        // span and every transport leaves the delivery unsettled, but the ingress still logged
        // "Ingress worker job execution failed." at Error for it on every rolling deploy.
        var logger = new CapturingLogger<AsyncResponseIngress>();
        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        services.AddSingleton<ILogger<AsyncResponseIngress>>(logger);
        services.AddSingleton<IHandBackWorker>(new HandBackWorker());
        services.AddAsyncResponse().WithInMemoryChannel();
        await using var provider = services.BuildServiceProvider();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        var json = AsyncResponseJson.Serialize(new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IHandBackWorker).FullName!,
                MethodName = nameof(IHandBackWorker.Run),
                Params = []
            },
            CorrelationId = "corr-hand-back"
        });

        await Assert.ThrowsAsync<DurableFlowInterruptedException>(() => ingress.HandleWorkerMessageAsync(json));
        Assert.DoesNotContain(logger.Entries, entry => entry.Level >= LogLevel.Error);
    }

    [Fact]
    public async Task HandleWorkerMessageAsync_ExecutionFailure_PropagatesForTransportRetryOrDeadLetter()
    {
        var ingress = CreateIngress(new ThrowingRawPublisher(), new RecordingPublisher());

        // A well-formed envelope whose execution fails is the transport dispatcher's decision to
        // make (per its AckMode and MaxDeliveryAttempts), so it must still propagate.
        await Assert.ThrowsAnyAsync<Exception>(
            () => ingress.HandleWorkerMessageAsync("""{"SchemaVersion":2147483647,"Call":{"ServiceInterfaceFullName":"X","MethodName":"Y","Params":[]}}"""));
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

    [Fact]
    public async Task HandleResponseMessageAsync_RetryBackoff_RunsOnTheInjectedTimeProvider()
    {
        // Regression (review fix): the ingress armed its transient-retry backoff on the system
        // clock even when the host registered a TimeProvider, so virtual-time tests (and any
        // deployment steering time) saw the retry fire on real time. Built through DI rather than
        // the direct-construction helper because the fix under test is precisely that the
        // library's own registration forwards a registered TimeProvider into the retry.
        var time = new VirtualTimeProvider();
        var rawPublisher = new ThrowingRawPublisher(new TimeoutException("store blip"), _failures: 1);
        var publisher = new RecordingPublisher();

        var services = new ServiceCollection();
        services.AddSingleton(typeof(ILogger<>), typeof(NullLogger<>));
        // Registered BEFORE AddAsyncResponse: the library's TryAdd registrations yield to these.
        services.AddSingleton<TimeProvider>(time);
        services.AddSingleton<IRawAsyncResponsePublisher>(rawPublisher);
        services.AddSingleton<IAsyncResponsePublisher>(publisher);
        services.AddAsyncResponse();
        await using var provider = services.BuildServiceProvider();
        var ingress = provider.GetRequiredService<IAsyncResponseIngress>();

        var handling = ingress.HandleResponseMessageAsync("""{"Status":2}""", "corr-tp");

        // The first attempt failed and the retry is parked on the VIRTUAL clock: ample real time
        // passes and the handling must still be pending (the old code's 125-250ms system-clock
        // backoff would long since have fired and completed it).
        await Task.Delay(TimeSpan.FromMilliseconds(600));
        Assert.False(handling.IsCompleted, "the retry backoff ran on the system clock instead of the injected TimeProvider");
        Assert.Equal(1, rawPublisher.RawJsonCalls);

        time.Advance(TimeSpan.FromSeconds(2));
        await handling.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(2, rawPublisher.RawJsonCalls);
        Assert.Null(publisher.Exception);
    }

    [Fact]
    public async Task WorkerArgument_UnsupportedPolymorphicValue_DoesNotLeakIntoLogs()
    {
        const string secret = "private-customer-review-marker";
        var logger = new CollectingLogger();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IUnsupportedArgumentWorker, UnsupportedArgumentWorker>();
        services.AddAsyncResponse().AuthorizeCallbacks(a => a.Allow<IUnsupportedArgumentWorker>())
            .WithInMemoryChannel().WithInMemoryTransport();
        services.AddSingleton(logger.For<AsyncResponseIngress>());
        await using var provider = services.BuildServiceProvider();
        using var argument = System.Text.Json.JsonDocument.Parse("{\"" + secret + "\":{}}");
        var job = new WorkerJobEnvelope
        {
            Call = new ReflectionCallDto
            {
                ServiceInterfaceFullName = typeof(IUnsupportedArgumentWorker).FullName!,
                MethodName = nameof(IUnsupportedArgumentWorker.Run),
                Params = [CallbackParam.ForValue(argument.RootElement.Clone())]
            }
        };

        // An argument that cannot be converted is a wiring fault (CallbackTargetUnresolvableException,
        // an InvalidOperationException) wrapping the reader's body-free InvalidDataException; the
        // worker ingress still propagates it for the transport's retry/dead-letter decision.
        var thrown = await Assert.ThrowsAnyAsync<InvalidOperationException>(() => provider.GetRequiredService<IAsyncResponseIngress>()
            .HandleWorkerMessageAsync(AsyncResponseJson.Serialize(job)));
        Assert.IsType<InvalidDataException>(thrown.InnerException);
        Assert.Contains(logger.Entries, e => e.Exception?.InnerException is InvalidDataException);
        Assert.All(logger.Entries, e => Assert.DoesNotContain(secret, e.Message + e.Exception, StringComparison.Ordinal));
    }

    public interface IUnsupportedArgumentWorker
    {
        Task Run(Dictionary<string, JsonSafetyTests.AbstractInput> input);
    }

    private sealed class UnsupportedArgumentWorker : IUnsupportedArgumentWorker
    {
        public Task Run(Dictionary<string, JsonSafetyTests.AbstractInput> input) => throw new InvalidOperationException("Must fail before dispatch.");
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

    public interface IHandBackWorker
    {
        Task Run();
    }

    private sealed class HandBackWorker : IHandBackWorker
    {
        public Task Run() => throw new DurableFlowInterruptedException("The host is stopping; the delivery is handed back.");
    }

    public interface IJsonThrowingWorker
    {
        Task Parse(int value);
    }

    private sealed class JsonThrowingWorker : IJsonThrowingWorker
    {
        public Task Parse(int value) => throw new System.Text.Json.JsonException("the job body failed to parse its own data");
    }

    private sealed class ThrowingRawPublisher(Exception? _exception = null, int _failures = int.MaxValue) : IRawAsyncResponsePublisher
    {
        public int RawJsonCalls { get; private set; }

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
