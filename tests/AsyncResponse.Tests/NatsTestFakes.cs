using AsyncResponse.Channels.NATS;
using AsyncResponse.Transports.NATS;
using Microsoft.Extensions.Logging;
using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace AsyncResponse.Tests;

/// <summary>A logger whose <see cref="IsEnabled"/> always returns true so <c>IsEnabled</c>-guarded log
/// statements are exercised in coverage (NullLogger reports false and skips them).</summary>
internal class TestLogger : ILogger
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
    public bool IsEnabled(LogLevel logLevel) => true;
    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
}

internal sealed class TestLogger<T> : TestLogger, ILogger<T>;

/// <summary>A controllable <see cref="TimeProvider"/> for deterministic expiry tests.</summary>
internal sealed class TestTimeProvider : TimeProvider
{
    public DateTimeOffset Now { get; set; } = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    public override DateTimeOffset GetUtcNow() => Now;
    public void Advance(TimeSpan by) => Now += by;
}

/// <summary>In-memory <see cref="INatsKvStore"/> backed by a dictionary, for recovery-store tests.</summary>
internal sealed class FakeNatsKvStore : INatsKvStore
{
    public readonly Dictionary<string, string> Entries = new(StringComparer.Ordinal);
    public int PutCount, DeleteCount;

    public Task PutAsync(string key, string value, CancellationToken cancellationToken)
    {
        PutCount++;
        Entries[key] = value;
        return Task.CompletedTask;
    }

    public Task<string?> GetAsync(string key, CancellationToken cancellationToken)
        => Task.FromResult(Entries.TryGetValue(key, out var value) ? value : null);

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken)
    {
        DeleteCount++;
        return Task.FromResult(Entries.Remove(key));
    }

    public async IAsyncEnumerable<string> GetKeysAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var key in Entries.Keys.ToArray())
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return key;
            await Task.Yield();
        }
    }
}

/// <summary>
/// Fake <see cref="INatsResponseChannelClient"/>: <see cref="RequestAsync"/> returns a configurable
/// outcome and, on a non-probe delivery to a live subscription, fans the payload into the subscriber
/// (mirroring real NATS request/reply). Tests can also push messages directly.
/// </summary>
internal sealed class FakeNatsResponseChannelClient : INatsResponseChannelClient
{
    public NatsDeliveryOutcome NextOutcome { get; set; } = NatsDeliveryOutcome.Replied;
    public Func<bool, NatsDeliveryOutcome>? OutcomeForProbe { get; set; }
    public readonly List<(string Subject, string? Payload, bool Probe)> Requests = new();
    public readonly List<string> SubscribedSubjects = new();
    public int FlushCount;

    private FakeSubscription? _subscription;
    public bool HasSubscription => _subscription is not null;

    public Task<NatsDeliveryOutcome> RequestAsync(string subject, string? payload, bool probe, TimeSpan timeout, CancellationToken cancellationToken)
    {
        Requests.Add((subject, payload, probe));
        var outcome = OutcomeForProbe is not null && probe
            ? OutcomeForProbe(probe)
            : NextOutcome;

        if (!probe && outcome != NatsDeliveryOutcome.NoResponders && _subscription is not null && payload is not null)
            _subscription.Push(new NatsInboundResponse(payload, false, () => ValueTask.CompletedTask));

        return Task.FromResult(outcome);
    }

    public Task<INatsChannelSubscription> SubscribeAsync(string subject, CancellationToken cancellationToken)
    {
        SubscribedSubjects.Add(subject);
        _subscription = new FakeSubscription();
        return Task.FromResult<INatsChannelSubscription>(_subscription);
    }

    public Task FlushAsync(CancellationToken cancellationToken)
    {
        FlushCount++;
        return Task.CompletedTask;
    }

    /// <summary>Pushes a raw message (e.g. malformed JSON or a probe) into the live subscription.</summary>
    public void Push(string? payload, bool isProbe = false)
        => _subscription!.Push(new NatsInboundResponse(payload, isProbe, () => ValueTask.CompletedTask));

    /// <summary>Faults the live subscription's read loop with <paramref name="exception"/>.</summary>
    public void FailSubscription(Exception exception) => _subscription!.Fail(exception);

    private sealed class FakeSubscription : INatsChannelSubscription
    {
        private readonly Channel<NatsInboundResponse> _channel = Channel.CreateUnbounded<NatsInboundResponse>();

        public void Push(NatsInboundResponse message) => _channel.Writer.TryWrite(message);

        public void Fail(Exception exception) => _channel.Writer.TryComplete(exception);

        public async IAsyncEnumerable<NatsInboundResponse> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var message in _channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return message;
        }

        public ValueTask DisposeAsync()
        {
            _channel.Writer.TryComplete();
            return ValueTask.CompletedTask;
        }
    }
}

/// <summary>Records the ack/nak/term verbs invoked on a single JetStream delivery.</summary>
internal sealed class RecordingDelivery
{
    public int Acks { get; private set; }
    public int Terms { get; private set; }
    public readonly List<TimeSpan> Naks = new();

    public NatsJobDelivery Create(string payload, long numDelivered, string subject = "asyncresponse.transport.worker", IReadOnlyDictionary<string, string>? headers = null)
        => new(
            subject,
            payload,
            headers ?? new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
            numDelivered,
            () => { Acks++; return ValueTask.CompletedTask; },
            delay => { Naks.Add(delay); return ValueTask.CompletedTask; },
            () => { Terms++; return ValueTask.CompletedTask; });
}

/// <summary>Fake <see cref="INatsJetStreamTransport"/> recording topology/publish calls and yielding queued deliveries.</summary>
internal sealed class FakeNatsJetStreamTransport : INatsJetStreamTransport
{
    public readonly List<(string Stream, string Subject)> EnsuredStreams = new();
    public readonly List<(string Stream, string Durable)> EnsuredConsumers = new();
    public readonly List<(string Subject, string Payload, IReadOnlyDictionary<string, string>? Headers)> Published = new();

    /// <summary>Returns an exception to throw for a given (1-based) publish attempt, or null to succeed.</summary>
    public Func<int, Exception?>? PublishFailureForAttempt { get; set; }

    /// <summary>Returns an exception to throw for a given (1-based) EnsureConsumer attempt, or null to succeed.</summary>
    public Func<int, Exception?>? EnsureConsumerFailureForAttempt { get; set; }

    private int _publishAttempts;
    private int _ensureConsumerAttempts;
    private readonly Channel<NatsJobDelivery> _deliveries = Channel.CreateUnbounded<NatsJobDelivery>();

    public Task EnsureStreamAsync(string stream, string subject, long? maxMessages, CancellationToken cancellationToken)
    {
        EnsuredStreams.Add((stream, subject));
        return Task.CompletedTask;
    }

    public Task EnsureConsumerAsync(string stream, string durable, TimeSpan ackWait, CancellationToken cancellationToken)
    {
        _ensureConsumerAttempts++;
        var failure = EnsureConsumerFailureForAttempt?.Invoke(_ensureConsumerAttempts);
        if (failure is not null)
            throw failure;

        EnsuredConsumers.Add((stream, durable));
        return Task.CompletedTask;
    }

    public Task<string> PublishAsync(string subject, string payload, IReadOnlyDictionary<string, string>? headers, CancellationToken cancellationToken)
    {
        _publishAttempts++;
        var failure = PublishFailureForAttempt?.Invoke(_publishAttempts);
        if (failure is not null)
            throw failure;

        Published.Add((subject, payload, headers));
        return Task.FromResult(_publishAttempts.ToString());
    }

    public async IAsyncEnumerable<NatsJobDelivery> ConsumeAsync(string stream, string durable, int batchSize, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var delivery in _deliveries.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            yield return delivery;
    }

    public void EnqueueDelivery(NatsJobDelivery delivery) => _deliveries.Writer.TryWrite(delivery);
    public void CompleteDeliveries() => _deliveries.Writer.TryComplete();
}

/// <summary>A payload that does not override <see cref="IAsyncResponsePayload.ShouldResumeOnRecovery"/>.</summary>
internal sealed class UnclassifiedNatsPayload : IAsyncResponsePayload
{
    public string? Message { get; set; }
}

/// <summary>Recovery-callback target resolved by the lost-subscriber dispatcher in channel tests.</summary>
internal interface INatsRecoverySpy
{
    Task ResumeAsync(OperationResult payload, string correlationId);
    Task FailAsync(Exception exception, string correlationId);
}

internal sealed class NatsRecoverySpy : INatsRecoverySpy
{
    public OperationResult? Resumed { get; private set; }
    public Exception? Failed { get; private set; }
    public string? CorrelationId { get; private set; }

    public Task ResumeAsync(OperationResult payload, string correlationId)
    {
        Resumed = payload;
        CorrelationId = correlationId;
        return Task.CompletedTask;
    }

    public Task FailAsync(Exception exception, string correlationId)
    {
        Failed = exception;
        CorrelationId = correlationId;
        return Task.CompletedTask;
    }
}

/// <summary>Records ingress calls so subscriber-service tests can assert routing without the real engine.</summary>
internal sealed class FakeAsyncResponseIngress : IAsyncResponseIngress
{
    private readonly object _gate = new();
    public List<string> WorkerMessages { get; } = new();
    public List<(string Json, string? CorrelationId)> ResponseMessages { get; } = new();

    public Task HandleResponseMessageAsync(string messageJson, string? correlationId = null)
    {
        lock (_gate)
            ResponseMessages.Add((messageJson, correlationId));
        return Task.CompletedTask;
    }

    public Task HandleWorkerMessageAsync(string messageJson)
    {
        lock (_gate)
            WorkerMessages.Add(messageJson);
        return Task.CompletedTask;
    }

    public int WorkerCount
    {
        get { lock (_gate) return WorkerMessages.Count; }
    }

    public int ResponseCount
    {
        get { lock (_gate) return ResponseMessages.Count; }
    }
}
