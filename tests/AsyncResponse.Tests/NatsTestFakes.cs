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

/// <summary>
/// In-memory <see cref="INatsKvStore"/> backed by a dictionary with per-key revisions, for
/// recovery-store tests. Revisions make the store's optimistic CAS paths real: a conditional write
/// or delete with a stale revision fails exactly like NATS KV, so retry loops are exercised.
/// </summary>
internal sealed class FakeNatsKvStore : INatsKvStore
{
    private readonly object _gate = new();
    private readonly Dictionary<string, ulong> _revisions = new(StringComparer.Ordinal);
    private ulong _revisionCounter;

    public readonly Dictionary<string, string> Entries = new(StringComparer.Ordinal);
    public int PutCount, DeleteCount;
    public int ForcedCreateConflicts { get; set; }
    public int ForcedUpdateConflicts { get; set; }
    public int ForcedDeleteConflicts { get; set; }
    public Exception? DeleteException { get; set; }

    /// <summary>
    /// One-shot hook that runs after a successful <see cref="GetAsync"/>, before the value is
    /// returned — lets a test interleave a competing write between a CAS read and its conditional
    /// write deterministically. Cleared before it runs so a re-entrant store call cannot recurse.
    /// </summary>
    public Func<string, Task>? AfterGet;

    public Task PutAsync(string key, string value, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            PutCount++;
            Entries[key] = value;
            _revisions[key] = ++_revisionCounter;
        }

        return Task.CompletedTask;
    }

    public Task<bool> TryCreateAsync(string key, string value, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (ForcedCreateConflicts > 0)
            {
                ForcedCreateConflicts--;
                return Task.FromResult(false);
            }

            if (Entries.ContainsKey(key))
                return Task.FromResult(false);

            PutCount++;
            Entries[key] = value;
            _revisions[key] = ++_revisionCounter;
            return Task.FromResult(true);
        }
    }

    public Task<bool> TryUpdateAsync(string key, string value, ulong expectedRevision, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (ForcedUpdateConflicts > 0)
            {
                ForcedUpdateConflicts--;
                return Task.FromResult(false);
            }

            if (!Entries.ContainsKey(key) || !_revisions.TryGetValue(key, out var revision) || revision != expectedRevision)
                return Task.FromResult(false);

            PutCount++;
            Entries[key] = value;
            _revisions[key] = ++_revisionCounter;
            return Task.FromResult(true);
        }
    }

    public async Task<NatsKvEntry?> GetAsync(string key, CancellationToken cancellationToken)
    {
        NatsKvEntry? result;
        lock (_gate)
        {
            if (!Entries.TryGetValue(key, out var value))
            {
                result = null;
            }
            else
            {
                // Entries seeded directly by a test carry no revision yet; assign one lazily so
                // conditional writes against seeded data behave like NATS KV.
                if (!_revisions.TryGetValue(key, out var revision))
                {
                    revision = ++_revisionCounter;
                    _revisions[key] = revision;
                }

                result = new NatsKvEntry(value, revision);
            }
        }

        if (result is not null && AfterGet is { } hook)
        {
            AfterGet = null;
            await hook(key);
        }

        return result;
    }

    public Task<bool> DeleteAsync(string key, CancellationToken cancellationToken)
    {
        if (DeleteException is not null)
            throw DeleteException;

        lock (_gate)
        {
            DeleteCount++;
            _revisions.Remove(key);
            return Task.FromResult(Entries.Remove(key));
        }
    }

    public Task<bool> TryDeleteAsync(string key, ulong expectedRevision, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (ForcedDeleteConflicts > 0)
            {
                ForcedDeleteConflicts--;
                return Task.FromResult(false);
            }

            if (!_revisions.TryGetValue(key, out var revision) || revision != expectedRevision)
                return Task.FromResult(false);

            DeleteCount++;
            _revisions.Remove(key);
            return Task.FromResult(Entries.Remove(key));
        }
    }

    public async IAsyncEnumerable<string> GetKeysAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        string[] keys;
        lock (_gate)
            keys = Entries.Keys.ToArray();

        foreach (var key in keys)
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
    public Exception? RequestException { get; set; }

    /// <summary>
    /// Per-call outcomes for non-probe deliveries, consumed in order before falling back to
    /// <see cref="NextOutcome"/>. Needed for the re-publish path, where the first delivery finds no
    /// responders and the retry after the live-subscriber re-check has to behave differently.
    /// </summary>
    public readonly Queue<NatsDeliveryOutcome> DeliveryOutcomes = new();
    public readonly List<(string Subject, string? Payload, bool Probe)> Requests = new();
    public readonly List<string> SubscribedSubjects = new();
    public int FlushCount;

    private FakeSubscription? _subscription;
    private int _subscriptionDisposeCount;
    public bool HasSubscription => _subscription is not null;
    public int SubscriptionDisposeCount => Volatile.Read(ref _subscriptionDisposeCount);

    /// <summary>
    /// Replaces the subscription's default teardown (complete the stream) so a test can make it
    /// throw or hang without a bespoke <see cref="INatsChannelSubscription"/> implementation. The
    /// dispose count still increments, so exactly-once teardown stays assertable.
    /// </summary>
    public Func<ValueTask>? SubscriptionDisposeOverride { get; set; }

    /// <summary>The lifetime token the channel handed to the live subscription.</summary>
    public CancellationToken SubscriptionLifetime { get; private set; }

    public Task<NatsDeliveryOutcome> RequestAsync(string subject, string? payload, bool probe, TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (RequestException is not null)
            throw RequestException;

        Requests.Add((subject, payload, probe));
        var outcome = OutcomeForProbe is not null && probe
            ? OutcomeForProbe(probe)
            : !probe && DeliveryOutcomes.Count > 0
                ? DeliveryOutcomes.Dequeue()
                : NextOutcome;

        if (!probe && outcome != NatsDeliveryOutcome.NoResponders && _subscription is not null && payload is not null)
            _subscription.Push(new NatsInboundResponse(payload, false, () => ValueTask.CompletedTask));

        return Task.FromResult(outcome);
    }

    public Task<INatsChannelSubscription> SubscribeAsync(string subject, CancellationToken cancellationToken)
    {
        SubscribedSubjects.Add(subject);
        SubscriptionLifetime = cancellationToken;
        _subscription = new FakeSubscription(this, cancellationToken);
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

    public void PushWithReplyFailure(string? payload, Exception exception)
        => _subscription!.Push(new NatsInboundResponse(payload, IsProbe: false, () => throw exception));

    /// <summary>Faults the live subscription's read loop with <paramref name="exception"/>.</summary>
    public void FailSubscription(Exception exception) => _subscription!.Fail(exception);

    private sealed class FakeSubscription : INatsChannelSubscription
    {
        private readonly FakeNatsResponseChannelClient _owner;
        private readonly Channel<NatsInboundResponse> _channel = Channel.CreateUnbounded<NatsInboundResponse>();

        public FakeSubscription(FakeNatsResponseChannelClient owner, CancellationToken lifetime)
        {
            _owner = owner;
            // Like the real adapter: the read ends GRACEFULLY when the lifetime token handed to
            // SubscribeAsync ends the subscription (the channel's backstop cancel when teardown
            // failed or hung). Completing the channel ends ReadAllAsync without throwing;
            // Fail(exception) still propagates because its faulted completion wins first.
            lifetime.Register(() => _channel.Writer.TryComplete());
        }

        public void Push(NatsInboundResponse message) => _channel.Writer.TryWrite(message);

        public void Fail(Exception exception) => _channel.Writer.TryComplete(exception);

        public async IAsyncEnumerable<NatsInboundResponse> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var message in _channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                yield return message;
        }

        public ValueTask DisposeAsync()
        {
            Interlocked.Increment(ref _owner._subscriptionDisposeCount);
            if (_owner.SubscriptionDisposeOverride is { } teardown)
                return teardown();

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

/// <summary>A payload that does not override <see cref="IAsyncResponsePayload.OnRecovery"/>.</summary>
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
