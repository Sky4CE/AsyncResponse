using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Net;
using System.Diagnostics;
using System.Text.Json;

namespace AsyncResponse.Transports.NATS;

/// <summary>
/// Publishes <see cref="WorkerJobEnvelope"/> messages to a NATS JetStream subject.
/// </summary>
/// <remarks>
/// JetStream provides durable queueing and explicit acknowledgement. The correlation id travels as a
/// message header so the consuming side can correlate without parsing the body, and transient NATS
/// failures are retried with bounded exponential backoff before the exception is returned to the
/// caller.
/// </remarks>
public sealed class NatsWorkerTransport : IWorkerTransport
{
    private readonly NatsAsyncResponseTransportOptions _options;
    private readonly INatsJetStreamTransport _jetStream;
    private readonly NatsTransportSubjectSchema _schema;
    private readonly SemaphoreSlim _ensureStreamGate = new(1, 1);
    private bool _streamEnsured;

    /// <summary>Runs the NatsWorkerTransport operation.</summary>
    public NatsWorkerTransport(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsConnection connection)
        : this(options, new NatsJetStreamTransportAdapter(connection.CreateJetStreamContext()))
    {
    }

    internal NatsWorkerTransport(
        IOptions<NatsAsyncResponseTransportOptions> options,
        INatsJetStreamTransport jetStream)
    {
        _options = options.Value;
        NatsTransportOptionsValidator.ValidateCommon(_options);
        _jetStream = jetStream;
        _schema = new NatsTransportSubjectSchema(_options);
    }

    /// <summary>Publishes the supplied message.</summary>
    public async Task PublishAsync(WorkerJobEnvelope job, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(job);

        using var activity = AsyncResponseDiagnostics.StartActivity(
            "asyncresponse.worker.publish",
            ActivityKind.Producer,
            job.CorrelationId);
        activity?.SetTag("asyncresponse.transport", "nats");
        activity?.SetTag("messaging.system", "nats");
        activity?.SetTag("messaging.destination.name", _schema.WorkerSubject);
        AsyncResponseDiagnostics.SetReplyTarget(activity, job.ReplyTarget);
        AsyncResponseDiagnostics.SetWorker(activity, job.Call);

        try
        {
            if (_options.CreateStreams)
                await EnsureWorkerStreamOnceAsync(cancellationToken).ConfigureAwait(false);

            var headers = string.IsNullOrWhiteSpace(job.CorrelationId)
                ? null
                : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
                {
                    [_options.CorrelationIdHeader] = job.CorrelationId!
                };

            var payload = AsyncResponseJson.Serialize(job);
            var sequence = await NatsTransportRetry.ExecuteAsync(
                token => _jetStream.PublishAsync(_schema.WorkerSubject, payload, headers, token),
                _options.PublishMaxAttempts,
                _options.PublishRetryBaseDelay,
                _options.PublishRetryMaxDelay,
                cancellationToken).ConfigureAwait(false);

            activity?.SetTag("messaging.message.id", sequence);
        }
        catch (Exception ex)
        {
            AsyncResponseDiagnostics.SetError(activity, ex);
            throw;
        }
    }

    private async Task EnsureWorkerStreamOnceAsync(CancellationToken cancellationToken)
    {
        if (_streamEnsured)
            return;

        await _ensureStreamGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!_streamEnsured)
            {
                await _jetStream.EnsureStreamAsync(
                    _schema.WorkerStream,
                    _schema.WorkerSubject,
                    _options.StreamMaxMessages,
                    cancellationToken).ConfigureAwait(false);
                _streamEnsured = true;
            }
        }
        finally
        {
            _ensureStreamGate.Release();
        }
    }
}
