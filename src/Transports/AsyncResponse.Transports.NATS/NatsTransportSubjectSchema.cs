namespace AsyncResponse.Transports.NATS;

/// <summary>
/// Resolves NATS subject and JetStream stream names from transport options. NATS stream names cannot
/// contain dots, so a stream defaulted from the subject prefix replaces dots with underscores. These
/// names are deployment contracts: changing them while messages are in flight strands unprocessed
/// worker or response messages on the old stream.
/// </summary>
internal sealed class NatsTransportSubjectSchema(NatsAsyncResponseTransportOptions _options)
{
    public string WorkerSubject => ResolveSubject(_options.WorkerSubject, "worker");
    public string ResponseSubject => ResolveSubject(_options.ResponseSubject, "response");
    public string DeadLetterSubject => ResolveSubject(_options.DeadLetterSubject, "deadletter");

    public string WorkerStream => ResolveStream(_options.WorkerStream, WorkerSubject);
    public string ResponseStream => ResolveStream(_options.ResponseStream, ResponseSubject);
    public string DeadLetterStream => ResolveStream(_options.DeadLetterStream, DeadLetterSubject);

    private string ResolveSubject(string? configured, string role)
        => !string.IsNullOrWhiteSpace(configured)
            ? configured
            : $"{_options.SubjectPrefix}.transport.{role}";

    private static string ResolveStream(string? configured, string subject)
        => !string.IsNullOrWhiteSpace(configured)
            ? configured
            : SanitizeStreamName(subject);

    /// <summary>Turns a subject into a valid JetStream stream name (NATS stream names forbid dots and wildcards).</summary>
    internal static string SanitizeStreamName(string subject)
    {
        // Options validation caps subjects at 255 chars, but this must stay safe for any caller:
        // a stackalloc sized by an unbounded input is a stack overflow — an uncatchable process
        // kill — so longer inputs take the heap path.
        Span<char> buffer = subject.Length <= 256
            ? stackalloc char[subject.Length]
            : new char[subject.Length];
        for (var i = 0; i < subject.Length; i++)
        {
            var c = subject[i];
            buffer[i] = char.IsAsciiLetterOrDigit(c) || c is '-' or '_' ? c : '_';
        }

        return new string(buffer);
    }
}
