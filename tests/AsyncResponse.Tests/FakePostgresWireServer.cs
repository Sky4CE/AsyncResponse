using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace AsyncResponse.Tests;

/// <summary>
/// A loopback server speaking just enough of the PostgreSQL wire protocol (v3) for Npgsql to open
/// a connection and run parameterless statements, so a unit test can put a real
/// <c>NpgsqlConnection</c> into states no container produces on cue: a statement the server never
/// answers (a half-open socket), answers with an error, or answers and then drops the socket.
/// Connect with <see cref="ConnectionString"/> (trust auth, no TLS, no type loading).
/// <para>
/// Each statement batch (everything up to a <c>Sync</c>) is answered by <see cref="Respond"/>,
/// awaited, so a test holds a statement by returning a task it completes later. Sessions are
/// numbered from 1 in the order they complete startup; a cancel request arrives on a connection
/// of its own and is only counted.
/// </para>
/// </summary>
internal sealed class FakePostgresWireServer : IAsyncDisposable
{
    private const int SslRequestCode = 80877103;
    private const int GssEncRequestCode = 80877104;
    private const int CancelRequestCode = 80877102;

    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentDictionary<int, TaskCompletionSource> _terminated = new();
    private readonly ConcurrentDictionary<int, TaskCompletionSource> _dropped = new();
    private readonly ConcurrentQueue<(int Connection, string Sql)> _statements = new();
    private readonly List<Task> _served = [];
    private readonly Task _acceptLoop;
    private int _sessions;
    private int _cancelRequests;

    public FakePostgresWireServer()
    {
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoop = Task.Run(AcceptLoopAsync);
    }

    public int Port { get; }

    /// <summary>A pooled connection string for this server; <paramref name="extra"/> is appended verbatim.</summary>
    public string ConnectionString(string extra = "")
        => $"Host=127.0.0.1;Port={Port};Username=fake;Password=fake;Database=fake;SSL Mode=Disable;Gss Encryption Mode=Disable;" +
           $"Server Compatibility Mode=NoTypeLoading;No Reset On Close=true;Timeout=5;{extra}";

    /// <summary>How a statement batch is answered. Default: completed with the statement's first word as its tag.</summary>
    public Func<int, string, Task<Reply>> Respond { get; set; } = (_, sql) => Task.FromResult(Reply.Complete(sql));

    /// <summary>Sessions that completed startup so far (a pooled reuse opens none).</summary>
    public int Sessions => Volatile.Read(ref _sessions);

    public int CancelRequests => Volatile.Read(ref _cancelRequests);

    public IReadOnlyList<(int Connection, string Sql)> Statements => _statements.ToArray();

    /// <summary>Completes once session <paramref name="session"/> sent Terminate — the client closed a healthy connection for good rather than pooling it.</summary>
    public Task Terminated(int session) => _terminated.GetOrAdd(session, _ => NewSignal()).Task;

    /// <summary>Completes once the client dropped session <paramref name="session"/>'s socket without a Terminate (a connection it had broken).</summary>
    public Task Dropped(int session) => _dropped.GetOrAdd(session, _ => NewSignal()).Task;

    private static TaskCompletionSource NewSignal() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_stop.Token).ConfigureAwait(false);
                lock (_served)
                    _served.Add(Task.Run(() => ServeAsync(client)));
            }
        }
        catch (Exception) when (_stop.IsCancellationRequested)
        {
        }
    }

    private async Task ServeAsync(TcpClient client)
    {
        using var _ = client;
        var stream = client.GetStream();
        var token = _stop.Token;
        try
        {
            // Startup: refuse TLS/GSS negotiation, count cancel requests, accept any protocol.
            while (true)
            {
                var length = BinaryPrimitives.ReadInt32BigEndian(await ReadExactAsync(stream, 4, token).ConfigureAwait(false));
                var body = await ReadExactAsync(stream, length - 4, token).ConfigureAwait(false);
                var code = BinaryPrimitives.ReadInt32BigEndian(body);
                if (code is SslRequestCode or GssEncRequestCode)
                {
                    await stream.WriteAsync("N"u8.ToArray(), token).ConfigureAwait(false);
                    continue;
                }

                if (code == CancelRequestCode)
                {
                    Interlocked.Increment(ref _cancelRequests);
                    return;
                }

                break;
            }

            var session = Interlocked.Increment(ref _sessions);
            await stream.WriteAsync(StartupResponse(session), token).ConfigureAwait(false);

            var pending = new List<char>();
            string? sql = null;
            while (true)
            {
                var type = new byte[1];
                if (await stream.ReadAsync(type, token).ConfigureAwait(false) == 0)
                {
                    _dropped.GetOrAdd(session, _ => NewSignal()).TrySetResult();
                    return;
                }
                var length = BinaryPrimitives.ReadInt32BigEndian(await ReadExactAsync(stream, 4, token).ConfigureAwait(false));
                var body = await ReadExactAsync(stream, length - 4, token).ConfigureAwait(false);
                switch ((char)type[0])
                {
                    case 'X':
                        _terminated.GetOrAdd(session, _ => NewSignal()).TrySetResult();
                        return;
                    case 'P':
                        // Parse: statement name, then the query text, both NUL-terminated.
                        var nameEnd = Array.IndexOf(body, (byte)0);
                        sql = Encoding.UTF8.GetString(body, nameEnd + 1, Array.IndexOf(body, (byte)0, nameEnd + 1) - nameEnd - 1);
                        pending.Add('P');
                        break;
                    case 'D':
                        pending.Add(body[0] == (byte)'S' ? 's' : 'D');
                        break;
                    case 'S':
                        var statement = sql ?? string.Empty;
                        _statements.Enqueue((session, statement));
                        var reply = await Respond(session, statement).WaitAsync(token).ConfigureAwait(false);
                        if (reply.Kind == ReplyKind.Close)
                            return;
                        await stream.WriteAsync(Answer(pending, reply), token).ConfigureAwait(false);
                        if (reply.CloseAfter)
                            return;
                        pending.Clear();
                        sql = null;
                        break;
                    default:
                        pending.Add((char)type[0]);
                        break;
                }
            }
        }
        catch (Exception) when (token.IsCancellationRequested)
        {
        }
        catch (IOException)
        {
        }
        catch (SocketException)
        {
        }
    }

    private static byte[] StartupResponse(int session)
    {
        var buffer = new MemoryStream();
        Write(buffer, 'R', [0, 0, 0, 0]); // AuthenticationOk
        foreach (var (name, value) in new[]
                 {
                     ("server_version", "16.0"), ("server_encoding", "UTF8"), ("client_encoding", "UTF8"),
                     ("DateStyle", "ISO, MDY"), ("integer_datetimes", "on"), ("TimeZone", "UTC"),
                     ("standard_conforming_strings", "on"), ("is_superuser", "off"), ("session_authorization", "fake")
                 })
        {
            Write(buffer, 'S', [.. CString(name), .. CString(value)]);
        }

        var keyData = new byte[8];
        BinaryPrimitives.WriteInt32BigEndian(keyData, session);
        BinaryPrimitives.WriteInt32BigEndian(keyData.AsSpan(4), session);
        Write(buffer, 'K', keyData);
        Write(buffer, 'Z', "I"u8.ToArray());
        return buffer.ToArray();
    }

    private static byte[] Answer(List<char> batch, Reply reply)
    {
        var buffer = new MemoryStream();
        if (reply.Kind == ReplyKind.Error)
        {
            // An error discards the rest of the batch; the server answers the Sync regardless.
            // 55000 (object_not_in_prerequisite_state): an ordinary statement error. Npgsql breaks the
            // connection on critical classes (XX internal errors, 57P admin shutdowns), not on this.
            Write(buffer, 'E', [.. Field('S', "ERROR"), .. Field('V', "ERROR"), .. Field('C', "55000"), .. Field('M', reply.Text), 0]);
        }
        else
        {
            foreach (var message in batch)
            {
                switch (message)
                {
                    case 'P': Write(buffer, '1', []); break;               // ParseComplete
                    case 'B': Write(buffer, '2', []); break;               // BindComplete
                    case 's': Write(buffer, 't', [0, 0]); Write(buffer, 'n', []); break; // no parameters, no rows
                    case 'D': Write(buffer, 'n', []); break;               // NoData
                    case 'E': Write(buffer, 'C', CString(reply.Text)); break; // CommandComplete
                }
            }
        }

        Write(buffer, 'Z', "I"u8.ToArray());
        return buffer.ToArray();

        static byte[] Field(char code, string value) => [(byte)code, .. CString(value)];
    }

    private static byte[] CString(string value) => [.. Encoding.UTF8.GetBytes(value), 0];

    private static void Write(Stream buffer, char type, byte[] body)
    {
        var header = new byte[5];
        header[0] = (byte)type;
        BinaryPrimitives.WriteInt32BigEndian(header.AsSpan(1), body.Length + 4);
        buffer.Write(header);
        buffer.Write(body);
    }

    private static async Task<byte[]> ReadExactAsync(Stream stream, int count, CancellationToken token)
    {
        var buffer = new byte[count];
        await stream.ReadExactlyAsync(buffer, token).ConfigureAwait(false);
        return buffer;
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _listener.Stop();
        await _acceptLoop;
        Task[] served;
        lock (_served)
            served = [.. _served];
        await Task.WhenAll(served);
        _stop.Dispose();
    }

    public enum ReplyKind
    {
        Complete,
        Error,
        Close
    }

    /// <summary>How the server answers one statement batch.</summary>
    public readonly record struct Reply(ReplyKind Kind, string Text, bool CloseAfter = false)
    {
        /// <summary>Success, tagged with the statement's first word (<c>LISTEN</c>, <c>UNLISTEN</c>, ...).</summary>
        public static Reply Complete(string sql, bool closeAfter = false)
            => new(ReplyKind.Complete, sql.Split(' ', ';')[0].Trim(), closeAfter);

        public static Reply Error(string message) => new(ReplyKind.Error, message);

        /// <summary>Drop the socket without answering.</summary>
        public static Reply Close() => new(ReplyKind.Close, string.Empty);
    }
}
