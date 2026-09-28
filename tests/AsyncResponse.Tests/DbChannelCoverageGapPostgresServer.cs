using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace AsyncResponse.Tests;

/// <summary>
/// A loopback PostgreSQL wire-protocol (v3) server that answers each statement with a scripted
/// result set, so the PostgreSQL channel store runs its real queries without a container. Unlike
/// <see cref="FakePostgresWireServer"/> (parameterless statements only, never a row), every
/// extended-protocol statement is handed to <see cref="Respond"/> with its bound parameters and
/// answered with rows in Npgsql's binary format (uuid, text, timestamptz, int8, int4, bool), a
/// command tag, an error with a chosen SQLSTATE, or a dropped socket. Connect with
/// <see cref="ConnectionString"/> (trust auth, no TLS, no type loading).
/// </summary>
internal sealed class DbChannelCoverageGapPostgresServer : IAsyncDisposable
{
    private const int SslRequestCode = 80877103;
    private const int GssEncRequestCode = 80877104;
    private const int CancelRequestCode = 80877102;

    public const int Bool = 16;
    public const int Int8 = 20;
    public const int Int4 = 23;
    public const int Text = 25;
    public const int TimestampTz = 1184;
    public const int Uuid = 2950;
    public const int TimeTz = 1266;

    private static readonly DateTime PostgresEpoch = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentQueue<Statement> _statements = new();
    private readonly List<Task> _served = [];
    private readonly Task _acceptLoop;

    public DbChannelCoverageGapPostgresServer()
    {
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoop = Task.Run(AcceptLoopAsync);
    }

    public int Port { get; }

    /// <summary>A pooled connection string for this server.</summary>
    public string ConnectionString()
        => $"Host=127.0.0.1;Port={Port};Username=fake;Password=fake;Database=fake;SSL Mode=Disable;Gss Encryption Mode=Disable;" +
           "Server Compatibility Mode=NoTypeLoading;No Reset On Close=true;Timeout=5;Command Timeout=30";

    /// <summary>How a statement is answered. Default: completed with no rows.</summary>
    public Func<Statement, Task<Reply>> Respond { get; set; } = statement => Task.FromResult(Reply.Command(statement.Sql));

    /// <summary>Every statement received so far, in arrival order.</summary>
    public IReadOnlyList<Statement> Statements => _statements.ToArray();

    /// <summary>One executed statement: its SQL and its bound parameter values (raw binary, <c>null</c> for SQL NULL).</summary>
    public sealed record Statement(int Session, string Sql, IReadOnlyList<byte[]?> Parameters)
    {
        /// <summary>Parameter <paramref name="index"/> (0-based) as UTF-8 text.</summary>
        public string? Text(int index) => Parameters[index] is { } bytes ? Encoding.UTF8.GetString(bytes) : null;

        /// <summary>Parameter <paramref name="index"/> (0-based) as a uuid.</summary>
        public Guid Uuid(int index) => new(Parameters[index]!, bigEndian: true);

        public bool Contains(string fragment) => Sql.Contains(fragment, StringComparison.Ordinal);
    }

    public enum ReplyKind
    {
        Rows,
        Command,
        Error,
        Close
    }

    /// <summary>How one statement is answered.</summary>
    public sealed record Reply(ReplyKind Kind)
    {
        public (string Name, int Oid)[] Columns { get; init; } = [];
        public IReadOnlyList<object?[]> Values { get; init; } = [];
        public string Tag { get; init; } = "SELECT 0";
        public string SqlState { get; init; } = "55000";
        public string Message { get; init; } = string.Empty;

        /// <summary>A result set: <paramref name="columns"/> described, one row per entry of <paramref name="rows"/>.</summary>
        public static Reply Rows((string Name, int Oid)[] columns, params object?[][] rows)
            => new(ReplyKind.Rows) { Columns = columns, Values = rows, Tag = $"SELECT {rows.Length}" };

        /// <summary>No result set; tagged with the statement's first word.</summary>
        public static Reply Command(string sql)
            => new(ReplyKind.Command) { Tag = sql.TrimStart().Split(' ', ';', '\n')[0].Trim().ToUpperInvariant() switch
            {
                "INSERT" => "INSERT 0 1",
                "UPDATE" => "UPDATE 1",
                "DELETE" => "DELETE 1",
                var word => word
            } };

        /// <summary>An ordinary statement error with <paramref name="sqlState"/> (40001 is transient to Npgsql; 22P02 is not).</summary>
        public static Reply Error(string sqlState, string message) => new(ReplyKind.Error) { SqlState = sqlState, Message = message };

        /// <summary>Drop the socket without answering.</summary>
        public static Reply Close() => new(ReplyKind.Close);
    }

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

    private sealed class Pending
    {
        public string Sql = string.Empty;
        public List<byte[]?> Parameters = [];
        public List<char> Messages = [];
    }

    private int _sessions;

    private async Task ServeAsync(TcpClient client)
    {
        using var _ = client;
        var stream = client.GetStream();
        var token = _stop.Token;
        try
        {
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
                    return;
                break;
            }

            var session = Interlocked.Increment(ref _sessions);
            await stream.WriteAsync(StartupResponse(session), token).ConfigureAwait(false);

            var batch = new List<Pending>();
            Pending? current = null;
            var inTransaction = false;
            while (true)
            {
                var type = new byte[1];
                if (await stream.ReadAsync(type, token).ConfigureAwait(false) == 0)
                    return;
                var length = BinaryPrimitives.ReadInt32BigEndian(await ReadExactAsync(stream, 4, token).ConfigureAwait(false));
                var body = await ReadExactAsync(stream, length - 4, token).ConfigureAwait(false);
                switch ((char)type[0])
                {
                    case 'X':
                        return;
                    case 'Q':
                    {
                        // Simple query (transaction control): completed, with the status tracked.
                        var sql = Encoding.UTF8.GetString(body, 0, Array.IndexOf(body, (byte)0));
                        var word = sql.TrimStart().Split(' ', ';')[0].ToUpperInvariant();
                        if (word is "BEGIN" or "START")
                            inTransaction = true;
                        else if (word is "COMMIT" or "ROLLBACK" or "END")
                            inTransaction = false;
                        var buffer = new MemoryStream();
                        Write(buffer, 'C', CString(word));
                        Write(buffer, 'Z', [(byte)(inTransaction ? 'T' : 'I')]);
                        await stream.WriteAsync(buffer.ToArray(), token).ConfigureAwait(false);
                        break;
                    }
                    case 'P':
                    {
                        var nameEnd = Array.IndexOf(body, (byte)0);
                        current = new Pending
                        {
                            Sql = Encoding.UTF8.GetString(body, nameEnd + 1, Array.IndexOf(body, (byte)0, nameEnd + 1) - nameEnd - 1)
                        };
                        current.Messages.Add('P');
                        batch.Add(current);
                        break;
                    }
                    case 'B':
                        current ??= NewUnparsed(batch);
                        current.Parameters = ParseBind(body);
                        current.Messages.Add('B');
                        break;
                    case 'D':
                        (current ??= NewUnparsed(batch)).Messages.Add(body[0] == (byte)'S' ? 's' : 'D');
                        break;
                    case 'E':
                        (current ??= NewUnparsed(batch)).Messages.Add('E');
                        current = null;
                        break;
                    case 'S':
                    {
                        var buffer = new MemoryStream();
                        foreach (var statement in batch)
                        {
                            var word = statement.Sql.TrimStart().Split(' ', ';')[0].ToUpperInvariant();
                            if (word is "BEGIN" or "START")
                                inTransaction = true;
                            else if (word is "COMMIT" or "ROLLBACK" or "END")
                                inTransaction = false;
                            var recorded = new Statement(session, statement.Sql, statement.Parameters);
                            _statements.Enqueue(recorded);
                            var reply = await Respond(recorded).WaitAsync(token).ConfigureAwait(false);
                            if (reply.Kind == ReplyKind.Close)
                                return;
                            if (!Answer(buffer, statement, reply))
                                break;
                        }

                        Write(buffer, 'Z', [(byte)(inTransaction ? 'T' : 'I')]);
                        await stream.WriteAsync(buffer.ToArray(), token).ConfigureAwait(false);
                        batch.Clear();
                        current = null;
                        break;
                    }
                    default:
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

    private static Pending NewUnparsed(List<Pending> batch)
    {
        var pending = new Pending();
        batch.Add(pending);
        return pending;
    }

    /// <summary>Writes one statement's responses; <c>false</c> when it failed and the rest of the batch is skipped.</summary>
    private static bool Answer(MemoryStream buffer, Pending statement, Reply reply)
    {
        if (reply.Kind == ReplyKind.Error)
        {
            Write(buffer, 'E', [.. Field('S', "ERROR"), .. Field('V', "ERROR"), .. Field('C', reply.SqlState), .. Field('M', reply.Message), 0]);
            return false;
        }

        foreach (var message in statement.Messages)
        {
            switch (message)
            {
                case 'P': Write(buffer, '1', []); break;
                case 'B': Write(buffer, '2', []); break;
                case 's': Write(buffer, 't', [0, 0]); Write(buffer, 'n', []); break;
                case 'D':
                    if (reply.Kind == ReplyKind.Rows)
                        Write(buffer, 'T', RowDescription(reply.Columns));
                    else
                        Write(buffer, 'n', []);
                    break;
                case 'E':
                    if (reply.Kind == ReplyKind.Rows)
                    {
                        foreach (var row in reply.Values)
                            Write(buffer, 'D', DataRow(reply.Columns, row));
                    }
                    Write(buffer, 'C', CString(reply.Tag));
                    break;
            }
        }

        return true;

        static byte[] Field(char code, string value) => [(byte)code, .. CString(value)];
    }

    private static List<byte[]?> ParseBind(byte[] body)
    {
        var offset = Array.IndexOf(body, (byte)0) + 1;        // portal
        offset = Array.IndexOf(body, (byte)0, offset) + 1;    // statement
        var formatCodes = BinaryPrimitives.ReadInt16BigEndian(body.AsSpan(offset));
        offset += 2 + 2 * formatCodes;
        var count = BinaryPrimitives.ReadInt16BigEndian(body.AsSpan(offset));
        offset += 2;
        var parameters = new List<byte[]?>(count);
        for (var i = 0; i < count; i++)
        {
            var length = BinaryPrimitives.ReadInt32BigEndian(body.AsSpan(offset));
            offset += 4;
            if (length < 0)
            {
                parameters.Add(null);
                continue;
            }

            parameters.Add(body.AsSpan(offset, length).ToArray());
            offset += length;
        }

        return parameters;
    }

    private static byte[] RowDescription((string Name, int Oid)[] columns)
    {
        var buffer = new MemoryStream();
        var count = new byte[2];
        BinaryPrimitives.WriteInt16BigEndian(count, (short)columns.Length);
        buffer.Write(count);
        foreach (var (name, oid) in columns)
        {
            buffer.Write(CString(name));
            var field = new byte[18];
            BinaryPrimitives.WriteInt32BigEndian(field.AsSpan(0), 0);            // table oid
            BinaryPrimitives.WriteInt16BigEndian(field.AsSpan(4), 0);            // attribute number
            BinaryPrimitives.WriteInt32BigEndian(field.AsSpan(6), oid);          // type oid
            BinaryPrimitives.WriteInt16BigEndian(field.AsSpan(10), TypeLength(oid));
            BinaryPrimitives.WriteInt32BigEndian(field.AsSpan(12), -1);          // type modifier
            BinaryPrimitives.WriteInt16BigEndian(field.AsSpan(16), 1);           // binary
            buffer.Write(field);
        }

        return buffer.ToArray();

        static short TypeLength(int oid) => oid switch
        {
            Bool => 1,
            Int4 => 4,
            Int8 or TimestampTz => 8,
            TimeTz => 12,
            Uuid => 16,
            _ => -1
        };
    }

    private static byte[] DataRow((string Name, int Oid)[] columns, object?[] row)
    {
        var buffer = new MemoryStream();
        var count = new byte[2];
        BinaryPrimitives.WriteInt16BigEndian(count, (short)columns.Length);
        buffer.Write(count);
        for (var i = 0; i < columns.Length; i++)
        {
            var value = i < row.Length ? row[i] : null;
            var length = new byte[4];
            if (value is null)
            {
                BinaryPrimitives.WriteInt32BigEndian(length, -1);
                buffer.Write(length);
                continue;
            }

            var bytes = Encode(columns[i].Oid, value);
            BinaryPrimitives.WriteInt32BigEndian(length, bytes.Length);
            buffer.Write(length);
            buffer.Write(bytes);
        }

        return buffer.ToArray();
    }

    private static byte[] Encode(int oid, object value)
    {
        switch (oid)
        {
            case Bool:
                return [(byte)((bool)value ? 1 : 0)];
            case Int4:
            {
                var bytes = new byte[4];
                BinaryPrimitives.WriteInt32BigEndian(bytes, Convert.ToInt32(value, System.Globalization.CultureInfo.InvariantCulture));
                return bytes;
            }
            case Int8:
            {
                var bytes = new byte[8];
                BinaryPrimitives.WriteInt64BigEndian(bytes, Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture));
                return bytes;
            }
            case TimestampTz:
            {
                var utc = value switch
                {
                    DateTimeOffset dto => dto.UtcDateTime,
                    DateTime dt => dt.ToUniversalTime(),
                    _ => throw new ArgumentException($"Not a timestamp: {value}")
                };
                var bytes = new byte[8];
                BinaryPrimitives.WriteInt64BigEndian(bytes, (utc - PostgresEpoch).Ticks / 10);
                return bytes;
            }
            case Uuid:
                return ((Guid)value).ToByteArray(bigEndian: true);
            case TimeTz:
            {
                // Microseconds since midnight, then the zone as seconds WEST of UTC.
                var dto = (DateTimeOffset)value;
                var bytes = new byte[12];
                BinaryPrimitives.WriteInt64BigEndian(bytes, dto.TimeOfDay.Ticks / 10);
                BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(8), -(int)dto.Offset.TotalSeconds);
                return bytes;
            }
            default:
                return Encoding.UTF8.GetBytes(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)!);
        }
    }

    private static byte[] StartupResponse(int session)
    {
        var buffer = new MemoryStream();
        Write(buffer, 'R', [0, 0, 0, 0]);
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
}
