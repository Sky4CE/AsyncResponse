using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace AsyncResponse.Tests;

/// <summary>
/// A loopback server speaking just enough TDS 7.4 for Microsoft.Data.SqlClient to log in without
/// TLS and run commands, so the SQL Server channel store executes its real statements without a
/// container. Every SQL batch and every <c>sp_executesql</c> RPC is handed to <see cref="Respond"/>
/// with its statement text and its decoded parameter values, and answered with scripted result sets
/// (uniqueidentifier, nvarchar, datetime2, datetimeoffset, bigint, int, bit), a rows-affected count,
/// a server error with a chosen number, or a dropped socket. Attention (cancel) packets are
/// acknowledged. Connect with <see cref="ConnectionString"/> (no pooling, no encryption).
/// </summary>
internal sealed class DbChannelCoverageGapSqlServer : IAsyncDisposable
{
    private static readonly byte[] Collation = [0x09, 0x04, 0xD0, 0x00, 0x34]; // Latin1_General_CI_AS

    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentQueue<Statement> _statements = new();
    private readonly List<Task> _served = [];
    private readonly Task _acceptLoop;

    public DbChannelCoverageGapSqlServer()
    {
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoop = Task.Run(AcceptLoopAsync);
    }

    public int Port { get; }

    public string ConnectionString()
        => $"Server=127.0.0.1,{Port};Database=fake;User Id=fake;Password=fake;Encrypt=False;Pooling=false;Connect Timeout=5;Command Timeout=30";

    /// <summary>How a statement is answered. Default: one row affected, no result set.</summary>
    public Func<Statement, Task<Reply>> Respond { get; set; } = _ => Task.FromResult(Reply.Affected(1));

    public IReadOnlyList<Statement> Statements => _statements.ToArray();

    /// <summary>One executed batch or RPC: its text and its parameters by name (without the <c>@</c>).</summary>
    public sealed record Statement(string Sql, IReadOnlyDictionary<string, object?> Parameters)
    {
        public bool Contains(string fragment) => Sql.Contains(fragment, StringComparison.Ordinal);

        public object? this[string name] => Parameters.TryGetValue(name, out var value) ? value : null;
    }

    /// <summary>Column type of a scripted result set.</summary>
    public enum Type
    {
        Guid,
        NVarChar,
        NVarCharMax,
        DateTime2,
        DateTimeOffset,
        BigInt,
        Int,
        Bit
    }

    public enum ReplyKind
    {
        Results,
        Error,
        Close
    }

    /// <summary>One result set, or (with no columns) a rows-affected count.</summary>
    public sealed record Result((string Name, Type Type)[] Columns, IReadOnlyList<object?[]> Rows, long Affected);

    public sealed record Reply(ReplyKind Kind)
    {
        public IReadOnlyList<Result> Results { get; init; } = [];
        public int Number { get; init; }
        public byte Class { get; init; } = 16;
        public string Message { get; init; } = string.Empty;

        public static Reply Rows((string Name, Type Type)[] columns, params object?[][] rows)
            => new(ReplyKind.Results) { Results = [new Result(columns, rows, rows.Length)] };

        public static Reply Affected(long count) => new(ReplyKind.Results) { Results = [new Result([], [], count)] };

        /// <summary>A server error (1205 deadlock is transient; 2627 is a primary-key violation).</summary>
        public static Reply Error(int number, string message, byte @class = 16) => new(ReplyKind.Error) { Number = number, Message = message, Class = @class };

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

    private async Task ServeAsync(TcpClient client)
    {
        using var _ = client;
        var stream = client.GetStream();
        var token = _stop.Token;
        var writeGate = new SemaphoreSlim(1, 1);
        try
        {
            // PRELOGIN: no encryption supported, so the login travels in the clear.
            var (preloginType, _) = await ReadMessageAsync(stream, token).ConfigureAwait(false);
            if (preloginType != 0x12)
                return;
            await WritePacketsAsync(stream, 0x04, PreloginResponse(), token).ConfigureAwait(false);

            var (loginType, _) = await ReadMessageAsync(stream, token).ConfigureAwait(false);
            if (loginType != 0x10)
                return;
            await WritePacketsAsync(stream, 0x04, LoginResponse(), token).ConfigureAwait(false);

            // Requests are read on their own loop so an attention (cancel) packet is seen while a
            // scripted reply is still pending.
            var requests = System.Threading.Channels.Channel.CreateUnbounded<(byte Type, byte[] Body)>();
            var state = new ConnectionState();
            var reader = Task.Run(async () =>
            {
                try
                {
                    while (true)
                    {
                        var message = await ReadMessageAsync(stream, token).ConfigureAwait(false);
                        if (message.Type == 0x06)
                        {
                            bool acknowledgeNow;
                            lock (state)
                            {
                                acknowledgeNow = !state.InProgress;
                                if (!acknowledgeNow)
                                {
                                    state.AttentionPending = true;
                                    state.Attention.Cancel();
                                }
                            }

                            if (acknowledgeNow)
                                await WriteLockedAsync(writeGate, stream, Done(0xFD, 0x0020, 0, 0), token).ConfigureAwait(false);
                            continue;
                        }

                        lock (state)
                            state.InProgress = true;
                        await requests.Writer.WriteAsync(message, token).ConfigureAwait(false);
                    }
                }
                catch (Exception)
                {
                    requests.Writer.TryComplete();
                }
            }, token);

            await foreach (var (type, body) in requests.Reader.ReadAllAsync(token).ConfigureAwait(false))
            {
                CancellationToken attention;
                lock (state)
                    attention = state.Attention.Token;

                byte[] response;
                if (type == 0x0E)
                {
                    // Transaction manager request: acknowledged without a transaction.
                    response = Done(0xFD, 0, 0, 0);
                }
                else
                {
                    var statement = type == 0x03 ? ParseRpc(body) : ParseBatch(body);
                    _statements.Enqueue(statement);
                    Reply? reply;
                    try
                    {
                        using var pending = CancellationTokenSource.CreateLinkedTokenSource(token, attention);
                        reply = await Respond(statement).WaitAsync(pending.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (attention.IsCancellationRequested && !token.IsCancellationRequested)
                    {
                        reply = null;
                    }

                    if (reply?.Kind == ReplyKind.Close)
                        return;
                    response = reply is null ? [] : Response(reply, rpc: type == 0x03);
                }

                bool attentionPending;
                lock (state)
                {
                    attentionPending = state.AttentionPending;
                    state.AttentionPending = false;
                    state.InProgress = false;
                    if (attentionPending)
                    {
                        state.Attention.Dispose();
                        state.Attention = new CancellationTokenSource();
                    }
                }

                if (attentionPending)
                    response = [.. response, .. Done(0xFD, 0x0020, 0, 0)];
                await WriteLockedAsync(writeGate, stream, response, token).ConfigureAwait(false);
            }

            await reader.ConfigureAwait(false);
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

    private sealed class ConnectionState
    {
        public bool InProgress;
        public bool AttentionPending;
        public CancellationTokenSource Attention = new();
    }

    private static async Task WriteLockedAsync(SemaphoreSlim gate, Stream stream, byte[] tokens, CancellationToken token)
    {
        await gate.WaitAsync(token).ConfigureAwait(false);
        try
        {
            await WritePacketsAsync(stream, 0x04, tokens, token).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    private static async Task<(byte Type, byte[] Body)> ReadMessageAsync(Stream stream, CancellationToken token)
    {
        var body = new MemoryStream();
        while (true)
        {
            var header = new byte[8];
            await stream.ReadExactlyAsync(header, token).ConfigureAwait(false);
            var length = BinaryPrimitives.ReadUInt16BigEndian(header.AsSpan(2));
            var payload = new byte[length - 8];
            await stream.ReadExactlyAsync(payload, token).ConfigureAwait(false);
            body.Write(payload);
            if ((header[1] & 0x01) != 0)
                return (header[0], body.ToArray());
        }
    }

    private static async Task WritePacketsAsync(Stream stream, byte type, byte[] payload, CancellationToken token)
    {
        const int MaxPayload = 4096 - 8;
        var output = new MemoryStream();
        var offset = 0;
        byte packetId = 1;
        do
        {
            var size = Math.Min(MaxPayload, payload.Length - offset);
            var last = offset + size >= payload.Length;
            var header = new byte[8];
            header[0] = type;
            header[1] = (byte)(last ? 0x01 : 0x00);
            BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(2), (ushort)(size + 8));
            header[6] = packetId++;
            output.Write(header);
            output.Write(payload, offset, size);
            offset += size;
        }
        while (offset < payload.Length);

        await stream.WriteAsync(output.ToArray(), token).ConfigureAwait(false);
        await stream.FlushAsync(token).ConfigureAwait(false);
    }

    private static byte[] PreloginResponse()
    {
        var options = new MemoryStream();
        var data = new MemoryStream();
        const int OptionCount = 5;
        var dataStart = OptionCount * 5 + 1;
        void Option(byte token, byte[] value)
        {
            options.WriteByte(token);
            var position = new byte[4];
            BinaryPrimitives.WriteUInt16BigEndian(position, (ushort)(dataStart + data.Length));
            BinaryPrimitives.WriteUInt16BigEndian(position.AsSpan(2), (ushort)value.Length);
            options.Write(position);
            data.Write(value);
        }

        Option(0x00, [16, 0, 0x07, 0xD0, 0, 0]); // VERSION 16.0.2000
        Option(0x01, [0x02]);                    // ENCRYPTION: not supported
        Option(0x02, [0x00]);                    // INSTOPT
        Option(0x03, [0, 0, 0, 0]);              // THREADID
        Option(0x04, [0x00]);                    // MARS off
        options.WriteByte(0xFF);
        return [.. options.ToArray(), .. data.ToArray()];
    }

    private static byte[] LoginResponse()
    {
        var tokens = new MemoryStream();
        tokens.Write(EnvChange(1, BVarChar("fake"), BVarChar("master")));
        tokens.Write(EnvChange(7, [5, .. Collation], [0]));
        tokens.Write(EnvChange(4, BVarChar("4096"), BVarChar("4096")));

        var loginAck = new MemoryStream();
        loginAck.WriteByte(0x01);                          // interface: SQL
        loginAck.Write([0x74, 0x00, 0x00, 0x04]);          // TDS 7.4
        loginAck.Write(BVarChar("Microsoft SQL Server"));
        loginAck.Write([16, 0, 0x07, 0xD0]);
        tokens.WriteByte(0xAD);
        tokens.Write(UInt16(loginAck.Length));
        tokens.Write(loginAck.ToArray());

        tokens.Write(Done(0xFD, 0, 0, 0));
        return tokens.ToArray();
    }

    private static byte[] EnvChange(byte type, byte[] newValue, byte[] oldValue)
    {
        var body = new byte[1 + newValue.Length + oldValue.Length];
        body[0] = type;
        newValue.CopyTo(body, 1);
        oldValue.CopyTo(body, 1 + newValue.Length);
        return [0xE3, .. UInt16(body.Length), .. body];
    }

    private static byte[] Response(Reply reply, bool rpc)
    {
        var tokens = new MemoryStream();
        if (reply.Kind == ReplyKind.Error)
        {
            var error = new MemoryStream();
            error.Write(Int32(reply.Number));
            error.WriteByte(1);                                 // state
            error.WriteByte(reply.Class);
            error.Write(UsVarChar(reply.Message));
            error.Write(BVarChar("fake"));
            error.Write(BVarChar(string.Empty));
            error.Write(Int32(1));
            tokens.WriteByte(0xAA);
            tokens.Write(UInt16(error.Length));
            tokens.Write(error.ToArray());
            tokens.Write(rpc ? Done(0xFE, 0x0002, 0, 0) : Done(0xFD, 0x0002, 0, 0));
            return tokens.ToArray();
        }

        for (var i = 0; i < reply.Results.Count; i++)
        {
            var result = reply.Results[i];
            var more = rpc || i < reply.Results.Count - 1;
            if (result.Columns.Length > 0)
            {
                tokens.Write(ColumnMetadata(result.Columns));
                foreach (var row in result.Rows)
                    tokens.Write(Row(result.Columns, row));
                tokens.Write(Done(rpc ? (byte)0xFF : (byte)0xFD, (ushort)(0x0010 | (more ? 0x0001 : 0)), 0xC1, result.Rows.Count));
            }
            else
            {
                tokens.Write(Done(rpc ? (byte)0xFF : (byte)0xFD, (ushort)(0x0010 | (more ? 0x0001 : 0)), 0xC3, result.Affected));
            }
        }

        if (rpc)
        {
            tokens.WriteByte(0x79);
            tokens.Write(Int32(0));
            tokens.Write(Done(0xFE, 0, 0xE0, 0));
        }

        return tokens.ToArray();
    }

    private static byte[] ColumnMetadata((string Name, Type Type)[] columns)
    {
        var metadata = new MemoryStream();
        metadata.WriteByte(0x81);
        metadata.Write(UInt16(columns.Length));
        foreach (var (name, type) in columns)
        {
            metadata.Write(Int32(0));      // user type
            metadata.Write(UInt16(0x0001)); // nullable
            metadata.Write(type switch
            {
                Type.Guid => [0x24, 16],
                Type.NVarChar => [0xE7, .. UInt16(8000), .. Collation],
                Type.NVarCharMax => [0xE7, 0xFF, 0xFF, .. Collation],
                Type.DateTime2 => [0x2A, 7],
                Type.DateTimeOffset => [0x2B, 7],
                Type.BigInt => [0x26, 8],
                Type.Int => [0x26, 4],
                Type.Bit => [0x68, 1],
                _ => throw new ArgumentOutOfRangeException(nameof(columns))
            });
            metadata.Write(BVarChar(name));
        }

        return metadata.ToArray();
    }

    private static byte[] Row((string Name, Type Type)[] columns, object?[] row)
    {
        var output = new MemoryStream();
        output.WriteByte(0xD1);
        for (var i = 0; i < columns.Length; i++)
        {
            var value = i < row.Length ? row[i] : null;
            switch (columns[i].Type)
            {
                case Type.NVarChar:
                    if (value is null)
                        output.Write([0xFF, 0xFF]);
                    else
                    {
                        var text = Encoding.Unicode.GetBytes((string)value);
                        output.Write(UInt16(text.Length));
                        output.Write(text);
                    }
                    break;
                case Type.NVarCharMax:
                    if (value is null)
                        output.Write(Enumerable.Repeat((byte)0xFF, 8).ToArray());
                    else
                    {
                        var text = Encoding.Unicode.GetBytes((string)value);
                        var length = new byte[8];
                        BinaryPrimitives.WriteInt64LittleEndian(length, text.Length);
                        output.Write(length);
                        if (text.Length > 0)
                        {
                            output.Write(Int32(text.Length));
                            output.Write(text);
                        }
                        output.Write(Int32(0));
                    }
                    break;
                default:
                    if (value is null)
                    {
                        output.WriteByte(0);
                        break;
                    }
                    var bytes = FixedValue(columns[i].Type, value);
                    output.WriteByte((byte)bytes.Length);
                    output.Write(bytes);
                    break;
            }
        }

        return output.ToArray();
    }

    private static byte[] FixedValue(Type type, object value)
    {
        switch (type)
        {
            case Type.Guid:
                return ((Guid)value).ToByteArray();
            case Type.BigInt:
            {
                var bytes = new byte[8];
                BinaryPrimitives.WriteInt64LittleEndian(bytes, Convert.ToInt64(value, System.Globalization.CultureInfo.InvariantCulture));
                return bytes;
            }
            case Type.Int:
                return Int32(Convert.ToInt32(value, System.Globalization.CultureInfo.InvariantCulture));
            case Type.Bit:
                return [(byte)((bool)value ? 1 : 0)];
            case Type.DateTime2:
            {
                var utc = value switch
                {
                    DateTimeOffset dto => dto.UtcDateTime,
                    DateTime dt => dt,
                    _ => throw new ArgumentException($"Not a datetime2: {value}")
                };
                return [.. TimeAndDate(utc)];
            }
            case Type.DateTimeOffset:
            {
                var dto = (DateTimeOffset)value;
                var offset = new byte[2];
                BinaryPrimitives.WriteInt16LittleEndian(offset, (short)dto.Offset.TotalMinutes);
                return [.. TimeAndDate(dto.UtcDateTime), .. offset];
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(type));
        }

        static byte[] TimeAndDate(DateTime value)
        {
            var time = new byte[8];
            BinaryPrimitives.WriteInt64LittleEndian(time, value.TimeOfDay.Ticks);
            var date = new byte[4];
            BinaryPrimitives.WriteInt32LittleEndian(date, (int)(value.Date - DateTime.MinValue).TotalDays);
            return [.. time.AsSpan(0, 5), .. date.AsSpan(0, 3)];
        }
    }

    private static Statement ParseBatch(byte[] body)
    {
        var headers = BinaryPrimitives.ReadInt32LittleEndian(body);
        return new Statement(Encoding.Unicode.GetString(body, headers, body.Length - headers), new Dictionary<string, object?>());
    }

    private static Statement ParseRpc(byte[] body)
    {
        var offset = BinaryPrimitives.ReadInt32LittleEndian(body);
        var nameLength = BinaryPrimitives.ReadUInt16LittleEndian(body.AsSpan(offset));
        offset += 2;
        offset += nameLength == 0xFFFF ? 2 : nameLength * 2;
        offset += 2; // option flags

        var parameters = new Dictionary<string, object?>(StringComparer.Ordinal);
        var sql = string.Empty;
        var index = 0;
        try
        {
            while (offset < body.Length)
            {
                var name = BVarCharAt(body, ref offset);
                offset++; // status flags
                var value = ParameterValue(body, ref offset);
                if (index == 0)
                    sql = value as string ?? string.Empty;
                else if (index > 1)
                    parameters[name.TrimStart('@')] = value;
                index++;
            }
        }
        catch (NotSupportedException)
        {
            // A parameter type this fake does not decode: the rest are left out.
        }

        return new Statement(sql, parameters);
    }

    private static object? ParameterValue(byte[] body, ref int offset)
    {
        var type = body[offset++];
        switch (type)
        {
            case 0xE7:
            case 0xA7:
            case 0xA5:
            case 0xAD:
            {
                var maxLength = BinaryPrimitives.ReadUInt16LittleEndian(body.AsSpan(offset));
                offset += 2;
                if (type is 0xE7 or 0xA7)
                    offset += 5;
                byte[]? bytes;
                if (maxLength == 0xFFFF)
                {
                    var total = BinaryPrimitives.ReadUInt64LittleEndian(body.AsSpan(offset));
                    offset += 8;
                    if (total == ulong.MaxValue)
                        bytes = null;
                    else
                    {
                        var chunks = new MemoryStream();
                        while (true)
                        {
                            var chunk = BinaryPrimitives.ReadInt32LittleEndian(body.AsSpan(offset));
                            offset += 4;
                            if (chunk == 0)
                                break;
                            chunks.Write(body, offset, chunk);
                            offset += chunk;
                        }
                        bytes = chunks.ToArray();
                    }
                }
                else
                {
                    var length = BinaryPrimitives.ReadUInt16LittleEndian(body.AsSpan(offset));
                    offset += 2;
                    if (length == 0xFFFF)
                        bytes = null;
                    else
                    {
                        bytes = body.AsSpan(offset, length).ToArray();
                        offset += length;
                    }
                }

                return bytes is null ? null
                    : type == 0xE7 ? Encoding.Unicode.GetString(bytes)
                    : type == 0xA7 ? Encoding.ASCII.GetString(bytes)
                    : bytes;
            }
            case 0x24:
            case 0x26:
            case 0x68:
            case 0x6D:
            case 0x6F:
            {
                offset++; // declared length
                var length = body[offset++];
                if (length == 0)
                    return null;
                var bytes = body.AsSpan(offset, length).ToArray();
                offset += length;
                return type switch
                {
                    0x24 => new Guid(bytes),
                    0x26 => length switch
                    {
                        1 => bytes[0],
                        2 => BinaryPrimitives.ReadInt16LittleEndian(bytes),
                        4 => BinaryPrimitives.ReadInt32LittleEndian(bytes),
                        _ => BinaryPrimitives.ReadInt64LittleEndian(bytes)
                    },
                    0x68 => bytes[0] != 0,
                    _ => bytes
                };
            }
            case 0x2A:
            case 0x2B:
            case 0x29:
            {
                offset++; // scale
                var length = body[offset++];
                if (length == 0)
                    return null;
                var bytes = body.AsSpan(offset, length).ToArray();
                offset += length;
                if (type == 0x29)
                    return bytes;
                var timeLength = length - 3 - (type == 0x2B ? 2 : 0);
                var timeBytes = new byte[8];
                bytes.AsSpan(0, timeLength).CopyTo(timeBytes);
                var dateBytes = new byte[4];
                bytes.AsSpan(timeLength, 3).CopyTo(dateBytes);
                var utc = DateTime.MinValue.AddDays(BinaryPrimitives.ReadInt32LittleEndian(dateBytes))
                    .AddTicks(BinaryPrimitives.ReadInt64LittleEndian(timeBytes));
                // Boxed per branch: a conditional would convert the DateTime to DateTimeOffset.
                if (type == 0x2B)
                    return new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Unspecified), TimeSpan.Zero);
                return DateTime.SpecifyKind(utc, DateTimeKind.Utc);
            }
            default:
                throw new NotSupportedException($"TDS parameter type 0x{type:X2}");
        }
    }

    private static string BVarCharAt(byte[] body, ref int offset)
    {
        var length = body[offset++];
        var value = Encoding.Unicode.GetString(body, offset, length * 2);
        offset += length * 2;
        return value;
    }

    private static byte[] Done(byte token, ushort status, ushort command, long count)
    {
        var done = new byte[13];
        done[0] = token;
        BinaryPrimitives.WriteUInt16LittleEndian(done.AsSpan(1), status);
        BinaryPrimitives.WriteUInt16LittleEndian(done.AsSpan(3), command);
        BinaryPrimitives.WriteInt64LittleEndian(done.AsSpan(5), count);
        return done;
    }

    private static byte[] BVarChar(string value) => [(byte)value.Length, .. Encoding.Unicode.GetBytes(value)];

    private static byte[] UsVarChar(string value) => [.. UInt16(value.Length), .. Encoding.Unicode.GetBytes(value)];

    private static byte[] UInt16(long value)
    {
        var bytes = new byte[2];
        BinaryPrimitives.WriteUInt16LittleEndian(bytes, (ushort)value);
        return bytes;
    }

    private static byte[] Int32(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
        return bytes;
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
