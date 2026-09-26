using System.Net;
using System.Net.Sockets;

namespace AsyncResponse.Tests;

/// <summary>
/// A loopback listener that accepts connections and never says a word — a database whose
/// connection handshake hangs — so a unit test can hold a driver's <c>OpenAsync</c> in flight for as
/// long as it likes, and end it on cue by dropping the connections it accepted
/// (<see cref="CloseAll"/>). From then on it drops every later connection as soon as it arrives.
/// </summary>
internal sealed class SilentTcpServer : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly List<TcpClient> _clients = [];
    private readonly TaskCompletionSource _firstAccepted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Task _acceptLoop;
    private bool _rejecting;
    private int _accepted;

    public SilentTcpServer()
    {
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptLoop = Task.Run(AcceptLoopAsync);
    }

    public int Port { get; }

    /// <summary>Connections accepted so far.</summary>
    public int Accepted => Volatile.Read(ref _accepted);

    /// <summary>Completes once the first connection has been accepted.</summary>
    public Task FirstAccepted => _firstAccepted.Task;

    /// <summary>Drops every connection accepted so far, and every one accepted from now on.</summary>
    public void CloseAll()
    {
        lock (_clients)
        {
            _rejecting = true;
            foreach (var client in _clients)
                client.Dispose();
            _clients.Clear();
        }
    }

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_stop.Token).ConfigureAwait(false);
                Interlocked.Increment(ref _accepted);
                lock (_clients)
                {
                    if (_rejecting)
                        client.Dispose();
                    else
                        _clients.Add(client);
                }

                _firstAccepted.TrySetResult();
            }
        }
        catch (Exception) when (_stop.IsCancellationRequested)
        {
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _listener.Stop();
        await _acceptLoop;
        CloseAll();
        _stop.Dispose();
    }
}
