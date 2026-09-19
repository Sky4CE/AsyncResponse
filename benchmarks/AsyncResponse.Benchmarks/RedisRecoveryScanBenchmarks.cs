using AsyncResponse.Channels.Redis;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using System.Net;
using System.Reflection;
using System.Text.Json;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// The Redis recovery scan against a server that answers with network latency — the dimension
/// <see cref="RecoveryBenchmarks"/> cannot show, because it measures the in-memory store. The
/// scan's cost is round trips, so each fake command here completes after
/// <see cref="RoundTripMilliseconds"/>: <see cref="Scan"/> reads registrations in pipelined
/// batches, and <see cref="OneRoundTripPerKey"/> replays the read pattern the scanner used until
/// round 40 (await each GET before sending the next) against the same fake, as the reference
/// line. Wall-clock is the result that matters; run with <c>-c Release --filter *RedisRecoveryScan*</c>.
/// </summary>
[MemoryDiagnoser]
public class RedisRecoveryScanBenchmarks
{
    private const string KeyPrefix = "bench";
    private ServiceProvider _provider = null!;
    private IRecoveryStateScanner _scanner = null!;
    private IDatabase _database = null!;
    private RedisKey[] _keys = [];

    [Params(1024, 8192)]
    public int Registrations;

    [Params(1)]
    public int RoundTripMilliseconds;

    [GlobalSetup]
    public void Setup()
    {
        _keys = Enumerable.Range(0, Registrations)
            .Select(index => (RedisKey)$"{KeyPrefix}:recovery:bench-{index}")
            .ToArray();
        var latency = TimeSpan.FromMilliseconds(RoundTripMilliseconds);
        var expiresAtUtc = DateTimeOffset.UtcNow.AddDays(1);

        _database = LatencyProxy.Create<IDatabase>((method, args) => method.Name switch
        {
            nameof(IDatabase.StringGetAsync) when args is [RedisKey key, CommandFlags] => AfterRoundTrip(latency, Blob(key, expiresAtUtc)),
            _ => throw new NotSupportedException(method.Name)
        });

        var endPoint = new DnsEndPoint("bench-redis", 6379);
        var server = LatencyProxy.Create<IServer>((method, _) => method.Name switch
        {
            "get_IsConnected" => true,
            "get_IsReplica" => false,
            "get_ServerType" => ServerType.Standalone,
            "get_EndPoint" => endPoint,
            nameof(IServer.KeysAsync) => PagedKeys(_keys, latency),
            _ => throw new NotSupportedException(method.Name)
        });

        var multiplexer = LatencyProxy.Create<IConnectionMultiplexer>((method, _) => method.Name switch
        {
            nameof(IConnectionMultiplexer.GetDatabase) => _database,
            nameof(IConnectionMultiplexer.GetEndPoints) => new EndPoint[] { endPoint },
            nameof(IConnectionMultiplexer.GetServer) => server,
            _ => throw new NotSupportedException(method.Name)
        });

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(multiplexer);
        services.AddAsyncResponse().WithRedisChannel(options => options.KeyPrefix = KeyPrefix);
        _provider = services.BuildServiceProvider();
        _scanner = _provider.GetRequiredService<IRecoveryStateScanner>();
    }

    [GlobalCleanup]
    public void Cleanup() => _provider.Dispose();

    [Benchmark]
    public async Task<int> Scan()
    {
        var count = 0;
        await foreach (var _ in _scanner.ScanAsync())
            count++;
        return count;
    }

    [Benchmark(Baseline = true)]
    public async Task<int> OneRoundTripPerKey()
    {
        var count = 0;
        foreach (var key in _keys)
        {
            if (!(await _database.StringGetAsync(key)).IsNullOrEmpty)
                count++;
        }

        return count;
    }

    private static async Task<RedisValue> AfterRoundTrip(TimeSpan latency, RedisValue value)
    {
        await Task.Delay(latency);
        return value;
    }

    private static RedisValue Blob(RedisKey key, DateTimeOffset expiresAtUtc)
    {
        var state = new RecoveryState
        {
            RegistrationId = Guid.NewGuid(),
            CorrelationId = key.ToString()[$"{KeyPrefix}:recovery:".Length..],
            PayloadTypeFullName = typeof(BenchPayload).FullName,
            RegisteredAtUtc = DateTime.UtcNow
        };
        return $"{{\"Registrations\":[{{\"State\":{JsonSerializer.Serialize(state)},\"ExpiresAtUtc\":\"{expiresAtUtc:O}\"}}]}}";
    }

    /// <summary>One round trip per SCAN page, like the server.</summary>
    private static async IAsyncEnumerable<RedisKey> PagedKeys(RedisKey[] keys, TimeSpan latency)
    {
        for (var index = 0; index < keys.Length; index++)
        {
            if (index % 250 == 0)
                await Task.Delay(latency);
            yield return keys[index];
        }
    }

    /// <summary>
    /// Implements only the members a scan touches. The StackExchange.Redis interfaces run to
    /// hundreds of members; anything else throws, so a scan that starts using a new command shows
    /// up here as a failure rather than as an unmeasured no-op.
    /// </summary>
    public class LatencyProxy : DispatchProxy
    {
        private Func<MethodInfo, object?[]?, object?> _handler = null!;

        public static T Create<T>(Func<MethodInfo, object?[]?, object?> handler)
            where T : class
        {
            var proxy = Create<T, LatencyProxy>();
            ((LatencyProxy)(object)proxy)._handler = handler;
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => _handler(targetMethod!, args);
    }
}
