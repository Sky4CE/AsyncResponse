using AsyncResponse.Channels.NATS;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StackExchange.Redis;
using System.Runtime.CompilerServices;
using System.Text;

namespace AsyncResponse.Benchmarks;

/// <summary>
/// Registering <see cref="Registrations"/> recoverable waiters on ONE correlation id, on the two
/// channels that keep an id's registrations in a single stored value (Redis, NATS). Every save
/// reads, parses and rewrites all of its siblings, so the work grows with the square of the
/// fan-out — the cost <c>MaxRecoveryRegistrationsPerCorrelationId</c> bounds. The deletion-side
/// benchmarks (a fan-out's batch delete) do not show it. The fakes answer instantly, so the time
/// and allocations are the store's own; each benchmark returns the UTF-8 bytes it wrote in total,
/// the part a real server adds network and storage cost to (<see cref="FinalValueBytes"/> is the
/// size of the value the last registration left). Registrations carry both callbacks, a payload
/// placeholder, one literal argument and a propagated trace context, like a durable flow's.
/// Run with <c>-c Release --filter *RecoveryRegistrationGrowth*</c>.
/// </summary>
[MemoryDiagnoser]
public class RecoveryRegistrationGrowthBenchmarks
{
    private static readonly TimeSpan Ttl = TimeSpan.FromHours(1);

    private ServiceProvider _provider = null!;
    private IRecoveryStateStore _redis = null!;
    private CountingRedis _redisServer = null!;

    [Params(16, 64, 256)]
    public int Registrations;

    /// <summary>Size of the stored value after the last registration (set by the last run).</summary>
    public static long FinalValueBytes { get; private set; }

    [GlobalSetup]
    public void Setup()
    {
        _redisServer = new CountingRedis();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(_redisServer.Multiplexer);
        services.AddAsyncResponse().WithRedisChannel(options => options.MaxRecoveryRegistrationsPerCorrelationId = int.MaxValue);
        _provider = services.BuildServiceProvider();
        _redis = _provider.GetRequiredService<IRecoveryStateStore>();
    }

    [GlobalCleanup]
    public void Cleanup() => _provider.Dispose();

    [Benchmark]
    public async Task<long> Nats()
    {
        var kv = new CountingKvStore();
        var store = new NatsRecoveryStateStore(
            kv,
            Options.Create(new NatsAsyncResponseChannelOptions { MaxRecoveryRegistrationsPerCorrelationId = int.MaxValue }),
            NullLogger<NatsRecoveryStateStore>.Instance);
        for (var i = 0; i < Registrations; i++)
            await store.SaveAsync("fan-out", NewRegistration("fan-out"), Ttl).ConfigureAwait(false);

        FinalValueBytes = kv.LastValueBytes;
        return kv.BytesWritten;
    }

    [Benchmark]
    public async Task<long> Redis()
    {
        _redisServer.Reset();
        for (var i = 0; i < Registrations; i++)
            await _redis.SaveAsync("fan-out", NewRegistration("fan-out"), Ttl).ConfigureAwait(false);

        FinalValueBytes = _redisServer.LastValueBytes;
        return _redisServer.BytesWritten;
    }

    internal static RecoveryState NewRegistration(string correlationId) => new()
    {
        RegistrationId = Guid.NewGuid(),
        CorrelationId = correlationId,
        PayloadTypeFullName = typeof(BenchPayload).FullName,
        RegisteredAtUtc = DateTime.UtcNow,
        ResumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "Contoso.Orders.IOrderFlowCallbacks",
            MethodName = "ResumeAsync",
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Payload), CallbackParam.ForValue("order-2026-000123")]
        },
        FailureCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "Contoso.Orders.IOrderFlowCallbacks",
            MethodName = "FailAsync",
            Params = [CallbackParam.ForPlaceholder(PlaceholderType.Exception), CallbackParam.ForValue("order-2026-000123")]
        },
        Context = new Dictionary<string, string>
        {
            ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            ["tenant"] = "contoso-eu"
        }
    };

    /// <summary>A NATS Key-Value bucket that holds one value per key and counts what is written.</summary>
    private sealed class CountingKvStore : INatsKvStore
    {
        private readonly Dictionary<string, NatsKvEntry> _entries = new(StringComparer.Ordinal);
        private ulong _revision;

        public long BytesWritten { get; private set; }
        public long LastValueBytes { get; private set; }

        public Task<NatsKvEntry?> GetAsync(string key, CancellationToken cancellationToken)
            => Task.FromResult<NatsKvEntry?>(_entries.TryGetValue(key, out var entry) ? entry : null);

        public Task<bool> TryCreateAsync(string key, string value, CancellationToken cancellationToken)
            => Task.FromResult(!_entries.ContainsKey(key) && Write(key, value));

        public Task<bool> TryUpdateAsync(string key, string value, ulong expectedRevision, CancellationToken cancellationToken)
            => Task.FromResult(_entries.TryGetValue(key, out var entry) && entry.Revision == expectedRevision && Write(key, value));

        public Task<bool> TryDeleteAsync(string key, ulong expectedRevision, CancellationToken cancellationToken)
            => Task.FromResult(_entries.Remove(key));

        public async IAsyncEnumerable<string> GetKeysAsync([EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask.ConfigureAwait(false);
            foreach (var key in _entries.Keys)
                yield return key;
        }

        public Task<long> PurgeDeleteMarkersAsync(TimeSpan olderThan, CancellationToken cancellationToken) => Task.FromResult(0L);

        private bool Write(string key, string value)
        {
            LastValueBytes = Encoding.UTF8.GetByteCount(value);
            BytesWritten += LastValueBytes;
            _entries[key] = new NatsKvEntry(value, ++_revision);
            return true;
        }
    }

    /// <summary>
    /// The Redis commands a recovery save issues — GET, then MULTI with a value condition, SET, EXEC —
    /// against one in-memory value, counting what is written. Anything else throws.
    /// </summary>
    private sealed class CountingRedis
    {
        private RedisValue _value = RedisValue.Null;

        public CountingRedis()
        {
            var database = RedisRecoveryScanBenchmarks.LatencyProxy.Create<IDatabase>((method, _) => method.Name switch
            {
                nameof(IDatabase.StringGetAsync) => Task.FromResult(_value),
                nameof(IDatabase.CreateTransaction) => NewTransaction(),
                _ => throw new NotSupportedException(method.Name)
            });
            Multiplexer = RedisRecoveryScanBenchmarks.LatencyProxy.Create<IConnectionMultiplexer>((method, _) => method.Name switch
            {
                nameof(IConnectionMultiplexer.GetDatabase) => database,
                _ => throw new NotSupportedException(method.Name)
            });
        }

        public IConnectionMultiplexer Multiplexer { get; }
        public long BytesWritten { get; private set; }
        public long LastValueBytes { get; private set; }

        public void Reset()
        {
            _value = RedisValue.Null;
            BytesWritten = 0;
        }

        private ITransaction NewTransaction()
        {
            var readAtCreation = _value;
            RedisValue? pending = null;
            return RedisRecoveryScanBenchmarks.LatencyProxy.Create<ITransaction>((method, args) => method.Name switch
            {
                nameof(ITransaction.AddCondition) => null,
                nameof(IDatabase.StringSetAsync) => Stage(args![1]!),
                nameof(ITransaction.ExecuteAsync) => Task.FromResult(Commit()),
                _ => throw new NotSupportedException(method.Name)
            });

            Task<bool> Stage(object value)
            {
                pending = (RedisValue)value;
                return Task.FromResult(true);
            }

            bool Commit()
            {
                if (_value != readAtCreation || pending is not { } written)
                    return false;

                _value = written;
                LastValueBytes = Encoding.UTF8.GetByteCount(written.ToString());
                BytesWritten += LastValueBytes;
                return true;
            }
        }
    }
}
