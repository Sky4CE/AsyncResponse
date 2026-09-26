using Xunit;

namespace AsyncResponse.Tests;

public class InMemoryRecoveryStateStoreTests
{
    [Fact]
    public async Task SaveAsync_ValidatesInputsAndCancellation()
    {
        var store = new InMemoryRecoveryStateStore();
        var state = new RecoveryState { CorrelationId = "corr-a", RegisteredAtUtc = DateTime.UtcNow };

        await Assert.ThrowsAsync<ArgumentException>(() => store.SaveAsync(" ", state, TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentNullException>(() => store.SaveAsync("corr-a", null!, TimeSpan.FromSeconds(1)));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => store.SaveAsync("corr-a", state, TimeSpan.Zero));
        await Assert.ThrowsAsync<ArgumentException>(() => store.TryDeleteAsync("corr-a", Guid.Empty));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            store.SaveAsync("corr-a", state, TimeSpan.FromSeconds(1), canceled.Token));
    }

    [Fact]
    public async Task GetAllAsync_RemovesExpiredEntriesAndHonorsCancellation()
    {
        var store = new InMemoryRecoveryStateStore();
        await store.SaveAsync(
            "corr-a",
            new RecoveryState { CorrelationId = "corr-a", RegisteredAtUtc = DateTime.UtcNow },
            TimeSpan.FromMilliseconds(1));
        await Task.Delay(20);

        Assert.Empty(await store.GetAllAsync("corr-a"));

        await Assert.ThrowsAsync<ArgumentException>(() => store.GetAllAsync(" "));
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() => store.GetAllAsync("corr-a", canceled.Token));
    }

    [Fact]
    public async Task ScanAsync_YieldsOnlyLiveEntriesAndHonorsCancellation()
    {
        var store = new InMemoryRecoveryStateStore();
        await store.SaveAsync(
            "live",
            new RecoveryState { CorrelationId = "live", RegisteredAtUtc = DateTime.UtcNow },
            TimeSpan.FromMinutes(1));
        await store.SaveAsync(
            "expired",
            new RecoveryState { CorrelationId = "expired", RegisteredAtUtc = DateTime.UtcNow },
            TimeSpan.FromMilliseconds(1));
        await Task.Delay(20);

        var states = new List<RecoveryState>();
        await foreach (var state in store.ScanAsync())
            states.Add(state);

        Assert.Single(states);
        Assert.Equal("live", states[0].CorrelationId);

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in store.ScanAsync(canceled.Token))
            {
            }
        });
    }

    [Fact]
    public async Task SameCorrelationId_AppendsAndDeletesOneRegistration()
    {
        var store = new InMemoryRecoveryStateStore();
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();

        await store.SaveAsync(
            "corr-a",
            new RecoveryState { RegistrationId = firstId, CorrelationId = "corr-a", RegisteredAtUtc = DateTime.UtcNow },
            TimeSpan.FromMinutes(1));
        await store.SaveAsync(
            "corr-a",
            new RecoveryState { RegistrationId = secondId, CorrelationId = "corr-a", RegisteredAtUtc = DateTime.UtcNow },
            TimeSpan.FromMinutes(1));

        var states = await store.GetAllAsync("corr-a");
        Assert.Equal(2, states.Count);

        Assert.True(await store.TryDeleteAsync("corr-a", firstId));

        var remaining = Assert.Single(await store.GetAllAsync("corr-a"));
        Assert.Equal(secondId, remaining.RegistrationId);
        Assert.True(await store.TryDeleteAsync("corr-a", secondId));
        Assert.Empty(await store.GetAllAsync("corr-a"));
    }

    [Fact]
    public async Task SameRegistrationId_ReplacesExistingState()
    {
        var store = new InMemoryRecoveryStateStore();
        var registrationId = Guid.NewGuid();

        await store.SaveAsync(
            "corr-a",
            new RecoveryState { RegistrationId = registrationId, CorrelationId = "corr-a", PayloadTypeFullName = "old" },
            TimeSpan.FromMinutes(1));
        await store.SaveAsync(
            "corr-a",
            new RecoveryState { RegistrationId = registrationId, CorrelationId = "corr-a", PayloadTypeFullName = "new" },
            TimeSpan.FromMinutes(1));

        var state = Assert.Single(await store.GetAllAsync("corr-a"));
        Assert.Equal("new", state.PayloadTypeFullName);
    }

    [Fact]
    public async Task SameRegistrationId_ReplacesExistingStateWithinManyBucket()
    {
        var store = new InMemoryRecoveryStateStore();
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();

        await store.SaveAsync("corr-a", State(firstId, "old-first"), TimeSpan.FromMinutes(1));
        await store.SaveAsync("corr-a", State(secondId, "second"), TimeSpan.FromMinutes(1));
        await store.SaveAsync("corr-a", State(firstId, "new-first"), TimeSpan.FromMinutes(1));

        Assert.Equal(
            ["new-first", "second"],
            (await store.GetAllAsync("corr-a")).Select(state => state.PayloadTypeFullName).Order());
    }

    [Fact]
    public async Task TryDeleteAsync_RemovesOneOfManyRegistrations()
    {
        var store = new InMemoryRecoveryStateStore();
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();
        var thirdId = Guid.NewGuid();

        await store.SaveAsync("corr-a", State(firstId, "first"), TimeSpan.FromMinutes(1));
        await store.SaveAsync("corr-a", State(secondId, "second"), TimeSpan.FromMinutes(1));
        await store.SaveAsync("corr-a", State(thirdId, "third"), TimeSpan.FromMinutes(1));

        Assert.False(await store.TryDeleteAsync("corr-a", Guid.NewGuid()));
        Assert.True(await store.TryDeleteAsync("corr-a", secondId));

        Assert.Equal(["first", "third"], (await store.GetAllAsync("corr-a")).Select(state => state.PayloadTypeFullName));
    }

    [Fact]
    public async Task SaveAsync_RejectsUnrecognizedSchemaVersion()
    {
        var store = new InMemoryRecoveryStateStore();
        var state = new RecoveryState
        {
            RegistrationId = Guid.NewGuid(),
            CorrelationId = "corr-a",
            SchemaVersion = RecoveryStateSchema.Current + 1
        };

        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.SaveAsync("corr-a", state, TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task GetAllAsync_PrunesManyExpiredEntriesToEmpty()
    {
        var store = new InMemoryRecoveryStateStore();
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();

        await store.SaveAsync("corr-a", State(firstId, "first"), TimeSpan.FromMilliseconds(1));
        await store.SaveAsync("corr-a", State(secondId, "second"), TimeSpan.FromMilliseconds(1));
        await Task.Delay(20);

        Assert.Empty(await store.GetAllAsync("corr-a"));
        Assert.False(await store.TryDeleteAsync("corr-a", firstId));
    }

    [Fact]
    public async Task TryDeleteAsync_WithRegistrationId_PrunesExpiredBucketToMissing()
    {
        var store = new InMemoryRecoveryStateStore();
        var firstId = Guid.NewGuid();
        var secondId = Guid.NewGuid();

        await store.SaveAsync("corr-a", State(firstId, "first"), TimeSpan.FromMilliseconds(1));
        await store.SaveAsync("corr-a", State(secondId, "second"), TimeSpan.FromMilliseconds(1));
        await Task.Delay(20);

        Assert.False(await store.TryDeleteAsync("corr-a", firstId));
        Assert.Empty(await store.GetAllAsync("corr-a"));
    }

    [Fact]
    public async Task GetAllAsync_PrunesManyExpiredEntriesToSingleLiveEntry()
    {
        var store = new InMemoryRecoveryStateStore();
        var expiredId = Guid.NewGuid();
        var liveId = Guid.NewGuid();

        await store.SaveAsync("corr-a", State(expiredId, "expired"), TimeSpan.FromMilliseconds(1));
        await store.SaveAsync("corr-a", State(liveId, "live"), TimeSpan.FromMinutes(1));
        await Task.Delay(20);

        var state = Assert.Single(await store.GetAllAsync("corr-a"));

        Assert.Equal(liveId, state.RegistrationId);
        Assert.Equal("live", state.PayloadTypeFullName);
    }

    [Fact]
    public async Task ScanAsync_PrunesManyExpiredEntriesToMultipleLiveEntries()
    {
        var store = new InMemoryRecoveryStateStore();

        await store.SaveAsync("corr-a", State(Guid.NewGuid(), "expired"), TimeSpan.FromMilliseconds(1));
        await store.SaveAsync("corr-a", State(Guid.NewGuid(), "first"), TimeSpan.FromMinutes(1));
        await store.SaveAsync("corr-a", State(Guid.NewGuid(), "second"), TimeSpan.FromMinutes(1));
        await Task.Delay(20);

        var states = new List<RecoveryState>();
        await foreach (var state in store.ScanAsync())
            states.Add(state);

        Assert.Equal(["first", "second"], states.Select(state => state.PayloadTypeFullName).OrderBy(value => value));
    }

    [Fact]
    public async Task SaveAndRead_CoverGeneratedIdsMismatchesMissingEntries()
    {
        var store = new InMemoryRecoveryStateStore();
        var generated = new RecoveryState { CorrelationId = "generated" };

        await store.SaveAsync("generated", generated, TimeSpan.FromMinutes(1));

        Assert.NotEqual(Guid.Empty, generated.RegistrationId);
        Assert.False(await store.TryDeleteAsync("missing", Guid.NewGuid()));
        Assert.Empty(await store.GetAllAsync("missing"));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            store.SaveAsync(
                "expected",
                new RecoveryState { CorrelationId = "different" },
                TimeSpan.FromMinutes(1)));

        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            store.TryDeleteAsync("generated", generated.RegistrationId, canceled.Token));
    }

    [Fact]
    public async Task SaveAsync_SnapshotsACapturedArgument_AsADurableStoreWould()
    {
        // A literal the callback expression captured is a live object. Durable stores serialize it
        // at save; pre-fix this store kept the instance, so a change made after registration (and
        // [JsonIgnore] state) reached the recovery callback in tests only.
        var store = new InMemoryRecoveryStateStore();
        var order = new CapturedOrder { Reference = "ORD-1", Secret = "in-process only" };
        var state = State(Guid.NewGuid(), "snapshot");
        state.ResumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "Contoso.IOrderFlow",
            MethodName = "Resume",
            Params = [CallbackParam.ForValue(order), CallbackParam.ForValue("ORD-1"), CallbackParam.ForPlaceholder(PlaceholderType.Payload)]
        };
        await store.SaveAsync("corr-a", state, TimeSpan.FromMinutes(1));

        order.Reference = "mutated after registration";

        var stored = Assert.Single(await store.GetAllAsync("corr-a")).ResumeCallback!.Params;
        var materialized = stored[0].Value.As<CapturedOrder>();
        Assert.Equal("ORD-1", materialized.Reference);
        Assert.Null(materialized.Secret);
        // Immutable scalars keep the caller's instance: nothing to diverge, nothing to pay for.
        Assert.Same(state.ResumeCallback.Params[1].Value, stored[1].Value);
        Assert.Equal(PlaceholderType.Payload, stored[2].Placeholder);
        // The caller's descriptor is never rewritten in place.
        Assert.Same(order, state.ResumeCallback.Params[0].Value);
    }

    [Fact]
    public async Task SaveAsync_AnArgumentWithNoWireForm_ThrowsAtSave_AsADurableStoreWould()
    {
        // Pre-fix a cyclic argument passed every in-memory test and then threw at waiter creation
        // on Redis or a database, whose save serializes it.
        var store = new InMemoryRecoveryStateStore();
        var cyclic = new CyclicArgument();
        cyclic.Self = cyclic;
        var state = State(Guid.NewGuid(), "cyclic");
        state.FailureCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "Contoso.IOrderFlow",
            MethodName = "Fail",
            Params = [CallbackParam.ForValue(cyclic)]
        };

        await Assert.ThrowsAnyAsync<System.Text.Json.JsonException>(() => store.SaveAsync("corr-a", state, TimeSpan.FromMinutes(1)));
        Assert.Empty(await store.GetAllAsync("corr-a"));
    }

    [Fact]
    public async Task SaveAsync_SnapshotsPrimitivesAndEnums_AsTheJsonElementADurableStoreHandsBack()
    {
        // Pre-fix primitives and enums were exempt as "immutable". They are — but a durable store
        // still hands them back as a JsonElement, which the conversion plan reads into a parameter
        // of the value's own type and NOT into an object- or interface-typed one: `tag is MyEnum`
        // passed under the in-memory store (and the Testing harness) and failed on every durable
        // store. Strings stay exempt (the flow engine's allocation-free hot path).
        var store = new InMemoryRecoveryStateStore();
        var state = State(Guid.NewGuid(), "scalars");
        state.ResumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "Contoso.IOrderFlow",
            MethodName = "Resume",
            Params = [CallbackParam.ForValue(42), CallbackParam.ForValue(DayOfWeek.Friday), CallbackParam.ForValue("flow-1")]
        };
        await store.SaveAsync("corr-a", state, TimeSpan.FromMinutes(1));

        var stored = Assert.Single(await store.GetAllAsync("corr-a")).ResumeCallback!.Params;
        var number = Assert.IsType<System.Text.Json.JsonElement>(stored[0].Value);
        Assert.Equal(42, number.GetInt32());
        var weekday = Assert.IsType<System.Text.Json.JsonElement>(stored[1].Value);
        Assert.Equal((int)DayOfWeek.Friday, weekday.GetInt32());
        Assert.Same(state.ResumeCallback.Params[2].Value, stored[2].Value);
    }

    [Theory]
    [InlineData("nan")]
    [InlineData("intptr")]
    public async Task SaveAsync_APrimitiveWithNoWireForm_ThrowsAtSave_AsADurableStoreWould(string shape)
    {
        // A durable store's save serializes the argument, and System.Text.Json writes neither a
        // non-finite double nor an IntPtr: pre-fix the in-memory store accepted both.
        var store = new InMemoryRecoveryStateStore();
        var state = State(Guid.NewGuid(), "no-wire-form");
        state.ResumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "Contoso.IOrderFlow",
            MethodName = "Resume",
            Params = [CallbackParam.ForValue(shape == "nan" ? double.NaN : (object)new IntPtr(7))]
        };

        // The serializer's own exceptions, exactly what a durable store's save surfaces.
        var thrown = await Record.ExceptionAsync(() => store.SaveAsync("corr-a", state, TimeSpan.FromMinutes(1)));
        if (shape == "nan")
            Assert.IsAssignableFrom<ArgumentException>(thrown);
        else
            Assert.IsType<NotSupportedException>(thrown);
        Assert.Empty(await store.GetAllAsync("corr-a"));
    }

    [Fact]
    public async Task SaveAsync_AJsonElementArgument_IsClonedOffTheCallersDocument()
    {
        // A durable store serializes a JsonElement argument at save. Kept by reference here, one
        // whose JsonDocument the caller disposed after registering threw ObjectDisposedException
        // at in-memory dispatch only.
        var store = new InMemoryRecoveryStateStore();
        var state = State(Guid.NewGuid(), "element");
        var document = System.Text.Json.JsonDocument.Parse("""{"orderId":"ORD-7"}""");
        state.ResumeCallback = new ReflectionCallDto
        {
            ServiceInterfaceFullName = "Contoso.IOrderFlow",
            MethodName = "Resume",
            Params = [CallbackParam.ForValue(document.RootElement)]
        };
        await store.SaveAsync("corr-a", state, TimeSpan.FromMinutes(1));
        document.Dispose();

        var stored = Assert.Single(await store.GetAllAsync("corr-a")).ResumeCallback!.Params;
        var element = Assert.IsType<System.Text.Json.JsonElement>(stored[0].Value);
        Assert.Equal("ORD-7", element.GetProperty("orderId").GetString());
    }

    public sealed class CapturedOrder
    {
        public string? Reference { get; set; }

        [System.Text.Json.Serialization.JsonIgnore]
        public string? Secret { get; set; }
    }

    public sealed class CyclicArgument
    {
        public CyclicArgument? Self { get; set; }
    }

    private static RecoveryState State(Guid registrationId, string payloadType)
        => new()
        {
            RegistrationId = registrationId,
            CorrelationId = "corr-a",
            PayloadTypeFullName = payloadType,
            RegisteredAtUtc = DateTime.UtcNow
        };

}
