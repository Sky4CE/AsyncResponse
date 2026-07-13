using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using System.Reflection;
using Xunit;

namespace AsyncResponse.Tests;

public sealed class InMemoryChannelInternalCoverageTests
{
    [Fact]
    public async Task SubscriptionGroup_CoversManyCollapseClosedAndOwnerRetryPaths()
    {
        var (channel, _) = CreateChannel();
        var first = CreateSubscription(channel, "group");
        var second = CreateSubscription(channel, "group");
        var third = CreateSubscription(channel, "group");
        var groupType = typeof(InMemoryAsyncResponseChannel).GetNestedType("SubscriptionGroup", BindingFlags.NonPublic)!;
        var group = Activator.CreateInstance(groupType, nonPublic: true)!;

        Assert.True(Invoke<bool>(group, "TryAdd", first));
        Assert.True(Invoke<bool>(group, "TryAdd", second));
        Assert.True(Invoke<bool>(group, "TryAdd", third));
        Assert.Equal(3, GetProperty<int>(group, "Count"));
        Assert.NotNull(Invoke(group, "Snapshot"));
        Assert.False(Invoke<bool>(group, "Remove", Guid.NewGuid()));
        Assert.False(Invoke<bool>(group, "Remove", Id(second)));
        Assert.Equal(2, GetProperty<int>(group, "Count"));
        Assert.False(Invoke<bool>(group, "Remove", Id(first)));
        Assert.Equal(1, GetProperty<int>(group, "Count"));
        Assert.True(Invoke<bool>(group, "Remove", Id(third)));
        Assert.Equal(0, GetProperty<int>(group, "Count"));
        Assert.False(Invoke<bool>(group, "Remove", Guid.NewGuid()));
        Assert.False(Invoke<bool>(group, "TryAdd", first));
        Assert.NotNull(Invoke(group, "Snapshot"));

        var closed = Activator.CreateInstance(groupType, nonPublic: true)!;
        Assert.True(Invoke<bool>(closed, "TryAdd", first));
        Assert.True(Invoke<bool>(closed, "Remove", Id(first)));
        var subscriptions = GetField(channel, "_subscriptions");
        Assert.True(Invoke<bool>(subscriptions, "TryAdd", "retry", closed));
        Invoke(channel, "AddSubscription", "retry", second);
        Assert.Equal(1, await channel.CountActiveSubscribersAsync("retry"));

        await CleanupAsync(first);
        await CleanupAsync(second);
        await CleanupAsync(third);
        Invoke(channel, "RemoveSubscription", "missing", Guid.NewGuid());
    }

    [Fact]
    public async Task DispatchManyAndDispatchSerial_CoverCompletedPendingAndSynchronousFailurePaths()
    {
        var (channel, _) = CreateChannel();
        var first = CreateSubscription(channel, "dispatch");
        var second = CreateSubscription(channel, "dispatch");
        var baseType = first.GetType().BaseType!;
        var dispatchType = typeof(Func<,,>).MakeGenericType(baseType, typeof(int), typeof(Task));
        var probe = Delegate.CreateDelegate(
            dispatchType,
            typeof(InMemoryChannelInternalCoverageTests).GetMethod(nameof(ProbeDispatch), BindingFlags.Static | BindingFlags.NonPublic)!);
        var throwing = Delegate.CreateDelegate(
            dispatchType,
            typeof(InMemoryChannelInternalCoverageTests).GetMethod(nameof(ThrowingDispatch), BindingFlags.Static | BindingFlags.NonPublic)!);
        var dispatchMany = typeof(InMemoryAsyncResponseChannel)
            .GetMethod("DispatchManyAsync", BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(typeof(int));

        await (Task)dispatchMany.Invoke(null, [null, probe, 0])!;
        await (Task)dispatchMany.Invoke(null, [Array.CreateInstance(baseType, 0), probe, 0])!;

        var one = Array.CreateInstance(baseType, 1);
        one.SetValue(first, 0);
        await (Task)dispatchMany.Invoke(null, [one, probe, 1])!;

        var two = Array.CreateInstance(baseType, 2);
        two.SetValue(first, 0);
        two.SetValue(second, 1);
        await (Task)dispatchMany.Invoke(null, [two, probe, 0])!;
        await (Task)dispatchMany.Invoke(null, [two, probe, 1])!;

        var dispatchSerial = baseType
            .GetMethod("DispatchSerialAsync", BindingFlags.Instance | BindingFlags.NonPublic)!
            .MakeGenericMethod(typeof(int));
        var exception = Assert.Throws<TargetInvocationException>(() =>
            dispatchSerial.Invoke(first, [0, throwing]));
        Assert.IsType<InvalidOperationException>(exception.InnerException);

        await CleanupAsync(first);
        await CleanupAsync(second);
    }

    [Fact]
    public async Task SubscriptionTerminalPaths_CoverCleanedDuplicateDisposedTimerAndAsyncFailureRaces()
    {
        var (channel, _) = CreateChannel();

        var cleaned = CreateSubscription(channel, "cleaned");
        await CleanupAsync(cleaned);
        Invoke(cleaned, "ArmTimeout");
        await InvokeTaskAsync(cleaned, "DispatchExceptionAsync", new InvalidOperationException("late"));
        await InvokeTaskAsync(cleaned, "DispatchResponseAsync", new OperationResult());
        await InvokeTaskAsync(cleaned, "DispatchRawJsonResponseAsync", new RawJsonResponse("{}"));
        await InvokeTaskAsync(cleaned, "TimeoutCoreAsync");

        var terminal = CreateSubscription(
            channel,
            "terminal",
            _ => throw new InvalidOperationException("predicate failed"));
        SetField(terminal, "_terminal", 1);
        await InvokeTaskAsync(terminal, "DispatchExceptionAsync", new InvalidOperationException("duplicate"));
        await InvokeTaskAsync(terminal, "TimeoutCoreAsync");
        await InvokeTaskAsync(terminal, "DispatchResponseAsync", new OperationResult());
        await InvokeTaskAsync(terminal, "FaultAsync", new InvalidOperationException("duplicate fault"));
        Assert.False(ResponseTask(terminal).IsCompleted);
        await CleanupAsync(terminal);

        var disposedTimer = CreateSubscription(channel, "disposed-timer");
        ((CancellationTokenSource)GetField(disposedTimer, "_timeoutCts")).Dispose();
        Invoke(disposedTimer, "ArmTimeout");
        await CleanupAsync(disposedTimer);

        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var asyncFailure = CreateSubscription(
            channel,
            "async-failure",
            async _ =>
            {
                entered.TrySetResult();
                await release.Task.ConfigureAwait(false);
                throw new InvalidOperationException("async predicate failed");
            });
        var dispatch = InvokeTaskAsync(asyncFailure, "DispatchResponseAsync", new OperationResult());
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(2));
        SetField(asyncFailure, "_terminal", 1);
        release.TrySetResult();
        await dispatch;
        Assert.False(ResponseTask(asyncFailure).IsCompleted);
        await CleanupAsync(asyncFailure);
    }

    private static (InMemoryAsyncResponseChannel Channel, Mock<IRecoveryStateStore> Store) CreateChannel()
    {
        var provider = new ServiceCollection().BuildServiceProvider();
        var store = new Mock<IRecoveryStateStore>();
        store.Setup(instance => instance.TryDeleteAsync(
                It.IsAny<string>(),
                It.IsAny<Guid>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);
        var channel = new InMemoryAsyncResponseChannel(
            provider.GetRequiredService<IServiceScopeFactory>(),
            store.Object,
            Options.Create(new InMemoryAsyncResponseOptions
            {
                DefaultTimeout = TimeSpan.FromMinutes(1),
                RecoveryStateExpiry = TimeSpan.FromMinutes(1)
            }),
            new AsyncResponseContextPropagation([]),
            NullLogger<InMemoryAsyncResponseChannel>.Instance);
        return (channel, store);
    }

    private static object CreateSubscription(
        InMemoryAsyncResponseChannel channel,
        string correlationId,
        Func<OperationResult, ValueTask<bool>>? predicate = null)
    {
        var type = typeof(InMemoryAsyncResponseChannel)
            .GetNestedType("Subscription`1", BindingFlags.NonPublic)!
            .MakeGenericType(typeof(OperationResult));
        return Activator.CreateInstance(
            type,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            [
                channel,
                correlationId,
                TimeSpan.FromMinutes(1),
                predicate ?? (_ => new ValueTask<bool>(true)),
                null,
                null
            ],
            culture: null)!;
    }

    private static Guid Id(object subscription)
        => GetProperty<Guid>(subscription, "Id");

    private static Task ResponseTask(object subscription)
        => GetProperty<Task>(subscription, "ResponseTask");

    private static async ValueTask CleanupAsync(object subscription)
        => await (ValueTask)subscription.GetType().GetMethod("CleanupOnceAsync")!.Invoke(subscription, null)!;

    private static Task ProbeDispatch(object _, int state)
        => state == 0 ? Task.CompletedTask : Task.Delay(1);

    private static Task ThrowingDispatch(object _, int __)
        => throw new InvalidOperationException("synchronous dispatch failure");

    private static object? Invoke(object target, string name, params object?[] arguments)
        => FindMethod(target.GetType(), name).Invoke(target, arguments);

    private static T Invoke<T>(object target, string name, params object?[] arguments)
        => (T)Invoke(target, name, arguments)!;

    private static Task InvokeTaskAsync(object target, string name, params object?[] arguments)
        => (Task)Invoke(target, name, arguments)!;

    private static MethodInfo FindMethod(Type type, string name)
    {
        for (var current = type; current is not null; current = current.BaseType)
        {
            if (current.GetMethod(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic) is { } method)
                return method;
        }

        throw new MissingMethodException(type.FullName, name);
    }

    private static object GetField(object target, string name)
    {
        for (var current = target.GetType(); current is not null; current = current.BaseType)
        {
            if (current.GetField(name, BindingFlags.Instance | BindingFlags.NonPublic) is { } field)
                return field.GetValue(target)!;
        }

        throw new MissingFieldException(target.GetType().FullName, name);
    }

    private static void SetField(object target, string name, object value)
    {
        for (var current = target.GetType(); current is not null; current = current.BaseType)
        {
            if (current.GetField(name, BindingFlags.Instance | BindingFlags.NonPublic) is { } field)
            {
                field.SetValue(target, value);
                return;
            }
        }

        throw new MissingFieldException(target.GetType().FullName, name);
    }

    private static T GetProperty<T>(object target, string name)
        => (T)target.GetType().GetProperty(name)!.GetValue(target)!;
}
