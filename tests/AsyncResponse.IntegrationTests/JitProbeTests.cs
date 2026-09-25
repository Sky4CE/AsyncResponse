using System.Reflection;
using System.Runtime.CompilerServices;
using AsyncResponse.Conformance;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using Xunit;

namespace AsyncResponse.IntegrationTests;

// Permanent probe for IL corrupted by the coverage instrumenter: forces the JIT to compile every
// method of every AsyncResponse assembly in this test host, so an assembly whose IL the coverage
// instrumentation corrupted fails HERE with the exact method list instead of as
// InvalidProgramException deep inside a container test. It runs in the batch=none leg, which CI runs
// under scripts/coverage-collect.sh — the probe that script relies on to validate the pipeline
// against ANY instrumenter, not just the dynamic CLR profiler whose invalid IL it was written to
// chase before the move to coverlet. Keep it.
[Trait(Batches.Trait, Batches.None)]
public sealed class JitProbeTests
{
    [Fact]
    public async Task RedisSetResponse_TypedPublishPath_JitsAtTierZero()
    {
        // Builds the REAL RedisAsyncResponseChannel through the public DI path (same wiring as the
        // conformance harness) against a no-op multiplexer proxy, then makes a genuine first CALL
        // to SetResponse<T> so the async state machine's MoveNext is compiled by the normal
        // tier-0(+PGO) pipeline — the path CI's InvalidProgramException comes from — instead of
        // PrepareMethod's optimized pipeline. The blank correlation id makes the call a no-op
        // before any Redis I/O.
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(NoOpProxy.Multiplexer());
        services.AddAsyncResponse().WithRedisChannel(options => options.KeyPrefix = "jitprobe");
        await using var provider = services.BuildServiceProvider();
        var publisher = provider.GetRequiredService<IAsyncResponsePublisher>();

        await publisher.SetResponse(new ConformanceResult { Message = "jit" }, " ");
    }

    public class NoOpProxy : DispatchProxy
    {
        public static IConnectionMultiplexer Multiplexer()
        {
            var proxy = Create<IConnectionMultiplexer, NoOpProxy>();
            return proxy;
        }

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args)
        {
            var returnType = targetMethod?.ReturnType;
            if (returnType == typeof(ISubscriber))
                return Create<ISubscriber, NoOpProxy>();
            if (returnType is null || returnType == typeof(void))
                return null;
            if (returnType == typeof(string))
                return "jitprobe";
            return returnType.IsValueType ? Activator.CreateInstance(returnType) : null;
        }
    }

    [Fact]
    public void ForceJit_EveryAsyncResponseMethod_CompilesWithoutInvalidProgram()
    {
        var failures = new List<string>();
        var baseDir = AppContext.BaseDirectory;
        foreach (var path in Directory.GetFiles(baseDir, "AsyncResponse*.dll").OrderBy(p => p, StringComparer.Ordinal))
        {
            Assembly assembly;
            try
            {
                assembly = Assembly.LoadFrom(path);
            }
            catch
            {
                continue;
            }

            foreach (var type in SafeGetTypes(assembly))
            {
                var closedType = type;
                if (type.IsGenericTypeDefinition)
                {
                    closedType = TryClose(type);
                    if (closedType is null)
                        continue;
                }

                var methods = closedType
                    .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                    .Cast<MethodBase>()
                    .Concat(closedType.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static));
                foreach (var method in methods)
                {
                    if (method.IsAbstract)
                        continue;

                    try
                    {
                        if (method is MethodInfo { IsGenericMethodDefinition: true } definition)
                        {
                            var closedMethod = TryCloseMethod(definition);
                            if (closedMethod is null)
                                continue;
                            RuntimeHelpers.PrepareMethod(closedMethod.MethodHandle);
                        }
                        else if (!method.ContainsGenericParameters)
                        {
                            RuntimeHelpers.PrepareMethod(method.MethodHandle);
                        }
                    }
                    catch (InvalidProgramException ex)
                    {
                        failures.Add($"{closedType.FullName}::{method.Name} -- {ex.Message}");
                    }
                    catch
                    {
                        // Other failures (missing native deps, platform gates) are not what this
                        // probe is hunting.
                    }
                }
            }
        }

        Assert.Empty(failures);
    }

    private static Type[] SafeGetTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            return [.. ex.Types.Where(t => t is not null)!];
        }
    }

    private static Type? TryClose(Type definition)
    {
        var arguments = definition.GetGenericArguments().Select(CandidateFor).ToArray();
        if (arguments.Any(a => a is null))
            return null;
        try
        {
            return definition.MakeGenericType(arguments!);
        }
        catch
        {
            return null;
        }
    }

    private static MethodInfo? TryCloseMethod(MethodInfo definition)
    {
        var arguments = definition.GetGenericArguments().Select(CandidateFor).ToArray();
        if (arguments.Any(a => a is null))
            return null;
        try
        {
            return definition.MakeGenericMethod(arguments!);
        }
        catch
        {
            return null;
        }
    }

    private static Type? CandidateFor(Type parameter)
    {
        var constraints = parameter.GetGenericParameterConstraints();
        if (constraints.Length == 0)
        {
            return (parameter.GenericParameterAttributes & GenericParameterAttributes.NotNullableValueTypeConstraint) != 0
                ? typeof(int)
                : typeof(object);
        }

        foreach (var constraint in constraints)
        {
            if (constraint.ContainsGenericParameters || constraint.IsGenericTypeDefinition)
                continue;
            return constraint;
        }

        return null;
    }
}
