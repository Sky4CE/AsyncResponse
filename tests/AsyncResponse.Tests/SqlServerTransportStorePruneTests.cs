using AsyncResponse.Transports.SqlServer;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using Xunit;

namespace AsyncResponse.Tests;

/// <summary>
/// Regression (round 33): the SQL Server transport's dead-letter prune was an unbounded
/// <c>DELETE FROM</c> over the queue table that live claims share. Past SQL Server's ~5,000-lock
/// escalation threshold the statement takes a table X lock that READPAST cannot skip, so every
/// claim, ACK and lease renewal blocked behind it for up to the command timeout — a live handler's
/// lease lapsed and a peer re-ran its job concurrently. The prune is now <c>DELETE TOP (1000)</c>
/// (SQL Server channel parity). The statement is only assembled on the far side of an opened
/// connection, so this fact reads the literals the compiled method carries — the same reflection
/// stance as the channel's bounded-prune fact: an older build has no bounded statement, so this
/// fails there instead of failing to compile.
/// </summary>
public sealed class SqlServerTransportStorePruneTests
{
    [Fact]
    public void DeadLetterPrune_IsBounded_SoItCannotEscalateToATableLock()
    {
        var instructions = Decode(AsyncBody(typeof(SqlServerTransportStore), "PruneDeadLettersIfDueAsync")).ToArray();
        var literals = instructions
            .Where(instruction => instruction.Op == OpCodes.Ldstr)
            .Select(instruction => (string)instruction.Operand!)
            .ToArray();

        Assert.Contains(literals, literal => literal.StartsWith("DELETE TOP (", StringComparison.Ordinal));
        Assert.DoesNotContain(literals, literal => literal.StartsWith("DELETE FROM", StringComparison.Ordinal));

        // The batch size. An int constant hole is appended as `ldc.i4 1000`; a compiler that folds
        // it into the literal instead satisfies the first arm.
        Assert.True(
            literals.Any(literal => literal.StartsWith("DELETE TOP (1000)", StringComparison.Ordinal))
            || instructions.Any(instruction => instruction.Op == OpCodes.Ldc_I4 && instruction.Operand is 1000),
            "the dead-letter prune batch is not 1000 rows");

        // Still the retention prune, on the server clock.
        Assert.Contains(literals, literal => literal.Contains("created_at <", StringComparison.Ordinal));
    }

    /// <summary>The compiled body of an async method: its state machine's <c>MoveNext</c>.</summary>
    private static MethodBase AsyncBody(Type type, string methodName)
    {
        var method = type.GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);
        var stateMachine = method!.GetCustomAttribute<AsyncStateMachineAttribute>()!.StateMachineType;
        var map = stateMachine.GetInterfaceMap(typeof(IAsyncStateMachine));
        var index = Array.FindIndex(map.InterfaceMethods, candidate => candidate.Name == nameof(IAsyncStateMachine.MoveNext));
        return map.TargetMethods[index];
    }

    private static readonly Dictionary<ushort, OpCode> OpCodeTable = typeof(OpCodes)
        .GetFields(BindingFlags.Public | BindingFlags.Static)
        .Select(field => (OpCode)field.GetValue(null)!)
        .Where(op => op.OpCodeType != OpCodeType.Nternal)
        .ToDictionary(op => (ushort)op.Value);

    /// <summary>A minimal IL walk: every instruction, with string and int32 operands resolved.</summary>
    private static IEnumerable<(OpCode Op, object? Operand)> Decode(MethodBase method)
    {
        var il = method.GetMethodBody()!.GetILAsByteArray()!;
        var module = method.Module;
        for (var i = 0; i < il.Length;)
        {
            ushort code = il[i++];
            if (code == 0xFE)
                code = (ushort)(0xFE00 | il[i++]);
            var op = OpCodeTable[code];
            object? operand = null;
            switch (op.OperandType)
            {
                case OperandType.InlineNone:
                    break;
                case OperandType.ShortInlineBrTarget:
                case OperandType.ShortInlineI:
                case OperandType.ShortInlineVar:
                    i += 1;
                    break;
                case OperandType.InlineVar:
                    i += 2;
                    break;
                case OperandType.InlineI8:
                case OperandType.InlineR:
                    i += 8;
                    break;
                case OperandType.InlineSwitch:
                    i += 4 + 4 * BitConverter.ToInt32(il, i);
                    break;
                case OperandType.InlineString:
                    operand = module.ResolveString(BitConverter.ToInt32(il, i));
                    i += 4;
                    break;
                case OperandType.InlineI:
                    operand = BitConverter.ToInt32(il, i);
                    i += 4;
                    break;
                default:
                    // InlineBrTarget, InlineField, InlineMethod, InlineSig, InlineTok, InlineType, ShortInlineR.
                    i += 4;
                    break;
            }

            yield return (op, operand);
        }
    }
}
