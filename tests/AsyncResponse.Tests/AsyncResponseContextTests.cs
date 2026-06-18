using Xunit;
using System.Reflection;

namespace AsyncResponse.Tests;

/// <summary>
/// The ambient correlation/reply-target context: generation that stores vs. doesn't, idempotent
/// "ensure", and the validation guarding what can be set.
/// </summary>
[Collection("AmbientContext")] // these mutate AsyncLocal state; keep them off the parallel path
public class AsyncResponseContextTests
{
    public AsyncResponseContextTests()
    {
        AsyncResponseContext.ClearCorrelationId();
        AsyncResponseContext.ClearReplyTarget();
    }

    [Fact]
    public void CreateCorrelationId_GeneratesAndStores()
    {
        var id = AsyncResponseContext.CreateCorrelationId();

        Assert.False(string.IsNullOrWhiteSpace(id));
        Assert.Equal(id, AsyncResponseContext.CorrelationId);
    }

    [Fact]
    public void GenerateCorrelationId_DoesNotStore()
    {
        var id = AsyncResponseContext.GenerateCorrelationId();

        Assert.True(Guid.TryParse(id, out _));
        Assert.Null(AsyncResponseContext.CorrelationId); // generated but never stored
    }

    [Fact]
    public void EnsureCorrelationId_IsStable_AndGeneratesWhenMissing()
    {
        var first = AsyncResponseContext.EnsureCorrelationId();
        var second = AsyncResponseContext.EnsureCorrelationId();

        Assert.Equal(first, second);
        Assert.Equal(first, AsyncResponseContext.CorrelationId);
    }

    [Fact]
    public void EnsureCorrelationId_KeepsAnExistingId()
    {
        AsyncResponseContext.SetCorrelationId("already-set");

        Assert.Equal("already-set", AsyncResponseContext.EnsureCorrelationId());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void SetCorrelationId_RejectsNullOrWhitespace(string? value)
        => Assert.Throws<ArgumentException>(() => AsyncResponseContext.SetCorrelationId(value!));

    [Fact]
    public void SetReplyTarget_NullThrows()
        => Assert.Throws<ArgumentNullException>(() => AsyncResponseContext.SetReplyTarget(null!));

    [Theory]
    [InlineData("", "transport", "address")]
    [InlineData("name", "", "address")]
    [InlineData("name", "transport", "")]
    public void SetReplyTarget_RequiresAllFields(string name, string transport, string address)
    {
        var target = new AsyncResponseReplyTarget { Name = name, Transport = transport, Address = address };

        Assert.Throws<ArgumentException>(() => AsyncResponseContext.SetReplyTarget(target));
    }

    [Fact]
    public void SetThenClear_RoundTrips()
    {
        AsyncResponseContext.SetCorrelationId("cid");
        AsyncResponseContext.SetReplyTarget(new AsyncResponseReplyTarget { Name = "n", Transport = "t", Address = "a" });

        Assert.Equal("cid", AsyncResponseContext.CorrelationId);
        Assert.Equal("n", AsyncResponseContext.ReplyTarget!.Name);

        AsyncResponseContext.ClearCorrelationId();
        AsyncResponseContext.ClearReplyTarget();

        Assert.Null(AsyncResponseContext.CorrelationId);
        Assert.Null(AsyncResponseContext.ReplyTarget);
    }

    [Fact]
    public void PushCorrelationId_RestoresPreviousValueAndIsIdempotent()
    {
        AsyncResponseContext.SetCorrelationId("outer");
        try
        {
            var scope = PushCorrelationId("inner");

            Assert.Equal("inner", AsyncResponseContext.CorrelationId);

            scope.Dispose();
            scope.Dispose();

            Assert.Equal("outer", AsyncResponseContext.CorrelationId);

            using (PushCorrelationId(" "))
            {
                Assert.Null(AsyncResponseContext.CorrelationId);
            }

            Assert.Equal("outer", AsyncResponseContext.CorrelationId);
        }
        finally
        {
            AsyncResponseContext.ClearCorrelationId();
        }
    }

    [Fact]
    public void PushContext_RestoresCorrelationAndReplyTargetAndIsIdempotent()
    {
        var outer = new AsyncResponseReplyTarget
        {
            Name = "outer",
            Transport = "test",
            Address = "test://outer"
        };
        var inner = new AsyncResponseReplyTarget
        {
            Name = "inner",
            Transport = "test",
            Address = "test://inner"
        };
        AsyncResponseContext.SetCorrelationId("outer-corr");
        AsyncResponseContext.SetReplyTarget(outer);
        try
        {
            var scope = PushContext("inner-corr", inner);

            Assert.Equal("inner-corr", AsyncResponseContext.CorrelationId);
            Assert.Same(inner, AsyncResponseContext.ReplyTarget);

            scope.Dispose();
            scope.Dispose();

            Assert.Equal("outer-corr", AsyncResponseContext.CorrelationId);
            Assert.Same(outer, AsyncResponseContext.ReplyTarget);
        }
        finally
        {
            AsyncResponseContext.ClearCorrelationId();
            AsyncResponseContext.ClearReplyTarget();
        }
    }

    private static IDisposable PushCorrelationId(string? correlationId)
        => (IDisposable)typeof(AsyncResponseContext)
            .GetMethod("PushCorrelationId", BindingFlags.Static | BindingFlags.NonPublic)!
            .Invoke(null, [correlationId])!;

    private static IDisposable PushContext(string? correlationId, AsyncResponseReplyTarget? replyTarget)
        => (IDisposable)typeof(AsyncResponseContext)
            .GetMethod("PushContext", BindingFlags.Static | BindingFlags.NonPublic)!
            .Invoke(null, [correlationId, replyTarget])!;
}
