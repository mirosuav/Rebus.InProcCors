using System.Runtime.CompilerServices;
using Rebus.Messages;

namespace Rebus.InProcCors.Tests;

public class MessageReferenceTableTests
{
    sealed record Payload(string Value);

    [Fact]
    public void SentinelsAreOneByteAndReferenceDistinct()
    {
        var a = MessageReferenceTable.CreateSentinel();
        var b = MessageReferenceTable.CreateSentinel();

        Assert.Single(a);
        Assert.Single(b);
        Assert.False(ReferenceEquals(a, b));
    }

    [Fact]
    public void RegisteredInstanceIsResolvedByReference()
    {
        var sentinel = MessageReferenceTable.CreateSentinel();
        var payload = new Payload("hello");

        MessageReferenceTable.Register(sentinel, payload);

        Assert.True(MessageReferenceTable.TryResolve(sentinel, out var resolved));
        Assert.Same(payload, resolved);
    }

    [Fact]
    public void UnregisteredSentinelDoesNotResolve()
    {
        Assert.False(MessageReferenceTable.TryResolve(MessageReferenceTable.CreateSentinel(), out _));
    }

    [Fact]
    public void ResolutionSurvivesTheCloneThatDeadLetteringPerforms()
    {
        // Rebus/Bus/MessageExtensions.cs:129 clones as new TransportMessage(headers.Clone(), message.Body).
        // The subclass is lost; the Body array instance is not. That is the whole basis of the fallback.
        var payload = new Payload("hello");
        var sentinel = MessageReferenceTable.CreateSentinel();
        MessageReferenceTable.Register(sentinel, payload);

        var original = new ReferenceTransportMessage(new Dictionary<string, string>(), sentinel, payload);
        var cloned = new TransportMessage(new Dictionary<string, string>(original.Headers), original.Body);

        Assert.IsNotType<ReferenceTransportMessage>(cloned);
        Assert.True(MessageReferenceTable.TryResolve(cloned.Body, out var resolved));
        Assert.Same(payload, resolved);
    }

    [Fact]
    public void ReferenceTransportMessageExposesTheInstanceAndUsesTheSentinelAsBody()
    {
        var payload = new Payload("hello");
        var sentinel = MessageReferenceTable.CreateSentinel();

        var message = new ReferenceTransportMessage(
            new Dictionary<string, string> { ["rbs2-msg-id"] = "abc" }, sentinel, payload);

        Assert.Same(payload, message.MessageInstance);
        Assert.Same(sentinel, message.Body);
        Assert.Equal("abc", message.Headers["rbs2-msg-id"]);
    }

    [Fact]
    public void CollectingTheSentinelReleasesTheTableEntry()
    {
        // The table keys weakly, so entry lifetime is exactly Body-array lifetime (design §4).
        var reference = CreateCollectableEntry();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.False(reference.IsAlive);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    static WeakReference CreateCollectableEntry()
    {
        var sentinel = MessageReferenceTable.CreateSentinel();
        var payload = new Payload("collect me");
        MessageReferenceTable.Register(sentinel, payload);
        return new WeakReference(payload);
    }
}
