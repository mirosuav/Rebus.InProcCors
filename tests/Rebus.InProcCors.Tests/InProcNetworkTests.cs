using Rebus.Messages;

namespace Rebus.InProcCors.Tests;

public class InProcNetworkTests
{
    static TransportMessage Msg(string id) =>
        new(new Dictionary<string, string> { ["rbs2-msg-id"] = id }, new byte[1]);

    [Fact]
    public void DeliveredMessagesComeBackInOrderAndAreTheSameInstance()
    {
        var network = new InProcNetwork();
        network.CreateQueue("orders");
        var first = Msg("1");
        var second = Msg("2");

        network.Deliver("orders", first);
        network.Deliver("orders", second);

        var queue = network.GetOrCreateQueue("orders");
        Assert.True(queue.TryDequeue(out var a));
        Assert.True(queue.TryDequeue(out var b));
        Assert.Same(first, a);
        Assert.Same(second, b);
        Assert.False(queue.TryDequeue(out _));
    }

    [Fact]
    public void CountTracksQueueDepth()
    {
        var network = new InProcNetwork();
        network.CreateQueue("orders");

        Assert.Equal(0, network.GetCount("orders"));
        network.Deliver("orders", Msg("1"));
        network.Deliver("orders", Msg("2"));
        Assert.Equal(2, network.GetCount("orders"));

        network.GetOrCreateQueue("orders").TryDequeue(out _);
        Assert.Equal(1, network.GetCount("orders"));
    }

    [Fact]
    public async Task DequeueAsyncParksUntilAMessageArrives()
    {
        var network = new InProcNetwork();
        var queue = network.GetOrCreateQueue("orders");

        var pending = queue.DequeueAsync(CancellationToken.None);
        Assert.False(pending.IsCompleted);

        var message = Msg("1");
        network.Deliver("orders", message);

        Assert.Same(message, await pending);
    }

    [Fact]
    public async Task DequeueAsyncReturnsNullWhenCancelled()
    {
        var network = new InProcNetwork();
        var queue = network.GetOrCreateQueue("orders");
        using var cts = new CancellationTokenSource();

        var pending = queue.DequeueAsync(cts.Token);
        cts.Cancel();

        Assert.Null(await pending);
    }

    [Fact]
    public async Task DeliverDoesNotRunAParkedReadersContinuationInline()
    {
        // AllowSynchronousContinuations = false is a correctness requirement, not tuning (design §5).
        // If it were true, the thread calling Deliver would run the awaiting reader's continuation -
        // turning asynchronous handoff into inline invocation.
        var network = new InProcNetwork();
        var queue = network.GetOrCreateQueue("orders");
        var deliveringThread = Environment.CurrentManagedThreadId;
        var continuationThread = 0;

        var pending = Task.Run(async () =>
        {
            await queue.DequeueAsync(CancellationToken.None);
            continuationThread = Environment.CurrentManagedThreadId;
        });

        await Task.Delay(50);
        network.Deliver("orders", Msg("1"));
        await pending;

        Assert.NotEqual(deliveringThread, continuationThread);
    }

    [Fact]
    public void DeliveringToAnUnknownQueueCreatesIt()
    {
        var network = new InProcNetwork();

        network.Deliver("newly-created", Msg("1"));

        Assert.True(network.HasQueue("newly-created"));
        Assert.Equal(1, network.GetCount("newly-created"));
    }

    [Fact]
    public void QueueNamesAreCaseInsensitive()
    {
        var network = new InProcNetwork();
        network.CreateQueue("Orders");

        network.Deliver("orders", Msg("1"));

        Assert.Equal(1, network.GetCount("ORDERS"));
        Assert.Single(network.Queues);
    }

    [Fact]
    public void SubscribersAreRegisteredAndUnregisteredPerTopic()
    {
        var network = new InProcNetwork();

        network.AddSubscriber("SomeEvent", "module-a");
        network.AddSubscriber("SomeEvent", "module-b");
        network.AddSubscriber("SomeEvent", "module-a");

        Assert.Equal(new[] { "module-a", "module-b" }, network.GetSubscribers("SomeEvent").OrderBy(x => x));

        network.RemoveSubscriber("SomeEvent", "module-a");
        Assert.Equal(new[] { "module-b" }, network.GetSubscribers("SomeEvent"));
        Assert.Empty(network.GetSubscribers("UnknownTopic"));
    }

    [Fact]
    public void ResetClearsQueuesAndSubscribers()
    {
        var network = new InProcNetwork();
        network.Deliver("orders", Msg("1"));
        network.AddSubscriber("SomeEvent", "module-a");

        network.Reset();

        Assert.Empty(network.Queues);
        Assert.Empty(network.GetSubscribers("SomeEvent"));
    }

    [Fact]
    public void TwoNetworksDoNotObserveEachOthersTraffic()
    {
        var a = new InProcNetwork();
        var b = new InProcNetwork();

        a.Deliver("orders", Msg("1"));

        Assert.Equal(1, a.GetCount("orders"));
        Assert.Equal(0, b.GetCount("orders"));
    }

    [Fact]
    public void ALimitedQueueDropsTheOldestMessageWhenFull()
    {
        var network = new InProcNetwork();
        network.LimitQueue("error", 2);

        network.Deliver("error", Msg("1"));
        network.Deliver("error", Msg("2"));
        network.Deliver("error", Msg("3"));

        Assert.Equal(2, network.GetCount("error"));

        var queue = network.GetOrCreateQueue("error");
        Assert.True(queue.TryDequeue(out var a));
        Assert.True(queue.TryDequeue(out var b));
        Assert.Equal("2", a!.Headers["rbs2-msg-id"]);
        Assert.Equal("3", b!.Headers["rbs2-msg-id"]);
        Assert.Equal(0, network.GetCount("error"));
    }

    [Fact]
    public void ADroppedMessageIsReleasedForCollection()
    {
        var network = new InProcNetwork();
        network.LimitQueue("error", 1);

        var dropped = DeliverAndTrack(network, "error");
        network.Deliver("error", Msg("2"));

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.False(dropped.IsAlive);
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    static WeakReference DeliverAndTrack(InProcNetwork network, string address)
    {
        var message = Msg("1");
        network.Deliver(address, message);
        return new WeakReference(message);
    }

    [Fact]
    public void TheLimitSurvivesReset()
    {
        var network = new InProcNetwork();
        network.LimitQueue("error", 1);
        network.CreateQueue("error");

        network.Reset();
        network.Deliver("error", Msg("1"));
        network.Deliver("error", Msg("2"));

        Assert.Equal(1, network.GetCount("error"));
    }

    [Fact]
    public void AnExistingQueueCannotBeLimited()
    {
        var network = new InProcNetwork();
        network.CreateQueue("error");

        Assert.Throws<InvalidOperationException>(() => network.LimitQueue("error", 10));
        Assert.Throws<ArgumentOutOfRangeException>(() => network.LimitQueue("other", 0));
    }
}
