using System.Collections.Concurrent;
using System.Diagnostics;
using Rebus.Messages;
using Rebus.Transport;

namespace Rebus.InProcCors.Tests;

public class InProcTransportTests
{
    static TransportMessage Msg(string id) =>
        new(new Dictionary<string, string> { [Headers.MessageId] = id }, new byte[1]);

    static InProcTransport CreateTransport(InProcNetwork network, string? address,
        InProcReceiveMode mode = InProcReceiveMode.Blocking)
    {
        var transport = new InProcTransport(network, address,
            new InProcTransportOptions { ReceiveMode = mode, PollingInterval = TimeSpan.FromMilliseconds(50) });
        transport.Initialize();
        return transport;
    }

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task SendThenReceiveYieldsTheIdenticalTransportMessageInstance(InProcReceiveMode mode)
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders", mode);
        var message = Msg("1");

        using (var scope = new RebusTransactionScope())
        {
            await transport.Send("orders", message, scope.TransactionContext);
            await scope.CompleteAsync();
        }

        using var receiveScope = new RebusTransactionScope();
        var received = await transport.Receive(receiveScope.TransactionContext, CancellationToken.None);

        Assert.Same(message, received);
    }

    [Fact]
    public async Task SendIsDeferredUntilTheAmbientTransactionCommits()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders");

        using (var scope = new RebusTransactionScope())
        {
            await transport.Send("orders", Msg("1"), scope.TransactionContext);
            Assert.Equal(0, network.GetCount("orders"));
            await scope.CompleteAsync();
        }

        Assert.Equal(1, network.GetCount("orders"));
    }

    [Fact]
    public void InitializeCreatesTheInputQueue()
    {
        var network = new InProcNetwork();

        CreateTransport(network, "orders");

        Assert.True(network.HasQueue("orders"));
    }

    [Fact]
    public async Task ANackedMessageGoesBackOntoTheQueue()
    {
        // Nack is not reachable through RebusTransactionScope: Dispose fires only OnDisposed, and
        // CompleteAsync forces SetResult(commit: true, ack: true). Rebus's worker drives a nack by
        // calling SetResult(false, false) and then TransactionContext.Complete(), and Complete() is
        // not on ITransactionContext. So we invoke the registered callback the way the worker would.
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders");
        var message = Msg("1");
        network.Deliver("orders", message);

        var context = new CallbackCapturingTransactionContext();
        var received = await transport.Receive(context, CancellationToken.None);
        Assert.Same(message, received);
        Assert.Equal(0, network.GetCount("orders"));

        await context.InvokeNackAsync();

        Assert.Equal(1, network.GetCount("orders"));
        Assert.True(network.GetOrCreateQueue("orders").TryDequeue(out var requeued));
        Assert.Same(message, requeued);
    }

    /// <summary>
    /// Minimal <see cref="ITransactionContext"/> that records the callbacks a transport registers, so a
    /// test can fire the nack path that <see cref="RebusTransactionScope"/> deliberately does not expose.
    /// </summary>
    sealed class CallbackCapturingTransactionContext : ITransactionContext
    {
        readonly List<Func<ITransactionContext, Task>> _onNack = new();

        public ConcurrentDictionary<string, object> Items { get; } = new();

        public void OnNack(Func<ITransactionContext, Task> nackAction) => _onNack.Add(nackAction);

        public void OnCommit(Func<ITransactionContext, Task> commitAction) { }
        public void OnRollback(Func<ITransactionContext, Task> rollbackAction) { }
        public void OnAck(Func<ITransactionContext, Task> ackAction) { }
        public void OnDisposed(Action<ITransactionContext> disposeAction) { }
        public void SetResult(bool commit, bool ack) { }
        public void Dispose() { }

        public async Task InvokeNackAsync()
        {
            Assert.NotEmpty(_onNack);

            foreach (var callback in _onNack) await callback(this);
        }
    }

    [Fact]
    public async Task BlockingReceiveParksUntilAMessageArrives()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders", InProcReceiveMode.Blocking);
        using var scope = new RebusTransactionScope();

        var pending = transport.Receive(scope.TransactionContext, CancellationToken.None);
        Assert.False(pending.IsCompleted);

        var message = Msg("1");
        network.Deliver("orders", message);

        Assert.Same(message, await pending);
    }

    [Fact]
    public async Task BlockingReceiveReturnsNullWhenCancelled()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders", InProcReceiveMode.Blocking);
        using var scope = new RebusTransactionScope();
        using var cts = new CancellationTokenSource();

        var pending = transport.Receive(scope.TransactionContext, cts.Token);
        cts.Cancel();

        Assert.Null(await pending);
    }

    [Fact]
    public async Task PollingReceiveReturnsNullAfterTheIntervalWithoutThrowing()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders", InProcReceiveMode.Polling);
        using var scope = new RebusTransactionScope();

        var stopwatch = Stopwatch.StartNew();
        var received = await transport.Receive(scope.TransactionContext, CancellationToken.None);
        stopwatch.Stop();

        Assert.Null(received);
        Assert.True(stopwatch.Elapsed >= TimeSpan.FromMilliseconds(25),
            $"expected the receive to wait out its interval, waited {stopwatch.ElapsedMilliseconds} ms");
    }

    [Fact]
    public async Task AOneWayClientCannotReceive()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, address: null);
        using var scope = new RebusTransactionScope();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => transport.Receive(scope.TransactionContext, CancellationToken.None));
    }

    [Fact]
    public async Task GetPropertiesReportsQueueLength()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders");
        network.Deliver("orders", Msg("1"));
        network.Deliver("orders", Msg("2"));

        var properties = await transport.GetProperties(CancellationToken.None);

        Assert.Equal("2", properties[TransportInspectorPropertyKeys.QueueLength]);
    }

    [Fact]
    public async Task SubscriptionStorageIsCentralizedAndDelegatesToTheNetwork()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders");

        Assert.True(transport.IsCentralized);

        await transport.RegisterSubscriber("SomeEvent", "orders");
        Assert.Equal(new[] { "orders" }, await transport.GetSubscriberAddresses("SomeEvent"));
        Assert.Equal(new[] { "orders" }, network.GetSubscribers("SomeEvent"));

        await transport.UnregisterSubscriber("SomeEvent", "orders");
        Assert.Empty(await transport.GetSubscriberAddresses("SomeEvent"));
    }

    [Fact]
    public async Task ResetWakesAParkedBlockingReceiveAndTheNextReceiveUsesTheNewQueue()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders", InProcReceiveMode.Blocking);

        using (var scope = new RebusTransactionScope())
        {
            var pending = transport.Receive(scope.TransactionContext, CancellationToken.None);
            Assert.False(pending.IsCompleted);

            network.Reset();

            var completed = await Task.WhenAny(pending, Task.Delay(TimeSpan.FromSeconds(5)));
            Assert.Same(pending, completed);
            Assert.Null(await pending);
        }

        var message = Msg("1");
        network.Deliver("orders", message);

        using var receiveScope = new RebusTransactionScope();
        Assert.Same(message, await transport.Receive(receiveScope.TransactionContext, CancellationToken.None));
    }

    [Fact]
    public async Task ResetDiscardsWaitingMessages()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders", InProcReceiveMode.Polling);
        network.Deliver("orders", Msg("1"));

        network.Reset();

        using var scope = new RebusTransactionScope();
        Assert.Null(await transport.Receive(scope.TransactionContext, CancellationToken.None));
        Assert.Equal(0, network.GetCount("orders"));
    }

    [Fact]
    public async Task EachDestinationOfOneTransportMessageGetsItsOwnHeaders()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, null);
        var message = Msg("1");

        using (var scope = new RebusTransactionScope())
        {
            await transport.Send("a", message, scope.TransactionContext);
            await transport.Send("b", message, scope.TransactionContext);
            await transport.Send("c", message, scope.TransactionContext);
            await scope.CompleteAsync();
        }

        var received = new[] { "a", "b", "c" }
            .Select(address => network.GetOrCreateQueue(address).TryDequeue(out var m) ? m! : null)
            .ToArray();

        Assert.Same(message, received[0]);
        Assert.Equal(3, received.Select(m => m!.Headers).Distinct(ReferenceEqualityComparer.Instance).Count());
        Assert.All(received, m => Assert.Same(message.Body, m!.Body));
        Assert.All(received, m => Assert.Equal("1", m!.Headers[Headers.MessageId]));

        received[1]!.Headers["mutated"] = "yes";
        Assert.False(received[0]!.Headers.ContainsKey("mutated"));
        Assert.False(received[2]!.Headers.ContainsKey("mutated"));
    }

    [Fact]
    public async Task DistinctTransportMessagesInOneCommitAreDeliveredUnchanged()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, null);
        var first = Msg("1");
        var second = Msg("2");

        using (var scope = new RebusTransactionScope())
        {
            await transport.Send("a", first, scope.TransactionContext);
            await transport.Send("a", second, scope.TransactionContext);
            await scope.CompleteAsync();
        }

        var queue = network.GetOrCreateQueue("a");
        Assert.True(queue.TryDequeue(out var a));
        Assert.True(queue.TryDequeue(out var b));
        Assert.Same(first, a);
        Assert.Same(second, b);
    }
}
