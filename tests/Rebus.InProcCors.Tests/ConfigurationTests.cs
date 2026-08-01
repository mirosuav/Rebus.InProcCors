using Rebus.Activation;
using Rebus.Config;
using Rebus.Routing.TypeBased;
using Rebus.Serialization;
using Rebus.Transport;

namespace Rebus.InProcCors.Tests;

public class ConfigurationTests
{
    sealed record PlaceOrder(string Sku);

    [Fact]
    public async Task AConfiguredBusDeliversByReference()
    {
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var activator = new BuiltinHandlerActivator();
        activator.Handle<PlaceOrder>(message =>
        {
            received.TrySetResult(message);
            return Task.CompletedTask;
        });

        var bus = Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, "orders"))
            .Start();

        var sent = new PlaceOrder("ABC");
        await bus.SendLocal(sent);

        var handled = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Same(sent, handled);
    }

    [Fact]
    public async Task TheOptionsCallbackSelectsTheReceiveMode()
    {
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var activator = new BuiltinHandlerActivator();
        activator.Handle<PlaceOrder>(message =>
        {
            received.TrySetResult(message);
            return Task.CompletedTask;
        });

        var bus = Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, "orders", o => o.ReceiveMode = InProcReceiveMode.Polling))
            .Start();

        var sent = new PlaceOrder("ABC");
        await bus.SendLocal(sent);

        Assert.Same(sent, await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task AOneWayClientCanSendToAnotherBus()
    {
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var receiverActivator = new BuiltinHandlerActivator();
        receiverActivator.Handle<PlaceOrder>(message =>
        {
            received.TrySetResult(message);
            return Task.CompletedTask;
        });
        Configure.With(receiverActivator)
            .Transport(t => t.UseInProcTransport(network, "orders"))
            .Start();

        using var senderActivator = new BuiltinHandlerActivator();
        var sender = Configure.With(senderActivator)
            .Transport(t => t.UseInProcTransportAsOneWayClient(network))
            .Routing(r => r.TypeBased().Map<PlaceOrder>("orders"))
            .Start();

        var sent = new PlaceOrder("ABC");
        await sender.Send(sent);

        Assert.Same(sent, await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task PassingRegisterReferenceSerializerFalseLeavesTheDefaultSerializerInPlace()
    {
        // Correction C1: Rebus's Injectionist throws on a duplicate primary registration and does not
        // expose PossiblyRegisterDefault to extension authors, so opting out is a flag rather than
        // an override. This also gives benchmark arm 2 (InProcTransport + System.Text.Json) its config.
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var activator = new BuiltinHandlerActivator();
        activator.Handle<PlaceOrder>(message =>
        {
            received.TrySetResult(message);
            return Task.CompletedTask;
        });

        var bus = Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, "orders", registerReferenceSerializer: false))
            .Start();

        var sent = new PlaceOrder("ABC");
        await bus.SendLocal(sent);

        var handled = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(sent, handled);
        Assert.NotSame(sent, handled);  // it went through JSON
    }

    [Fact]
    public async Task AnExplicitSerializationCallCombinesWithRegisterReferenceSerializerFalse()
    {
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var activator = new BuiltinHandlerActivator();
        activator.Handle<PlaceOrder>(message =>
        {
            received.TrySetResult(message);
            return Task.CompletedTask;
        });

        var bus = Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, "orders", registerReferenceSerializer: false))
            .Serialization(s => s.UseReferenceSerializer())
            .Start();

        var sent = new PlaceOrder("ABC");
        await bus.SendLocal(sent);

        Assert.Same(sent, await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void RegisteringTheSerializerTwiceFailsLoudly()
    {
        // Documents correction C1: this is why registerReferenceSerializer exists.
        var network = new InProcNetwork();
        using var activator = new BuiltinHandlerActivator();

        Assert.Throws<InvalidOperationException>(() => Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, "orders"))
            .Serialization(s => s.UseReferenceSerializer())
            .Start());
    }

    [Fact]
    public async Task PubSubWorksWithoutExtraSubscriptionConfiguration()
    {
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var subscriberActivator = new BuiltinHandlerActivator();
        subscriberActivator.Handle<PlaceOrder>(message =>
        {
            received.TrySetResult(message);
            return Task.CompletedTask;
        });
        var subscriber = Configure.With(subscriberActivator)
            .Transport(t => t.UseInProcTransport(network, "subscriber"))
            .Start();
        await subscriber.Subscribe<PlaceOrder>();

        using var publisherActivator = new BuiltinHandlerActivator();
        var publisher = Configure.With(publisherActivator)
            .Transport(t => t.UseInProcTransport(network, "publisher"))
            .Start();

        var sent = new PlaceOrder("ABC");
        await publisher.Publish(sent);

        Assert.Same(sent, await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }
}
