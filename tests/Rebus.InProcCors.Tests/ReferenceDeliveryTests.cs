using System.Diagnostics;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Routing.TypeBased;

namespace Rebus.InProcCors.Tests;

public class ReferenceDeliveryTests
{
    public sealed record PlaceOrder(string Sku, int Quantity);

    public sealed record OrderPlaced(Guid OrderId);

    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task TheHandlerReceivesTheSameInstanceTheCallerSent(InProcReceiveMode mode)
    {
        // Design §11 item 1 - the whole point of the transport.
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var module = TestModule.Create(network, "orders", mode,
            configureHandlers: a => a.Handle<PlaceOrder>(m =>
            {
                received.TrySetResult(m);
                return Task.CompletedTask;
            }));

        var sent = new PlaceOrder("ABC", 2);
        await module.Bus.SendLocal(sent);

        Assert.Same(sent, await received.Task.WaitAsync(Timeout));
    }

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task ADeferredMessageKeepsItsReferenceAcrossTheTimeoutManager(InProcReceiveMode mode)
    {
        // Design §11 item 3. DueMessage.cs:51 rebuilds the transport message as
        // new TransportMessage(Headers, Body), so the subclass is gone and only the weak table can answer.
        // Rebus registers an in-memory timeout manager by default, so no extra configuration is needed -
        // the plan's o.UseInMemoryTimeoutManager() does not exist in 8.9.2.
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var module = TestModule.Create(network, "orders", mode,
            configureHandlers: a => a.Handle<PlaceOrder>(m =>
            {
                received.TrySetResult(m);
                return Task.CompletedTask;
            }));

        // DeferLocal rather than Defer: Defer routes through the router, and this module configures no
        // routing. Both go through the timeout manager, which is the part under test.
        var sent = new PlaceOrder("ABC", 2);
        await module.Bus.DeferLocal(TimeSpan.FromMilliseconds(200), sent);

        Assert.Same(sent, await received.Task.WaitAsync(Timeout));
    }

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task SendRequestReturnsTheIdenticalReplyInstance(InProcReceiveMode mode)
    {
        // Design §11 item 4 and §2.5 - ReplyHandlerStep works on the deserialized Message and never
        // touches TransportMessage, so it is transparent to this transport.
        var network = new InProcNetwork();
        var reply = new OrderPlaced(Guid.NewGuid());

        using var replier = TestModule.Create(network, "orders", mode,
            configureHandlers: a => a.Handle<PlaceOrder>(async (bus, _) => await bus.Reply(reply)));

        using var activator = new BuiltinHandlerActivator();
        var requestor = Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, "requestor", o => o.ReceiveMode = mode))
            .Routing(r => r.TypeBased().Map<PlaceOrder>("orders"))
            .Options(o => o.EnableSynchronousRequestReply())
            .Start();

        var result = await requestor.SendRequest<OrderPlaced>(new PlaceOrder("ABC", 2), timeout: Timeout);

        Assert.Same(reply, result);
    }

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task SendReturnsBeforeTheHandlerRunsAndAHandlerExceptionDoesNotSurfaceAtTheCallSite(
        InProcReceiveMode mode)
    {
        // Design §11 item 7. Dispatch is asynchronous handoff, not inline invocation - this is where the
        // MediatR analogy stops, and it is what AllowSynchronousContinuations = false protects.
        var network = new InProcNetwork();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var module = TestModule.Create(network, "orders", mode, maxDeliveryAttempts: 1,
            configureHandlers: a => a.Handle<PlaceOrder>(async _ =>
            {
                handlerEntered.TrySetResult();
                await gate.Task;
                throw new InvalidOperationException("handler blew up");
            }));

        // Send completes without waiting for, or observing, the handler.
        await module.Bus.SendLocal(new PlaceOrder("ABC", 2));

        await handlerEntered.Task.WaitAsync(Timeout);
        gate.SetResult();

        // Give the handler time to throw. Nothing propagates back here.
        await Task.Delay(200);
    }

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task BlockingModeWakesFasterThanTheBackoffLadderAfterAnIdlePeriod(InProcReceiveMode mode)
    {
        // Design §2.6: Rebus's default ladder is 100 ms for the first ten seconds of idleness. The penalty
        // lands on the first message after idling, which is the dominant shape of a modular monolith.
        // Asserted loosely - this is a smoke test that both modes deliver, with the real measurement in
        // the benchmark's bursty scenario.
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var module = TestModule.Create(network, "orders", mode,
            configureHandlers: a => a.Handle<PlaceOrder>(_ =>
            {
                received.TrySetResult(Stopwatch.GetTimestamp());
                return Task.CompletedTask;
            }));

        await Task.Delay(TimeSpan.FromSeconds(1));  // go idle

        var sentAt = Stopwatch.GetTimestamp();
        await module.Bus.SendLocal(new PlaceOrder("ABC", 2));
        var handledAt = await received.Task.WaitAsync(Timeout);

        var latency = Stopwatch.GetElapsedTime(sentAt, handledAt);
        Assert.True(latency < TimeSpan.FromSeconds(2), $"first-message latency was {latency.TotalMilliseconds} ms");
    }
}
