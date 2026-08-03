using Rebus.Bus;
using Rebus.Handlers;
using Rebus.InProcCors.Sample.Contracts;

namespace Rebus.InProcCors.Sample.Modules.Orders;

/// <summary>
/// The orchestrating handler, and the one place in the sample where all three patterns meet:
/// it <em>asks</em> Inventory a question and waits for the answer, then <em>commands</em> Inventory
/// without waiting, then <em>announces</em> what happened to whoever is listening.
/// </summary>
sealed class PlaceOrderHandler(IBus bus, OrderBook orders, ScenarioSignals signals, ConsoleLog log)
    : IHandleMessages<PlaceOrder>
{
    static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

    public async Task Handle(PlaceOrder message)
    {
        var orderRef = message.OrderId.ToString()[..8];
        log.Write($"command PlaceOrder   order {orderRef}: {message.Customer} wants {message.Sku} x{message.Quantity}");

        // Request/reply, from Rebus.Async. This is the only place in the sample where a module blocks on
        // another module, and after extraction it becomes a real network round trip - which is exactly why
        // it is worth being able to see it in the transcript.
        log.Write($"        asking inventory whether {message.Sku} x{message.Quantity} is available...");
        var stock = await bus.SendRequest<StockLevel>(new CheckStock(message.Sku, message.Quantity),
            timeout: RequestTimeout);
        log.Write($"        inventory answered: {stock.Available} available, " +
                  $"{(stock.IsSufficient ? "sufficient" : "INSUFFICIENT")}");

        if (!stock.IsSufficient)
        {
            var reason = $"rejected - only {stock.Available} of {message.Sku} on hand, {message.Quantity} requested";
            log.Write($"        order {orderRef} {reason}");
            signals.Complete(message.OrderId, reason);
            return;
        }

        orders.Accept(message.OrderId, message.Customer);

        // A command: addressed to one queue, and Send returns as soon as the message is queued. The
        // reservation has not happened yet when the next line runs.
        await bus.Send(new ReserveStock(message.OrderId, message.Sku, message.Quantity));
        log.Write($"        sent ReserveStock to inventory (not waiting for it)");

        // An event: addressed to nobody, delivered to every subscriber. Orders does not know, and must not
        // need to know, that Inventory and Shipping are both listening.
        await bus.Publish(new OrderPlaced(message.OrderId, message.Customer, message.Sku, message.Quantity));
        log.Write($"        published OrderPlaced -> fan-out to every subscriber");
    }
}

/// <summary>
/// Closes the loop. Shipping published this event; Orders happens to subscribe. Neither module references
/// the other in any way beyond the shared contract type.
/// </summary>
sealed class OrderShippedHandler(OrderBook orders, ScenarioSignals signals, ConsoleLog log)
    : IHandleMessages<OrderShipped>
{
    public Task Handle(OrderShipped message)
    {
        var orderRef = message.OrderId.ToString()[..8];
        orders.MarkShipped(message.OrderId, message.TrackingNumber);

        var outcome = $"shipped via {message.Carrier}, tracking {message.TrackingNumber}";
        log.Write($"event   OrderShipped order {orderRef} {outcome}");
        signals.Complete(message.OrderId, outcome);
        return Task.CompletedTask;
    }
}
