using Rebus.Bus;
using Rebus.Handlers;
using Rebus.InProcCors.Sample.Contracts;

namespace Rebus.InProcCors.Sample.Modules.Shipping;

/// <summary>
/// The second subscriber to <see cref="OrderPlaced"/>. Inventory is the first, and the two run
/// concurrently on their own modules' worker threads - if their log lines interleave differently between
/// runs, that is the fan-out being genuinely parallel rather than a bug.
/// <para>
/// This handler then publishes an event of its own, which is the useful part: any module can publish, not
/// just the one that started the flow, and Orders picks <see cref="OrderShipped"/> up without Shipping
/// ever naming it.
/// </para>
/// </summary>
sealed class ShipOnOrderPlacedHandler(IBus bus, ConsoleLog log) : IHandleMessages<OrderPlaced>
{
    public async Task Handle(OrderPlaced message)
    {
        var orderRef = message.OrderId.ToString()[..8];
        log.Write($"event   OrderPlaced  order {orderRef} accepted for dispatch to {message.Customer}");

        // Stand-in for the real work of picking, packing and handing over to a carrier.
        await Task.Delay(TimeSpan.FromMilliseconds(120));

        var tracking = $"TRK-{message.OrderId.ToString()[..6].ToUpperInvariant()}";
        log.Write($"        dispatched   order {orderRef} via Speedy Freight, tracking {tracking}");

        await bus.Publish(new OrderShipped(message.OrderId, "Speedy Freight", tracking));
    }
}
