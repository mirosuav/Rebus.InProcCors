using Rebus.Bus;
using Rebus.Handlers;
using Rebus.InProcCors.Sample.Contracts;

namespace Rebus.InProcCors.Sample.Modules.Inventory;

/// <summary>
/// Answers <see cref="CheckStock"/> with a <see cref="StockLevel"/>. This is the replying half of the
/// request/reply pattern, and the notable thing about it is how ordinary it looks: it calls
/// <c>bus.Reply</c> and knows nothing about Rebus.Async, correlation ids, or the fact that somebody on the
/// other side is blocked awaiting a <see cref="Task"/>. All of that lives in the requesting bus.
/// </summary>
sealed class CheckStockHandler(IBus bus, StockLedger ledger, ConsoleLog log) : IHandleMessages<CheckStock>
{
    public async Task Handle(CheckStock message)
    {
        var available = ledger.Available(message.Sku);
        var sufficient = available >= message.Quantity;

        log.Write($"query   CheckStock  {message.Sku} x{message.Quantity} -> {available} on hand, " +
                  $"{(sufficient ? "sufficient" : "INSUFFICIENT")}");

        // Reply goes back to whatever queue the incoming message named as its return address.
        await bus.Reply(new StockLevel(message.Sku, available, sufficient));
    }
}

/// <summary>
/// Handles the <see cref="ReserveStock"/> command. A command, unlike a query, produces no answer - the
/// sender has already moved on by the time this runs.
/// </summary>
sealed class ReserveStockHandler(StockLedger ledger, ConsoleLog log) : IHandleMessages<ReserveStock>
{
    public Task Handle(ReserveStock message)
    {
        var remaining = ledger.Reserve(message.Sku, message.Quantity);
        log.Write($"command ReserveStock {message.Sku} x{message.Quantity} for order {Short(message.OrderId)} " +
                  $"-> {remaining} left on hand");
        return Task.CompletedTask;
    }

    static string Short(Guid id) => id.ToString()[..8];
}

/// <summary>
/// Handles the <see cref="OrderPlaced"/> event. Inventory is one of two subscribers - Shipping is the
/// other - and neither knows the other exists. This is the difference that matters between an event and a
/// command: the publisher names no destination, so adding a third subscriber later changes nothing here.
/// </summary>
sealed class InventoryOrderPlacedHandler(ConsoleLog log) : IHandleMessages<OrderPlaced>
{
    public Task Handle(OrderPlaced message)
    {
        log.Write($"event   OrderPlaced  order {message.OrderId.ToString()[..8]} noted in the stock journal");
        return Task.CompletedTask;
    }
}
