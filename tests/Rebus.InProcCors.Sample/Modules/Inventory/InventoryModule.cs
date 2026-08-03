using Microsoft.Extensions.DependencyInjection;
using Rebus.Activation;
using Rebus.InProcCors.Sample.Contracts;

namespace Rebus.InProcCors.Sample.Modules.Inventory;

/// <summary>
/// Stock levels and reservations. Plays three roles at once, which is why it is worth reading first:
/// it answers a query, accepts a command, and subscribes to an event.
/// </summary>
sealed class InventoryModule(InProcNetwork network) : ModuleHost(network, "inventory", ConsoleColor.Cyan)
{
    protected override void ConfigureServices(IServiceCollection services) =>
        services.AddSingleton<StockLedger>();

    protected override void RegisterHandlers(BuiltinHandlerActivator activator, IServiceProvider provider)
    {
        Handler<CheckStockHandler>(activator, provider);
        Handler<ReserveStockHandler>(activator, provider);
        Handler<InventoryOrderPlacedHandler>(activator, provider);
    }

    // No routing: this module only ever replies, and a reply is addressed by the incoming message.
    protected override Task SubscribeAsync() => Bus.Subscribe<OrderPlaced>();
}
