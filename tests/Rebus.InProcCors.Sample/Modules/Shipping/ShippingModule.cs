using Rebus.Activation;
using Rebus.InProcCors.Sample.Contracts;

namespace Rebus.InProcCors.Sample.Modules.Shipping;

/// <summary>
/// Dispatch. The smallest module in the sample: it subscribes to one event and publishes another, and has
/// no queue-to-queue routing at all. Pure publish/subscribe, no commands in either direction.
/// </summary>
sealed class ShippingModule(InProcNetwork network) : ModuleHost(network, "shipping", ConsoleColor.Magenta)
{
    protected override void RegisterHandlers(BuiltinHandlerActivator activator, IServiceProvider provider) =>
        Handler<ShipOnOrderPlacedHandler>(activator, provider);

    protected override Task SubscribeAsync() => Bus.Subscribe<OrderPlaced>();
}
