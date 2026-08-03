using Microsoft.Extensions.DependencyInjection;
using Rebus.Activation;
using Rebus.Config;
using Rebus.InProcCors.Sample.Contracts;
using Rebus.Routing;
using Rebus.Routing.TypeBased;

namespace Rebus.InProcCors.Sample.Modules.Orders;

/// <summary>
/// The order lifecycle, and the module that starts every flow. It is the only one that needs routing
/// (it sends to another module by name) and the only one that needs request/reply enabled.
/// </summary>
sealed class OrdersModule(InProcNetwork network, ScenarioSignals signals)
    : ModuleHost(network, "orders", ConsoleColor.Yellow)
{
    protected override void ConfigureServices(IServiceCollection services)
    {
        services.AddSingleton<OrderBook>();
        services.AddSingleton(signals);
    }

    protected override void RegisterHandlers(BuiltinHandlerActivator activator, IServiceProvider provider)
    {
        Handler<PlaceOrderHandler>(activator, provider);
        Handler<OrderShippedHandler>(activator, provider);
    }

    /// <summary>
    /// Commands and queries are addressed, so their destinations are declared here. Events are not - which
    /// is why <see cref="OrderPlaced"/> is absent from this map despite being published by this module.
    /// <para>
    /// This map is also the whole of what changes when Inventory is extracted into its own process: the
    /// queue names stay the same, the transport underneath them changes, and no handler is touched.
    /// </para>
    /// </summary>
    protected override void ConfigureRouting(StandardConfigurer<IRouter> routing) =>
        routing.TypeBased()
            .Map<CheckStock>("inventory")
            .Map<ReserveStock>("inventory");

    protected override void ConfigureOptions(OptionsConfigurer options)
    {
        // Installs the step that intercepts correlated replies and completes the Task returned by
        // SendRequest. Needed only on the requesting side; Inventory just calls bus.Reply.
        options.EnableSynchronousRequestReply();

        // Rebus defaults to ONE worker per bus, and PlaceOrderHandler calls SendRequest from inside a
        // handler - so the reply arrives on this same queue while the only worker is still awaiting it.
        // With one worker that deadlocks until the request times out. Two workers is the minimum for
        // request/reply issued from within a handler, and is a real constraint, not a sample artefact.
        options.SetNumberOfWorkers(2);
    }

    protected override Task SubscribeAsync() => Bus.Subscribe<OrderShipped>();
}
