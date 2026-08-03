using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Rebus.InProcCors;
using Rebus.InProcCors.Sample;
using Rebus.InProcCors.Sample.Modules.Inventory;
using Rebus.InProcCors.Sample.Modules.Orders;
using Rebus.InProcCors.Sample.Modules.Shipping;

ConsoleLog.Note("""
    Rebus.InProcCors sample - three modules in one process, talking only over the bus.

      orders     yellow    places orders, asks inventory, publishes OrderPlaced
      inventory  cyan      answers stock queries, reserves stock, subscribes to OrderPlaced
      shipping   magenta   subscribes to OrderPlaced, publishes OrderShipped
      driver     green     runs the two scenarios below

    Handlers run on Rebus worker threads, so lines from different modules genuinely interleave.
    """);

// The one thing every module shares. Each module gets its own bus and its own container; the network is
// the only common object, and the only way across it is a message.
var network = new InProcNetwork();

// Shared between the driver and the Orders module only - see ScenarioSignals for why it is needed at all.
var signals = new ScenarioSignals();

var builder = Host.CreateApplicationBuilder(args);

// The sample's transcript is ConsoleLog. Silence the host's own logging so it does not compete.
builder.Logging.ClearProviders();

builder.Services.AddSingleton(signals);

// Registration order is load-bearing: the generic host starts IHostedService instances sequentially, so
// every module below finishes subscribing before ScenarioDriver publishes anything.
AddModule(new InventoryModule(network));
AddModule(new ShippingModule(network));
AddModule(new OrdersModule(network, signals));
builder.Services.AddHostedService<ScenarioDriver>();

await builder.Build().RunAsync();
return;

// Registered twice on purpose: once as its concrete type, so ScenarioDriver can resolve OrdersModule and
// reach its bus, and once as IHostedService so the host starts and stops it.
void AddModule<TModule>(TModule module) where TModule : ModuleHost
{
    builder.Services.AddSingleton(module);
    builder.Services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<TModule>());
}
