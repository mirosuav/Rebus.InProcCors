using Microsoft.Extensions.Hosting;
using Rebus.InProcCors.Sample.Contracts;
using Rebus.InProcCors.Sample.Modules.Orders;

namespace Rebus.InProcCors.Sample;

/// <summary>
/// Runs the scripted scenarios and then shuts the host down. Registered last, so every module has already
/// started and subscribed by the time <see cref="ExecuteAsync"/> runs.
/// </summary>
sealed class ScenarioDriver(OrdersModule orders, ScenarioSignals signals, IHostApplicationLifetime lifetime)
    : BackgroundService
{
    static readonly TimeSpan OutcomeTimeout = TimeSpan.FromSeconds(30);

    readonly ConsoleLog _log = new("driver", ConsoleColor.Green);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            ConsoleLog.Banner("SCENARIO 1: stock is available, the order ships");
            await RunAsync("Ada Lovelace", "ACME-WIDGET", 2);

            ConsoleLog.Banner("SCENARIO 2: stock is short, the order is rejected");
            ConsoleLog.Note("  the request/reply answer is what decides this - the flow stops before any event",
                ConsoleColor.DarkGray);
            await RunAsync("Grace Hopper", "ACME-GIZMO", 99);

            ConsoleLog.Banner("done");
        }
        catch (Exception exception)
        {
            _log.Write($"scenario failed: {exception.Message}");
        }
        finally
        {
            lifetime.StopApplication();
        }
    }

    async Task RunAsync(string customer, string sku, int quantity)
    {
        var orderId = Guid.NewGuid();

        // SendLocal, not Send: this message goes to the Orders module's own queue. Even so it is a real
        // queue hop onto a worker thread, so this call returns before the handler has done anything.
        _log.Write($"placing order {orderId.ToString()[..8]} for {customer}: {sku} x{quantity}");
        await orders.Bus.SendLocal(new PlaceOrder(orderId, customer, sku, quantity));

        var outcome = await signals.Outcome(orderId, OutcomeTimeout);
        _log.Write($"order {orderId.ToString()[..8]} finished: {outcome}");

        // A beat so the last handler's log line lands before the next banner.
        await Task.Delay(TimeSpan.FromMilliseconds(150));
    }
}
