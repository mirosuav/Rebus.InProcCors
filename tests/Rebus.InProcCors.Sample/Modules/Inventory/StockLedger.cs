using System.Collections.Concurrent;

namespace Rebus.InProcCors.Sample.Modules.Inventory;

/// <summary>
/// The Inventory module's private state. It is registered only in the Inventory module's container, so no
/// other module can resolve it, read it, or mutate it - the only way to learn a stock level is to ask over
/// the bus and wait for the answer.
/// </summary>
sealed class StockLedger
{
    readonly ConcurrentDictionary<string, int> _onHand = new(StringComparer.OrdinalIgnoreCase)
    {
        ["ACME-WIDGET"] = 12,
        ["ACME-GIZMO"] = 3,
        ["ACME-SPROCKET"] = 40,
    };

    public int Available(string sku) => _onHand.TryGetValue(sku, out var quantity) ? quantity : 0;

    /// <summary>Removes stock, returning the level afterwards. Never goes below zero.</summary>
    public int Reserve(string sku, int quantity) =>
        _onHand.AddOrUpdate(sku, 0, (_, current) => Math.Max(0, current - quantity));
}
