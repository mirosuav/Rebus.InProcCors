using System.Collections.Concurrent;

namespace Rebus.InProcCors.Sample.Modules.Orders;

/// <summary>
/// The Orders module's private state, registered only in the Orders module's container. Inventory and
/// Shipping cannot resolve it - they learn about orders only from the messages they receive.
/// </summary>
sealed class OrderBook
{
    readonly ConcurrentDictionary<Guid, string> _status = new();

    public void Accept(Guid orderId, string customer) => _status[orderId] = $"accepted for {customer}";

    public void MarkShipped(Guid orderId, string trackingNumber) => _status[orderId] = $"shipped ({trackingNumber})";

    public string StatusOf(Guid orderId) => _status.TryGetValue(orderId, out var status) ? status : "unknown";
}
