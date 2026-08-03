namespace Rebus.InProcCors.Sample.Contracts;

// Every contract here is a deeply immutable record over primitives, which is the discipline
// Rebus.InProcCors requires: in-process the handler receives the *same instance* the caller sent,
// so a mutable contract would let two modules share writable state through the bus.
//
// Three kinds of message appear in this sample, and the distinction is about intent, not about
// any Rebus type - they are all just records:
//
//   command   sent to exactly one queue, no answer expected      PlaceOrder, ReserveStock
//   query     sent to one queue, an answer is awaited            CheckStock -> StockLevel
//   event     published to every subscriber, fan-out             OrderPlaced, OrderShipped

#region Commands

/// <summary>Asks the Orders module to accept a new order. Sent by the scenario driver.</summary>
public sealed record PlaceOrder(Guid OrderId, string Customer, string Sku, int Quantity);

/// <summary>Tells the Inventory module to hold stock for an order. Orders -> Inventory, no reply.</summary>
public sealed record ReserveStock(Guid OrderId, string Sku, int Quantity);

#endregion

#region Request / reply

/// <summary>Asks the Inventory module whether a quantity is available. Expects <see cref="StockLevel"/>.</summary>
public sealed record CheckStock(string Sku, int Quantity);

/// <summary>The answer to <see cref="CheckStock"/>.</summary>
public sealed record StockLevel(string Sku, int Available, bool IsSufficient);

#endregion

#region Events

/// <summary>Published by Orders once an order is accepted. Inventory and Shipping both subscribe.</summary>
public sealed record OrderPlaced(Guid OrderId, string Customer, string Sku, int Quantity);

/// <summary>Published by Shipping once an order leaves the warehouse. Orders subscribes.</summary>
public sealed record OrderShipped(Guid OrderId, string Carrier, string TrackingNumber);

#endregion
