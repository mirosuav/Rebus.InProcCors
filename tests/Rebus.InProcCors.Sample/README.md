# Rebus.InProcCors sample

Three modules in one process, communicating only over the bus. Run it:

```bash
dotnet run --project tests/Rebus.InProcCors.Sample
```

The process runs two scripted scenarios, prints a colour-coded transcript, and exits.

## The modules

| Module | Queue | Colour | Role |
|---|---|---|---|
| Orders | `orders` | yellow | places orders, asks Inventory, publishes `OrderPlaced` |
| Inventory | `inventory` | cyan | answers stock queries, reserves stock, subscribes to `OrderPlaced` |
| Shipping | `shipping` | magenta | subscribes to `OrderPlaced`, publishes `OrderShipped` |

Each module owns a **private `ServiceProvider`**, its own `BuiltinHandlerActivator`, and its own `IBus`.
The only object all three share is the `InProcNetwork`, and the only way across it is a message. That is
what makes `Modules/Inventory/StockLedger.cs` genuinely private to Inventory: no other module can resolve
it, so the only way to learn a stock level is to ask and wait for the answer.

## The three patterns

**Command** — addressed to one queue, no answer. `bus.Send(new ReserveStock(...))` returns as soon as the
message is queued; the reservation has not happened yet when the next line runs. `bus.SendLocal` is the
same thing addressed to the sender's own queue — still a real queue hop onto a worker thread, which is
where this stops resembling MediatR.

**Request/reply** — `Rebus.Async`'s `bus.SendRequest<StockLevel>(new CheckStock(...))`, in
`Modules/Orders/OrdersHandlers.cs`. Only the *requesting* bus needs `EnableSynchronousRequestReply()`;
Inventory just calls `bus.Reply(...)` and knows nothing about correlation ids or that anyone is awaiting.

**Event** — `bus.Publish(new OrderPlaced(...))` reaches every subscriber and names none of them. Inventory
and Shipping both handle it, and neither knows the other exists. Adding a fourth subscriber would require
no change to Orders.

## What the transcript shows

```
22:50:43.210  [orders   ]         published OrderPlaced -> fan-out to every subscriber
22:50:43.214  [inventory] event   OrderPlaced  order 80bfc0d5 noted in the stock journal
22:50:43.215  [shipping ] event   OrderPlaced  order 80bfc0d5 accepted for dispatch to Ada Lovelace
22:50:43.215  [inventory] command ReserveStock ACME-WIDGET x2 for order 80bfc0d5 -> 10 left on hand
```

`ReserveStock` was **sent before** `OrderPlaced` was published, but it is handled after both subscribers.
That ordering is not stable and is not meant to be — handlers run on Rebus worker threads, so the lines
interleave differently between runs. Anything that depends on the order two modules react in is depending
on something the bus does not promise.

Scenario 2 asks for 99 of a SKU with 3 on hand. The request/reply answer is what decides it, so the flow
stops before any event is published — the rejection path exists to make the awaited answer load-bearing
rather than decorative.

## Two constraints worth knowing

**Rebus defaults to one worker per bus.** `PlaceOrderHandler` calls `SendRequest` from *inside* a handler,
so the reply arrives on the same queue while the only worker is still awaiting it — with one worker that
deadlocks until the request times out. `OrdersModule` sets `SetNumberOfWorkers(2)`. This is a real
constraint of request/reply issued from within a handler, not an artefact of the sample.

**Subscriptions must be in place before anyone publishes.** The generic host starts `IHostedService`
instances sequentially in registration order, so `Program.cs` registers all three modules before
`ScenarioDriver`. Every `Subscribe` has completed before the driver sends its first message.

## Why `ScenarioSignals` exists

`bus.Send` has no return value to await — it returns before any handler has run. So the driver learns an
order's outcome the way anything else would: by observing the effect. The Orders module completes the
signal when it rejects an order or when it sees `OrderShipped`. Inventory and Shipping never see it; they
communicate purely over the bus.

## Extracting a module

`Modules/Orders/OrdersModule.cs` maps `CheckStock` and `ReserveStock` to the `inventory` queue. Moving
Inventory into its own process means pointing that module at a real broker transport instead of
`UseInProcTransport`. The queue names stay the same and no handler is touched — which is the claim the
whole library exists to support.
