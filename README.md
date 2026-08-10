# Rebus.InProcCors

**A modular monolith puts its modules behind a bus so they can later become services. But Rebus serializes
every message — even in-process — taxing traffic that never leaves the machine.**

## The approach

A custom Rebus `ITransport` + `ISerializer` pair that passes the message **by reference** through a
`Channel<T>` instead of serializing it. The Rebus pipeline is left completely untouched — retries, sagas,
outbox, headers, unit of work all still apply — so extracting a module into its own service is a **one-line
transport change**, and handler code never moves.

Everything is remote by default; in-proc is the optimization. That direction is deliberate: semantics stay
identical in both shapes (a handler exception retries and dead-letters in-proc exactly as it would over
RabbitMQ) rather than being "nicer" locally and surprising you on extraction day.

Two consequences worth knowing before you reach for it. Dispatch is **asynchronous handoff, not inline
invocation**: `bus.Send` writes into the channel and returns, so the handler has not run yet and its exception
never surfaces at the call site. And the handler is resolved from **its own module's container, in a fresh
scope** — the caller cannot leak a `DbContext` or an ambient transaction into it, because it has no way to
reach into the other module's container.

Two packages, deliberately independent of each other:

- **`Rebus.InProcCors`** — the transport. No dependencies beyond `Rebus` itself.
- **`Rebus.InProcCors.Verification`** — contract verification. Never references the transport; contract
  discipline is worth having in any Rebus app.

## Usage

The network is host-owned and passed explicitly — there is no static default, because a singleton would make
two independent test fixtures silently share a network.

```csharp
var network = new InProcNetwork();

Configure.With(ordersActivator)
    .Transport(t => t.UseInProcTransport(network, "orders"))
    .Start();

Configure.With(shippingActivator)
    .Transport(t => t.UseInProcTransport(network, "shipping"))
    .Start();
```

Extracting the shipping module is a change to one line, in one file:

```csharp
    .Transport(t => t.UseRabbitMq(connectionString, "shipping"))
```

Handlers, message contracts and `IBus` calls are untouched.

### Options

```csharp
// Blocking (default) parks on the channel; Polling walks Rebus's ordinary backoff ladder.
.Transport(t => t.UseInProcTransport(network, "orders",
    o => o.ReceiveMode = InProcReceiveMode.Polling))

// Send-only endpoint: no input queue, no workers.
.Transport(t => t.UseInProcTransportAsOneWayClient(network))
```

`ReferenceSerializer` is registered for you. To configure serialization yourself, opt out first — Rebus throws
on a duplicate primary registration, so the two cannot both be present:

```csharp
.Transport(t => t.UseInProcTransport(network, "orders", registerReferenceSerializer: false))
.Serialization(s => s.UseReferenceSerializer())
```

Receive mode is not visible under saturation, and is worth roughly **two and a half orders of magnitude on
the first message after an idle period** (~0.4 ms to handler entry, against ~130–160 ms polling). See
[the benchmark results](Docs/PRD.md#151-results--2026-08-01).

## Important: message contracts must be deeply immutable

This is the load-bearing constraint, not a footnote. Passing by reference means several handlers can hold the
**same instance**. That is harmless if nobody can mutate it — and a data race with no distributed counterpart
if anybody can. Orleans buys the same safety by deep-copying every call argument; this design refuses the copy,
so the commitment Orleans makes optional is **mandatory and verified** here.

Register the check on each module's own container:

```csharp
services.AddRebusInProcContractVerification();
```

It discovers message types from ground truth — the `T` in every registered `IHandleMessages<T>`, so there is
no list to maintain and no naming convention to obey — and asserts two things per type:

1. **Round-trip byte idempotence.** `serialize(deserialize(serialize(x)))` must equal `serialize(x)` as bytes.
   Catches the contract that will only break once it hits a real broker: an unbound constructor parameter, a
   private setter, an interface-typed property that deserializes to its base type.
2. **Deep immutability.** Every property get-only or `init`-only, no publicly writable field, no exposed
   mutable collection — recursing transitively through the reachable graph.

Outside Production a violation **fails startup**; in Production it is logged at error level and startup
continues.

```csharp
services.AddRebusInProcContractVerification(o =>
{
    o.Serializer = myProductionSerializer;  // default: System.Text.Json
    o.InstanceSource = myFixtures;          // for contracts the default factory can't construct
    o.VerifyOnStartup = false;              // or drive MessageContractVerifier from a test instead
    o.VerifyImmutability = false;           // legacy DTOs: keep check 1, drop check 2 — see below
});
```

### Opting out of the immutability check

`VerifyImmutability = false` is the only escape hatch, and it is deliberately coarse — it exists for the
legacy module with hundreds of plain-DTO contracts that will never be made immutable, and it keeps the
round-trip check, which is the one that guards extractability. There is **no per-type or per-member
exemption**: either a module's contracts are all immutable or nobody checks, because a per-member hatch
degrades the guarantee continuously instead of visibly. There is no way to switch off the round-trip check —
a verifier that checks nothing should not be registered.

Opting out is recorded, not hidden: `report.ImmutabilityVerified` is false, `Describe()` renders
`Verified 143 message contract(s) (round trip only; immutability check disabled).`, and the startup check
logs the same at information level on every boot.

**What the check does not cover:** mutable state *reachable* from an immutable message (a shared service or
cache held by reference), mutation by reflection, and members whose declared type is immutable while the
runtime instance is not. Keep contracts made of data.

## Status

`0.1.0`, not published to NuGet. `net8.0` / `net9.0` / `net10.0`.

```bash
dotnet build   # warnings-as-errors, XML docs required
dotnet test    # 107 tests

dotnet run --project tests/Rebus.InProcCors.Sample   # three modules, one process, scripted transcript
```

## Docs

- [PRD](Docs/PRD.md) — the single living document: theoretical grounding (Waldo, Akka, Orleans), prior art,
  exception semantics, the reference carrier and the weak side table that survives Rebus's `Clone()`, the
  verification design, benchmark results, and the questions still open. Two appendices are worth knowing
  about: [the findings from the Rebus sources](Docs/PRD.md#appendix-a-findings-from-the-rebus-sources) the
  design rests on, and [the Rebus APIs that do not exist](Docs/PRD.md#appendix-b-rebus-apis-that-do-not-exist)
  despite appearing in earlier drafts.
- [Sample](tests/Rebus.InProcCors.Sample/README.md) — three modules in one process, talking only over the bus.
- [Remote cancellation handoff](Docs/2026-08-09-remote-cancellation-handoff.md) — an open investigation.
