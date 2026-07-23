# Rebus.InProcCors

A Rebus plugin for modular monoliths: modules talk to each other over the bus, but **within a single process the message travels by reference** instead of being serialized. Extracting a module into a standalone service is a one-line transport configuration change — handler code is untouched. Message serializability is policed separately, by a reflective test.

## Goal

Keep the bus as the module boundary without paying the serialization tax on traffic that never leaves the process, and without weakening the guarantee that the same handler code will still work once a module is carved out.

## Problem

Rebus serializes messages **even on the in-memory transport** — deliberately, for production fidelity. In a modular monolith where each module owns its DI container, and the bus was chosen precisely so that a module can later be extracted into a service, this means a serialization tax on **all** in-process communication.

This cannot be bypassed "from above": Rebus's `ITransport` operates on `TransportMessage`, which carries a `byte[] Body`.

## Prior art: Wolverine gets this for free

Before building anything, ask what a user already has for free today. The answer here is firm, and it comes from the sources rather than the documentation:

- `Wolverine/Transports/Local/BufferedLocalQueue.cs` — has **no `IMessageSerializer` field at all**. It pushes an `Envelope` (which holds a reference to the object) into an in-memory `Block<Envelope>`. Zero serialization.
- `Wolverine/Transports/Local/DurableLocalQueue.cs` — **does** hold a `_serializer` and throws `ArgumentOutOfRangeException` when it is missing, because it persists to an inbox.

So **in-proc pass-by-reference is already free and MIT-licensed** — for `EndpointMode.BufferedInMemory`. That closes off any claim of novelty for the pass-by-reference part alone.

What survives once Wolverine is subtracted:

1. **Wolverine has no serializability verification.** This part is about *adjudicating correctness*, not generating code.
2. **Wolverine assumes one host and one container** (source-gen discovery across the whole application). Per-module DI isolation is a configuration Rebus supports naturally — one bus instance per container, sharing an in-proc network — and Wolverine does not.
3. Being already on Rebus makes switching to Wolverine a real and separate cost.

None of these three alone justifies a product. Together they are enough to solve the problem at hand and to measure the difference.

## Design

**The seam: a custom `ITransport` plus a custom `ISerializer`.** The Rebus pipeline — retries, sagas, outbox, headers, unit of work — is left untouched. That is exactly why swapping in RabbitMQ later is a configuration change rather than a code change.

### Reference carrier

A **subclass of `TransportMessage`** carrying an `object MessageInstance`.

- `Rebus/Messages/TransportMessage.cs` is a plain `public class`, **not `sealed`**, so deriving from it is legal.
- `ISerializer.Deserialize(TransportMessage)` receives the very instance the transport returned, so a downcast is sufficient.
- **Lifetime is managed by the GC.** The alternative — a `ConcurrentDictionary<long, object>` plus an 8-byte handle stored in `Body` — would require cleanup on ack, nack, retry and DLQ; every unhandled path is a leak. Rejected.

Rebus's own `InMemTransport` cannot be reused: its `Receive` calls `nextMessage.ToTransportMessage()`, which **constructs a new** `TransportMessage` and loses the subclass.

### Queue

**`Channel<T>`.** Rebus's `InMemNetwork` sits on a `ConcurrentQueue`, and workers poll it with a backoff. `Channel.Reader.WaitToReadAsync(cancellationToken)` waits asynchronously, giving lower latency and lower CPU. This win is expected to be **independent of serialization**, and possibly larger than it — which makes it the real subject of the benchmark.

### One mode

The transport carries exactly one mode: **`Reference`**. No flags, no conditional modes. Verification lives outside the transport.

### Deliberately out of scope

- **A static Roslyn analyzer for serializability.** The static route loses to running the real pipeline, because it produces false positives, and false positives destroy trust. Serializability is statically undecidable in precisely the places that matter: `object`- and interface-typed properties, polymorphism, open generics, and cycles that arise from data rather than from types.
- **Enforcing message immutability.**
- **Any abstraction over brokers.**
- **Plain `Channels` without Rebus** for in-proc traffic — that forfeits retries, sagas, outbox and headers, and breaks the core premise that the same code keeps working after extraction.

## Serializability verification

Not part of the transport. An ordinary test in the suite.

1. **Enumerate message types from handler registrations**, not from a naming convention: pull `T` out of every `IHandleMessages<T>` closure registered in the modules' containers. That is ground truth — no list to maintain, and no reliance on a `*Command` / `*Event` suffix. A message with no handler is dead code anyway.
2. **For each type, check round-trip idempotence.** `s1 = serialize(instance)`, `s2 = serialize(deserialize(s1))`, then compare `s1` and `s2` **as bytes**.

Comparing serialized output rather than object graphs is deliberate: a recursive graph comparer produces false positives on collection ordering, `DateTime` precision and floating-point precision — exactly the failure mode being avoided. Comparing two `byte[]` values has none of those problems and requires no comparer to be written.

### Guarantee boundary

Stated explicitly, because a guarantee without a stated boundary is not a guarantee.

**Caught:**

- a type the serializer cannot handle at all (it throws);
- silent data loss **on the deserialization side** — an `init`-only property with no matching constructor parameter, a private setter, an interface-typed property deserialized to its base type. In each case `s2` differs from `s1`.

**Not caught:**

- fields the serializer **never saw** — absent from `s1`, therefore also absent from `s2`;
- cases that depend on **runtime values** rather than types: an `object Payload`, a polymorphic collection. Reflection enumerates types; it cannot guess what will actually end up inside them in production;
- **aliasing** — a property of handlers, not of types. See below.

## Accepted risk: aliasing

In-memory without serialization means shared objects can be mutated by multiple handlers; without a good mitigation, the differentiator turns into a footgun.

**Decision (2026-07): no mechanical mitigation.** Discipline and code review are what stands in its place.

Considered and rejected: a `Faithful` mode (round-tripping so the handler receives a copy, run in integration tests to prove no handler depends on aliasing), source-generated deep cloning, and enforced immutability.

**This was a choice, not an oversight.** The consequence worth remembering: an aliasing bug is silent — nothing blows up, the data is simply different — and it will surface only on the day a module is extracted into a service, which is precisely the day this whole construction exists for. If that day ever approaches, the mitigation goes back on the table *before* extraction, not after.

## Open questions

A live list — the design is not closed.

- [ ] Does the Rebus worker loop correctly tolerate a `Receive` that blocks on `WaitToReadAsync` instead of returning `null` on an empty queue? **A question for a prototype, not for the documentation.** If it does not, the polling win disappears and only the serialization win remains.
- [ ] Do the Rebus pipeline steps (deferral, forwarding, error/DLQ handling) **reconstruct** the `TransportMessage` instance? If they do, the subclass is lost in flight and the rejected handle-dictionary variant comes back, along with all of its cleanup. This is a wipeout risk for the entire design — **check it first**.
- [ ] Per-module DI isolation: where does the shared in-proc network instance live, given that each module has its own container? Registered in the host container and injected downward, or a static singleton?
- [ ] Durability: a module that wants durable in-proc delivery **must** serialize — the same wall Wolverine's `DurableLocalQueue` runs into. Do we allow a mixed mode, with some queues by reference and some durable, or does durability rule this design out entirely?
- [ ] Where do instances for the reflective test come from — hand-written fixtures per type, or a generator such as AutoFixture — and what does that choice do to coverage of the value-dependent class of errors?
- [ ] **Benchmark:** this transport vs `Rebus.InMem` vs `Wolverine.BufferedLocalQueue` — throughput, latency, allocations. The two wins must be separated: absence of serialization vs `Channel<T>` instead of polling. Without that separation the benchmark says nothing interesting.

## Sources

- `Rebus/Messages/TransportMessage.cs`, `Rebus/Serialization/ISerializer.cs`, `Rebus/Transport/ITransport.cs`, `Rebus/Transport/InMem/InMemTransport.cs` — https://github.com/rebus-org/Rebus
- `src/Wolverine/Transports/Local/BufferedLocalQueue.cs`, `DurableLocalQueue.cs` — https://github.com/JasperFx/wolverine
- https://wolverinefx.net/guide/messaging/transports/local.html
- https://github.com/rebus-org/Rebus/issues/599 — mookid8000 confirms the in-mem transport as an in-process bus
