# Rebus.InProcCors

A Rebus plugin for modular monoliths: modules talk to each other over the bus, but **within a single process the message travels by reference** instead of being serialized. Extracting a module into a standalone service is a one-line transport configuration change — handler code is untouched. Message serializability is policed separately, by a reflective test.

## Goal

Keep the bus as the module boundary without paying the serialization tax on traffic that never leaves the process, and without weakening the guarantee that the same handler code will still work once a module is carved out.

## Mental model: one codebase, two runtime shapes

The same handler code, the same `IBus` calls, and the same message contracts run in two shapes, selected by transport configuration alone.

**While the system is a modular monolith**, bus traffic behaves much like MediatR: in-process, no broker, no network, and the message instance passed **by reference**. What differs from MediatR — and this difference is the entire point — is that the handler is **resolved from its own module's container, in a fresh scope**, rather than from the caller's ambient scope. The caller cannot leak a `DbContext`, an ambient transaction, or any of its own registrations into the handler, because it has no way to reach into the other module's container.

**Once a module is extracted into a service**, the exact same code runs as an ordinary Rebus queue over whatever infrastructure sits underneath — RabbitMQ, Azure Service Bus, or anything else Rebus supports. Handler code does not change. The transport registration does.

### Where the MediatR analogy stops

The analogy is about *feel and cost*, not about semantics. Dispatch is **asynchronous handoff, not inline invocation**: `bus.Send` puts the message into the `Channel<T>` and returns, and a Rebus worker picks it up on another thread microseconds later. The handler has not run when `Send` returns, and a handler exception becomes a retry and then a dead-letter message — it never surfaces at the call site.

| Aspect | MediatR | This transport, in-proc | After extraction |
|---|---|---|---|
| Transport | direct method call | `Channel<T>`, by reference | broker |
| Serialization | none | none | yes |
| Handler resolution | caller's ambient scope | own module's container, new scope | own module's container, new scope |
| Completion awaited by caller | yes | no | no |
| Handler exception surfaces | at the call site | retry, then DLQ | retry, then DLQ |
| Retries, sagas, outbox, headers | no | yes | yes |
| Latency | nanoseconds | microseconds | milliseconds |

Note also that `ITransport.Send` enlists in the ambient transaction context, so a message sent *from inside a handler* is dispatched when that handler's unit of work commits — deferred by construction, in both shapes.

### Request/reply ergonomics: Rebus.Async

Where a caller genuinely needs an answer back, [`Rebus.Async`](https://github.com/rebus-org/Rebus.Async) supplies the MediatR-like shape without abandoning the messaging semantics:

```csharp
.Options(o => o.EnableSynchronousRequestReply())

var reply = await bus.SendRequest<SomeReply>(new SomeRequest(), timeout: TimeSpan.FromSeconds(7));
```

This still travels the full transport — the request is dispatched, a worker handles it, the reply is correlated back and completes a pending `TaskCompletionSource`. It is *awaited* round-trip, not inline execution, so it survives extraction unchanged. Version 10.0.0 targets `netstandard2.0` and requires Rebus 8.0.1 or later.

Three caveats, carried deliberately:

- **It must be enabled at both ends**, requestor and replier. In-proc that is one configuration; after extraction it is two, in two repositories.
- **The requestor holds transient in-memory state while awaiting.** If the process dies, the reply has nobody left to handle it. In-proc this is invisible, because requestor and replier are the same process and die together. After extraction they do not.
- **The timeout changes character at extraction.** In-proc a seven-second timeout will essentially never fire; over a broker it is a live failure path. This is a silent-until-extraction-day risk, the class of defect this project exists to eliminate.

The package's own author cautions against leaning on it heavily, and prefers genuinely asynchronous modelling with explicit correlation identifiers. That advice is accepted here: `SendRequest` is the exception, not the default.

### Exception semantics

Stated explicitly, because the natural intuition — that in-proc a handler exception propagates to the caller while a distributed one does not — **is false for this design**.

A handler never runs on the caller's stack. `bus.Send` writes into the `Channel<T>` and returns; a Rebus worker reads it and runs the incoming pipeline on its own thread. A handler exception is therefore caught by Rebus's retry machinery and ends in the error queue — **identically in both shapes**. This symmetry is a consequence of leaving the pipeline untouched, not something the transport has to add.

Two compensations were considered and rejected:

- **Swallowing and logging handler exceptions.** Actively harmful. The exception escaping the handler is the *signal* Rebus uses to decide retry and dead-lettering. Swallow it and Rebus sees success and acks the message — silent loss, in-proc only. The compensation would create the divergence it was meant to remove.
- **Wrapping every handler exception** in a common type such as `MessageHandlerCaughtException(Exception inner)`. Less destructive, still a regression: retry policies, fail-fast checks and any custom `IErrorHandler` decide by exception type. Collapsing every failure into one wrapper erases the distinction those policies match on.

Where the asymmetry actually lives:

| Path | In-proc | Extracted |
|---|---|---|
| Handler throws | retry, then error queue | retry, then error queue — **same** |
| `bus.Send` itself fails | effectively never | broker down, connection lost, message too large |
| `SendRequest` times out | effectively never | genuine, routine failure path |

The second row runs *opposite* to the intuition above and is the dangerous one: in-proc, `Send` succeeding is close to a certainty, so call sites are naturally written as though it cannot fail. After extraction it can.

**Dead-lettering carries a live reference.** Rebus dead-letters by sending the failed `TransportMessage` to the error queue, which on this transport means the error queue holds a live reference to the message object. Two consequences with no distributed counterpart: the object cannot be collected while it sits there, and anything inspecting or replaying it receives the same mutable instance the failed handler may already have mutated.

### Failure as part of the reply contract

The one place worth designing for is `SendRequest`. When a handler throws, no reply is sent, the pending `TaskCompletionSource` never completes, and the caller waits out the entire timeout only to receive a `TimeoutException` that says nothing about the cause — in **both** shapes.

The fix is not in the transport. It is to make failure an explicit part of the reply contract:

```csharp
public sealed record OrderPlaced(Guid OrderId);
public sealed record OrderRejected(string Code, string Message);
```

The replier catches the expected failure, maps it to a failure reply and sends that. The caller gets a fast, typed, actionable answer instead of a slow timeout, and the behaviour is unchanged after extraction because a failure reply is an ordinary message.

Two rules keep this honest:

- **The failure reply carries a code and a message, never the exception object.** Passing a live `Exception` by reference would work in-proc and break at extraction — precisely the class of defect this project exists to prevent. Being an ordinary message contract, the failure reply is covered by the reflective serializability test for free.
- **Mapping to a failure reply asserts that the failure is expected and final.** Unexpected exceptions must still escape the handler so that Rebus can retry and dead-letter them. Mapping everything into failure replies discards retries — the swallowing problem in better clothes.

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
- [ ] Do the Rebus pipeline steps (deferral, forwarding, error/DLQ handling) **reconstruct** the `TransportMessage` instance? If they do, the subclass is lost in flight and the rejected handle-dictionary variant comes back, along with all of its cleanup. This is a wipeout risk for the entire design — **check it first**. Include `Rebus.Async` in this check: it inserts its own pipeline step to intercept correlated replies, so it is an additional place the subclass can be dropped.
- [ ] Does the `Rebus.Async` reply path work when the reply itself travels **by reference**? The reply is an ordinary message on this transport, so it should, but the correlation step and the `TaskCompletionSource` completion are the parts to verify rather than assume.
- [ ] Should the transport **serialize on dead-letter**, as the single exception to the reference-only rule? The error queue is the one place where bytes are wanted anyway — it ends the live-reference retention, gives replay a clean instance rather than a mutated one, and proves serializability at exactly the moment it matters. Cost: the transport stops being strictly single-mode, which the design deliberately avoids.
- [ ] Where is the line between an expected failure mapped to a failure reply and an unexpected exception left to escape? Drawn wrongly in one direction it silently discards retries; in the other it turns routine domain rejections into dead letters.
- [ ] Is a **symmetry test** worth writing — one asserting that a throwing handler produces the same observable outcome (retry count, error queue arrival, headers) on this transport and on a real broker? It would convert the claim of identical exception semantics from an argument into a check.
- [ ] Does MediatR-like ergonomics tempt handlers into `SendRequest` chains that are free in-proc but become N sequential network round trips after extraction? If so, the mitigation is a review rule, not a mechanism — but it should be named before the first chain appears.
- [ ] Per-module DI isolation: where does the shared in-proc network instance live, given that each module has its own container? Registered in the host container and injected downward, or a static singleton?
- [ ] Durability: a module that wants durable in-proc delivery **must** serialize — the same wall Wolverine's `DurableLocalQueue` runs into. Do we allow a mixed mode, with some queues by reference and some durable, or does durability rule this design out entirely?
- [ ] Where do instances for the reflective test come from — hand-written fixtures per type, or a generator such as AutoFixture — and what does that choice do to coverage of the value-dependent class of errors?
- [ ] **Benchmark:** this transport vs `Rebus.InMem` vs `Wolverine.BufferedLocalQueue` — throughput, latency, allocations. The two wins must be separated: absence of serialization vs `Channel<T>` instead of polling. Without that separation the benchmark says nothing interesting.

## Sources

- `Rebus/Messages/TransportMessage.cs`, `Rebus/Serialization/ISerializer.cs`, `Rebus/Transport/ITransport.cs`, `Rebus/Transport/InMem/InMemTransport.cs` — https://github.com/rebus-org/Rebus
- `src/Wolverine/Transports/Local/BufferedLocalQueue.cs`, `DurableLocalQueue.cs` — https://github.com/JasperFx/wolverine
- https://github.com/rebus-org/Rebus.Async — synchronous request/reply over Rebus, including the author's caveats
- https://www.nuget.org/packages/Rebus.Async — 10.0.0, `netstandard2.0`, requires Rebus >= 8.0.1
- https://wolverinefx.net/guide/messaging/transports/local.html
- https://github.com/rebus-org/Rebus/issues/599 — mookid8000 confirms the in-mem transport as an in-process bus
