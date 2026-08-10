# Rebus.InProcCors — product requirements and design

> The single living document for this project: what it is for, why each decision was taken, and how it is
> built. It supersedes the separate rationale and implementation-design documents, which were merged into it.
> [`2026-08-09-remote-cancellation-handoff.md`](2026-08-09-remote-cancellation-handoff.md) remains separate —
> it is an active investigation, not settled design.

A Rebus plugin for modular monoliths: modules talk to each other over the bus, but **within a single process
the message travels by reference** instead of being serialized. Extracting a module into a standalone service
is a one-line transport configuration change — handler code is untouched. Message serializability is policed
separately, by a reflective check over the contracts.

---

## 1. Goal

Keep the bus as the module boundary without paying the serialization tax on traffic that never leaves the
process, and without weakening the guarantee that the same handler code will still work once a module is
carved out.

## 2. The problem

Rebus serializes messages **even on the in-memory transport** — deliberately, for production fidelity. In a
modular monolith where each module owns its DI container, and the bus was chosen precisely so that a module
can later be extracted into a service, this means a serialization tax on **all** in-process communication.

This cannot be bypassed "from above": Rebus's `ITransport` operates on `TransportMessage`, which carries a
`byte[] Body`.

## 3. Theoretical grounding

The design is not novel in principle. It is an application of a well-established position, and naming that
position is what keeps the decisions below from looking arbitrary.

**Waldo, Wyant, Wollrath and Kendall, *A Note on Distributed Computing* (Sun, 1994)** identifies four
irreducible differences between local and remote computing — **latency, memory access, partial failure,
concurrency** — and argues that any system papering over the local/remote distinction fails to support basic
requirements of robustness and reliability. All four are load-bearing here:

| Difference | Where it appears in this design |
|---|---|
| Memory access | Pass-by-reference and aliasing — see [§13 Contract verification](#13-contract-verification) |
| Partial failure | `bus.Send` cannot fail in-proc, can fail after extraction — see [§7.3](#73-exception-semantics) |
| Latency | `SendRequest` chains that are free in-proc and are round trips afterwards |
| Concurrency | Modules share one process and one thread pool in-proc, and do not after extraction — [§16](#16-open-questions) |

**Akka's location transparency** states the principle this project actually follows:

> The key for enabling this is to go from remote to local by way of optimization instead of trying to go from
> local to remote by way of generalization.

The direction matters. Waldo's warning is aimed at making *remote look local* — the RPC-transparency failure
mode. This design does the inverse: it starts from remote semantics (queue, retries, dead-lettering, no
inline exception propagation) and optimizes the local case. Everything is remote by default; in-proc is the
optimization. That is the justification for leaving the Rebus pipeline untouched, and for
[exception semantics](#73-exception-semantics) being identical in both shapes rather than "nicer" in-proc.

**Orleans** solved the same problem with the opposite default, and its choice is instructive. Orleans
deep-copies grain call arguments *even within a single silo*, specifically to stop the caller mutating them
afterwards, and lets you opt out per type, parameter or field with `[Immutable]`. Its documentation is
precise about what that opt-out means:

> Using `Immutable<T>` implies neither the provider nor the recipient of the value will modify it in the
> future. It's a mutual, dual-sided commitment, not a one-sided one.

And about what immutability has to mean to be worth anything:

> …use bitwise immutability rather than logical immutability.

This design inverts the default — reference always, never copy — which is only defensible because the
commitment Orleans makes optional is made **mandatory and verified** here. See
[§13.2](#132-why-contracts-must-be-deeply-immutable).

**The fallacies of distributed computing** (Deutsch, Gosling) name the two beliefs the in-proc shape silently
teaches call sites: *the network is reliable* and *latency is zero*. Both are true in-proc and false
afterwards, which is why they are called out explicitly rather than left to be discovered.

## 4. Mental model: one codebase, two runtime shapes

The same handler code, the same `IBus` calls, and the same message contracts run in two shapes, selected by
transport configuration alone.

**While the system is a modular monolith**, bus traffic behaves much like MediatR: in-process, no broker, no
network, and the message instance passed **by reference**. What differs from MediatR — and this difference is
the entire point — is that the handler is **resolved from its own module's container, in a fresh scope**,
rather than from the caller's ambient scope. The caller cannot leak a `DbContext`, an ambient transaction, or
any of its own registrations into the handler, because it has no way to reach into the other module's
container.

**Once a module is extracted into a service**, the exact same code runs as an ordinary Rebus queue over
whatever infrastructure sits underneath — RabbitMQ, Azure Service Bus, or anything else Rebus supports.
Handler code does not change. The transport registration does.

## 5. Prior art: Wolverine gets this for free

Before building anything, ask what a user already has for free today. The answer here is firm, and it comes
from the sources rather than the documentation:

- `Wolverine/Transports/Local/BufferedLocalQueue.cs` — has **no `IMessageSerializer` field at all**. It
  pushes an `Envelope` (which holds a reference to the object) into an in-memory `Block<Envelope>`. Zero
  serialization.
- `Wolverine/Transports/Local/DurableLocalQueue.cs` — **does** hold a `_serializer` and throws
  `ArgumentOutOfRangeException` when it is missing, because it persists to an inbox.

So **in-proc pass-by-reference is already free and MIT-licensed** — for `EndpointMode.BufferedInMemory`. That
closes off any claim of novelty for the pass-by-reference part alone.

What survives once Wolverine is subtracted:

1. **Wolverine has no serializability verification.** That part is about *adjudicating correctness*, not
   generating code.
2. **Wolverine assumes one host and one container** (source-gen discovery across the whole application).
   Per-module DI isolation is a configuration Rebus supports naturally — one bus instance per container,
   sharing an in-proc network — and Wolverine does not.
3. Being already on Rebus makes switching to Wolverine a real and separate cost.

None of these three alone justifies a product. Together they are enough to solve the problem at hand and to
measure the difference.

## 6. Scope

**The seam: a custom `ITransport` plus a custom `ISerializer`.** The Rebus pipeline — retries, sagas, outbox,
headers, unit of work — is left untouched. That is exactly why swapping in RabbitMQ later is a configuration
change rather than a code change.

The transport carries exactly one mode: **`Reference`**. No flags, no conditional modes. Verification lives
outside the transport.

Deliberately out of scope:

- **A static Roslyn analyzer for serializability.** The static route loses to running the real pipeline,
  because it produces false positives, and false positives destroy trust. Serializability is statically
  undecidable in precisely the places that matter: `object`- and interface-typed properties, polymorphism,
  open generics, and cycles that arise from data rather than from types. (This reasoning is specific to
  serializability; for immutability the calculus is different — see [§16](#16-open-questions).)
- **Enforcing immutability at runtime**, by copying or cloning. Immutability *is* required of message
  contracts, but it is checked over the contracts rather than bought with a per-message copy —
  [§13.2](#132-why-contracts-must-be-deeply-immutable).
- **Any abstraction over brokers.**
- **Plain `Channel`s without Rebus** for in-proc traffic — that forfeits retries, sagas, outbox and headers,
  and breaks the core premise that the same code keeps working after extraction.

## 7. Usage

The network is host-owned and passed explicitly — there is no static default, because a singleton would make
two independent test fixtures silently share a network.

```csharp
var network = new InProcNetwork();

// Module: orders
Configure.With(ordersActivator)
    .Transport(t => t.UseInProcTransport(network, "orders"))
    .Start();

// Module: shipping
Configure.With(shippingActivator)
    .Transport(t => t.UseInProcTransport(network, "shipping"))
    .Start();
```

Extracting the shipping module is a change to one line, in one file:

```csharp
    .Transport(t => t.UseRabbitMq(connectionString, "shipping"))
```

Each module verifies its own contracts, on its own container:

```csharp
services.AddRebusInProcContractVerification();
```

Outside Production a violation fails startup; in Production it is logged and startup continues.

Two receive modes are available. `Blocking` is the default and parks on the channel; `Polling` walks Rebus's
ordinary backoff ladder. The difference is not visible under saturation and is roughly two and a half orders
of magnitude on the first message after an idle period — see [§15 Benchmark](#15-benchmark).

```csharp
.Transport(t => t.UseInProcTransport(network, "orders", o => o.ReceiveMode = InProcReceiveMode.Polling))
```

`ReferenceSerializer` is registered by default. To configure serialization yourself, opt out of that
registration first — Rebus throws on a duplicate primary registration, so the two cannot both be present:

```csharp
.Transport(t => t.UseInProcTransport(network, "orders", registerReferenceSerializer: false))
.Serialization(s => s.UseReferenceSerializer())
```

### 7.1 Where the MediatR analogy stops

The analogy is about *feel and cost*, not about semantics. Dispatch is **asynchronous handoff, not inline
invocation**: `bus.Send` puts the message into the `Channel<T>` and returns, and a Rebus worker picks it up
on another thread microseconds later. The handler has not run when `Send` returns, and a handler exception
becomes a retry and then a dead-letter message — it never surfaces at the call site.

| Aspect | MediatR | This transport, in-proc | After extraction |
|---|---|---|---|
| Transport | direct method call | `Channel<T>`, by reference | broker |
| Serialization | none | none | yes |
| Handler resolution | caller's ambient scope | own module's container, new scope | own module's container, new scope |
| Completion awaited by caller | yes | no | no |
| Handler exception surfaces | at the call site | retry, then DLQ | retry, then DLQ |
| Retries, sagas, outbox, headers | no | yes | yes |
| Latency | nanoseconds | microseconds | milliseconds |

Note also that `ITransport.Send` enlists in the ambient transaction context, so a message sent *from inside a
handler* is dispatched when that handler's unit of work commits — deferred by construction, in both shapes.

### 7.2 Request/reply ergonomics: Rebus.Async

Where a caller genuinely needs an answer back, [`Rebus.Async`](https://github.com/rebus-org/Rebus.Async)
supplies the MediatR-like shape without abandoning the messaging semantics:

```csharp
.Options(o => o.EnableSynchronousRequestReply())

var reply = await bus.SendRequest<SomeReply>(new SomeRequest(), timeout: TimeSpan.FromSeconds(7));
```

This still travels the full transport — the request is dispatched, a worker handles it, the reply is
correlated back and completes a pending `TaskCompletionSource`. It is *awaited* round-trip, not inline
execution, so it survives extraction unchanged. Version 10.0.0 targets `netstandard2.0` and requires Rebus
8.0.1 or later. `EnableSynchronousRequestReply` lives in `Rebus.Config` and `SendRequest` in namespace
`Rebus` — see [Appendix B](#appendix-b-rebus-apis-that-do-not-exist).

Three caveats, carried deliberately:

- **It must be enabled at both ends**, requestor and replier. In-proc that is one configuration; after
  extraction it is two, in two repositories.
- **The requestor holds transient in-memory state while awaiting.** If the process dies, the reply has nobody
  left to handle it. In-proc this is invisible, because requestor and replier are the same process and die
  together. After extraction they do not.
- **The timeout changes character at extraction.** In-proc a seven-second timeout will essentially never
  fire; over a broker it is a live failure path. This is a silent-until-extraction-day risk, the class of
  defect this project exists to eliminate.

The package's own author cautions against leaning on it heavily, and prefers genuinely asynchronous modelling
with explicit correlation identifiers. That advice is accepted here: `SendRequest` is the exception, not the
default.

### 7.3 Exception semantics

Stated explicitly, because the natural intuition — that in-proc a handler exception propagates to the caller
while a distributed one does not — **is false for this design**.

A handler never runs on the caller's stack. `bus.Send` writes into the `Channel<T>` and returns; a Rebus
worker reads it and runs the incoming pipeline on its own thread. A handler exception is therefore caught by
Rebus's retry machinery and ends in the error queue — **identically in both shapes**. This symmetry is a
consequence of leaving the pipeline untouched, not something the transport has to add.

Two compensations were considered and rejected:

- **Swallowing and logging handler exceptions.** Actively harmful. The exception escaping the handler is the
  *signal* Rebus uses to decide retry and dead-lettering. Swallow it and Rebus sees success and acks the
  message — silent loss, in-proc only. The compensation would create the divergence it was meant to remove.
- **Wrapping every handler exception** in a common type such as `MessageHandlerCaughtException(Exception
  inner)`. Less destructive, still a regression: retry policies, fail-fast checks and any custom
  `IErrorHandler` decide by exception type. Collapsing every failure into one wrapper erases the distinction
  those policies match on.

Where the asymmetry actually lives:

| Path | In-proc | Extracted |
|---|---|---|
| Handler throws | retry, then error queue | retry, then error queue — **same** |
| `bus.Send` itself fails | effectively never | broker down, connection lost, message too large |
| `SendRequest` times out | effectively never | genuine, routine failure path |

The second row runs *opposite* to the intuition above and is the dangerous one: in-proc, `Send` succeeding is
close to a certainty, so call sites are naturally written as though it cannot fail. After extraction it can.

**Dead-lettering carries a live reference.** The message reaches the error queue with its object identity
intact — via the weak side table, since the `TransportMessage` subclass itself is discarded on the way there
([Appendix A.2](#a2-the-transportmessage-subclass-is-lost-at-the-dead-letter-boundary)). Two consequences
with no distributed counterpart: the object cannot be collected while it sits there, and anything inspecting
or replaying it receives the same instance the failed handler already saw.

### 7.4 Failure as part of the reply contract

The one place worth designing for is `SendRequest`. When a handler throws, no reply is sent, the pending
`TaskCompletionSource` never completes, and the caller waits out the entire timeout only to receive a
`TimeoutException` that says nothing about the cause — in **both** shapes.

The fix is not in the transport. It is to make failure an explicit part of the reply contract:

```csharp
public sealed record OrderPlaced(Guid OrderId);
public sealed record OrderRejected(string Code, string Message);
```

The replier catches the expected failure, maps it to a failure reply and sends that. The caller gets a fast,
typed, actionable answer instead of a slow timeout, and the behaviour is unchanged after extraction because a
failure reply is an ordinary message.

Two rules keep this honest:

- **The failure reply carries a code and a message, never the exception object.** Passing a live `Exception`
  by reference would work in-proc and break at extraction — precisely the class of defect this project exists
  to prevent. Being an ordinary message contract, the failure reply is covered by the verification checks for
  free.
- **Mapping to a failure reply asserts that the failure is expected and final.** Unexpected exceptions must
  still escape the handler so that Rebus can retry and dead-letter them. Mapping everything into failure
  replies discards retries — the swallowing problem in better clothes.

## 8. Target frameworks

`net8.0;net9.0;net10.0`.

`netstandard2.0` is **not** targeted. That target is the only reason `System.Threading.Channels` would be a
NuGet dependency; on `net6.0` and later it is in the shared framework. Dropping it gives zero package
references while keeping `Channel<T>`.

The development machine has **only** the .NET 10 SDK and runtime (`10.0.302` / `10.0.10`) — a project
targeting `net9.0` compiles but cannot launch. Therefore, per the project owner's explicit decision:

- **`src/` libraries** multi-target `net8.0;net9.0;net10.0`. The 8 and 9 targets are **compile-only**, built
  against targeting packs. They are never executed; their purpose is to prove that the public surface stays
  within the .NET 8 baseline.
- **Test and benchmark projects** target `net10.0` only, because that is the only runtime present.

## 9. The reference carrier

Two mechanisms, one lookup order.

```csharp
sealed class ReferenceTransportMessage : TransportMessage
{
    public object MessageInstance { get; }
}

static class MessageReferenceTable   // ConditionalWeakTable<byte[], object>
{
    public static void Register(byte[] sentinel, object instance);
    public static bool TryResolve(byte[] sentinel, out object instance);
}
```

The subclass alone would be the whole mechanism if the pipeline never rebuilt a `TransportMessage` — but it
does, at the dead-letter and deferral boundaries
([Appendix A.2](#a2-the-transportmessage-subclass-is-lost-at-the-dead-letter-boundary)), while passing the
**same `Body` array** through ([A.3](#a3-but-the-body-array-survives-every-reconstruction-that-matters)). So
object identity rides on the `Body` array where the subclass cannot.

Every reference-carried message gets a **freshly allocated 1-byte sentinel array** as its `Body`, registered
in the weak table against the message instance. One byte rather than zero, because `Array.Empty<byte>()` is a
shared singleton and every message's table key must be a distinct object.

Resolution order in the serializer:

1. `transportMessage as ReferenceTransportMessage` → `MessageInstance`. The common path; a type check, no
   table access.
2. `MessageReferenceTable.TryResolve(transportMessage.Body, out var instance)`. The reconstructed path —
   dead-letter, deferral, auditing, auto-forward-on-exception.
3. Otherwise throw a diagnostic naming the message type and the likely cause ([§12](#12-unsupported-configurations)).

Recovering the instance from `transactionContext.Items["stepContext"]` instead of a side table was
investigated and does not work — see [Appendix A.4](#a4-recovering-the-instance-via-the-transaction-context-does-not-work).

### 9.1 Why `ConditionalWeakTable` and not `ConcurrentDictionary<long, object>`

Considered and rejected, on failure mode rather than on speed.

Reads are equivalent — both lock-free, order 10 ns, against a dispatch of order 1000 ns, and only on the
fallback path.

On **writes** `ConcurrentDictionary` is genuinely faster: striped locks (~4×`ProcessorCount` buckets) versus
`ConditionalWeakTable.Add`'s single table-wide lock, whose resize additionally scans for dead entries under
that lock. This is a real, accepted cost, on the hot path, one write per message.

It is accepted because `ConcurrentDictionary` is a strong GC root. Every entry keeps its message alive until
explicitly removed — on ack, nack, retry, dead-letter and shutdown. Any missed path is an unbounded leak, and
a large long-lived gen2 dictionary referencing young objects inflates card-marking and gen2 scan cost on
every collection. That is a latent, global, uptime-dependent failure that no unit test detects.
`ConditionalWeakTable` holds its key weakly, so entry lifetime is exactly `Body`-array lifetime,
automatically, with no cleanup code to write and therefore none to forget.

**Decision: `ConditionalWeakTable`, unsharded, and no benchmark arm comparing the two.** If write contention
ever proves to matter, sharding across N tables keyed by
`RuntimeHelpers.GetHashCode(sentinel) & (N-1)` sits entirely behind the `Register`/`TryResolve` API — a
reversible, ~15-line change.

An earlier variant — a `ConcurrentDictionary<long, object>` plus an 8-byte handle stored in `Body` — is
rejected for the same reason in stronger form: it requires cleanup on ack, nack, retry and DLQ, and every
unhandled path is a leak.

## 10. The queue

`InProcNetwork` holds a `ConcurrentDictionary<string, Channel<TransportMessage>>`, one channel per queue
name, plus the subscriber map.

```csharp
Channel.CreateUnbounded<TransportMessage>(new UnboundedChannelOptions
{
    SingleReader = false,               // N workers drain one queue
    SingleWriter = false,               // any module may send to it
    AllowSynchronousContinuations = false
});
```

**Why `Channel<T>` at all.** Rebus's `InMemNetwork` sits on a `ConcurrentQueue` and workers poll it with a
backoff ladder costing 100–250 ms on a cold queue
([Appendix A.6](#a6-the-polling-tax-is-100250-ms)). `ChannelReader.WaitToReadAsync(cancellationToken)` waits
asynchronously instead, giving lower latency and lower CPU. This win is **independent of serialization**, and
possibly larger than it — which is why the benchmark separates the two.

**Unbounded**, deliberately. Rebus's own `InMemNetwork` is unbounded; `bus.Send` effectively never fails
in-proc; and a bounded channel would let a handler that sends to a queue its own worker pool drains deadlock
awaiting capacity it can only free by returning.

**`AllowSynchronousContinuations = false` is a correctness requirement, not tuning.** At its `true` default
the thread calling `Writer.TryWrite` runs a parked reader's continuation inline, so `bus.Send` would
synchronously execute deserialization, handler resolution and the handler itself on the caller's stack. That
converts the transport from asynchronous handoff into inline invocation — the MediatR semantic
[§7.1](#71-where-the-mediatr-analogy-stops) explicitly disclaims — and does so only in-proc, so a handler
exception would surface at the call site instead of going to retry and DLQ. It is exactly the
silent-until-extraction-day divergence the project exists to eliminate.

**Why not `ConcurrentQueue<T>` directly**, as `InMemNetwork` uses: it has no await-for-an-item operation, so
it cannot support the `Blocking` receive mode and forfeits the wake-up win. `Channel.CreateUnbounded` is
implemented on a `ConcurrentQueue<T>` plus a parked-reader list, so the storage and its throughput
characteristics are identical; the channel adds only the signalling. Hand-rolling that signalling with
`SemaphoreSlim` is rejected: `SemaphoreSlim.Release()` completes async waiters synchronously, reintroducing
the inline-execution hazard above, and cancellation races risk lost wake-ups.

**Nack reorders.** Channels offer no peek-and-requeue, so a nacked message is written back to the tail.
`InMemTransport.cs:53` behaves identically, so this matches existing Rebus in-proc behaviour, but it differs
from a broker that redelivers in place. Documented, not fixed.

**Queue depth** for `ITransportInspector` comes from an `Interlocked` counter maintained by `InProcNetwork`,
not from `ChannelReader<T>.Count`, to avoid a per-target-framework behavioural split on one diagnostic
property.

**No static default network.** The `InProcNetwork` instance is constructed by the host and passed explicitly
to each module's configurer. A static singleton would make two independent test fixtures silently share a
network.

## 11. The transport and the serializer

`InProcTransport : AbstractRebusTransport, IInitializable, ITransportInspector, ISubscriptionStorage`,
mirroring `InMemTransport`'s surface.

- `SendOutgoingMessages` writes each `TransportMessage` to the destination channel unchanged.
- `Receive` reads per the configured mode, and registers `context.OnNack` to write the message back to the
  tail.
- `CreateQueue` / `Initialize` create the input queue.
- Subscription storage is centralized, delegating to `InProcNetwork`'s subscriber map.

Deriving from `AbstractRebusTransport` is what gives ambient-transaction deferral for free with no risk to
the reference ([Appendix A.7](#a7-abstractrebustransport-preserves-the-message-reference)).

### 11.1 Receive modes

```csharp
public enum InProcReceiveMode { Blocking, Polling }

public class InProcTransportOptions
{
    public InProcReceiveMode ReceiveMode { get; set; } = InProcReceiveMode.Blocking;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(100);
}
```

`Blocking` parks on `WaitToReadAsync(token)`. `Polling` links the token to a `PollingInterval` timeout and
returns `null` on expiry, dropping Rebus back into its normal backoff ladder. One branch in `Receive`,
switchable per bus without touching handler code. Parking is safe for the worker loop — see
[Appendix A.1](#a1-the-worker-loop-tolerates-a-blocking-receive).

### 11.2 `ReferenceSerializer`

- `Serialize(Message)` → a `ReferenceTransportMessage` carrying `message.Body`, with a fresh sentinel
  registered in `MessageReferenceTable`. Headers are populated as an ordinary serializer would, including
  `Headers.Type` via the configured `IMessageTypeNameConvention`, so that anything reading headers — routing,
  logging, the error queue — behaves normally.
- `Deserialize(TransportMessage)` → resolves per [§9](#9-the-reference-carrier) and returns
  `new Message(headers, instance)`.

The transport and the serializer are **independent**. The transport carries any `TransportMessage`, so
pairing `InProcTransport` with `System.Text.Json` is valid and is what the benchmark's middle arm uses.

### 11.3 Configuration

```csharp
.Transport(t => t.UseInProcTransport(network, "orders"))
.Transport(t => t.UseInProcTransport(network, "orders", o => o.ReceiveMode = InProcReceiveMode.Polling))
.Transport(t => t.UseInProcTransportAsOneWayClient(network))

// Explicit serialization: opt out of the default registration first.
.Transport(t => t.UseInProcTransport(network, "orders", registerReferenceSerializer: false))
.Serialization(s => s.UseReferenceSerializer())
```

Mirrors `InMemTransportConfigurationExtensions`, including registering the network as `ISubscriptionStorage`
by default so pub/sub needs no extra configuration, and calling `OneWayClientBackdoor.ConfigureOneWayClient`
for the one-way overload.

**Why the `registerReferenceSerializer` flag exists.** A transport extension has no way to register a
*losable* default: `PossiblyRegisterDefault` is private to `RebusConfigurer`, and the only primitive
available is `Injectionist.Register`, which **throws on a duplicate primary registration**. A
`ReferenceSerializer` registration and an explicit `.Serialization(...)` call therefore cannot both be
present — the second fails loudly rather than losing gracefully. The `bool registerReferenceSerializer =
true` parameter mirrors Rebus's own `registerSubscriptionStorage` precedent on the InMem extension, and
callers who want to configure serialization explicitly opt out first.

This is proven, not asserted: `ConfigurationTests.RegisteringTheSerializerTwiceFailsLoudly` demonstrates the
`Injectionist` throwing on the duplicate. The flag has an independent second justification — benchmark arm 2
(`InProcJson`, [§15](#15-benchmark)) needs the InProc transport *with* the stock JSON serializer, which is
only expressible through it.

## 12. Unsupported configurations

Encryption, compression, and the databus claim-check steps all construct
`new TransportMessage(headers, aDifferentBody)`, discarding the sentinel and defeating both carrier
mechanisms. All three are meaningless for in-proc traffic.

`ReferenceSerializer.Deserialize` throws `InProcReferenceLostException` when both lookups fail, naming the
message type and listing those three features as the likely cause. Failing loudly at the point of loss is
preferred to a null reference surfacing inside a handler.

## 13. Contract verification

`Rebus.InProcCors.Verification`, a separate package. Not part of the transport — an ordinary check that runs
in the test suite and, by default, at host startup.

Two properties are checked, and they are independent of each other:

1. **Round-trip serializability** — the property that makes extraction safe. Always checked.
2. **Deep immutability** — the property that makes pass-by-reference safe. Checked by default, and switchable
   off per module ([§13.7](#137-the-escape-hatch-all-or-nothing-per-module)).

### 13.1 Why serializability is verified at runtime rather than analyzed statically

A static analyzer produces false positives, and false positives destroy trust. Serializability is statically
undecidable in precisely the places that matter: `object`- and interface-typed properties, polymorphism, open
generics, and cycles that arise from data rather than from types. Running the real serializer over a real
instance decides the question by execution instead.

### 13.2 Why contracts must be deeply immutable

Passing by reference means several handlers can hold the same instance. That is a hazard only if someone can
mutate it. **If message contracts are deeply immutable, aliasing is harmless by construction** — which is the
guarantee the actor model has always relied on, and the same guarantee Orleans buys by copying instead.

So immutability is required of every message contract, and it is verified rather than trusted.

The requirement is Orleans' standard, adopted verbatim: **bitwise immutability, not logical immutability.**
The object graph reachable from a message is not modified at all — not "modified only in thread-safe ways".
And it is the dual-sided commitment Orleans describes: neither sender nor handler mutates the instance, ever.

The alternatives were all *runtime* mitigations, and each was rejected on cost:

- a `Faithful` mode that round-trips so the handler receives a copy — reintroduces the serialization tax this
  project exists to remove, and requires the mode flag the transport deliberately does not have;
- source-generated deep cloning — Orleans' approach, and viable, but Orleans pays that cost because it is a
  general-purpose framework that cannot know what its users will do. A known system optimising a known set of
  contracts does not have to;
- immutability by convention alone — unverified, therefore not a guarantee.

Checking immutability *over the contracts* dominates the others: zero runtime cost, no copy, no mode flag,
and a real check instead of discipline.

### 13.3 Registration and startup behaviour

Each module registers verification on **its own** `IServiceCollection`:

```csharp
services.AddRebusInProcContractVerification();

services.AddRebusInProcContractVerification(o =>
{
    o.VerifyOnStartup = true;         // default: true, in every environment
    o.VerifyImmutability = true;      // default: true — see §13.7
    o.Serializer = mySerializer;      // default: SystemTextJsonContractSerializer
    o.InstanceSource = mySource;      // default: none — see §13.6
});
```

This registers `MessageContractVerifier` as a singleton, so a test resolves it from the module's provider and
calls `Verify()` with no duplicate wiring — test and application agree on what is checked because they read
the same registration.

Per-module registration verifies exactly the right set. Every message type has its handler in exactly one
module by construction, so each module polices its own contracts and the host gathers nothing centrally.

`MessageContractVerifier.ForHandlersIn(params IServiceCollection[])` is the underlying primitive, so the
verifier is testable without a host. An overload taking options first —
`ForHandlersIn(MessageContractVerificationOptions, params IServiceCollection[])` — exists because `params`
occupies the last slot and a module that changed its options must be able to express them from a test.

`VerifyOnStartup` adds an `IHostedService` that runs `Verify()` at boot and, reading `IHostEnvironment`:

- **outside `Production`** — throws, failing local startup and the build;
- **in `Production`** — logs the report at error level and continues.

The asymmetry is deliberate. Boot-time failure is the fastest possible feedback and catches the developer who
never wrote the test; but a violation reaching production was already in a build that passed CI, and a
reflection check must never be the cause of an outage. `VerifyOnStartup` is `true` in **every** environment —
the environment decides throw-versus-log, so a violation in Production is logged rather than invisible.

### 13.4 Type discovery

Walk each `IServiceCollection`'s registered `ServiceType`s, keep closed `IHandleMessages<T>` constructions,
project out `T`. That is ground truth — no naming convention and no list to maintain, and a message with no
handler is dead code anyway. `IServiceCollection` rather than a built `IServiceProvider`, because a provider
cannot enumerate its own registrations and only the type shape is needed.

Enumeration is **lazy**, performed inside `Verify()`, not when `AddRebusInProcContractVerification` is
called. `IServiceCollection` is a live `IList<ServiceDescriptor>`, so capturing the instance and reading it
later makes registration order irrelevant. Eager enumeration would see only the handlers registered above the
call and silently verify a subset — worse than not verifying.

**Source-generated discovery: considered and rejected.** A source generator cannot see DI registrations. They
are runtime behaviour expressed in lambdas and assembly scans; `AddRebusHandlersFromAssemblyOf<X>()` resolves
to nothing statically. A generator would have to substitute a different rule — every type in the compilation
implementing `IHandleMessages<T>` — which is a **different set**: it includes handlers that exist but were
never registered, and can miss handlers registered from referenced assemblies. That is the same class of
approximation as a `*Command`/`*Event` naming convention. It also saves nothing: discovery is a list scan
over `ServiceDescriptor`s, while the cost is the serialize round-trip and the graph walk, neither of which a
generator eliminates.

### 13.5 Check 1 — round-trip byte idempotence

`s1 = serialize(x)`, `s2 = serialize(deserialize(s1))`, compare `s1.Body` and `s2.Body` as bytes, using the
same `ISerializer` the extracted service will use.

Comparing serialized output rather than object graphs is deliberate: a recursive graph comparer produces
false positives on collection ordering, `DateTime` precision and floating-point precision — exactly the
failure mode being avoided. Comparing two `byte[]` values has none of those problems and requires no comparer
to be written.

**The package ships its own serializer.** Rebus's `SystemTextJsonSerializer` is `internal` to the Rebus
assembly and cannot be constructed from outside it, so `Rebus.InProcCors.Verification` owns a ~50-line public
`SystemTextJsonContractSerializer` instead. (Rebus's `SimpleAssemblyQualifiedMessageTypeNameConvention` is
internal for the same reason, so the package carries an equivalent convention as a nested private class.)
Owning it is the better default regardless: the byte output of this serializer is precisely what the check
compares, so it should be pinned and explicit rather than inherited from whatever Rebus does this release.

**Guarantee boundary**, stated explicitly, because a guarantee without a stated boundary is not a guarantee.

Caught:

- a type the serializer cannot handle at all (it throws);
- silent data loss **on the deserialization side** — a private setter, an interface-typed property
  deserialized to its base type. In each case `s2` differs from `s1`.

Not caught:

- members the serializer **never saw** — absent from `s1`, therefore also absent from `s2`. A public writable
  **field** is the case in practice: `System.Text.Json` does not serialize fields unless `IncludeFields` is
  set. Check 2 flags it as an immutability violation regardless, so the case is covered — by the other check;
- cases that depend on **runtime values** rather than types: an `object Payload`, a polymorphic collection.
  Reflection enumerates types; it cannot guess what will end up inside them in production;
- **aliasing** — not a property this check speaks to at all. That is Check 2's job.

A corollary worth stating: **`init`-only properties round-trip correctly.** `init` is a compile-time-only
restriction — the setter is an ordinary public setter in IL, distinguished solely by the `IsExternalInit`
modreq — so `System.Text.Json` populates it happily. An `init`-only property with no matching constructor
parameter is *not* a defect, and the check correctly does not flag one.

### 13.6 Check 2 — deep immutability

Reflection over the reachable graph:

- every public property is get-only or `init`-only;
- no public writable fields;
- no member typed as a mutable collection — `List<>`, `Dictionary<>`, `T[]`, `ICollection<>`, `IList<>`;
  `IReadOnlyList<>`, `IReadOnlyDictionary<>` and `ImmutableArray<>` are accepted;
- recursion is transitive, with a visited set for cycles;
- `string`, primitives, `Guid`, `DateTime`, `DateTimeOffset`, `decimal`, `TimeSpan`, `Uri` and enums
  terminate the walk; `Nullable<T>` is unwrapped.

Detecting `init` has no direct reflection API. An `init` accessor is a setter whose return parameter carries
a required custom modifier for `System.Runtime.CompilerServices.IsExternalInit`:

```csharp
property.SetMethod.ReturnParameter
    .GetRequiredCustomModifiers()
    .Contains(typeof(IsExternalInit))
```

Without this every `record` reads as mutable and the check is worthless.

**What this does not cover:**

- **Mutable state reachable from an immutable message** — an immutable record holding a reference to a shared
  mutable service or cache. The check follows the message's own graph; it cannot know that some referenced
  object is a live handle. Keep contracts made of data.
- **Mutation by reflection**, which nothing short of a runtime copy prevents.
- **Interface- or `object`-typed members**, where the declared type may be immutable and the runtime instance
  not. The same runtime-versus-declared-type wall Check 1 hits, for the same reason.

**Instance construction.** Instances are built by `DefaultMessageInstanceFactory`: the greediest public
constructor, filled with **deterministic, type-derived, non-default values**, recursively. Non-default values
are load-bearing — filled with defaults (`null`, `0`, `""`), a property the serializer silently drops is
invisible: default in `s1`, default after deserialization, default in `s2`, bytes equal, check green.

**The constructor alone is not enough.** A deserializer *runs the constructor*, so every value the
constructor establishes is re-established on the way back; nothing a constructor sets can ever be observed as
lost. Only state set **outside** the constructor can be. The factory therefore also fills every writable
property the constructor did not cover — including `init`-only and **private** setters, which is exactly the
state space code inside the contract's own assembly can put an instance into. With that, a private setter
(which `System.Text.Json` cannot restore) produces genuinely different bytes and is caught.

No AutoFixture dependency: determinism matters more than variety, because a flaky serialization test gets
deleted. `IMessageInstanceSource` allows a per-type override for contracts the factory cannot construct; a
contract that can be constructed by neither is reported as a `Construction` violation rather than skipped.

### 13.7 The escape hatch: all-or-nothing, per module

```csharp
services.AddRebusInProcContractVerification(o => o.VerifyImmutability = false);
```

**The only escape hatch is this switch, and it is deliberately coarse.** It exists for one situation: a
legacy module with hundreds of plain-DTO contracts that will never be made immutable, where the round-trip
check is still worth having. Such a module keeps the check that guards extractability and drops the one it
cannot satisfy.

Three properties make this defensible:

- **There is no per-type or per-member exemption.** An earlier design had `[ImmutabilityExempt(reason)]` on a
  type or member, carrying a mandatory written justification. It was removed. A per-member hatch invites
  death by a thousand exemptions: each one is individually reasonable, the aggregate is a contract set nobody
  can reason about, and the guarantee degrades continuously rather than visibly. Either a module's contracts
  are all immutable, or nobody checks — a binary a reviewer can actually hold in their head.
- **It does not switch off the round-trip check.** There is no `VerifyRoundTrip` flag. A verifier that checks
  nothing should not be registered at all.
- **The report says so.** `VerificationReport.ImmutabilityVerified` is false, `Describe()` renders
  `Verified 143 message contract(s) (round trip only; immutability check disabled).`, and the startup hosted
  service logs the same at information level on every boot. A quiet report from a module that checked half as
  much must never read like a clean bill of health — which was the one genuinely good property of the
  exemption attribute it replaces, preserved at module granularity.

**Turning the check off does not make the aliasing hazard go away.** It records that a module accepts it. The
honest fix, where a contract genuinely cannot be immutable, is still to send a projection of it that can be.

## 14. Test suite

`Rebus.InProcCors.Tests`, xUnit. Beyond ordinary delivery and pub/sub, the tests that earn their place prove
the findings in [Appendix A](#appendix-a-findings-from-the-rebus-sources):

1. The handler receives the **same instance** the caller sent (`ReferenceEquals`).
2. A throwing handler retries and lands in the error queue, and **the message is still readable there** — the
   weak-table fallback, i.e. the case where the subclass is provably gone.
3. A deferred message survives the timeout-manager round trip with its reference intact.
4. `Rebus.Async` `SendRequest` returns the identical reply instance.
5. Two `InProcNetwork` instances do not observe each other's traffic.
6. A handler resolved from another module's container cannot see the caller's registrations.
7. `bus.Send` returns before the handler runs, and a handler exception does not surface at the call site.
8. Every one of the above passes under **both** `ReceiveMode`s.
9. Sentinel arrays are reference-distinct, and a collected message's table entry does not retain it.
10. `InProcReferenceLostException` is thrown, with a useful message, when both carrier mechanisms miss.

`Rebus.InProcCors.Verification.Tests` covers the verification package: types with a private setter, a dropped
member, a mutable `List<>` member, a public writable field and a cycle each produce the expected report.
Plus:

- handlers registered **after** `AddRebusInProcContractVerification()` are still discovered, proving
  enumeration is lazy;
- the startup hosted service throws under a `Development` `IHostEnvironment` and logs-and-continues under
  `Production`;
- with `VerifyImmutability = false`, a mutable contract passes, **a contract that also fails the round trip
  still fails**, the report and the startup log both state that immutability was not checked.

The second of those is the one that matters: a flag that switches off "the immutability check" is one
careless `if` away from switching off the whole loop body, and on a legacy codebase — the only place the flag
is used — a silently empty report is exactly what a working implementation would also produce.

## 15. Benchmark

`Rebus.InProcCors.Benchmarks`, BenchmarkDotNet. Rebus `InMem` is the only comparison — Wolverine is out of
scope.

Three arms, separating the two wins:

| Arm | Transport | Serializer | Isolates |
|---|---|---|---|
| 1 — `InMemJson` | `InMem` | `System.Text.Json` | baseline |
| 2 — `InProcJson` | `InProcTransport` | `System.Text.Json` | the polling → `Channel<T>` win |
| 3 — `InProcReference` | `InProcTransport` | `ReferenceSerializer` | + the serialization win |

`ReceiveMode` is a parameter across arms 2 and 3, which is what tells us whether `Blocking` was the right
default.

**A bursty/idle latency scenario is mandatory, not optional.** The backoff ladder resets on every successful
receive ([A.6](#a6-the-polling-tax-is-100250-ms)), so a saturated throughput benchmark shows the `Channel<T>`
win as approximately zero and misses the larger of the two effects. The scenario sends a single message after
a deliberate idle period and measures time to handler entry.

### 15.1 Results — 2026-08-01

**These are `--job short` smoke numbers** (1 launch, 3 warmup, 3 iterations), not a full run: the `Error`
column is frequently larger than the `Mean`, so treat the small differences as noise and only the
order-of-magnitude differences as real.

- Machine: Windows 11 (10.0.26200), X64 RyuJIT AVX-512
- .NET SDK 10.0.302, runtime 10.0.10; BenchmarkDotNet v0.14.0

```bash
dotnet run --project benchmarks/Rebus.InProcCors.Benchmarks -c Release -- --filter '*BurstyLatency*' --job short
dotnet run --project benchmarks/Rebus.InProcCors.Benchmarks -c Release -- --filter '*Throughput*'    --job short
```

Drop `--job short` for publishable numbers.

**Bursty latency — time to handler entry after a 500 ms idle period.** This is the scenario that decides the
default.

| Arm | Mode | Median | Allocated |
|-----|------|-------:|----------:|
| InMemJson | Blocking | 127,461 µs | 61.13 KB |
| InMemJson | Polling | 143,370 µs | 61.13 KB |
| **InProcJson** | **Blocking** | **497 µs** | 45.14 KB |
| InProcJson | Polling | 158,669 µs | 31.77 KB |
| **InProcReference** | **Blocking** | **400 µs** | 19.11 KB |
| InProcReference | Polling | 159,972 µs | 30.85 KB |

**`Blocking` is decisively the right default — by roughly two and a half orders of magnitude.** `InProcJson`
blocking lands at ~497 µs against ~127 ms for InMem, while `InProcJson` under *polling* lands at ~159 ms,
i.e. no better than InMem. The blocking path parks on `ChannelReader.WaitToReadAsync` and is woken by the
write; the polling path and InMem both wait out Rebus's backoff ladder, which has climbed to its top rung
during the idle period. **The transport choice is irrelevant under polling** — what buys the latency is never
sleeping in the first place.

The `Allocated` column separates the arms in the expected direction: `InProcReference` blocking allocates
19.11 KB against InMem's 61.13 KB, since the message body is never serialized.

**Throughput — 10,000 `SendLocal` calls, per-message cost.**

| Arm | Mode | Mean | Allocated |
|-----|------|-----:|----------:|
| InMemJson | Blocking | 40.10 µs | 18.27 KB |
| InMemJson | Polling | 38.75 µs | 18.27 KB |
| InProcJson | Blocking | 36.09 µs | 18.01 KB |
| InProcJson | Polling | 35.82 µs | 18.12 KB |
| InProcReference | Blocking | 34.74 µs | 17.78 KB |
| InProcReference | Polling | 34.71 µs | 17.77 KB |

**The `Channel<T>` win is approximately zero here, exactly as predicted**, and this is the result that
justifies the bursty scenario existing at all. `DefaultBackoffStrategy.Reset()` runs on every successful
receive, so under saturation the ladder never climbs and there is nothing for the channel to improve on — the
two receive modes are within noise of each other in every arm.

The ~13% spread from `InMemJson` to `InProcReference` is at the edge of what this smoke run can resolve (the
`Error` column reaches ±88 µs). It should not be quoted as a result without a full run.

`Mode` is meaningless for the `InMemJson` arm — it configures the InMem transport either way — so those two
rows are a repeated measurement of the same configuration. Their spread (127 ms vs 143 ms bursty; 40.10 µs vs
38.75 µs throughput) is a useful read on this run's noise floor.

## 16. Open questions

A live list — the design is not closed.

- **A Roslyn analyzer for deep immutability.** [§6](#6-scope) rejects a static analyzer for *serializability*,
  but that reasoning does not transfer: **deep immutability of a closed graph of concrete types is largely
  statically decidable** — whether a property is `init`-only, whether a field is writable, whether a member
  is typed `List<T>` are facts about types that Roslyn knows exactly. The undecidable residue is `object`-
  and interface-typed members, which [§13.6](#136-check-2--deep-immutability) already lists as out of scope
  for the reflection check too. An analyzer would be strictly better on immutability, reporting at the
  offending line at compile time rather than as a test failure naming a type. Deferred only on cost: a
  separate package with its own Roslyn version matrix and test harness, against a reflection check of roughly
  300 lines that covers both checks today.
- **Concurrency and bulkheading** — the fourth Waldo difference, and the one with no coverage. In-proc every
  module's handlers share one process and one thread pool, so a slow or blocking handler in one module
  starves another module's workers; after extraction they are isolated by definition. This degrades only
  under load, so no functional test will surface it. Do we bulkhead in-proc (per-module worker counts,
  separate schedulers) to keep the shapes comparable, or accept it and measure it?
- **Durability** — a module that wants durable in-proc delivery **must** serialize, the same wall Wolverine's
  `DurableLocalQueue` runs into. Do we allow a mixed mode, with some queues by reference and some durable, or
  does durability rule this design out entirely?
- **Where the line falls** between an expected failure mapped to a failure reply
  ([§7.4](#74-failure-as-part-of-the-reply-contract)) and an unexpected exception left to escape. Drawn
  wrongly in one direction it silently discards retries; in the other it turns routine domain rejections into
  dead letters. A per-system judgement, not enforced by the library.
- **`SendRequest` chain discipline** — free in-proc, N sequential network round trips after extraction. The
  mitigation is a review rule, not a mechanism, but it should be named before the first chain appears.
- **A symmetry test against a real broker** — one asserting that a throwing handler produces the same
  observable outcome (retry count, error queue arrival, headers) on this transport and on a real broker. It
  would convert the claim of identical exception semantics from an argument into a check. Out of scope for
  this cut.
- **Remote handler cancellation** — has its own document,
  [`2026-08-09-remote-cancellation-handoff.md`](2026-08-09-remote-cancellation-handoff.md), including
  verified assembly facts and one open decision.

---

## Appendix A. Findings from the Rebus sources

All references are to `rebus-org/Rebus` at `master`, read directly rather than from documentation. These
findings drove the design.

### A.1 The worker loop tolerates a blocking `Receive`

`Rebus/Workers/ThreadPoolBased/ThreadPoolWorker.cs:57` runs
`while (!token.IsCancellationRequested) TryReceiveNextMessage(token)` on a dedicated thread, and
`TryReceiveNextMessage` (line 84) **starts `TryAsyncReceive` without awaiting it**. A `Receive` that parks
does not stall the loop. The loop issues further receives until `ParallelOperationsManager.TryBegin()` is
exhausted, then falls into `_backoffStrategy.Wait(token)`.

Consequence: parking on the queue is safe. The cost is that idle receives occupy parallelism slots and hold
open `TransactionContextWithOwningBus` instances.

### A.2 The `TransportMessage` subclass is lost at the dead-letter boundary

`Rebus/Retry/PoisonQueues/DeadletterQueueErrorHandler.cs:49` calls `transportMessage.Clone()`.
`Rebus/Bus/MessageExtensions.cs:129` implements that as
`new TransportMessage(message.Headers.Clone(), message.Body)`. The subclass, and with it `MessageInstance`,
is discarded before the message reaches the error queue.

### A.3 …but the `Body` array survives every reconstruction that matters

`Clone()` builds a new header dictionary and passes **the same `byte[]` instance** through. The same holds
for the deferral path: `Rebus/Timeouts/DueMessage.cs:51` is `new TransportMessage(Headers, Body)`.

So object identity can ride on the `Body` array even where the subclass cannot. This is the basis of the
fallback mechanism in [§9](#9-the-reference-carrier).

### A.4 Recovering the instance via the transaction context does not work

`Rebus/Pipeline/IncomingStepContext.cs:20` stores the step context in `transactionContext.Items["stepContext"]`,
which in principle lets `ITransport.Send` reach back to the original incoming `TransportMessage` and recover
the instance at zero per-message cost. It is not viable:

- `Rebus/Retry/Simple/RetryStrategySettings.cs:42` defaults `ErrorHandlerMode` to `Immediately`, which makes
  `DeadletterQueueErrorHandler` open a **fresh** `RebusTransactionScope` and pass that instead — no
  `stepContext` item.
- `HandleDeferredMessagesStep.TimerElapsed` runs on a background task with no incoming context at all.

A side table is therefore genuinely required.

### A.5 `Rebus.Async` is transparent to this transport

`Rebus.Async/Internals/ReplyHandlerStep.cs` calls `context.Load<Message>()` and never touches
`TransportMessage`. It operates entirely after deserialization. The reply travels by reference and
`result.Body is TReply` yields the identical instance.

### A.6 The polling tax is 100–250 ms

`Rebus/Config/RebusConfigurer.cs:247` sets the default backoff ladder to `100 ms` for the first ten seconds
of idleness, then `250 ms` indefinitely. A transport whose `Receive` returns `null` on an empty queue incurs
this as wake-up latency.

`DefaultBackoffStrategy.Reset()` is called on every successful receive (`ThreadPoolWorker.cs:118`), so **the
ladder never climbs under saturation**. The penalty lands only on the first message after an idle period —
which is the dominant traffic shape of a modular monolith, and is invisible to a throughput-only benchmark.

### A.7 `AbstractRebusTransport` preserves the message reference

`Rebus/Transport/AbstractRebusTransport.cs` enqueues the caller's `TransportMessage` instance unchanged into
a per-transaction `ConcurrentQueue<OutgoingTransportMessage>` and flushes on commit. Deriving from it gives
ambient-transaction deferral for free with no risk to the reference.

## Appendix B. Rebus APIs that do not exist

Recorded because the design and its implementation plan described Rebus APIs by name, and several of those
names are wrong. Each was found by dumping the real **Rebus 8.9.2** assembly surface by reflection — not
guessed at — and each is reflected in shipped, passing tests. This appendix exists so the same wrong names
are not reintroduced.

**A nack is not reachable through `RebusTransactionScope`.** Disposing a scope without completing does *not*
roll it back and fire `OnNack`. `Dispose()` fires **only** `OnDisposed`, and `CompleteAsync()` forces
`SetResult(commit: true, ack: true)`, so it cannot express a nack either. Rebus's worker drives a nack via
`SetResult(false, false)` followed by `TransactionContext.Complete()` — and **`Complete()` is not on
`ITransactionContext`**; it is on the internal concrete type. `RebusTransactionScope` is a *sending*
abstraction, while ack/nack is a *receiving* concept, and the two do not meet.

`ANackedMessageGoesBackOntoTheQueue` therefore uses a hand-rolled `CallbackCapturingTransactionContext`
implementing the six public interface members, capturing and firing the callback the way the worker would.
That was chosen over reflecting into the internal `Complete()`, which would couple the test suite to a detail
that can change in any patch release.

**`BuiltinHandlerActivator.UseServiceProvider` does not exist.** It ships in the separate
`Rebus.ServiceProvider` package, which this solution does not reference. The activator offers only
`Handle<T>` and `Register<THandler>`. The `TestModule` fixture links each module's container explicitly via
`activator.Register<THandler>(() => …)` resolving from that module's own provider.

**`OptionsConfigurer.UseInMemoryTimeoutManager` does not exist.** Only
`UseExternalTimeoutManager(StandardConfigurer<ITimeoutManager>, string)` exists; an in-memory timeout manager
is **Rebus's default**. The deferral test simply drops the call.

**`Rebus.Async` namespaces.** `EnableSynchronousRequestReply` is in **`Rebus.Config`**, not
`Rebus.Async.Config`. `SendRequest` is in namespace `Rebus`.

**`Defer` requires routing; `DeferLocal` does not.** `Defer` resolves its destination through the router, so
the deliberately routing-free `TestModule` fixture threw `Cannot get destination for message of type …`. The
deferral test uses `DeferLocal`, which exercises the identical timeout-manager path. A real deployment
configures routing and never hits this.

**Confirmed correct as written** (checked against the assembly, no change needed):
`RetryStrategy(maxDeliveryAttempts:, errorQueueName:)` in `Rebus.Retry.Simple`; `Headers.ErrorDetails` =
`rbs2-error-details`; `Headers.Type` = `rbs2-msg-type`; `IMessageTypeNameConvention` is public with exactly
`GetTypeName(Type)` / `GetType(string)`.

**Rebus's `SystemTextJsonSerializer` and `SimpleAssemblyQualifiedMessageTypeNameConvention` are `internal`**
and cannot be constructed from outside the Rebus assembly — see
[§13.5](#135-check-1--round-trip-byte-idempotence).

## Sources

- `Rebus/Messages/TransportMessage.cs`, `Rebus/Serialization/ISerializer.cs`, `Rebus/Transport/ITransport.cs`,
  `Rebus/Transport/InMem/InMemTransport.cs` — https://github.com/rebus-org/Rebus
- `src/Wolverine/Transports/Local/BufferedLocalQueue.cs`, `DurableLocalQueue.cs` —
  https://github.com/JasperFx/wolverine
- https://github.com/rebus-org/Rebus.Async — synchronous request/reply over Rebus, including the author's
  caveats
- https://www.nuget.org/packages/Rebus.Async — 10.0.0, `netstandard2.0`, requires Rebus >= 8.0.1
- Waldo, Wyant, Wollrath, Kendall — *A Note on Distributed Computing*, Sun Microsystems Laboratories, 1994 —
  https://scholar.harvard.edu/files/waldo/files/waldo-94.pdf
- Akka — *Location Transparency*, on optimizing remote-to-local rather than generalizing local-to-remote —
  https://doc.akka.io/libraries/akka-core/current/general/remoting.html
- Orleans — *Serialization of immutable types*, deep copy by default and the `[Immutable]` opt-out —
  https://learn.microsoft.com/en-us/dotnet/orleans/host/configuration-guide/serialization-immutability
- https://wolverinefx.net/guide/messaging/transports/local.html
- https://github.com/rebus-org/Rebus/issues/599 — mookid8000 confirms the in-mem transport as an in-process
  bus
