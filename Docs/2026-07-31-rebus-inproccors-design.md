# Rebus.InProcCors — implementation design

Date: 2026-07-31
Status: approved, ready for planning
Supersedes: nothing. Refines `README.md`, and corrects it where the Rebus sources contradict it.

## 1. Purpose

Implement the library specified in `README.md`: a Rebus transport that passes messages **by reference** within a process, so a modular monolith can use the bus as its module boundary without paying a serialization tax, and can extract a module into a service by changing transport configuration alone.

This document records the decisions taken before implementation, the evidence behind them, and the parts of `README.md` that turned out to be wrong.

## 2. Findings from the Rebus sources

All references are to `rebus-org/Rebus` at `master`, read directly rather than from documentation. These findings drove the design and resolve four of the README's open questions.

### 2.1 The worker loop tolerates a blocking `Receive`

`Rebus/Workers/ThreadPoolBased/ThreadPoolWorker.cs:57` runs `while (!token.IsCancellationRequested) TryReceiveNextMessage(token)` on a dedicated thread, and `TryReceiveNextMessage` (line 84) **starts `TryAsyncReceive` without awaiting it**. A `Receive` that parks does not stall the loop. The loop issues further receives until `ParallelOperationsManager.TryBegin()` is exhausted, then falls into `_backoffStrategy.Wait(token)`.

Consequence: parking on the queue is safe. The cost is that idle receives occupy parallelism slots and hold open `TransactionContextWithOwningBus` instances.

### 2.2 The `TransportMessage` subclass is lost at the dead-letter boundary

`Rebus/Retry/PoisonQueues/DeadletterQueueErrorHandler.cs:49` calls `transportMessage.Clone()`. `Rebus/Bus/MessageExtensions.cs:129` implements that as `new TransportMessage(message.Headers.Clone(), message.Body)`.

**The README's claim that "the error queue holds a live reference to the message object" is false.** The subclass, and with it `MessageInstance`, is discarded before the message reaches the error queue.

### 2.3 …but the `Body` array survives every reconstruction that matters

`Clone()` builds a new header dictionary and passes **the same `byte[]` instance** through. The same holds for the deferral path: `Rebus/Timeouts/DueMessage.cs:51` is `new TransportMessage(Headers, Body)`.

So object identity can ride on the `Body` array even where the subclass cannot. This is the basis of the fallback mechanism in §4.

### 2.4 Recovering the instance via the transaction context does not work

`Rebus/Pipeline/IncomingStepContext.cs:20` stores the step context in `transactionContext.Items["stepContext"]`, which in principle lets `ITransport.Send` reach back to the original incoming `TransportMessage` and recover the instance at zero per-message cost.

It is not viable:
- `Rebus/Retry/Simple/RetryStrategySettings.cs:42` defaults `ErrorHandlerMode` to `Immediately`, which makes `DeadletterQueueErrorHandler` open a **fresh** `RebusTransactionScope` and pass that instead — no `stepContext` item.
- `HandleDeferredMessagesStep.TimerElapsed` runs on a background task with no incoming context at all.

A side table is therefore genuinely required.

### 2.5 `Rebus.Async` is transparent to this transport

`Rebus.Async/Internals/ReplyHandlerStep.cs` calls `context.Load<Message>()` and never touches `TransportMessage`. It operates entirely after deserialization. The reply travels by reference and `result.Body is TReply` yields the identical instance. README open question 3 is answered: no risk.

### 2.6 The polling tax is 100–250 ms

`Rebus/Config/RebusConfigurer.cs:247` sets the default backoff ladder to `100 ms` for the first ten seconds of idleness, then `250 ms` indefinitely. A transport whose `Receive` returns `null` on an empty queue incurs this as wake-up latency.

`DefaultBackoffStrategy.Reset()` is called on every successful receive (`ThreadPoolWorker.cs:118`), so **the ladder never climbs under saturation**. The penalty lands only on the first message after an idle period — which is the dominant traffic shape of a modular monolith, and is invisible to a throughput-only benchmark.

### 2.7 `AbstractRebusTransport` preserves the message reference

`Rebus/Transport/AbstractRebusTransport.cs` enqueues the caller's `TransportMessage` instance unchanged into a per-transaction `ConcurrentQueue<OutgoingTransportMessage>` and flushes on commit. Deriving from it gives ambient-transaction deferral for free with no risk to the reference.

## 3. Target frameworks

`net8.0;net9.0;net10.0`.

`netstandard2.0` is **not** targeted. That target is the only reason `System.Threading.Channels` would be a NuGet dependency; on `net6.0` and later it is in the shared framework. Dropping it gives zero package references while keeping `Channel<T>`.

**Implementation note (superseded).** This section originally recorded that the .NET 10 SDK was missing and that the initial `csproj` would ship `net8.0;net9.0`. That is no longer the situation. The development machine now has **only** the .NET 10 SDK and runtime (`10.0.302` / `10.0.10`) — there is no .NET 8 or .NET 9 runtime, so a project targeting `net9.0` compiles but cannot launch.

What is implemented, per the project owner's explicit decision:

- **`src/` libraries** multi-target `net8.0;net9.0;net10.0`. The 8 and 9 targets are **compile-only**, built against targeting packs. They are never executed; their purpose is to prove that the public surface stays within the .NET 8 baseline.
- **Test and benchmark projects** target `net10.0` only, because that is the only runtime present.

## 4. The reference carrier

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

Every reference-carried message gets a **freshly allocated 1-byte sentinel array** as its `Body`, registered in the weak table against the message instance. One byte rather than zero, because `Array.Empty<byte>()` is a shared singleton and every message's table key must be a distinct object.

Resolution order in the serializer:

1. `transportMessage as ReferenceTransportMessage` → `MessageInstance`. The common path; a type check, no table access.
2. `MessageReferenceTable.TryResolve(transportMessage.Body, out var instance)`. The reconstructed path — dead-letter (§2.2), deferral, auditing, auto-forward-on-exception.
3. Otherwise throw a diagnostic naming the message type and the likely cause (§9).

### Why `ConditionalWeakTable` and not `ConcurrentDictionary<long, object>`

Considered and rejected, on failure mode rather than on speed.

Reads are equivalent — both lock-free, order 10 ns, against a dispatch the README puts at order 1000 ns, and only on the fallback path.

On **writes** `ConcurrentDictionary` is genuinely faster: striped locks (~4×`ProcessorCount` buckets) versus `ConditionalWeakTable.Add`'s single table-wide lock, whose resize additionally scans for dead entries under that lock. This is a real, accepted cost, on the hot path, one write per message.

It is accepted because `ConcurrentDictionary` is a strong GC root. Every entry keeps its message alive until explicitly removed — on ack, nack, retry, dead-letter and shutdown. Any missed path is an unbounded leak, and a large long-lived gen2 dictionary referencing young objects inflates card-marking and gen2 scan cost on every collection. That is a latent, global, uptime-dependent failure that no unit test detects. `ConditionalWeakTable` holds its key weakly, so entry lifetime is exactly `Body`-array lifetime, automatically, with no cleanup code to write and therefore none to forget.

**Decision: `ConditionalWeakTable`, unsharded, and no benchmark arm comparing the two.** If write contention ever proves to matter, sharding across N tables keyed by `RuntimeHelpers.GetHashCode(sentinel) & (N-1)` sits entirely behind the `Register`/`TryResolve` API — a reversible, ~15-line change.

## 5. The queue

`InProcNetwork` holds a `ConcurrentDictionary<string, Channel<TransportMessage>>`, one channel per queue name, plus the subscriber map.

```csharp
Channel.CreateUnbounded<TransportMessage>(new UnboundedChannelOptions
{
    SingleReader = false,               // N workers drain one queue
    SingleWriter = false,               // any module may send to it
    AllowSynchronousContinuations = false
});
```

**Unbounded**, deliberately. Rebus's own `InMemNetwork` is unbounded; the README states `bus.Send` effectively never fails in-proc; and a bounded channel would let a handler that sends to a queue its own worker pool drains deadlock awaiting capacity it can only free by returning.

**`AllowSynchronousContinuations = false` is a correctness requirement, not tuning.** At its `true` default the thread calling `Writer.TryWrite` runs a parked reader's continuation inline, so `bus.Send` would synchronously execute deserialization, handler resolution and the handler itself on the caller's stack. That converts the transport from asynchronous handoff into inline invocation — the MediatR semantic the README explicitly disclaims — and does so only in-proc, so a handler exception would surface at the call site instead of going to retry and DLQ. It is exactly the silent-until-extraction-day divergence the project exists to eliminate.

**Why not `ConcurrentQueue<T>` directly**, as `InMemNetwork` uses: `ConcurrentQueue<T>` has no await-for-an-item operation, so it cannot support the `Blocking` receive mode and forfeits the wake-up win quantified in §2.6. `Channel.CreateUnbounded` is implemented on a `ConcurrentQueue<T>` plus a parked-reader list, so the storage and its throughput characteristics are identical; the channel adds only the signalling. Hand-rolling that signalling with `SemaphoreSlim` is rejected: `SemaphoreSlim.Release()` completes async waiters synchronously, reintroducing the inline-execution hazard above, and cancellation races risk lost wake-ups.

**Nack reorders.** Channels offer no peek-and-requeue, so a nacked message is written back to the tail. `InMemTransport.cs:53` behaves identically, so this matches existing Rebus in-proc behaviour, but it differs from a broker that redelivers in place. Documented, not fixed.

**Queue depth** for `ITransportInspector` comes from an `Interlocked` counter maintained by `InProcNetwork`, not from `ChannelReader<T>.Count`, to avoid a per-target-framework behavioural split on one diagnostic property.

**No static default network.** The `InProcNetwork` instance is constructed by the host and passed explicitly to each module's configurer. A static singleton would make two independent test fixtures silently share a network.

## 6. The transport

`InProcTransport : AbstractRebusTransport, IInitializable, ITransportInspector, ISubscriptionStorage`, mirroring `InMemTransport`'s surface.

- `SendOutgoingMessages` writes each `TransportMessage` to the destination channel unchanged.
- `Receive` reads per the configured mode, and registers `context.OnNack` to write the message back to the tail.
- `CreateQueue` / `Initialize` create the input queue.
- Subscription storage is centralized, delegating to `InProcNetwork`'s subscriber map.

### Receive modes

```csharp
public enum InProcReceiveMode { Blocking, Polling }

public class InProcTransportOptions
{
    public InProcReceiveMode ReceiveMode { get; set; } = InProcReceiveMode.Blocking;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(100);
}
```

`Blocking` parks on `WaitToReadAsync(token)`. `Polling` links the token to a `PollingInterval` timeout and returns `null` on expiry, dropping Rebus back into its normal backoff ladder. One branch in `Receive`, switchable per bus without touching handler code.

## 7. The serializer

`ReferenceSerializer : ISerializer`.

- `Serialize(Message)` → a `ReferenceTransportMessage` carrying `message.Body`, with a fresh sentinel registered in `MessageReferenceTable`. Headers are populated as an ordinary serializer would, including `Headers.Type` via the configured `IMessageTypeNameConvention`, so that anything reading headers — routing, logging, the error queue — behaves normally.
- `Deserialize(TransportMessage)` → resolves per §4 and returns `new Message(headers, instance)`.

The transport and the serializer are **independent**. The transport carries any `TransportMessage`, so pairing `InProcTransport` with `System.Text.Json` is valid and is what the benchmark's middle arm uses.

## 8. Configuration

```csharp
.Transport(t => t.UseInProcTransport(network, "orders"))
.Transport(t => t.UseInProcTransport(network, "orders", o => o.ReceiveMode = InProcReceiveMode.Polling))
.Transport(t => t.UseInProcTransportAsOneWayClient(network))

// Explicit serialization: opt out of the default registration first.
.Transport(t => t.UseInProcTransport(network, "orders", registerReferenceSerializer: false))
.Serialization(s => s.UseReferenceSerializer())
```

Mirrors `InMemTransportConfigurationExtensions`, including registering the network as `ISubscriptionStorage` by default so pub/sub needs no extra configuration, and calling `OneWayClientBackdoor.ConfigureOneWayClient` for the one-way overload.

**Correction C1 — there is no `PossiblyRegisterDefault` to use.** This section previously said `ReferenceSerializer` is registered "via `PossiblyRegisterDefault` semantics — a default, so an explicit `.Serialization(...)` call still wins." That mechanism is not reachable from a transport extension: `PossiblyRegisterDefault` is **private to `RebusConfigurer`**, and the only registration primitive available here is `Injectionist.Register`, which **throws on a duplicate primary registration**. So a `ReferenceSerializer` registration and an explicit `.Serialization(...)` call cannot both be present — the second one fails loudly rather than losing gracefully.

The resolution is a `bool registerReferenceSerializer = true` parameter, mirroring Rebus's own `registerSubscriptionStorage` precedent on the InMem extension. Callers who want to configure serialization explicitly opt out of the default registration first.

This is proven, not merely asserted: `ConfigurationTests.RegisteringTheSerializerTwiceFailsLoudly` demonstrates the `Injectionist` throwing on the duplicate. The flag also has an independent second justification — benchmark arm 2 (`InProcJson`, §12) needs the InProc transport *with* the stock JSON serializer, which is only expressible through it.

## 9. Unsupported configurations

Encryption, compression, and the databus claim-check steps all construct `new TransportMessage(headers, aDifferentBody)`, discarding the sentinel and defeating both carrier mechanisms. All three are meaningless for in-proc traffic.

`ReferenceSerializer.Deserialize` throws `InProcReferenceLostException` when both lookups fail, naming the message type and listing those three features as the likely cause. Failing loudly at the point of loss is preferred to a null reference surfacing inside a handler.

## 10. Verification library

`Rebus.InProcCors.Verification`, a separate package. Not part of the transport.

### Registration

Each module registers verification on **its own** `IServiceCollection`:

```csharp
services.AddRebusInProcContractVerification();

services.AddRebusInProcContractVerification(o =>
{
    o.VerifyOnStartup = true;      // default: true, in every environment
    o.Serializer = mySerializer;   // default: SystemTextJsonContractSerializer
});
```

This registers `MessageContractVerifier` as a singleton, so a test resolves it from the module's provider and calls `Verify()` with no duplicate wiring — test and application agree on what is checked because they read the same registration.

Per-module registration verifies exactly the right set. Every message type has its handler in exactly one module by construction, so each module polices its own contracts and the host gathers nothing centrally.

`MessageContractVerifier.ForHandlersIn(params IServiceCollection[])` remains the underlying primitive, so the verifier is testable without a host.

### Startup behaviour

`VerifyOnStartup` adds an `IHostedService` that runs `Verify()` at boot and, reading `IHostEnvironment`:

- **outside `Production`** — throws, failing local startup and the build;
- **in `Production`** — logs the report at error level and continues.

The asymmetry is deliberate. Boot-time failure is the fastest possible feedback and catches the developer who never wrote the test; but a violation reaching production was already in a build that passed CI, and a reflection check must never be the cause of an outage.

### Type discovery

Walk each `IServiceCollection`'s registered `ServiceType`s, keep closed `IHandleMessages<T>` constructions, project out `T`. Ground truth per the README — no naming convention and no list to maintain. `IServiceCollection` rather than a built `IServiceProvider`, because a provider cannot enumerate its own registrations and only the type shape is needed.

Enumeration is **lazy**, performed inside `Verify()`, not when `AddRebusInProcContractVerification` is called. `IServiceCollection` is a live `IList<ServiceDescriptor>`, so capturing the instance and reading it later makes registration order irrelevant. Eager enumeration would see only the handlers registered above the call and silently verify a subset — worse than not verifying.

### Source-generated discovery: considered and rejected

A source generator cannot see DI registrations. They are runtime behaviour expressed in lambdas and assembly scans; `AddRebusHandlersFromAssemblyOf<X>()` resolves to nothing statically. A generator would have to substitute a different rule — every type in the compilation implementing `IHandleMessages<T>` — which is a **different set**: it includes handlers that exist but were never registered, and can miss handlers registered from referenced assemblies. That is the same class of approximation as the `*Command`/`*Event` naming convention the README rejects.

It also saves nothing. Discovery is a list scan over `ServiceDescriptor`s; the cost of verification is the serialize round-trip and the immutability graph walk, neither of which a generator eliminates.

### Check 1 — round-trip byte idempotence

`s1 = serialize(x)`, `s2 = serialize(deserialize(s1))`, compare `s1.Body` and `s2.Body` as bytes, using the same `ISerializer` the extracted service will use (`SystemTextJsonContractSerializer` by default, injectable).

**Correction C2 — the verification package ships its own serializer.** This section previously named `SystemTextJsonSerializer`, meaning Rebus's own. That type is **`internal` to the Rebus assembly** and cannot be constructed from outside it, so `Rebus.InProcCors.Verification` owns a ~50-line public `SystemTextJsonContractSerializer` instead. (Rebus's `SimpleAssemblyQualifiedMessageTypeNameConvention` is internal for the same reason, so the package carries an equivalent convention as a nested private class.) Owning it is the better default regardless: the byte output of this serializer is precisely what the check compares, so it should be pinned and explicit rather than inherited from whatever Rebus does this release.

**`VerifyOnStartup` is `true` unconditionally.** The startup snippet above originally commented it as "default: true outside Production". It is implemented as `true` in every environment, with the *environment* deciding throw-versus-log inside the hosted service — which is what "Startup behaviour" below specifies, and the better shape: the check still runs in Production, so a violation is logged rather than invisible.

### Check 2 — deep immutability

Reflection over the reachable graph:

- every public property is get-only or `init`-only;
- no public writable fields;
- no member typed as a mutable collection — `List<>`, `Dictionary<>`, `T[]`, `ICollection<>`, `IList<>`; `IReadOnlyList<>`, `IReadOnlyDictionary<>` and `ImmutableArray<>` are accepted;
- recursion is transitive, with a visited set for cycles;
- `string`, primitives, `Guid`, `DateTime`, `DateTimeOffset`, `decimal`, `TimeSpan`, `Uri` and enums terminate the walk; `Nullable<T>` is unwrapped.

Detecting `init` has no direct reflection API. An `init` accessor is a setter whose return parameter carries a required custom modifier for `System.Runtime.CompilerServices.IsExternalInit`:

```csharp
property.SetMethod.ReturnParameter
    .GetRequiredCustomModifiers()
    .Contains(typeof(IsExternalInit))
```

Without this every `record` reads as mutable and the check is worthless.

### Instance construction

Instances are built by `DefaultMessageInstanceFactory`: the greediest public constructor, filled with **deterministic, type-derived, non-default values**, recursively.

Non-default values are load-bearing. Filled with defaults (`null`, `0`, `""`), a property the serializer silently drops is invisible — default in `s1`, default after deserialization, default in `s2`, bytes equal, test green. The check only has teeth when every value differs from the type's default.

No AutoFixture dependency: determinism matters more than variety, because a flaky serialization test gets deleted. `IMessageInstanceSource` allows a per-type override for contracts the factory cannot construct.

**The constructor alone is not enough.** This section originally described construction as ending at the greediest constructor. Filling only constructor parameters makes Check 1 **vacuously green**, for a reason the "non-default values are load-bearing" paragraph above almost states but stops one step short of: a deserializer *runs the constructor*, so every value the constructor establishes is re-established on the way back. Nothing a constructor sets can ever be observed as lost. Only state set **outside** the constructor can be.

The factory therefore also fills every writable property the constructor did not cover — including `init`-only and **private** setters, which is exactly the state space code inside the contract's own assembly can put an instance into. With that, a private setter (which `System.Text.Json` cannot restore) produces genuinely different bytes and is caught.

A corollary worth stating, because the implementation plan assumed the opposite: **`init`-only properties round-trip correctly.** `init` is a compile-time-only restriction — the setter is an ordinary public setter in IL, distinguished solely by the `IsExternalInit` modreq — so `System.Text.Json` populates it happily. An init-only property with no matching constructor parameter is *not* a defect, and the check correctly does not flag one.

**Known limitation.** Byte comparison cannot see a member that is absent from `s1` in the first place. A public writable **field** is the case in practice: `System.Text.Json` does not serialize fields unless `IncludeFields` is set, so the field is missing from both passes and the bytes match. Check 2 flags public writable fields as an immutability violation regardless, so the case is covered — but by the other check, not this one.

### Escape hatch

`[ImmutabilityExempt(string reason)]`, on a type or a member, with a mandatory non-empty reason. Exemptions do not fail the report; they are **listed** in it, so they appear in test output on every run and in code review as an attribute carrying a written justification.

## 11. Test suite

`Rebus.InProcCors.Tests`, xUnit. Beyond ordinary delivery and pub/sub, the tests that earn their place prove the findings above:

1. The handler receives the **same instance** the caller sent (`ReferenceEquals`).
2. A throwing handler retries and lands in the error queue, and **the message is still readable there** — the weak-table fallback, i.e. the case where the subclass is provably gone.
3. A deferred message survives the timeout-manager round trip with its reference intact.
4. `Rebus.Async` `SendRequest` returns the identical reply instance.
5. Two `InProcNetwork` instances do not observe each other's traffic.
6. A handler resolved from another module's container cannot see the caller's registrations.
7. `bus.Send` returns before the handler runs, and a handler exception does not surface at the call site.
8. Every one of the above passes under **both** `ReceiveMode`s.
9. Sentinel arrays are reference-distinct, and a collected message's table entry does not retain it.
10. `InProcReferenceLostException` is thrown, with a useful message, when both carrier mechanisms miss.

The verification library gets its own tests: types with a dropped `init` property, a private setter, a mutable `List<>` member, a cycle, and an exempted member each produce the expected report. Plus:

- handlers registered **after** `AddRebusInProcContractVerification()` are still discovered, proving enumeration is lazy;
- the startup hosted service throws under a `Development` `IHostEnvironment` and logs-and-continues under `Production`.

## 12. Benchmark

`Rebus.InProcCors.Benchmarks`, BenchmarkDotNet. Rebus `InMem` is the only comparison — Wolverine is out of scope.

Three arms, separating the two wins:

| Arm | Transport | Serializer | Isolates |
|---|---|---|---|
| 1 | `InMem` | `System.Text.Json` | baseline |
| 2 | `InProcTransport` | `System.Text.Json` | the polling → `Channel<T>` win |
| 3 | `InProcTransport` | `ReferenceSerializer` | + the serialization win |

Measured: throughput, end-to-end latency, allocations. `ReceiveMode` is a parameter across arms 2 and 3, which is what tells us whether `Blocking` was the right default.

**A bursty/idle latency scenario is mandatory, not optional.** Per §2.6 the backoff ladder resets on every successful receive, so a saturated throughput benchmark will show the `Channel<T>` win as approximately zero and the measurement will miss the larger of the two effects. The scenario must send a single message after a deliberate idle period and measure time to handler entry.

## 13. README open questions resolved by this document

| Question | Resolution |
|---|---|
| Worker loop tolerates blocking `Receive`? | Yes — §2.1. Both modes implemented anyway, §6. |
| Do pipeline steps reconstruct `TransportMessage`? | Yes, at dead-letter and deferral. Subclass alone is insufficient; weak-table fallback added — §2.2, §2.3, §4. |
| Does `Rebus.Async` reply-by-reference work? | Yes, and it never touches `TransportMessage` — §2.5. |
| Serialize on dead-letter? | No. The weak table preserves the reference across `Clone()`, so the single-mode rule holds — §4. |
| Where does the shared network live? | Host-owned instance, injected. No static singleton — §5. |
| Immutability check: hand-rolled or analyzer? Escape hatch? | Hand-rolled reflection now; `[ImmutabilityExempt(reason)]`, reported not silent — §10. Analyzer deferred, not ruled out — §14. |
| Where do test instances come from? | Deterministic non-default factory, `IMessageInstanceSource` override. No AutoFixture — §10. |
| Benchmark shape | Three arms, `InMem` only, mandatory bursty-latency scenario — §12. |

## 14. Still open

Not blocking implementation; recorded so they are not lost.

- **A Roslyn analyzer for deep immutability.** The README rejects "a static Roslyn analyzer for serializability", but that reasoning is specific to serializability: it is undecidable exactly where it matters, because it depends on the runtime contents of `object`- and interface-typed members. **Deep immutability of a closed graph of concrete types is largely statically decidable** — whether a property is `init`-only, whether a field is writable, whether a member is typed `List<T>` are facts about types that Roslyn knows exactly. The undecidable residue is `object`- and interface-typed members, which §10 already lists as out of scope for the reflection check too. An analyzer would therefore be strictly better on immutability, reporting at the offending line at compile time rather than as a test failure naming a type. Deferred only on cost: it is a separate package with its own Roslyn version matrix and test harness, against a reflection check of roughly 300 lines that covers both checks today. **The README's blanket rejection of static analysis should not be read as ruling this out.**
- **Concurrency and bulkheading** — the fourth Waldo difference. In-proc, all modules share one thread pool, so a slow handler in one module starves another's workers. Not addressed; to be measured before deciding whether to bulkhead.
- **Durability** — a module wanting durable in-proc delivery must serialize. Not supported; whether to allow a mixed durable/reference configuration is deferred.
- **`SendRequest` chain discipline** — free in-proc, N sequential round trips after extraction. A review rule, not a mechanism; no tooling planned.
- **Symmetry test against a real broker** — would convert the claim of identical exception semantics into a check. Out of scope for this cut.
- **Where the line falls** between an expected failure mapped to a failure reply and an unexpected exception left to escape. A per-system judgement, documented in the README, not enforced by the library.

## 15. Rebus APIs this document described that do not exist

Corrections C1–C3 are folded into §8, §10 and §3 above. The remainder are recorded here because the design and its implementation plan describe Rebus APIs by name, and several of those names are wrong. Each was found by dumping the real Rebus 8.9.2 assembly surface by reflection — not guessed at — and each is reflected in shipped, passing tests.

**C4 — a nack is not reachable through `RebusTransactionScope`.** The plan asserted that "disposing a scope without completing rolls it back, which fires `OnNack`." It does not. `Dispose()` fires **only** `OnDisposed`, and `CompleteAsync()` forces `SetResult(commit: true, ack: true)`, so it cannot express a nack either. Rebus's worker drives a nack via `SetResult(false, false)` followed by `TransactionContext.Complete()` — and **`Complete()` is not on `ITransactionContext`**; it is on the internal concrete type. `RebusTransactionScope` is a *sending* abstraction, while ack/nack is a *receiving* concept, and the two do not meet.

`ANackedMessageGoesBackOntoTheQueue` therefore uses a hand-rolled `CallbackCapturingTransactionContext` implementing the six public interface members, capturing and firing the callback the way the worker would. That was chosen over reflecting into the internal `Complete()`, which would couple the test suite to a detail that can change in any patch release. **The transport code was correct; only the test's model of Rebus was wrong.**

**C5 — `BuiltinHandlerActivator.UseServiceProvider` does not exist.** It ships in the separate `Rebus.ServiceProvider` package, which this solution does not reference. The activator offers only `Handle<T>` and `Register<THandler>`. The `TestModule` fixture links each module's container explicitly via `activator.Register<THandler>(() => …)` resolving from that module's own provider — which is the point of the fixture either way.

**C6 — `OptionsConfigurer.UseInMemoryTimeoutManager` does not exist.** Only `UseExternalTimeoutManager(StandardConfigurer<ITimeoutManager>, string)` exists; an in-memory timeout manager is **Rebus's default**. The deferral test simply drops the call.

**C7 — `Rebus.Async` namespaces.** `EnableSynchronousRequestReply` is in **`Rebus.Config`**, not `Rebus.Async.Config`. `SendRequest` is in namespace `Rebus`.

**`Defer` requires routing; `DeferLocal` does not.** `Defer` resolves its destination through the router, so the deliberately routing-free `TestModule` fixture threw `Cannot get destination for message of type …`. The deferral test uses `DeferLocal`, which exercises the identical timeout-manager path. A real deployment configures routing and never hits this.

**Confirmed correct as written** (checked against the assembly, no change needed): `RetryStrategy(maxDeliveryAttempts:, errorQueueName:)` in `Rebus.Retry.Simple`; `Headers.ErrorDetails` = `rbs2-error-details`; `Headers.Type` = `rbs2-msg-type`; `IMessageTypeNameConvention` is public with exactly `GetTypeName(Type)` / `GetType(string)`.
