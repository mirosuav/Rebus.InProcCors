# Handoff — Rebus.InProcCors implementation

**Last updated:** 2026-08-01 · **Branch:** `main` · **Status: the implementation plan is complete.**

`Docs/plans/2026-07-31-rebus-inproccors-implementation-plan.md` — **all 13 tasks done**, each committed
separately. It implements `Docs/2026-07-31-rebus-inproccors-design.md`, which has been brought back into
agreement with what was actually built (design §3, §8, §10, and the new §15).

Verify the state:

```bash
dotnet build     # net8.0, net9.0, net10.0 — warnings-as-errors, XML docs required. Expect 0 warnings.
dotnet test      # expect: Passed: 106, Failed: 0  (61 transport + 45 verification)
```

---

# 1. What exists

Two packages plus a benchmark executable.

**`src/Rebus.InProcCors`** — the transport. Zero NuGet dependencies beyond `Rebus` itself.

```
InProcReceiveMode.cs / InProcTransportOptions.cs    Blocking | Polling, PollingInterval
InProcQueue.cs                        internal      unbounded Channel + Interlocked depth counter
InProcNetwork.cs                      public        queue map + subscriber map, host-owned
InProcTransport.cs                    public        AbstractRebusTransport + IInitializable
                                                    + ITransportInspector + ISubscriptionStorage
ReferenceTransportMessage.cs          internal      TransportMessage subclass carrying the instance
MessageReferenceTable.cs              internal      ConditionalWeakTable<byte[], object>
ReferenceSerializer.cs                public        ISerializer, two-carrier resolution
InProcReferenceLostException.cs       public        thrown when both carriers miss
Config/InProcTransportConfigurationExtensions.cs    UseInProcTransport, …AsOneWayClient
Config/ReferenceSerializerConfigurationExtensions.cs  UseReferenceSerializer
```

**`src/Rebus.InProcCors.Verification`** — contract verification. Deliberately **independent of the
transport**: it references `Rebus` and `Microsoft.Extensions.*` only, never `Rebus.InProcCors`. Contract
discipline is worth having in any Rebus app, and coupling it to the transport would make adopting the checks
mean adopting the transport.

```
ImmutabilityExemptAttribute.cs        public        [ImmutabilityExempt(reason)], reason mandatory
VerificationReport.cs                 public        VerificationCheck, …Violation, …Exemption, report
ImmutabilityChecker.cs                public        deep-immutability reflection walk (Check 2)
IMessageInstanceSource.cs             public        per-type construction override
DefaultMessageInstanceFactory.cs      public        deterministic non-default instance construction
SystemTextJsonContractSerializer.cs   public        ships here because Rebus's is internal (C2)
RoundTripChecker.cs                   public        serialize/deserialize/serialize byte compare (Check 1)
HandlerMessageTypeDiscovery.cs        public        IHandleMessages<T> → T, lazy over IServiceCollection
MessageContractVerificationOptions.cs public
MessageContractVerificationException.cs public       carries the report
MessageContractVerifier.cs            public        orchestrator; ForHandlersIn / Verify / VerifyAndThrow
ContractVerificationHostedService.cs  internal      throws outside Production, logs in Production
ServiceCollectionExtensions.cs        public        AddRebusInProcContractVerification
```

**`benchmarks/Rebus.InProcCors.Benchmarks`** — three arms (InMem+Json, InProc+Json, InProc+Reference) ×
two receive modes, throughput and bursty latency. Results in `Docs/2026-08-01-benchmark-results.md`.

Test classes: `tests/Rebus.InProcCors.Tests/` (`ScaffoldTests`, `MessageReferenceTableTests`,
`InProcNetworkTests`, `ReferenceSerializerTests`, `InProcTransportTests`, `ConfigurationTests`,
`ReferenceDeliveryTests`, `FailurePathTests`, `IsolationTests`, `TestModule`) and
`tests/Rebus.InProcCors.Verification.Tests/` (`ImmutabilityCheckerTests`,
`DefaultMessageInstanceFactoryTests`, `RoundTripCheckerTests`, `MessageContractVerifierTests`,
`RegistrationTests`).

---

# 2. What is strongly confirmed

Claims backed by passing tests rather than by design reasoning. If a future change breaks one of these, the
design premise itself is broken.

**The core promise holds.** `TheHandlerReceivesTheSameInstanceTheCallerSent` asserts `Assert.Same` between
the object sent and the object a handler receives, through a fully configured real bus, under **both**
receive modes.

**The two-carrier fallback is load-bearing, not defensive.**
`AThrowingHandlerRetriesAndTheMessageIsStillReadableInTheErrorQueue` asserts the dead-lettered message
`IsNotType<ReferenceTransportMessage>` — Rebus's `Clone()` genuinely destroys the subclass — and *then* that
`Assert.Same(sent, recovered.Body)` still holds. The weak side table alone recovers identity there.

**Deferral survives the timeout manager.** `ADeferredMessageKeepsItsReferenceAcrossTheTimeoutManager`
confirms `DueMessage` rebuilds the transport message and that only the weak table can answer.

**`Rebus.Async` is transparent to this transport.** `SendRequestReturnsTheIdenticalReplyInstance` returns the
identical reply object.

**Dispatch is asynchronous handoff, not inline invocation.** A handler exception does not surface at the
`Send` call site. This is where the MediatR analogy stops.

**Module DI isolation and network isolation are real**, and **weak keying releases correctly** (collecting
the sentinel array releases the table entry).

**`Blocking` is the right default, by roughly two and a half orders of magnitude.** Measured, not argued:
~0.4–0.5 ms to handler entry after a 500 ms idle period, against ~127 ms for InMem and ~159 ms for InProc
under polling. Under saturation the three arms are within ~13% — which is exactly why the bursty scenario
is the one that decides. See `Docs/2026-08-01-benchmark-results.md` (smoke-run numbers; the note says so).

---

# 3. Corrections made to the plan and design, and why

The plan's self-review admitted none of its code had ever been compiled. Everything below was verified by
dumping the real Rebus 8.9.2 assembly surface by reflection, and is now recorded in **design §15** (C4–C7)
and in **§3, §8, §10** (C1–C3). Nothing here is left only in this file.

- **C1** `PossiblyRegisterDefault` is private to `RebusConfigurer`; `Injectionist.Register` throws on a
  duplicate primary registration → `registerReferenceSerializer` flag. Design §8.
- **C2** Rebus's `SystemTextJsonSerializer` and `SimpleAssemblyQualifiedMessageTypeNameConvention` are
  internal → the verification package ships its own. Design §10.
- **C3** Target frameworks: libraries `net8.0;net9.0;net10.0` (8 and 9 compile-only), tests and benchmarks
  `net10.0`. Design §3.
- **C4** A nack is not reachable through `RebusTransactionScope`. Design §15.
- **C5** `BuiltinHandlerActivator.UseServiceProvider` does not exist. Design §15.
- **C6** `OptionsConfigurer.UseInMemoryTimeoutManager` does not exist. Design §15.
- **C7** `Rebus.Async` namespaces. Design §15.
- **`Defer` needs routing, `DeferLocal` does not.** Design §15.

**One correction found while implementing Task 9, and it is the most consequential of them.** The plan's
`DefaultMessageInstanceFactory` filled only constructor parameters. That makes Check 1 **vacuously green**:
a deserializer runs the constructor, so every value the constructor establishes is re-established on the way
back, and nothing a constructor sets can ever be observed as lost. The factory now also fills writable
properties the constructor did not cover, including `init`-only and private setters. Relatedly, the plan's
`DroppedInitProperty` fixture demonstrated nothing — `init` is a compile-time-only restriction, so
`System.Text.Json` restores init-only properties correctly. It was replaced by an unbound constructor
parameter, plus a test pinning that init-only round-trips cleanly. All of this is written up in design §10
under "Instance construction", including the known limitation (public writable fields are invisible to a
byte comparison, and are caught by Check 2 instead).

**Two deliberate strengthenings beyond the plan.** `IsolationTests.AHandlerCannotSeeTheCallersRegistrations`
as written only compared two `IServiceProvider` instances and never resolved a handler, so it could not
demonstrate the isolation claim it was named for; it was rewritten with a real handler that reports the
secret *it* can see. And `VerifyOnStartup` is `true` unconditionally, with the environment deciding
throw-versus-log inside the hosted service, so a violation in Production is logged rather than invisible.

---

# 4. What is left

Nothing in the plan. Genuinely open items, in rough order of how load-bearing they are:

1. **Concurrency and bulkheading** — the fourth Waldo difference, still with no coverage. In-proc every
   module's handlers share one process and one thread pool, so a slow handler in one module starves
   another's workers. Degrades only under load, so no functional test will surface it. Design §14.
2. **A full benchmark run.** The recorded numbers are `--job short` smoke numbers whose `Error` column
   frequently exceeds the `Mean`. The order-of-magnitude conclusion is safe; the ~13% throughput spread is
   not quotable without a real run.
3. **Durability**, **`SendRequest` chain discipline**, **the symmetry test against a real broker**, and
   **where the expected/unexpected failure line falls** — the four remaining unchecked README questions.
4. **Remote handler cancellation** — a transport-agnostic `CancelRemoteMessage(MessageId)` protocol, explored
   2026-08-09 and left at an open decision. See `Docs/2026-08-09-remote-cancellation-handoff.md`. Note that
   it turns item 1 above from a throughput question into a correctness one.
5. **A Roslyn analyzer for deep immutability**, deferred but explicitly not ruled out. Design §14 argues
   the README's blanket rejection of static analysis does not apply to immutability.

Nothing has been published to NuGet. `Version` is `0.1.0` in `Directory.Build.props`.

---

## Working agreements

- **Commit after every task**, conventional-commit prefixes (`feat:`, `test:`, `chore:`, `docs:`).
- **Work directly on `main`** — the user's explicit choice over a branch or worktree.
- **Strict TDD:** write the failing test, *run it and see it fail*, then implement.
- Treat the plan's code blocks as **intent, not compiling source**. When a Rebus API is uncertain, dump the
  real assembly surface by reflection before writing against it — that is how C4–C7 were found, and it is
  far faster than iterating through build errors.
- **CS1998 is an error here** (`TreatWarningsAsErrors`), so an `async` lambda with no `await` fails the
  build. **CS1574 too**: a `<see cref="Foo"/>` naming a type that does not exist yet breaks the build.
- Zero NuGet dependencies in `Rebus.InProcCors` beyond `Rebus` itself. Central Package Management is on —
  versions go in `Directory.Packages.props`, never inline in a `.csproj`.
