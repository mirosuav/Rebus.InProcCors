# Handoff — Rebus.InProcCors implementation

**Last updated:** 2026-08-01 · **Branch:** `main`, pushed, in sync with `origin/main` · **HEAD:** `855aec3`

Executing `Docs/plans/2026-07-31-rebus-inproccors-implementation-plan.md`, which implements
`Docs/2026-07-31-rebus-inproccors-design.md`. **Resume at Task 8.**

Verify the starting state before touching anything:

```bash
dotnet build     # net8.0, net9.0, net10.0 — warnings-as-errors, XML docs required
dotnet test      # expect: Passed: 61, Failed: 0
```

---

# 1. What has been done

**Tasks 1–7 of 13, committed and pushed. 61 tests pass, 0 fail** — confirmed over three consecutive
runs, so the timing-sensitive integration tests are not flaky.

| Task | Subject | Commit | Tests |
|------|---------|--------|-------|
| 1 | Solution scaffold, central package versions | `86cf098` | 1 |
| 2 | Reference carrier | `8136e5f` | 6 |
| 3 | Queue / network | `7cddc63` | 10 |
| 4 | Reference serializer | `373505c` | 9 |
| 5 | Transport | `1188fb7` | 11 |
| 6 | Configuration extensions | `73fcec0` | 7 |
| 7 | Integration tests | `377dc6b` | 17 |
| — | Package URL fix (not a plan task) | `855aec3` | — |

Shipped in `src/Rebus.InProcCors/`:

```
InProcReceiveMode.cs                              Blocking | Polling
InProcTransportOptions.cs                         ReceiveMode, PollingInterval
InProcQueue.cs                        internal    unbounded Channel + Interlocked depth counter
InProcNetwork.cs                      public      queue map + subscriber map, host-owned
InProcTransport.cs                    public      AbstractRebusTransport + IInitializable
                                                  + ITransportInspector + ISubscriptionStorage
ReferenceTransportMessage.cs          internal    TransportMessage subclass carrying the instance
MessageReferenceTable.cs              internal    ConditionalWeakTable<byte[], object>
ReferenceSerializer.cs                public      ISerializer, two-carrier resolution
InProcReferenceLostException.cs       public      thrown when both carriers miss
Config/InProcTransportConfigurationExtensions.cs  UseInProcTransport, …AsOneWayClient
Config/ReferenceSerializerConfigurationExtensions.cs  UseReferenceSerializer
```

Test classes in `tests/Rebus.InProcCors.Tests/`: `ScaffoldTests`, `MessageReferenceTableTests`,
`InProcNetworkTests`, `ReferenceSerializerTests`, `InProcTransportTests`, `ConfigurationTests`,
`ReferenceDeliveryTests`, `FailurePathTests`, `IsolationTests`, plus the `TestModule` fixture.

---

# 2. What is strongly confirmed

Claims now backed by passing tests rather than by design reasoning. These are the load-bearing
results — if a future change breaks one of these, the design premise itself is broken.

**The core promise holds.** `TheHandlerReceivesTheSameInstanceTheCallerSent` asserts `Assert.Same`
between the object sent and the object a handler receives, through a fully configured real bus, under
**both** receive modes.

**The two-carrier fallback is load-bearing, not defensive.**
`AThrowingHandlerRetriesAndTheMessageIsStillReadableInTheErrorQueue` asserts the dead-lettered message
`IsNotType<ReferenceTransportMessage>` — Rebus's `Clone()` genuinely destroys the subclass — and *then*
that `Assert.Same(sent, recovered.Body)` still holds. The weak side table alone recovers identity
there. Without it, dead-lettered messages would be unreadable.

**Deferral survives the timeout manager.** `ADeferredMessageKeepsItsReferenceAcrossTheTimeoutManager`
confirms `DueMessage` rebuilds the transport message and that only the weak table can answer.

**`Rebus.Async` is transparent to this transport.** `SendRequestReturnsTheIdenticalReplyInstance`
returns the identical reply object — `ReplyHandlerStep` operates on the deserialized `Message` and
never touches `TransportMessage`.

**Dispatch is asynchronous handoff, not inline invocation.** A handler exception does not surface at
the `Send` call site. This is where the MediatR analogy stops, and it is what
`AllowSynchronousContinuations = false` protects.

**Module DI isolation is real.** A handler resolved from its own module's container sees
`"orders-secret"` and never the caller's `"caller-secret"`.

**Network isolation is real.** Two `InProcNetwork` instances do not observe each other's traffic —
the justification for having no static default instance.

**Weak keying releases correctly.** Collecting the sentinel array releases the table entry, so there
is no unbounded growth and no cleanup path to forget.

**Correction C1 from the plan is proven, not merely asserted.** `RegisteringTheSerializerTwiceFailsLoudly`
demonstrates that Rebus's `Injectionist` throws on a duplicate primary registration — which is exactly
why `registerReferenceSerializer` had to be a flag rather than an overridable default.

**Confirmed correct in the plan as written** (checked against the assembly, no change needed):
`RetryStrategy(maxDeliveryAttempts:, errorQueueName:)` in `Rebus.Retry.Simple`; `Headers.ErrorDetails`
= `rbs2-error-details`.

---

# 3. What changes were justified and introduced

The plan's self-review admits none of its code was ever compiled. Seven assumptions proved wrong.
Each was verified by dumping the real Rebus 8.9.2 assembly surface by reflection — not guessed at.
**Task 13 must fold all of these into the design document.**

### C3 (superseded) — target frameworks

*Plan said:* libraries `net8.0;net9.0`, tests `net9.0`, because the machine had no .NET 8 runtime.
*Reality:* this machine now has **only** the .NET 10 SDK and runtime (`10.0.302` / `10.0.10`), so a
`net9.0` test project compiles but cannot launch.
*Introduced,* per the user's explicit decision: `src/` libraries multi-target
**`net8.0;net9.0;net10.0`** (8 and 9 compile-only, via targeting packs, proving the floor still
builds); test and benchmark projects target **`net10.0`**.

### C4 — nack is not reachable through `RebusTransactionScope`

*Plan said:* "disposing a scope without completing rolls it back, which fires `OnNack`."
*Reality:* `Dispose()` fires **only** `OnDisposed`; `CompleteAsync()` forces
`SetResult(commit: true, ack: true)` so it cannot express a nack either. Rebus's worker drives a nack
via `SetResult(false, false)` then `TransactionContext.Complete()` — and **`Complete()` is not on
`ITransactionContext`**, it is on the internal concrete type. `RebusTransactionScope` is a *sending*
abstraction; ack/nack is a *receiving* concept.
*Introduced:* `ANackedMessageGoesBackOntoTheQueue` uses a hand-rolled
`CallbackCapturingTransactionContext` implementing the six public interface members, capturing and
firing the callback the way the worker would. Chosen over reflecting into the internal `Complete()`,
which would couple the suite to a detail that can change in any patch release. **The transport code
was correct; only the test's model of Rebus was wrong.**

### C5 — `BuiltinHandlerActivator.UseServiceProvider` does not exist

*Reality:* it ships in the separate `Rebus.ServiceProvider` package, which this solution does not
reference. The activator offers only `Handle<T>` and `Register<THandler>`.
*Introduced:* `TestModule` links each module's container explicitly via
`activator.Register<THandler>(() => …)` resolving from that module's own provider — which is the point
of the fixture either way.

### C6 — `OptionsConfigurer.UseInMemoryTimeoutManager` does not exist

*Reality:* only `UseExternalTimeoutManager(StandardConfigurer<ITimeoutManager>, string)` exists; an
in-memory timeout manager is **Rebus's default**.
*Introduced:* the deferral test simply drops the call — no configuration needed.

### C7 — `Rebus.Async` namespaces

*Reality:* `EnableSynchronousRequestReply` is in **`Rebus.Config`**, not `Rebus.Async.Config`.
`SendRequest` is in namespace `Rebus`, already in scope from `Rebus.InProcCors.Tests`.

### `Defer` → `DeferLocal` in the test fixture

*Reality:* `Defer` resolves its destination through the router; `SendLocal` does not. The routing-free
`TestModule` therefore threw `Cannot get destination for message of type …`.
*Introduced:* `DeferLocal`, which exercises the identical timeout-manager path. A real deployment
configures routing and never hits this.

### One deliberate strengthening beyond the plan

`IsolationTests.AHandlerCannotSeeTheCallersRegistrations` as written only compared two
`IServiceProvider` instances — it never resolved a handler, so it could not actually demonstrate the
isolation claim it was named for. Since `UseServiceProvider` was unavailable anyway (C5), it was
rewritten with a real `IHandleMessages<PlaceOrder>` that reports the secret *it* can see. This is the
difference from MediatR that the README rests on, now actually tested.

### Repository hygiene (not a plan task)

`PackageProjectUrl` and `RepositoryUrl` said `miedziarek`, but `origin` is
`github.com/mirosuav/Rebus.InProcCors`. Corrected in `855aec3` and verified in the emitted `.nuspec`.
This mattered because `RepositoryUrl` feeds SourceLink, which pairs it with the commit hash — a wrong
value points consumers' source-stepping at a repo where that commit does not exist.

### Recurring mechanical fixes — expect these in Tasks 8–13 too

- **CS1998 is an error here.** `TreatWarningsAsErrors` is on, so the plan's `async` lambdas with no
  `await` (and one `async Task` test with no `await`) fail the build. Convert to explicit
  `Task.CompletedTask` returns, or make the method non-`async`.
- **CS1574 on forward references.** `GenerateDocumentationFile` + warnings-as-errors means a
  `<see cref="Foo"/>` naming a type that does not exist yet breaks the build. Write `<c>Foo</c>` and
  restore the `see cref` in the task that introduces the type.
- **Missing usings.** `using Rebus.Routing.TypeBased;` is needed wherever `.TypeBased()` is called.
- The test project declares `<Using Include="Xunit" />`, so plan test files compile without a per-file
  `using Xunit;`. Do the same in the new verification test project.
- The plan's predicted test counts are reliable except Task 5, where it says 12 and the real count is
  11 (9 facts + one 2-case theory). No test is missing.

---

# 4. What exactly of the plan is left to implement

Six tasks. Tasks 8–11 build `src/Rebus.InProcCors.Verification/`, a package **independent of the
transport** — it references `Rebus` and `Microsoft.Extensions.*` only, never `Rebus.InProcCors`.

### Task 8 — Verification project scaffold, report model, immutability check *(plan line 2311)*
Create `src/Rebus.InProcCors.Verification/{Rebus.InProcCors.Verification.csproj,
ImmutabilityExemptAttribute.cs, VerificationReport.cs, ImmutabilityChecker.cs}` and the test project
`tests/Rebus.InProcCors.Verification.Tests/`, with `ImmutabilityCheckerTests.cs`.
`ImmutabilityChecker` is a deep-immutability reflection walk. 8 steps, TDD order.
→ commit `feat:`

### Task 9 — Instance factory, contract serializer, round-trip check *(plan line 2975)*
Create `IMessageInstanceSource.cs`, `DefaultMessageInstanceFactory.cs` (deterministic non-default
instance construction), `SystemTextJsonContractSerializer.cs`, `RoundTripChecker.cs`; tests
`DefaultMessageInstanceFactoryTests.cs`, `RoundTripCheckerTests.cs`.
**Plan correction C2 is real and still applies:** `Rebus.Serialization.Json.SystemTextJsonSerializer`
is `internal`, so this package must ship its own ~50-line public serializer. 8 steps.
→ commit `feat:`

### Task 10 — Discovery and the verifier *(plan line 3644)*
Create `HandlerMessageTypeDiscovery.cs` (`IHandleMessages<T>` → `T`, lazy over `IServiceCollection`),
`MessageContractVerificationOptions.cs`, `MessageContractVerificationException.cs`,
`MessageContractVerifier.cs` (orchestrator); tests `MessageContractVerifierTests.cs` — including
`AHandlerRegisteredAfterTheVerifierIsConstructedIsStillDiscovered`. 8 steps.
→ commit `feat:`

### Task 11 — Registration and the startup check *(plan line 4076)*
Create `ContractVerificationHostedService.cs` and `ServiceCollectionExtensions.cs`
(`AddRebusInProcContractVerification`); tests `RegistrationTests.cs`.
**Note the plan's deliberate deviation from the design here:** `VerifyOnStartup` is `true`
unconditionally, and the *environment* decides throw-versus-log inside the hosted service — so a
violation is logged in Production rather than being invisible. Tests
`TheStartupCheckThrowsOutsideProduction`, `TheStartupCheckLogsAndContinuesInProduction`. 6 steps.
→ commit `feat:`

### Task 12 — Benchmarks *(plan line 4390)*
Create `benchmarks/Rebus.InProcCors.Benchmarks/{csproj, BusArm.cs, ThroughputBenchmark.cs,
BurstyLatencyBenchmark.cs, Program.cs}`. Three arms: InProc+Reference, InProc+System.Text.Json,
InMemory+Json. **Arm 2 needs `UseInProcTransport(..., registerReferenceSerializer: false)`** — this is
the independent justification for that flag (C1). Target `net10.0` per C3. 7 steps.
→ commit `feat:` or `chore:`

### Task 13 — Documentation *(plan line 4654)*
Modify `Docs/2026-07-31-rebus-inproccors-design.md` and `README.md`:
1. Correct design §8 (C1 — `PossiblyRegisterDefault` unavailable)
2. Correct design §10 (C2 — `SystemTextJsonSerializer` internal)
3. Correct design §3 (target frameworks — **use C3-as-superseded above, not the plan's wording**)
4. **Additionally record C4–C7 and the `Defer`/`DeferLocal` finding from section 3 of this document** —
   otherwise the design keeps describing Rebus APIs that do not exist
5. Add the usage section to `README.md`
6. Update the README's open-questions list: tick what design §13 resolves, leave genuinely open items
   (concurrency/bulkheading, durability, `SendRequest` chain discipline, the symmetry test, the
   expected/unexpected failure line) unchecked
7. Full `dotnet build` + `dotnet test`, report the actual count
→ commit `docs:`

---

## Working agreements

- **Commit after every task**, conventional-commit prefixes (`feat:`, `test:`, `chore:`, `docs:`).
- **Work directly on `main`** — the user's explicit choice over a branch or worktree.
- **Strict TDD:** write the failing test, *run it and see it fail*, then implement.
- Treat the plan's code blocks as **intent, not compiling source**. When a Rebus API is uncertain, dump
  the real assembly surface by reflection before writing against it — that is how C4–C7 were found, and
  it is far faster than iterating through build errors.
- Zero NuGet dependencies in `Rebus.InProcCors` beyond `Rebus` itself; `System.Threading.Channels` is in
  the shared framework. Central Package Management is on — versions go in `Directory.Packages.props`,
  never inline in a `.csproj`.
