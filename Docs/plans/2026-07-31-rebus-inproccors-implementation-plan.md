# Rebus.InProcCors Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> ## ⚠ EXECUTION STATUS — read `Docs/HANDOFF.md` before continuing
>
> **Tasks 1–7 are DONE, committed and pushed** (HEAD `855aec3`, 61 tests passing). **Resume at Task 8.**
>
> Several code blocks below call Rebus APIs that **do not exist** in 8.9.2 and were corrected during
> implementation — `BuiltinHandlerActivator.UseServiceProvider`, `OptionsConfigurer.UseInMemoryTimeoutManager`,
> the `Rebus.Async.Config` namespace, and the nack-on-scope-dispose premise in Task 5. The target frameworks
> in Global Constraints are also stale (this machine now has only the .NET 10 SDK). All corrections, with
> evidence, are in `Docs/HANDOFF.md` — **Task 13 must fold them into the design document.**
>
> Treat the code blocks below as intent, not as compiling source.

**Goal:** Build a Rebus transport + serializer that pass messages by reference within one process, plus a separate library that verifies message contracts are round-trip serializable and deeply immutable.

**Architecture:** `InProcTransport` derives from `AbstractRebusTransport` and moves `TransportMessage` instances through per-queue unbounded `Channel<T>`s held by a host-owned `InProcNetwork`. `ReferenceSerializer` puts the live message object on a `TransportMessage` subclass and registers it in a `ConditionalWeakTable` keyed by a freshly allocated 1-byte sentinel `Body`, so the reference survives the `Clone()` that the dead-letter and deferral paths perform. `Rebus.InProcCors.Verification` is an independent package that discovers message types from `IHandleMessages<T>` DI registrations and asserts serializability and deep immutability by reflection.

**Tech Stack:** C# 12, .NET 8/9, Rebus 8.9.2, Rebus.Async 10.0.0, `System.Threading.Channels` (in-box), xUnit, `Microsoft.Extensions.DependencyInjection`, BenchmarkDotNet.

**Source of truth:** `Docs/2026-07-31-rebus-inproccors-design.md` (design) and `README.md` (specification). Section references below (§2.1, §4, …) point at the design document.

## Global Constraints

- Library target frameworks: `net8.0;net9.0`. `net10.0` is added to `<TargetFrameworks>` once the .NET 10 SDK is installed (design §3).
- **Test and benchmark projects target `net9.0` only.** Verified on this machine: `dotnet --list-runtimes` reports Microsoft.NETCore.App 6.0.36 and 9.0.18 — there is no .NET 8 runtime, so a `net8.0` test run cannot execute. The `net8.0` *compile* target still works because the targeting pack restores from NuGet.
- **Zero NuGet package references in `Rebus.InProcCors`** beyond `Rebus` itself. `System.Threading.Channels` is in the shared framework from `net6.0` onward; do not add the package (design §3).
- Rebus version floor: `8.9.2`. Rebus.Async: `10.0.0`.
- `LangVersion` = `latest`, `Nullable` = `enable`, `TreatWarningsAsErrors` = `true`, `GenerateDocumentationFile` = `true` on `src/` projects.
- Public API gets XML doc comments — `GenerateDocumentationFile` with warnings-as-errors makes this enforced, not optional.
- Every integration test in Task 8 must run under **both** `InProcReceiveMode.Blocking` and `InProcReceiveMode.Polling` (design §11 item 8). Use xUnit `[Theory]` with `[InlineData(InProcReceiveMode.Blocking)] [InlineData(InProcReceiveMode.Polling)]`.
- **No static default `InProcNetwork`.** The instance is constructed by the host and passed explicitly (design §5).
- `AllowSynchronousContinuations = false` on every channel is a correctness requirement, not tuning (design §5).
- Commit after every task. Conventional-commit prefixes (`feat:`, `test:`, `chore:`, `docs:`).

## Corrections to the design document

Three points in the design do not survive contact with the Rebus 8.9.2 sources or this machine. Each is resolved below and must also be written back into `Docs/2026-07-31-rebus-inproccors-design.md` in Task 13.

**C1 — §8: `PossiblyRegisterDefault` is not available to extension authors.**
`PossiblyRegisterDefault<T>` is a *private method of `RebusConfigurer`* (`Rebus/Config/RebusConfigurer.cs:215-343`), not a member of `StandardConfigurer<T>`. `StandardConfigurer<T>` exposes only `Register`, `Decorate` and `OtherService<TOther>()`. `Injectionist.Register` throws `InvalidOperationException("Attempted to register …, but a primary registration already exists")` when a primary registration is made twice (`Rebus/Injection/Injectionist.cs:108-122`), and `Injectionist.Has<T>()` is not reachable through `StandardConfigurer<T>`.

So `UseInProcTransport` cannot register `ISerializer` in a way that a later `.Serialization(...)` overrides — registering it eagerly makes the combination *throw*.

*Resolution:* mirror Rebus's own precedent for this exact problem. `UseInMemoryTransport` takes `bool registerSubscriptionStorage = true` because it has the same collision risk with `.Subscriptions(...)`. `UseInProcTransport` therefore takes `bool registerReferenceSerializer = true` alongside it, and a standalone `StandardConfigurer<ISerializer>.UseReferenceSerializer()` extension exists for callers who pass `false`, or who want the serializer without the transport. Benchmark arm 2 (`InProcTransport` + `System.Text.Json`) needs `registerReferenceSerializer: false` anyway, so the flag earns its place independently.

**C2 — §10: `Rebus.Serialization.Json.SystemTextJsonSerializer` is internal.**
It is declared `sealed class SystemTextJsonSerializer : ISerializer` with no `public` modifier, so the Verification package cannot use it as its default `ISerializer`.

*Resolution:* `Rebus.InProcCors.Verification` ships its own public `SystemTextJsonContractSerializer : ISerializer`, a ~50-line implementation over `System.Text.Json` that mirrors Rebus's own (`Headers.Type` via `IMessageTypeNameConvention`, `Headers.ContentType`, UTF-8). This is arguably better than depending on a Rebus internal: the verification serializer should be pinned and explicit, since its byte output is what the test compares.

**C3 — §3: no .NET 8 runtime on this machine.**
`dotnet --list-runtimes` reports only 6.0.36 and 9.0.18. Recorded as a Global Constraint above: library multi-targets `net8.0;net9.0`, test and benchmark projects are `net9.0` only.

---

## File Structure

```
Rebus.InProcCors.sln
Directory.Build.props                       shared TFMs, nullable, warnings-as-errors
Directory.Packages.props                    central package version management

src/Rebus.InProcCors/
  Rebus.InProcCors.csproj
  InProcReceiveMode.cs                      enum: Blocking | Polling
  InProcTransportOptions.cs                 ReceiveMode, PollingInterval
  InProcQueue.cs                            internal: one Channel<TransportMessage> + depth counter
  InProcNetwork.cs                          public: queue map + subscriber map, host-owned
  InProcTransport.cs                        AbstractRebusTransport + inspector + subscriptions
  ReferenceTransportMessage.cs              internal: TransportMessage subclass carrying the instance
  MessageReferenceTable.cs                  internal: ConditionalWeakTable<byte[], object>
  ReferenceSerializer.cs                    public ISerializer, two-step resolution
  InProcReferenceLostException.cs           public, thrown when both carriers miss
  Config/InProcTransportConfigurationExtensions.cs
  Config/ReferenceSerializerConfigurationExtensions.cs

src/Rebus.InProcCors.Verification/
  Rebus.InProcCors.Verification.csproj
  ImmutabilityExemptAttribute.cs
  IMessageInstanceSource.cs
  DefaultMessageInstanceFactory.cs          deterministic non-default instance construction
  ImmutabilityChecker.cs                    deep-immutability reflection walk
  RoundTripChecker.cs                       serialize/deserialize/serialize byte comparison
  SystemTextJsonContractSerializer.cs       per correction C2
  VerificationReport.cs                     report, violation, exemption, check enum
  MessageContractVerificationException.cs
  MessageContractVerificationOptions.cs
  HandlerMessageTypeDiscovery.cs            IHandleMessages<T> -> T, lazy over IServiceCollection
  MessageContractVerifier.cs                orchestrator
  ContractVerificationHostedService.cs      startup check, env-dependent severity
  ServiceCollectionExtensions.cs            AddRebusInProcContractVerification

tests/Rebus.InProcCors.Tests/               unit + integration, xUnit
tests/Rebus.InProcCors.Verification.Tests/  xUnit
benchmarks/Rebus.InProcCors.Benchmarks/     BenchmarkDotNet, three arms
```

---

## Task 1: Solution scaffold

**Files:**
- Create: `Directory.Build.props`, `Directory.Packages.props`, `.gitignore`
- Create: `src/Rebus.InProcCors/Rebus.InProcCors.csproj`
- Create: `tests/Rebus.InProcCors.Tests/Rebus.InProcCors.Tests.csproj`
- Create: `tests/Rebus.InProcCors.Tests/ScaffoldTests.cs`
- Create: `Rebus.InProcCors.sln`

**Interfaces:**
- Consumes: nothing.
- Produces: a solution where `dotnet test` runs. Assembly `Rebus.InProcCors` grants `InternalsVisibleTo` to `Rebus.InProcCors.Tests` and `Rebus.InProcCors.Benchmarks`.

- [ ] **Step 1: Create `.gitignore`**

```gitignore
bin/
obj/
.vs/
*.user
BenchmarkDotNet.Artifacts/
TestResults/
artifacts/
```

- [ ] **Step 2: Create `Directory.Build.props`**

```xml
<Project>
  <PropertyGroup>
    <LangVersion>latest</LangVersion>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    <ManagePackageVersionsCentrally>true</ManagePackageVersionsCentrally>
    <Authors>Mirek</Authors>
    <PackageProjectUrl>https://github.com/miedziarek/Rebus.InProcCors</PackageProjectUrl>
    <RepositoryUrl>https://github.com/miedziarek/Rebus.InProcCors</RepositoryUrl>
    <Version>0.1.0</Version>
  </PropertyGroup>
</Project>
```

- [ ] **Step 3: Create `Directory.Packages.props`**

Central versions, so the Rebus floor lives in exactly one place.

```xml
<Project>
  <ItemGroup>
    <PackageVersion Include="Rebus" Version="8.9.2" />
    <PackageVersion Include="Rebus.Async" Version="10.0.0" />
    <PackageVersion Include="Microsoft.Extensions.DependencyInjection" Version="9.0.0" />
    <PackageVersion Include="Microsoft.Extensions.DependencyInjection.Abstractions" Version="9.0.0" />
    <PackageVersion Include="Microsoft.Extensions.Hosting.Abstractions" Version="9.0.0" />
    <PackageVersion Include="Microsoft.Extensions.Logging.Abstractions" Version="9.0.0" />
    <PackageVersion Include="Microsoft.Extensions.Hosting" Version="9.0.0" />
    <PackageVersion Include="Microsoft.NET.Test.Sdk" Version="17.11.1" />
    <PackageVersion Include="xunit" Version="2.9.2" />
    <PackageVersion Include="xunit.runner.visualstudio" Version="2.8.2" />
    <PackageVersion Include="BenchmarkDotNet" Version="0.14.0" />
  </ItemGroup>
</Project>
```

- [ ] **Step 4: Create `src/Rebus.InProcCors/Rebus.InProcCors.csproj`**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net8.0;net9.0</TargetFrameworks>
    <GenerateDocumentationFile>true</GenerateDocumentationFile>
    <PackageId>Rebus.InProcCors</PackageId>
    <Description>Rebus transport that passes messages by reference within a single process.</Description>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Rebus" />
  </ItemGroup>

  <ItemGroup>
    <InternalsVisibleTo Include="Rebus.InProcCors.Tests" />
    <InternalsVisibleTo Include="Rebus.InProcCors.Benchmarks" />
  </ItemGroup>
</Project>
```

- [ ] **Step 5: Create `tests/Rebus.InProcCors.Tests/Rebus.InProcCors.Tests.csproj`**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net9.0</TargetFramework>
    <IsPackable>false</IsPackable>
    <NoWarn>$(NoWarn);CS1591</NoWarn>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" />
    <PackageReference Include="xunit" />
    <PackageReference Include="xunit.runner.visualstudio" />
    <PackageReference Include="Microsoft.Extensions.DependencyInjection" />
    <PackageReference Include="Rebus.Async" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="../../src/Rebus.InProcCors/Rebus.InProcCors.csproj" />
  </ItemGroup>
</Project>
```

- [ ] **Step 6: Write the scaffold test**

`tests/Rebus.InProcCors.Tests/ScaffoldTests.cs`:

```csharp
namespace Rebus.InProcCors.Tests;

public class ScaffoldTests
{
    [Fact]
    public void TheSolutionBuildsAndTestsRun()
    {
        Assert.Equal(4, 2 + 2);
    }
}
```

- [ ] **Step 7: Create the solution and run the test**

```bash
cd /c/github/miedziarek/Rebus.InProcCors
dotnet new sln -n Rebus.InProcCors
dotnet sln add src/Rebus.InProcCors/Rebus.InProcCors.csproj
dotnet sln add tests/Rebus.InProcCors.Tests/Rebus.InProcCors.Tests.csproj
dotnet test
```

Expected: build succeeds for `net8.0` and `net9.0`, 1 test passes.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "chore: scaffold solution, central package versions, test project"
```

---

## Task 2: The reference carrier

The two carrier mechanisms of design §4 and the exception of §9. Pure data structures, no Rebus pipeline involvement, so this task is fully unit testable on its own.

**Files:**
- Create: `src/Rebus.InProcCors/ReferenceTransportMessage.cs`
- Create: `src/Rebus.InProcCors/MessageReferenceTable.cs`
- Create: `src/Rebus.InProcCors/InProcReferenceLostException.cs`
- Test: `tests/Rebus.InProcCors.Tests/MessageReferenceTableTests.cs`

**Interfaces:**
- Consumes: `Rebus.Messages.TransportMessage`.
- Produces:
  - `internal sealed class ReferenceTransportMessage : TransportMessage`, ctor `(Dictionary<string,string> headers, byte[] sentinel, object messageInstance)`, property `object MessageInstance { get; }`.
  - `internal static class MessageReferenceTable` with `static byte[] CreateSentinel()`, `static void Register(byte[] sentinel, object instance)`, `static bool TryResolve(byte[] sentinel, out object? instance)`.
  - `public sealed class InProcReferenceLostException : Exception`, ctor `(string messageType)`, property `string MessageType { get; }`.

- [ ] **Step 1: Write the failing tests**

`tests/Rebus.InProcCors.Tests/MessageReferenceTableTests.cs`:

```csharp
using System.Runtime.CompilerServices;
using Rebus.Messages;

namespace Rebus.InProcCors.Tests;

public class MessageReferenceTableTests
{
    sealed record Payload(string Value);

    [Fact]
    public void SentinelsAreOneByteAndReferenceDistinct()
    {
        var a = MessageReferenceTable.CreateSentinel();
        var b = MessageReferenceTable.CreateSentinel();

        Assert.Single(a);
        Assert.Single(b);
        Assert.False(ReferenceEquals(a, b));
    }

    [Fact]
    public void RegisteredInstanceIsResolvedByReference()
    {
        var sentinel = MessageReferenceTable.CreateSentinel();
        var payload = new Payload("hello");

        MessageReferenceTable.Register(sentinel, payload);

        Assert.True(MessageReferenceTable.TryResolve(sentinel, out var resolved));
        Assert.Same(payload, resolved);
    }

    [Fact]
    public void UnregisteredSentinelDoesNotResolve()
    {
        Assert.False(MessageReferenceTable.TryResolve(MessageReferenceTable.CreateSentinel(), out _));
    }

    [Fact]
    public void ResolutionSurvivesTheCloneThatDeadLetteringPerforms()
    {
        // Rebus/Bus/MessageExtensions.cs:129 clones as new TransportMessage(headers.Clone(), message.Body).
        // The subclass is lost; the Body array instance is not. That is the whole basis of the fallback.
        var payload = new Payload("hello");
        var sentinel = MessageReferenceTable.CreateSentinel();
        MessageReferenceTable.Register(sentinel, payload);

        var original = new ReferenceTransportMessage(new Dictionary<string, string>(), sentinel, payload);
        var cloned = new TransportMessage(new Dictionary<string, string>(original.Headers), original.Body);

        Assert.IsNotType<ReferenceTransportMessage>(cloned);
        Assert.True(MessageReferenceTable.TryResolve(cloned.Body, out var resolved));
        Assert.Same(payload, resolved);
    }

    [Fact]
    public void ReferenceTransportMessageExposesTheInstanceAndUsesTheSentinelAsBody()
    {
        var payload = new Payload("hello");
        var sentinel = MessageReferenceTable.CreateSentinel();

        var message = new ReferenceTransportMessage(
            new Dictionary<string, string> { ["rbs2-msg-id"] = "abc" }, sentinel, payload);

        Assert.Same(payload, message.MessageInstance);
        Assert.Same(sentinel, message.Body);
        Assert.Equal("abc", message.Headers["rbs2-msg-id"]);
    }

    [Fact]
    public void CollectingTheSentinelReleasesTheTableEntry()
    {
        // The table keys weakly, so entry lifetime is exactly Body-array lifetime (design §4).
        var reference = CreateCollectableEntry();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        Assert.False(reference.IsAlive);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    static WeakReference CreateCollectableEntry()
    {
        var sentinel = MessageReferenceTable.CreateSentinel();
        var payload = new Payload("collect me");
        MessageReferenceTable.Register(sentinel, payload);
        return new WeakReference(payload);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test --filter FullyQualifiedName~MessageReferenceTableTests`
Expected: FAIL to build — `MessageReferenceTable` and `ReferenceTransportMessage` do not exist (CS0103 / CS0246).

- [ ] **Step 3: Write `MessageReferenceTable.cs`**

```csharp
using System.Runtime.CompilerServices;

namespace Rebus.InProcCors;

/// <summary>
/// Side table mapping a message's sentinel <c>Body</c> array to the live message instance.
/// Keyed weakly, so an entry lives exactly as long as its sentinel array and there is no cleanup
/// path to forget. See design §4 for why this is a <see cref="ConditionalWeakTable{TKey,TValue}"/>
/// rather than a <c>ConcurrentDictionary</c>.
/// </summary>
static class MessageReferenceTable
{
    static readonly ConditionalWeakTable<byte[], object> Table = new();

    /// <summary>
    /// Allocates a fresh single-byte array to act as a message's <c>Body</c> and its table key.
    /// One byte rather than zero, because <see cref="Array.Empty{T}"/> is a shared singleton and
    /// every message's key must be a distinct object.
    /// </summary>
    public static byte[] CreateSentinel() => new byte[1];

    public static void Register(byte[] sentinel, object instance) => Table.Add(sentinel, instance);

    public static bool TryResolve(byte[] sentinel, out object? instance) => Table.TryGetValue(sentinel, out instance);
}
```

- [ ] **Step 4: Write `ReferenceTransportMessage.cs`**

```csharp
using Rebus.Messages;

namespace Rebus.InProcCors;

/// <summary>
/// A <see cref="TransportMessage"/> that additionally carries the live message object. This is the fast
/// carrier; it is lost wherever Rebus reconstructs the transport message (dead-lettering, deferral),
/// which is what <see cref="MessageReferenceTable"/> exists to cover.
/// </summary>
sealed class ReferenceTransportMessage : TransportMessage
{
    public ReferenceTransportMessage(Dictionary<string, string> headers, byte[] sentinel, object messageInstance)
        : base(headers, sentinel)
    {
        MessageInstance = messageInstance ?? throw new ArgumentNullException(nameof(messageInstance));
    }

    public object MessageInstance { get; }
}
```

- [ ] **Step 5: Write `InProcReferenceLostException.cs`**

```csharp
namespace Rebus.InProcCors;

/// <summary>
/// Thrown by <see cref="ReferenceSerializer"/> when neither the transport message subclass nor the weak
/// side table can supply the message instance. Encryption, compression and the data bus claim-check step
/// all construct a new transport message around a different body, defeating both carriers. None of the
/// three is meaningful for in-process traffic (design §9).
/// </summary>
public sealed class InProcReferenceLostException : Exception
{
    /// <summary>
    /// Creates the exception for a message whose <c>rbs2-msg-type</c> header said <paramref name="messageType"/>.
    /// </summary>
    public InProcReferenceLostException(string messageType)
        : base($"Could not recover the message instance for a message of type '{messageType}'. The transport " +
               "message carried neither the reference subclass nor a body registered in the reference table, " +
               "which means the body was replaced somewhere in the pipeline. The usual causes are encryption, " +
               "compression, or the data bus claim-check step - none of which are supported by Rebus.InProcCors, " +
               "and none of which do anything useful for in-process traffic. Remove the offending step, or " +
               "configure this endpoint with an ordinary serializer instead of ReferenceSerializer.")
    {
        MessageType = messageType;
    }

    /// <summary>
    /// Gets the value of the message's type header, or a placeholder when that header was absent.
    /// </summary>
    public string MessageType { get; }
}
```

`ReferenceSerializer` does not exist yet, so the `<see cref="ReferenceSerializer"/>` in the doc comment will
fail the build under `TreatWarningsAsErrors` (CS1574). Write the comment as `<c>ReferenceSerializer</c>` for
now and change it to `<see cref="..."/>` in Task 4, once the type exists.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `dotnet test --filter FullyQualifiedName~MessageReferenceTableTests`
Expected: PASS, 6 tests.

If `CollectingTheSentinelReleasesTheTableEntry` fails under a debugger, that is the JIT extending `sentinel`'s
lifetime to the end of the enclosing method — the `NoInlining` helper exists to bound it. Do not add
`GC.KeepAlive` and do not weaken the assertion.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: reference carrier - weak side table, transport message subclass, lost-reference exception"
```

---

## Task 3: The queue

`InProcQueue` and `InProcNetwork` from design §5. No transport yet — this task is about the channel, the depth
counter and the subscriber map in isolation.

**Files:**
- Create: `src/Rebus.InProcCors/InProcQueue.cs`
- Create: `src/Rebus.InProcCors/InProcNetwork.cs`
- Test: `tests/Rebus.InProcCors.Tests/InProcNetworkTests.cs`

**Interfaces:**
- Consumes: `Rebus.Messages.TransportMessage`.
- Produces:
  - `internal sealed class InProcQueue` with `int Count { get; }`, `void Enqueue(TransportMessage message)`, `bool TryDequeue(out TransportMessage? message)`, `ValueTask<TransportMessage?> DequeueAsync(CancellationToken cancellationToken)`.
  - `public sealed class InProcNetwork` with ctor `()`, and members:
    `void CreateQueue(string address)`, `bool HasQueue(string address)`, `IEnumerable<string> Queues { get; }`,
    `void Deliver(string destinationAddress, TransportMessage message)`,
    `int GetCount(string address)`, `void Reset()`,
    `void AddSubscriber(string topic, string subscriberAddress)`,
    `void RemoveSubscriber(string topic, string subscriberAddress)`,
    `IReadOnlyList<string> GetSubscribers(string topic)`,
    and `internal InProcQueue GetOrCreateQueue(string address)`.

- [ ] **Step 1: Write the failing tests**

`tests/Rebus.InProcCors.Tests/InProcNetworkTests.cs`:

```csharp
using Rebus.Messages;

namespace Rebus.InProcCors.Tests;

public class InProcNetworkTests
{
    static TransportMessage Msg(string id) =>
        new(new Dictionary<string, string> { ["rbs2-msg-id"] = id }, new byte[1]);

    [Fact]
    public void DeliveredMessagesComeBackInOrderAndAreTheSameInstance()
    {
        var network = new InProcNetwork();
        network.CreateQueue("orders");
        var first = Msg("1");
        var second = Msg("2");

        network.Deliver("orders", first);
        network.Deliver("orders", second);

        var queue = network.GetOrCreateQueue("orders");
        Assert.True(queue.TryDequeue(out var a));
        Assert.True(queue.TryDequeue(out var b));
        Assert.Same(first, a);
        Assert.Same(second, b);
        Assert.False(queue.TryDequeue(out _));
    }

    [Fact]
    public void CountTracksQueueDepth()
    {
        var network = new InProcNetwork();
        network.CreateQueue("orders");

        Assert.Equal(0, network.GetCount("orders"));
        network.Deliver("orders", Msg("1"));
        network.Deliver("orders", Msg("2"));
        Assert.Equal(2, network.GetCount("orders"));

        network.GetOrCreateQueue("orders").TryDequeue(out _);
        Assert.Equal(1, network.GetCount("orders"));
    }

    [Fact]
    public async Task DequeueAsyncParksUntilAMessageArrives()
    {
        var network = new InProcNetwork();
        var queue = network.GetOrCreateQueue("orders");

        var pending = queue.DequeueAsync(CancellationToken.None);
        Assert.False(pending.IsCompleted);

        var message = Msg("1");
        network.Deliver("orders", message);

        Assert.Same(message, await pending);
    }

    [Fact]
    public async Task DequeueAsyncReturnsNullWhenCancelled()
    {
        var network = new InProcNetwork();
        var queue = network.GetOrCreateQueue("orders");
        using var cts = new CancellationTokenSource();

        var pending = queue.DequeueAsync(cts.Token);
        cts.Cancel();

        Assert.Null(await pending);
    }

    [Fact]
    public async Task DeliverDoesNotRunAParkedReadersContinuationInline()
    {
        // AllowSynchronousContinuations = false is a correctness requirement, not tuning (design §5).
        // If it were true, the thread calling Deliver would run the awaiting reader's continuation -
        // turning asynchronous handoff into inline invocation.
        var network = new InProcNetwork();
        var queue = network.GetOrCreateQueue("orders");
        var deliveringThread = Environment.CurrentManagedThreadId;
        var continuationThread = 0;

        var pending = Task.Run(async () =>
        {
            await queue.DequeueAsync(CancellationToken.None);
            continuationThread = Environment.CurrentManagedThreadId;
        });

        await Task.Delay(50);
        network.Deliver("orders", Msg("1"));
        await pending;

        Assert.NotEqual(deliveringThread, continuationThread);
    }

    [Fact]
    public void DeliveringToAnUnknownQueueCreatesIt()
    {
        var network = new InProcNetwork();

        network.Deliver("newly-created", Msg("1"));

        Assert.True(network.HasQueue("newly-created"));
        Assert.Equal(1, network.GetCount("newly-created"));
    }

    [Fact]
    public void QueueNamesAreCaseInsensitive()
    {
        var network = new InProcNetwork();
        network.CreateQueue("Orders");

        network.Deliver("orders", Msg("1"));

        Assert.Equal(1, network.GetCount("ORDERS"));
        Assert.Single(network.Queues);
    }

    [Fact]
    public void SubscribersAreRegisteredAndUnregisteredPerTopic()
    {
        var network = new InProcNetwork();

        network.AddSubscriber("SomeEvent", "module-a");
        network.AddSubscriber("SomeEvent", "module-b");
        network.AddSubscriber("SomeEvent", "module-a");

        Assert.Equal(new[] { "module-a", "module-b" }, network.GetSubscribers("SomeEvent").OrderBy(x => x));

        network.RemoveSubscriber("SomeEvent", "module-a");
        Assert.Equal(new[] { "module-b" }, network.GetSubscribers("SomeEvent"));
        Assert.Empty(network.GetSubscribers("UnknownTopic"));
    }

    [Fact]
    public void ResetClearsQueuesAndSubscribers()
    {
        var network = new InProcNetwork();
        network.Deliver("orders", Msg("1"));
        network.AddSubscriber("SomeEvent", "module-a");

        network.Reset();

        Assert.Empty(network.Queues);
        Assert.Empty(network.GetSubscribers("SomeEvent"));
    }

    [Fact]
    public void TwoNetworksDoNotObserveEachOthersTraffic()
    {
        var a = new InProcNetwork();
        var b = new InProcNetwork();

        a.Deliver("orders", Msg("1"));

        Assert.Equal(1, a.GetCount("orders"));
        Assert.Equal(0, b.GetCount("orders"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test --filter FullyQualifiedName~InProcNetworkTests`
Expected: FAIL to build — `InProcNetwork` does not exist.

- [ ] **Step 3: Write `InProcQueue.cs`**

```csharp
using System.Threading.Channels;
using Rebus.Messages;

namespace Rebus.InProcCors;

/// <summary>
/// One queue: an unbounded <see cref="Channel{T}"/> of transport messages plus a depth counter.
/// </summary>
sealed class InProcQueue
{
    readonly Channel<TransportMessage> _channel = Channel.CreateUnbounded<TransportMessage>(
        new UnboundedChannelOptions
        {
            SingleReader = false,               // N workers drain one queue
            SingleWriter = false,               // any module may send to it
            AllowSynchronousContinuations = false
        });

    int _count;

    /// <summary>
    /// Gets the queue depth. Maintained with <see cref="Interlocked"/> rather than read from
    /// <see cref="ChannelReader{T}.Count"/>, to avoid a per-target-framework behavioural split on one
    /// diagnostic property (design §5).
    /// </summary>
    public int Count => Volatile.Read(ref _count);

    public void Enqueue(TransportMessage message)
    {
        // Increment first: an over-report for a few nanoseconds is safe, an under-report is not.
        Interlocked.Increment(ref _count);

        if (!_channel.Writer.TryWrite(message))
        {
            Interlocked.Decrement(ref _count);
            throw new InvalidOperationException(
                "Could not write to an unbounded channel, which should not be possible unless the network has been completed.");
        }
    }

    public bool TryDequeue(out TransportMessage? message)
    {
        if (!_channel.Reader.TryRead(out message)) return false;

        Interlocked.Decrement(ref _count);
        return true;
    }

    /// <summary>
    /// Waits for the next message, returning null when <paramref name="cancellationToken"/> fires.
    /// </summary>
    public async ValueTask<TransportMessage?> DequeueAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                // WaitToReadAsync completing is not a reservation: another worker may have taken the
                // message already, in which case we go round again.
                if (TryDequeue(out var message)) return message;
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown, or a polling-mode interval expiring. Both mean "no message".
        }

        return null;
    }
}
```

- [ ] **Step 4: Write `InProcNetwork.cs`**

```csharp
using System.Collections.Concurrent;
using Rebus.Messages;

namespace Rebus.InProcCors;

/// <summary>
/// The set of queues and subscriptions that a group of in-process buses share. Construct one per host
/// and pass it explicitly to each module's transport configuration. There is deliberately no static
/// default instance: a singleton would make two independent test fixtures silently share a network
/// (design §5).
/// </summary>
public sealed class InProcNetwork
{
    readonly ConcurrentDictionary<string, InProcQueue> _queues = new(StringComparer.OrdinalIgnoreCase);

    readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _subscribers =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets the names of all queues that currently exist on this network.
    /// </summary>
    public IEnumerable<string> Queues => _queues.Keys;

    /// <summary>
    /// Creates the queue with the given address if it does not already exist.
    /// </summary>
    public void CreateQueue(string address) => GetOrCreateQueue(address);

    /// <summary>
    /// Gets whether a queue with the given address exists on this network.
    /// </summary>
    public bool HasQueue(string address) => _queues.ContainsKey(address);

    /// <summary>
    /// Delivers <paramref name="message"/> to the queue named <paramref name="destinationAddress"/>,
    /// creating that queue if it does not exist. The message instance is enqueued unchanged.
    /// </summary>
    public void Deliver(string destinationAddress, TransportMessage message)
    {
        if (destinationAddress == null) throw new ArgumentNullException(nameof(destinationAddress));
        if (message == null) throw new ArgumentNullException(nameof(message));

        GetOrCreateQueue(destinationAddress).Enqueue(message);
    }

    /// <summary>
    /// Gets the number of messages waiting in the queue with the given address.
    /// </summary>
    public int GetCount(string address) =>
        _queues.TryGetValue(address, out var queue) ? queue.Count : 0;

    /// <summary>
    /// Deletes all queues, their messages, and all subscriptions.
    /// </summary>
    public void Reset()
    {
        _queues.Clear();
        _subscribers.Clear();
    }

    /// <summary>
    /// Registers <paramref name="subscriberAddress"/> as a subscriber of <paramref name="topic"/>.
    /// </summary>
    public void AddSubscriber(string topic, string subscriberAddress) =>
        _subscribers.GetOrAdd(topic, _ => new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase))
            .TryAdd(subscriberAddress, 0);

    /// <summary>
    /// Unregisters <paramref name="subscriberAddress"/> as a subscriber of <paramref name="topic"/>.
    /// </summary>
    public void RemoveSubscriber(string topic, string subscriberAddress)
    {
        if (_subscribers.TryGetValue(topic, out var set)) set.TryRemove(subscriberAddress, out _);
    }

    /// <summary>
    /// Gets the addresses subscribed to <paramref name="topic"/>.
    /// </summary>
    public IReadOnlyList<string> GetSubscribers(string topic) =>
        _subscribers.TryGetValue(topic, out var set) ? set.Keys.ToArray() : Array.Empty<string>();

    internal InProcQueue GetOrCreateQueue(string address)
    {
        if (address == null) throw new ArgumentNullException(nameof(address));

        return _queues.GetOrAdd(address, _ => new InProcQueue());
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dotnet test --filter FullyQualifiedName~InProcNetworkTests`
Expected: PASS, 10 tests.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: InProcNetwork and InProcQueue over unbounded channels"
```

---

## Task 4: The reference serializer

Design §7 and the resolution order of §4. Testable against hand-built `TransportMessage`s with no bus.

**Files:**
- Create: `src/Rebus.InProcCors/ReferenceSerializer.cs`
- Modify: `src/Rebus.InProcCors/InProcReferenceLostException.cs` (restore the `<see cref>` deferred in Task 2)
- Test: `tests/Rebus.InProcCors.Tests/ReferenceSerializerTests.cs`

**Interfaces:**
- Consumes: `MessageReferenceTable`, `ReferenceTransportMessage`, `InProcReferenceLostException` (Task 2).
- Produces: `public sealed class ReferenceSerializer : ISerializer` with ctor `(IMessageTypeNameConvention messageTypeNameConvention)` and the two `ISerializer` members. Public const `string ReferenceContentType = "application/x-rebus-inproc-reference"`.

- [ ] **Step 1: Write the failing tests**

`tests/Rebus.InProcCors.Tests/ReferenceSerializerTests.cs`:

```csharp
using Rebus.Messages;
using Rebus.Serialization;

namespace Rebus.InProcCors.Tests;

public class ReferenceSerializerTests
{
    sealed record PlaceOrder(string Sku, int Quantity);

    sealed class TestTypeNameConvention : IMessageTypeNameConvention
    {
        public string GetTypeName(Type type) => type.FullName!;
        public Type GetType(string name) => Type.GetType(name)!;
    }

    static ReferenceSerializer CreateSerializer() => new(new TestTypeNameConvention());

    static Message LogicalMessage(object body) => new(new Dictionary<string, string>(), body);

    [Fact]
    public async Task SerializeProducesAReferenceTransportMessageWithAFreshSentinelBody()
    {
        var serializer = CreateSerializer();
        var order = new PlaceOrder("ABC", 2);

        var first = await serializer.Serialize(LogicalMessage(order));
        var second = await serializer.Serialize(LogicalMessage(order));

        var reference = Assert.IsType<ReferenceTransportMessage>(first);
        Assert.Same(order, reference.MessageInstance);
        Assert.Single(first.Body);
        Assert.False(ReferenceEquals(first.Body, second.Body));
    }

    [Fact]
    public async Task SerializePopulatesTheTypeAndContentTypeHeaders()
    {
        var serializer = CreateSerializer();

        var transportMessage = await serializer.Serialize(LogicalMessage(new PlaceOrder("ABC", 2)));

        Assert.Equal(typeof(PlaceOrder).FullName, transportMessage.Headers[Headers.Type]);
        Assert.Equal(ReferenceSerializer.ReferenceContentType, transportMessage.Headers[Headers.ContentType]);
    }

    [Fact]
    public async Task SerializePreservesCallerSuppliedHeadersWithoutMutatingTheInput()
    {
        var serializer = CreateSerializer();
        var headers = new Dictionary<string, string> { [Headers.MessageId] = "abc" };
        var message = new Message(headers, new PlaceOrder("ABC", 2));

        var transportMessage = await serializer.Serialize(message);

        Assert.Equal("abc", transportMessage.Headers[Headers.MessageId]);
        Assert.False(ReferenceEquals(headers, transportMessage.Headers));
        Assert.False(headers.ContainsKey(Headers.ContentType));
    }

    [Fact]
    public async Task SerializeDoesNotOverwriteAnExplicitTypeHeader()
    {
        var serializer = CreateSerializer();
        var headers = new Dictionary<string, string> { [Headers.Type] = "Explicit.Type.Name" };

        var transportMessage = await serializer.Serialize(new Message(headers, new PlaceOrder("ABC", 2)));

        Assert.Equal("Explicit.Type.Name", transportMessage.Headers[Headers.Type]);
    }

    [Fact]
    public async Task DeserializeReturnsTheIdenticalInstanceViaTheSubclass()
    {
        var serializer = CreateSerializer();
        var order = new PlaceOrder("ABC", 2);

        var transportMessage = await serializer.Serialize(LogicalMessage(order));
        var roundTripped = await serializer.Deserialize(transportMessage);

        Assert.Same(order, roundTripped.Body);
    }

    [Fact]
    public async Task DeserializeReturnsTheIdenticalInstanceAfterTheSubclassIsLost()
    {
        var serializer = CreateSerializer();
        var order = new PlaceOrder("ABC", 2);

        var transportMessage = await serializer.Serialize(LogicalMessage(order));
        var cloned = new TransportMessage(new Dictionary<string, string>(transportMessage.Headers), transportMessage.Body);

        var roundTripped = await serializer.Deserialize(cloned);

        Assert.Same(order, roundTripped.Body);
    }

    [Fact]
    public async Task DeserializeCopiesTheHeadersOntoTheLogicalMessage()
    {
        var serializer = CreateSerializer();
        var transportMessage = await serializer.Serialize(
            new Message(new Dictionary<string, string> { [Headers.MessageId] = "abc" }, new PlaceOrder("ABC", 2)));

        var roundTripped = await serializer.Deserialize(transportMessage);

        Assert.Equal("abc", roundTripped.Headers[Headers.MessageId]);
        Assert.False(ReferenceEquals(transportMessage.Headers, roundTripped.Headers));
    }

    [Fact]
    public async Task DeserializeThrowsWhenBothCarriersMiss()
    {
        var serializer = CreateSerializer();
        var headers = new Dictionary<string, string> { [Headers.Type] = typeof(PlaceOrder).FullName! };
        var replacedBody = new TransportMessage(headers, new byte[] { 1, 2, 3 });

        var exception = await Assert.ThrowsAsync<InProcReferenceLostException>(
            () => serializer.Deserialize(replacedBody));

        Assert.Equal(typeof(PlaceOrder).FullName, exception.MessageType);
        Assert.Contains("encryption", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("compression", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("data bus", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DeserializeReportsAPlaceholderWhenTheTypeHeaderIsAbsent()
    {
        var serializer = CreateSerializer();

        var exception = await Assert.ThrowsAsync<InProcReferenceLostException>(
            () => serializer.Deserialize(new TransportMessage(new Dictionary<string, string>(), new byte[] { 1 })));

        Assert.Equal("(no rbs2-msg-type header)", exception.MessageType);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test --filter FullyQualifiedName~ReferenceSerializerTests`
Expected: FAIL to build — `ReferenceSerializer` does not exist.

- [ ] **Step 3: Write `ReferenceSerializer.cs`**

```csharp
using Rebus.Messages;
using Rebus.Serialization;

namespace Rebus.InProcCors;

/// <summary>
/// An <see cref="ISerializer"/> that does not serialize. It attaches the live message object to the
/// transport message by two independent carriers and hands the identical instance back on the way in.
/// <para>
/// The transport and the serializer are independent: <see cref="InProcTransport"/> carries any transport
/// message, so pairing it with an ordinary JSON serializer is valid and supported.
/// </para>
/// </summary>
public sealed class ReferenceSerializer : ISerializer
{
    /// <summary>
    /// The value written to the <c>rbs2-content-type</c> header. Present so that anything reading headers -
    /// logging, the error queue, a human - can see at a glance that no bytes were produced.
    /// </summary>
    public const string ReferenceContentType = "application/x-rebus-inproc-reference";

    const string UnknownMessageType = "(no rbs2-msg-type header)";

    readonly IMessageTypeNameConvention _messageTypeNameConvention;

    /// <summary>
    /// Creates the serializer, using <paramref name="messageTypeNameConvention"/> to populate the
    /// <c>rbs2-msg-type</c> header exactly as an ordinary serializer would.
    /// </summary>
    public ReferenceSerializer(IMessageTypeNameConvention messageTypeNameConvention)
    {
        _messageTypeNameConvention = messageTypeNameConvention
                                     ?? throw new ArgumentNullException(nameof(messageTypeNameConvention));
    }

    /// <inheritdoc />
    public Task<TransportMessage> Serialize(Message message)
    {
        if (message == null) throw new ArgumentNullException(nameof(message));

        var body = message.Body;
        var headers = new Dictionary<string, string>(message.Headers);

        headers[Headers.ContentType] = ReferenceContentType;

        if (!headers.ContainsKey(Headers.Type))
        {
            headers[Headers.Type] = _messageTypeNameConvention.GetTypeName(body.GetType());
        }

        var sentinel = MessageReferenceTable.CreateSentinel();
        MessageReferenceTable.Register(sentinel, body);

        return Task.FromResult<TransportMessage>(new ReferenceTransportMessage(headers, sentinel, body));
    }

    /// <inheritdoc />
    /// <exception cref="InProcReferenceLostException">
    /// Thrown when neither carrier can supply the instance - see design §9.
    /// </exception>
    public Task<Message> Deserialize(TransportMessage transportMessage)
    {
        if (transportMessage == null) throw new ArgumentNullException(nameof(transportMessage));

        var instance = ResolveInstance(transportMessage);
        var headers = new Dictionary<string, string>(transportMessage.Headers);

        return Task.FromResult(new Message(headers, instance));
    }

    static object ResolveInstance(TransportMessage transportMessage)
    {
        // 1. The common path: a type check, no table access.
        if (transportMessage is ReferenceTransportMessage reference) return reference.MessageInstance;

        // 2. The reconstructed path - dead-letter, deferral, auditing, auto-forward-on-exception.
        //    Clone() and DueMessage both pass the same Body array through, so identity rides on it.
        if (MessageReferenceTable.TryResolve(transportMessage.Body, out var instance) && instance != null)
        {
            return instance;
        }

        // 3. The body was replaced. Fail loudly, here, rather than let a null surface inside a handler.
        var messageType = transportMessage.Headers.TryGetValue(Headers.Type, out var type)
            ? type
            : UnknownMessageType;

        throw new InProcReferenceLostException(messageType);
    }
}
```

- [ ] **Step 4: Restore the deferred doc reference**

In `src/Rebus.InProcCors/InProcReferenceLostException.cs`, change `<c>ReferenceSerializer</c>` back to
`<see cref="ReferenceSerializer"/>` now that the type exists.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dotnet test --filter FullyQualifiedName~ReferenceSerializerTests`
Expected: PASS, 9 tests.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: ReferenceSerializer with two-carrier resolution and loud failure on loss"
```

---

## Task 5: The transport

Design §6. `InProcTransport` plus its options. Still no bus wiring — tested directly against a
`RebusTransactionScope`.

**Files:**
- Create: `src/Rebus.InProcCors/InProcReceiveMode.cs`
- Create: `src/Rebus.InProcCors/InProcTransportOptions.cs`
- Create: `src/Rebus.InProcCors/InProcTransport.cs`
- Test: `tests/Rebus.InProcCors.Tests/InProcTransportTests.cs`

**Interfaces:**
- Consumes: `InProcNetwork`, `InProcQueue` (Task 3).
- Produces:
  - `public enum InProcReceiveMode { Blocking, Polling }`.
  - `public sealed class InProcTransportOptions` with `InProcReceiveMode ReceiveMode { get; set; } = InProcReceiveMode.Blocking` and `TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(100)`.
  - `public sealed class InProcTransport : AbstractRebusTransport, IInitializable, ITransportInspector, ISubscriptionStorage` with ctor `(InProcNetwork network, string? inputQueueAddress, InProcTransportOptions? options = null)`.

- [ ] **Step 1: Write `InProcReceiveMode.cs` and `InProcTransportOptions.cs`**

These are declarations with no behaviour, so they precede the test rather than following it.

```csharp
namespace Rebus.InProcCors;

/// <summary>
/// How <see cref="InProcTransport"/> waits for the next message.
/// </summary>
public enum InProcReceiveMode
{
    /// <summary>
    /// Park on the queue until a message arrives. Avoids the 100-250 ms wake-up latency that Rebus's
    /// backoff ladder imposes on the first message after an idle period, which is the dominant traffic
    /// shape of a modular monolith. The Rebus worker loop tolerates this: ThreadPoolWorker starts the
    /// receive without awaiting it, so a parked receive does not stall the loop (design §2.1).
    /// </summary>
    Blocking,

    /// <summary>
    /// Wait at most <see cref="InProcTransportOptions.PollingInterval"/> and then report no message,
    /// dropping Rebus back into its normal backoff ladder.
    /// </summary>
    Polling
}
```

```csharp
namespace Rebus.InProcCors;

/// <summary>
/// Options for <see cref="InProcTransport"/>. Switchable per bus without touching handler code.
/// </summary>
public sealed class InProcTransportOptions
{
    /// <summary>
    /// Gets or sets how the transport waits for the next message. Defaults to
    /// <see cref="InProcReceiveMode.Blocking"/>.
    /// </summary>
    public InProcReceiveMode ReceiveMode { get; set; } = InProcReceiveMode.Blocking;

    /// <summary>
    /// Gets or sets how long a receive waits before reporting no message when
    /// <see cref="ReceiveMode"/> is <see cref="InProcReceiveMode.Polling"/>. Defaults to 100 ms.
    /// Ignored in <see cref="InProcReceiveMode.Blocking"/> mode.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(100);
}
```

- [ ] **Step 2: Write the failing tests**

`tests/Rebus.InProcCors.Tests/InProcTransportTests.cs`:

```csharp
using System.Diagnostics;
using Rebus.Messages;
using Rebus.Transport;

namespace Rebus.InProcCors.Tests;

public class InProcTransportTests
{
    static TransportMessage Msg(string id) =>
        new(new Dictionary<string, string> { [Headers.MessageId] = id }, new byte[1]);

    static InProcTransport CreateTransport(InProcNetwork network, string? address,
        InProcReceiveMode mode = InProcReceiveMode.Blocking)
    {
        var transport = new InProcTransport(network, address,
            new InProcTransportOptions { ReceiveMode = mode, PollingInterval = TimeSpan.FromMilliseconds(50) });
        transport.Initialize();
        return transport;
    }

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task SendThenReceiveYieldsTheIdenticalTransportMessageInstance(InProcReceiveMode mode)
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders", mode);
        var message = Msg("1");

        using (var scope = new RebusTransactionScope())
        {
            await transport.Send("orders", message, scope.TransactionContext);
            await scope.CompleteAsync();
        }

        using var receiveScope = new RebusTransactionScope();
        var received = await transport.Receive(receiveScope.TransactionContext, CancellationToken.None);

        Assert.Same(message, received);
    }

    [Fact]
    public async Task SendIsDeferredUntilTheAmbientTransactionCommits()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders");

        using (var scope = new RebusTransactionScope())
        {
            await transport.Send("orders", Msg("1"), scope.TransactionContext);
            Assert.Equal(0, network.GetCount("orders"));
            await scope.CompleteAsync();
        }

        Assert.Equal(1, network.GetCount("orders"));
    }

    [Fact]
    public async Task InitializeCreatesTheInputQueue()
    {
        var network = new InProcNetwork();

        CreateTransport(network, "orders");

        Assert.True(network.HasQueue("orders"));
    }

    [Fact]
    public async Task ANackedMessageGoesBackOntoTheQueue()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders");
        var message = Msg("1");
        network.Deliver("orders", message);

        using (var scope = new RebusTransactionScope())
        {
            var received = await transport.Receive(scope.TransactionContext, CancellationToken.None);
            Assert.Same(message, received);
            // Disposing without completing rolls the scope back, which fires OnNack.
        }

        Assert.Equal(1, network.GetCount("orders"));
    }

    [Fact]
    public async Task BlockingReceiveParksUntilAMessageArrives()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders", InProcReceiveMode.Blocking);
        using var scope = new RebusTransactionScope();

        var pending = transport.Receive(scope.TransactionContext, CancellationToken.None);
        Assert.False(pending.IsCompleted);

        var message = Msg("1");
        network.Deliver("orders", message);

        Assert.Same(message, await pending);
    }

    [Fact]
    public async Task BlockingReceiveReturnsNullWhenCancelled()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders", InProcReceiveMode.Blocking);
        using var scope = new RebusTransactionScope();
        using var cts = new CancellationTokenSource();

        var pending = transport.Receive(scope.TransactionContext, cts.Token);
        cts.Cancel();

        Assert.Null(await pending);
    }

    [Fact]
    public async Task PollingReceiveReturnsNullAfterTheIntervalWithoutThrowing()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders", InProcReceiveMode.Polling);
        using var scope = new RebusTransactionScope();

        var stopwatch = Stopwatch.StartNew();
        var received = await transport.Receive(scope.TransactionContext, CancellationToken.None);
        stopwatch.Stop();

        Assert.Null(received);
        Assert.True(stopwatch.Elapsed >= TimeSpan.FromMilliseconds(25),
            $"expected the receive to wait out its interval, waited {stopwatch.ElapsedMilliseconds} ms");
    }

    [Fact]
    public async Task AOneWayClientCannotReceive()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, address: null);
        using var scope = new RebusTransactionScope();

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => transport.Receive(scope.TransactionContext, CancellationToken.None));
    }

    [Fact]
    public async Task GetPropertiesReportsQueueLength()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders");
        network.Deliver("orders", Msg("1"));
        network.Deliver("orders", Msg("2"));

        var properties = await transport.GetProperties(CancellationToken.None);

        Assert.Equal("2", properties[TransportInspectorPropertyKeys.QueueLength]);
    }

    [Fact]
    public async Task SubscriptionStorageIsCentralizedAndDelegatesToTheNetwork()
    {
        var network = new InProcNetwork();
        var transport = CreateTransport(network, "orders");

        Assert.True(transport.IsCentralized);

        await transport.RegisterSubscriber("SomeEvent", "orders");
        Assert.Equal(new[] { "orders" }, await transport.GetSubscriberAddresses("SomeEvent"));
        Assert.Equal(new[] { "orders" }, network.GetSubscribers("SomeEvent"));

        await transport.UnregisterSubscriber("SomeEvent", "orders");
        Assert.Empty(await transport.GetSubscriberAddresses("SomeEvent"));
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `dotnet test --filter FullyQualifiedName~InProcTransportTests`
Expected: FAIL to build — `InProcTransport` does not exist.

- [ ] **Step 4: Write `InProcTransport.cs`**

```csharp
using Rebus.Bus;
using Rebus.Messages;
using Rebus.Subscriptions;
using Rebus.Transport;

namespace Rebus.InProcCors;

/// <summary>
/// A transport that moves <see cref="TransportMessage"/> instances between queues on an
/// <see cref="InProcNetwork"/> without copying them. Deriving from <see cref="AbstractRebusTransport"/>
/// gives ambient-transaction deferral for free and, importantly, preserves the message reference: the
/// base class enqueues the caller's instance unchanged and flushes on commit (design §2.7).
/// </summary>
public sealed class InProcTransport : AbstractRebusTransport, IInitializable, ITransportInspector, ISubscriptionStorage
{
    readonly InProcNetwork _network;
    readonly string? _inputQueueAddress;
    readonly InProcTransportOptions _options;

    /// <summary>
    /// Creates the transport on the given <paramref name="network"/>. Pass null for
    /// <paramref name="inputQueueAddress"/> to make a one-way client.
    /// </summary>
    public InProcTransport(InProcNetwork network, string? inputQueueAddress, InProcTransportOptions? options = null)
        : base(inputQueueAddress!)
    {
        _network = network ?? throw new ArgumentNullException(nameof(network));
        _inputQueueAddress = inputQueueAddress;
        _options = options ?? new InProcTransportOptions();
    }

    /// <inheritdoc />
    public override void CreateQueue(string address) => _network.CreateQueue(address);

    /// <summary>
    /// Creates this transport's own input queue. Does nothing for a one-way client.
    /// </summary>
    public void Initialize()
    {
        if (_inputQueueAddress == null) return;

        CreateQueue(_inputQueueAddress);
    }

    /// <inheritdoc />
    protected override async Task SendOutgoingMessages(
        IEnumerable<OutgoingTransportMessage> outgoingMessages, ITransactionContext context)
    {
        foreach (var message in outgoingMessages)
        {
            // Unchanged: the very instance the serializer produced, subclass and all.
            _network.Deliver(message.DestinationAddress, message.TransportMessage);
        }

        await Task.CompletedTask;
    }

    /// <inheritdoc />
    public override async Task<TransportMessage?> Receive(ITransactionContext context, CancellationToken cancellationToken)
    {
        if (context == null) throw new ArgumentNullException(nameof(context));

        if (_inputQueueAddress == null)
        {
            throw new InvalidOperationException(
                "This in-proc transport was initialized without an input queue, so it cannot receive anything.");
        }

        var queue = _network.GetOrCreateQueue(_inputQueueAddress);
        var message = await ReceiveNext(queue, cancellationToken).ConfigureAwait(false);

        if (message == null) return null;

        // No peek-and-requeue on a channel, so a nack goes to the tail. InMemTransport.cs:53 does the same,
        // but this differs from a broker that redelivers in place (design §5).
        context.OnNack(_ =>
        {
            _network.Deliver(_inputQueueAddress, message);
            return Task.CompletedTask;
        });

        return message;
    }

    async ValueTask<TransportMessage?> ReceiveNext(InProcQueue queue, CancellationToken cancellationToken)
    {
        if (_options.ReceiveMode == InProcReceiveMode.Blocking)
        {
            return await queue.DequeueAsync(cancellationToken).ConfigureAwait(false);
        }

        if (queue.TryDequeue(out var immediate)) return immediate;

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_options.PollingInterval);

        // DequeueAsync swallows cancellation and returns null, which is exactly what an expired
        // polling interval means: no message, drop back into Rebus's backoff ladder.
        return await queue.DequeueAsync(cts.Token).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<Dictionary<string, object>> GetProperties(CancellationToken cancellationToken)
    {
        if (_inputQueueAddress == null)
        {
            throw new InvalidOperationException("Cannot get the message count of a one-way transport.");
        }

        await Task.CompletedTask;

        return new Dictionary<string, object>
        {
            [TransportInspectorPropertyKeys.QueueLength] = _network.GetCount(_inputQueueAddress).ToString()
        };
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> GetSubscriberAddresses(string topic)
    {
        await Task.CompletedTask;
        return _network.GetSubscribers(topic);
    }

    /// <inheritdoc />
    public async Task RegisterSubscriber(string topic, string subscriberAddress)
    {
        await Task.CompletedTask;
        _network.AddSubscriber(topic, subscriberAddress);
    }

    /// <inheritdoc />
    public async Task UnregisterSubscriber(string topic, string subscriberAddress)
    {
        await Task.CompletedTask;
        _network.RemoveSubscriber(topic, subscriberAddress);
    }

    /// <summary>
    /// Always true - the network is a single shared object, so subscriptions are established directly.
    /// </summary>
    public bool IsCentralized => true;
}
```

`AbstractRebusTransport.Receive` is declared as returning `Task<TransportMessage>` in a Rebus assembly
compiled without nullable annotations, so the `Task<TransportMessage?>` override may produce CS8609. If it
does, drop the `?` from the override's return type and keep `#nullable` behaviour internal to the method.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dotnet test --filter FullyQualifiedName~InProcTransportTests`
Expected: PASS, 12 tests (the first is a `[Theory]` with 2 cases).

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: InProcTransport with blocking and polling receive modes"
```

---

## Task 6: Configuration extensions

Design §8, as corrected by **C1** at the top of this plan. This is the task that first wires a real
`IBus` together, so its test is an end-to-end delivery.

**Files:**
- Create: `src/Rebus.InProcCors/Config/InProcTransportConfigurationExtensions.cs`
- Create: `src/Rebus.InProcCors/Config/ReferenceSerializerConfigurationExtensions.cs`
- Test: `tests/Rebus.InProcCors.Tests/ConfigurationTests.cs`

**Interfaces:**
- Consumes: `InProcTransport`, `InProcTransportOptions`, `InProcNetwork`, `ReferenceSerializer`.
- Produces, all in namespace `Rebus.Config`:
  - `public static void UseInProcTransport(this StandardConfigurer<ITransport> configurer, InProcNetwork network, string inputQueueName, Action<InProcTransportOptions>? configureOptions = null, bool registerSubscriptionStorage = true, bool registerReferenceSerializer = true)`
  - `public static void UseInProcTransportAsOneWayClient(this StandardConfigurer<ITransport> configurer, InProcNetwork network, Action<InProcTransportOptions>? configureOptions = null, bool registerSubscriptionStorage = true, bool registerReferenceSerializer = true)`
  - `public static void UseReferenceSerializer(this StandardConfigurer<ISerializer> configurer)`

- [ ] **Step 1: Write the failing tests**

`tests/Rebus.InProcCors.Tests/ConfigurationTests.cs`:

```csharp
using Rebus.Activation;
using Rebus.Config;
using Rebus.Serialization;
using Rebus.Transport;

namespace Rebus.InProcCors.Tests;

public class ConfigurationTests
{
    sealed record PlaceOrder(string Sku);

    [Fact]
    public async Task AConfiguredBusDeliversByReference()
    {
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var activator = new BuiltinHandlerActivator();
        activator.Handle<PlaceOrder>(async message => received.TrySetResult(message));

        var bus = Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, "orders"))
            .Start();

        var sent = new PlaceOrder("ABC");
        await bus.SendLocal(sent);

        var handled = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Same(sent, handled);
    }

    [Fact]
    public async Task TheOptionsCallbackSelectsTheReceiveMode()
    {
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var activator = new BuiltinHandlerActivator();
        activator.Handle<PlaceOrder>(async message => received.TrySetResult(message));

        var bus = Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, "orders", o => o.ReceiveMode = InProcReceiveMode.Polling))
            .Start();

        var sent = new PlaceOrder("ABC");
        await bus.SendLocal(sent);

        Assert.Same(sent, await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task AOneWayClientCanSendToAnotherBus()
    {
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var receiverActivator = new BuiltinHandlerActivator();
        receiverActivator.Handle<PlaceOrder>(async message => received.TrySetResult(message));
        Configure.With(receiverActivator)
            .Transport(t => t.UseInProcTransport(network, "orders"))
            .Start();

        using var senderActivator = new BuiltinHandlerActivator();
        var sender = Configure.With(senderActivator)
            .Transport(t => t.UseInProcTransportAsOneWayClient(network))
            .Routing(r => r.TypeBased().Map<PlaceOrder>("orders"))
            .Start();

        var sent = new PlaceOrder("ABC");
        await sender.Send(sent);

        Assert.Same(sent, await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task PassingRegisterReferenceSerializerFalseLeavesTheDefaultSerializerInPlace()
    {
        // Correction C1: Rebus's Injectionist throws on a duplicate primary registration and does not
        // expose PossiblyRegisterDefault to extension authors, so opting out is a flag rather than
        // an override. This also gives benchmark arm 2 (InProcTransport + System.Text.Json) its config.
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var activator = new BuiltinHandlerActivator();
        activator.Handle<PlaceOrder>(async message => received.TrySetResult(message));

        var bus = Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, "orders", registerReferenceSerializer: false))
            .Start();

        var sent = new PlaceOrder("ABC");
        await bus.SendLocal(sent);

        var handled = await received.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(sent, handled);
        Assert.NotSame(sent, handled);  // it went through JSON
    }

    [Fact]
    public async Task AnExplicitSerializationCallCombinesWithRegisterReferenceSerializerFalse()
    {
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var activator = new BuiltinHandlerActivator();
        activator.Handle<PlaceOrder>(async message => received.TrySetResult(message));

        var bus = Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, "orders", registerReferenceSerializer: false))
            .Serialization(s => s.UseReferenceSerializer())
            .Start();

        var sent = new PlaceOrder("ABC");
        await bus.SendLocal(sent);

        Assert.Same(sent, await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void RegisteringTheSerializerTwiceFailsLoudly()
    {
        // Documents correction C1: this is why registerReferenceSerializer exists.
        var network = new InProcNetwork();
        using var activator = new BuiltinHandlerActivator();

        Assert.Throws<InvalidOperationException>(() => Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, "orders"))
            .Serialization(s => s.UseReferenceSerializer())
            .Start());
    }

    [Fact]
    public async Task PubSubWorksWithoutExtraSubscriptionConfiguration()
    {
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var subscriberActivator = new BuiltinHandlerActivator();
        subscriberActivator.Handle<PlaceOrder>(async message => received.TrySetResult(message));
        var subscriber = Configure.With(subscriberActivator)
            .Transport(t => t.UseInProcTransport(network, "subscriber"))
            .Start();
        await subscriber.Subscribe<PlaceOrder>();

        using var publisherActivator = new BuiltinHandlerActivator();
        var publisher = Configure.With(publisherActivator)
            .Transport(t => t.UseInProcTransport(network, "publisher"))
            .Start();

        var sent = new PlaceOrder("ABC");
        await publisher.Publish(sent);

        Assert.Same(sent, await received.Task.WaitAsync(TimeSpan.FromSeconds(5)));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test --filter FullyQualifiedName~ConfigurationTests`
Expected: FAIL to build — `UseInProcTransport` does not exist.

- [ ] **Step 3: Write `Config/InProcTransportConfigurationExtensions.cs`**

```csharp
using Rebus.InProcCors;
using Rebus.Serialization;
using Rebus.Subscriptions;
using Rebus.Transport;

namespace Rebus.Config;

/// <summary>
/// Configuration extensions for the in-proc transport.
/// </summary>
public static class InProcTransportConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to deliver and receive messages on the given <paramref name="network"/>, using
    /// <paramref name="inputQueueName"/> as this endpoint's input queue.
    /// </summary>
    /// <param name="configurer">The transport configurer.</param>
    /// <param name="network">The host-owned network shared by the modules that talk to each other.</param>
    /// <param name="inputQueueName">This endpoint's input queue.</param>
    /// <param name="configureOptions">Optional callback for receive mode and polling interval.</param>
    /// <param name="registerSubscriptionStorage">
    /// When true (the default) the network doubles as subscription storage, so pub/sub needs no extra
    /// configuration. Pass false when registering a subscription storage explicitly.
    /// </param>
    /// <param name="registerReferenceSerializer">
    /// When true (the default) <see cref="ReferenceSerializer"/> is registered as the endpoint's serializer.
    /// Pass false when calling <c>.Serialization(...)</c> explicitly - Rebus throws on a duplicate primary
    /// registration, so the two cannot both be present.
    /// </param>
    public static void UseInProcTransport(
        this StandardConfigurer<ITransport> configurer,
        InProcNetwork network,
        string inputQueueName,
        Action<InProcTransportOptions>? configureOptions = null,
        bool registerSubscriptionStorage = true,
        bool registerReferenceSerializer = true)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (network == null) throw new ArgumentNullException(nameof(network));
        if (inputQueueName == null) throw new ArgumentNullException(nameof(inputQueueName));

        Register(configurer, network, inputQueueName, configureOptions, registerSubscriptionStorage,
            registerReferenceSerializer);

        configurer.OtherService<ITransportInspector>().Register(context => context.Get<InProcTransport>());
    }

    /// <summary>
    /// Configures Rebus to send on the given <paramref name="network"/> as a one-way client, with no input
    /// queue and no workers.
    /// </summary>
    /// <param name="configurer">The transport configurer.</param>
    /// <param name="network">The host-owned network shared by the modules that talk to each other.</param>
    /// <param name="configureOptions">Optional callback for receive mode and polling interval.</param>
    /// <param name="registerSubscriptionStorage">See <see cref="UseInProcTransport"/>.</param>
    /// <param name="registerReferenceSerializer">See <see cref="UseInProcTransport"/>.</param>
    public static void UseInProcTransportAsOneWayClient(
        this StandardConfigurer<ITransport> configurer,
        InProcNetwork network,
        Action<InProcTransportOptions>? configureOptions = null,
        bool registerSubscriptionStorage = true,
        bool registerReferenceSerializer = true)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (network == null) throw new ArgumentNullException(nameof(network));

        Register(configurer, network, inputQueueName: null, configureOptions, registerSubscriptionStorage,
            registerReferenceSerializer);

        OneWayClientBackdoor.ConfigureOneWayClient(configurer);
    }

    static void Register(
        StandardConfigurer<ITransport> configurer,
        InProcNetwork network,
        string? inputQueueName,
        Action<InProcTransportOptions>? configureOptions,
        bool registerSubscriptionStorage,
        bool registerReferenceSerializer)
    {
        var options = new InProcTransportOptions();
        configureOptions?.Invoke(options);

        configurer.OtherService<InProcTransport>()
            .Register(_ => new InProcTransport(network, inputQueueName, options));

        if (registerSubscriptionStorage)
        {
            configurer.OtherService<ISubscriptionStorage>().Register(context => context.Get<InProcTransport>());
        }

        if (registerReferenceSerializer)
        {
            configurer.OtherService<ISerializer>()
                .Register(context => new ReferenceSerializer(context.Get<IMessageTypeNameConvention>()));
        }

        configurer.Register(context => context.Get<InProcTransport>());
    }
}
```

- [ ] **Step 4: Write `Config/ReferenceSerializerConfigurationExtensions.cs`**

```csharp
using Rebus.InProcCors;
using Rebus.Serialization;

namespace Rebus.Config;

/// <summary>
/// Configuration extensions for <see cref="ReferenceSerializer"/>.
/// </summary>
public static class ReferenceSerializerConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to pass message objects by reference instead of serializing them. Requires a
    /// transport that preserves the transport message instance and its body array - in practice
    /// <c>UseInProcTransport</c>. When using that method, pass
    /// <c>registerReferenceSerializer: false</c> to it, or Rebus will reject the duplicate registration.
    /// </summary>
    public static void UseReferenceSerializer(this StandardConfigurer<ISerializer> configurer)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));

        configurer.Register(context => new ReferenceSerializer(context.Get<IMessageTypeNameConvention>()));
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dotnet test --filter FullyQualifiedName~ConfigurationTests`
Expected: PASS, 7 tests.

If `RegisteringTheSerializerTwiceFailsLoudly` reports the exception at `.Serialization(...)` rather than at
`.Start()`, move the `Assert.Throws` to wrap only the offending call. Either location proves the point; the
test asserts the behaviour that motivates the flag.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: transport and serializer configuration extensions"
```

---

## Task 7: Integration tests that prove the findings

Design §11 items 1-8 and 10. These are the tests that earn their place: each one pins a claim from §2 that
the whole design rests on. Every one runs under both receive modes.

**Files:**
- Create: `tests/Rebus.InProcCors.Tests/TestModule.cs`
- Create: `tests/Rebus.InProcCors.Tests/ReferenceDeliveryTests.cs`
- Create: `tests/Rebus.InProcCors.Tests/FailurePathTests.cs`
- Create: `tests/Rebus.InProcCors.Tests/IsolationTests.cs`

**Interfaces:**
- Consumes: everything from Tasks 2-6.
- Produces: `TestModule`, a per-test helper owning one `IServiceCollection`, one `ServiceProvider` and one bus on a shared `InProcNetwork`, disposable.

- [ ] **Step 1: Write the test module helper**

`tests/Rebus.InProcCors.Tests/TestModule.cs`. This is fixture code, not a test, so it precedes the tests.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Retry.Simple;

namespace Rebus.InProcCors.Tests;

/// <summary>
/// One module: its own container, its own bus, on a shared network. Mirrors the per-module DI isolation
/// the README describes - the caller cannot reach into another module's registrations.
/// </summary>
sealed class TestModule : IDisposable
{
    readonly ServiceProvider _provider;
    readonly BuiltinHandlerActivator _activator;

    TestModule(ServiceProvider provider, BuiltinHandlerActivator activator, IBus bus)
    {
        _provider = provider;
        _activator = activator;
        Bus = bus;
    }

    public IBus Bus { get; }

    public IServiceProvider Services => _provider;

    public static TestModule Create(
        InProcNetwork network,
        string inputQueue,
        InProcReceiveMode mode,
        Action<IServiceCollection>? configureServices = null,
        Action<BuiltinHandlerActivator>? configureHandlers = null,
        Action<OptionsConfigurer>? configureOptions = null,
        int maxDeliveryAttempts = 5)
    {
        var services = new ServiceCollection();
        configureServices?.Invoke(services);
        var provider = services.BuildServiceProvider();

        var activator = new BuiltinHandlerActivator();
        activator.UseServiceProvider(provider);
        configureHandlers?.Invoke(activator);

        var bus = Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, inputQueue, o =>
            {
                o.ReceiveMode = mode;
                o.PollingInterval = TimeSpan.FromMilliseconds(50);
            }))
            .Options(o =>
            {
                o.RetryStrategy(maxDeliveryAttempts: maxDeliveryAttempts, errorQueueName: "error");
                configureOptions?.Invoke(o);
            })
            .Start();

        return new TestModule(provider, activator, bus);
    }

    public void Dispose()
    {
        _activator.Dispose();
        _provider.Dispose();
    }
}
```

`activator.UseServiceProvider(provider)` comes from `Rebus.Activation`. If that overload is not present in
Rebus 8.9.2's `BuiltinHandlerActivator`, register handlers through `activator.Register<THandler>(() => ...)`
resolving from `provider` instead, and adjust the `IsolationTests` accordingly — the point of the helper is
that each module resolves handlers from its own provider, not the exact API used to do it.

`RetryStrategy(...)` lives in `Rebus.Retry.Simple`. Confirm the parameter names against
`Rebus/Config/OptionsConfigurerExtensions.cs` for 8.9.2 and adjust if they differ.

- [ ] **Step 2: Write the reference-delivery tests**

`tests/Rebus.InProcCors.Tests/ReferenceDeliveryTests.cs`:

```csharp
using System.Diagnostics;
using Rebus.Async.Config;
using Rebus.Activation;
using Rebus.Config;

namespace Rebus.InProcCors.Tests;

public class ReferenceDeliveryTests
{
    public sealed record PlaceOrder(string Sku, int Quantity);

    public sealed record OrderPlaced(Guid OrderId);

    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task TheHandlerReceivesTheSameInstanceTheCallerSent(InProcReceiveMode mode)
    {
        // Design §11 item 1 - the whole point of the transport.
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var module = TestModule.Create(network, "orders", mode,
            configureHandlers: a => a.Handle<PlaceOrder>(async m => received.TrySetResult(m)));

        var sent = new PlaceOrder("ABC", 2);
        await module.Bus.SendLocal(sent);

        Assert.Same(sent, await received.Task.WaitAsync(Timeout));
    }

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task ADeferredMessageKeepsItsReferenceAcrossTheTimeoutManager(InProcReceiveMode mode)
    {
        // Design §11 item 3. DueMessage.cs:51 rebuilds the transport message as
        // new TransportMessage(Headers, Body), so the subclass is gone and only the weak table can answer.
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<PlaceOrder>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var module = TestModule.Create(network, "orders", mode,
            configureHandlers: a => a.Handle<PlaceOrder>(async m => received.TrySetResult(m)),
            configureOptions: o => o.UseInMemoryTimeoutManager());

        var sent = new PlaceOrder("ABC", 2);
        await module.Bus.Defer(TimeSpan.FromMilliseconds(200), sent);

        Assert.Same(sent, await received.Task.WaitAsync(Timeout));
    }

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task SendRequestReturnsTheIdenticalReplyInstance(InProcReceiveMode mode)
    {
        // Design §11 item 4 and §2.5 - ReplyHandlerStep works on the deserialized Message and never
        // touches TransportMessage, so it is transparent to this transport.
        var network = new InProcNetwork();
        var reply = new OrderPlaced(Guid.NewGuid());

        using var replier = TestModule.Create(network, "orders", mode,
            configureHandlers: a => a.Handle<PlaceOrder>(async (bus, _) => await bus.Reply(reply)));

        using var activator = new BuiltinHandlerActivator();
        var requestor = Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, "requestor", o => o.ReceiveMode = mode))
            .Routing(r => r.TypeBased().Map<PlaceOrder>("orders"))
            .Options(o => o.EnableSynchronousRequestReply())
            .Start();

        var result = await requestor.SendRequest<OrderPlaced>(new PlaceOrder("ABC", 2), timeout: Timeout);

        Assert.Same(reply, result);
    }

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task SendReturnsBeforeTheHandlerRunsAndAHandlerExceptionDoesNotSurfaceAtTheCallSite(
        InProcReceiveMode mode)
    {
        // Design §11 item 7. Dispatch is asynchronous handoff, not inline invocation - this is where the
        // MediatR analogy stops, and it is what AllowSynchronousContinuations = false protects.
        var network = new InProcNetwork();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handlerEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var module = TestModule.Create(network, "orders", mode, maxDeliveryAttempts: 1,
            configureHandlers: a => a.Handle<PlaceOrder>(async _ =>
            {
                handlerEntered.TrySetResult();
                await gate.Task;
                throw new InvalidOperationException("handler blew up");
            }));

        // Send completes without waiting for, or observing, the handler.
        await module.Bus.SendLocal(new PlaceOrder("ABC", 2));

        await handlerEntered.Task.WaitAsync(Timeout);
        gate.SetResult();

        // Give the handler time to throw. Nothing propagates back here.
        await Task.Delay(200);
    }

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task BlockingModeWakesFasterThanTheBackoffLadderAfterAnIdlePeriod(InProcReceiveMode mode)
    {
        // Design §2.6: Rebus's default ladder is 100 ms for the first ten seconds of idleness. The penalty
        // lands on the first message after idling, which is the dominant shape of a modular monolith.
        // Asserted loosely - this is a smoke test that both modes deliver, with the real measurement in
        // the benchmark's bursty scenario.
        var network = new InProcNetwork();
        var received = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var module = TestModule.Create(network, "orders", mode,
            configureHandlers: a => a.Handle<PlaceOrder>(async _ => received.TrySetResult(Stopwatch.GetTimestamp())));

        await Task.Delay(TimeSpan.FromSeconds(1));  // go idle

        var sentAt = Stopwatch.GetTimestamp();
        await module.Bus.SendLocal(new PlaceOrder("ABC", 2));
        var handledAt = await received.Task.WaitAsync(Timeout);

        var latency = Stopwatch.GetElapsedTime(sentAt, handledAt);
        Assert.True(latency < TimeSpan.FromSeconds(2), $"first-message latency was {latency.TotalMilliseconds} ms");
    }
}
```

`UseInMemoryTimeoutManager` lives in `Rebus.Config` (`Rebus.Timeouts`). `EnableSynchronousRequestReply` and
`SendRequest` come from `Rebus.Async` — confirm the namespaces against the installed package and fix the
`using` directives; the package's own README names them.

- [ ] **Step 3: Write the failure-path tests**

`tests/Rebus.InProcCors.Tests/FailurePathTests.cs`:

```csharp
using Rebus.Messages;
using Rebus.Serialization;
using Rebus.Transport;

namespace Rebus.InProcCors.Tests;

public class FailurePathTests
{
    public sealed record PlaceOrder(string Sku, int Quantity);

    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task AThrowingHandlerRetriesAndTheMessageIsStillReadableInTheErrorQueue(InProcReceiveMode mode)
    {
        // Design §11 item 2, and the correction of §2.2: DeadletterQueueErrorHandler calls Clone(), which
        // discards the subclass. Only the weak table can recover the instance here, so this test is the
        // one that proves the fallback is load-bearing rather than defensive.
        var network = new InProcNetwork();
        var attempts = 0;

        using var module = TestModule.Create(network, "orders", mode, maxDeliveryAttempts: 3,
            configureHandlers: a => a.Handle<PlaceOrder>(async _ =>
            {
                Interlocked.Increment(ref attempts);
                throw new InvalidOperationException("handler blew up");
            }));

        var sent = new PlaceOrder("ABC", 2);
        await module.Bus.SendLocal(sent);

        await WaitUntil(() => network.GetCount("error") == 1, Timeout);
        Assert.Equal(3, Volatile.Read(ref attempts));

        // Read it out of the error queue and deserialize it, exactly as a replay tool would.
        var errorTransport = new InProcTransport(network, "error");
        errorTransport.Initialize();
        using var scope = new RebusTransactionScope();
        var deadLettered = await errorTransport.Receive(scope.TransactionContext, CancellationToken.None);
        await scope.CompleteAsync();

        Assert.NotNull(deadLettered);
        Assert.IsNotType<ReferenceTransportMessage>(deadLettered);  // Clone() dropped the subclass

        var serializer = new ReferenceSerializer(new SimpleTypeNameConvention());
        var recovered = await serializer.Deserialize(deadLettered!);

        Assert.Same(sent, recovered.Body);
        Assert.True(deadLettered!.Headers.ContainsKey(Headers.ErrorDetails));
    }

    [Fact]
    public async Task DeserializingAMessageWhoseBodyWasReplacedThrowsWithAUsefulMessage()
    {
        // Design §11 item 10.
        var serializer = new ReferenceSerializer(new SimpleTypeNameConvention());
        var headers = new Dictionary<string, string> { [Headers.Type] = typeof(PlaceOrder).FullName! };

        var exception = await Assert.ThrowsAsync<InProcReferenceLostException>(
            () => serializer.Deserialize(new TransportMessage(headers, new byte[] { 9, 9, 9 })));

        Assert.Contains(typeof(PlaceOrder).FullName!, exception.Message);
        Assert.Contains("ReferenceSerializer", exception.Message);
    }

    static async Task WaitUntil(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(25);
        }

        throw new TimeoutException($"Condition was not satisfied within {timeout}.");
    }

    sealed class SimpleTypeNameConvention : IMessageTypeNameConvention
    {
        public string GetTypeName(Type type) => type.FullName!;
        public Type GetType(string name) => Type.GetType(name)!;
    }
}
```

`Headers.ErrorDetails` is the constant Rebus writes on dead-lettering. If the name differs in 8.9.2, check
`Rebus/Messages/Headers.cs` and use whatever key `DeadletterQueueErrorHandler` sets.

- [ ] **Step 4: Write the isolation tests**

`tests/Rebus.InProcCors.Tests/IsolationTests.cs`:

```csharp
using Microsoft.Extensions.DependencyInjection;

namespace Rebus.InProcCors.Tests;

public class IsolationTests
{
    public sealed record PlaceOrder(string Sku);

    sealed class ModuleSecret
    {
        public string Value { get; init; } = "";
    }

    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task TwoNetworksDoNotObserveEachOthersTraffic(InProcReceiveMode mode)
    {
        // Design §11 item 5, and why there is no static default network (§5).
        var networkA = new InProcNetwork();
        var networkB = new InProcNetwork();
        var receivedOnB = false;
        var receivedOnA = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var moduleA = TestModule.Create(networkA, "orders", mode,
            configureHandlers: a => a.Handle<PlaceOrder>(async _ => receivedOnA.TrySetResult()));
        using var moduleB = TestModule.Create(networkB, "orders", mode,
            configureHandlers: a => a.Handle<PlaceOrder>(async _ => receivedOnB = true));

        await moduleA.Bus.SendLocal(new PlaceOrder("ABC"));
        await receivedOnA.Task.WaitAsync(Timeout);
        await Task.Delay(200);

        Assert.False(receivedOnB);
    }

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task AHandlerCannotSeeTheCallersRegistrations(InProcReceiveMode mode)
    {
        // Design §11 item 6. This is the difference from MediatR that the README says is the entire point:
        // the handler is resolved from its own module's container, so the caller cannot leak a DbContext,
        // an ambient transaction, or any of its own registrations into it.
        var network = new InProcNetwork();
        var observed = new TaskCompletionSource<ModuleSecret?>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var caller = TestModule.Create(network, "caller", mode,
            configureServices: s => s.AddSingleton(new ModuleSecret { Value = "caller-secret" }));

        using var handlerModule = TestModule.Create(network, "orders", mode,
            configureHandlers: a => a.Handle<PlaceOrder>(async _ => { }));

        Assert.Equal("caller-secret", caller.Services.GetRequiredService<ModuleSecret>().Value);
        Assert.Null(handlerModule.Services.GetService<ModuleSecret>());

        await Task.CompletedTask;
        observed.TrySetResult(handlerModule.Services.GetService<ModuleSecret>());
        Assert.Null(await observed.Task);
    }
}
```

- [ ] **Step 5: Run the full suite**

Run: `dotnet test`
Expected: PASS. Every `[Theory]` above contributes two cases, one per receive mode.

These are timing-sensitive integration tests. If one is flaky, raise its timeout — do **not** add
`Task.Delay` to make an assertion pass, and do not weaken `Assert.Same` to `Assert.Equal`. `Assert.Same` is
the assertion the project exists to satisfy.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "test: integration tests proving reference delivery, dead-letter fallback, and module isolation"
```

---

## Task 8: Verification project scaffold, report model, and the immutability check

Design §10 "Check 2 — deep immutability" and the escape hatch. Self-contained reflection, no Rebus and no DI
involved, so this task is testable in isolation.

**Files:**
- Create: `src/Rebus.InProcCors.Verification/Rebus.InProcCors.Verification.csproj`
- Create: `src/Rebus.InProcCors.Verification/ImmutabilityExemptAttribute.cs`
- Create: `src/Rebus.InProcCors.Verification/VerificationReport.cs`
- Create: `src/Rebus.InProcCors.Verification/ImmutabilityChecker.cs`
- Create: `tests/Rebus.InProcCors.Verification.Tests/Rebus.InProcCors.Verification.Tests.csproj`
- Test: `tests/Rebus.InProcCors.Verification.Tests/ImmutabilityCheckerTests.cs`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `public sealed class ImmutabilityExemptAttribute : Attribute`, ctor `(string reason)`, property `string Reason { get; }`.
  - `public enum VerificationCheck { Construction, RoundTrip, Immutability }`.
  - `public sealed record VerificationViolation(Type MessageType, VerificationCheck Check, string Member, string Description)`.
  - `public sealed record VerificationExemption(Type MessageType, string Member, string Reason)`.
  - `public sealed class VerificationReport` with `IReadOnlyList<Type> VerifiedTypes`, `IReadOnlyList<VerificationViolation> Violations`, `IReadOnlyList<VerificationExemption> Exemptions`, `bool IsSuccess`, `string Describe()`.
  - `public sealed class ImmutabilityChecker` with `void Check(Type messageType, ICollection<VerificationViolation> violations, ICollection<VerificationExemption> exemptions)`.

- [ ] **Step 1: Create the projects**

`src/Rebus.InProcCors.Verification/Rebus.InProcCors.Verification.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFrameworks>net8.0;net9.0</TargetFrameworks>
    <GenerateDocumentationFile>true</GenerateDocumentationFile>
    <PackageId>Rebus.InProcCors.Verification</PackageId>
    <Description>Verifies that Rebus message contracts are round-trip serializable and deeply immutable.</Description>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Rebus" />
    <PackageReference Include="Microsoft.Extensions.DependencyInjection.Abstractions" />
    <PackageReference Include="Microsoft.Extensions.Hosting.Abstractions" />
    <PackageReference Include="Microsoft.Extensions.Logging.Abstractions" />
  </ItemGroup>

  <ItemGroup>
    <InternalsVisibleTo Include="Rebus.InProcCors.Verification.Tests" />
  </ItemGroup>
</Project>
```

`tests/Rebus.InProcCors.Verification.Tests/Rebus.InProcCors.Verification.Tests.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net9.0</TargetFramework>
    <IsPackable>false</IsPackable>
    <NoWarn>$(NoWarn);CS1591</NoWarn>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" />
    <PackageReference Include="xunit" />
    <PackageReference Include="xunit.runner.visualstudio" />
    <PackageReference Include="Microsoft.Extensions.DependencyInjection" />
    <PackageReference Include="Microsoft.Extensions.Hosting" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="../../src/Rebus.InProcCors.Verification/Rebus.InProcCors.Verification.csproj" />
  </ItemGroup>
</Project>
```

Then:

```bash
dotnet sln add src/Rebus.InProcCors.Verification/Rebus.InProcCors.Verification.csproj
dotnet sln add tests/Rebus.InProcCors.Verification.Tests/Rebus.InProcCors.Verification.Tests.csproj
```

- [ ] **Step 2: Write the failing tests**

`tests/Rebus.InProcCors.Verification.Tests/ImmutabilityCheckerTests.cs`:

```csharp
using System.Collections.Immutable;

namespace Rebus.InProcCors.Verification.Tests;

public class ImmutabilityCheckerTests
{
    sealed record GoodOrder(string Sku, int Quantity, IReadOnlyList<string> Tags);

    sealed record OrderWithImmutableArray(ImmutableArray<string> Tags);

    sealed class MutableProperty
    {
        public string Sku { get; set; } = "";
    }

    sealed class PrivateSetter
    {
        public string Sku { get; private set; } = "";
    }

    sealed class PublicWritableField
    {
        public string Sku = "";
    }

    sealed class ReadonlyField
    {
        public readonly string Sku = "";
    }

    sealed record MutableCollectionMember(List<string> Tags);

    sealed record ArrayMember(string[] Tags);

    sealed record DictionaryMember(Dictionary<string, string> Attributes);

    sealed record ReadOnlyDictionaryMember(IReadOnlyDictionary<string, string> Attributes);

    sealed record Outer(Inner Inner);

    sealed class Inner
    {
        public string Value { get; set; } = "";
    }

    sealed record Node(string Name, Node? Next);

    sealed record ScalarTerminators(
        Guid Id, DateTime At, DateTimeOffset AtOffset, decimal Amount, TimeSpan Duration, Uri Link, DayOfWeek Day,
        int? MaybeCount);

    [ImmutabilityExempt("Legacy contract owned by the billing team, tracked in TICKET-42.")]
    sealed class ExemptedType
    {
        public string Sku { get; set; } = "";
    }

    sealed class TypeWithExemptedMember
    {
        public string Sku { get; init; } = "";

        [ImmutabilityExempt("Interop buffer handed straight to the native layer, tracked in TICKET-43.")]
        public List<string> Buffer { get; set; } = new();
    }

    static (List<VerificationViolation> Violations, List<VerificationExemption> Exemptions) Check<T>()
    {
        var violations = new List<VerificationViolation>();
        var exemptions = new List<VerificationExemption>();
        new ImmutabilityChecker().Check(typeof(T), violations, exemptions);
        return (violations, exemptions);
    }

    [Fact]
    public void AGetOnlyRecordWithReadOnlyCollectionsPasses()
    {
        var (violations, exemptions) = Check<GoodOrder>();

        Assert.Empty(violations);
        Assert.Empty(exemptions);
    }

    [Fact]
    public void ImmutableArrayIsAcceptedDespiteImplementingIList()
    {
        Assert.Empty(Check<OrderWithImmutableArray>().Violations);
    }

    [Fact]
    public void ScalarTypesTerminateTheWalk()
    {
        Assert.Empty(Check<ScalarTerminators>().Violations);
    }

    [Fact]
    public void APublicSetterIsAViolation()
    {
        var violation = Assert.Single(Check<MutableProperty>().Violations);

        Assert.Equal(VerificationCheck.Immutability, violation.Check);
        Assert.Equal(nameof(MutableProperty.Sku), violation.Member);
    }

    [Fact]
    public void APrivateSetterIsNotAViolationBecauseItIsNotPubliclyWritable()
    {
        Assert.Empty(Check<PrivateSetter>().Violations);
    }

    [Fact]
    public void APublicWritableFieldIsAViolation()
    {
        var violation = Assert.Single(Check<PublicWritableField>().Violations);
        Assert.Equal(nameof(PublicWritableField.Sku), violation.Member);
    }

    [Fact]
    public void APublicReadonlyFieldIsNotAViolation()
    {
        Assert.Empty(Check<ReadonlyField>().Violations);
    }

    [Theory]
    [InlineData(typeof(MutableCollectionMember))]
    [InlineData(typeof(ArrayMember))]
    [InlineData(typeof(DictionaryMember))]
    public void MutableCollectionMembersAreViolations(Type messageType)
    {
        var violations = new List<VerificationViolation>();
        new ImmutabilityChecker().Check(messageType, violations, new List<VerificationExemption>());

        Assert.Single(violations);
    }

    [Fact]
    public void ReadOnlyDictionaryMembersAreAccepted()
    {
        Assert.Empty(Check<ReadOnlyDictionaryMember>().Violations);
    }

    [Fact]
    public void TheWalkRecursesTransitively()
    {
        var violation = Assert.Single(Check<Outer>().Violations);

        Assert.Contains(nameof(Inner.Value), violation.Member);
        Assert.Contains(nameof(Inner), violation.Member);
    }

    [Fact]
    public void ACycleTerminatesInsteadOfOverflowing()
    {
        Assert.Empty(Check<Node>().Violations);
    }

    [Fact]
    public void AnExemptedTypeIsListedRatherThanFailed()
    {
        var (violations, exemptions) = Check<ExemptedType>();

        Assert.Empty(violations);
        var exemption = Assert.Single(exemptions);
        Assert.Contains("TICKET-42", exemption.Reason);
    }

    [Fact]
    public void AnExemptedMemberIsListedRatherThanFailed()
    {
        var (violations, exemptions) = Check<TypeWithExemptedMember>();

        Assert.Empty(violations);
        var exemption = Assert.Single(exemptions);
        Assert.Equal(nameof(TypeWithExemptedMember.Buffer), exemption.Member);
        Assert.Contains("TICKET-43", exemption.Reason);
    }

    [Fact]
    public void AnEmptyExemptionReasonIsRejectedAtConstruction()
    {
        Assert.Throws<ArgumentException>(() => new ImmutabilityExemptAttribute("  "));
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `dotnet test tests/Rebus.InProcCors.Verification.Tests`
Expected: FAIL to build — none of the types exist.

- [ ] **Step 4: Write `ImmutabilityExemptAttribute.cs`**

```csharp
namespace Rebus.InProcCors.Verification;

/// <summary>
/// Exempts a type or member from the deep-immutability check. The reason is mandatory and non-empty:
/// exemptions do not fail the report, they are listed in it, so they appear in test output on every run
/// and in code review as an attribute carrying a written justification (design §10).
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Property | AttributeTargets.Field,
    Inherited = false)]
public sealed class ImmutabilityExemptAttribute : Attribute
{
    /// <summary>
    /// Creates the exemption with a mandatory justification.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when <paramref name="reason"/> is null, empty or whitespace.</exception>
    public ImmutabilityExemptAttribute(string reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
        {
            throw new ArgumentException(
                "An immutability exemption must carry a non-empty reason. It is going to be printed in test " +
                "output on every run and read in code review, so write the justification down.", nameof(reason));
        }

        Reason = reason;
    }

    /// <summary>
    /// Gets the written justification for this exemption.
    /// </summary>
    public string Reason { get; }
}
```

- [ ] **Step 5: Write `VerificationReport.cs`**

```csharp
using System.Text;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Which check produced a violation.
/// </summary>
public enum VerificationCheck
{
    /// <summary>A test instance of the contract could not be constructed.</summary>
    Construction,

    /// <summary>Serialize, deserialize, serialize again did not produce identical bytes.</summary>
    RoundTrip,

    /// <summary>The contract's reachable object graph is not deeply immutable.</summary>
    Immutability
}

/// <summary>
/// One failed check against one message contract.
/// </summary>
/// <param name="MessageType">The contract that failed.</param>
/// <param name="Check">Which check failed.</param>
/// <param name="Member">The offending member path, or an empty string when the whole type failed.</param>
/// <param name="Description">What is wrong, in a sentence.</param>
public sealed record VerificationViolation(Type MessageType, VerificationCheck Check, string Member, string Description);

/// <summary>
/// One <see cref="ImmutabilityExemptAttribute"/> encountered during the walk.
/// </summary>
/// <param name="MessageType">The contract the exemption was found on or under.</param>
/// <param name="Member">The exempted member path, or an empty string when the whole type is exempt.</param>
/// <param name="Reason">The written justification.</param>
public sealed record VerificationExemption(Type MessageType, string Member, string Reason);

/// <summary>
/// The outcome of verifying a set of message contracts.
/// </summary>
public sealed class VerificationReport
{
    /// <summary>
    /// Creates the report.
    /// </summary>
    public VerificationReport(
        IReadOnlyList<Type> verifiedTypes,
        IReadOnlyList<VerificationViolation> violations,
        IReadOnlyList<VerificationExemption> exemptions)
    {
        VerifiedTypes = verifiedTypes;
        Violations = violations;
        Exemptions = exemptions;
    }

    /// <summary>Gets the contracts that were checked.</summary>
    public IReadOnlyList<Type> VerifiedTypes { get; }

    /// <summary>Gets the failed checks.</summary>
    public IReadOnlyList<VerificationViolation> Violations { get; }

    /// <summary>Gets the exemptions encountered. These do not fail the report.</summary>
    public IReadOnlyList<VerificationExemption> Exemptions { get; }

    /// <summary>Gets whether every contract passed every check.</summary>
    public bool IsSuccess => Violations.Count == 0;

    /// <summary>
    /// Renders the report as human-readable text, suitable for a test failure message or a log entry.
    /// </summary>
    public string Describe()
    {
        var builder = new StringBuilder();

        builder.Append("Verified ").Append(VerifiedTypes.Count).AppendLine(" message contract(s).");

        if (Violations.Count == 0)
        {
            builder.AppendLine("No violations.");
        }
        else
        {
            builder.Append(Violations.Count).AppendLine(" violation(s):");

            foreach (var violation in Violations)
            {
                builder.Append("  [").Append(violation.Check).Append("] ").Append(violation.MessageType.FullName);

                if (!string.IsNullOrEmpty(violation.Member)) builder.Append('.').Append(violation.Member);

                builder.Append(" - ").AppendLine(violation.Description);
            }
        }

        if (Exemptions.Count > 0)
        {
            builder.Append(Exemptions.Count).AppendLine(" exemption(s), listed but not failed:");

            foreach (var exemption in Exemptions)
            {
                builder.Append("  ").Append(exemption.MessageType.FullName);

                if (!string.IsNullOrEmpty(exemption.Member)) builder.Append('.').Append(exemption.Member);

                builder.Append(" - ").AppendLine(exemption.Reason);
            }
        }

        return builder.ToString();
    }
}
```

- [ ] **Step 6: Write `ImmutabilityChecker.cs`**

```csharp
using System.Collections;
using System.Collections.Immutable;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Walks a contract's reachable object graph and reports everything that is publicly mutable.
/// Design §10 "Check 2".
/// </summary>
public sealed class ImmutabilityChecker
{
    static readonly HashSet<Type> TerminatingTypes =
    [
        typeof(string), typeof(decimal), typeof(Guid), typeof(DateTime), typeof(DateTimeOffset),
        typeof(TimeSpan), typeof(Uri), typeof(object), typeof(Type)
    ];

    static readonly HashSet<Type> MutableCollectionDefinitions =
    [
        typeof(List<>), typeof(Dictionary<,>), typeof(HashSet<>), typeof(Queue<>), typeof(Stack<>),
        typeof(SortedList<,>), typeof(SortedDictionary<,>),
        typeof(ICollection<>), typeof(IList<>), typeof(IDictionary<,>), typeof(ISet<>)
    ];

    static readonly HashSet<Type> AcceptedCollectionDefinitions =
    [
        typeof(IReadOnlyList<>), typeof(IReadOnlyCollection<>), typeof(IReadOnlyDictionary<,>),
        typeof(IEnumerable<>), typeof(ImmutableArray<>), typeof(ImmutableList<>), typeof(ImmutableDictionary<,>),
        typeof(ImmutableHashSet<>), typeof(ImmutableSortedSet<>), typeof(ImmutableSortedDictionary<,>),
        typeof(KeyValuePair<,>)
    ];

    /// <summary>
    /// Checks <paramref name="messageType"/>, appending anything it finds to <paramref name="violations"/>
    /// and <paramref name="exemptions"/>.
    /// </summary>
    public void Check(
        Type messageType,
        ICollection<VerificationViolation> violations,
        ICollection<VerificationExemption> exemptions)
    {
        if (messageType == null) throw new ArgumentNullException(nameof(messageType));

        Walk(messageType, messageType, memberPath: "", new HashSet<Type>(), violations, exemptions);
    }

    void Walk(
        Type rootType,
        Type currentType,
        string memberPath,
        HashSet<Type> visited,
        ICollection<VerificationViolation> violations,
        ICollection<VerificationExemption> exemptions)
    {
        currentType = Nullable.GetUnderlyingType(currentType) ?? currentType;

        if (IsTerminating(currentType)) return;

        if (TryGetElementType(currentType, out var elementType))
        {
            Walk(rootType, elementType!, memberPath, visited, violations, exemptions);
            return;
        }

        if (!visited.Add(currentType)) return;   // cycle

        if (TryGetExemption(currentType, out var typeReason))
        {
            exemptions.Add(new VerificationExemption(rootType, memberPath, typeReason!));
            return;
        }

        foreach (var property in currentType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (property.GetIndexParameters().Length > 0) continue;

            var path = Combine(memberPath, currentType, property.Name, rootType);

            if (TryGetExemption(property, out var propertyReason))
            {
                exemptions.Add(new VerificationExemption(rootType, path, propertyReason!));
                continue;
            }

            if (IsPubliclyWritable(property))
            {
                violations.Add(new VerificationViolation(rootType, VerificationCheck.Immutability, path,
                    "the property has a public setter; make it get-only or init-only"));
                continue;
            }

            if (CheckMemberType(rootType, property.PropertyType, path, violations)) continue;

            Walk(rootType, property.PropertyType, path, visited, violations, exemptions);
        }

        foreach (var field in currentType.GetFields(BindingFlags.Public | BindingFlags.Instance))
        {
            var path = Combine(memberPath, currentType, field.Name, rootType);

            if (TryGetExemption(field, out var fieldReason))
            {
                exemptions.Add(new VerificationExemption(rootType, path, fieldReason!));
                continue;
            }

            if (!field.IsInitOnly)
            {
                violations.Add(new VerificationViolation(rootType, VerificationCheck.Immutability, path,
                    "the field is publicly writable; make it readonly, or expose it as a get-only property"));
                continue;
            }

            if (CheckMemberType(rootType, field.FieldType, path, violations)) continue;

            Walk(rootType, field.FieldType, path, visited, violations, exemptions);
        }

        visited.Remove(currentType);
    }

    static string Combine(string memberPath, Type declaringType, string memberName, Type rootType)
    {
        var name = declaringType == rootType ? memberName : $"{declaringType.Name}.{memberName}";

        return string.IsNullOrEmpty(memberPath) ? name : $"{memberPath} -> {name}";
    }

    /// <summary>
    /// Returns true when the member's type is itself the problem, so the caller should not recurse into it.
    /// </summary>
    static bool CheckMemberType(
        Type rootType, Type memberType, string path, ICollection<VerificationViolation> violations)
    {
        memberType = Nullable.GetUnderlyingType(memberType) ?? memberType;

        if (!IsMutableCollection(memberType)) return false;

        violations.Add(new VerificationViolation(rootType, VerificationCheck.Immutability, path,
            $"the member is typed as the mutable collection '{Describe(memberType)}'; use IReadOnlyList<>, " +
            "IReadOnlyDictionary<> or ImmutableArray<> instead"));

        return true;
    }

    static bool IsTerminating(Type type) =>
        type.IsPrimitive || type.IsEnum || TerminatingTypes.Contains(type);

    static bool IsMutableCollection(Type type)
    {
        if (type == typeof(string)) return false;
        if (type.IsArray) return true;

        if (type.IsConstructedGenericType)
        {
            var definition = type.GetGenericTypeDefinition();

            // Accepted definitions win outright: ImmutableArray<T> implements IList<T> explicitly, so the
            // interface test below would otherwise reject the very type the design recommends.
            if (AcceptedCollectionDefinitions.Contains(definition)) return false;
            if (MutableCollectionDefinitions.Contains(definition)) return true;
        }

        return type.GetInterfaces().Any(i => i.IsConstructedGenericType
                                             && MutableCollectionDefinitions.Contains(i.GetGenericTypeDefinition()))
               || typeof(IList).IsAssignableFrom(type)
               || typeof(IDictionary).IsAssignableFrom(type);
    }

    /// <summary>
    /// Gets the element type to recurse into for an accepted read-only collection, so that a
    /// <c>IReadOnlyList&lt;Mutable&gt;</c> is still caught.
    /// </summary>
    static bool TryGetElementType(Type type, out Type? elementType)
    {
        elementType = null;

        if (type == typeof(string)) return false;

        if (type.IsConstructedGenericType && AcceptedCollectionDefinitions.Contains(type.GetGenericTypeDefinition()))
        {
            var arguments = type.GetGenericArguments();
            elementType = arguments[arguments.Length - 1];   // value type for dictionaries, element otherwise
            return true;
        }

        return false;
    }

    static bool IsPubliclyWritable(PropertyInfo property)
    {
        var setter = property.SetMethod;

        if (setter == null || !setter.IsPublic) return false;

        // An init accessor is a setter whose return parameter carries a required custom modifier for
        // IsExternalInit. Without this every record reads as mutable and the check is worthless (design §10).
        return !setter.ReturnParameter.GetRequiredCustomModifiers().Contains(typeof(IsExternalInit));
    }

    static bool TryGetExemption(MemberInfo member, out string? reason)
    {
        var attribute = member.GetCustomAttribute<ImmutabilityExemptAttribute>(inherit: false);
        reason = attribute?.Reason;
        return attribute != null;
    }

    static string Describe(Type type) =>
        type.IsConstructedGenericType
            ? $"{type.Name.Split('`')[0]}<{string.Join(", ", type.GetGenericArguments().Select(a => a.Name))}>"
            : type.Name;
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `dotnet test tests/Rebus.InProcCors.Verification.Tests --filter FullyQualifiedName~ImmutabilityCheckerTests`
Expected: PASS, 16 tests.

`TheWalkRecursesTransitively` asserts the member path names both the nested type and the offending property.
If `Combine` produces a different shape than the test expects, adjust the *test's* assertions to match the
path format you implemented — but keep both names in the path, because a violation that says only "Value" is
useless in a report covering thirty contracts.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: verification project, report model, deep-immutability checker"
```

---

## Task 9: Instance factory, contract serializer, and the round-trip check

Design §10 "Check 1", "Instance construction", and correction **C2**.

**Files:**
- Create: `src/Rebus.InProcCors.Verification/IMessageInstanceSource.cs`
- Create: `src/Rebus.InProcCors.Verification/DefaultMessageInstanceFactory.cs`
- Create: `src/Rebus.InProcCors.Verification/SystemTextJsonContractSerializer.cs`
- Create: `src/Rebus.InProcCors.Verification/RoundTripChecker.cs`
- Test: `tests/Rebus.InProcCors.Verification.Tests/DefaultMessageInstanceFactoryTests.cs`
- Test: `tests/Rebus.InProcCors.Verification.Tests/RoundTripCheckerTests.cs`

**Interfaces:**
- Consumes: `VerificationViolation`, `VerificationCheck` (Task 8).
- Produces:
  - `public interface IMessageInstanceSource { bool TryCreate(Type messageType, out object? instance); }`
  - `public sealed class DefaultMessageInstanceFactory : IMessageInstanceSource`, ctor `()`.
  - `public sealed class SystemTextJsonContractSerializer : ISerializer`, ctor `(IMessageTypeNameConvention? convention = null)`.
  - `public sealed class RoundTripChecker`, ctor `(ISerializer serializer)`, method `Task CheckAsync(Type messageType, object instance, ICollection<VerificationViolation> violations)`.

- [ ] **Step 1: Write the failing tests**

`tests/Rebus.InProcCors.Verification.Tests/DefaultMessageInstanceFactoryTests.cs`:

```csharp
namespace Rebus.InProcCors.Verification.Tests;

public class DefaultMessageInstanceFactoryTests
{
    sealed record Scalars(
        string Text, int Count, long Big, bool Flag, double Rate, decimal Amount,
        Guid Id, DateTime At, DateTimeOffset AtOffset, TimeSpan Duration, Uri Link, DayOfWeek Day);

    sealed record WithNullable(int? Count, string? Text);

    sealed record WithCollection(IReadOnlyList<string> Tags);

    sealed record Nested(Scalars Inner);

    sealed record Cyclic(string Name, Cyclic? Next);

    sealed class NoPublicConstructor
    {
        NoPublicConstructor() { }
    }

    sealed record TwoConstructors
    {
        public TwoConstructors(string a) : this(a, 0) { }

        public TwoConstructors(string a, int b)
        {
            A = a;
            B = b;
        }

        public string A { get; }
        public int B { get; }
    }

    static object Create<T>()
    {
        Assert.True(new DefaultMessageInstanceFactory().TryCreate(typeof(T), out var instance));
        Assert.NotNull(instance);
        return instance!;
    }

    [Fact]
    public void EveryScalarGetsANonDefaultValue()
    {
        // Non-default values are load-bearing: filled with defaults, a property the serializer silently
        // drops is invisible - default in s1, default after deserialization, bytes equal, test green.
        var scalars = (Scalars)Create<Scalars>();

        Assert.False(string.IsNullOrEmpty(scalars.Text));
        Assert.NotEqual(0, scalars.Count);
        Assert.NotEqual(0L, scalars.Big);
        Assert.True(scalars.Flag);
        Assert.NotEqual(0d, scalars.Rate);
        Assert.NotEqual(0m, scalars.Amount);
        Assert.NotEqual(Guid.Empty, scalars.Id);
        Assert.NotEqual(default, scalars.At);
        Assert.NotEqual(default, scalars.AtOffset);
        Assert.NotEqual(TimeSpan.Zero, scalars.Duration);
        Assert.NotNull(scalars.Link);
    }

    [Fact]
    public void ConstructionIsDeterministicAcrossInstancesAndProcesses()
    {
        // A flaky serialization test gets deleted, so determinism matters more than variety.
        // String.GetHashCode is randomized per process, so the factory must not use it for its seed.
        var first = (Scalars)Create<Scalars>();
        var second = (Scalars)Create<Scalars>();

        Assert.Equal(first, second);
    }

    [Fact]
    public void DifferentMembersGetDifferentValues()
    {
        var scalars = (Scalars)Create<Scalars>();
        var nested = (Nested)Create<Nested>();

        Assert.NotEqual(scalars.Text, scalars.Id.ToString());
        Assert.NotNull(nested.Inner);
        Assert.False(string.IsNullOrEmpty(nested.Inner.Text));
    }

    [Fact]
    public void NullablesAreFilledRatherThanLeftNull()
    {
        var value = (WithNullable)Create<WithNullable>();

        Assert.NotNull(value.Count);
        Assert.False(string.IsNullOrEmpty(value.Text));
    }

    [Fact]
    public void CollectionsGetAtLeastOneElement()
    {
        var value = (WithCollection)Create<WithCollection>();

        Assert.NotEmpty(value.Tags);
        Assert.False(string.IsNullOrEmpty(value.Tags[0]));
    }

    [Fact]
    public void TheGreediestConstructorIsChosen()
    {
        var value = (TwoConstructors)Create<TwoConstructors>();

        Assert.NotEqual(0, value.B);
    }

    [Fact]
    public void ACycleIsBrokenWithNullRatherThanOverflowing()
    {
        var value = (Cyclic)Create<Cyclic>();

        Assert.False(string.IsNullOrEmpty(value.Name));
        // Some depth is produced, then the chain terminates.
        var depth = 0;
        for (var node = value; node != null; node = node.Next) depth++;
        Assert.InRange(depth, 1, 10);
    }

    [Fact]
    public void ATypeThatCannotBeConstructedIsDeclinedRatherThanThrowing()
    {
        Assert.False(new DefaultMessageInstanceFactory().TryCreate(typeof(NoPublicConstructor), out var instance));
        Assert.Null(instance);
    }
}
```

`tests/Rebus.InProcCors.Verification.Tests/RoundTripCheckerTests.cs`:

```csharp
namespace Rebus.InProcCors.Verification.Tests;

public class RoundTripCheckerTests
{
    public sealed record GoodOrder(string Sku, int Quantity);

    public sealed class DroppedInitProperty
    {
        // No constructor parameter matches Sku, so System.Text.Json cannot populate it on the way back:
        // present in s1, absent in s2.
        public DroppedInitProperty(int quantity) => Quantity = quantity;

        public int Quantity { get; }

        public string Sku { get; init; } = "";
    }

    public sealed class PrivateSetterProperty
    {
        public PrivateSetterProperty(int quantity) => Quantity = quantity;

        public int Quantity { get; }

        public string Sku { get; private set; } = "";
    }

    static async Task<List<VerificationViolation>> Check(Type messageType)
    {
        var violations = new List<VerificationViolation>();
        Assert.True(new DefaultMessageInstanceFactory().TryCreate(messageType, out var instance));

        await new RoundTripChecker(new SystemTextJsonContractSerializer())
            .CheckAsync(messageType, instance!, violations);

        return violations;
    }

    [Fact]
    public async Task AWellFormedRecordRoundTripsToIdenticalBytes()
    {
        Assert.Empty(await Check(typeof(GoodOrder)));
    }

    [Fact]
    public async Task APropertyTheSerializerCannotRestoreIsAViolation()
    {
        var violation = Assert.Single(await Check(typeof(DroppedInitProperty)));

        Assert.Equal(VerificationCheck.RoundTrip, violation.Check);
        Assert.Equal(typeof(DroppedInitProperty), violation.MessageType);
    }

    [Fact]
    public async Task APrivateSetterIsAViolation()
    {
        Assert.Single(await Check(typeof(PrivateSetterProperty)));
    }

    [Fact]
    public async Task ASerializerThatThrowsBecomesAViolationRatherThanAnEscapingException()
    {
        var violations = new List<VerificationViolation>();

        await new RoundTripChecker(new ThrowingSerializer())
            .CheckAsync(typeof(GoodOrder), new GoodOrder("ABC", 2), violations);

        var violation = Assert.Single(violations);
        Assert.Equal(VerificationCheck.RoundTrip, violation.Check);
        Assert.Contains("nope", violation.Description);
    }

    sealed class ThrowingSerializer : Rebus.Serialization.ISerializer
    {
        public Task<Rebus.Messages.TransportMessage> Serialize(Rebus.Messages.Message message) =>
            throw new NotSupportedException("nope");

        public Task<Rebus.Messages.Message> Deserialize(Rebus.Messages.TransportMessage transportMessage) =>
            throw new NotSupportedException("nope");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/Rebus.InProcCors.Verification.Tests`
Expected: FAIL to build — `DefaultMessageInstanceFactory`, `SystemTextJsonContractSerializer` and
`RoundTripChecker` do not exist.

- [ ] **Step 3: Write `IMessageInstanceSource.cs`**

```csharp
namespace Rebus.InProcCors.Verification;

/// <summary>
/// Supplies a test instance for a message contract. Implement this to override construction for a contract
/// that <see cref="DefaultMessageInstanceFactory"/> cannot build, and set it on
/// <c>MessageContractVerificationOptions.InstanceSource</c>; it is consulted first, and the default factory
/// handles whatever it declines.
/// </summary>
public interface IMessageInstanceSource
{
    /// <summary>
    /// Attempts to create an instance of <paramref name="messageType"/>. Return false to decline.
    /// </summary>
    bool TryCreate(Type messageType, out object? instance);
}
```

- [ ] **Step 4: Write `DefaultMessageInstanceFactory.cs`**

```csharp
using System.Collections.Immutable;
using System.Reflection;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Builds a contract instance from its greediest public constructor, filled with deterministic,
/// type-derived, non-default values, recursively. No AutoFixture: determinism matters more than variety,
/// because a flaky serialization test gets deleted (design §10).
/// </summary>
public sealed class DefaultMessageInstanceFactory : IMessageInstanceSource
{
    const int MaxDepth = 5;

    /// <inheritdoc />
    public bool TryCreate(Type messageType, out object? instance)
    {
        if (messageType == null) throw new ArgumentNullException(nameof(messageType));

        try
        {
            instance = Create(messageType, Seed(messageType.FullName ?? messageType.Name), depth: 0);
            return instance != null;
        }
        catch (Exception)
        {
            instance = null;
            return false;
        }
    }

    static object? Create(Type type, uint seed, int depth)
    {
        var underlying = Nullable.GetUnderlyingType(type);
        if (underlying != null) return Create(underlying, seed, depth);

        if (type == typeof(string)) return $"value-{seed % 100000}";
        if (type == typeof(bool)) return true;
        if (type == typeof(byte)) return (byte)(seed % 200 + 1);
        if (type == typeof(sbyte)) return (sbyte)(seed % 100 + 1);
        if (type == typeof(short)) return (short)(seed % 30000 + 1);
        if (type == typeof(ushort)) return (ushort)(seed % 60000 + 1);
        if (type == typeof(int)) return (int)(seed % 1000000 + 1);
        if (type == typeof(uint)) return seed % 1000000 + 1;
        if (type == typeof(long)) return (long)(seed % 1000000 + 1);
        if (type == typeof(ulong)) return (ulong)(seed % 1000000 + 1);
        if (type == typeof(float)) return seed % 1000 + 1.5f;
        if (type == typeof(double)) return seed % 1000 + 1.5d;
        if (type == typeof(decimal)) return seed % 1000 + 1.5m;
        if (type == typeof(char)) return (char)('a' + seed % 26);
        if (type == typeof(Guid)) return DeterministicGuid(seed);
        if (type == typeof(DateTime)) return new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(seed % 100000);
        if (type == typeof(DateTimeOffset)) return new DateTimeOffset(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(seed % 100000), TimeSpan.Zero);
        if (type == typeof(TimeSpan)) return TimeSpan.FromSeconds(seed % 10000 + 1);
        if (type == typeof(Uri)) return new Uri($"https://example.test/{seed % 100000}");

        if (type.IsEnum)
        {
            var values = Enum.GetValues(type);
            // Prefer a non-zero member, so a dropped enum property is visible.
            foreach (var value in values)
            {
                if (Convert.ToInt64(value) != 0) return value;
            }

            return values.Length > 0 ? values.GetValue(0) : Activator.CreateInstance(type);
        }

        if (depth >= MaxDepth) return null;

        if (TryCreateCollection(type, seed, depth, out var collection)) return collection;

        return CreateComplex(type, seed, depth);
    }

    static bool TryCreateCollection(Type type, uint seed, int depth, out object? collection)
    {
        collection = null;

        if (type.IsArray)
        {
            var elementType = type.GetElementType()!;
            var element = Create(elementType, Mix(seed, "element"), depth + 1);
            if (element == null) return false;

            var array = Array.CreateInstance(elementType, 1);
            array.SetValue(element, 0);
            collection = array;
            return true;
        }

        if (!type.IsConstructedGenericType) return false;

        var definition = type.GetGenericTypeDefinition();
        var arguments = type.GetGenericArguments();

        if (definition == typeof(IReadOnlyDictionary<,>) || definition == typeof(IDictionary<,>)
            || definition == typeof(Dictionary<,>) || definition == typeof(ImmutableDictionary<,>))
        {
            var key = Create(arguments[0], Mix(seed, "key"), depth + 1);
            var value = Create(arguments[1], Mix(seed, "value"), depth + 1);
            if (key == null || value == null) return false;

            var dictionary = (System.Collections.IDictionary)Activator.CreateInstance(
                typeof(Dictionary<,>).MakeGenericType(arguments))!;
            dictionary[key] = value;

            collection = definition == typeof(ImmutableDictionary<,>)
                ? typeof(ImmutableDictionary).GetMethods()
                    .First(m => m.Name == nameof(ImmutableDictionary.ToImmutableDictionary) && m.GetParameters().Length == 1)
                    .MakeGenericMethod(arguments).Invoke(null, [dictionary])
                : dictionary;

            return collection != null;
        }

        if (arguments.Length != 1) return false;

        var itemType = arguments[0];
        var item = Create(itemType, Mix(seed, "item"), depth + 1);
        if (item == null) return false;

        var list = (System.Collections.IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(itemType))!;
        list.Add(item);

        if (definition == typeof(ImmutableArray<>))
        {
            collection = typeof(ImmutableArray).GetMethods()
                .First(m => m.Name == nameof(ImmutableArray.ToImmutableArray) && m.GetParameters().Length == 1)
                .MakeGenericMethod(itemType).Invoke(null, [list]);
            return collection != null;
        }

        if (definition == typeof(ImmutableList<>))
        {
            collection = typeof(ImmutableList).GetMethods()
                .First(m => m.Name == nameof(ImmutableList.ToImmutableList) && m.GetParameters().Length == 1)
                .MakeGenericMethod(itemType).Invoke(null, [list]);
            return collection != null;
        }

        if (definition == typeof(List<>) || definition == typeof(IReadOnlyList<>)
            || definition == typeof(IReadOnlyCollection<>) || definition == typeof(IList<>)
            || definition == typeof(ICollection<>) || definition == typeof(IEnumerable<>))
        {
            collection = list;
            return true;
        }

        return false;
    }

    static object? CreateComplex(Type type, uint seed, int depth)
    {
        var constructor = type.GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .OrderByDescending(c => c.GetParameters().Length)
            .FirstOrDefault();

        if (constructor == null) return null;

        var parameters = constructor.GetParameters();
        var arguments = new object?[parameters.Length];

        for (var i = 0; i < parameters.Length; i++)
        {
            var parameter = parameters[i];
            var value = Create(parameter.ParameterType, Mix(seed, parameter.Name ?? i.ToString()), depth + 1);

            if (value == null)
            {
                // Depth limit or an unconstructable member. Null is legal only for a reference or nullable
                // type - this is how a cycle terminates.
                if (parameter.ParameterType.IsValueType && Nullable.GetUnderlyingType(parameter.ParameterType) == null)
                {
                    return null;
                }
            }

            arguments[i] = value;
        }

        return constructor.Invoke(arguments);
    }

    static Guid DeterministicGuid(uint seed)
    {
        var bytes = new byte[16];
        BitConverter.GetBytes(seed).CopyTo(bytes, 0);
        BitConverter.GetBytes(seed * 2654435761u).CopyTo(bytes, 4);
        BitConverter.GetBytes(seed * 40503u).CopyTo(bytes, 8);
        BitConverter.GetBytes(seed ^ 0x5bf03635u).CopyTo(bytes, 12);
        return new Guid(bytes);
    }

    static uint Mix(uint seed, string name) => Seed(name) ^ (seed * 16777619u);

    /// <summary>
    /// FNV-1a. Deliberately not <see cref="string.GetHashCode()"/>, which is randomized per process and
    /// would make the generated values differ between runs.
    /// </summary>
    static uint Seed(string text)
    {
        var hash = 2166136261u;

        foreach (var c in text)
        {
            hash ^= c;
            hash *= 16777619u;
        }

        return hash;
    }
}
```

- [ ] **Step 5: Write `SystemTextJsonContractSerializer.cs`**

```csharp
using System.Text;
using System.Text.Json;
using Rebus.Messages;
using Rebus.Serialization;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// The default serializer used by the round-trip check: UTF-8 <c>System.Text.Json</c>, with the message type
/// carried in the <c>rbs2-msg-type</c> header.
/// <para>
/// This exists rather than reusing Rebus's own <c>SystemTextJsonSerializer</c> because that type is internal
/// to the Rebus assembly. Owning it here is also the better default: the byte output of this serializer is
/// what the check compares, so it should be pinned and explicit.
/// </para>
/// </summary>
public sealed class SystemTextJsonContractSerializer : ISerializer
{
    static readonly JsonSerializerOptions DefaultOptions = new()
    {
        AllowTrailingCommas = true,
        ReadCommentHandling = JsonCommentHandling.Skip
    };

    readonly IMessageTypeNameConvention _convention;
    readonly JsonSerializerOptions _options;

    /// <summary>
    /// Creates the serializer.
    /// </summary>
    /// <param name="convention">
    /// How type names are written to and read from the type header. Defaults to the assembly-qualified name.
    /// </param>
    /// <param name="options">JSON options. Defaults to trailing commas and comments allowed.</param>
    public SystemTextJsonContractSerializer(
        IMessageTypeNameConvention? convention = null, JsonSerializerOptions? options = null)
    {
        _convention = convention ?? new AssemblyQualifiedTypeNameConvention();
        _options = options ?? DefaultOptions;
    }

    /// <inheritdoc />
    public Task<TransportMessage> Serialize(Message message)
    {
        if (message == null) throw new ArgumentNullException(nameof(message));

        var headers = new Dictionary<string, string>(message.Headers);
        var body = message.Body;

        headers[Headers.ContentType] = "application/json;charset=utf-8";

        if (!headers.ContainsKey(Headers.Type))
        {
            headers[Headers.Type] = _convention.GetTypeName(body.GetType());
        }

        var json = JsonSerializer.Serialize(body, body.GetType(), _options);

        return Task.FromResult(new TransportMessage(headers, Encoding.UTF8.GetBytes(json)));
    }

    /// <inheritdoc />
    public Task<Message> Deserialize(TransportMessage transportMessage)
    {
        if (transportMessage == null) throw new ArgumentNullException(nameof(transportMessage));

        if (!transportMessage.Headers.TryGetValue(Headers.Type, out var typeName))
        {
            throw new InvalidOperationException(
                $"The transport message carries no '{Headers.Type}' header, so its type is unknown.");
        }

        var type = _convention.GetType(typeName)
                   ?? throw new InvalidOperationException($"Could not resolve the message type '{typeName}'.");

        var json = Encoding.UTF8.GetString(transportMessage.Body);
        var body = JsonSerializer.Deserialize(json, type, _options)
                   ?? throw new InvalidOperationException($"Deserializing a '{typeName}' produced null.");

        return Task.FromResult(new Message(new Dictionary<string, string>(transportMessage.Headers), body));
    }

    sealed class AssemblyQualifiedTypeNameConvention : IMessageTypeNameConvention
    {
        public string GetTypeName(Type type) => type.AssemblyQualifiedName ?? type.FullName ?? type.Name;

        public Type GetType(string name) => Type.GetType(name, throwOnError: true)!;
    }
}
```

- [ ] **Step 6: Write `RoundTripChecker.cs`**

```csharp
using Rebus.Messages;
using Rebus.Serialization;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Serializes, deserializes, serializes again, and compares the two bodies as bytes.
/// <para>
/// Comparing serialized output rather than object graphs is deliberate: a recursive graph comparer produces
/// false positives on collection ordering, <c>DateTime</c> precision and floating-point precision - exactly
/// the failure mode being avoided. Comparing two byte arrays has none of those problems and requires no
/// comparer to be written.
/// </para>
/// </summary>
public sealed class RoundTripChecker
{
    readonly ISerializer _serializer;

    /// <summary>
    /// Creates the checker over the serializer the extracted service will use.
    /// </summary>
    public RoundTripChecker(ISerializer serializer) =>
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));

    /// <summary>
    /// Checks <paramref name="instance"/>, appending anything it finds to <paramref name="violations"/>.
    /// A serializer that throws becomes a violation rather than an escaping exception, so one bad contract
    /// does not hide the rest of the report.
    /// </summary>
    public async Task CheckAsync(Type messageType, object instance, ICollection<VerificationViolation> violations)
    {
        if (messageType == null) throw new ArgumentNullException(nameof(messageType));
        if (instance == null) throw new ArgumentNullException(nameof(instance));

        try
        {
            var first = await _serializer.Serialize(new Message(new Dictionary<string, string>(), instance))
                .ConfigureAwait(false);

            var roundTripped = await _serializer.Deserialize(first).ConfigureAwait(false);

            var second = await _serializer.Serialize(new Message(new Dictionary<string, string>(), roundTripped.Body))
                .ConfigureAwait(false);

            if (first.Body.AsSpan().SequenceEqual(second.Body)) return;

            violations.Add(new VerificationViolation(messageType, VerificationCheck.RoundTrip, "",
                "serializing, deserializing and serializing again did not produce identical bytes, which means " +
                "the serializer cannot restore part of this contract. The usual causes are an init-only or " +
                "get-only property with no matching constructor parameter, a private setter, or an " +
                "interface-typed property that deserializes to its base type. " +
                $"First: {Preview(first.Body)}. Second: {Preview(second.Body)}"));
        }
        catch (Exception exception)
        {
            violations.Add(new VerificationViolation(messageType, VerificationCheck.RoundTrip, "",
                $"the serializer threw while round-tripping this contract: {exception.Message}"));
        }
    }

    static string Preview(byte[] body)
    {
        var text = System.Text.Encoding.UTF8.GetString(body);
        return text.Length <= 300 ? text : text[..300] + "...";
    }
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `dotnet test tests/Rebus.InProcCors.Verification.Tests`
Expected: PASS — 8 factory tests and 4 round-trip tests, on top of Task 8's 16.

If `APrivateSetterIsAViolation` passes unexpectedly (no violation), `System.Text.Json` may be populating the
private setter through the constructor-matching rules. Change the fixture so the property genuinely cannot be
restored — e.g. give it a name that matches no constructor parameter — since the point is to prove the check
detects deserialization-side data loss, not to pin one specific `System.Text.Json` behaviour.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: deterministic instance factory, contract serializer, round-trip check"
```

---

## Task 10: Discovery and the verifier

Design §10 "Type discovery" and `MessageContractVerifier`. The lazy-enumeration property is the one that
earns a dedicated test.

**Files:**
- Create: `src/Rebus.InProcCors.Verification/HandlerMessageTypeDiscovery.cs`
- Create: `src/Rebus.InProcCors.Verification/MessageContractVerificationOptions.cs`
- Create: `src/Rebus.InProcCors.Verification/MessageContractVerificationException.cs`
- Create: `src/Rebus.InProcCors.Verification/MessageContractVerifier.cs`
- Test: `tests/Rebus.InProcCors.Verification.Tests/MessageContractVerifierTests.cs`

**Interfaces:**
- Consumes: `ImmutabilityChecker`, `RoundTripChecker`, `DefaultMessageInstanceFactory`, `SystemTextJsonContractSerializer`, `VerificationReport` (Tasks 8-9).
- Produces:
  - `public static class HandlerMessageTypeDiscovery` with `static IReadOnlyList<Type> Discover(IEnumerable<IServiceCollection> serviceCollections)`.
  - `public sealed class MessageContractVerificationOptions` with `bool VerifyOnStartup { get; set; } = true`, `ISerializer? Serializer { get; set; }`, `IMessageInstanceSource? InstanceSource { get; set; }`.
  - `public sealed class MessageContractVerificationException : Exception`, property `VerificationReport Report { get; }`.
  - `public sealed class MessageContractVerifier` with ctor `(IEnumerable<IServiceCollection> serviceCollections, MessageContractVerificationOptions? options = null)`, `static MessageContractVerifier ForHandlersIn(params IServiceCollection[] serviceCollections)`, `Task<VerificationReport> VerifyAsync()`, `VerificationReport Verify()`, `void VerifyAndThrow()`.

- [ ] **Step 1: Write the failing tests**

`tests/Rebus.InProcCors.Verification.Tests/MessageContractVerifierTests.cs`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Rebus.Handlers;

namespace Rebus.InProcCors.Verification.Tests;

public class MessageContractVerifierTests
{
    public sealed record GoodOrder(string Sku, int Quantity);

    public sealed record AnotherGoodOrder(string Sku);

    public sealed class BadOrder
    {
        public string Sku { get; set; } = "";
    }

    sealed class GoodOrderHandler : IHandleMessages<GoodOrder>
    {
        public Task Handle(GoodOrder message) => Task.CompletedTask;
    }

    sealed class AnotherGoodOrderHandler : IHandleMessages<AnotherGoodOrder>
    {
        public Task Handle(AnotherGoodOrder message) => Task.CompletedTask;
    }

    sealed class BadOrderHandler : IHandleMessages<BadOrder>
    {
        public Task Handle(BadOrder message) => Task.CompletedTask;
    }

    sealed class MultiHandler : IHandleMessages<GoodOrder>, IHandleMessages<AnotherGoodOrder>
    {
        public Task Handle(GoodOrder message) => Task.CompletedTask;
        public Task Handle(AnotherGoodOrder message) => Task.CompletedTask;
    }

    [Fact]
    public void DiscoveryProjectsTheMessageTypeOutOfEveryHandlerRegistration()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();
        services.AddTransient<IHandleMessages<AnotherGoodOrder>, AnotherGoodOrderHandler>();

        var discovered = HandlerMessageTypeDiscovery.Discover([services]);

        Assert.Equal(2, discovered.Count);
        Assert.Contains(typeof(GoodOrder), discovered);
        Assert.Contains(typeof(AnotherGoodOrder), discovered);
    }

    [Fact]
    public void DiscoveryIgnoresRegistrationsThatAreNotHandlerClosures()
    {
        var services = new ServiceCollection();
        services.AddSingleton("not a handler");
        services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();

        Assert.Equal([typeof(GoodOrder)], HandlerMessageTypeDiscovery.Discover([services]));
    }

    [Fact]
    public void DiscoveryDeduplicatesAcrossCollections()
    {
        var first = new ServiceCollection();
        first.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();
        var second = new ServiceCollection();
        second.AddTransient<IHandleMessages<GoodOrder>, MultiHandler>();

        Assert.Equal([typeof(GoodOrder)], HandlerMessageTypeDiscovery.Discover([first, second]));
    }

    [Fact]
    public void AHandlerRegisteredAfterTheVerifierIsConstructedIsStillDiscovered()
    {
        // Design §10: IServiceCollection is a live IList<ServiceDescriptor>, so capturing the instance and
        // reading it later makes registration order irrelevant. Eager enumeration would silently verify a
        // subset - worse than not verifying.
        var services = new ServiceCollection();
        var verifier = MessageContractVerifier.ForHandlersIn(services);

        services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();

        Assert.Equal([typeof(GoodOrder)], verifier.Verify().VerifiedTypes);
    }

    [Fact]
    public void AWellFormedContractSetPasses()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();
        services.AddTransient<IHandleMessages<AnotherGoodOrder>, AnotherGoodOrderHandler>();

        var report = MessageContractVerifier.ForHandlersIn(services).Verify();

        Assert.True(report.IsSuccess, report.Describe());
        Assert.Equal(2, report.VerifiedTypes.Count);
    }

    [Fact]
    public void AMutableContractFailsAndTheReportNamesIt()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<BadOrder>, BadOrderHandler>();

        var report = MessageContractVerifier.ForHandlersIn(services).Verify();

        Assert.False(report.IsSuccess);
        Assert.Contains(report.Violations, v => v.Check == VerificationCheck.Immutability
                                                && v.MessageType == typeof(BadOrder));
        Assert.Contains(nameof(BadOrder), report.Describe());
        Assert.Contains(nameof(BadOrder.Sku), report.Describe());
    }

    [Fact]
    public void VerifyAndThrowThrowsCarryingTheReport()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<BadOrder>, BadOrderHandler>();

        var exception = Assert.Throws<MessageContractVerificationException>(
            () => MessageContractVerifier.ForHandlersIn(services).VerifyAndThrow());

        Assert.False(exception.Report.IsSuccess);
        Assert.Contains(nameof(BadOrder), exception.Message);
    }

    [Fact]
    public void VerifyAndThrowIsSilentWhenEverythingPasses()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();

        MessageContractVerifier.ForHandlersIn(services).VerifyAndThrow();
    }

    [Fact]
    public void AContractThatCannotBeConstructedIsAConstructionViolationRatherThanASilentPass()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<Unconstructable>, UnconstructableHandler>();

        var report = MessageContractVerifier.ForHandlersIn(services).Verify();

        Assert.Contains(report.Violations, v => v.Check == VerificationCheck.Construction);
    }

    [Fact]
    public void AnInstanceSourceOverridesTheDefaultFactory()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<Unconstructable>, UnconstructableHandler>();

        var report = new MessageContractVerifier([services], new MessageContractVerificationOptions
        {
            InstanceSource = new UnconstructableSource()
        }).Verify();

        Assert.DoesNotContain(report.Violations, v => v.Check == VerificationCheck.Construction);
    }

    public sealed class Unconstructable
    {
        Unconstructable(string sku) => Sku = sku;

        public string Sku { get; }

        internal static Unconstructable Create(string sku) => new(sku);
    }

    sealed class UnconstructableHandler : IHandleMessages<Unconstructable>
    {
        public Task Handle(Unconstructable message) => Task.CompletedTask;
    }

    sealed class UnconstructableSource : IMessageInstanceSource
    {
        public bool TryCreate(Type messageType, out object? instance)
        {
            if (messageType == typeof(Unconstructable))
            {
                instance = Unconstructable.Create("ABC");
                return true;
            }

            instance = null;
            return false;
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/Rebus.InProcCors.Verification.Tests --filter FullyQualifiedName~MessageContractVerifierTests`
Expected: FAIL to build — `HandlerMessageTypeDiscovery` and `MessageContractVerifier` do not exist.

- [ ] **Step 3: Write `HandlerMessageTypeDiscovery.cs`**

```csharp
using Microsoft.Extensions.DependencyInjection;
using Rebus.Handlers;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Finds message contracts by pulling <c>T</c> out of every registered <see cref="IHandleMessages{TMessage}"/>
/// closure. That is ground truth - no naming convention, no list to maintain, and a message with no handler
/// is dead code anyway.
/// <para>
/// Reads <see cref="IServiceCollection"/> rather than a built <see cref="IServiceProvider"/>, because a
/// provider cannot enumerate its own registrations and only the type shape is needed.
/// </para>
/// </summary>
public static class HandlerMessageTypeDiscovery
{
    /// <summary>
    /// Enumerates <paramref name="serviceCollections"/> now and returns the distinct message types found.
    /// Call this at verification time, not at registration time: the collections are live lists, so reading
    /// them late makes registration order irrelevant.
    /// </summary>
    public static IReadOnlyList<Type> Discover(IEnumerable<IServiceCollection> serviceCollections)
    {
        if (serviceCollections == null) throw new ArgumentNullException(nameof(serviceCollections));

        return serviceCollections
            .SelectMany(collection => collection)
            .Select(descriptor => descriptor.ServiceType)
            .Where(serviceType => serviceType.IsConstructedGenericType
                                  && serviceType.GetGenericTypeDefinition() == typeof(IHandleMessages<>))
            .Select(serviceType => serviceType.GetGenericArguments()[0])
            .Distinct()
            .ToArray();
    }
}
```

- [ ] **Step 4: Write `MessageContractVerificationOptions.cs`**

```csharp
using Rebus.Serialization;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Options for <see cref="MessageContractVerifier"/> and the startup check.
/// </summary>
public sealed class MessageContractVerificationOptions
{
    /// <summary>
    /// Gets or sets whether to run the check at host startup. Defaults to true. Outside the Production
    /// environment a failure throws, failing local startup and the build; in Production it is logged at
    /// error level and startup continues (design §10).
    /// </summary>
    public bool VerifyOnStartup { get; set; } = true;

    /// <summary>
    /// Gets or sets the serializer the round-trip check uses - it should be the one the extracted service
    /// will use. Defaults to <see cref="SystemTextJsonContractSerializer"/>.
    /// </summary>
    public ISerializer? Serializer { get; set; }

    /// <summary>
    /// Gets or sets a per-type override for contracts <see cref="DefaultMessageInstanceFactory"/> cannot
    /// construct. Consulted first; the default factory handles whatever it declines.
    /// </summary>
    public IMessageInstanceSource? InstanceSource { get; set; }
}
```

- [ ] **Step 5: Write `MessageContractVerificationException.cs`**

```csharp
namespace Rebus.InProcCors.Verification;

/// <summary>
/// Thrown by <see cref="MessageContractVerifier.VerifyAndThrow"/> and by the startup check outside
/// Production when one or more contracts failed verification.
/// </summary>
public sealed class MessageContractVerificationException : Exception
{
    /// <summary>
    /// Creates the exception carrying <paramref name="report"/>, whose description becomes the message.
    /// </summary>
    public MessageContractVerificationException(VerificationReport report)
        : base("One or more message contracts failed verification." + Environment.NewLine + report.Describe())
    {
        Report = report;
    }

    /// <summary>
    /// Gets the full report, including exemptions.
    /// </summary>
    public VerificationReport Report { get; }
}
```

- [ ] **Step 6: Write `MessageContractVerifier.cs`**

```csharp
using Microsoft.Extensions.DependencyInjection;
using Rebus.Serialization;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Verifies that every message contract handled by a module is round-trip serializable and deeply immutable.
/// Register it per module, so each module polices its own contracts: every message type has its handler in
/// exactly one module by construction, so the host gathers nothing centrally (design §10).
/// </summary>
public sealed class MessageContractVerifier
{
    readonly IReadOnlyList<IServiceCollection> _serviceCollections;
    readonly MessageContractVerificationOptions _options;
    readonly DefaultMessageInstanceFactory _defaultFactory = new();
    readonly ImmutabilityChecker _immutabilityChecker = new();
    readonly RoundTripChecker _roundTripChecker;

    /// <summary>
    /// Creates the verifier over the given collections. The collections are captured, not copied: they are
    /// enumerated inside <see cref="VerifyAsync"/>, so handlers registered after this call are still found.
    /// </summary>
    public MessageContractVerifier(
        IEnumerable<IServiceCollection> serviceCollections, MessageContractVerificationOptions? options = null)
    {
        if (serviceCollections == null) throw new ArgumentNullException(nameof(serviceCollections));

        _serviceCollections = serviceCollections.ToArray();
        _options = options ?? new MessageContractVerificationOptions();
        _roundTripChecker = new RoundTripChecker(_options.Serializer ?? new SystemTextJsonContractSerializer());
    }

    /// <summary>
    /// Creates a verifier over the given collections. The primitive underneath
    /// <c>AddRebusInProcContractVerification</c>, so the verifier is testable without a host.
    /// </summary>
    public static MessageContractVerifier ForHandlersIn(params IServiceCollection[] serviceCollections) =>
        new(serviceCollections);

    /// <summary>
    /// Runs both checks over every discovered contract and returns the report.
    /// </summary>
    public async Task<VerificationReport> VerifyAsync()
    {
        var messageTypes = HandlerMessageTypeDiscovery.Discover(_serviceCollections);
        var violations = new List<VerificationViolation>();
        var exemptions = new List<VerificationExemption>();

        foreach (var messageType in messageTypes)
        {
            _immutabilityChecker.Check(messageType, violations, exemptions);

            if (!TryCreateInstance(messageType, out var instance))
            {
                violations.Add(new VerificationViolation(messageType, VerificationCheck.Construction, "",
                    "could not construct a test instance of this contract. Give it a public constructor, or " +
                    "supply an IMessageInstanceSource that can build it."));
                continue;
            }

            await _roundTripChecker.CheckAsync(messageType, instance!, violations).ConfigureAwait(false);
        }

        return new VerificationReport(messageTypes, violations, exemptions);
    }

    /// <summary>
    /// Synchronous <see cref="VerifyAsync"/>, for use from a test.
    /// </summary>
    public VerificationReport Verify() => VerifyAsync().GetAwaiter().GetResult();

    /// <summary>
    /// Runs the checks and throws <see cref="MessageContractVerificationException"/> if any failed.
    /// </summary>
    public void VerifyAndThrow()
    {
        var report = Verify();

        if (!report.IsSuccess) throw new MessageContractVerificationException(report);
    }

    bool TryCreateInstance(Type messageType, out object? instance)
    {
        if (_options.InstanceSource != null && _options.InstanceSource.TryCreate(messageType, out instance))
        {
            return instance != null;
        }

        return _defaultFactory.TryCreate(messageType, out instance);
    }
}
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `dotnet test tests/Rebus.InProcCors.Verification.Tests`
Expected: PASS, 10 new tests on top of the earlier 28.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: lazy handler-registration discovery and the contract verifier"
```

---

## Task 11: Registration and the startup check

Design §10 "Registration" and "Startup behaviour".

**Files:**
- Create: `src/Rebus.InProcCors.Verification/ServiceCollectionExtensions.cs`
- Create: `src/Rebus.InProcCors.Verification/ContractVerificationHostedService.cs`
- Test: `tests/Rebus.InProcCors.Verification.Tests/RegistrationTests.cs`

**Interfaces:**
- Consumes: `MessageContractVerifier`, `MessageContractVerificationOptions`, `MessageContractVerificationException` (Task 10).
- Produces:
  - `public static IServiceCollection AddRebusInProcContractVerification(this IServiceCollection services, Action<MessageContractVerificationOptions>? configureOptions = null)`.
  - `internal sealed class ContractVerificationHostedService : IHostedService`.

- [ ] **Step 1: Write the failing tests**

`tests/Rebus.InProcCors.Verification.Tests/RegistrationTests.cs`:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Rebus.Handlers;

namespace Rebus.InProcCors.Verification.Tests;

public class RegistrationTests
{
    public sealed record GoodOrder(string Sku, int Quantity);

    public sealed class BadOrder
    {
        public string Sku { get; set; } = "";
    }

    sealed class GoodOrderHandler : IHandleMessages<GoodOrder>
    {
        public Task Handle(GoodOrder message) => Task.CompletedTask;
    }

    sealed class BadOrderHandler : IHandleMessages<BadOrder>
    {
        public Task Handle(BadOrder message) => Task.CompletedTask;
    }

    sealed class FakeEnvironment : IHostEnvironment
    {
        public string EnvironmentName { get; set; } = Environments.Development;
        public string ApplicationName { get; set; } = "tests";
        public string ContentRootPath { get; set; } = AppContext.BaseDirectory;
        public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
    }

    sealed class RecordingLoggerProvider : ILoggerProvider
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public ILogger CreateLogger(string categoryName) => new RecordingLogger(Entries);

        public void Dispose() { }

        sealed class RecordingLogger(List<(LogLevel, string)> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter) =>
                entries.Add((logLevel, formatter(state, exception)));
        }
    }

    static ServiceProvider BuildProvider(
        Action<IServiceCollection> configure, string environmentName, RecordingLoggerProvider? loggerProvider = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<IHostEnvironment>(new FakeEnvironment { EnvironmentName = environmentName });
        services.AddLogging(b =>
        {
            if (loggerProvider != null) b.AddProvider(loggerProvider);
        });
        configure(services);
        return services.BuildServiceProvider();
    }

    [Fact]
    public void TheVerifierIsResolvableAsASingletonSoATestUsesTheSameWiringAsTheApp()
    {
        using var provider = BuildProvider(services =>
        {
            services.AddRebusInProcContractVerification();
            services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();
        }, Environments.Development);

        var verifier = provider.GetRequiredService<MessageContractVerifier>();

        Assert.Same(verifier, provider.GetRequiredService<MessageContractVerifier>());
        Assert.True(verifier.Verify().IsSuccess);
    }

    [Fact]
    public void HandlersRegisteredAfterTheCallAreStillDiscovered()
    {
        using var provider = BuildProvider(services =>
        {
            services.AddRebusInProcContractVerification();
            services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();
        }, Environments.Development);

        Assert.Equal([typeof(GoodOrder)], provider.GetRequiredService<MessageContractVerifier>().Verify().VerifiedTypes);
    }

    [Fact]
    public async Task TheStartupCheckThrowsOutsideProduction()
    {
        using var provider = BuildProvider(services =>
        {
            services.AddRebusInProcContractVerification();
            services.AddTransient<IHandleMessages<BadOrder>, BadOrderHandler>();
        }, Environments.Development);

        var hostedService = provider.GetServices<IHostedService>().Single();

        await Assert.ThrowsAsync<MessageContractVerificationException>(
            () => hostedService.StartAsync(CancellationToken.None));
    }

    [Fact]
    public async Task TheStartupCheckLogsAndContinuesInProduction()
    {
        // A violation reaching production was already in a build that passed CI, and a reflection check
        // must never be the cause of an outage (design §10).
        var loggerProvider = new RecordingLoggerProvider();

        using var provider = BuildProvider(services =>
        {
            services.AddRebusInProcContractVerification();
            services.AddTransient<IHandleMessages<BadOrder>, BadOrderHandler>();
        }, Environments.Production, loggerProvider);

        var hostedService = provider.GetServices<IHostedService>().Single();
        await hostedService.StartAsync(CancellationToken.None);

        Assert.Contains(loggerProvider.Entries, e => e.Level == LogLevel.Error && e.Message.Contains(nameof(BadOrder)));
    }

    [Fact]
    public async Task TheStartupCheckIsSilentWhenEverythingPasses()
    {
        var loggerProvider = new RecordingLoggerProvider();

        using var provider = BuildProvider(services =>
        {
            services.AddRebusInProcContractVerification();
            services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();
        }, Environments.Development, loggerProvider);

        await provider.GetServices<IHostedService>().Single().StartAsync(CancellationToken.None);

        Assert.DoesNotContain(loggerProvider.Entries, e => e.Level == LogLevel.Error);
    }

    [Fact]
    public void VerifyOnStartupFalseRegistersNoHostedService()
    {
        using var provider = BuildProvider(services =>
        {
            services.AddRebusInProcContractVerification(o => o.VerifyOnStartup = false);
            services.AddTransient<IHandleMessages<BadOrder>, BadOrderHandler>();
        }, Environments.Development);

        Assert.Empty(provider.GetServices<IHostedService>());
        Assert.NotNull(provider.GetRequiredService<MessageContractVerifier>());
    }
}
```

Add `using Microsoft.Extensions.FileProviders;` for `IFileProvider` and `NullFileProvider`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `dotnet test tests/Rebus.InProcCors.Verification.Tests --filter FullyQualifiedName~RegistrationTests`
Expected: FAIL to build — `AddRebusInProcContractVerification` does not exist.

- [ ] **Step 3: Write `ContractVerificationHostedService.cs`**

```csharp
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Runs the contract check at boot. Outside Production a failure throws, failing local startup and the
/// build - the fastest possible feedback, and it catches the developer who never wrote the test. In
/// Production it logs at error level and continues, because a violation reaching production was already in
/// a build that passed CI, and a reflection check must never be the cause of an outage (design §10).
/// </summary>
sealed class ContractVerificationHostedService : IHostedService
{
    readonly MessageContractVerifier _verifier;
    readonly IHostEnvironment _environment;
    readonly ILogger<ContractVerificationHostedService> _logger;

    public ContractVerificationHostedService(
        MessageContractVerifier verifier,
        IHostEnvironment environment,
        ILogger<ContractVerificationHostedService> logger)
    {
        _verifier = verifier;
        _environment = environment;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var report = await _verifier.VerifyAsync().ConfigureAwait(false);

        if (report.Exemptions.Count > 0)
        {
            _logger.LogInformation("Message contract verification found {Count} immutability exemption(s): {Report}",
                report.Exemptions.Count, report.Describe());
        }

        if (report.IsSuccess) return;

        if (_environment.IsProduction())
        {
            _logger.LogError("Message contract verification failed: {Report}", report.Describe());
            return;
        }

        throw new MessageContractVerificationException(report);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
```

- [ ] **Step 4: Write `ServiceCollectionExtensions.cs`**

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Registration for the message contract verification library.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="MessageContractVerifier"/> as a singleton over this module's own
    /// <see cref="IServiceCollection"/>, and - unless
    /// <see cref="MessageContractVerificationOptions.VerifyOnStartup"/> is turned off - a hosted service that
    /// runs the check at boot.
    /// <para>
    /// Call this on each module's collection. Every message type has its handler in exactly one module by
    /// construction, so each module polices its own contracts and the host gathers nothing centrally.
    /// </para>
    /// <para>
    /// Registration order does not matter: the collection is captured and enumerated later, inside
    /// <c>Verify()</c>, so handlers registered below this call are still discovered.
    /// </para>
    /// </summary>
    public static IServiceCollection AddRebusInProcContractVerification(
        this IServiceCollection services,
        Action<MessageContractVerificationOptions>? configureOptions = null)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));

        var options = new MessageContractVerificationOptions();
        configureOptions?.Invoke(options);

        // Captured, not enumerated: this is the live IList<ServiceDescriptor>.
        var verifier = new MessageContractVerifier([services], options);

        services.TryAddSingleton(verifier);

        if (options.VerifyOnStartup)
        {
            services.AddSingleton<IHostedService, ContractVerificationHostedService>();
        }

        return services;
    }
}
```

Registering the verifier as an already-constructed instance is what makes a test resolve the *same* wiring
the application uses — test and application agree on what is checked because they read the same registration.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `dotnet test tests/Rebus.InProcCors.Verification.Tests`
Expected: PASS, 6 new tests on top of the earlier 38.

Note that `AddRebusInProcContractVerification` adds a descriptor to the very collection the verifier will
later enumerate. That is harmless — a `MessageContractVerifier` registration is not an `IHandleMessages<T>`
closure, so discovery skips it.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: per-module registration and the environment-aware startup check"
```

---

## Task 12: Benchmarks

Design §12. Three arms, `InMem` only as comparison, and the bursty scenario is mandatory.

**Files:**
- Create: `benchmarks/Rebus.InProcCors.Benchmarks/Rebus.InProcCors.Benchmarks.csproj`
- Create: `benchmarks/Rebus.InProcCors.Benchmarks/BusArm.cs`
- Create: `benchmarks/Rebus.InProcCors.Benchmarks/ThroughputBenchmark.cs`
- Create: `benchmarks/Rebus.InProcCors.Benchmarks/BurstyLatencyBenchmark.cs`
- Create: `benchmarks/Rebus.InProcCors.Benchmarks/Program.cs`

**Interfaces:**
- Consumes: `InProcNetwork`, `UseInProcTransport`, `InProcReceiveMode`.
- Produces: a runnable benchmark executable. No other task depends on it.

- [ ] **Step 1: Create the project**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net9.0</TargetFramework>
    <IsPackable>false</IsPackable>
    <Optimize>true</Optimize>
    <NoWarn>$(NoWarn);CS1591</NoWarn>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="BenchmarkDotNet" />
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="../../src/Rebus.InProcCors/Rebus.InProcCors.csproj" />
  </ItemGroup>
</Project>
```

```bash
dotnet sln add benchmarks/Rebus.InProcCors.Benchmarks/Rebus.InProcCors.Benchmarks.csproj
```

- [ ] **Step 2: Write `BusArm.cs`**

```csharp
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.InProcCors;
using Rebus.Transport.InMem;

namespace Rebus.InProcCors.Benchmarks;

public sealed record BenchmarkMessage(string Sku, int Quantity, Guid CorrelationId);

/// <summary>
/// Which of the three arms of design §12 to build.
/// </summary>
public enum Arm
{
    /// <summary>Rebus InMem transport with the default JSON serializer. The baseline.</summary>
    InMemJson,

    /// <summary>InProcTransport with the default JSON serializer. Isolates the polling to Channel win.</summary>
    InProcJson,

    /// <summary>InProcTransport with ReferenceSerializer. Adds the serialization win.</summary>
    InProcReference
}

/// <summary>
/// One configured bus plus whatever the arm needs to observe delivery.
/// </summary>
public sealed class BusArm : IDisposable
{
    readonly BuiltinHandlerActivator _activator;

    BusArm(BuiltinHandlerActivator activator, IBus bus)
    {
        _activator = activator;
        Bus = bus;
    }

    public IBus Bus { get; }

    public static BusArm Create(Arm arm, InProcReceiveMode mode, Action<BenchmarkMessage> onHandled)
    {
        var activator = new BuiltinHandlerActivator();
        activator.Handle<BenchmarkMessage>(async message => onHandled(message));

        var configurer = Configure.With(activator);

        var bus = arm switch
        {
            Arm.InMemJson => configurer
                .Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "bench"))
                .Start(),

            Arm.InProcJson => configurer
                .Transport(t => t.UseInProcTransport(new InProcNetwork(), "bench",
                    o => o.ReceiveMode = mode, registerReferenceSerializer: false))
                .Start(),

            Arm.InProcReference => configurer
                .Transport(t => t.UseInProcTransport(new InProcNetwork(), "bench",
                    o => o.ReceiveMode = mode))
                .Start(),

            _ => throw new ArgumentOutOfRangeException(nameof(arm))
        };

        return new BusArm(activator, bus);
    }

    public void Dispose() => _activator.Dispose();
}
```

- [ ] **Step 3: Write `ThroughputBenchmark.cs`**

```csharp
using BenchmarkDotNet.Attributes;

namespace Rebus.InProcCors.Benchmarks;

[MemoryDiagnoser]
public class ThroughputBenchmark
{
    const int MessageCount = 10_000;

    BusArm _arm = null!;
    CountdownEvent _countdown = null!;

    [Params(Arm.InMemJson, Arm.InProcJson, Arm.InProcReference)]
    public Arm Arm { get; set; }

    [Params(InProcReceiveMode.Blocking, InProcReceiveMode.Polling)]
    public InProcReceiveMode Mode { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _countdown = new CountdownEvent(MessageCount);
        _arm = BusArm.Create(Arm, Mode, _ => _countdown.Signal());
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _arm.Dispose();
        _countdown.Dispose();
    }

    [IterationSetup]
    public void IterationSetup() => _countdown.Reset(MessageCount);

    [Benchmark(OperationsPerInvoke = MessageCount)]
    public void SendAndHandle()
    {
        for (var i = 0; i < MessageCount; i++)
        {
            _arm.Bus.SendLocal(new BenchmarkMessage("ABC", i, Guid.NewGuid())).GetAwaiter().GetResult();
        }

        if (!_countdown.Wait(TimeSpan.FromMinutes(2)))
        {
            throw new TimeoutException("Not every message was handled within two minutes.");
        }
    }
}
```

- [ ] **Step 4: Write `BurstyLatencyBenchmark.cs`**

```csharp
using BenchmarkDotNet.Attributes;

namespace Rebus.InProcCors.Benchmarks;

/// <summary>
/// Mandatory, not optional (design §12). DefaultBackoffStrategy.Reset() runs on every successful receive, so
/// the ladder never climbs under saturation and a throughput-only benchmark shows the Channel win as
/// approximately zero. The win lands on the first message after an idle period, which is the dominant
/// traffic shape of a modular monolith. This scenario measures exactly that.
/// </summary>
[MemoryDiagnoser]
public class BurstyLatencyBenchmark
{
    static readonly TimeSpan IdlePeriod = TimeSpan.FromMilliseconds(500);

    BusArm _arm = null!;
    TaskCompletionSource _handled = null!;

    [Params(Arm.InMemJson, Arm.InProcJson, Arm.InProcReference)]
    public Arm Arm { get; set; }

    [Params(InProcReceiveMode.Blocking, InProcReceiveMode.Polling)]
    public InProcReceiveMode Mode { get; set; }

    [GlobalSetup]
    public void Setup() => _arm = BusArm.Create(Arm, Mode, _ => _handled?.TrySetResult());

    [GlobalCleanup]
    public void Cleanup() => _arm.Dispose();

    [IterationSetup]
    public void IterationSetup()
    {
        _handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // Go idle, so the backoff ladder is at its top rung when the message arrives.
        Thread.Sleep(IdlePeriod);
    }

    [Benchmark]
    public void TimeToHandlerEntryAfterIdling()
    {
        _arm.Bus.SendLocal(new BenchmarkMessage("ABC", 1, Guid.NewGuid())).GetAwaiter().GetResult();

        if (!_handled.Task.Wait(TimeSpan.FromSeconds(30)))
        {
            throw new TimeoutException("The message was not handled within thirty seconds.");
        }
    }
}
```

`IterationSetup` runs per iteration and its cost is excluded from the measurement, but BenchmarkDotNet will
warn that the benchmark is far slower than its setup allows for. If the warning is noisy, add
`[SimpleJob(RunStrategy.Monitoring, iterationCount: 50)]` to the class — monitoring mode is the right strategy
for a benchmark whose subject is wall-clock latency rather than throughput.

- [ ] **Step 5: Write `Program.cs`**

```csharp
using BenchmarkDotNet.Running;

BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);

/// <summary>Entry point marker.</summary>
public partial class Program;
```

- [ ] **Step 6: Verify the benchmarks build and run**

```bash
dotnet build benchmarks/Rebus.InProcCors.Benchmarks -c Release
dotnet run --project benchmarks/Rebus.InProcCors.Benchmarks -c Release -- --filter '*BurstyLatency*' --job short
```

Expected: BenchmarkDotNet runs and prints a table. `--job short` keeps the smoke run to a couple of minutes;
the real numbers come from a full run without it.

Record the results in `Docs/` as a short note: three arms, both receive modes, throughput and bursty latency.
The number that decides whether `Blocking` was the right default is the bursty latency of `InProcJson` under
each mode compared against `InMemJson`.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "feat: three-arm benchmark with the mandatory bursty-latency scenario"
```

---

## Task 13: Documentation

Fold the three corrections back into the design document, and give the README the usage section it currently
lacks.

**Files:**
- Modify: `Docs/2026-07-31-rebus-inproccors-design.md`
- Modify: `README.md`

- [ ] **Step 1: Correct design §8**

Replace the configuration snippet and the `PossiblyRegisterDefault` sentence with the real API and the reason
it differs. State plainly that `PossiblyRegisterDefault` is private to `RebusConfigurer`, that
`Injectionist.Register` throws on a duplicate primary registration, and that `registerReferenceSerializer`
mirrors Rebus's own `registerSubscriptionStorage` precedent. Show the corrected surface:

```csharp
.Transport(t => t.UseInProcTransport(network, "orders"))
.Transport(t => t.UseInProcTransport(network, "orders", o => o.ReceiveMode = InProcReceiveMode.Polling))
.Transport(t => t.UseInProcTransportAsOneWayClient(network))

// Explicit serialization: opt out of the default registration first.
.Transport(t => t.UseInProcTransport(network, "orders", registerReferenceSerializer: false))
.Serialization(s => s.UseReferenceSerializer())
```

- [ ] **Step 2: Correct design §10**

Change "default: `SystemTextJsonSerializer`" to `SystemTextJsonContractSerializer`, and add a sentence saying
Rebus's own `SystemTextJsonSerializer` is internal to the Rebus assembly, so the verification package owns its
serializer — which is the better default anyway, since its byte output is what the check compares.

- [ ] **Step 3: Correct design §3**

The implementation note already says the .NET 10 SDK is missing. Add that there is no .NET 8 *runtime* either,
so test and benchmark projects target `net9.0` only while the libraries multi-target `net8.0;net9.0`.

- [ ] **Step 4: Add a usage section to `README.md`**

Insert it after "Mental model: one codebase, two runtime shapes" and before "Where the MediatR analogy stops".
Cover, with real code: constructing the host-owned `InProcNetwork`; configuring two modules against it;
switching a module to a broker (the one-line change the project exists to make possible); and registering
verification per module.

```markdown
## Usage

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
```

- [ ] **Step 5: Update the README's open-questions list**

Every item resolved by the design document is already tabulated in design §13. In `README.md`, mark those
checkboxes `[x]` and append `— resolved, see Docs/2026-07-31-rebus-inproccors-design.md §N` to each. Leave the
genuinely open ones (concurrency and bulkheading, durability, `SendRequest` chain discipline, the symmetry
test, where the expected/unexpected failure line falls) unchecked.

- [ ] **Step 6: Run the full suite one last time**

```bash
dotnet build
dotnet test
```

Expected: everything passes. Report the actual test count.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "docs: fold implementation corrections into the design, add README usage"
```

---

## Self-review notes

Checked after writing, recorded so the implementer knows what was and was not verified.

**Spec coverage.** Every numbered section of the design maps to a task: §3 → Task 1 and the Global
Constraints; §4 → Task 2; §5 → Task 3; §6 → Task 5; §7 → Task 4; §8 → Task 6; §9 → Tasks 2 and 4; §10 →
Tasks 8-11; §11 → Tasks 2-7 (each of the eleven listed tests has a named test method); §12 → Task 12; §13
and §14 → Task 13. §2's findings are not implemented but *asserted*, in Task 7.

**Design §11's eleven tests, and where each lands.** 1 → `TheHandlerReceivesTheSameInstanceTheCallerSent`.
2 → `AThrowingHandlerRetriesAndTheMessageIsStillReadableInTheErrorQueue`. 3 →
`ADeferredMessageKeepsItsReferenceAcrossTheTimeoutManager`. 4 → `SendRequestReturnsTheIdenticalReplyInstance`.
5 → `TwoNetworksDoNotObserveEachOthersTraffic`. 6 → `AHandlerCannotSeeTheCallersRegistrations`. 7 →
`SendReturnsBeforeTheHandlerRunsAndAHandlerExceptionDoesNotSurfaceAtTheCallSite`. 8 → the `[Theory]`
parameterisation on every integration test. 9 → `SentinelsAreOneByteAndReferenceDistinct` and
`CollectingTheSentinelReleasesTheTableEntry`. 10 →
`DeserializingAMessageWhoseBodyWasReplacedThrowsWithAUsefulMessage`. The verification library's own listed
cases are in Tasks 8-11, including the two named explicitly: lazy discovery
(`AHandlerRegisteredAfterTheVerifierIsConstructedIsStillDiscovered`) and the environment-dependent startup
behaviour (`TheStartupCheckThrowsOutsideProduction`, `TheStartupCheckLogsAndContinuesInProduction`).

**Not verified against a running compiler.** Every code block here was written against the Rebus 8.9.2
sources read directly from `rebus-org/Rebus` at `master`, but none of it has been compiled. The API details
most likely to need a small correction, each flagged inline at the point of use:

- `BuiltinHandlerActivator.UseServiceProvider` (Task 7) — the exact API for resolving handlers from a
  provider.
- `OptionsConfigurer.RetryStrategy` parameter names (Task 7).
- `Headers.ErrorDetails` (Task 7) — the constant `DeadletterQueueErrorHandler` writes.
- `Rebus.Async` namespaces for `EnableSynchronousRequestReply` and `SendRequest` (Task 7).
- `AbstractRebusTransport.Receive`'s nullability annotation (Task 5) — the override may need `TransportMessage`
  rather than `TransportMessage?`.

None of these changes the design; all are import-and-signature adjustments the implementer resolves at the
first build.

**One deliberate deviation from the design's own wording.** Design §10's options snippet comments
`VerifyOnStartup` as "default: true outside Production". This plan implements `VerifyOnStartup = true`
unconditionally, with the *environment* deciding throw-versus-log inside the hosted service. That is what the
design's own "Startup behaviour" paragraph specifies, and it is the better shape: the check still runs in
Production, so a violation is logged rather than invisible.
