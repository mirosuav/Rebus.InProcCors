# Handoff — remote handler cancellation

**Written:** 2026-08-09 · **Status: exploration only. No code, no test, no plan. Nothing has been decided.**

This session explored one question and stopped before designing. Read this before touching the topic again;
the assembly facts in §2 cost a build to obtain and should not be re-derived.

---

# 1. The question

> Can we propagate a caller's `CancellationToken` to the receiver's handler, so that an in-flight
> `Rebus.Async` `SendRequest` can be cancelled asynchronously?

The question was then reframed by the user, and **the reframing is the important part of this session**:

> Transport-agnostic *and* `Rebus.Async`-agnostic. Just a protocol. The caller knows the id of the message it
> sent a moment ago, so it sends a `CancelRemoteMessage(MessageId)` to the same queue.

Everything below is about the reframed version. The original framing is dead — see §3.

---

# 2. Verified assembly facts

Dumped by reflection from the real packages, per the working agreement in `HANDOFF.md`. These are facts, not
recollection. Do not re-derive them.

## `Rebus.Async` 10.0.0 — the entire type list

```
Rebus.AsyncBusExtensions                                      [public]
   Task<TReply> SendRequest(IBus, object request, IDictionary<string,string> optionalHeaders,
                            TimeSpan? timeout, CancellationToken externalCancellationToken)
   Task<TReply> SendRequest(IRoutingApi, string destinationAddress, object request, …same tail…)
   Task<TReply> InnerProcessRequest(Func<…> busAction, …)                       private
   bool         TryGrabDesiredMessageId(IDictionary<string,string>, out string) private
Rebus.Config.AsyncConfigurationExtensions                     [public]
   void EnableSynchronousRequestReply(OptionsConfigurer)
Rebus.Internals.ReplyHandlerStep                              [internal]
   Task Process(IncomingStepContext, Func<Task> next)
   const string SpecialMessageIdPrefix = "request-reply"
```

That is all of it. The package is one incoming pipeline step plus two extension methods.

**`SendRequest` already takes a `CancellationToken`.** It is **caller-side only**: it abandons the
`TaskCompletionSource` that `ReplyHandlerStep` would have completed. The request is already in the queue; the
handler runs to completion and its reply lands nowhere. So the feature was never "add a token" — it is
"make that existing token mean something on the far side."

Package targets `netstandard2.0`, depends on `rebus >= 8.0.1`.

## `Rebus` 8.9.2 — the relevant surface

```
Rebus.Extensions.MessageContextExtensions.GetCancellationToken(IMessageContext)
    XML doc, verbatim: "Gets the bus' shutdown CancellationToken."
Rebus.Pipeline.StepContext.Save<T>/Load<T>  (+ string-keyed overloads)   ← the extension point
Rebus.Pipeline.PipelineMessageContextExtensions.AbortDispatch(IMessageContext)
Rebus.Transport.ITransactionContext.Items / OnCommit / OnRollback / OnAck / OnNack / OnDisposed
Rebus.Bus.Advanced.IRoutingApi.Send(string destinationAddress, object, IDictionary<string,string>)
Rebus.Bus.Advanced.ITopicsApi.Publish(string topic, object, IDictionary<string,string>)
Rebus.Routing.IRouter.GetDestinationAddress(Message)
Rebus.Config.Options.DefaultNumberOfWorkers  = 1
Rebus.Config.Options.DefaultMaxParallelism   = 5
```

`Rebus.Messages.Headers` has 22 constants; **none of them concern cancellation.** A custom header would be
needed if the protocol wants one.

`IBus.Send` returns `Task`, **not** a message id.

---

# 3. Why the original framing is dead

A `CancellationToken` is a heap object with a callback list. It has no wire representation. Anything crossing
a queue boundary carries at most a correlation id meaning "the caller for request X is gone"; the receiver
must own its own `CancellationTokenSource` and look it up. **The abstraction inverts across the boundary** —
caller-side it is a token, receiver-side it is a registry. So there is no "propagating the token"; there is
only a protocol. The user reached this conclusion independently, which is why the reframing above is the
live version.

Two further reasons not to revive it:

**`GetCancellationToken` is an occupied slot.** It means *the bus is shutting down*, which handlers are meant
to treat as "abandon, the message will be redelivered." Caller-cancellation means the opposite: nobody wants
the result, do not redeliver. Folding them into one token destroys the handler's ability to tell them apart.
If a handler-side accessor is ever added it must be a **separate** one.

**A heap-based in-proc-only shortcut is available and should be refused.** `MessageReferenceTable`
(`ConditionalWeakTable<byte[], object>`) already smuggles live references alongside messages; a CTS is just
another live reference, so the in-proc arm would be nearly free. That is exactly the local/remote asymmetry
`README.md:26` exists to forbid — remote semantics first, in-proc as the optimization. Noted here so the
next session does not rediscover the shortcut and mistake it for a good idea.

---

# 4. What is right about the `CancelRemoteMessage(MessageId)` protocol

**Caller-minted ids are correct, and Rebus forces the issue.** Since `Send` returns no id, there is nothing to
"know afterwards" — the caller must mint the id and pass it as `optionalHeaders[Headers.MessageId]`. This is
already a supported Rebus pattern: it is precisely what `TryGrabDesiredMessageId` exists to do inside
`Rebus.Async`. So:

```csharp
var id = Guid.NewGuid().ToString();
await bus.Send(new DoWork(…), new Dictionary<string,string> { [Headers.MessageId] = id });
// …later, from anywhere…
await bus.Advanced.Routing.Send(dest, new CancelRemoteMessage(id));
```

**Dropping the `Rebus.Async` dependency makes it strictly more useful than the original ask.** It cancels any
long-running handler, not only request/reply ones — a fire-and-forget 40-second import is arguably the better
case. `Rebus.Async` then becomes one *consumer* of the protocol (its `externalCancellationToken` gets a
`.Register(…)` that emits the cancel), never a dependency of it.

**Two properties the protocol must have, both forced by the queue giving no ordering guarantee:**

- **Cancel-after-completion is a silent no-op.**
- **Cancel-before-dispatch must still cancel.** So the receiver needs more than a live
  `ConcurrentDictionary<string, CancellationTokenSource>` — it needs a short-lived **tombstone set** of
  already-cancelled ids consulted at dispatch. That set needs eviction; its TTL is a real tunable (too short
  loses races, too long is an unbounded leak keyed on caller-supplied ids).

---

# 5. Three problems, ranked. None are plumbing.

## P1 — Head-of-line blocking. Structural.

The cancel message queues behind the very work it is trying to stop. With Rebus's defaults
(`NumberOfWorkers = 1`, `MaxParallelism = 5`) the single worker thread still issues receives while handlers
are in flight, so in practice the cancel usually gets through. **But the failure is correlated with the use
case**: you cancel long handlers, and long handlers are what occupy all five parallelism slots. The feature
is least likely to work exactly when it is most needed.

This is `README.md:294` — concurrency and bulkheading, the fourth Waldo difference — resurfacing as a
*correctness* problem rather than a throughput one. That is new information about an already-open question
and is the strongest argument yet for bulkheading.

## P2 — Competing consumers. Invisible in-proc, fatal after extraction.

Three instances of a service share one queue. The request was dispatched to instance A. The cancel is an
ordinary message, so it is delivered to whichever instance is idle — probably B, which holds no CTS for that
id and drops it. **In-proc there is exactly one instance, so this appears to work flawlessly and then stops
working silently on the day you scale out.** No test writable today can surface it.

The implied fix is that cancellation is a **broadcast, not a send** — `Publish`/`Topics`, so every instance
sees it and only the owner reacts, at the cost of a subscription per endpoint and N−1 wasted deliveries.

## P3 — The retry pipeline will fight you.

If cancellation surfaces as `OperationCanceledException`, `DefaultRetryStep` retries five times and
dead-letters. A request the caller explicitly abandoned would be executed four more times and then poison the
error queue. **The protocol needs an incoming pipeline step that recognises "cancelled by requester" and acks
the message.** That step, not the token, is the actual feature.

Unresolved inside P3: **cancel is not rollback.** A half-run handler may already have `bus.Send`-ed messages,
which Rebus commits on transaction commit. Ack-with-cancel lets them out; nack retries everything. Neither is
obviously right and whichever is chosen is a semantic that must be documented, not defaulted.

---

# 6. The open decision

The id-keyed control message is agreed. **The delivery path is not.** Three candidates were put on the table
and the user's response — "there are many ways to handle it apparently" — closed the session before a choice.

| | Solves P1 | Solves P2 | Cost |
|---|---|---|---|
| **A — `Publish` to a topic** | no | yes | a subscription per endpoint; N−1 wasted deliveries |
| **B — dedicated control endpoint per module**, own queue, own workers | yes | yes | a second bus and queue per module; visible infra change |
| **C — `Send` to the same queue**, documented best-effort | no | no | none |

P3 must be solved in all three.

**The recommendation on the table when the session ended was A**, reasoning that P2 is the failure that
cannot be detected in-proc and is therefore the one the project's own principles say to eliminate first,
while P1 at least degrades loudly under load where a benchmark can catch it. **The counter-argument, which
was not resolved, is that B is the only one that is actually correct — and since bulkheading is coming anyway
(`README.md:294`), B might be one design instead of two.** Settle that before designing anything.

C is not a joke option. If the payoff is only "stop burning CPU when we can," honest best-effort with a
blunt docstring may be the whole feature.

---

# 7. Where to pick this up

1. Decide A vs B vs C — §6. Everything downstream depends on it, and the B-subsumes-bulkheading argument
   should be settled first because it may reframe the whole thing as a bulkheading task.
2. Then, and only then, brainstorm → spec → plan as usual. This document is **not** a spec.
3. Re-read `README.md:13-26` (the Waldo framing) before proposing anything. Two of the three problems above
   are that framing biting, and any design that feels clean in-proc should be assumed guilty.

## Reproducing the assembly dump

A throwaway `net10.0` console project referencing `Rebus 8.9.2` + `Rebus.Async 10.0.0`, loading
`Rebus.Async.dll` via `Assembly.LoadFrom` (it is `netstandard2.0`, so it does not resolve as a normal
`PackageReference` in this shape) and walking `GetTypes()` with
`BindingFlags.Public | NonPublic | Static | Instance | DeclaredOnly`. It lives in the session scratchpad and
was not committed; it takes about two minutes to rewrite and is not worth keeping.

## Not verified

Everything in §5 and §6 is **reasoned, not measured**. In particular P1's "usually gets through" is inferred
from `MaxParallelism = 5` plus the worker-loop reading in design §2, and has never been observed. If P1 ends
up load-bearing in the decision, measure it first.
