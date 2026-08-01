# Benchmark results — 2026-08-01

Design §12, three arms × two receive modes. **These are `--job short` smoke numbers** (1 launch,
3 warmup, 3 iterations), not a full run: the `Error` column is frequently larger than the `Mean`,
so treat the small differences as noise and only the order-of-magnitude differences as real.

- Machine: Windows 11 (10.0.26200), X64 RyuJIT AVX-512
- .NET SDK 10.0.302, runtime 10.0.10
- BenchmarkDotNet v0.14.0

Reproduce with:

```bash
dotnet run --project benchmarks/Rebus.InProcCors.Benchmarks -c Release -- --filter '*BurstyLatency*' --job short
dotnet run --project benchmarks/Rebus.InProcCors.Benchmarks -c Release -- --filter '*Throughput*'    --job short
```

Drop `--job short` for publishable numbers.

---

## Bursty latency — time to handler entry after a 500 ms idle period

This is the scenario design §12 calls mandatory, and it is the one that decides the default.

| Arm | Mode | Median | Allocated |
|-----|------|-------:|----------:|
| InMemJson | Blocking | 127,461 µs | 61.13 KB |
| InMemJson | Polling | 143,370 µs | 61.13 KB |
| **InProcJson** | **Blocking** | **497 µs** | 45.14 KB |
| InProcJson | Polling | 158,669 µs | 31.77 KB |
| **InProcReference** | **Blocking** | **400 µs** | 19.11 KB |
| InProcReference | Polling | 159,972 µs | 30.85 KB |

**`Blocking` is decisively the right default — by roughly two and a half orders of magnitude.**
The comparison the plan singles out is `InProcJson` under each mode against `InMemJson`: ~497 µs
blocking versus ~127 ms for InMem, with `InProcJson` under *polling* landing at ~159 ms, i.e. no
better than InMem. That is the expected shape. The blocking path parks on
`ChannelReader.WaitToReadAsync` and is woken by the write; the polling path and InMem both wait out
Rebus's backoff ladder, which has climbed to its top rung during the idle period. The transport
choice is irrelevant under polling — what buys the latency is never sleeping in the first place.

The `Allocated` column also separates the arms in the expected direction: `InProcReference`
blocking allocates 19.11 KB against InMem's 61.13 KB, since the message body is never serialized.

## Throughput — 10,000 `SendLocal` calls, per-message cost

| Arm | Mode | Mean | Allocated |
|-----|------|-----:|----------:|
| InMemJson | Blocking | 40.10 µs | 18.27 KB |
| InMemJson | Polling | 38.75 µs | 18.27 KB |
| InProcJson | Blocking | 36.09 µs | 18.01 KB |
| InProcJson | Polling | 35.82 µs | 18.12 KB |
| InProcReference | Blocking | 34.74 µs | 17.78 KB |
| InProcReference | Polling | 34.71 µs | 17.77 KB |

**The Channel win is approximately zero here, exactly as design §12 predicted**, and this is the
result that justifies the bursty scenario existing at all. `DefaultBackoffStrategy.Reset()` runs on
every successful receive, so under saturation the ladder never climbs and there is nothing for the
`Channel` to improve on — the two receive modes are within noise of each other in every arm.

The ~13% spread from `InMemJson` to `InProcReference` is at the edge of what this smoke run can
resolve (the `Error` column reaches ±88 µs). It should not be quoted as a result without a full run.

### Caveat on the `Mode` parameter for `InMemJson`

`Mode` is meaningless for the `InMemJson` arm — it configures the InMem transport either way — so
those two rows are a repeated measurement of the same configuration. Their spread (127 ms vs 143 ms
bursty; 40.10 µs vs 38.75 µs throughput) is a useful read on this run's noise floor.
