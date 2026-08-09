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
    static readonly BenchmarkMessage Message = BenchmarkMessage.CreateSample();

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
        _arm.Bus.SendLocal(Message).GetAwaiter().GetResult();

        if (!_handled.Task.Wait(TimeSpan.FromSeconds(30)))
        {
            throw new TimeoutException("The message was not handled within thirty seconds.");
        }
    }
}

/*

Small message:
| Method                        | Arm             | Mode     | Mean         | Error        | StdDev       | Median       | Allocated |
|------------------------------ |---------------- |--------- |-------------:|-------------:|-------------:|-------------:|----------:|
| TimeToHandlerEntryAfterIdling | InMemJson       | Blocking | 115,142.4 us | 16,374.20 us | 45,099.30 us | 139,220.5 us |  35.66 KB |
| TimeToHandlerEntryAfterIdling | InMemJson       | Polling  | 114,949.1 us | 17,131.56 us | 45,430.48 us | 139,036.9 us |  35.69 KB |
| TimeToHandlerEntryAfterIdling | InProcJson      | Blocking |     419.8 us |     29.82 us |     86.04 us |     402.9 us |   19.7 KB |
| TimeToHandlerEntryAfterIdling | InProcJson      | Polling  | 122,746.1 us | 13,978.04 us | 39,195.92 us | 139,161.9 us |  31.44 KB |
| TimeToHandlerEntryAfterIdling | InProcReference | Blocking |     389.3 us |     25.31 us |     73.01 us |     369.8 us |  18.78 KB |
| TimeToHandlerEntryAfterIdling | InProcReference | Polling  | 121,442.5 us | 14,127.13 us | 39,845.84 us | 139,218.4 us |  30.52 KB |

Big message

| Method                        | Arm             | Mode     | Mean         | Error        | StdDev       | Median       | Allocated |
|------------------------------ |---------------- |--------- |-------------:|-------------:|-------------:|-------------:|----------:|
| TimeToHandlerEntryAfterIdling | InMemJson       | Blocking | 129,491.8 us | 14,404.41 us | 42,471.72 us | 142,722.6 us |  46.66 KB |
| TimeToHandlerEntryAfterIdling | InMemJson       | Polling  | 144,733.1 us | 11,522.60 us | 33,974.65 us | 158,929.6 us |  46.66 KB |
| TimeToHandlerEntryAfterIdling | InProcJson      | Blocking |     461.9 us |     37.92 us |    108.80 us |     427.0 us |  30.66 KB |
| TimeToHandlerEntryAfterIdling | InProcJson      | Polling  | 147,303.0 us | 10,167.17 us | 29,978.14 us | 159,471.8 us |  42.41 KB |
| TimeToHandlerEntryAfterIdling | InProcReference | Blocking |     399.7 us |     29.40 us |     85.30 us |     383.1 us |  24.34 KB |
| TimeToHandlerEntryAfterIdling | InProcReference | Polling  | 127,612.3 us | 15,550.75 us | 45,851.75 us | 147,916.8 us |  36.14 KB |



*/