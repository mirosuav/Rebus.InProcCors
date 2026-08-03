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

/*


| Method                        | Arm             | Mode     | Mean         | Error        | StdDev       | Median       | Allocated |
|------------------------------ |---------------- |--------- |-------------:|-------------:|-------------:|-------------:|----------:|
| TimeToHandlerEntryAfterIdling | InMemJson       | Blocking | 115,142.4 us | 16,374.20 us | 45,099.30 us | 139,220.5 us |  35.66 KB |
| TimeToHandlerEntryAfterIdling | InMemJson       | Polling  | 114,949.1 us | 17,131.56 us | 45,430.48 us | 139,036.9 us |  35.69 KB |
| TimeToHandlerEntryAfterIdling | InProcJson      | Blocking |     419.8 us |     29.82 us |     86.04 us |     402.9 us |   19.7 KB |
| TimeToHandlerEntryAfterIdling | InProcJson      | Polling  | 122,746.1 us | 13,978.04 us | 39,195.92 us | 139,161.9 us |  31.44 KB |
| TimeToHandlerEntryAfterIdling | InProcReference | Blocking |     389.3 us |     25.31 us |     73.01 us |     369.8 us |  18.78 KB |
| TimeToHandlerEntryAfterIdling | InProcReference | Polling  | 121,442.5 us | 14,127.13 us | 39,845.84 us | 139,218.4 us |  30.52 KB |

*/