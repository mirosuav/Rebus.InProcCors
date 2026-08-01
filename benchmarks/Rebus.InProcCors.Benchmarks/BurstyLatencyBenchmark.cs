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
