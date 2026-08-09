using BenchmarkDotNet.Attributes;

namespace Rebus.InProcCors.Benchmarks;

[MemoryDiagnoser]
public class ThroughputBenchmark
{
    const int MessageCount = 10_000;

    // Built once, sent 10,000 times. Constructing the (deliberately fat) message inside the measured loop
    // would charge every arm the same allocation cost and dilute the difference the benchmark exists to show.
    static readonly BenchmarkMessage Message = BenchmarkMessage.CreateSample();

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
            _arm.Bus.SendLocal(Message).GetAwaiter().GetResult();
        }

        if (!_countdown.Wait(TimeSpan.FromMinutes(2)))
        {
            throw new TimeoutException("Not every message was handled within two minutes.");
        }
    }
}
