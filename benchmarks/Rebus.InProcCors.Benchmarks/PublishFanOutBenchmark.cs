using BenchmarkDotNet.Attributes;
using Rebus.Bus;
using Rebus.Config;

namespace Rebus.InProcCors.Benchmarks;

/// <summary>
/// One publisher, <see cref="Subscribers"/> subscribers on the same network. Publish is the path where Rebus
/// hands one transport message instance to several destinations, so it is where per-destination header
/// isolation costs anything. Blocking mode only: polling adds nothing to what this measures.
/// </summary>
[MemoryDiagnoser]
public class PublishFanOutBenchmark
{
    const int MessageCount = 2_000;

    static readonly BenchmarkMessage Message = BenchmarkMessage.CreateSample();

    readonly List<IBus> _buses = [];
    IBus _publisher = null!;
    CountdownEvent _countdown = null!;

    [Params(Arm.InProcJson, Arm.InProcReference)]
    public Arm Arm { get; set; }

    [Params(1, 4)]
    public int Subscribers { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _countdown = new CountdownEvent(MessageCount * Subscribers);
        var network = new InProcNetwork();
        var reference = Arm == Arm.InProcReference;

        for (var i = 0; i < Subscribers; i++)
        {
            var subscriber = Configure.With(new SingleHandlerActivator(new BenchmarkMessageHandler(_ => _countdown.Signal())))
                .Transport(t => t.UseInProcTransport(network, $"subscriber-{i}", registerReferenceSerializer: reference))
                .Start();

            subscriber.Subscribe<BenchmarkMessage>().GetAwaiter().GetResult();
            _buses.Add(subscriber);
        }

        _publisher = Configure.With(new SingleHandlerActivator(new object()))
            .Transport(t => t.UseInProcTransportAsOneWayClient(network, registerReferenceSerializer: reference))
            .Start();

        _buses.Add(_publisher);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        foreach (var bus in _buses) bus.Dispose();
        _countdown.Dispose();
    }

    [IterationSetup]
    public void IterationSetup() => _countdown.Reset(MessageCount * Subscribers);

    [Benchmark(OperationsPerInvoke = MessageCount)]
    public void PublishAndHandle()
    {
        for (var i = 0; i < MessageCount; i++)
        {
            _publisher.Publish(Message).GetAwaiter().GetResult();
        }

        if (!_countdown.Wait(TimeSpan.FromMinutes(2)))
        {
            throw new TimeoutException("Not every subscriber handled every message within two minutes.");
        }
    }
}

/*

Results, 2026-09-23 (--launchCount 1-2 --warmupCount 2-3 --iterationCount 10-15, net10.0):

| Method           | Arm             | Subscribers | Mean     | Error    | Allocated |
|----------------- |---------------- |------------ |---------:|---------:|----------:|
| PublishAndHandle | InProcJson      | 1           | 139.4 us | 20.44 us |  28.08 KB |
| PublishAndHandle | InProcJson      | 4           | 224.6 us | 10.01 us |  68.08 KB |
| PublishAndHandle | InProcReference | 1           |  99.9 us |  3.39 us |  22.14 KB |
| PublishAndHandle | InProcReference | 4           | 221.2 us |  8.71 us |  49.66 KB |

Per-destination headers cost ~1.3 KB per publish to 4 subscribers (48.28 KB before); timings within noise.

*/
