using BenchmarkDotNet.Attributes;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Transport.InMem;

namespace Rebus.InProcCors.Benchmarks;

/// <summary>A message that is almost entirely one binary payload - the large-attachment edge case.</summary>
public sealed record BlobMessage(string Name, byte[] Payload);

sealed class BlobMessageHandler(Action<BlobMessage> onHandled) : IHandleMessages<BlobMessage>
{
    public Task Handle(BlobMessage message)
    {
        onHandled(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// One send of a message carrying <see cref="PayloadBytes"/> of binary data, timed to handler entry. The JSON
/// arms Base64-encode the array on the way out and decode it on the way in; the reference arm hands the array
/// over untouched, so its cost should stay flat as the payload grows. Arrays over 85 KB land on the large object
/// heap, so the allocation column is as important as the time column here.
/// </summary>
[MemoryDiagnoser]
public class PayloadSizeBenchmark
{
    IBus _bus = null!;
    BlobMessage _message = null!;
    TaskCompletionSource _handled = null!;

    [Params(Arm.InMemJson, Arm.InProcJson, Arm.InProcReference)]
    public Arm Arm { get; set; }

    [Params(1_024, 1_048_576, 104_857_600)]
    public int PayloadBytes { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var payload = new byte[PayloadBytes];
        new Random(42).NextBytes(payload);   // incompressible and fixed, so every iteration is identical
        _message = new BlobMessage("blob", payload);

        var configurer = Configure.With(new SingleHandlerActivator(new BlobMessageHandler(_ => _handled.TrySetResult())));

        _bus = Arm switch
        {
            Arm.InMemJson => configurer.Transport(t => t.UseInMemoryTransport(new InMemNetwork(), "bench")).Start(),
            Arm.InProcJson => configurer.Transport(t => t.UseInProcTransport(new InProcNetwork(), "bench",
                registerReferenceSerializer: false)).Start(),
            Arm.InProcReference => configurer.Transport(t => t.UseInProcTransport(new InProcNetwork(), "bench")).Start(),
            _ => throw new ArgumentOutOfRangeException(nameof(Arm))
        };
    }

    [GlobalCleanup]
    public void Cleanup() => _bus.Dispose();

    [IterationSetup]
    public void IterationSetup() =>
        _handled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

    [Benchmark]
    public void SendAndHandle()
    {
        _bus.SendLocal(_message).GetAwaiter().GetResult();

        if (!_handled.Task.Wait(TimeSpan.FromMinutes(2)))
        {
            throw new TimeoutException("The message was not handled within two minutes.");
        }
    }
}

/*

Results, 2026-09-23 (--launchCount 1-2 --warmupCount 2-3 --iterationCount 10-15, net10.0):

| Method        | Arm             | PayloadBytes | Mean         | Error       | StdDev      | Median       | Allocated    |
|-------------- |---------------- |------------- |-------------:|------------:|------------:|-------------:|-------------:|
| SendAndHandle | InMemJson       | 1024         | 212,560.5 us |  3,055.4 us |  2,021.0 us | 212,794.0 us |     24.22 KB |
| SendAndHandle | InMemJson       | 1048576      | 220,635.6 us | 10,491.8 us |  6,939.7 us | 223,236.8 us |   2424.71 KB |
| SendAndHandle | InMemJson       | 104857600    | 340,934.1 us | 38,151.1 us | 25,234.6 us | 334,851.5 us | 238968.71 KB |
| SendAndHandle | InProcJson      | 1024         |     387.2 us |    379.5 us |    198.5 us |     308.8 us |     21.63 KB |
| SendAndHandle | InProcJson      | 1048576      |   2,653.4 us |  1,406.7 us |    837.1 us |   2,181.3 us |   2408.76 KB |
| SendAndHandle | InProcJson      | 104857600    | 170,206.2 us | 26,220.5 us | 17,343.2 us | 169,022.6 us | 238954.62 KB |
| SendAndHandle | InProcReference | 1024         |     344.1 us |    310.9 us |    162.6 us |     279.2 us |     18.52 KB |
| SendAndHandle | InProcReference | 1048576      |     421.6 us |    552.4 us |    288.9 us |     303.3 us |     19.73 KB |
| SendAndHandle | InProcReference | 104857600    |     421.5 us |    404.9 us |    211.8 us |     388.3 us |     24.03 KB |

The InMem arm includes Rebus's idle backoff on every iteration (~210 ms at 1 KB), which is the sparse-traffic
case the channel removes.

*/
