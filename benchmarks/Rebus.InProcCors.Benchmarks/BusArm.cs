using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
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

        // Not an async lambda: TreatWarningsAsErrors turns CS1998 (async without await) into a build error.
        activator.Handle<BenchmarkMessage>(message =>
        {
            onHandled(message);
            return Task.CompletedTask;
        });

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
