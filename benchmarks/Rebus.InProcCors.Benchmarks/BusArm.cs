using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Transport;
using Rebus.Transport.InMem;

namespace Rebus.InProcCors.Benchmarks;

/// <summary>One line of an order. Small on purpose - the point is that there are several of them.</summary>
public sealed record OrderLine(string Sku, string Description, int Quantity, decimal UnitPrice);

/// <summary>One entry in a message's audit trail. The second collection on <see cref="BenchmarkMessage"/>.</summary>
public sealed record AuditEntry(string Actor, string Action, DateTime OccurredAtUtc);

/// <summary>
/// The message the three arms carry. Deliberately fat - several strings, integral and floating-point
/// numbers, decimals, dates and two collections of small objects.
/// <para>
/// Size is the whole point. <see cref="ReferenceSerializer"/> never reads the payload (it registers a
/// sentinel and hands the live instance back), so every property added here is free in the
/// <see cref="Arm.InProcReference"/> arm and costs real work in the two JSON arms. A three-property
/// message makes that difference invisible.
/// </para>
/// </summary>
public sealed record BenchmarkMessage(
    string OrderReference,
    string Sku,
    string CustomerName,
    string CustomerEmail,
    string ShippingAddress,
    string Notes,
    int Quantity,
    int WarehouseId,
    long SequenceNumber,
    double WeightKilograms,
    decimal UnitPrice,
    decimal TotalPrice,
    decimal DiscountRate,
    DateTime OrderedAtUtc,
    DateTime RequestedDeliveryOn,
    DateTimeOffset CreatedAt,
    Guid CorrelationId,
    List<OrderLine> Lines,
    List<AuditEntry> AuditTrail)
{
    /// <summary>
    /// A fully populated message built from hardcoded data.
    /// <para>
    /// Nothing here varies: no <c>Guid.NewGuid</c>, no <c>DateTime.UtcNow</c>. Every call produces the
    /// identical payload, so the JSON arms serialize the same number of bytes on every iteration and the
    /// measurement carries no RNG.
    /// </para>
    /// </summary>
    public static BenchmarkMessage CreateSample() => new(
        OrderReference: "ORD-2026-000417",
        Sku: "WIDGET-BLUE-LARGE",
        CustomerName: "Contoso Manufacturing GmbH",
        CustomerEmail: "procurement@contoso.example",
        ShippingAddress: "Hafenstrasse 14, 20457 Hamburg, Germany",
        Notes: "Deliver to loading bay 3. Call ahead - forklift required for pallets over 200 kg.",
        Quantity: 24,
        WarehouseId: 7,
        SequenceNumber: 9_007_199_254L,
        WeightKilograms: 312.75,
        UnitPrice: 149.95m,
        TotalPrice: 3_598.80m,
        DiscountRate: 0.075m,
        OrderedAtUtc: new DateTime(2026, 3, 14, 9, 30, 0, DateTimeKind.Utc),
        RequestedDeliveryOn: new DateTime(2026, 3, 21, 0, 0, 0, DateTimeKind.Utc),
        CreatedAt: new DateTimeOffset(2026, 3, 14, 10, 30, 0, TimeSpan.FromHours(1)),
        CorrelationId: new Guid("3f2504e0-4f89-11d3-9a0c-0305e82c3301"),
        Lines:
        [
            new OrderLine("WIDGET-BLUE-LARGE", "Blue widget, large", 12, 149.95m),
            new OrderLine("WIDGET-RED-SMALL", "Red widget, small", 6, 89.50m),
            new OrderLine("BRACKET-STEEL-M8", "Steel mounting bracket M8", 48, 4.25m),
            new OrderLine("SEAL-NITRILE-40MM", "Nitrile seal, 40 mm", 96, 1.10m),
            new OrderLine("MANUAL-EN-PRINT", "Printed manual, English", 1, 12.00m)
        ],
        AuditTrail:
        [
            new AuditEntry("web-checkout", "Created", new DateTime(2026, 3, 14, 9, 30, 0, DateTimeKind.Utc)),
            new AuditEntry("credit-service", "CreditApproved", new DateTime(2026, 3, 14, 9, 30, 4, DateTimeKind.Utc)),
            new AuditEntry("pricing-service", "DiscountApplied", new DateTime(2026, 3, 14, 9, 30, 6, DateTimeKind.Utc)),
            new AuditEntry("a.schmidt", "ManuallyReviewed", new DateTime(2026, 3, 14, 11, 12, 0, DateTimeKind.Utc))
        ]);
}

/// <summary>
/// The handler under measurement. An ordinary <see cref="IHandleMessages{TMessage}"/> class, exactly as a
/// consuming application would write one - not a closure registered on <c>BuiltinHandlerActivator</c>.
/// </summary>
sealed class BenchmarkMessageHandler(Action<BenchmarkMessage> onHandled) : IHandleMessages<BenchmarkMessage>
{
    // Not async: TreatWarningsAsErrors turns CS1998 (async without await) into a build error.
    public Task Handle(BenchmarkMessage message)
    {
        onHandled(message);
        return Task.CompletedTask;
    }
}

/// <summary>
/// The smallest activator that can serve one handler instance: no container, no reflection, no scanning.
/// <para>
/// Rebus needs <em>some</em> <see cref="IHandlerActivator"/> to turn a message into handler instances, and
/// the benchmark wants none of the machinery a real one brings - a container resolve per dispatch would be
/// measured alongside the transport it is meant to isolate. Returning a pre-built singleton keeps the cost
/// of activation at zero and identical across all three arms.
/// </para>
/// </summary>
sealed class SingleHandlerActivator(object handler) : IHandlerActivator
{
    public Task<IEnumerable<IHandleMessages<TMessage>>> GetHandlers<TMessage>(
        TMessage message, ITransactionContext transactionContext)
    {
        // Rebus asks for handlers of its own internal message types too; those get an empty sequence.
        IEnumerable<IHandleMessages<TMessage>> handlers = handler is IHandleMessages<TMessage> typed
            ? [typed]
            : [];

        return Task.FromResult(handlers);
    }
}

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
    BusArm(IBus bus) => Bus = bus;

    public IBus Bus { get; }

    public static BusArm Create(Arm arm, InProcReceiveMode mode, Action<BenchmarkMessage> onHandled)
    {
        var activator = new SingleHandlerActivator(new BenchmarkMessageHandler(onHandled));

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

        return new BusArm(bus);
    }

    // BuiltinHandlerActivator used to own the bus and dispose it for us. A plain activator owns nothing,
    // so the bus is disposed here - otherwise its worker threads outlive every benchmark iteration.
    public void Dispose() => Bus.Dispose();
}
