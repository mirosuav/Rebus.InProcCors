using Rebus.Messages;
using Rebus.Serialization;
using Rebus.Transport;

namespace Rebus.InProcCors.Tests;

public class FailurePathTests
{
    public sealed record PlaceOrder(string Sku, int Quantity);

    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task AThrowingHandlerRetriesAndTheMessageIsStillReadableInTheErrorQueue(InProcReceiveMode mode)
    {
        // Design §11 item 2, and the correction of §2.2: DeadletterQueueErrorHandler calls Clone(), which
        // discards the subclass. Only the weak table can recover the instance here, so this test is the
        // one that proves the fallback is load-bearing rather than defensive.
        var network = new InProcNetwork();
        var attempts = 0;

        using var module = TestModule.Create(network, "orders", mode, maxDeliveryAttempts: 3,
            configureHandlers: a => a.Handle<PlaceOrder>(_ =>
            {
                Interlocked.Increment(ref attempts);
                throw new InvalidOperationException("handler blew up");
            }));

        var sent = new PlaceOrder("ABC", 2);
        await module.Bus.SendLocal(sent);

        await WaitUntil(() => network.GetCount("error") == 1, Timeout);
        Assert.Equal(3, Volatile.Read(ref attempts));

        // Read it out of the error queue and deserialize it, exactly as a replay tool would.
        var errorTransport = new InProcTransport(network, "error");
        errorTransport.Initialize();
        using var scope = new RebusTransactionScope();
        var deadLettered = await errorTransport.Receive(scope.TransactionContext, CancellationToken.None);
        await scope.CompleteAsync();

        Assert.NotNull(deadLettered);
        Assert.IsNotType<ReferenceTransportMessage>(deadLettered);  // Clone() dropped the subclass

        var serializer = new ReferenceSerializer(new SimpleTypeNameConvention());
        var recovered = await serializer.Deserialize(deadLettered!);

        Assert.Same(sent, recovered.Body);
        Assert.True(deadLettered!.Headers.ContainsKey(Headers.ErrorDetails));
    }

    [Fact]
    public async Task DeserializingAMessageWhoseBodyWasReplacedThrowsWithAUsefulMessage()
    {
        // Design §11 item 10.
        var serializer = new ReferenceSerializer(new SimpleTypeNameConvention());
        var headers = new Dictionary<string, string> { [Headers.Type] = typeof(PlaceOrder).FullName! };

        var exception = await Assert.ThrowsAsync<InProcReferenceLostException>(
            () => serializer.Deserialize(new TransportMessage(headers, new byte[] { 9, 9, 9 })));

        Assert.Contains(typeof(PlaceOrder).FullName!, exception.Message);
        Assert.Contains("ReferenceSerializer", exception.Message);
    }

    static async Task WaitUntil(Func<bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (condition()) return;
            await Task.Delay(25);
        }

        throw new TimeoutException($"Condition was not satisfied within {timeout}.");
    }

    sealed class SimpleTypeNameConvention : IMessageTypeNameConvention
    {
        public string GetTypeName(Type type) => type.FullName!;
        public Type GetType(string name) => Type.GetType(name)!;
    }
}
