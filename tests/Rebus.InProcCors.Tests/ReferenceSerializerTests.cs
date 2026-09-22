using Rebus.Messages;
using Rebus.Serialization;

namespace Rebus.InProcCors.Tests;

public class ReferenceSerializerTests
{
    sealed record PlaceOrder(string Sku, int Quantity);

    sealed class TestTypeNameConvention : IMessageTypeNameConvention
    {
        public string GetTypeName(Type type) => type.FullName!;
        public Type GetType(string name) => Type.GetType(name)!;
    }

    static ReferenceSerializer CreateSerializer() => new(new TestTypeNameConvention());

    static Message LogicalMessage(object body) => new(new Dictionary<string, string>(), body);

    [Fact]
    public async Task SerializeProducesAReferenceTransportMessageWithAFreshSentinelBody()
    {
        var serializer = CreateSerializer();
        var order = new PlaceOrder("ABC", 2);

        var first = await serializer.Serialize(LogicalMessage(order));
        var second = await serializer.Serialize(LogicalMessage(order));

        var reference = Assert.IsType<ReferenceTransportMessage>(first);
        Assert.Same(order, reference.MessageInstance);
        Assert.Single(first.Body);
        Assert.False(ReferenceEquals(first.Body, second.Body));
    }

    [Fact]
    public async Task SerializePopulatesTheTypeAndContentTypeHeaders()
    {
        var serializer = CreateSerializer();

        var transportMessage = await serializer.Serialize(LogicalMessage(new PlaceOrder("ABC", 2)));

        Assert.Equal(typeof(PlaceOrder).FullName, transportMessage.Headers[Headers.Type]);
        Assert.Equal(ReferenceSerializer.ReferenceContentType, transportMessage.Headers[Headers.ContentType]);
    }

    [Fact]
    public async Task SerializePreservesCallerSuppliedHeadersWithoutMutatingTheInput()
    {
        var serializer = CreateSerializer();
        var headers = new Dictionary<string, string> { [Headers.MessageId] = "abc" };
        var message = new Message(headers, new PlaceOrder("ABC", 2));

        var transportMessage = await serializer.Serialize(message);

        Assert.Equal("abc", transportMessage.Headers[Headers.MessageId]);
        Assert.False(ReferenceEquals(headers, transportMessage.Headers));
        Assert.False(headers.ContainsKey(Headers.ContentType));
    }

    [Fact]
    public async Task SerializeDoesNotOverwriteAnExplicitTypeHeader()
    {
        var serializer = CreateSerializer();
        var headers = new Dictionary<string, string> { [Headers.Type] = "Explicit.Type.Name" };

        var transportMessage = await serializer.Serialize(new Message(headers, new PlaceOrder("ABC", 2)));

        Assert.Equal("Explicit.Type.Name", transportMessage.Headers[Headers.Type]);
    }

    [Fact]
    public async Task DeserializeReturnsTheIdenticalInstanceViaTheSubclass()
    {
        var serializer = CreateSerializer();
        var order = new PlaceOrder("ABC", 2);

        var transportMessage = await serializer.Serialize(LogicalMessage(order));
        var roundTripped = await serializer.Deserialize(transportMessage);

        Assert.Same(order, roundTripped.Body);
    }

    [Fact]
    public async Task DeserializeReturnsTheIdenticalInstanceAfterTheSubclassIsLost()
    {
        var serializer = CreateSerializer();
        var order = new PlaceOrder("ABC", 2);

        var transportMessage = await serializer.Serialize(LogicalMessage(order));
        var cloned = new TransportMessage(new Dictionary<string, string>(transportMessage.Headers), transportMessage.Body);

        var roundTripped = await serializer.Deserialize(cloned);

        Assert.Same(order, roundTripped.Body);
    }

    [Fact]
    public async Task DeserializeCopiesTheHeadersOntoTheLogicalMessage()
    {
        var serializer = CreateSerializer();
        var transportMessage = await serializer.Serialize(
            new Message(new Dictionary<string, string> { [Headers.MessageId] = "abc" }, new PlaceOrder("ABC", 2)));

        var roundTripped = await serializer.Deserialize(transportMessage);

        Assert.Equal("abc", roundTripped.Headers[Headers.MessageId]);
        Assert.False(ReferenceEquals(transportMessage.Headers, roundTripped.Headers));
    }

    [Fact]
    public async Task DeserializeThrowsWhenBothCarriersMiss()
    {
        var serializer = CreateSerializer();
        var headers = new Dictionary<string, string> { [Headers.Type] = typeof(PlaceOrder).FullName! };
        var replacedBody = new TransportMessage(headers, new byte[] { 1, 2, 3 });

        var exception = await Assert.ThrowsAsync<InProcReferenceLostException>(
            () => serializer.Deserialize(replacedBody));

        Assert.Equal(typeof(PlaceOrder).FullName, exception.MessageType);
        Assert.Contains("encryption", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("compression", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("data bus", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task DeserializeReportsAPlaceholderWhenTheTypeHeaderIsAbsent()
    {
        var serializer = CreateSerializer();

        var exception = await Assert.ThrowsAsync<InProcReferenceLostException>(
            () => serializer.Deserialize(new TransportMessage(new Dictionary<string, string>(), new byte[] { 1 })));

        Assert.Equal("(no rbs2-msg-type header)", exception.MessageType);
    }

    [Fact]
    public async Task DeserializeNamesTheSerializerMismatchWhenTheContentTypeIsForeign()
    {
        var serializer = CreateSerializer();
        var headers = new Dictionary<string, string>
        {
            [Headers.Type] = typeof(PlaceOrder).FullName!,
            [Headers.ContentType] = "application/json;charset=utf-8"
        };

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => serializer.Deserialize(new TransportMessage(headers, new byte[] { 123, 125 })));

        Assert.Contains("application/json", exception.Message);
        Assert.Contains("same serializer", exception.Message);
    }
}
