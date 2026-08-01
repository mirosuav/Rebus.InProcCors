namespace Rebus.InProcCors.Verification.Tests;

public class RoundTripCheckerTests
{
    public sealed record GoodOrder(string Sku, int Quantity);

    public sealed class UnboundConstructorParameter
    {
        // The parameter 'sku' matches no property, so System.Text.Json cannot bind the constructor and
        // refuses to deserialize the contract at all.
        public UnboundConstructorParameter(int quantity, string sku)
        {
            Quantity = quantity;
            Code = sku;
        }

        public int Quantity { get; }

        public string Code { get; }
    }

    public sealed class RoundTrippableInitProperty
    {
        // init is a compile-time-only restriction; the setter is public IL, so the serializer restores it.
        public RoundTrippableInitProperty(int quantity) => Quantity = quantity;

        public int Quantity { get; }

        public string Sku { get; init; } = "";
    }

    public sealed class PrivateSetterProperty
    {
        public PrivateSetterProperty(int quantity) => Quantity = quantity;

        public int Quantity { get; }

        public string Sku { get; private set; } = "";
    }

    static async Task<List<VerificationViolation>> Check(Type messageType)
    {
        var violations = new List<VerificationViolation>();
        Assert.True(new DefaultMessageInstanceFactory().TryCreate(messageType, out var instance));

        await new RoundTripChecker(new SystemTextJsonContractSerializer())
            .CheckAsync(messageType, instance!, violations);

        return violations;
    }

    [Fact]
    public async Task AWellFormedRecordRoundTripsToIdenticalBytes()
    {
        Assert.Empty(await Check(typeof(GoodOrder)));
    }

    [Fact]
    public async Task AConstructorParameterThatBindsToNoPropertyIsAViolation()
    {
        var violation = Assert.Single(await Check(typeof(UnboundConstructorParameter)));

        Assert.Equal(VerificationCheck.RoundTrip, violation.Check);
        Assert.Equal(typeof(UnboundConstructorParameter), violation.MessageType);
    }

    [Fact]
    public async Task AnInitOnlyPropertyIsRestoredAndSoIsNotAViolation()
    {
        Assert.Empty(await Check(typeof(RoundTrippableInitProperty)));
    }

    [Fact]
    public async Task APrivateSetterIsAViolation()
    {
        // The factory sets Sku through the private setter, the way code inside the owning assembly does.
        // System.Text.Json ignores non-public setters, so the value is gone after the round trip.
        var violation = Assert.Single(await Check(typeof(PrivateSetterProperty)));

        Assert.Equal(VerificationCheck.RoundTrip, violation.Check);
    }

    [Fact]
    public async Task ASerializerThatThrowsBecomesAViolationRatherThanAnEscapingException()
    {
        var violations = new List<VerificationViolation>();

        await new RoundTripChecker(new ThrowingSerializer())
            .CheckAsync(typeof(GoodOrder), new GoodOrder("ABC", 2), violations);

        var violation = Assert.Single(violations);
        Assert.Equal(VerificationCheck.RoundTrip, violation.Check);
        Assert.Contains("nope", violation.Description);
    }

    sealed class ThrowingSerializer : Rebus.Serialization.ISerializer
    {
        public Task<Rebus.Messages.TransportMessage> Serialize(Rebus.Messages.Message message) =>
            throw new NotSupportedException("nope");

        public Task<Rebus.Messages.Message> Deserialize(Rebus.Messages.TransportMessage transportMessage) =>
            throw new NotSupportedException("nope");
    }
}
