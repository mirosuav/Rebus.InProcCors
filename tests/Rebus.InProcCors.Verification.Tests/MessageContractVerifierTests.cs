using Microsoft.Extensions.DependencyInjection;
using Rebus.Handlers;

namespace Rebus.InProcCors.Verification.Tests;

public class MessageContractVerifierTests
{
    public sealed record GoodOrder(string Sku, int Quantity);

    public sealed record AnotherGoodOrder(string Sku);

    public sealed class BadOrder
    {
        public string Sku { get; set; } = "";
    }

    sealed class GoodOrderHandler : IHandleMessages<GoodOrder>
    {
        public Task Handle(GoodOrder message) => Task.CompletedTask;
    }

    sealed class AnotherGoodOrderHandler : IHandleMessages<AnotherGoodOrder>
    {
        public Task Handle(AnotherGoodOrder message) => Task.CompletedTask;
    }

    sealed class BadOrderHandler : IHandleMessages<BadOrder>
    {
        public Task Handle(BadOrder message) => Task.CompletedTask;
    }

    sealed class MultiHandler : IHandleMessages<GoodOrder>, IHandleMessages<AnotherGoodOrder>
    {
        public Task Handle(GoodOrder message) => Task.CompletedTask;
        public Task Handle(AnotherGoodOrder message) => Task.CompletedTask;
    }

    [Fact]
    public void DiscoveryProjectsTheMessageTypeOutOfEveryHandlerRegistration()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();
        services.AddTransient<IHandleMessages<AnotherGoodOrder>, AnotherGoodOrderHandler>();

        var discovered = HandlerMessageTypeDiscovery.Discover([services]);

        Assert.Equal(2, discovered.Count);
        Assert.Contains(typeof(GoodOrder), discovered);
        Assert.Contains(typeof(AnotherGoodOrder), discovered);
    }

    [Fact]
    public void DiscoveryIgnoresRegistrationsThatAreNotHandlerClosures()
    {
        var services = new ServiceCollection();
        services.AddSingleton("not a handler");
        services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();

        Assert.Equal([typeof(GoodOrder)], HandlerMessageTypeDiscovery.Discover([services]));
    }

    [Fact]
    public void DiscoveryDeduplicatesAcrossCollections()
    {
        var first = new ServiceCollection();
        first.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();
        var second = new ServiceCollection();
        second.AddTransient<IHandleMessages<GoodOrder>, MultiHandler>();

        Assert.Equal([typeof(GoodOrder)], HandlerMessageTypeDiscovery.Discover([first, second]));
    }

    [Fact]
    public void AHandlerRegisteredAfterTheVerifierIsConstructedIsStillDiscovered()
    {
        // Design §10: IServiceCollection is a live IList<ServiceDescriptor>, so capturing the instance and
        // reading it later makes registration order irrelevant. Eager enumeration would silently verify a
        // subset - worse than not verifying.
        var services = new ServiceCollection();
        var verifier = MessageContractVerifier.ForHandlersIn(services);

        services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();

        Assert.Equal([typeof(GoodOrder)], verifier.Verify().VerifiedTypes);
    }

    [Fact]
    public void AWellFormedContractSetPasses()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();
        services.AddTransient<IHandleMessages<AnotherGoodOrder>, AnotherGoodOrderHandler>();

        var report = MessageContractVerifier.ForHandlersIn(services).Verify();

        Assert.True(report.IsSuccess, report.Describe());
        Assert.Equal(2, report.VerifiedTypes.Count);
    }

    [Fact]
    public void AMutableContractFailsAndTheReportNamesIt()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<BadOrder>, BadOrderHandler>();

        var report = MessageContractVerifier.ForHandlersIn(services).Verify();

        Assert.False(report.IsSuccess);
        Assert.Contains(report.Violations, v => v.Check == VerificationCheck.Immutability
                                                && v.MessageType == typeof(BadOrder));
        Assert.Contains(nameof(BadOrder), report.Describe());
        Assert.Contains(nameof(BadOrder.Sku), report.Describe());
    }

    [Fact]
    public void VerifyAndThrowThrowsCarryingTheReport()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<BadOrder>, BadOrderHandler>();

        var exception = Assert.Throws<MessageContractVerificationException>(
            () => MessageContractVerifier.ForHandlersIn(services).VerifyAndThrow());

        Assert.False(exception.Report.IsSuccess);
        Assert.Contains(nameof(BadOrder), exception.Message);
    }

    [Fact]
    public void VerifyAndThrowIsSilentWhenEverythingPasses()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<GoodOrder>, GoodOrderHandler>();

        MessageContractVerifier.ForHandlersIn(services).VerifyAndThrow();
    }

    [Fact]
    public void AContractThatCannotBeConstructedIsAConstructionViolationRatherThanASilentPass()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<Unconstructable>, UnconstructableHandler>();

        var report = MessageContractVerifier.ForHandlersIn(services).Verify();

        Assert.Contains(report.Violations, v => v.Check == VerificationCheck.Construction);
    }

    [Fact]
    public void AnInstanceSourceOverridesTheDefaultFactory()
    {
        var services = new ServiceCollection();
        services.AddTransient<IHandleMessages<Unconstructable>, UnconstructableHandler>();

        var report = new MessageContractVerifier([services], new MessageContractVerificationOptions
        {
            InstanceSource = new UnconstructableSource()
        }).Verify();

        Assert.DoesNotContain(report.Violations, v => v.Check == VerificationCheck.Construction);
    }

    public sealed class Unconstructable
    {
        Unconstructable(string sku) => Sku = sku;

        public string Sku { get; }

        internal static Unconstructable Create(string sku) => new(sku);
    }

    sealed class UnconstructableHandler : IHandleMessages<Unconstructable>
    {
        public Task Handle(Unconstructable message) => Task.CompletedTask;
    }

    sealed class UnconstructableSource : IMessageInstanceSource
    {
        public bool TryCreate(Type messageType, out object? instance)
        {
            if (messageType == typeof(Unconstructable))
            {
                instance = Unconstructable.Create("ABC");
                return true;
            }

            instance = null;
            return false;
        }
    }
}
