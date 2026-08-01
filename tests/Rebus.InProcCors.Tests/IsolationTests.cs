using Microsoft.Extensions.DependencyInjection;
using Rebus.Handlers;

namespace Rebus.InProcCors.Tests;

public class IsolationTests
{
    public sealed record PlaceOrder(string Sku);

    sealed class ModuleSecret
    {
        public string Value { get; init; } = "";
    }

    /// <summary>
    /// A handler that reports the secret its own module's container holds. Resolved from the handler
    /// module's provider, so what it can see is exactly what that module registered.
    /// </summary>
    sealed class SecretReportingHandler : IHandleMessages<PlaceOrder>
    {
        readonly ModuleSecret? _secret;
        readonly TaskCompletionSource<string?> _observed;

        public SecretReportingHandler(ModuleSecret? secret, TaskCompletionSource<string?> observed)
        {
            _secret = secret;
            _observed = observed;
        }

        public Task Handle(PlaceOrder message)
        {
            _observed.TrySetResult(_secret?.Value);
            return Task.CompletedTask;
        }
    }

    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task TwoNetworksDoNotObserveEachOthersTraffic(InProcReceiveMode mode)
    {
        // Design §11 item 5, and why there is no static default network (§5).
        var networkA = new InProcNetwork();
        var networkB = new InProcNetwork();
        var receivedOnB = false;
        var receivedOnA = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var moduleA = TestModule.Create(networkA, "orders", mode,
            configureHandlers: a => a.Handle<PlaceOrder>(_ =>
            {
                receivedOnA.TrySetResult();
                return Task.CompletedTask;
            }));
        using var moduleB = TestModule.Create(networkB, "orders", mode,
            configureHandlers: a => a.Handle<PlaceOrder>(_ =>
            {
                receivedOnB = true;
                return Task.CompletedTask;
            }));

        await moduleA.Bus.SendLocal(new PlaceOrder("ABC"));
        await receivedOnA.Task.WaitAsync(Timeout);
        await Task.Delay(200);

        Assert.False(receivedOnB);
    }

    [Theory]
    [InlineData(InProcReceiveMode.Blocking)]
    [InlineData(InProcReceiveMode.Polling)]
    public async Task AHandlerCannotSeeTheCallersRegistrations(InProcReceiveMode mode)
    {
        // Design §11 item 6. This is the difference from MediatR that the README says is the entire point:
        // the handler is resolved from its own module's container, so the caller cannot leak a DbContext,
        // an ambient transaction, or any of its own registrations into it.
        var network = new InProcNetwork();
        var observed = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var caller = TestModule.Create(network, "caller", mode,
            configureServices: s => s.AddSingleton(new ModuleSecret { Value = "caller-secret" }));

        using var handlerModule = TestModule.Create(network, "orders", mode,
            configureServices: s =>
            {
                s.AddSingleton(new ModuleSecret { Value = "orders-secret" });
                s.AddSingleton(observed);
            },
            registerHandlers: (activator, provider) => activator.Register(() =>
                new SecretReportingHandler(
                    provider.GetService<ModuleSecret>(),
                    provider.GetRequiredService<TaskCompletionSource<string?>>())));

        // The two containers are genuinely separate objects with separate registrations.
        Assert.Equal("caller-secret", caller.Services.GetRequiredService<ModuleSecret>().Value);
        Assert.Equal("orders-secret", handlerModule.Services.GetRequiredService<ModuleSecret>().Value);

        await caller.Bus.Advanced.Routing.Send("orders", new PlaceOrder("ABC"));

        // The handler saw its own module's registration - never the caller's.
        Assert.Equal("orders-secret", await observed.Task.WaitAsync(Timeout));
    }
}
