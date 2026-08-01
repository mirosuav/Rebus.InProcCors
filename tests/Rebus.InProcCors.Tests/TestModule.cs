using Microsoft.Extensions.DependencyInjection;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Retry.Simple;

namespace Rebus.InProcCors.Tests;

/// <summary>
/// One module: its own container, its own bus, on a shared network. Mirrors the per-module DI isolation
/// the README describes - the caller cannot reach into another module's registrations.
/// </summary>
sealed class TestModule : IDisposable
{
    readonly ServiceProvider _provider;
    readonly BuiltinHandlerActivator _activator;

    TestModule(ServiceProvider provider, BuiltinHandlerActivator activator, IBus bus)
    {
        _provider = provider;
        _activator = activator;
        Bus = bus;
    }

    public IBus Bus { get; }

    public IServiceProvider Services => _provider;

    /// <summary>
    /// Builds a module. Handlers are registered either as inline lambdas (<paramref name="configureHandlers"/>)
    /// or as types resolved from this module's own provider (<paramref name="registerHandlers"/>).
    /// <para>
    /// Rebus 8.9.2's <see cref="BuiltinHandlerActivator"/> has no <c>UseServiceProvider</c> - that lives in the
    /// separate Rebus.ServiceProvider package - so the container link is made explicitly through
    /// <see cref="BuiltinHandlerActivator.Register{THandler}(Func{THandler})"/>, which is the point either way:
    /// a handler resolves from its own module's provider and can see nothing else.
    /// </para>
    /// </summary>
    public static TestModule Create(
        InProcNetwork network,
        string inputQueue,
        InProcReceiveMode mode,
        Action<IServiceCollection>? configureServices = null,
        Action<BuiltinHandlerActivator>? configureHandlers = null,
        Action<BuiltinHandlerActivator, IServiceProvider>? registerHandlers = null,
        Action<OptionsConfigurer>? configureOptions = null,
        int maxDeliveryAttempts = 5)
    {
        var services = new ServiceCollection();
        configureServices?.Invoke(services);
        var provider = services.BuildServiceProvider();

        var activator = new BuiltinHandlerActivator();
        configureHandlers?.Invoke(activator);
        registerHandlers?.Invoke(activator, provider);

        var bus = Configure.With(activator)
            .Transport(t => t.UseInProcTransport(network, inputQueue, o =>
            {
                o.ReceiveMode = mode;
                o.PollingInterval = TimeSpan.FromMilliseconds(50);
            }))
            .Options(o =>
            {
                o.RetryStrategy(maxDeliveryAttempts: maxDeliveryAttempts, errorQueueName: "error");
                configureOptions?.Invoke(o);
            })
            .Start();

        return new TestModule(provider, activator, bus);
    }

    public void Dispose()
    {
        _activator.Dispose();
        _provider.Dispose();
    }
}
