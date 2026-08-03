using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Logging;
using Rebus.Routing;

namespace Rebus.InProcCors.Sample;

/// <summary>
/// One module: its own DI container, its own <see cref="BuiltinHandlerActivator"/>, its own bus, all
/// three sharing a single <see cref="InProcNetwork"/> with the other modules.
/// <para>
/// The private container is the point. A module's handlers resolve from a provider that no other module
/// holds a reference to, so no module can reach into another's registrations - the only thing they share
/// is the network, and the only way across it is a message. That is what makes the boundary real rather
/// than conventional, and it is why extracting a module later is a transport change and nothing else.
/// </para>
/// <para>
/// Rebus 8.9.2's <see cref="BuiltinHandlerActivator"/> has no <c>UseServiceProvider</c> - that lives in the
/// separate Rebus.ServiceProvider package, which this repository deliberately does not depend on - so the
/// container link is made explicitly in <see cref="Handler{THandler}"/>.
/// </para>
/// </summary>
abstract class ModuleHost : IHostedService, IDisposable
{
    readonly InProcNetwork _network;
    ServiceProvider? _provider;
    BuiltinHandlerActivator? _activator;
    IBus? _bus;

    protected ModuleHost(InProcNetwork network, string queueName, ConsoleColor color)
    {
        _network = network;
        QueueName = queueName;
        Log = new ConsoleLog(queueName, color);
    }

    /// <summary>This module's input queue, and the address other modules route to.</summary>
    public string QueueName { get; }

    /// <summary>This module's bus. Valid only after <see cref="StartAsync"/> has completed.</summary>
    public IBus Bus => _bus ?? throw new InvalidOperationException($"Module '{QueueName}' has not started yet.");

    protected ConsoleLog Log { get; }

    /// <summary>Registers this module's own services. The base registers <see cref="ConsoleLog"/>.</summary>
    protected virtual void ConfigureServices(IServiceCollection services) { }

    /// <summary>Registers this module's handler types, normally by calling <see cref="Handler{THandler}"/>.</summary>
    protected abstract void RegisterHandlers(BuiltinHandlerActivator activator, IServiceProvider provider);

    /// <summary>Maps outgoing message types to other modules' queues. Only senders need this.</summary>
    protected virtual void ConfigureRouting(StandardConfigurer<IRouter> routing) { }

    /// <summary>Adjusts bus options. See <c>OrdersModule</c> for why worker count matters here.</summary>
    protected virtual void ConfigureOptions(OptionsConfigurer options) { }

    /// <summary>Subscribes to events, after the bus has started.</summary>
    protected virtual Task SubscribeAsync() => Task.CompletedTask;

    /// <summary>
    /// Wires a handler type so that Rebus constructs it from <em>this module's</em> container. Constructor
    /// dependencies - including <see cref="IBus"/> - are resolved normally; nothing is passed positionally.
    /// </summary>
    protected static void Handler<THandler>(BuiltinHandlerActivator activator, IServiceProvider provider)
        where THandler : class, IHandleMessages
        => activator.Register(() => ActivatorUtilities.CreateInstance<THandler>(provider));

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var services = new ServiceCollection();
        services.AddSingleton(Log);

        // The chicken-and-egg: the bus does not exist until Start() returns, but handler registrations must
        // be in place before that. Registering a factory that reads the field later resolves it, because
        // handlers are constructed at dispatch time - by which point _bus is assigned.
        services.AddSingleton<IBus>(_ => Bus);

        ConfigureServices(services);
        _provider = services.BuildServiceProvider();

        _activator = new BuiltinHandlerActivator();
        RegisterHandlers(_activator, _provider);

        _bus = Configure.With(_activator)
            .Transport(t => t.UseInProcTransport(_network, QueueName))
            // Rebus's default console logging would drown out this sample's transcript, but silencing it
            // entirely hides genuine failures - a handler that throws surfaces nowhere near the call site,
            // because Send is an asynchronous handoff. Warnings and errors only.
            .Logging(l => l.ColoredConsole(LogLevel.Warn))
            .Routing(ConfigureRouting)
            .Options(ConfigureOptions)
            .Start();

        // Subscriptions must be in place before anyone publishes. The generic host starts hosted services
        // sequentially in registration order, and ScenarioDriver is registered last, so by the time it runs
        // every module below has completed this method.
        await SubscribeAsync();

        Log.Write($"module started on queue '{QueueName}'");
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        Log.Write("module stopping");
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        // Disposing the activator disposes the bus it created.
        _activator?.Dispose();
        _provider?.Dispose();
    }
}
