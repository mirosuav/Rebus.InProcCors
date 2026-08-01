using Rebus.InProcCors;
using Rebus.Serialization;
using Rebus.Subscriptions;
using Rebus.Transport;

namespace Rebus.Config;

/// <summary>
/// Configuration extensions for the in-proc transport.
/// </summary>
public static class InProcTransportConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to deliver and receive messages on the given <paramref name="network"/>, using
    /// <paramref name="inputQueueName"/> as this endpoint's input queue.
    /// </summary>
    /// <param name="configurer">The transport configurer.</param>
    /// <param name="network">The host-owned network shared by the modules that talk to each other.</param>
    /// <param name="inputQueueName">This endpoint's input queue.</param>
    /// <param name="configureOptions">Optional callback for receive mode and polling interval.</param>
    /// <param name="registerSubscriptionStorage">
    /// When true (the default) the network doubles as subscription storage, so pub/sub needs no extra
    /// configuration. Pass false when registering a subscription storage explicitly.
    /// </param>
    /// <param name="registerReferenceSerializer">
    /// When true (the default) <see cref="ReferenceSerializer"/> is registered as the endpoint's serializer.
    /// Pass false when calling <c>.Serialization(...)</c> explicitly - Rebus throws on a duplicate primary
    /// registration, so the two cannot both be present.
    /// </param>
    public static void UseInProcTransport(
        this StandardConfigurer<ITransport> configurer,
        InProcNetwork network,
        string inputQueueName,
        Action<InProcTransportOptions>? configureOptions = null,
        bool registerSubscriptionStorage = true,
        bool registerReferenceSerializer = true)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (network == null) throw new ArgumentNullException(nameof(network));
        if (inputQueueName == null) throw new ArgumentNullException(nameof(inputQueueName));

        Register(configurer, network, inputQueueName, configureOptions, registerSubscriptionStorage,
            registerReferenceSerializer);

        configurer.OtherService<ITransportInspector>().Register(context => context.Get<InProcTransport>());
    }

    /// <summary>
    /// Configures Rebus to send on the given <paramref name="network"/> as a one-way client, with no input
    /// queue and no workers.
    /// </summary>
    /// <param name="configurer">The transport configurer.</param>
    /// <param name="network">The host-owned network shared by the modules that talk to each other.</param>
    /// <param name="configureOptions">Optional callback for receive mode and polling interval.</param>
    /// <param name="registerSubscriptionStorage">See <see cref="UseInProcTransport"/>.</param>
    /// <param name="registerReferenceSerializer">See <see cref="UseInProcTransport"/>.</param>
    public static void UseInProcTransportAsOneWayClient(
        this StandardConfigurer<ITransport> configurer,
        InProcNetwork network,
        Action<InProcTransportOptions>? configureOptions = null,
        bool registerSubscriptionStorage = true,
        bool registerReferenceSerializer = true)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));
        if (network == null) throw new ArgumentNullException(nameof(network));

        Register(configurer, network, inputQueueName: null, configureOptions, registerSubscriptionStorage,
            registerReferenceSerializer);

        OneWayClientBackdoor.ConfigureOneWayClient(configurer);
    }

    static void Register(
        StandardConfigurer<ITransport> configurer,
        InProcNetwork network,
        string? inputQueueName,
        Action<InProcTransportOptions>? configureOptions,
        bool registerSubscriptionStorage,
        bool registerReferenceSerializer)
    {
        var options = new InProcTransportOptions();
        configureOptions?.Invoke(options);

        configurer.OtherService<InProcTransport>()
            .Register(_ => new InProcTransport(network, inputQueueName, options));

        if (registerSubscriptionStorage)
        {
            configurer.OtherService<ISubscriptionStorage>().Register(context => context.Get<InProcTransport>());
        }

        if (registerReferenceSerializer)
        {
            configurer.OtherService<ISerializer>()
                .Register(context => new ReferenceSerializer(context.Get<IMessageTypeNameConvention>()));
        }

        configurer.Register(context => context.Get<InProcTransport>());
    }
}
