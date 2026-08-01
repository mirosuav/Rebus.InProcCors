using Rebus.InProcCors;
using Rebus.Serialization;

namespace Rebus.Config;

/// <summary>
/// Configuration extensions for <see cref="ReferenceSerializer"/>.
/// </summary>
public static class ReferenceSerializerConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to pass message objects by reference instead of serializing them. Requires a
    /// transport that preserves the transport message instance and its body array - in practice
    /// <c>UseInProcTransport</c>. When using that method, pass
    /// <c>registerReferenceSerializer: false</c> to it, or Rebus will reject the duplicate registration.
    /// </summary>
    public static void UseReferenceSerializer(this StandardConfigurer<ISerializer> configurer)
    {
        if (configurer == null) throw new ArgumentNullException(nameof(configurer));

        configurer.Register(context => new ReferenceSerializer(context.Get<IMessageTypeNameConvention>()));
    }
}
