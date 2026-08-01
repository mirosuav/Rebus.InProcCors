using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Registration for the message contract verification library.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="MessageContractVerifier"/> as a singleton over this module's own
    /// <see cref="IServiceCollection"/>, and - unless
    /// <see cref="MessageContractVerificationOptions.VerifyOnStartup"/> is turned off - a hosted service that
    /// runs the check at boot.
    /// <para>
    /// Call this on each module's collection. Every message type has its handler in exactly one module by
    /// construction, so each module polices its own contracts and the host gathers nothing centrally.
    /// </para>
    /// <para>
    /// Registration order does not matter: the collection is captured and enumerated later, inside
    /// <c>Verify()</c>, so handlers registered below this call are still discovered.
    /// </para>
    /// </summary>
    /// <param name="services">The module's service collection.</param>
    /// <param name="configureOptions">Optional configuration of the verification options.</param>
    /// <returns>The same collection, for chaining.</returns>
    public static IServiceCollection AddRebusInProcContractVerification(
        this IServiceCollection services,
        Action<MessageContractVerificationOptions>? configureOptions = null)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));

        var options = new MessageContractVerificationOptions();
        configureOptions?.Invoke(options);

        // Captured, not enumerated: this is the live IList<ServiceDescriptor>.
        var verifier = new MessageContractVerifier([services], options);

        services.TryAddSingleton(verifier);

        if (options.VerifyOnStartup)
        {
            services.AddSingleton<IHostedService, ContractVerificationHostedService>();
        }

        return services;
    }
}
