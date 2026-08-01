using Microsoft.Extensions.DependencyInjection;
using Rebus.Handlers;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Finds message contracts by pulling <c>T</c> out of every registered <see cref="IHandleMessages{TMessage}"/>
/// closure. That is ground truth - no naming convention, no list to maintain, and a message with no handler
/// is dead code anyway.
/// <para>
/// Reads <see cref="IServiceCollection"/> rather than a built <see cref="IServiceProvider"/>, because a
/// provider cannot enumerate its own registrations and only the type shape is needed.
/// </para>
/// </summary>
public static class HandlerMessageTypeDiscovery
{
    /// <summary>
    /// Enumerates <paramref name="serviceCollections"/> now and returns the distinct message types found.
    /// Call this at verification time, not at registration time: the collections are live lists, so reading
    /// them late makes registration order irrelevant.
    /// </summary>
    /// <param name="serviceCollections">The live service collections to read.</param>
    /// <returns>The distinct message contract types that have a registered handler.</returns>
    public static IReadOnlyList<Type> Discover(IEnumerable<IServiceCollection> serviceCollections)
    {
        if (serviceCollections == null) throw new ArgumentNullException(nameof(serviceCollections));

        return serviceCollections
            .SelectMany(collection => collection)
            .Select(descriptor => descriptor.ServiceType)
            .Where(serviceType => serviceType.IsConstructedGenericType
                                  && serviceType.GetGenericTypeDefinition() == typeof(IHandleMessages<>))
            .Select(serviceType => serviceType.GetGenericArguments()[0])
            .Distinct()
            .ToArray();
    }
}
