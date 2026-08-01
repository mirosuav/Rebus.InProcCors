namespace Rebus.InProcCors.Verification;

/// <summary>
/// Supplies a test instance for a message contract. Implement this to override construction for a contract
/// that <see cref="DefaultMessageInstanceFactory"/> cannot build, and set it on
/// <c>MessageContractVerificationOptions.InstanceSource</c>; it is consulted first, and the default factory
/// handles whatever it declines.
/// </summary>
public interface IMessageInstanceSource
{
    /// <summary>
    /// Attempts to create an instance of <paramref name="messageType"/>. Return false to decline.
    /// </summary>
    /// <param name="messageType">The contract to build.</param>
    /// <param name="instance">The created instance, or null when declining.</param>
    /// <returns>True when an instance was produced.</returns>
    bool TryCreate(Type messageType, out object? instance);
}
