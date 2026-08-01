using Rebus.Serialization;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Options for <see cref="MessageContractVerifier"/> and the startup check.
/// </summary>
public sealed class MessageContractVerificationOptions
{
    /// <summary>
    /// Gets or sets whether to run the check at host startup. Defaults to true. Outside the Production
    /// environment a failure throws, failing local startup and the build; in Production it is logged at
    /// error level and startup continues (design §10).
    /// </summary>
    public bool VerifyOnStartup { get; set; } = true;

    /// <summary>
    /// Gets or sets the serializer the round-trip check uses - it should be the one the extracted service
    /// will use. Defaults to <see cref="SystemTextJsonContractSerializer"/>.
    /// </summary>
    public ISerializer? Serializer { get; set; }

    /// <summary>
    /// Gets or sets a per-type override for contracts <see cref="DefaultMessageInstanceFactory"/> cannot
    /// construct. Consulted first; the default factory handles whatever it declines.
    /// </summary>
    public IMessageInstanceSource? InstanceSource { get; set; }
}
