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
    /// Gets or sets whether the deep-immutability check runs. Defaults to true. Turning it off is the one
    /// escape hatch, and it is deliberately all-or-nothing per module: a legacy module whose hundreds of
    /// plain-DTO contracts will never be immutable keeps the round-trip check, which is the one that guards
    /// extractability. There is no per-type exemption - either a module's contracts are all immutable, or
    /// nobody checks and <see cref="VerificationReport.Describe"/> says so out loud (design §10).
    /// <para>
    /// The round-trip check has no switch. A verifier that checks nothing should not be registered at all.
    /// </para>
    /// </summary>
    public bool VerifyImmutability { get; set; } = true;

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
