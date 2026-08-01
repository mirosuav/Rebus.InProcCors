namespace Rebus.InProcCors.Verification;

/// <summary>
/// Exempts a type or member from the deep-immutability check. The reason is mandatory and non-empty:
/// exemptions do not fail the report, they are listed in it, so they appear in test output on every run
/// and in code review as an attribute carrying a written justification (design §10).
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Property | AttributeTargets.Field,
    Inherited = false)]
public sealed class ImmutabilityExemptAttribute : Attribute
{
    /// <summary>
    /// Creates the exemption with a mandatory justification.
    /// </summary>
    /// <param name="reason">The written justification for the exemption.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="reason"/> is null, empty or whitespace.</exception>
    public ImmutabilityExemptAttribute(string reason)
    {
        if (string.IsNullOrWhiteSpace(reason))
        {
            throw new ArgumentException(
                "An immutability exemption must carry a non-empty reason. It is going to be printed in test " +
                "output on every run and read in code review, so write the justification down.", nameof(reason));
        }

        Reason = reason;
    }

    /// <summary>
    /// Gets the written justification for this exemption.
    /// </summary>
    public string Reason { get; }
}
