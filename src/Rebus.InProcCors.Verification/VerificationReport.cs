using System.Text;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Which check produced a violation.
/// </summary>
public enum VerificationCheck
{
    /// <summary>A test instance of the contract could not be constructed.</summary>
    Construction,

    /// <summary>Serialize, deserialize, serialize again did not produce identical bytes.</summary>
    RoundTrip,

    /// <summary>The contract's reachable object graph is not deeply immutable.</summary>
    Immutability
}

/// <summary>
/// One failed check against one message contract.
/// </summary>
/// <param name="MessageType">The contract that failed.</param>
/// <param name="Check">Which check failed.</param>
/// <param name="Member">The offending member path, or an empty string when the whole type failed.</param>
/// <param name="Description">What is wrong, in a sentence.</param>
public sealed record VerificationViolation(Type MessageType, VerificationCheck Check, string Member, string Description);

/// <summary>
/// The outcome of verifying a set of message contracts.
/// </summary>
public sealed class VerificationReport
{
    /// <summary>
    /// Creates the report.
    /// </summary>
    /// <param name="verifiedTypes">The contracts that were checked.</param>
    /// <param name="violations">The failed checks.</param>
    /// <param name="immutabilityVerified">Whether the deep-immutability check ran.</param>
    public VerificationReport(
        IReadOnlyList<Type> verifiedTypes,
        IReadOnlyList<VerificationViolation> violations,
        bool immutabilityVerified)
    {
        VerifiedTypes = verifiedTypes;
        Violations = violations;
        ImmutabilityVerified = immutabilityVerified;
    }

    /// <summary>Gets the contracts that were checked.</summary>
    public IReadOnlyList<Type> VerifiedTypes { get; }

    /// <summary>Gets the failed checks.</summary>
    public IReadOnlyList<VerificationViolation> Violations { get; }

    /// <summary>
    /// Gets whether the deep-immutability check ran. False when
    /// <see cref="MessageContractVerificationOptions.VerifyImmutability"/> was turned off, in which case a
    /// report with no violations means the contracts round-trip - not that they are immutable.
    /// </summary>
    public bool ImmutabilityVerified { get; }

    /// <summary>Gets whether every contract passed every check.</summary>
    public bool IsSuccess => Violations.Count == 0;

    /// <summary>
    /// Renders the report as human-readable text, suitable for a test failure message or a log entry.
    /// </summary>
    /// <returns>The rendered report.</returns>
    public string Describe()
    {
        var builder = new StringBuilder();

        builder.Append("Verified ").Append(VerifiedTypes.Count).Append(" message contract(s)");

        // A quiet report from a module that checked half as much must not read like a clean bill of health.
        builder.AppendLine(ImmutabilityVerified ? "." : " (round trip only; immutability check disabled).");

        if (Violations.Count == 0)
        {
            builder.AppendLine("No violations.");
        }
        else
        {
            builder.Append(Violations.Count).AppendLine(" violation(s):");

            foreach (var violation in Violations)
            {
                builder.Append("  [").Append(violation.Check).Append("] ").Append(violation.MessageType.FullName);

                if (!string.IsNullOrEmpty(violation.Member)) builder.Append('.').Append(violation.Member);

                builder.Append(" - ").AppendLine(violation.Description);
            }
        }

        return builder.ToString();
    }
}
