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
/// One <see cref="ImmutabilityExemptAttribute"/> encountered during the walk.
/// </summary>
/// <param name="MessageType">The contract the exemption was found on or under.</param>
/// <param name="Member">The exempted member path, or an empty string when the whole type is exempt.</param>
/// <param name="Reason">The written justification.</param>
public sealed record VerificationExemption(Type MessageType, string Member, string Reason);

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
    /// <param name="exemptions">The exemptions encountered.</param>
    public VerificationReport(
        IReadOnlyList<Type> verifiedTypes,
        IReadOnlyList<VerificationViolation> violations,
        IReadOnlyList<VerificationExemption> exemptions)
    {
        VerifiedTypes = verifiedTypes;
        Violations = violations;
        Exemptions = exemptions;
    }

    /// <summary>Gets the contracts that were checked.</summary>
    public IReadOnlyList<Type> VerifiedTypes { get; }

    /// <summary>Gets the failed checks.</summary>
    public IReadOnlyList<VerificationViolation> Violations { get; }

    /// <summary>Gets the exemptions encountered. These do not fail the report.</summary>
    public IReadOnlyList<VerificationExemption> Exemptions { get; }

    /// <summary>Gets whether every contract passed every check.</summary>
    public bool IsSuccess => Violations.Count == 0;

    /// <summary>
    /// Renders the report as human-readable text, suitable for a test failure message or a log entry.
    /// </summary>
    /// <returns>The rendered report.</returns>
    public string Describe()
    {
        var builder = new StringBuilder();

        builder.Append("Verified ").Append(VerifiedTypes.Count).AppendLine(" message contract(s).");

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

        if (Exemptions.Count > 0)
        {
            builder.Append(Exemptions.Count).AppendLine(" exemption(s), listed but not failed:");

            foreach (var exemption in Exemptions)
            {
                builder.Append("  ").Append(exemption.MessageType.FullName);

                if (!string.IsNullOrEmpty(exemption.Member)) builder.Append('.').Append(exemption.Member);

                builder.Append(" - ").AppendLine(exemption.Reason);
            }
        }

        return builder.ToString();
    }
}
