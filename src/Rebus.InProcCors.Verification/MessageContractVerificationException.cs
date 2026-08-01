namespace Rebus.InProcCors.Verification;

/// <summary>
/// Thrown by <see cref="MessageContractVerifier.VerifyAndThrow"/> and by the startup check outside
/// Production when one or more contracts failed verification.
/// </summary>
public sealed class MessageContractVerificationException : Exception
{
    /// <summary>
    /// Creates the exception carrying <paramref name="report"/>, whose description becomes the message.
    /// </summary>
    /// <param name="report">The report describing what failed.</param>
    public MessageContractVerificationException(VerificationReport report)
        : base("One or more message contracts failed verification." + Environment.NewLine + report.Describe())
    {
        Report = report;
    }

    /// <summary>
    /// Gets the full report, including exemptions.
    /// </summary>
    public VerificationReport Report { get; }
}
