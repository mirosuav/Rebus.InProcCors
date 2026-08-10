using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Runs the contract check at boot. Outside Production a failure throws, failing local startup and the
/// build - the fastest possible feedback, and it catches the developer who never wrote the test. In
/// Production it logs at error level and continues, because a violation reaching production was already in
/// a build that passed CI, and a reflection check must never be the cause of an outage (design §10).
/// </summary>
sealed class ContractVerificationHostedService : IHostedService
{
    readonly MessageContractVerifier _verifier;
    readonly IHostEnvironment _environment;
    readonly ILogger<ContractVerificationHostedService> _logger;

    public ContractVerificationHostedService(
        MessageContractVerifier verifier,
        IHostEnvironment environment,
        ILogger<ContractVerificationHostedService> logger)
    {
        _verifier = verifier;
        _environment = environment;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var report = await _verifier.VerifyAsync().ConfigureAwait(false);

        if (!report.ImmutabilityVerified)
        {
            // Said out loud at every boot: a module that opted out verified less than the default, and a
            // silent green log would be indistinguishable from a module that verified everything.
            _logger.LogInformation(
                "Message contract verification checked round-trip serializability only for {Count} contract(s); " +
                "the immutability check is disabled for this module.", report.VerifiedTypes.Count);
        }

        if (report.IsSuccess) return;

        if (_environment.IsProduction())
        {
            _logger.LogError("Message contract verification failed: {Report}", report.Describe());
            return;
        }

        throw new MessageContractVerificationException(report);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
