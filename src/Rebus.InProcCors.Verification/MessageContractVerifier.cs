using Microsoft.Extensions.DependencyInjection;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Verifies that every message contract handled by a module is round-trip serializable, and - unless
/// <see cref="MessageContractVerificationOptions.VerifyImmutability"/> is turned off - deeply immutable.
/// Register it per module, so each module polices its own contracts: every message type has its handler in
/// exactly one module by construction, so the host gathers nothing centrally (design §10).
/// </summary>
public sealed class MessageContractVerifier
{
    readonly IReadOnlyList<IServiceCollection> _serviceCollections;
    readonly MessageContractVerificationOptions _options;
    readonly DefaultMessageInstanceFactory _defaultFactory = new();
    readonly ImmutabilityChecker _immutabilityChecker = new();
    readonly RoundTripChecker _roundTripChecker;

    /// <summary>
    /// Creates the verifier over the given collections. The collections are captured, not copied: they are
    /// enumerated inside <see cref="VerifyAsync"/>, so handlers registered after this call are still found.
    /// </summary>
    /// <param name="serviceCollections">The live service collections to discover handlers in.</param>
    /// <param name="options">Optional overrides for the serializer and instance construction.</param>
    public MessageContractVerifier(
        IEnumerable<IServiceCollection> serviceCollections, MessageContractVerificationOptions? options = null)
    {
        if (serviceCollections == null) throw new ArgumentNullException(nameof(serviceCollections));

        _serviceCollections = serviceCollections.ToArray();
        _options = options ?? new MessageContractVerificationOptions();
        _roundTripChecker = new RoundTripChecker(_options.Serializer ?? new SystemTextJsonContractSerializer());
    }

    /// <summary>
    /// Creates a verifier over the given collections. The primitive underneath
    /// <c>AddRebusInProcContractVerification</c>, so the verifier is testable without a host.
    /// </summary>
    /// <param name="serviceCollections">The live service collections to discover handlers in.</param>
    /// <returns>A verifier over those collections.</returns>
    public static MessageContractVerifier ForHandlersIn(params IServiceCollection[] serviceCollections) =>
        new(serviceCollections);

    /// <summary>
    /// Creates a verifier over the given collections with explicit options - the entry point for a test in a
    /// module that turned <see cref="MessageContractVerificationOptions.VerifyImmutability"/> off, which the
    /// options-less overload cannot express. Options come first because <c>params</c> must come last.
    /// </summary>
    /// <param name="options">Overrides for the checks, the serializer and instance construction.</param>
    /// <param name="serviceCollections">The live service collections to discover handlers in.</param>
    /// <returns>A verifier over those collections.</returns>
    public static MessageContractVerifier ForHandlersIn(
        MessageContractVerificationOptions options, params IServiceCollection[] serviceCollections) =>
        new(serviceCollections, options);

    /// <summary>
    /// Runs the enabled checks over every discovered contract and returns the report.
    /// </summary>
    /// <returns>The report describing everything that was checked and everything that failed.</returns>
    public async Task<VerificationReport> VerifyAsync()
    {
        var messageTypes = HandlerMessageTypeDiscovery.Discover(_serviceCollections);
        var violations = new List<VerificationViolation>();

        foreach (var messageType in messageTypes)
        {
            // Only the immutability walk is skipped. The round trip - the check that guards extractability -
            // always runs, so this is never a way to verify nothing.
            if (_options.VerifyImmutability) _immutabilityChecker.Check(messageType, violations);

            if (!TryCreateInstance(messageType, out var instance))
            {
                violations.Add(new VerificationViolation(messageType, VerificationCheck.Construction, "",
                    "could not construct a test instance of this contract. Give it a public constructor, or " +
                    "supply an IMessageInstanceSource that can build it."));
                continue;
            }

            await _roundTripChecker.CheckAsync(messageType, instance!, violations).ConfigureAwait(false);
        }

        return new VerificationReport(messageTypes, violations, _options.VerifyImmutability);
    }

    /// <summary>
    /// Synchronous <see cref="VerifyAsync"/>, for use from a test.
    /// </summary>
    /// <returns>The report describing everything that was checked and everything that failed.</returns>
    public VerificationReport Verify() => VerifyAsync().GetAwaiter().GetResult();

    /// <summary>
    /// Runs the checks and throws <see cref="MessageContractVerificationException"/> if any failed.
    /// </summary>
    /// <exception cref="MessageContractVerificationException">Thrown when any contract failed a check.</exception>
    public void VerifyAndThrow()
    {
        var report = Verify();

        if (!report.IsSuccess) throw new MessageContractVerificationException(report);
    }

    bool TryCreateInstance(Type messageType, out object? instance)
    {
        if (_options.InstanceSource != null && _options.InstanceSource.TryCreate(messageType, out instance))
        {
            return instance != null;
        }

        return _defaultFactory.TryCreate(messageType, out instance);
    }
}
