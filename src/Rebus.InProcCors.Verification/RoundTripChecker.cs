using Rebus.Messages;
using Rebus.Serialization;

namespace Rebus.InProcCors.Verification;

/// <summary>
/// Serializes, deserializes, serializes again, and compares the two bodies as bytes.
/// <para>
/// Comparing serialized output rather than object graphs is deliberate: a recursive graph comparer produces
/// false positives on collection ordering, <c>DateTime</c> precision and floating-point precision - exactly
/// the failure mode being avoided. Comparing two byte arrays has none of those problems and requires no
/// comparer to be written.
/// </para>
/// </summary>
public sealed class RoundTripChecker
{
    readonly ISerializer _serializer;

    /// <summary>
    /// Creates the checker over the serializer the extracted service will use.
    /// </summary>
    /// <param name="serializer">The serializer whose byte output is compared.</param>
    public RoundTripChecker(ISerializer serializer) =>
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));

    /// <summary>
    /// Checks <paramref name="instance"/>, appending anything it finds to <paramref name="violations"/>.
    /// A serializer that throws becomes a violation rather than an escaping exception, so one bad contract
    /// does not hide the rest of the report.
    /// </summary>
    /// <param name="messageType">The contract being checked.</param>
    /// <param name="instance">A filled instance of that contract.</param>
    /// <param name="violations">Receives anything the round trip could not restore.</param>
    /// <returns>A task that completes when the check is done.</returns>
    public async Task CheckAsync(Type messageType, object instance, ICollection<VerificationViolation> violations)
    {
        if (messageType == null) throw new ArgumentNullException(nameof(messageType));
        if (instance == null) throw new ArgumentNullException(nameof(instance));

        try
        {
            var first = await _serializer.Serialize(new Message(new Dictionary<string, string>(), instance))
                .ConfigureAwait(false);

            var roundTripped = await _serializer.Deserialize(first).ConfigureAwait(false);

            var second = await _serializer.Serialize(new Message(new Dictionary<string, string>(), roundTripped.Body))
                .ConfigureAwait(false);

            if (first.Body.AsSpan().SequenceEqual(second.Body)) return;

            violations.Add(new VerificationViolation(messageType, VerificationCheck.RoundTrip, "",
                "serializing, deserializing and serializing again did not produce identical bytes, which means " +
                "the serializer cannot restore part of this contract. The usual causes are an init-only or " +
                "get-only property with no matching constructor parameter, a private setter, or an " +
                "interface-typed property that deserializes to its base type. " +
                $"First: {Preview(first.Body)}. Second: {Preview(second.Body)}"));
        }
        catch (Exception exception)
        {
            violations.Add(new VerificationViolation(messageType, VerificationCheck.RoundTrip, "",
                $"the serializer threw while round-tripping this contract: {exception.Message}"));
        }
    }

    static string Preview(byte[] body)
    {
        var text = System.Text.Encoding.UTF8.GetString(body);
        return text.Length <= 300 ? text : text[..300] + "...";
    }
}
