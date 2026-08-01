using System.Runtime.CompilerServices;

namespace Rebus.InProcCors;

/// <summary>
/// Side table mapping a message's sentinel <c>Body</c> array to the live message instance.
/// Keyed weakly, so an entry lives exactly as long as its sentinel array and there is no cleanup
/// path to forget. See design §4 for why this is a <see cref="ConditionalWeakTable{TKey,TValue}"/>
/// rather than a <c>ConcurrentDictionary</c>.
/// </summary>
static class MessageReferenceTable
{
    static readonly ConditionalWeakTable<byte[], object> Table = new();

    /// <summary>
    /// Allocates a fresh single-byte array to act as a message's <c>Body</c> and its table key.
    /// One byte rather than zero, because <see cref="Array.Empty{T}"/> is a shared singleton and
    /// every message's key must be a distinct object.
    /// </summary>
    public static byte[] CreateSentinel() => new byte[1];

    public static void Register(byte[] sentinel, object instance) => Table.Add(sentinel, instance);

    public static bool TryResolve(byte[] sentinel, out object? instance) => Table.TryGetValue(sentinel, out instance);
}
