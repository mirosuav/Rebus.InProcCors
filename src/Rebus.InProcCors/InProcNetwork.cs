using System.Collections.Concurrent;
using Rebus.Messages;

namespace Rebus.InProcCors;

/// <summary>
/// The set of queues and subscriptions that a group of in-process buses share. Construct one per host
/// and pass it explicitly to each module's transport configuration. There is deliberately no static
/// default instance: a singleton would make two independent test fixtures silently share a network
/// (design §5).
/// </summary>
public sealed class InProcNetwork
{
    readonly ConcurrentDictionary<string, InProcQueue> _queues = new(StringComparer.OrdinalIgnoreCase);

    readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _subscribers =
        new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Gets the names of all queues that currently exist on this network.
    /// </summary>
    public IEnumerable<string> Queues => _queues.Keys;

    /// <summary>
    /// Creates the queue with the given address if it does not already exist.
    /// </summary>
    public void CreateQueue(string address) => GetOrCreateQueue(address);

    /// <summary>
    /// Gets whether a queue with the given address exists on this network.
    /// </summary>
    public bool HasQueue(string address) => _queues.ContainsKey(address);

    /// <summary>
    /// Delivers <paramref name="message"/> to the queue named <paramref name="destinationAddress"/>,
    /// creating that queue if it does not exist. The message instance is enqueued unchanged.
    /// </summary>
    public void Deliver(string destinationAddress, TransportMessage message)
    {
        if (destinationAddress == null) throw new ArgumentNullException(nameof(destinationAddress));
        if (message == null) throw new ArgumentNullException(nameof(message));

        GetOrCreateQueue(destinationAddress).Enqueue(message);
    }

    /// <summary>
    /// Gets the number of messages waiting in the queue with the given address.
    /// </summary>
    public int GetCount(string address) =>
        _queues.TryGetValue(address, out var queue) ? queue.Count : 0;

    /// <summary>
    /// Deletes all queues, their messages, and all subscriptions.
    /// </summary>
    public void Reset()
    {
        _queues.Clear();
        _subscribers.Clear();
    }

    /// <summary>
    /// Registers <paramref name="subscriberAddress"/> as a subscriber of <paramref name="topic"/>.
    /// </summary>
    public void AddSubscriber(string topic, string subscriberAddress) =>
        _subscribers.GetOrAdd(topic, _ => new ConcurrentDictionary<string, byte>(StringComparer.OrdinalIgnoreCase))
            .TryAdd(subscriberAddress, 0);

    /// <summary>
    /// Unregisters <paramref name="subscriberAddress"/> as a subscriber of <paramref name="topic"/>.
    /// </summary>
    public void RemoveSubscriber(string topic, string subscriberAddress)
    {
        if (_subscribers.TryGetValue(topic, out var set)) set.TryRemove(subscriberAddress, out _);
    }

    /// <summary>
    /// Gets the addresses subscribed to <paramref name="topic"/>.
    /// </summary>
    public IReadOnlyList<string> GetSubscribers(string topic) =>
        _subscribers.TryGetValue(topic, out var set) ? set.Keys.ToArray() : Array.Empty<string>();

    internal InProcQueue GetOrCreateQueue(string address)
    {
        if (address == null) throw new ArgumentNullException(nameof(address));

        return _queues.GetOrAdd(address, _ => new InProcQueue());
    }
}
