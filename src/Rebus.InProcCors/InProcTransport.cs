using Rebus.Bus;
using Rebus.Messages;
using Rebus.Subscriptions;
using Rebus.Transport;

namespace Rebus.InProcCors;

/// <summary>
/// A transport that moves <see cref="TransportMessage"/> instances between queues on an
/// <see cref="InProcNetwork"/> without copying them. Deriving from <see cref="AbstractRebusTransport"/>
/// gives ambient-transaction deferral for free and, importantly, preserves the message reference: the
/// base class enqueues the caller's instance unchanged and flushes on commit (design §2.7).
/// </summary>
public sealed class InProcTransport : AbstractRebusTransport, IInitializable, ITransportInspector, ISubscriptionStorage
{
    readonly InProcNetwork _network;
    readonly string? _inputQueueAddress;
    readonly InProcTransportOptions _options;

    /// <summary>
    /// Creates the transport on the given <paramref name="network"/>. Pass null for
    /// <paramref name="inputQueueAddress"/> to make a one-way client.
    /// </summary>
    public InProcTransport(InProcNetwork network, string? inputQueueAddress, InProcTransportOptions? options = null)
        : base(inputQueueAddress!)
    {
        _network = network ?? throw new ArgumentNullException(nameof(network));
        _inputQueueAddress = inputQueueAddress;
        _options = options ?? new InProcTransportOptions();
    }

    /// <inheritdoc />
    public override void CreateQueue(string address) => _network.CreateQueue(address);

    /// <summary>
    /// Creates this transport's own input queue. Does nothing for a one-way client.
    /// </summary>
    public void Initialize()
    {
        if (_inputQueueAddress == null) return;

        CreateQueue(_inputQueueAddress);
    }

    /// <inheritdoc />
    protected override async Task SendOutgoingMessages(
        IEnumerable<OutgoingTransportMessage> outgoingMessages, ITransactionContext context)
    {
        // Publish hands one instance to every subscriber (SendOutgoingMessageStep), and some receive steps -
        // routing slips, forwarding on error - write to the incoming headers in place. The first destination
        // gets the instance unchanged; each further one gets its own headers dictionary, so concurrent
        // receivers never share a mutable Dictionary. The body and message object stay shared. The set is
        // only allocated once a commit carries a second message, keeping the single-send path free.
        TransportMessage? first = null;
        HashSet<TransportMessage>? delivered = null;

        foreach (var message in outgoingMessages)
        {
            var transportMessage = message.TransportMessage;

            if (first == null)
            {
                first = transportMessage;
            }
            else
            {
                delivered ??= new HashSet<TransportMessage>(ReferenceEqualityComparer.Instance) { first };

                if (!delivered.Add(transportMessage)) transportMessage = WithOwnHeaders(transportMessage);
            }

            _network.Deliver(message.DestinationAddress, transportMessage);
        }

        await Task.CompletedTask;
    }

    static TransportMessage WithOwnHeaders(TransportMessage message)
    {
        var headers = new Dictionary<string, string>(message.Headers);

        return message is ReferenceTransportMessage reference
            ? new ReferenceTransportMessage(headers, reference.Body, reference.MessageInstance)
            : new TransportMessage(headers, message.Body);
    }

    /// <inheritdoc />
    public override async Task<TransportMessage?> Receive(ITransactionContext context, CancellationToken cancellationToken)
    {
        if (context == null) throw new ArgumentNullException(nameof(context));

        if (_inputQueueAddress == null)
        {
            throw new InvalidOperationException(
                "This in-proc transport was initialized without an input queue, so it cannot receive anything.");
        }

        var queue = _network.GetOrCreateQueue(_inputQueueAddress);
        var message = await ReceiveNext(queue, cancellationToken).ConfigureAwait(false);

        if (message == null) return null;

        // No peek-and-requeue on a channel, so a nack goes to the tail. InMemTransport.cs:53 does the same,
        // but this differs from a broker that redelivers in place (design §5).
        context.OnNack(_ =>
        {
            _network.Deliver(_inputQueueAddress, message);
            return Task.CompletedTask;
        });

        return message;
    }

    async ValueTask<TransportMessage?> ReceiveNext(InProcQueue queue, CancellationToken cancellationToken)
    {
        if (_options.ReceiveMode == InProcReceiveMode.Blocking)
        {
            return await queue.DequeueAsync(cancellationToken).ConfigureAwait(false);
        }

        if (queue.TryDequeue(out var immediate)) return immediate;

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(_options.PollingInterval);

        // DequeueAsync swallows cancellation and returns null, which is exactly what an expired
        // polling interval means: no message, drop back into Rebus's backoff ladder.
        return await queue.DequeueAsync(cts.Token).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<Dictionary<string, object>> GetProperties(CancellationToken cancellationToken)
    {
        if (_inputQueueAddress == null)
        {
            throw new InvalidOperationException("Cannot get the message count of a one-way transport.");
        }

        await Task.CompletedTask;

        return new Dictionary<string, object>
        {
            [TransportInspectorPropertyKeys.QueueLength] = _network.GetCount(_inputQueueAddress).ToString()
        };
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> GetSubscriberAddresses(string topic)
    {
        await Task.CompletedTask;
        return _network.GetSubscribers(topic);
    }

    /// <inheritdoc />
    public async Task RegisterSubscriber(string topic, string subscriberAddress)
    {
        await Task.CompletedTask;
        _network.AddSubscriber(topic, subscriberAddress);
    }

    /// <inheritdoc />
    public async Task UnregisterSubscriber(string topic, string subscriberAddress)
    {
        await Task.CompletedTask;
        _network.RemoveSubscriber(topic, subscriberAddress);
    }

    /// <summary>
    /// Always true - the network is a single shared object, so subscriptions are established directly.
    /// </summary>
    public bool IsCentralized => true;
}
