using Rebus.Messages;
using Rebus.Serialization;

namespace Rebus.InProcCors;

/// <summary>
/// An <see cref="ISerializer"/> that does not serialize. It attaches the live message object to the
/// transport message by two independent carriers and hands the identical instance back on the way in.
/// <para>
/// The transport and the serializer are independent: <c>InProcTransport</c> carries any transport
/// message, so pairing it with an ordinary JSON serializer is valid and supported.
/// </para>
/// </summary>
public sealed class ReferenceSerializer : ISerializer
{
    /// <summary>
    /// The value written to the <c>rbs2-content-type</c> header. Present so that anything reading headers -
    /// logging, the error queue, a human - can see at a glance that no bytes were produced.
    /// </summary>
    public const string ReferenceContentType = "application/x-rebus-inproc-reference";

    const string UnknownMessageType = "(no rbs2-msg-type header)";

    readonly IMessageTypeNameConvention _messageTypeNameConvention;

    /// <summary>
    /// Creates the serializer, using <paramref name="messageTypeNameConvention"/> to populate the
    /// <c>rbs2-msg-type</c> header exactly as an ordinary serializer would.
    /// </summary>
    public ReferenceSerializer(IMessageTypeNameConvention messageTypeNameConvention)
    {
        _messageTypeNameConvention = messageTypeNameConvention
                                     ?? throw new ArgumentNullException(nameof(messageTypeNameConvention));
    }

    /// <inheritdoc />
    public Task<TransportMessage> Serialize(Message message)
    {
        if (message == null) throw new ArgumentNullException(nameof(message));

        var body = message.Body;
        var headers = new Dictionary<string, string>(message.Headers);

        headers[Headers.ContentType] = ReferenceContentType;

        if (!headers.ContainsKey(Headers.Type))
        {
            headers[Headers.Type] = _messageTypeNameConvention.GetTypeName(body.GetType());
        }

        var sentinel = MessageReferenceTable.CreateSentinel();
        MessageReferenceTable.Register(sentinel, body);

        return Task.FromResult<TransportMessage>(new ReferenceTransportMessage(headers, sentinel, body));
    }

    /// <inheritdoc />
    /// <exception cref="InProcReferenceLostException">
    /// Thrown when neither carrier can supply the instance - see design §9.
    /// </exception>
    public Task<Message> Deserialize(TransportMessage transportMessage)
    {
        if (transportMessage == null) throw new ArgumentNullException(nameof(transportMessage));

        var instance = ResolveInstance(transportMessage);
        var headers = new Dictionary<string, string>(transportMessage.Headers);

        return Task.FromResult(new Message(headers, instance));
    }

    static object ResolveInstance(TransportMessage transportMessage)
    {
        // 1. The common path: a type check, no table access.
        if (transportMessage is ReferenceTransportMessage reference) return reference.MessageInstance;

        // 2. The reconstructed path - dead-letter, deferral, auditing, auto-forward-on-exception.
        //    Clone() and DueMessage both pass the same Body array through, so identity rides on it.
        if (MessageReferenceTable.TryResolve(transportMessage.Body, out var instance) && instance != null)
        {
            return instance;
        }

        // 3. The body was replaced. Fail loudly, here, rather than let a null surface inside a handler.
        var messageType = transportMessage.Headers.TryGetValue(Headers.Type, out var type)
            ? type
            : UnknownMessageType;

        throw new InProcReferenceLostException(messageType);
    }
}
