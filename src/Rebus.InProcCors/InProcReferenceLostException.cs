namespace Rebus.InProcCors;

/// <summary>
/// Thrown by <c>ReferenceSerializer</c> when neither the transport message subclass nor the weak
/// side table can supply the message instance. Encryption, compression and the data bus claim-check step
/// all construct a new transport message around a different body, defeating both carriers. None of the
/// three is meaningful for in-process traffic (design §9).
/// </summary>
public sealed class InProcReferenceLostException : Exception
{
    /// <summary>
    /// Creates the exception for a message whose <c>rbs2-msg-type</c> header said <paramref name="messageType"/>.
    /// </summary>
    public InProcReferenceLostException(string messageType)
        : base($"Could not recover the message instance for a message of type '{messageType}'. The transport " +
               "message carried neither the reference subclass nor a body registered in the reference table, " +
               "which means the body was replaced somewhere in the pipeline. The usual causes are encryption, " +
               "compression, or the data bus claim-check step - none of which are supported by Rebus.InProcCors, " +
               "and none of which do anything useful for in-process traffic. Remove the offending step, or " +
               "configure this endpoint with an ordinary serializer instead of ReferenceSerializer.")
    {
        MessageType = messageType;
    }

    /// <summary>
    /// Gets the value of the message's type header, or a placeholder when that header was absent.
    /// </summary>
    public string MessageType { get; }
}
