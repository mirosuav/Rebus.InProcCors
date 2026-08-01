using Rebus.Messages;

namespace Rebus.InProcCors;

/// <summary>
/// A <see cref="TransportMessage"/> that additionally carries the live message object. This is the fast
/// carrier; it is lost wherever Rebus reconstructs the transport message (dead-lettering, deferral),
/// which is what <see cref="MessageReferenceTable"/> exists to cover.
/// </summary>
sealed class ReferenceTransportMessage : TransportMessage
{
    public ReferenceTransportMessage(Dictionary<string, string> headers, byte[] sentinel, object messageInstance)
        : base(headers, sentinel)
    {
        MessageInstance = messageInstance ?? throw new ArgumentNullException(nameof(messageInstance));
    }

    public object MessageInstance { get; }
}
