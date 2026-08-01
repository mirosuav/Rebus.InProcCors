namespace Rebus.InProcCors;

/// <summary>
/// Options for <see cref="InProcTransport"/>. Switchable per bus without touching handler code.
/// </summary>
public sealed class InProcTransportOptions
{
    /// <summary>
    /// Gets or sets how the transport waits for the next message. Defaults to
    /// <see cref="InProcReceiveMode.Blocking"/>.
    /// </summary>
    public InProcReceiveMode ReceiveMode { get; set; } = InProcReceiveMode.Blocking;

    /// <summary>
    /// Gets or sets how long a receive waits before reporting no message when
    /// <see cref="ReceiveMode"/> is <see cref="InProcReceiveMode.Polling"/>. Defaults to 100 ms.
    /// Ignored in <see cref="InProcReceiveMode.Blocking"/> mode.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromMilliseconds(100);
}
