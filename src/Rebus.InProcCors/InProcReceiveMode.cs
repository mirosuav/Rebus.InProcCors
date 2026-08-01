namespace Rebus.InProcCors;

/// <summary>
/// How <see cref="InProcTransport"/> waits for the next message.
/// </summary>
public enum InProcReceiveMode
{
    /// <summary>
    /// Park on the queue until a message arrives. Avoids the 100-250 ms wake-up latency that Rebus's
    /// backoff ladder imposes on the first message after an idle period, which is the dominant traffic
    /// shape of a modular monolith. The Rebus worker loop tolerates this: ThreadPoolWorker starts the
    /// receive without awaiting it, so a parked receive does not stall the loop (design §2.1).
    /// </summary>
    Blocking,

    /// <summary>
    /// Wait at most <see cref="InProcTransportOptions.PollingInterval"/> and then report no message,
    /// dropping Rebus back into its normal backoff ladder.
    /// </summary>
    Polling
}
