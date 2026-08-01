using System.Threading.Channels;
using Rebus.Messages;

namespace Rebus.InProcCors;

/// <summary>
/// One queue: an unbounded <see cref="Channel{T}"/> of transport messages plus a depth counter.
/// </summary>
sealed class InProcQueue
{
    readonly Channel<TransportMessage> _channel = Channel.CreateUnbounded<TransportMessage>(
        new UnboundedChannelOptions
        {
            SingleReader = false,               // N workers drain one queue
            SingleWriter = false,               // any module may send to it
            AllowSynchronousContinuations = false
        });

    int _count;

    /// <summary>
    /// Gets the queue depth. Maintained with <see cref="Interlocked"/> rather than read from
    /// <see cref="ChannelReader{T}.Count"/>, to avoid a per-target-framework behavioural split on one
    /// diagnostic property (design §5).
    /// </summary>
    public int Count => Volatile.Read(ref _count);

    public void Enqueue(TransportMessage message)
    {
        // Increment first: an over-report for a few nanoseconds is safe, an under-report is not.
        Interlocked.Increment(ref _count);

        if (!_channel.Writer.TryWrite(message))
        {
            Interlocked.Decrement(ref _count);
            throw new InvalidOperationException(
                "Could not write to an unbounded channel, which should not be possible unless the network has been completed.");
        }
    }

    public bool TryDequeue(out TransportMessage? message)
    {
        if (!_channel.Reader.TryRead(out message)) return false;

        Interlocked.Decrement(ref _count);
        return true;
    }

    /// <summary>
    /// Waits for the next message, returning null when <paramref name="cancellationToken"/> fires.
    /// </summary>
    public async ValueTask<TransportMessage?> DequeueAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
            {
                // WaitToReadAsync completing is not a reservation: another worker may have taken the
                // message already, in which case we go round again.
                if (TryDequeue(out var message)) return message;
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown, or a polling-mode interval expiring. Both mean "no message".
        }

        return null;
    }
}
