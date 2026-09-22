using System.Threading.Channels;
using Rebus.Messages;

namespace Rebus.InProcCors;

/// <summary>
/// One queue: a <see cref="Channel{T}"/> of transport messages plus a depth counter. Unbounded by default;
/// with a <c>maxLength</c> it keeps only the newest messages, dropping the oldest instead of blocking the sender.
/// </summary>
sealed class InProcQueue
{
    readonly Channel<TransportMessage> _channel;

    int _count;
    volatile bool _completed;

    public InProcQueue(int? maxLength = null)
    {
        MaxLength = maxLength;

        _channel = maxLength == null
            ? Channel.CreateUnbounded<TransportMessage>(new UnboundedChannelOptions
            {
                SingleReader = false,               // N workers drain one queue
                SingleWriter = false,               // any module may send to it
                AllowSynchronousContinuations = false
            })
            : Channel.CreateBounded<TransportMessage>(new BoundedChannelOptions(maxLength.Value)
                {
                    SingleReader = false,
                    SingleWriter = false,
                    AllowSynchronousContinuations = false,
                    // Never Wait: a sender blocked on a full queue it also drains would deadlock itself.
                    FullMode = BoundedChannelFullMode.DropOldest
                },
                // The dropped message was counted on the way in; the reference goes with it, so its body can be
                // collected.
                _ => Interlocked.Decrement(ref _count));
    }

    /// <summary>
    /// Gets the maximum number of messages kept, or null when the queue is unbounded.
    /// </summary>
    public int? MaxLength { get; }

    /// <summary>
    /// Gets the queue depth. Maintained with <see cref="Interlocked"/> rather than read from
    /// <see cref="ChannelReader{T}.Count"/>, to avoid a per-target-framework behavioural split on one
    /// diagnostic property (design §5).
    /// </summary>
    public int Count => Volatile.Read(ref _count);

    /// <summary>
    /// Enqueues <paramref name="message"/> - dropping the oldest message first when a bounded queue is full -
    /// and returns false only when the queue has been completed by
    /// <see cref="InProcNetwork.Reset"/> - the caller should then deliver to the queue's replacement.
    /// </summary>
    public bool TryEnqueue(TransportMessage message)
    {
        // Increment first: an over-report for a few nanoseconds is safe, an under-report is not.
        Interlocked.Increment(ref _count);

        if (_channel.Writer.TryWrite(message)) return true;

        Interlocked.Decrement(ref _count);
        return false;
    }

    /// <summary>
    /// Marks the queue as deleted: rejects further writes, discards what is waiting, and wakes every parked
    /// receiver so it can return empty-handed and pick up the replacement queue on its next receive.
    /// </summary>
    public void Complete()
    {
        _completed = true;
        _channel.Writer.TryComplete();
    }

    public bool TryDequeue(out TransportMessage? message)
    {
        if (_completed)
        {
            message = null;
            return false;
        }

        if (!_channel.Reader.TryRead(out message)) return false;

        Interlocked.Decrement(ref _count);
        return true;
    }

    /// <summary>
    /// Waits for the next message, returning null when <paramref name="cancellationToken"/> fires or the
    /// queue is completed.
    /// </summary>
    public async ValueTask<TransportMessage?> DequeueAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!_completed && await _channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
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
