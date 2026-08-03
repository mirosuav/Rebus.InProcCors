using System.Collections.Concurrent;

namespace Rebus.InProcCors.Sample;

/// <summary>
/// Lets the scenario driver wait for an order to reach a terminal state.
/// <para>
/// This exists because <c>bus.Send</c> is an asynchronous handoff, not a call: it returns as soon as the
/// message is on the queue, long before any handler has run. There is no return value to await, so the
/// driver has to learn the outcome the same way anything else would - by observing the effect. The Orders
/// module completes the signal when it rejects an order or when it sees the <c>OrderShipped</c> event.
/// </para>
/// <para>
/// Note that this is deliberately <em>not</em> shared between modules. Only the driver and the Orders
/// module see it; Inventory and Shipping communicate purely over the bus.
/// </para>
/// </summary>
sealed class ScenarioSignals
{
    readonly ConcurrentDictionary<Guid, TaskCompletionSource<string>> _outcomes = new();

    TaskCompletionSource<string> Slot(Guid orderId) =>
        _outcomes.GetOrAdd(orderId, _ => new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously));

    /// <summary>Awaits the terminal outcome of an order, giving up after <paramref name="timeout"/>.</summary>
    public Task<string> Outcome(Guid orderId, TimeSpan timeout) => Slot(orderId).Task.WaitAsync(timeout);

    /// <summary>Records that an order reached a terminal state.</summary>
    public void Complete(Guid orderId, string outcome) => Slot(orderId).TrySetResult(outcome);
}
