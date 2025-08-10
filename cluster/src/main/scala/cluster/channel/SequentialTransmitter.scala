package readren.matrix
package cluster.channel

import cluster.channel.Transmitter.{Context, Report}
import cluster.serialization.{ProtocolVersion, Serializer}

import readren.sequencer.Maybe

import java.lang.invoke.VarHandle
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

/**
 * A thread-safe wrapper around [[Transmitter]] that allows multiple messages to be queued for sequential transmission.
 * 
 * The underlying [[Transmitter]] only supports one transmission at a time. This class provides a queue-based solution that ensures all messages are transmitted in the order they were submitted, even when multiple threads call `transmit()` concurrently.
 * 
 * == Design Overview ==
 * 
 * The implementation uses a combination of:
 * - An [[AtomicInteger]] to track the queue size and transmission state
 * - A [[ConcurrentLinkedQueue]] to store pending messages
 * - Memory barriers to handle visibility issues in concurrent scenarios
 * 
 * == Race Condition Handling ==
 * 
 * A key challenge is ensuring that when a transmission completes and `queueSize.decrementAndGet() > 0`, the next message can be reliably retrieved from the queue. The `poll()` method may return `null` even when elements exist due to memory visibility issues between threads.
 * 
 * This is solved using a `while` loop with `VarHandle.fullFence()` to ensure the polling thread can see the memory updates made by the thread that added the element to the queue.
 * 
 * @param channel the underlying channel for transmission
 * @param context the transmission context
 * @param buffersCapacity the capacity of transmission buffers
 * @tparam M the type of messages to transmit
 */
class SequentialTransmitter[M](channel: AsynchronousSocketChannel, val context: Context, buffersCapacity: Int = 8192) {

	private val transmitter = new Transmitter(channel, context, buffersCapacity)

	/**
	 * Tracks the number of messages in the queue plus one if a transmission is currently in progress.
	 * This counter ensures proper synchronization between concurrent calls to `transmit()`.
	 * 
	 * The counter is:
	 * - Incremented before adding a message to the queue or starting transmission
	 * - Decremented after a transmission completes
	 * - Used to determine if more messages are waiting to be processed
	 */
	private val queueSize: AtomicInteger = new AtomicInteger()
	
	/**
	 * Queue of pending messages and their completion callbacks.
	 * Messages are processed in FIFO order.
	 */
	private val transmissionQueue: ConcurrentLinkedQueue[(message: M, onComplete: Transmitter.Report => Unit)] = new ConcurrentLinkedQueue()

	/**
	 * Transmits a message, either immediately or by adding it to the queue for later processing.
	 * 
	 * If no transmission is currently in progress, the message is transmitted immediately.
	 * Otherwise, the message is added to the queue and will be processed when the current transmission completes.
	 * 
	 * @param message the message to transmit
	 * @param msgVersion the protocol version for the message
	 * @param behindTransmissionEnabled whether to enable behind-transmission optimization
	 * @param timeout the transmission timeout
	 * @param timeUnit the timeout unit
	 * @param onTransmissionComplete callback invoked when transmission completes
	 * @param serializer the serializer for the message type
	 */
	def transmit(message: M, msgVersion: ProtocolVersion, behindTransmissionEnabled: Boolean = true, timeout: Long = 1, timeUnit: TimeUnit = TimeUnit.SECONDS)(onTransmissionComplete: Report => Unit)(using serializer: Serializer[M]): Unit = {
		// Check if this is the first message (no transmission in progress)
		if queueSize.getAndIncrement() == 0 then {
			// Start transmission immediately
			transmitter.transmit[M](message, msgVersion, behindTransmissionEnabled, timeout, timeUnit) { report =>
				// After transmission completes, check if more messages are waiting
				if queueSize.decrementAndGet() > 0 then {
					// Process the next message from the queue
					var next = transmissionQueue.poll()
					
					// Handle potential memory visibility issues with concurrent collections.
					// The poll() method may return null even when elements exist due to memory visibility between threads. The VarHandle.fullFence() ensures the polling thread can see updates made by the adding thread.
					while next == null do {
						VarHandle.fullFence()
						next = transmissionQueue.poll()
					}
					
					// Recursively transmit the next message
					transmit(next.message, msgVersion, behindTransmissionEnabled, timeout, timeUnit)(next.onComplete)
				} else onIdleHandler()
				// Always invoke the completion callback for the current message
				onTransmissionComplete(report)
			}
		} else {
			// Add a message to queue for later processing
			transmissionQueue.add((message, onTransmissionComplete))
		}
	}


	@volatile private var onIdleHandler: () => Unit = () => ()

	/**
	 * Programs the onIdle handler to be called when no transmission is in progress and the queue is empty.
	 * 
	 * The handler will be called exactly once when the idle condition is met, and then reset to a no-op.
	 * If the idle condition is already met when this method is called, the handler will be executed immediately.
	 * 
	 * == Race Condition Note ==
	 * 
	 * The callback may be called after the transmitter stops being idle if the `transmit` method is called concurrently by other thread after the call to `triggerOnIdle`.
	 * 
	 * @param onIdle the handler to execute when idle
	 */
	def triggerOnIdle(onIdle: () => Unit): Unit = {
		val handlerToExecute = synchronized {
			val prev = onIdleHandler
			this.onIdleHandler = () => {
				prev()
				onIdle()
				synchronized {
					onIdleHandler = () => ()
				}
			}
			this.onIdleHandler
		}
		if queueSize.get() == 0 then handlerToExecute()
	}
}
