package readren.matrix

import Reactant.SerialNumber

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.MapView

abstract class ReactantRelay[-U] {

	/** thread-safe */
	val serial: Reactant.SerialNumber
	/** thread-safe */
	val admin: MatrixAdmin
	/** thread-safe */
	val endpointProvider: EndpointProvider[U]
	/** thread-safe */
	val path: String

	
	/** Indicates whether this [[Reactant]] was marked to be stopped, which does not necessarily mean that the stop process has already started.
	 * 
	 * This method is thread-safe. */
	def isMarkedToBeStopped: Boolean
	
	/** Should be called withing the [[admin]]. */
	def spawn[A](childReactantFactory: ReactantFactory)(initialChildBehaviorBuilder: ReactantRelay[A] => Behavior[A])(using isSignalTest: IsSignalTest[A]): admin.Duty[ReactantRelay[A]]

	/** Should be called within the [[admin]]. */
	def children: MapView[Long, ReactantRelay[?]]

	/**
	 * Instructs to stop this [[Reactant]].
	 * Supports being called from anywhere at any moment and many times.
	 * If this reactant is processing a message when this method is called, the process of that single message will continue but no other message will be processed after it.
	 * It is not necessary to trigger the execution of the returned [[Duty]] to start the stop process. The result can be ignored.
	 * thread-safe
	 * @return a [[Duty]] that completes when this [[Reactant]] is fully stopped. */
	def stop(): admin.Duty[Unit]
	
	/** A duty that completes when the [[Reactant]] behind this relay is fully stopped (after [[StopReceived]] signal was emitted and handled.
	 * This duty complete at the same time as the [[Duty]] returned by [[stop]].
 	 * thread-safe */
	def stopDuty: admin.Duty[Unit]

	/** Registers this [[Reactant]] to be notified with the received signal when the specified child is fully stopped.
	 * The notification is not sent to this [[Reactant]]'s [[Receiver]] as a regular messages. It behaves like signals: an execution of {{{behavior.handle(childStoppedSignal)}}} is queued directly in the task-queue of this [[Reactant.admin]]'s executor.
	 * 
	 * Should be called within the [[admin]].
	 * @return true if the registering is made, and false if the child is already fully stopped or does not exist. */
	def watchChild[CSS <: U](childSerial: SerialNumber, childStoppedSignal: CSS): Boolean

	/** Registers this [[Reactant]] to be notified with the received signal when the specified [[ReactantRelay]] is fully stopped.
	 * The notification is not sent to this [[Reactant]]'s [[Receiver]] as a regular messages. It behaves like signals: an execution of {{{behavior.handle(childStoppedSignal)}}} is queued directly in the task-queue of this [[Reactant.admin]]'s executor.
	 * 
	 * This method is thread-safe: may be called from any thread at any time. */
	def watch[SS <: U](watchedReactant: ReactantRelay[?], stoppedSignal: SS): Unit
	
	/** Returns debug info about this reactant. */
	def diagnose: admin.Duty[ReactantDiagnostic]
}
