package readren.matrix

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.MapView

trait ReactantRelay[-U] {

	/** thread-safe */
	val serial: Reactant.SerialNumber
	/** thread-safe */
	val admin: MatrixAdmin
	/** thread-safe */
	val endpointProvider: EndpointProvider[U]
	/** thread-safe */
	val path: String

	/** Tells if this [[Reactant]] is ready to be stimulated to process a message.
	 * Should be set to false whenever [[isMarkedToStop]] is set to true. */
	protected val isReadyToProcessAMsg: AtomicBoolean
	
	/** Tells if this [[Reactant]] was marked to be stopped.
	 * CAUTION: Should never be true when [[isReadyToProcessAMsg]] is true. Set it to false before setting this member to true. */
	protected val isMarkedToStop: AtomicBoolean

	/** Tells if this [[Reactant]] is ready to be stimulated to process a message. */
	inline def isReady: Boolean = isReadyToProcessAMsg.get

	/** Tells if this [[Reactant]] was marked to be stopped. */
	inline def isBeingStopped: Boolean = isMarkedToStop.get
	
	/** Should be called withing the [[admin]]. */
	def spawn[A](childReactantFactory: ReactantFactory)(initialChildBehaviorBuilder: ReactantRelay[A] => Behavior[A]): admin.Duty[ReactantRelay[A]]

	/** Should be called within the [[admin]]. */
	def children: MapView[Long, ReactantRelay[?]]

	/**
	 * Instructs to stop this [[Reactant]].
	 * Supports being called at any moment.
	 * thread-safe
	 * @return a [[Duty]] that completes when this [[Reactant]] is fully stopped. */
	def stop(): admin.Duty[Unit]
	
	/** A duty that completes when the [[Reactant]] behind this relay is fully stopped (after [[StopReceived]] signal was emitted and handled.
	 * This duty complete at the same time as the [[Duty]] returned by [[stop]].
 	 * thread-safe */
	def stopDuty: admin.Duty[Unit]

	/** Registers this [[Reactant]] to be notified with a [[ChildStopped]] signal when the specified child is fully stopped.
	 * Should be called within the [[admin]].
	 * @return true if the registering is made, and false if the child is already fully stopped or does not exist. */
	def watch(childSerial: Reactant.SerialNumber): Boolean
	
}
