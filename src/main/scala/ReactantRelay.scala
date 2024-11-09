package readren.matrix

import scala.collection.MapView

trait ReactantRelay[-U] {
	
	/** thread-safe */
	val admin: MatrixAdmin
	/** thread-safe */
	val endpointProvider: EndpointProvider[U]
	/** thread-safe */
	val path: String

	/** Should be called withing the [[admin]]. */
	def spawn[A](childReactantFactory: ReactantFactory)(initialChildBehaviorBuilder: Reactant[A] => Behavior[A]): admin.Duty[ReactantRelay[A]]

	/** Should be called within the [[admin]]. */
	def getChildren: MapView[Long, ReactantRelay[?]]

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
}
