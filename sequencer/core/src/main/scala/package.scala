package readren

package object sequencer {
	/** A time based on the [[System.nanoTime]] method converted to milliseconds. */
	type MilliTime = Long
	/** A time based on the [[System.nanoTime]]. */
	type NanoTime = Long

	/** A duration in milliseconds. */
	type MilliDuration = Long


	inline def nanosToMillisRoundedUp(nanos: Long): Long = (nanos + 999_999) / 1_000_000

	inline def nanosToMillisRoundedDown(nanos: Long): Long = nanos / 1_000_000


	trait MonotonicClock {
		val InitialValue: MilliTime

		val MaxValue: MilliTime

		def currentTimeRoundedDown: MilliTime

		def currentTimeRoundedUp: MilliTime

		/** Suspends the current [[Thread]] indefinitely, assuming the current [[Thread]] is the owner of the provided object's monitor.
		 * The provided duration is always greater than zero. */
		def suspend(lockObject: Object): Unit
		/** Suspends the current [[Thread]] for the provided duration, assuming the current [[Thread]] is the owner of the provided object's monitor.
		 * The provided duration is always greater than zero. */
		def suspend(lockObject: Object, duration: MilliDuration): Unit
	}
	
	class NanoTimeBasedMilliClock extends MonotonicClock {
		override val InitialValue: MilliTime = currentTimeRoundedDown

		// TODO either change this value to Long.MaxValue or make MilliTime opaque and define the comparision methdos using substraction.
		override val MaxValue: MilliTime = InitialValue + Long.MaxValue // Note that setting the max value this way forces MilliTime comparisons to be implemented with substracton `(a - b) > 0` instead of `a > b`.

		override def currentTimeRoundedDown: MilliTime = nanosToMillisRoundedDown(System.nanoTime)

		override def currentTimeRoundedUp: MilliTime = nanosToMillisRoundedUp(System.nanoTime)

		override def suspend(lockObject: Object): Unit = lockObject.wait()

		override def suspend(lockObject: Object, duration: MilliDuration): Unit = lockObject.wait(duration)
	}
}
