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

		def currentTimeTimeRoundedUp: MilliTime
	}

	class NanoTimeBasedMilliClock extends MonotonicClock {
		override val InitialValue: MilliTime = currentTimeRoundedDown

		override val MaxValue: MilliTime = InitialValue + Long.MaxValue

		override def currentTimeRoundedDown: MilliTime = nanosToMillisRoundedDown(System.nanoTime)

		override def currentTimeTimeRoundedUp: MilliTime = nanosToMillisRoundedUp(System.nanoTime)
	}
}
