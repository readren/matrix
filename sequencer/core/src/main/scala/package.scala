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


	trait MilliClock {
		def milliTimeRoundedDown: MilliTime

		def milliTimeRoundedUp: MilliTime
	}

	class NanoTimeBasedMilliClock extends MilliClock {
		override def milliTimeRoundedDown: MilliTime = nanosToMillisRoundedDown(System.nanoTime)

		override def milliTimeRoundedUp: MilliTime = nanosToMillisRoundedUp(System.nanoTime)
	}
}
