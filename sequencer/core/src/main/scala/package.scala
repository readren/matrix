package readren

package object sequencer {
	/** A time based on the [[System.nanoTime]] method converted to milliseconds. */
	type MilliTime = Long
	/** A time based on the [[System.nanoTime]]. */
	type NanoTime = Long

	/** A duration in milliseconds. */
	type MilliDuration = Long
}
