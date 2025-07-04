package readren.matrix
package cluster.misc

import java.nio.ByteBuffer

object LoggingTools {
	
	def show: StringBuilder = new StringBuilder

	extension (sb: StringBuilder) {
		/**
		 * Appends the contents of a [[DualEndedCircularStorage]] of [[ByteBuffer]]s to this [[StringBuilder]].
		 * 
		 * @param dualEndedCircularStorage The `DualEndedCircularStorage` to append.
		 * @return The `StringBuilder` with the appended contents.
		 */
		def circularStorage(dualEndedCircularStorage: DualEndedCircularStorage[ByteBuffer], isBeingFilledOrDrained: Boolean): StringBuilder = {
			sb.append("readEnd->[")
			for bb <- dualEndedCircularStorage.iterator do {
				if isBeingFilledOrDrained then sb.fillingByteBuffer(bb)
				else sb.drainingByteBuffer(bb)
			}
			sb.append("]<-writeEnd")
		}

		/**
		 * Appends the bytes contained in a draining [[ByteBuffer]] to this [[StringBuilder]] in hexadecimal format, prefixed with the position of the buffer.
		 * 
		 * @param byteBuffer The `ByteBuffer` to append.
		 * @return The `StringBuilder` with the appended contents.
		 */
		def drainingByteBuffer(byteBuffer: ByteBuffer): StringBuilder = {
			sb.append(s"/${byteBuffer.position()}\\ ")
			val pos = byteBuffer.position()
			val lim = byteBuffer.limit()
			val arr = new Array[Byte](lim - pos)
			byteBuffer.get(pos, arr)
			sb.append(arr.map(b => f"$b%02X").mkString(" "))
		}

		/**
		 * Appends the bytes contained in a filling [[ByteBuffer]] to this [[StringBuilder]] in hexadecimal format, prefixed with the position of the buffer.
		 * 
		 * @param byteBuffer The `ByteBuffer` to append.
		 * @return The `StringBuilder` with the appended contents.
		 */
		def fillingByteBuffer(byteBuffer: ByteBuffer): StringBuilder = {
			val pos = byteBuffer.position()
			val arr = new Array[Byte](pos)
			byteBuffer.get(0, arr)
			sb.append(arr.map(b => f"$b%02X").mkString(" "))
			sb.append(s"\\${byteBuffer.remaining()}/ ")
		}

		def end: String = sb.toString
	}
}
