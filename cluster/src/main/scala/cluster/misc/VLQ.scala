package readren.matrix
package cluster.misc

object VLQ {

	/** Applies for both, signed and unsigned int */
	inline val LONG_MAX_LENGTH = 10
	/** Applies for both, signed and unsigned long */
	inline val INT_MAX_LENGTH = 5

	/** Specifies the reader that the decode methods require to read the bytes consumed by them. */
	trait ByteReader {
		def readByte(): Byte
	}
	/** Specifies the writer that the encode methods require to write the bytes produced by them. */
	trait ByteWriter {
		def putByte(byte: Byte): Unit
	}

	/**
	 * Encodes a unsigned integer into a Variable Length Quantity (VLQ) format and writes the result to a `ByteBuffer`.
	 *
	 * The VLQ format uses a variable number of bytes to represent an integer. Each byte contains 7 bits of data,
	 * and the most significant bit (MSB) indicates whether the next byte is part of the same integer.
	 *
	 * @param value The unsigned integer to encode.
	 * @param writer The [[ByteWriter]] where the encoded bytes will be written.
	 */
	def encodeUnsignedInt(value: Int, writer: ByteWriter): Unit = {
		var m = value
		while true do {
			val sevenLSBits = m & 0x7f
			m >>>= 7
			if m > 0 then writer.putByte((sevenLSBits | 0x80).toByte)
			else {
				writer.putByte(sevenLSBits.toByte)
				return
			}
		}
	}

	/**
	 * Decodes a Variable-Length-Quantity (VLQ) from the provided [[ByteReader]] and returns the corresponding unsigned integer.
	 *
	 * The VLQ format uses a variable number of bytes to represent an integer. Each byte contains 7 bits of data,
	 * and the most significant bit (MSB) indicates whether the next byte is part of the same integer.
	 *
	 * @param reader The [[ByteReader]] from which the encoded bytes will be read.
	 * @return The decoded unsigned integer.
	 */
	def decodeUnsignedInt(reader: ByteReader): Int = {
		var value: Int = 0
		var m = reader.readByte()
		var displacement = 0
		while m < 0 do {
			value |= (m & 0x7f) << displacement
			m = reader.readByte()
			displacement += 7
		}
		value | (m << displacement)
	}

	/**
	 * Encodes a unsigned integer into a Variable Length Quantity (VLQ) format and writes the result to a `ByteBuffer`.
	 *
	 * The VLQ format uses a variable number of bytes to represent an integer. Each byte contains 7 bits of data,
	 * and the most significant bit (MSB) indicates whether the next byte is part of the same integer.
	 *
	 * @param value The unsigned long integer to encode.
	 * @param writer The [[ByteWriter]] where the encoded bytes will be written.
	 */
	def encodeUnsignedLong(value: Long, writer: ByteWriter): Unit = {
		var m = value
		while true do {
			val sevenLSBits = m & 0x7f
			m >>>= 7
			if m > 0 then writer.putByte((sevenLSBits | 0x80).toByte)
			else {
				writer.putByte(sevenLSBits.toByte)
				return
			}
		}
	}


	/**
	 * Decodes a Variable Length Quantity (VLQ) from a `ByteBuffer` and returns the corresponding unsigned long integer.
	 *
	 * The VLQ format uses a variable number of bytes to represent an integer. Each byte contains 7 bits of data,
	 * and the most significant bit (MSB) indicates whether the next byte is part of the same integer.
	 *
	 * @param reader The [[ByteReader]] from which the encoded bytes will be read.
	 * @return The decoded unsigned long integer.
	 */
	def decodeUnsignedLong(reader: ByteReader): Long = {
		var value: Long = 0L
		var m = reader.readByte()
		var displacement = 0
		while m < 0 do {
			value |= (m & 0x7f).toLong << displacement
			m = reader.readByte()
			displacement += 7
		}
		value | (m.toLong << displacement)
	}

	/**
	 * Encodes an [[Int]] into a Variable Length Quantity (VLQ) format and writes the result to a `ByteBuffer`.
	 *
	 * The VLQ format uses a variable number of bytes to represent an integer. Each byte contains 7 bits of data,
	 * and the most significant bit (MSB) indicates whether the next byte is part of the same integer. Negative values
	 * are encoded by converting them to their positive counterparts and setting a sign mark (second most significant bit of the final byte).
	 *
	 * @param value The signed integer to encode. Can be positive or negative.
	 * @param writer The [[ByteWriter]] where the encoded bytes will be written.
	 */
	def encodeInt(value: Int, writer: ByteWriter): Unit = {
		var m: Int = value
		var signMark = 0
		// if the value is negative treat it as positive. The sign is stored in the seconds most significant bit of the last byte (not in two-complement format).
		if value < 0 then {
			m = -value
			signMark = 0x40
		}

		var continue = true
		while continue do {
			val sevenLSBits = m & 0x7f
			m >>>= 7
			if m > 0 then writer.putByte((sevenLSBits | 0x80).toByte)
			else {
				// if the last fragment (the most significant) uses
				if sevenLSBits < 0x40 then writer.putByte((sevenLSBits | signMark).toByte)
				else {
					writer.putByte((sevenLSBits | 0x80).toByte)
					writer.putByte(signMark.toByte)
				}
				continue = false
			}
		}
	}


	/**
	 * Decodes a Variable Length Quantity (VLQ) from a `ByteBuffer` and returns the corresponding integer.
	 *
	 * The VLQ format uses a variable number of bytes to represent an integer. Each byte contains 7 bits of data,
	 * and the most significant bit (MSB) indicates whether the next byte is part of the same integer. Negative values
	 * are indicated by a sign mark (second most significant bit of the final byte).
	 *
	 * @param reader The [[ByteReader]] from which the encoded bytes will be read.
	 * @return The decoded signed integer.
	 */
	def decodeInt(reader: ByteReader): Int = {
		var value: Int = 0
		var m = reader.readByte()
		var displacement = 0
		while m < 0 do {
			value |= (m & 0x7f) << displacement
			m = reader.readByte()
			displacement += 7
		}
		if m < 0x40 then value | (m << displacement)
		else -(value | ((m & 0x3f) << displacement))
	}

	/**
	 * Encodes an integer into a Variable Length Quantity (VLQ) format and writes the result to a `ByteBuffer`.
	 *
	 * The VLQ format uses a variable number of bytes to represent an integer. Each byte contains 7 bits of data,
	 * and the most significant bit (MSB) indicates whether the next byte is part of the same integer. Negative values
	 * are encoded by converting them to their positive counterparts and setting a sign mark (second most significant bit of the final byte).
	 *
	 * @param value The signed long integer to encode.
	 * @param writer The [[ByteWriter]] where the encoded bytes will be written.
	 */
	def encodeLong(value: Long, writer: ByteWriter): Unit = {
		var m: Long = value
		var signMark = 0
		// if the value is negative, remove the sign. The sign is stored in the seconds most significant bit of the last byte.
		if value < 0 then {
			m = -value
			signMark = 0x40
		}

		var continue = true
		while continue do {
			val sevenLSBits = m & 0x7f
			m >>>= 7
			if m > 0 then writer.putByte((sevenLSBits | 0x80).toByte)
			else {
				// if the last fragment (the most significant) uses   
				if sevenLSBits < 0x40 then writer.putByte((sevenLSBits | signMark).toByte)
				else {
					writer.putByte((sevenLSBits | 0x80).toByte)
					writer.putByte(signMark.toByte)
				}
				continue = false
			}
		}
	}	

	/**
	 * Decodes a Variable Length Quantity (VLQ) from a `ByteBuffer` and returns the corresponding integer.
	 *
	 * The VLQ format uses a variable number of bytes to represent an integer. Each byte contains 7 bits of data,
	 * and the most significant bit (MSB) indicates whether the next byte is part of the same integer. Negative values
	 * are indicated by a sign mark (second most significant bit of the final byte).
	 *
	 * @param reader The [[ByteReader]] from which the encoded bytes will be read.
	 * @return The decoded signed long integer.
	 */
	def decodeLong(reader: ByteReader): Long = {
		var value: Long = 0L
		var m = reader.readByte()
		var displacement = 0
		while m < 0 do {
			value |= (m & 0x7f).toLong << displacement
			m = reader.readByte()
			displacement += 7
		}
		if m < 0x40 then value | (m.toLong << displacement)
		else -(value | ((m & 0x3f).toLong << displacement))
	}
}

