package readren.nexus
package cluster.channel

import cluster.serialization.CommonSerializers.given
import cluster.serialization.Deserializer.DeserializationException
import cluster.serialization.Serializer.SerializationException
import cluster.serialization.{Deserializer, Serializer}

import java.net.{InetAddress, InetSocketAddress, SocketAddress}

object Nio2Serializers {
	//// InetAddress
	given Serializer[InetAddress] = (address: InetAddress, writer: Serializer.Writer) => {
		writer.writeFull(address.getHostName)
		// Write the IP address as a byte array
		val ipAddress = address.getAddress
		writer.putByte(ipAddress.length.toByte) // Write the length of the IP address
		writer.putBytes(ipAddress) // Write the IP address bytes
	}

	given Deserializer[InetAddress] = (reader: Deserializer.Reader) => {
		val hostName = reader.read[String]
		val ipAddress = reader.readBytes(reader.readByte())
		InetAddress.getByAddress(hostName, ipAddress)
	}

	//// InetSocketAddress
	given Serializer[InetSocketAddress] = (message: InetSocketAddress, writer: Serializer.Writer) => {
		writer.writeFull(message.getAddress)
		writer.putInt(message.getPort)
	}

	given Deserializer[InetSocketAddress] = (reader: Deserializer.Reader) => {
		new InetSocketAddress(reader.read[InetAddress], reader.readInt())
	}


	//// SocketAddress
	given Serializer[SocketAddress] = (message: SocketAddress, writer: Serializer.Writer) => {
		message match {
			case inet: InetSocketAddress =>
				writer.putByte(1)
				writer.writeFull(inet)
			case _ =>
				throw new SerializationException(writer.position, s"This serializer only supports instances of `InetSocketAddress`")
		}
	}

	given Deserializer[SocketAddress] = (reader: Deserializer.Reader) => {
		reader.readByte() match {
			case 1 =>
				reader.read[InetSocketAddress]
			case x =>
				throw new DeserializationException(reader.position, s"A SocketAddress subtype discriminator was expected and $x was found.")
		}
	}
}
