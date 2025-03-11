package readren.matrix
package cluster.channel

import cluster.channel.CommonSerializers.given
import cluster.channel.{Deserializer, Serializer}

import java.net.{InetAddress, InetSocketAddress, SocketAddress}

object Nio2Serializers {
	//// InetAddress
	given Serializer[InetAddress] = new Serializer[InetAddress] {
		override def serialize(address: InetAddress, writer: Serializer.Writer): Serializer.Outcome = {
			writer.writeFull(address.getHostName)
			// Write the IP address as a byte array
			val ipAddress = address.getAddress
			writer.putByte(ipAddress.length.toByte) // Write the length of the IP address
			writer.putBytes(ipAddress) // Write the IP address bytes
			Serializer.Success

		}
	}

	given Deserializer[InetAddress] = new Deserializer[InetAddress] {
		override def deserialize(reader: Deserializer.Reader): Deserializer.Problem | InetAddress = {
			reader.read[String] match {
				case problem: Deserializer.Problem => problem
				case hostName: String =>
					val ipAddress = reader.readBytes(reader.readByte())
					InetAddress.getByAddress(hostName, ipAddress)
			}
		}
	}

	//// InetSocketAddress
	given Serializer[InetSocketAddress] = new Serializer[InetSocketAddress] {
		override def serialize(message: InetSocketAddress, writer: Serializer.Writer): Serializer.Outcome = {
			writer.putInt(message.getPort)
			writer.writeFull(message.getAddress)
		}
	}

	given Deserializer[InetSocketAddress] = new Deserializer[InetSocketAddress] {
		override def deserialize(reader: Deserializer.Reader): Deserializer.Problem | InetSocketAddress = {
			val port = reader.readInt()
			reader.read[InetAddress].map(address => new InetSocketAddress(address, port))
		}
	}


	//// SocketAddress
	given Serializer[SocketAddress] = new Serializer[SocketAddress] {
		override def serialize(message: SocketAddress, writer: Serializer.Writer): Serializer.Outcome = {
			message match {
				case inet: InetSocketAddress =>
					writer.putByte(1)
					writer.writeFull(inet)
				case _ =>
					Serializer.Unsupported(writer.position, s"This serializer only supports instances of `InetSocketAddress`")
			}
		}
	}

	given Deserializer[SocketAddress] = new Deserializer[SocketAddress] {
		override def deserialize(reader: Deserializer.Reader): Deserializer.Problem | SocketAddress = {
			reader.readByte() match {
				case 1 =>
					reader.read[InetSocketAddress]
				case x =>
					Deserializer.Mismatch(reader.position, "A SocketAddress subtype discriminator was expected and $x was found.")
			}
		}
	}
}
