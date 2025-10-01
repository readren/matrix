package readren.nexus
package cluster.service

import cluster.service.ChannelOrigin

import readren.common.Maybe

import java.net.SocketAddress

class ChannelId(val channelOrigin: ChannelOrigin, oClientAddress: Maybe[SocketAddress]) {
	val clientAddress: String = oClientAddress.fold("not-accessible")(_.toString)

	val clientPort: String = oClientAddress.fold("not-accessible") {
		case inet: java.net.InetSocketAddress => inet.getPort.toString
		case unix: java.net.UnixDomainSocketAddress => unix.getPath.toString
		case other => other.hashCode.toString
	}
	
	override def toString: String = s"$channelOrigin-$clientPort"
} 