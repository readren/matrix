package readren.nexus
package cluster.service

import cluster.serialization.ProtocolVersion
import cluster.service.Protocol.ContactAddress

import java.net.InetSocketAddress

/**
 * A cross-version contact information card that a participant use to share and propagate its existence among other participants.
 *
 * A `ContactCard` consist of the contact address of a participant and the set protocol-versions it supports.
 * This enables backward-compatible communication between participants running different versions of the cluster service.
 *
 * Participants share their `ContactCard` with others to allow discovery and propagation of their presence.
 */
type ContactCard = (ContactAddress, Set[ProtocolVersion])

object ContactCard {
	extension (contactCard: ContactCard) {
		inline def address: ContactAddress = contactCard._1
		inline def supportedVersions: Set[ProtocolVersion] = contactCard._2
	}

	/** Defines a ordering where cards that support newer versions come first. If the supported versions are the same then the lower address come first.  */
	val ordering: Ordering[ContactCard] = (cc1: ContactCard, cc2: ContactCard) => {
		val supportedVersionsComparison = compareSetsByNewerValues(cc1.supportedVersions, cc2.supportedVersions)
		if supportedVersionsComparison != 0 then supportedVersionsComparison
		else compareContactAddresses(cc1.address, cc2.address)
	}

	given Ordering[ContactCard] = ordering

	private def compareSetsByNewerValues(set1: Set[ProtocolVersion], set2: Set[ProtocolVersion]): Int = {
		// Convert to arrays and sort in-place in newer to older order
		val arr1 = set1.toArray
		scala.util.Sorting.quickSort(arr1)(using ProtocolVersion.newerFirstOrdering)

		val arr2 = set2.toArray
		scala.util.Sorting.quickSort(arr2)(using ProtocolVersion.newerFirstOrdering)

		val minLength = math.min(arr1.length, arr2.length)
		// Compare elements up to minLength
		var i = 0
		while i < minLength do {
			val comparison = ProtocolVersion.newerFirstOrdering.compare(arr1(i), arr2(i))
			if comparison != 0 then return comparison
			i += 1
		}

		// If all compared elements were equal, the longer set wins (comes first)
		arr1.length - arr2.length
	}

	def compareContactAddresses(ca1: ContactAddress, ca2: ContactAddress): Int = {
		(ca1, ca2) match {
			case (isa1: InetSocketAddress, isa2: InetSocketAddress) => ContactCard.compareInetSocketAddresses(isa1, isa2)
			case (o1, o2) => o1.toString.compareTo(o2.toString)
		}
	}

	private def compareInetSocketAddresses(isa1: InetSocketAddress, isa2: InetSocketAddress): Int = {
		// Get raw byte arrays once
		val bytes1 = isa1.getAddress.getAddress
		val bytes2 = isa2.getAddress.getAddress

		// Compare byte by byte without creating intermediate objects
		var i = 0
		val len = math.min(bytes1.length, bytes2.length)
		while i < len do {
			val cmp = (bytes1(i) & 0xff) - (bytes2(i) & 0xff) // unsigned byte comparison
			if cmp != 0 then return cmp
			i += 1
		}

		// If all bytes equal up to common length, compare array lengths
		val lengthCmp = bytes1.length - bytes2.length
		if lengthCmp != 0 then return lengthCmp

		// Finally compare ports if IPs are equal
		isa1.getPort - isa2.getPort
	}
}