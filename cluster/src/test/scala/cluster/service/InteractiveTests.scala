package readren.matrix
package cluster.service

import cluster.misc.TaskSequencer
import cluster.serialization.ProtocolVersion
import cluster.service.ClusterService.{ContactAddressFilter, DelegateConfig, EventListener, SocketOptionValue, given}
import cluster.service.Protocol.Instant
import providers.assistant.SchedulingDap

import scribe.file.*
import scribe.*

import java.net.{InetSocketAddress, StandardSocketOptions}
import scala.language.implicitConversions

object InteractiveTests {

	// Set root logger level and handlers
	Logger.root
		.clearHandlers()
		.withMinimumLevel(Level.Debug)
		.withHandler(
			minimumLevel = Some(Level.Debug), // Set min log level
		)
		.withHandler( // Add new handler
			minimumLevel = Some(Level.Debug), // Set min log level
			writer = FileWriter("logs" / ("app-" % year % "-" % month % "-" % day % ".log")) // Log to file
		)
		.replace()

	object csAEventListener extends EventListener {
		override def handle(event: ClusterServiceEvent): Unit = {
			scribe.info(s"Cluster A event: $event")
		}
	}
	object csBEventListener extends EventListener {
		override def handle(event: ClusterServiceEvent): Unit = {
			scribe.info(s"Cluster B event: $event")
		}
	}

	@main def testClusterFormation(): Unit = {
		scribe.info("Scribe works fine - info")
		scribe.debug("Scribe works fine - debug")
		val addressA = new InetSocketAddress("127.0.0.1", 8080)
		val addressB = new InetSocketAddress("127.0.0.1", 8081)
		val seeds = Set(addressA, addressB)

		val socketOptions: Set[SocketOptionValue[Any]] = Set(StandardSocketOptions.SO_REUSEADDR -> java.lang.Boolean.TRUE)
		val acceptedConnectionsFilter: ContactAddressFilter = {
			case isa: InetSocketAddress => 8080 <= isa.getPort && isa.getPort < 8090
			case _ => false
		}
		val configA = new ClusterService.Config(addressA, seeds, participantDelegatesConfig = DelegateConfig(false), acceptedConnectionsFilter = acceptedConnectionsFilter, socketOptions = socketOptions)
		val configB = new ClusterService.Config(addressB, seeds, participantDelegatesConfig = DelegateConfig(false), acceptedConnectionsFilter = acceptedConnectionsFilter, socketOptions = socketOptions)

		val schedulingDap = new SchedulingDap()
		val sequencer = new TaskSequencer {
			override type Assistant = SchedulingDap.SchedulingAssistant
			override val assistant: Assistant = schedulingDap.provide(0)
		}
		val clock = new ClusterService.Clock {
			override def getTime: Instant = System.currentTimeMillis()
		}

		val csA = ClusterService.start(sequencer, clock, configA, Some(csAEventListener))
		val csB = ClusterService.start(sequencer, clock, configB, Some(csBEventListener))
	}

}
