package readren.matrix.cluster.service


import readren.matrix.cluster.misc.TaskSequencer
import readren.matrix.cluster.service.ClusterService.{ContactAddressFilter, DelegateConfig, EventListener, SocketOptionValue}
import readren.matrix.cluster.service.Protocol.Instant
import common.ToStringWithFields.toStringWithFields // Ignore this warning
import readren.matrix.providers.assistant.{CooperativeWorkersDap, SchedulingDap}
import scribe.*
import java.net.{InetSocketAddress, StandardSocketOptions}
import scala.language.implicitConversions

object InteractiveTests {

	ScribeTestConfig.init(true)

	object csAEventListener extends EventListener {
		override def handle(event: ClusterServiceEvent): Unit = {
			scribe.info(s"Service A event: ${event.toStringWithFields}")
		}
	}
	object csBEventListener extends EventListener {
		override def handle(event: ClusterServiceEvent): Unit = {
			scribe.info(s"Service B event: ${event.toStringWithFields}")
		}
	}

	@main def testClusterFormation(): Unit = {
		val portA = 8080
		val portB = 8081
		val addressA = new InetSocketAddress("localhost", portA)
		val addressB = new InetSocketAddress("localhost", portB)
		val seeds = Set(addressA, addressB)

		val socketOptions: Set[SocketOptionValue[Any]] = Set(StandardSocketOptions.SO_REUSEADDR -> java.lang.Boolean.TRUE)
		val acceptedConnectionsFilter: ContactAddressFilter = _ => true
		val configA = new ClusterService.Config(addressA, seeds, participantDelegatesConfig = DelegateConfig(false), acceptedConnectionsFilter = acceptedConnectionsFilter, socketOptions = socketOptions)
		val configB = new ClusterService.Config(addressB, seeds, participantDelegatesConfig = DelegateConfig(false), acceptedConnectionsFilter = acceptedConnectionsFilter, socketOptions = socketOptions)

		val schedulingDap = new SchedulingDap(failureReporter = scribe.error(s"Unhandled exception in a task executed by the sequencer of the service at port ${CooperativeWorkersDap.currentAssistant.id}", _))
		val sequencerA = new TaskSequencer {
			override type Assistant = SchedulingDap.SchedulingAssistant
			override val assistant: Assistant = schedulingDap.provide(portA)
		}
		val sequencerB = new TaskSequencer {
			override type Assistant = SchedulingDap.SchedulingAssistant
			override val assistant: Assistant = schedulingDap.provide(portB)
		}
		val clock = new ClusterService.Clock {
			override def getTime: Instant = System.currentTimeMillis()
		}

		val csA = ClusterService.start(sequencerA, clock, configA, Some(csAEventListener))
		val csB = ClusterService.start(sequencerB, clock, configB, Some(csBEventListener))
	}

}
