package readren.matrix
package cluster.service

import cluster.service.ParticipantService.{ContactAddressFilter, DelegateConfig, EventListener, SocketOptionValue, TaskSequencer}
import cluster.service.Protocol.Instant

import readren.common.ToStringWithFields.toStringWithFields
import readren.sequencer.providers.CooperativeWorkersSchedulingDp
import scribe.*

import java.net.{InetSocketAddress, StandardSocketOptions}
import scala.language.implicitConversions

object InteractiveTests {

	ScribeTestConfig.init(true)

	object csAEventListener extends EventListener {
		override def handle(event: ParticipantServiceEvent): Unit = {
			scribe.info(s"Service A event: ${event.toStringWithFields}")
		}
	}
	object csBEventListener extends EventListener {
		override def handle(event: ParticipantServiceEvent): Unit = {
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
		val configA = new ParticipantService.Config(addressA, seeds, participantDelegatesConfig = DelegateConfig(false, receiverTimeout = 5_000), acceptedConnectionsFilter = acceptedConnectionsFilter, socketOptions = socketOptions)
		val configB = new ParticipantService.Config(addressB, seeds, participantDelegatesConfig = DelegateConfig(false, receiverTimeout = 5_000), acceptedConnectionsFilter = acceptedConnectionsFilter, socketOptions = socketOptions)

		val schedulingDap = new CooperativeWorkersSchedulingDp.Impl(failureReporter = (doer, e) => scribe.error(s"Unhandled exception in a task executed by the sequencer of the service at port ${doer.tag}", e))
		val sequencerA: TaskSequencer = schedulingDap.provide(portA.toString)
		val sequencerB: TaskSequencer = schedulingDap.provide(portB.toString)
		
		val clock = new ParticipantService.Clock {
			private val startingInstant = System.currentTimeMillis()
			override def getTime: Instant = System.currentTimeMillis() - startingInstant
		}

		val csA = ParticipantService.start(sequencerA, clock, configA, Some(csAEventListener))
		val csB = ParticipantService.start(sequencerB, clock, configB, Some(csBEventListener))
	}

}
