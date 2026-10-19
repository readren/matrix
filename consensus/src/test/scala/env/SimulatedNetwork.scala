package readren.consensus.env

import readren.consensus.ConsensusParticipantSdm.*

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/** Point-to-point FIFO channels, packet delivery, and packet drop mechanics for [[ConsensusEnvironment]]. */
class SimulatedNetwork(env: ConsensusEnvironment) {
	var rpcLifecycleListener: Option[RpcLifecycleListener] = None

	private var packetIdSequencer: PacketId = 0
	private val channels: mutable.Map[(NodeId, NodeId), mutable.ArrayDeque[Packet]] = mutable.Map.empty

	private[env] def reset(): Unit = {
		packetIdSequencer = 0
		channels.clear()
	}

	private[env] def nextPacketId(): PacketId = {
		packetIdSequencer += 1
		packetIdSequencer
	}

	def travelingPacketsCount: Int = channels.values.map(_.size).sum

	private def channelQueue(from: NodeId, to: NodeId): mutable.ArrayDeque[Packet] = {
		channels.getOrElseUpdate((from, to), mutable.ArrayDeque.empty)
	}

	private[env] def enqueuePacket(packet: Packet): Unit = {
		val onTheWay = travelingPacketsCount
		channelQueue(packet.source, packet.destination).append(packet)
		packet match {
			case req: RequestPacket =>
				rpcLifecycleListener.foreach(_.onRpcEnqueued(req, onTheWay))
			case _ => ()
		}
	}

	def pendingPackets: Seq[Packet] = channels.values.flatten.toSeq.sortBy(_.id)

	def pendingPacketsBetween(from: NodeRef, to: NodeRef): Seq[Packet] = channelQueue(from.asNodeId, to.asNodeId).toSeq

	def activeChannelKeys: Seq[(NodeId, NodeId)] = channels.filter(_._2.nonEmpty).keys.toSeq

	def channelHeadPacket(from: NodeRef, to: NodeRef): Option[Packet] = channels.get((from.asNodeId, to.asNodeId)).flatMap(_.headOption)

	def allChannels: Seq[((NodeId, NodeId), Seq[Packet])] = channels.map((k, v) => (k, v.toSeq)).toSeq

	def deliverPacket(packetId: PacketId): DeliverOutcome = env.journal.recordOrExecute(EnvOperation.DeliverPacket(packetId)) {
		val targetQueueOpt = channels.find(_._2.exists(_.id == packetId))
		targetQueueOpt match {
			case None => throw new NoSuchElementException(s"Packet $packetId not found in any channel")
			case Some((_, queue)) =>
				val idx = queue.indexWhere(_.id == packetId)
				val packet = queue.remove(idx)
				executeDeliver(packet)
		}
	}

	def deliverNext(from: NodeRef, to: NodeRef): Option[DeliverOutcome] = env.journal.recordOrExecute(EnvOperation.DeliverNext(from.asNodeId, to.asNodeId)) {
		val queue = channelQueue(from.asNodeId, to.asNodeId)
		if queue.isEmpty then None
		else Some(executeDeliver(queue.removeHead()))
	}

	def deliverFirstN(from: NodeRef, to: NodeRef, n: Int): Seq[DeliverOutcome] = env.journal.recordOrExecute(EnvOperation.DeliverFirstN(from.asNodeId, to.asNodeId, n)) {
		val queue = channelQueue(from.asNodeId, to.asNodeId)
		val count = math.min(n, queue.size)
		(0 until count).map(_ => executeDeliver(queue.removeHead()))
	}

	def deliverAllBetween(from: NodeRef, to: NodeRef): Seq[DeliverOutcome] = env.journal.recordOrExecute(EnvOperation.DeliverAllBetween(from.asNodeId, to.asNodeId)) {
		val queue = channelQueue(from.asNodeId, to.asNodeId)
		val res = mutable.ArrayBuffer.empty[DeliverOutcome]
		while queue.nonEmpty do {
			res.append(executeDeliver(queue.removeHead()))
		}
		res.toSeq
	}

	def deliverAllTo(to: NodeRef): Seq[DeliverOutcome] = env.journal.recordOrExecute(EnvOperation.DeliverAllTo(to.asNodeId)) {
		val toId = to.asNodeId
		val res = mutable.ArrayBuffer.empty[DeliverOutcome]
		for ((_, t), q) <- channels if t == toId do {
			while q.nonEmpty do res.append(executeDeliver(q.removeHead()))
		}
		res.toSeq
	}

	def deliverAll(): Seq[DeliverOutcome] = env.journal.recordOrExecute(EnvOperation.DeliverAll) {
		val all = pendingPackets
		all.map(p => deliverPacket(p.id))
	}

	def dropPacket(packetId: PacketId): Boolean = env.journal.recordOrExecute(EnvOperation.DropPacket(packetId)) {
		failPacket(packetId, new java.io.IOException(s"Network packet $packetId dropped in transit"))
	}

	def dropNext(from: NodeRef, to: NodeRef): Boolean = env.journal.recordOrExecute(EnvOperation.DropNext(from.asNodeId, to.asNodeId)) {
		dropFirstN(from, to, 1) > 0
	}

	def dropFirstN(from: NodeRef, to: NodeRef, n: Int): Int = env.journal.recordOrExecute(EnvOperation.DropFirstN(from.asNodeId, to.asNodeId, n)) {
		val q = channelQueue(from.asNodeId, to.asNodeId)
		val count = math.min(n, q.size)
		for _ <- 0 until count do {
			val packet = q.removeHead()
			val error = new java.io.IOException(s"Network packet ${packet.id} dropped in transit")
			packet match {
				case req: RequestPacket =>
					val syntheticResp = ResponsePacket(
						id = packet.id,
						source = req.destination,
						destination = req.source,
						departureTime = env.clock.currentVirtualTime,
						correlationRequestId = req.id,
						rpc = req.rpc,
						response = Failure(error),
						completeCaller = req.completeCaller
					)
					rpcLifecycleListener.foreach(_.onResponseDelivered(syntheticResp, travelingPacketsCount))
					req.completeCaller(Failure(error))
				case resp: ResponsePacket =>
					val failedResp = resp.copy(response = Failure(error))
					rpcLifecycleListener.foreach(_.onResponseDelivered(failedResp, travelingPacketsCount))
					resp.completeCaller(Failure(error))
			}
		}
		count
	}

	def dropAllBetween(from: NodeRef, to: NodeRef): Int = env.journal.recordOrExecute(EnvOperation.DropAllBetween(from.asNodeId, to.asNodeId)) {
		val q = channelQueue(from.asNodeId, to.asNodeId)
		dropFirstN(from, to, q.size)
	}

	def failPacket(packetId: PacketId, error: Throwable): Boolean = env.journal.recordOrExecute(EnvOperation.FailPacket(packetId, error.getMessage)) {
		channels.values.find(_.exists(_.id == packetId)).fold(false) { q =>
			val idx = q.indexWhere(_.id == packetId)
			val packet = q.remove(idx)
			packet match {
				case req: RequestPacket =>
					val syntheticResp = ResponsePacket(
						id = packet.id,
						source = req.destination,
						destination = req.source,
						departureTime = env.clock.currentVirtualTime,
						correlationRequestId = req.id,
						rpc = req.rpc,
						response = Failure(error),
						completeCaller = req.completeCaller
					)
					rpcLifecycleListener.foreach(_.onResponseDelivered(syntheticResp, travelingPacketsCount))
					req.completeCaller(Failure(error))
				case resp: ResponsePacket =>
					val failedResp = resp.copy(response = Failure(error))
					rpcLifecycleListener.foreach(_.onResponseDelivered(failedResp, travelingPacketsCount))
					resp.completeCaller(Failure(error))
			}
			true
		}
	}

	private def executeDeliver(packet: Packet): DeliverOutcome = {
		val destNode = env.nodeOpt(packet.destination).getOrElse(throw new IllegalStateException(s"Destination node ${packet.destination} does not exist"))
		val hadPending = destNode.stepDoer.hasPendingTasks
		val warning = if hadPending then Some(s"Destination node ${packet.destination} had ${destNode.stepDoer.pendingTasksCount} pending task(s) when packet ${packet.id} was delivered.") else None

		packet match {
			case req: RequestPacket =>
				destNode.stepDoer.executeSequentially(() => {
					if destNode.isDown || (destNode.participant eq null) then {
						val failure = Failure(new RuntimeException(s"Node ${destNode.myId} is down"))
						rpcLifecycleListener.foreach(_.onRpcCompleted(req, failure, travelingPacketsCount))
						val resp = ResponsePacket(
							id = nextPacketId(),
							source = req.destination,
							destination = req.source,
							departureTime = env.clock.currentVirtualTime,
							correlationRequestId = req.id,
							rpc = req.rpc,
							response = failure,
							completeCaller = req.completeCaller
						)
						enqueuePacket(resp)
					} else {
						val onSuccess: Any => Unit = result => {
							val outcome = Success(result)
							rpcLifecycleListener.foreach(_.onRpcCompleted(req, outcome, travelingPacketsCount))
							val resp = ResponsePacket(
								id = nextPacketId(),
								source = req.destination,
								destination = req.source,
								departureTime = env.clock.currentVirtualTime,
								correlationRequestId = req.id,
								rpc = req.rpc,
								response = outcome,
								completeCaller = req.completeCaller
							)
							enqueuePacket(resp)
						}
						val onError: Throwable => Unit = ex => {
							val outcome = Failure(ex)
							rpcLifecycleListener.foreach(_.onRpcCompleted(req, outcome, travelingPacketsCount))
							val resp = ResponsePacket(
								id = nextPacketId(),
								source = req.destination,
								destination = req.source,
								departureTime = env.clock.currentVirtualTime,
								correlationRequestId = req.id,
								rpc = req.rpc,
								response = outcome,
								completeCaller = req.completeCaller
							)
							enqueuePacket(resp)
						}

						rpcLifecycleListener.foreach(_.onRpcDelivering(req, travelingPacketsCount))

						req.rpc match {
							case ConsensusRpc.HowAreYou(inquirerInfo) =>
								destNode.clusterParticipant.delegate.onHowAreYou(req.source, inquirerInfo).triggerSyncCallbacks(onSuccess, onError)
							case ConsensusRpc.RequestBallot(inquirerId, inquirerInfo) =>
								destNode.clusterParticipant.delegate.onRequestBallot(inquirerId, inquirerInfo).triggerSyncCallbacks(onSuccess, onError)
							case ConsensusRpc.AppendRecords(inquirerTerm, prevLogIndex, prevLogTerm, batch, leaderCommit, termAtLeaderCommit) =>
								destNode.clusterParticipant.delegate.onAppendRecords(req.source, inquirerTerm, prevLogIndex, prevLogTerm, batch, leaderCommit, termAtLeaderCommit).triggerSyncCallbacks(onSuccess, onError)
							case ConsensusRpc.InstallSnapshot(inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit) =>
								destNode.clusterParticipant.delegate.onInstallSnapshot(req.source, inquirerTerm, snapshot, batch, leaderCommit, termAtLeaderCommit).triggerSyncCallbacks(onSuccess, onError)
							case ConsensusRpc.PermitQuiescence(indexOfGrantedSec) =>
								destNode.clusterParticipant.delegate.onQuiescencePermitted(req.source, indexOfGrantedSec)
								onSuccess(())
						}
					}
				})

			case resp: ResponsePacket =>
				rpcLifecycleListener.foreach(_.onResponseDelivered(resp, travelingPacketsCount))
				resp.completeCaller(resp.response)
		}

		DeliverOutcome(packet.id, packet.source, packet.destination, hadPending, warning)
	}
}
