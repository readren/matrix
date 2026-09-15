package readren.consensus

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import readren.common.Maybe
import readren.consensus.ConsensusParticipantSdm.*

import java.io.{ByteArrayOutputStream, IOException, InputStream, ObjectOutputStream, OutputStream}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CopyOnWriteArrayList, CountDownLatch, ExecutorService, Executors, TimeUnit}
import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

/** Embedded HTTP server providing a REST API and serving the single-page web UI
 * for interactive discrete-event testing with [[ConsensusEnvironment]].
 *
 * Uses only standard JDK networking (`com.sun.net.httpserver.HttpServer`)
 * to avoid adding external HTTP or JSON dependencies.
 */
class ConsensusPlaygroundServer(val port: Int = 8080) {

	private var server: HttpServer = null
	private var executor: ExecutorService = null
	private val lock = new Object()
	private val shutdownLatch = new CountDownLatch(1)
	private val threadCounter = new AtomicInteger(0)
	private val sseClients = new CopyOnWriteArrayList[SseClient]()
	private var env: ConsensusEnvironment = {
		val e = new ConsensusEnvironment(clusterSize = 3, initializer = _.startAllNodes())
		e.reset()
		e
	}

	def start(): Unit = lock.synchronized {
		if server != null then return
		val pool = Executors.newCachedThreadPool(r => {
			val thread = new Thread(r, s"playground-http-${threadCounter.incrementAndGet()}")
			thread.setDaemon(true)
			thread
		})
		executor = pool
		server = HttpServer.create(new InetSocketAddress(port), 0)
		server.setExecutor(pool)

		server.createContext("/", new StaticFileHandler())
		server.createContext("/api/state", new StateHandler())
		server.createContext("/api/events", new EventsHandler())
		server.createContext("/api/action", new ActionHandler())

		server.start()
	}

	def stop(): Unit = lock.synchronized {
		val it = sseClients.iterator()
		while it.hasNext do {
			it.next().close()
		}
		sseClients.clear()
		if server != null then {
			server.stop(0)
			server = null
		}
		if executor != null then {
			executor.shutdownNow()
			executor = null
		}
		shutdownLatch.countDown()
	}

	def awaitShutdown(): Unit = {
		shutdownLatch.await()
	}

	// =========================================================================
	// PRESETS
	// =========================================================================

	private def setupPresetFigure7(e: ConsensusEnvironment): Unit = {
		val initConfig = TransitionalConfigChange[String](1.asInstanceOf[Term], "cfg-0", Set.empty, e.defaultInitialParticipants)
		def cmd(t: Int, s: Int) = CommandRecord(t.asInstanceOf[Term], TestClientCommand(s, "c-1"))

		// Leader p-0
		val m0 = e.node(0).storage.savedMemory
		m0.currentTerm = 8.asInstanceOf[Term]
		m0.logBuffer.clear()
		m0.appendRecord(initConfig)
		m0.appendRecord(cmd(1, 2))
		m0.appendRecord(cmd(1, 3))
		m0.appendRecord(cmd(4, 4))
		m0.appendRecord(cmd(4, 5))
		m0.appendRecord(cmd(5, 6))
		m0.appendRecord(cmd(5, 7))
		m0.appendRecord(cmd(6, 8))
		m0.appendRecord(cmd(6, 9))
		m0.appendRecord(cmd(6, 10))

		// Followers a-f
		val mA = e.node(1).storage.savedMemory
		mA.currentTerm = 6.asInstanceOf[Term]
		mA.logBuffer.clear()
		for i <- 1 to 9 do mA.appendRecord(m0.getRecordAt(i))

		val mB = e.node(2).storage.savedMemory
		mB.currentTerm = 4.asInstanceOf[Term]
		mB.logBuffer.clear()
		for i <- 1 to 4 do mB.appendRecord(m0.getRecordAt(i))

		val mC = e.node(3).storage.savedMemory
		mC.currentTerm = 6.asInstanceOf[Term]
		mC.logBuffer.clear()
		for i <- 1 to 10 do mC.appendRecord(m0.getRecordAt(i))
		mC.appendRecord(cmd(6, 11))

		val mD = e.node(4).storage.savedMemory
		mD.currentTerm = 7.asInstanceOf[Term]
		mD.logBuffer.clear()
		for i <- 1 to 10 do mD.appendRecord(m0.getRecordAt(i))
		mD.appendRecord(cmd(7, 11))
		mD.appendRecord(cmd(7, 12))

		val mE = e.node(5).storage.savedMemory
		mE.currentTerm = 4.asInstanceOf[Term]
		mE.logBuffer.clear()
		for i <- 1 to 3 do mE.appendRecord(m0.getRecordAt(i))
		mE.appendRecord(cmd(4, 4))
		mE.appendRecord(cmd(4, 5))
		mE.appendRecord(cmd(4, 6))
		mE.appendRecord(cmd(4, 7))

		val mF = e.node(6).storage.savedMemory
		mF.currentTerm = 3.asInstanceOf[Term]
		mF.logBuffer.clear()
		for i <- 1 to 3 do mF.appendRecord(m0.getRecordAt(i))
		mF.appendRecord(cmd(2, 4))
		mF.appendRecord(cmd(2, 5))
		mF.appendRecord(cmd(2, 6))
		mF.appendRecord(cmd(3, 7))
		mF.appendRecord(cmd(3, 8))

		e.startNode(0)
		e.startNode(1)
		e.startNode(2)
		e.startNode(5)
		e.startNode(6)
	}

	private def setupPresetFigure8(e: ConsensusEnvironment): Unit = {
		val initConfig = TransitionalConfigChange[String](1.asInstanceOf[Term], "cfg-0", Set.empty, e.defaultInitialParticipants)
		def cmd(t: Int, s: Int) = CommandRecord(t.asInstanceOf[Term], TestClientCommand(s, "c-1"))

		val m0 = e.node(0).storage.savedMemory
		m0.currentTerm = 3.asInstanceOf[Term]
		m0.logBuffer.clear()
		m0.appendRecord(initConfig)
		m0.appendRecord(cmd(2, 2))

		val m1 = e.node(1).storage.savedMemory
		m1.currentTerm = 2.asInstanceOf[Term]
		m1.logBuffer.clear()
		m1.appendRecord(initConfig)
		m1.appendRecord(cmd(2, 2))

		val m2 = e.node(2).storage.savedMemory
		m2.currentTerm = 1.asInstanceOf[Term]
		m2.logBuffer.clear()
		m2.appendRecord(initConfig)

		val m3 = e.node(3).storage.savedMemory
		m3.currentTerm = 1.asInstanceOf[Term]
		m3.logBuffer.clear()
		m3.appendRecord(initConfig)

		val m4 = e.node(4).storage.savedMemory
		m4.currentTerm = 3.asInstanceOf[Term]
		m4.logBuffer.clear()
		m4.appendRecord(initConfig)
		m4.appendRecord(cmd(3, 2))

		e.startNode(0)
		e.startNode(1)
		e.startNode(2)
	}

	private def setupPresetFigure10(e: ConsensusEnvironment): Unit = {
		e.startNode(0)
		e.startNode(1)
		e.startNode(2)
	}

	private def setupPresetFigure13(e: ConsensusEnvironment): Unit = {
		val initConfig = TransitionalConfigChange[String](1.asInstanceOf[Term], "cfg-0", Set.empty, e.defaultInitialParticipants)
		def cmd(t: Int, s: Int) = CommandRecord(t.asInstanceOf[Term], TestClientCommand(s, "c-1"))

		val smBytes = new ByteArrayOutputStream()
		val smOut = new ObjectOutputStream(smBytes)
		smOut.writeInt(0)
		smOut.writeLong(5L)
		smOut.flush()
		val snapshotData = IArray.unsafeFromArray(smBytes.toByteArray)

		val snap = new SnapshotData[String](5, 1.asInstanceOf[Term], initConfig, 1, snapshotData)

		val m0 = e.node(0).storage.savedMemory
		m0.currentTerm = 1.asInstanceOf[Term]
		m0.maybeLatestSnapshot = Maybe(snap)
		m0._logBufferOffset = 6
		m0.logBuffer.clear()
		m0.appendRecord(cmd(1, 6))

		e.node(0).machine.highestAppliedCommandIndex = 5
		e.node(2).machine.highestAppliedCommandIndex = 5

		val m2 = e.node(2).storage.savedMemory
		m2.currentTerm = 1.asInstanceOf[Term]
		m2.maybeLatestSnapshot = Maybe(snap)
		m2._logBufferOffset = 6
		m2.logBuffer.clear()
		m2.appendRecord(cmd(1, 6))

		val m1 = e.node(1).storage.savedMemory
		m1.currentTerm = 1.asInstanceOf[Term]
		m1.logBuffer.clear()

		e.startAllNodes()
	}

	def resetCluster(
		size: Int = 3,
		seedNodes: Option[Set[String]] = None,
		maxInFlightAppendsPerPeer: Int = 2,
		logCompactionThreshold: Int = 5
	): Unit = lock.synchronized {
		env = new ConsensusEnvironment(
			clusterSize = size,
			initialSeedParticipants = seedNodes,
			maxInFlightAppendsPerPeer = maxInFlightAppendsPerPeer,
			logCompactionThreshold = logCompactionThreshold,
			initializer = _.startAllNodes()
		)
		env.reset()
	}

	def loadPreset(
		name: String,
		seedNodes: Option[Set[String]] = None,
		maxInFlightAppendsPerPeer: Int = 2,
		logCompactionThreshold: Int = 5
	): Unit = lock.synchronized {
		name.toLowerCase match {
			case "figure7" =>
				env = new ConsensusEnvironment(
					clusterSize = 7,
					initialSeedParticipants = seedNodes,
					maxInFlightAppendsPerPeer = maxInFlightAppendsPerPeer,
					logCompactionThreshold = logCompactionThreshold,
					initializer = setupPresetFigure7
				)
				env.reset()

			case "figure8" =>
				env = new ConsensusEnvironment(
					clusterSize = 5,
					initialSeedParticipants = seedNodes,
					maxInFlightAppendsPerPeer = maxInFlightAppendsPerPeer,
					logCompactionThreshold = logCompactionThreshold,
					initializer = setupPresetFigure8
				)
				env.reset()

			case "figure10" =>
				val seeds = seedNodes.orElse(Some(Set("p-0", "p-1", "p-2")))
				env = new ConsensusEnvironment(
					clusterSize = 4,
					initialSeedParticipants = seeds,
					maxInFlightAppendsPerPeer = maxInFlightAppendsPerPeer,
					logCompactionThreshold = logCompactionThreshold,
					initializer = setupPresetFigure10
				)
				env.reset()

			case "figure13" =>
				env = new ConsensusEnvironment(
					clusterSize = 3,
					initialSeedParticipants = seedNodes,
					maxInFlightAppendsPerPeer = maxInFlightAppendsPerPeer,
					logCompactionThreshold = logCompactionThreshold,
					initializer = setupPresetFigure13
				)
				env.reset()

			case _ =>
				resetCluster(3, seedNodes, maxInFlightAppendsPerPeer, logCompactionThreshold)
		}
	}

	// =========================================================================
	// JSON SERIALIZATION
	// =========================================================================

	private def escapeJson(str: String): String = {
		if str == null then ""
		else str.replace("\\", "\\\\")
			.replace("\"", "\\\"")
			.replace("\n", "\\n")
			.replace("\r", "\\r")
			.replace("\t", "\\t")
	}

	private def serializeRecord(r: Record, index: RecordIndex, leaderDiagOpt: Option[LeaderRoleDiagnostic[String]], nodeId: String): String = {
		val kind = r match {
			case _: CommandRecord[?] => "Cmd"
			case _: TransitionalConfigChange[?] => "TCC"
			case _: StableConfigChange[?] => "SCC"
			case _: LeaderTransition => "LT"
		}
		val summary = r match {
			case cmd: CommandRecord[?] => cmd.command match {
				case tc: TestClientCommand => s"#${tc.serial} from ${tc.clientId}"
				case other => s"$other"
			}
			case tcc: TransitionalConfigChange[?] =>
				val oldStr = tcc.oldParticipants.toSeq.map(_.toString).sorted.mkString(", ")
				val newStr = tcc.newParticipants.toSeq.map(_.toString).sorted.mkString(", ")
				s"{$oldStr} -> {$newStr}"
			case scc: StableConfigChange[?] =>
				val newStr = scc.newParticipants.toSeq.map(_.toString).sorted.mkString(", ")
				s"{$newStr}"
			case lt: LeaderTransition => s"term=${lt.term}"
		}
		val sb = new java.lang.StringBuilder()
		sb.append(s"""{"index":$index,"term":${r.term},"kind":"$kind","summary":"${escapeJson(summary)}"""")
		leaderDiagOpt match {
			case Some(ld) =>
				ld.activeConfigChange match {
					case tcc: TransitionalConfigChange[?] =>
						val oldPeers = tcc.oldParticipants.asInstanceOf[Set[String]] - nodeId
						val newPeers = tcc.newParticipants.asInstanceOf[Set[String]] - nodeId
						val appendedOld = ld.peerProgress.count(p => oldPeers.contains(p.peerId) && p.highestRecordIndexKnownToBeAppended >= index)
						val appendedNew = ld.peerProgress.count(p => newPeers.contains(p.peerId) && p.highestRecordIndexKnownToBeAppended >= index)
						val committedOld = ld.peerProgress.count(p => oldPeers.contains(p.peerId) && p.highestRecordIndexKnowToBeCommitted >= index)
						val committedNew = ld.peerProgress.count(p => newPeers.contains(p.peerId) && p.highestRecordIndexKnowToBeCommitted >= index)
						sb.append(s""","peersAppended":"(${appendedOld}/${oldPeers.size}, ${appendedNew}/${newPeers.size})"""")
						sb.append(s""","peersCommitted":"(${committedOld}/${oldPeers.size}, ${committedNew}/${newPeers.size})"""")
					case scc: StableConfigChange[?] =>
						val remotePeers = scc.newParticipants.asInstanceOf[Set[String]] - nodeId
						val appended = ld.peerProgress.count(p => remotePeers.contains(p.peerId) && p.highestRecordIndexKnownToBeAppended >= index)
						val committed = ld.peerProgress.count(p => remotePeers.contains(p.peerId) && p.highestRecordIndexKnowToBeCommitted >= index)
						sb.append(s""","peersAppended":"${appended}/${remotePeers.size}"""")
						sb.append(s""","peersCommitted":"${committed}/${remotePeers.size}"""")
				}
			case None =>
				sb.append(""","peersAppended":null,"peersCommitted":null""")
		}
		sb.append("}")
		sb.toString
	}

	def serializeState(): String = lock.synchronized {
		val sb = new java.lang.StringBuilder(16384)
		sb.append("{")
		sb.append("\"virtualTime\":").append(env.currentVirtualTime).append(",")
		sb.append("\"clusterSize\":").append(env.clusterSize).append(",")

		// Nodes
		sb.append("\"nodes\":[")
		val nodes = env.allNodes
		for i <- nodes.indices do {
			val n = nodes(i)
			if i > 0 then sb.append(",")
			val mem = n.storage.savedMemory
			val roleStr = env.nodeRole(n.myId)
			val term = mem.getCurrentTerm
			val offset = mem.logBufferOffset
			val firstEmpty = mem.firstEmptyRecordIndex
			val highestApplied = n.machine.highestAppliedCommandIndex
			val pendingTasks = n.stepDoer.pendingTasksCount
			val leaderDiagOpt = n.inspectRole match {
				case Some(ld: LeaderRoleDiagnostic[?]) => Some(ld.asInstanceOf[LeaderRoleDiagnostic[String]])
				case _ => None
			}

			sb.append("{")
			sb.append("\"id\":\"").append(n.myId).append("\",")
			sb.append("\"role\":\"").append(roleStr).append("\",")
			sb.append("\"currentTerm\":").append(term).append(",")
			sb.append("\"isDown\":").append(n.isDown).append(",")
			sb.append("\"logBufferOffset\":").append(offset).append(",")
			sb.append("\"firstEmptyRecordIndex\":").append(firstEmpty).append(",")
			sb.append("\"highestAppliedCommandIndex\":").append(highestApplied).append(",")
			sb.append("\"pendingTasksCount\":").append(pendingTasks).append(",")
			sb.append("\"hasPendingTasks\":").append(n.stepDoer.hasPendingTasks).append(",")
			sb.append("\"autoSucceedUntilTerm\":").append(n.autoSucceedUntilTerm).append(",")
			sb.append("\"autoSucceedUntilRecordIndex\":").append(n.autoSucceedUntilRecordIndex).append(",")
			sb.append("\"isStorageAutoSucceed\":").append(n.autoSucceedUntilTerm == Int.MaxValue && n.autoSucceedUntilRecordIndex == Long.MaxValue).append(",")

			// Records
			sb.append("\"records\":[")
			val records = mem.getRecordsBetween(offset, firstEmpty)
			for rIdx <- records.indices do {
				if rIdx > 0 then sb.append(",")
				val absIdx = offset + rIdx
				sb.append(serializeRecord(records(rIdx), absIdx, leaderDiagOpt, n.myId))
			}
			sb.append("],")

			// Snapshot
			sb.append("\"snapshot\":")
			if mem.latestSnapshot.isDefined then {
				val snap = mem.latestSnapshot.get
				sb.append(s"""{"lastIncludedRecordIndex":${snap.lastIncludedRecordIndex},"lastIncludedRecordTerm":${snap.lastIncludedRecordTerm}}""")
			} else {
				sb.append("null")
			}
			sb.append("}")
		}
		sb.append("],")

		// Channels
		sb.append("\"channels\":[")
		val channels = env.allChannels
		var chanCount = 0
		for ((pair, packets) <- channels if packets.nonEmpty) do {
			if chanCount > 0 then sb.append(",")
			val (src, dst) = pair
			sb.append("{")
			sb.append("\"source\":\"").append(src).append("\",")
			sb.append("\"destination\":\"").append(dst).append("\",")
			sb.append("\"packets\":[")
			for pIdx <- packets.indices do {
				if pIdx > 0 then sb.append(",")
				val p = packets(pIdx)
				val kind = if p.isInstanceOf[RequestPacket] then "Request" else "Response"
				sb.append("{")
				sb.append("\"id\":").append(p.id).append(",")
				sb.append("\"source\":\"").append(p.source).append("\",")
				sb.append("\"destination\":\"").append(p.destination).append("\",")
				sb.append("\"departureTime\":").append(p.departureTime).append(",")
				sb.append("\"kind\":\"").append(kind).append("\",")
				sb.append("\"rpcKind\":\"").append(escapeJson(p.rpcKind)).append("\",")
				if p.isInstanceOf[RequestPacket] then {
					val req = p.asInstanceOf[RequestPacket]
					sb.append("\"recordCount\":").append(req.records.length).append(",")
					sb.append("\"records\":[")
					for rIdx <- req.records.indices do {
						if rIdx > 0 then sb.append(",")
						val r = req.records(rIdx)
						sb.append("{")
						sb.append("\"index\":").append(r.index).append(",")
						sb.append("\"term\":").append(r.term).append(",")
						sb.append("\"kind\":\"").append(escapeJson(r.kind)).append("\",")
						sb.append("\"summary\":\"").append(escapeJson(r.summary)).append("\"")
						sb.append("}")
					}
					sb.append("],")
				}
				sb.append("\"summary\":\"").append(escapeJson(p.summary)).append("\"")
				sb.append("}")
			}
			sb.append("]")
			sb.append("}")
			chanCount += 1
		}
		sb.append("],")

		// Pending Packets (flattened for global table)
		sb.append("\"pendingPackets\":[")
		val allPackets = env.pendingPackets
		for pIdx <- allPackets.indices do {
			if pIdx > 0 then sb.append(",")
			val p = allPackets(pIdx)
			val kind = if p.isInstanceOf[RequestPacket] then "Request" else "Response"
			sb.append("{")
			sb.append("\"id\":").append(p.id).append(",")
			sb.append("\"source\":\"").append(p.source).append("\",")
			sb.append("\"destination\":\"").append(p.destination).append("\",")
			sb.append("\"departureTime\":").append(p.departureTime).append(",")
			sb.append("\"kind\":\"").append(kind).append("\",")
			sb.append("\"rpcKind\":\"").append(escapeJson(p.rpcKind)).append("\",")
			if p.isInstanceOf[RequestPacket] then {
				val req = p.asInstanceOf[RequestPacket]
				sb.append("\"recordCount\":").append(req.records.length).append(",")
				sb.append("\"records\":[")
				for rIdx <- req.records.indices do {
					if rIdx > 0 then sb.append(",")
					val r = req.records(rIdx)
					sb.append("{")
					sb.append("\"index\":").append(r.index).append(",")
					sb.append("\"term\":").append(r.term).append(",")
					sb.append("\"kind\":\"").append(escapeJson(r.kind)).append("\",")
					sb.append("\"summary\":\"").append(escapeJson(r.summary)).append("\"")
					sb.append("}")
				}
				sb.append("],")
			}
			sb.append("\"summary\":\"").append(escapeJson(p.summary)).append("\"")
			sb.append("}")
		}
		sb.append("],")

		// Pending Persistence
		sb.append("\"pendingPersistence\":[")
		val pOps = env.pendingPersistenceOperations
		for pIdx <- pOps.indices do {
			if pIdx > 0 then sb.append(",")
			val pop = pOps(pIdx)
			sb.append("{")
			sb.append("\"opId\":").append(pop.opId).append(",")
			sb.append("\"nodeId\":\"").append(pop.nodeId).append("\",")
			sb.append("\"term\":").append(pop.term).append(",")
			sb.append("\"logBufferOffset\":").append(pop.logBufferOffset).append(",")
			sb.append("\"firstEmptyRecordIndex\":").append(pop.firstEmptyRecordIndex).append(",")
			sb.append("\"recordsCount\":").append(pop.records.length)
			sb.append("}")
		}
		sb.append("],")

		// Pending WakeUps
		sb.append("\"pendingWakeUps\":[")
		val wakeUps = env.pendingWakeUps
		for wIdx <- wakeUps.indices do {
			if wIdx > 0 then sb.append(",")
			val w = wakeUps(wIdx)
			sb.append("{")
			sb.append("\"tokenId\":").append(w.tokenId).append(",")
			sb.append("\"nodeId\":\"").append(w.nodeId).append("\",")
			sb.append("\"reason\":\"").append(w.reason.toString).append("\",")
			sb.append("\"scheduledTime\":").append(w.scheduledTime).append(",")
			sb.append("\"wakeupsDone\":").append(w.wakeupsDone)
			sb.append("}")
		}
		sb.append("],")

		// Client Command Statuses
		sb.append("\"clientCommands\":[")
		val clientStatuses = env.allClientStatuses
		var cCount = 0
		for ((cmdId, status) <- clientStatuses) do {
			if cCount > 0 then sb.append(",")
			val statusStr = status match {
				case ClientCommandStatus.InFlight => "InFlight"
				case ClientCommandStatus.Processed(recIdx, res) => s"Processed(index=$recIdx, res=$res)"
				case ClientCommandStatus.Redirected(lId) => s"Redirected($lId)"
				case ClientCommandStatus.Unable(flag, _) => s"Unable($flag)"
				case ClientCommandStatus.Failed(e) => s"Failed(${e.getMessage})"
			}
			sb.append("{")
			sb.append("\"commandId\":").append(cmdId).append(",")
			sb.append("\"status\":\"").append(escapeJson(statusStr)).append("\"")
			sb.append("}")
			cCount += 1
		}
		sb.append("],")

		// Client Stats
		sb.append("\"clientStats\":{")
		val clientStats = env.allClientStats
		var csCount = 0
		for ((cId, cs) <- clientStats) do {
			if csCount > 0 then sb.append(",")
			sb.append("\"").append(escapeJson(cId)).append("\":{")
			sb.append("\"lastSent\":").append(cs.lastSent.map(_.toString).getOrElse("null")).append(",")
			sb.append("\"lastSuccess\":").append(cs.lastSuccess.map(_.toString).getOrElse("null")).append(",")
			sb.append("\"lastRecordIndex\":").append(cs.lastRecordIndex.map(_.toString).getOrElse("null"))
			sb.append("}")
			csCount += 1
		}
		sb.append("},")

		// Active Settings
		sb.append("\"settings\":{")
		sb.append("\"clusterSize\":").append(env.clusterSize).append(",")
		sb.append("\"seedNodes\":[")
		val seedNodes = env.defaultInitialParticipants.toSeq
		for sIdx <- seedNodes.indices do {
			if sIdx > 0 then sb.append(",")
			sb.append("\"").append(escapeJson(seedNodes(sIdx))).append("\"")
		}
		sb.append("],")
		sb.append("\"maxInFlightAppendsPerPeer\":").append(env.maxInFlightAppendsPerPeer).append(",")
		sb.append("\"logCompactionThreshold\":").append(env.logCompactionThreshold).append(",")
		sb.append("\"retiringParticipantMaxRetries\":").append(env.retiringParticipantMaxRetries).append(",")
		sb.append("\"logRetentionAfterSnapshot\":").append(env.logRetentionAfterSnapshot)
		sb.append("},")

		// Invariant Health Check
		var logMatchingPass = true
		var invariantError: String = ""
		try {
			env.checkLogMatching()
		} catch {
			case NonFatal(e) =>
				logMatchingPass = false
				invariantError = e.getMessage
		}

		sb.append("\"invariants\":{")
		sb.append("\"logMatching\":").append(logMatchingPass).append(",")
		sb.append("\"error\":\"").append(escapeJson(invariantError)).append("\"")
		sb.append("},")

		// History & Tags
		sb.append("\"appliedOperationsCount\":").append(env.appliedOperationsCount).append(",")
		sb.append("\"tags\":[")
		val tagList = env.tags.toSeq.sortBy(_._1)
		for tIdx <- tagList.indices do {
			if tIdx > 0 then sb.append(",")
			val (tName, tOps) = tagList(tIdx)
			sb.append("{\"name\":\"").append(escapeJson(tName)).append("\",\"opsCount\":").append(tOps.size).append("}")
		}
		sb.append("]")

		sb.append("}")
		sb.toString
	}

	// =========================================================================
	// HTTP HANDLERS
	// =========================================================================

	private class StaticFileHandler extends HttpHandler {
		private lazy val htmlContent: Array[Byte] = {
			val stream = getClass.getResourceAsStream("/consensus-playground.html")
			if stream != null then {
				try stream.readAllBytes()
				finally stream.close()
			} else {
				// Fallback to reading from test resources path
				val path = java.nio.file.Paths.get("consensus/src/test/resources/consensus-playground.html")
				if java.nio.file.Files.exists(path) then java.nio.file.Files.readAllBytes(path)
				else "<html><body><h1>consensus-playground.html not found</h1></body></html>".getBytes(StandardCharsets.UTF_8)
			}
		}

		override def handle(exchange: HttpExchange): Unit = {
			val path = exchange.getRequestURI.getPath
			if path == "/" || path == "/index.html" || path == "/consensus-playground.html" then {
				exchange.getResponseHeaders.set("Content-Type", "text/html; charset=UTF-8")
				exchange.getResponseHeaders.set("Access-Control-Allow-Origin", "*")
				exchange.sendResponseHeaders(200, htmlContent.length)
				val os = exchange.getResponseBody
				os.write(htmlContent)
				os.close()
			} else {
				exchange.sendResponseHeaders(404, -1)
			}
		}
	}

	private class StateHandler extends HttpHandler {
		override def handle(exchange: HttpExchange): Unit = {
			if exchange.getRequestMethod.equalsIgnoreCase("OPTIONS") then {
				exchange.getResponseHeaders.set("Access-Control-Allow-Origin", "*")
				exchange.getResponseHeaders.set("Access-Control-Allow-Methods", "GET, OPTIONS")
				exchange.sendResponseHeaders(204, -1)
				return
			}
			val json = serializeState()
			val bytes = json.getBytes(StandardCharsets.UTF_8)
			exchange.getResponseHeaders.set("Content-Type", "application/json; charset=UTF-8")
			exchange.getResponseHeaders.set("Access-Control-Allow-Origin", "*")
			exchange.sendResponseHeaders(200, bytes.length)
			val os = exchange.getResponseBody
			os.write(bytes)
			os.close()
		}
	}

	private class EventsHandler extends HttpHandler {
		override def handle(exchange: HttpExchange): Unit = {
			if exchange.getRequestMethod.equalsIgnoreCase("OPTIONS") then {
				exchange.getResponseHeaders.set("Access-Control-Allow-Origin", "*")
				exchange.getResponseHeaders.set("Access-Control-Allow-Methods", "GET, OPTIONS")
				exchange.getResponseHeaders.set("Access-Control-Allow-Headers", "Cache-Control")
				exchange.sendResponseHeaders(204, -1)
				return
			}

			if !exchange.getRequestMethod.equalsIgnoreCase("GET") then {
				exchange.sendResponseHeaders(405, -1)
				return
			}

			exchange.getResponseHeaders.set("Content-Type", "text/event-stream; charset=UTF-8")
			exchange.getResponseHeaders.set("Cache-Control", "no-cache")
			exchange.getResponseHeaders.set("Connection", "keep-alive")
			exchange.getResponseHeaders.set("Access-Control-Allow-Origin", "*")
			exchange.sendResponseHeaders(200, 0)

			val os = exchange.getResponseBody
			val client = new SseClient(os)
			sseClients.add(client)

			val initialJson = serializeState()
			try client.send(s"data: $initialJson\n\n".getBytes(StandardCharsets.UTF_8))
			catch {
				case NonFatal(_) =>
					client.close()
					sseClients.remove(client)
					return
			}

			try client.awaitClose()
			finally {
				client.close()
				sseClients.remove(client)
			}
		}
	}

	private class SseClient(val os: OutputStream) {
		private val closeLatch = new CountDownLatch(1)
		private val writeLock = new Object()
		@volatile private var closed = false

		def send(bytes: Array[Byte]): Unit = writeLock.synchronized {
			if closed then throw new IOException("SSE client is closed")
			os.write(bytes)
			os.flush()
		}

		def close(): Unit = {
			if !closed then {
				closed = true
				closeLatch.countDown()
				try os.close() catch {
					case NonFatal(_) => ()
				}
			}
		}

		def awaitClose(): Unit = {
			while !closed && server != null do {
				if closeLatch.await(10, TimeUnit.SECONDS) then return
				if !closed && server != null then {
					try send(": keep-alive\n\n".getBytes(StandardCharsets.UTF_8))
					catch {
						case NonFatal(_) =>
							close()
							return
					}
				}
			}
		}
	}

	def broadcastState(): Unit = {
		val json = serializeState()
		broadcastState(json)
	}

	private def broadcastState(json: String): Unit = {
		val payload = s"data: $json\n\n".getBytes(StandardCharsets.UTF_8)
		val it = sseClients.iterator()
		while it.hasNext do {
			val client = it.next()
			try client.send(payload)
			catch {
				case NonFatal(_) =>
					client.close()
					sseClients.remove(client)
			}
		}
	}

	private class ActionHandler extends HttpHandler {
		override def handle(exchange: HttpExchange): Unit = {
			if exchange.getRequestMethod.equalsIgnoreCase("OPTIONS") then {
				exchange.getResponseHeaders.set("Access-Control-Allow-Origin", "*")
				exchange.getResponseHeaders.set("Access-Control-Allow-Methods", "POST, OPTIONS")
				exchange.getResponseHeaders.set("Access-Control-Allow-Headers", "Content-Type")
				exchange.sendResponseHeaders(204, -1)
				return
			}

			if !exchange.getRequestMethod.equalsIgnoreCase("POST") then {
				exchange.sendResponseHeaders(405, -1)
				return
			}

			val body = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
			var success = true
			var message = "OK"

			lock.synchronized {
				try {
					val params = parseSimpleJson(body)
					val action = params.getOrElse("action", "")

					action match {
						case "stepNode" =>
							val n = params("node")
							env.stepNode(n)

						case "drainNode" =>
							val n = params("node")
							env.runNodeUntilIdle(n)

						case "stepAll" =>
							env.stepAllNodes()

						case "drainAll" =>
							env.runAllNodesUntilIdle()

						case "dispatchPacket" =>
							val id = params("packetId").toInt
							env.dispatchPacket(id)

						case "dropPacket" =>
							val id = params("packetId").toInt
							env.dropPacket(id)

						case "dispatchNext" =>
							val from = params("from")
							val to = params("to")
							env.dispatchNext(from, to)

						case "dropNext" =>
							val from = params("from")
							val to = params("to")
							env.dropNext(from, to)

						case "dispatchAllBetween" =>
							val from = params("from")
							val to = params("to")
							env.dispatchAllBetween(from, to)

						case "dispatchAll" =>
							env.dispatchAll()

						case "advanceTime" =>
							val ticks = params.getOrElse("ticks", "10").toInt
							env.advanceTime(ticks)

						case "submitCommand" =>
							val target = params.getOrElse("targetNode", "0")
							val client = params.getOrElse("client", "1")
							env.submitClientCommand(target, client)

						case "submitConfigChange" =>
							val target = params.getOrElse("targetNode", "0")
							val desiredStr = params.getOrElse("desired", "")
							val desired = desiredStr.split(",").map(_.trim).filter(_.nonEmpty).toSet
							env.submitConfigChange(target, desired.asInstanceOf[Set[NodeRef]])

						case "completeStorageSave" =>
							val node = params("node")
							env.completeNextStorageSave(node)

						case "failStorageSave" =>
							val opId = params("opId").toInt
							val err = params.getOrElse("error", "Simulated disk failure")
							env.failStorageSave(opId, new RuntimeException(err))

						case "startNode" =>
							val node = params("node")
							env.startNode(node)

						case "releaseNode" =>
							val node = params("node")
							env.crashNode(node)

						case "resetCluster" =>
							val size = params.getOrElse("clusterSize", "3").toInt
							resetCluster(size)

						case "loadPreset" =>
							val preset = params.getOrElse("preset", "figure7")
							loadPreset(preset)

						case "initScenario" =>
							val preset = params.getOrElse("preset", "custom")
							val size = params.getOrElse("clusterSize", "3").toInt
							val seedNodesParam = params.get("seedNodes").map(_.trim).filter(_.nonEmpty)
							val seedNodesOpt = seedNodesParam.map { str =>
								str.split(",").map(_.trim).filter(_.nonEmpty).toSet
							}
							val maxInFlight = params.getOrElse("maxInFlight", "2").toInt
							val logCompaction = params.getOrElse("logCompaction", "5").toInt

							if preset == "custom" || preset == "default" then {
								resetCluster(size, seedNodesOpt, maxInFlight, logCompaction)
							} else {
								loadPreset(preset, seedNodesOpt, maxInFlight, logCompaction)
							}

						case "undo" =>
							val undone = env.undo()
							if undone then {
								message = "Undid last operation"
							} else {
								success = false
								message = "No operations to undo"
							}

						case "createTag" =>
							val name = params.getOrElse("name", "").trim
							if name.isEmpty then {
								success = false
								message = "Tag name cannot be empty"
							} else {
								env.createTag(name)
								message = s"Created tag '$name'"
							}

						case "restoreTag" =>
							val name = params.getOrElse("name", "").trim
							if name.isEmpty then {
								success = false
								message = "Tag name cannot be empty"
							} else {
								val restored = env.restoreTag(name)
								if restored then {
									message = s"Restored to tag '$name'"
								} else {
									success = false
									message = s"Tag '$name' not found"
								}
							}

						case "deleteTag" =>
							val name = params.getOrElse("name", "").trim
							if name.isEmpty then {
								success = false
								message = "Tag name cannot be empty"
							} else {
								val deleted = env.deleteTag(name)
								if deleted then {
									message = s"Deleted tag '$name'"
								} else {
									success = false
									message = s"Tag '$name' not found"
								}
							}

						case "toggleStorageAutoSucceed" =>
							val targetOpt = params.get("node").filter(n => n.nonEmpty && n != "all")
							env.toggleStorageAutoSucceed(targetOpt)
							message = params.get("node") match {
								case Some("all") | None => "Toggled storage auto-succeed on all nodes"
								case Some(nodeId) => s"Toggled storage auto-succeed on $nodeId"
							}

						case "updateDynamicSettings" =>
							val retriesOpt = params.get("retiringMaxRetries").map(_.toInt)
							val retentionOpt = params.get("logRetention").map(_.toInt)
							val termOpt = params.get("autoSucceedUntilTerm").map(_.toInt.asInstanceOf[Term])
							val idxOpt = params.get("autoSucceedUntilRecordIndex").map(_.toLong)
							val nodeOpt = params.get("node").filter(n => n.nonEmpty && n != "all")
							env.updateDynamicSettings(retriesOpt, retentionOpt, termOpt, idxOpt, nodeOpt)
							message = "Dynamic settings updated"

						case "shutdown" =>
							message = "Server shutting down"
							val t = new Thread(() => {
								try {
									Thread.sleep(200)
									stop()
								} catch {
									case NonFatal(_) => ()
								}
							}, "server-shutdown-thread")
							t.setDaemon(true)
							t.start()

						case other =>
							success = false
							message = s"Unknown action: $other"
					}
				} catch {
					case NonFatal(e) =>
						success = false
						message = s"Action execution error: ${e.getMessage}"
				}
			}

			if success then broadcastState()

			val responseJson = s"""{"success":$success,"message":"${escapeJson(message)}"}"""
			val bytes = responseJson.getBytes(StandardCharsets.UTF_8)
			exchange.getResponseHeaders.set("Content-Type", "application/json; charset=UTF-8")
			exchange.getResponseHeaders.set("Access-Control-Allow-Origin", "*")
			exchange.sendResponseHeaders(if success then 200 else 400, bytes.length)
			val os = exchange.getResponseBody
			os.write(bytes)
			os.close()
		}

		private def parseSimpleJson(json: String): Map[String, String] = {
			val map = mutable.Map.empty[String, String]
			val clean = json.trim.stripPrefix("{").stripSuffix("}").trim
			if clean.isEmpty then return map.toMap

			val tokens = clean.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
			for token <- tokens do {
				val kv = token.split(":(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", 2)
				if kv.length == 2 then {
					val key = kv(0).trim.stripPrefix("\"").stripSuffix("\"")
					val value = kv(1).trim.stripPrefix("\"").stripSuffix("\"")
					map(key) = value
				}
			}
			map.toMap
		}
	}
}

object ConsensusPlaygroundServer {
	def main(args: Array[String]): Unit = {
		val port = if args.nonEmpty then args(0).toInt else 8080
		val server = new ConsensusPlaygroundServer(port)
		server.start()
		println(s"ConsensusPlaygroundServer running at http://localhost:$port")
		server.awaitShutdown()
		println("ConsensusPlaygroundServer stopped.")
	}
}
