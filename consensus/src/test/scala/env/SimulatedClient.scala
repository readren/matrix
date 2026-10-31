package readren.consensus.env

import readren.consensus.ConsensusParticipantSdm.*
import readren.consensus.protocol.*

import scala.collection.mutable

/** Parameter recommendation providing default inputs for a prospective client command submission.
 *
 * @param targetNode the recommended target participant endpoint.
 * @param serial the suggested serial number for the command.
 * @param attemptFlag the recommended protocol attempt flag (`FIRST_ATTEMPT`, `REDIRECTED`, or `FALLBACK`).
 * @param isRetry true if this recommendation corresponds to a retry of a previously attempted command. */
final case class ClientSubmissionHint(
	targetNode: NodeId,
	serial: Int,
	attemptFlag: CommandAttemptFlag,
	isRetry: Boolean
)

/** Simulated external client (Tier 3) driving business command submissions, tracking command
 * completion, and executing protocol-driven target failovers and retries under epistemic uncertainty.
 *
 * This component maintains strictly private endpoint discovery and target routing state without omniscient
 * access to physical node liveness (`n.isDown`) or internal consensus role diagnostics.
 *
 * @param clientId unique client identifier string.
 * @param env the target [[ConsensusEnvironment]].
 * @param initialKnownEndpoints initial cluster endpoints known to the client.
 * @param maxInFlightSteps number of elapsed steps before an in-flight command is considered timed out.
 * @param maxRetries maximum retry cycles across all known endpoints before reporting permanent failure.
 * @param pipelineWindow maximum number of concurrent in-flight commands permitted before awaiting resolution. */
class SimulatedClient(
	val clientId: String,
	val env: ConsensusEnvironment,
	initialKnownEndpoints: Iterable[NodeId],
	val maxInFlightSteps: Int = 10,
	val maxRetries: Int = 15,
	val pipelineWindow: Int = 1
) {
	private final class InFlightCommand(
		var handle: ClientCommandHandle,
		val serial: Int,
		var inFlightSteps: Int = 0,
		var retries: Int = 0,
		val triedEndpoints: mutable.Set[NodeId] = mutable.Set.empty,
		var attemptFlag: CommandAttemptFlag = FIRST_ATTEMPT
	)

	private var _currentTarget: NodeId = initialKnownEndpoints.headOption.getOrElse(NodeId("p-0"))
	private val _knownEndpoints: mutable.Set[NodeId] = mutable.Set.from(initialKnownEndpoints)
	private val _inFlightBySerial: mutable.LinkedHashMap[Int, InFlightCommand] = mutable.LinkedHashMap.empty
	private var _nextSerialToSubmit: Int = 1
	private var _lastSuccessfulSerial: Option[Int] = None
	private var _lastSuccessfulRecordIndex: Option[RecordIndex] = None
	private var _totalCommandsSubmitted: Int = 0
	private var _totalCommandsSucceeded: Int = 0

	/** Returns the participant identifier currently targeted by prospective client commands. */
	def currentTarget: NodeId = _currentTarget

	/** Returns an immutable snapshot of all cluster endpoints currently discovered and known to this client. */
	def knownEndpoints: Set[NodeId] = _knownEndpoints.toSet

	/** Returns the lowest uncompleted serial number, or the next prospective serial if idle. */
	def currentCommandSerial: Int =
		if _inFlightBySerial.nonEmpty then _inFlightBySerial.keys.min
		else _nextSerialToSubmit

	/** Returns the serial number of the most recently committed command on this client, if any. */
	def lastSuccessfulSerial: Option[Int] = _lastSuccessfulSerial

	/** Returns the log record index where the most recently committed command was applied, if any. */
	def lastSuccessfulRecordIndex: Option[RecordIndex] = _lastSuccessfulRecordIndex

	/** Returns the handle of the currently in-flight command, or [[None]] if the client is idle. */
	def activeCommandHandle: Option[ClientCommandHandle] = _inFlightBySerial.headOption.map(_._2.handle)

	/** True if there is no command currently awaiting response resolution. */
	def isIdle: Boolean = _inFlightBySerial.isEmpty

	/** Returns the cumulative count of command submission attempts dispatched by this client. */
	def totalCommandsSubmitted: Int = _totalCommandsSubmitted

	/** Returns the cumulative count of commands successfully committed and processed. */
	def totalCommandsSucceeded: Int = _totalCommandsSucceeded

	/** Returns the maximum retry count among currently in-flight commands, or 0 if idle. */
	def commandRetries: Int =
		if _inFlightBySerial.isEmpty then 0
		else _inFlightBySerial.values.map(_.retries).max

	/** Polls the status of active in-flight commands, updating routing and retry state if responses arrive. */
	def pollActiveCommand(): Unit = {
		if _inFlightBySerial.isEmpty then return
		val completedSerials = mutable.ArrayBuffer.empty[Int]
		for (serial, inFlight) <- _inFlightBySerial do {
			env.clientCommandStatus(inFlight.handle.commandId) match {
				case ClientCommandStatus.InFlight =>
					inFlight.inFlightSteps += 1
					if inFlight.inFlightSteps > maxInFlightSteps then {
						inFlight.inFlightSteps = 0
						val untried = _knownEndpoints.find(p => !inFlight.triedEndpoints.contains(p))
						untried match {
							case Some(next) =>
								_currentTarget = next
							case None =>
								inFlight.retries += 1
								if inFlight.retries > maxRetries then {
									throw new AssertionError(s"Client $clientId command $serial timed out after $maxRetries retries across endpoints ${_knownEndpoints}")
								}
								inFlight.triedEndpoints.clear()
								_currentTarget = _knownEndpoints.headOption.getOrElse(_currentTarget)
						}
						inFlight.triedEndpoints.add(_currentTarget)
						inFlight.attemptFlag = FALLBACK
						inFlight.handle = env.submitClientCommand(
							targetNode = _currentTarget,
							client = clientId,
							serial = Some(serial),
							attemptFlag = FALLBACK
						)
						_totalCommandsSubmitted += 1
					}

				case ClientCommandStatus.Processed(recIdx, _) =>
					_lastSuccessfulSerial = Some(math.max(_lastSuccessfulSerial.getOrElse(0), serial))
					_lastSuccessfulRecordIndex = Some(recIdx)
					_totalCommandsSucceeded += 1
					completedSerials += serial

				case ClientCommandStatus.Redirected(suggested) =>
					_knownEndpoints ++= suggested
					if suggested.nonEmpty then _currentTarget = suggested.head
					inFlight.inFlightSteps = 0
					inFlight.attemptFlag = REDIRECTED
					inFlight.triedEndpoints.add(_currentTarget)
					inFlight.handle = env.submitClientCommand(
						targetNode = _currentTarget,
						client = clientId,
						serial = Some(serial),
						attemptFlag = REDIRECTED
					)
					_totalCommandsSubmitted += 1

				case ClientCommandStatus.Unable(nextAttemptFlag, otherParticipants) =>
					_knownEndpoints ++= otherParticipants
					val candidatePool = otherParticipants ++ _knownEndpoints
					val untried = candidatePool.find(p => !inFlight.triedEndpoints.contains(p))
					untried match {
						case Some(next) =>
							_currentTarget = next
						case None =>
							inFlight.retries += 1
							if inFlight.retries > maxRetries then {
								throw new AssertionError(s"Client $clientId unable to progress command $serial after $maxRetries cycles across endpoints ${_knownEndpoints}")
							}
							inFlight.triedEndpoints.clear()
							_currentTarget = candidatePool.headOption.getOrElse(_currentTarget)
					}
					inFlight.inFlightSteps = 0
					inFlight.attemptFlag = nextAttemptFlag
					inFlight.triedEndpoints.add(_currentTarget)
					inFlight.handle = env.submitClientCommand(
						targetNode = _currentTarget,
						client = clientId,
						serial = Some(serial),
						attemptFlag = nextAttemptFlag
					)
					_totalCommandsSubmitted += 1

				case ClientCommandStatus.Failed(cause) =>
					inFlight.inFlightSteps = 0
					val untried = _knownEndpoints.find(p => !inFlight.triedEndpoints.contains(p))
					untried match {
						case Some(next) =>
							_currentTarget = next
						case None =>
							inFlight.retries += 1
							if inFlight.retries > maxRetries then {
								throw new AssertionError(s"Client $clientId command $serial failed after $maxRetries retries: ${cause.getMessage}", cause)
							}
							inFlight.triedEndpoints.clear()
							_currentTarget = _knownEndpoints.headOption.getOrElse(_currentTarget)
					}
					inFlight.triedEndpoints.add(_currentTarget)
					inFlight.attemptFlag = FALLBACK
					inFlight.handle = env.submitClientCommand(
						targetNode = _currentTarget,
						client = clientId,
						serial = Some(serial),
						attemptFlag = FALLBACK
					)
					_totalCommandsSubmitted += 1
			}
		}
		for s <- completedSerials do _inFlightBySerial.remove(s)
	}

	/** Returns a recommended submission parameter hint for UI dialog defaults or external coordinator inspection. */
	def nextSubmissionHint: ClientSubmissionHint = {
		pollActiveCommand()
		val flag = _inFlightBySerial.headOption.map(_._2.attemptFlag).getOrElse(FIRST_ATTEMPT)
		val isRetry = flag != FIRST_ATTEMPT
		ClientSubmissionHint(_currentTarget, currentCommandSerial, flag, isRetry)
	}

	/** Synchronizes client routing state when an external or manual command submission occurs.
	 *
	 * @param target the target participant receiving the manual submission.
	 * @param serial the command serial number submitted.
	 * @param attemptFlag the protocol attempt flag used. */
	def recordSubmission(target: NodeId, serial: Int, attemptFlag: CommandAttemptFlag): Unit = {
		_currentTarget = target
		_knownEndpoints.add(target)
		_nextSerialToSubmit = math.max(_nextSerialToSubmit, serial + 1)
	}

	/** Synchronizes client routing state when an external or manual command submission occurs via a submission handle.
	 *
	 * @param handle the submission handle returned by [[ConsensusEnvironment.submitClientCommand]].
	 * @param attemptFlag the protocol attempt flag used. */
	def recordSubmission(handle: ClientCommandHandle, attemptFlag: CommandAttemptFlag): Unit = {
		_currentTarget = handle.targetNodeId
		_knownEndpoints.add(handle.targetNodeId)
		_inFlightBySerial(handle.serial) = InFlightCommand(
			handle = handle,
			serial = handle.serial,
			inFlightSteps = 0,
			retries = 0,
			triedEndpoints = mutable.Set(handle.targetNodeId),
			attemptFlag = attemptFlag
		)
		_nextSerialToSubmit = math.max(_nextSerialToSubmit, handle.serial + 1)
		_totalCommandsSubmitted += 1
	}

	/** Progresses the client command lifecycle by polling active in-flight commands for completion or redirection, or dispatching up to `pipelineWindow` scheduled commands up to `targetCommandCount`.
	 *
	 * @param targetCommandCount the upper bound on command serials to drive. */
	def driveCommandLifecycle(targetCommandCount: Int): Unit = {
		pollActiveCommand()
		while _inFlightBySerial.size < pipelineWindow && _nextSerialToSubmit <= targetCommandCount do {
			val serial = _nextSerialToSubmit
			val handle = env.submitClientCommand(
				targetNode = _currentTarget,
				client = clientId,
				serial = Some(serial),
				attemptFlag = FIRST_ATTEMPT
			)
			_inFlightBySerial(serial) = InFlightCommand(
				handle = handle,
				serial = serial,
				inFlightSteps = 0,
				retries = 0,
				triedEndpoints = mutable.Set(_currentTarget),
				attemptFlag = FIRST_ATTEMPT
			)
			_nextSerialToSubmit += 1
			_totalCommandsSubmitted += 1
		}
	}
}
