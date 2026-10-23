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
 * @param maxRetries maximum retry cycles across all known endpoints before reporting permanent failure. */
class SimulatedClient(
	val clientId: String,
	val env: ConsensusEnvironment,
	initialKnownEndpoints: Iterable[NodeId],
	val maxInFlightSteps: Int = 10,
	val maxRetries: Int = 15
) {
	private var _currentTarget: NodeId = initialKnownEndpoints.headOption.getOrElse(NodeId("p-0"))
	private val _knownEndpoints: mutable.Set[NodeId] = mutable.Set.from(initialKnownEndpoints)
	private val _triedEndpointsForCommand: mutable.Set[NodeId] = mutable.Set.empty
	private var _currentAttemptFlag: CommandAttemptFlag = FIRST_ATTEMPT
	private var _activeCommandHandle: Option[ClientCommandHandle] = None
	private var _commandInFlightSteps: Int = 0
	private var _commandRetries: Int = 0
	private var _currentCommandSerial: Int = 1
	private var _lastSuccessfulSerial: Option[Int] = None
	private var _lastSuccessfulRecordIndex: Option[RecordIndex] = None
	private var _totalCommandsSubmitted: Int = 0
	private var _totalCommandsSucceeded: Int = 0

	/** Returns the participant identifier currently targeted by prospective client commands. */
	def currentTarget: NodeId = _currentTarget

	/** Returns an immutable snapshot of all cluster endpoints currently discovered and known to this client. */
	def knownEndpoints: Set[NodeId] = _knownEndpoints.toSet

	/** Returns the serial number of the command currently being submitted or retried. */
	def currentCommandSerial: Int = _currentCommandSerial

	/** Returns the serial number of the most recently committed command on this client, if any. */
	def lastSuccessfulSerial: Option[Int] = _lastSuccessfulSerial

	/** Returns the log record index where the most recently committed command was applied, if any. */
	def lastSuccessfulRecordIndex: Option[RecordIndex] = _lastSuccessfulRecordIndex

	/** Returns the handle of the currently in-flight command, or [[None]] if the client is idle. */
	def activeCommandHandle: Option[ClientCommandHandle] = _activeCommandHandle

	/** True if there is no command currently awaiting response resolution. */
	def isIdle: Boolean = _activeCommandHandle.isEmpty

	/** Returns the cumulative count of command submission attempts dispatched by this client. */
	def totalCommandsSubmitted: Int = _totalCommandsSubmitted

	/** Returns the cumulative count of commands successfully committed and processed. */
	def totalCommandsSucceeded: Int = _totalCommandsSucceeded

	/** Returns the number of retry cycles exhausted across known endpoints for the active command. */
	def commandRetries: Int = _commandRetries

	/** Polls the status of the active in-flight command, updating routing and retry state if a response has arrived. */
	def pollActiveCommand(): Unit = {
		_activeCommandHandle.foreach { handle =>
			env.clientCommandStatus(handle.commandId) match {
				case ClientCommandStatus.InFlight =>
					_commandInFlightSteps += 1
					if _commandInFlightSteps > maxInFlightSteps then {
						_commandInFlightSteps = 0
						_activeCommandHandle = None
						val untried = _knownEndpoints.find(p => !_triedEndpointsForCommand.contains(p))
						untried match {
							case Some(next) =>
								_currentTarget = next
							case None =>
								_commandRetries += 1
								if _commandRetries > maxRetries then {
									throw new AssertionError(s"Client $clientId command ${_currentCommandSerial} timed out after $maxRetries retries across endpoints ${_knownEndpoints}")
								}
								_triedEndpointsForCommand.clear()
								_currentTarget = _knownEndpoints.headOption.getOrElse(_currentTarget)
						}
						_currentAttemptFlag = FALLBACK
					}

				case ClientCommandStatus.Processed(recIdx, _) =>
					_lastSuccessfulSerial = Some(_currentCommandSerial)
					_lastSuccessfulRecordIndex = Some(recIdx)
					_totalCommandsSucceeded += 1
					_activeCommandHandle = None
					_commandInFlightSteps = 0
					_commandRetries = 0
					_triedEndpointsForCommand.clear()
					_currentCommandSerial += 1
					_currentAttemptFlag = FIRST_ATTEMPT

				case ClientCommandStatus.Redirected(suggested) =>
					_activeCommandHandle = None
					_commandInFlightSteps = 0
					_knownEndpoints ++= suggested
					if suggested.nonEmpty then _currentTarget = suggested.head
					_currentAttemptFlag = REDIRECTED

				case ClientCommandStatus.Unable(nextAttemptFlag, otherParticipants) =>
					_activeCommandHandle = None
					_commandInFlightSteps = 0
					_knownEndpoints ++= otherParticipants
					val candidatePool = otherParticipants ++ _knownEndpoints
					val untried = candidatePool.find(p => !_triedEndpointsForCommand.contains(p))
					untried match {
						case Some(next) =>
							_currentTarget = next
							_currentAttemptFlag = nextAttemptFlag
						case None =>
							_commandRetries += 1
							if _commandRetries > maxRetries then {
								throw new AssertionError(s"Client $clientId unable to progress command ${_currentCommandSerial} after $maxRetries cycles across endpoints ${_knownEndpoints}")
							}
							_triedEndpointsForCommand.clear()
							_currentTarget = candidatePool.headOption.getOrElse(_currentTarget)
							_currentAttemptFlag = nextAttemptFlag
					}

				case ClientCommandStatus.Failed(cause) =>
					_activeCommandHandle = None
					_commandInFlightSteps = 0
					val untried = _knownEndpoints.find(p => !_triedEndpointsForCommand.contains(p))
					untried match {
						case Some(next) =>
							_currentTarget = next
						case None =>
							_commandRetries += 1
							if _commandRetries > maxRetries then {
								throw new AssertionError(s"Client $clientId command ${_currentCommandSerial} failed after $maxRetries retries: ${cause.getMessage}", cause)
							}
							_triedEndpointsForCommand.clear()
							_currentTarget = _knownEndpoints.headOption.getOrElse(_currentTarget)
					}
					_currentAttemptFlag = FALLBACK
			}
		}
	}

	/** Returns a recommended submission parameter hint for UI dialog defaults or external coordinator inspection. */
	def nextSubmissionHint: ClientSubmissionHint = {
		pollActiveCommand()
		val isRetry = _currentAttemptFlag != FIRST_ATTEMPT
		ClientSubmissionHint(_currentTarget, _currentCommandSerial, _currentAttemptFlag, isRetry)
	}

	/** Synchronizes client routing state when an external or manual command submission occurs.
	 *
	 * @param target the target participant receiving the manual submission.
	 * @param serial the command serial number submitted.
	 * @param attemptFlag the protocol attempt flag used. */
	def recordSubmission(target: NodeId, serial: Int, attemptFlag: CommandAttemptFlag): Unit = {
		_currentTarget = target
		_knownEndpoints.add(target)
		_currentCommandSerial = serial
		_currentAttemptFlag = attemptFlag
		_triedEndpointsForCommand.add(target)
	}

	/** Synchronizes client routing state when an external or manual command submission occurs via a submission handle.
	 *
	 * @param handle the submission handle returned by [[ConsensusEnvironment.submitClientCommand]].
	 * @param attemptFlag the protocol attempt flag used. */
	def recordSubmission(handle: ClientCommandHandle, attemptFlag: CommandAttemptFlag): Unit = {
		_currentTarget = handle.targetNodeId
		_knownEndpoints.add(handle.targetNodeId)
		_currentCommandSerial = handle.serial
		_currentAttemptFlag = attemptFlag
		_activeCommandHandle = Some(handle)
		_triedEndpointsForCommand.add(handle.targetNodeId)
		_commandInFlightSteps = 0
		_totalCommandsSubmitted += 1
	}

	/** Progresses the client command lifecycle by polling active in-flight commands for completion or redirection, or dispatching the next scheduled command up to `targetCommandCount`.
	 *
	 * @param targetCommandCount the upper bound on command serials to drive. */
	def driveCommandLifecycle(targetCommandCount: Int): Unit = {
		if _currentCommandSerial <= targetCommandCount then {
			_activeCommandHandle match {
				case None =>
					val handle = env.submitClientCommand(
						targetNode = _currentTarget,
						client = clientId,
						serial = Some(_currentCommandSerial),
						attemptFlag = _currentAttemptFlag
					)
					_activeCommandHandle = Some(handle)
					_triedEndpointsForCommand.add(_currentTarget)
					_commandInFlightSteps = 0
					_totalCommandsSubmitted += 1

				case Some(_) =>
					pollActiveCommand()
			}
		}
	}
}
