package readren.nexus
package behaviors

import core.{Behavior, Continue, ContinueWith, HandleResult}

import scala.language.experimental.saferExceptions
import scala.reflect.Typeable

/**
 * A trait that represents a behavior that can handle messages with checked exception handling.
 *
 * This trait is designed to work with Scala's experimental safer exceptions feature, allowing behaviors to explicitly declare which exceptions they can throw and handle them in a type-safe manner.
 *
 * @tparam A The type of messages this behavior can handle
 * @tparam E The type of exception this behavior can throw (must be a subtype of Exception<strike> and have a Typeable instance</strike>)
 *
 * @see [[core.Behavior]] for the base behavior interface
 * @see [[core.HandleResult]] for possible handling results
 * @see [[CheckedBehavior.recover]] for converting to a safe behavior
 * @see [[CheckedBehavior.factory]] for creating instances from functions
 */
trait CheckedBehavior[-A, E <: Exception /* : Typeable*/] {

	/**
	 * Handles a message with checked exception handling.
	 *
	 * This method processes the given message and may throw exceptions of type `E`.
	 * The caller must have the appropriate [[CanThrow[M]] capability to handle these exceptions.
	 *
	 * @param message The message to handle
	 * @param ctM The capability to throw exceptions of type M
	 * @return Either the next behavior to use or a handling result. Returning a [[CheckedBehavior]] is like returning a [[ContinueWith]] but continuing with a [[CheckedBehavior]] instead of a regular [[Behavior]].
	 * @throws E the checked exception that the implementation may throw safely.
	 */
	def handleChecked(message: A)(using ctM: CanThrow[E]): CheckedBehavior[A, E] | HandleResult[A]


	/**
	 * Converts a [[CheckedBehavior]] to a regular [[Behavior]] that generates the [[CanThrow]] capabilities with the help of the provided `recovery` function.
	 *
	 * This method wraps the checked behavior in a safe behavior that catches exceptions of type `E` and applies the recovery handler to determine the next behavior or result.
	 *
	 * Implementation note: The `inline` modifier is necessary to allow the compiler to know the concrete type of `E` at compile-time.
	 * Otherwise, the `case m: M => ...` part would be unchecked, and we can't use [[TypeTest]] here because the current version of scala (2.6.2) only generates  capabilities for catch clauses of the form `case ex: Ex =>`.
	 * Additionally, the `safer` method must reside within `makeSafe` to preserve the visibility and proper scoping of `E`.
	 *
	 * @param recovery A complete function that converts exceptions of type `E` to an indicator of the behavior with which the host actant will handle the next message.
	 * @return A safe behavior that handles exceptions internally
	 *
	 * @example
	 * {{{
	 *	val nexus = new NexusTyped(...)
	 *  val parentDoer = nexus.provideDoer("parent", DefaultCooperativeWorkersDpd)
	 *	nexus.spawns[Cmd](RegularRf, parentDoer) { parent =>
	 *		CheckedBehavior.factory[Cmd, MyException] {
	 *			case cmd: DoWork =>
	 *				if (cmd.integer % 5) >= 3 then throw new MyException
	 *				cmd.replyTo.tell(Response(cmd.integer.toString))
	 *				Continue
	 *			case Relax(replyTo) =>
	 *				replyTo.tell(Response(null))
	 *				Continue
	 *		}.recover { (m: MyException) =>
	 *			println(s"Recovering from $m")
	 *			Continue
	 *		}
	 *	}
	 * }}} */
	inline def recover[A1 <: A](inline recovery: E => CheckedBehavior[A1, E] | HandleResult[A1]): Behavior[A1] = {
		def safer[A2](checkedBehavior: CheckedBehavior[A2, E]): Behavior[A2] =
			(message: A2) => {
				val next =
					try checkedBehavior.handleChecked(message)
					catch {
						case m: E => recovery(m)
					}
				next match {
					case hr: HandleResult[A2 @unchecked] => hr
					case cb: CheckedBehavior[A2 @unchecked, E @unchecked] =>
						if cb eq checkedBehavior then Continue else ContinueWith(safer(cb))
				}
			}

		safer(this)
	}
}

/**
 * Companion object providing utility methods for working with [[CheckedBehavior]] instances.
 */
object CheckedBehavior {

	/**
	 * Creates a [[CheckedBehavior]] instance from a message handling function.
	 *
	 * This factory method allows you to create checked behaviors from simple functions
	 * without manually implementing the trait. The function should return either the next
	 * behavior or a handling result.
	 *
	 * @tparam A The type of messages the behavior handles
	 * @tparam E The type of exception the behavior can throw
	 * @param msgHandler The function that handles messages and returns the next behavior or result
	 * @return A new CheckedBehavior instance that delegates to the provided function
	 *
	 * @example
	 * {{{
	 * val behavior = CheckedBehavior.factory[String, IllegalArgumentException, IllegalArgumentException] { msg =>
	 *   if (msg.isEmpty) {
	 *     throw new IllegalArgumentException("Message cannot be empty")
	 *   } else {
	 *     Continue
	 *   }
	 * }
	 * }}}
	 */
	inline def factory[A, E <: Exception /*: Typeable*/](
		inline msgHandler: A => CheckedBehavior[A, E] | HandleResult[A]
	): CheckedBehavior[A, E] = {
		// An object (instead of an anonymous class) was used to avoid the "New anonymous class definition will be duplicated at each inline site" warning. It is intended to create a new anonymous class at each call site instead of eta-expanding the `msgHandler`. 
		object anonymous extends CheckedBehavior[A, E] {
			override def handleChecked(message: A)(using ctM: CanThrow[E]): CheckedBehavior[A, E] | HandleResult[A] =
				msgHandler(message)
		}
		anonymous
	}
}
