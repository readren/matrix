package readren.matrix

import Behavior.defaultCatcher

import scala.reflect.TypeTest

trait MsgBehavior[-A] { thisMsgBehavior =>
	def handleMsg(message: A): HandleResult[A]
}

trait SignalBehavior[-A] { thisSignalBehavior =>
	def handleSignal(signal: Signal): HandleResult[A]
}

trait Behavior[-A] extends MsgBehavior[A], SignalBehavior[A]

object Behavior {
	inline def ignore: Behavior[Any] = IgnoreAllBehavior

	inline def handleMsg[A](msgHandler: MsgBehavior[A]): MsgBehavior[A] = msgHandler

	inline def handleSignal[A](signalHandler: SignalBehavior[A]): SignalBehavior[A] = signalHandler

	inline def fusion[A](mb: MsgBehavior[A])(sb: SignalBehavior[A]): Behavior[A] = new Fusion[A](mb, sb)

	inline def factory[A](inline msgHandler: A => HandleResult[A])(inline signalHandler: Signal => HandleResult[A]): Behavior[A] = {
		new Behavior[A] {
			override def handleMsg(message: A): HandleResult[A] = msgHandler(message)

			override def handleSignal(signal: Signal): HandleResult[A] = signalHandler(signal)
		}
	}
	
	inline def superviseNest[A](behavior: Behavior[A]): SupervisedNest[A] = new SupervisedNest[A](behavior, defaultCatcher, defaultCatcher)

	inline def restartNest[A](initializer: () => Behavior[A])(cleaner: () => Unit): Behavior[A] = new RestartNest(initializer(), initializer, cleaner)

	inline def unionAndSignalBehaviorNest[A, B](mbA: MsgBehavior[A], mbB: MsgBehavior[B], sbAB: SignalBehavior[A | B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]): Behavior[A | B] =
		new UnionAndSignalBehaviorNest[A, B](mbA, mbB, sbAB)(using ttA, ttB)

	def unitedNest[A, B](bA: Behavior[A], bB: Behavior[B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]): Behavior[A | B] = {

		def combineSignals(hrA: HandleResult[A], hrB: HandleResult[B]): HandleResult[A | B] = {
			(hrA, hrB) match {
				case (Unhandled, Unhandled) => Unhandled
				case (Continue | Unhandled, Continue | Unhandled) => Continue
				case (Stop, _) | (_, Stop) => Stop
				case (Restart, _) | (_, Restart) => Restart
				case (RestartWith(nbA), hsrB) => hsrB match {
					case Continue | Unhandled => RestartWith(unitedNest(nbA, bB))
					case ContinueWith(nbB) => RestartWith(unitedNest(nbA, nbB))
					case RestartWith(nbB) => RestartWith(unitedNest(nbA, nbB))
					case _ => unreachable
				}
				case (hsrA, RestartWith(nbB)) => hsrA match {
					case Continue | Unhandled => RestartWith(unitedNest(bA, nbB))
					case ContinueWith(nba) => RestartWith(unitedNest(nba, nbB))
					case _ => unreachable
				}
				case (ContinueWith(nbA), hsrB) => hsrB match {
					case Continue | Unhandled => ContinueWith(unitedNest(nbA, bB))
					case ContinueWith(nbB) => ContinueWith(unitedNest(nbA, nbB))
					case _ => unreachable
				}
				case (hsrA, ContinueWith(nbB)) => hsrA match {
					case Continue | Unhandled => ContinueWith(unitedNest(bA, nbB))
					case _ => unreachable
				}
			}
		}

		val signalBehavior: SignalBehavior[A | B] = (signal: Signal) => {
			if signal.isInitialization then { // TODO analyze if the order should be the opposite.
				val hsrB = bB.handleSignal(signal)
				val hsrA = bA.handleSignal(signal)
				combineSignals(hsrA, hsrB)
			} else {
				val hsrA = bA.handleSignal(signal)
				val hsrB = bB.handleSignal(signal)
				combineSignals(hsrA, hsrB)
			}
		}

		unionAndSignalBehaviorNest(bA, bB, signalBehavior)(using ttA, ttB)
	}

	def defaultCatcher[A]: PartialFunction[Throwable, HandleResult[A]] = {
		case scala.util.control.NonFatal(e) => Stop
	}

	def unreachable: Nothing = throw new AssertionError("unreachable")
}

object IgnoreAllBehavior extends Behavior[Any] {
	override def handleMsg(message: Any): HandleResult[Any] = Continue

	override def handleSignal(signal: Signal): HandleResult[Any] = Continue
}


class Fusion[A](mb: MsgBehavior[A], sb: SignalBehavior[A]) extends Behavior[A] {
	override def handleMsg(message: A): HandleResult[A] = mb.handleMsg(message)

	override def handleSignal(signal: Signal): HandleResult[A] = sb.handleSignal(signal)
}

class SupervisedNest[A](
	var backingBehavior: Behavior[A],
	var baseMsgCatcher: PartialFunction[Throwable, HandleResult[A]],
	var baseSignalCatcher: PartialFunction[Throwable, HandleResult[A]]
) extends Behavior[A] {
	private def update(
		newBackingBehavior: Behavior[A],
		newBaseMsgCatcher: PartialFunction[Throwable, HandleResult[A]],
		newBaseSignalCatcher: PartialFunction[Throwable, HandleResult[A]]
	): this.type = {
		backingBehavior = newBackingBehavior
		baseMsgCatcher = newBaseMsgCatcher
		baseSignalCatcher = newBaseSignalCatcher
		this
	}

	override def handleMsg(message: A): HandleResult[A] = {
		try backingBehavior.handleMsg(message).mapBehavior(bA => update(bA, baseMsgCatcher, baseSignalCatcher))
		catch baseMsgCatcher
	}

	override def handleSignal(signal: Signal): HandleResult[A] = {
		try backingBehavior.handleSignal(signal).mapBehavior(bA => update(bA, baseMsgCatcher, baseSignalCatcher))
		catch baseSignalCatcher
	}

	def withCatcher(extendingCatcher: PartialFunction[Throwable, HandleResult[A]]): SupervisedNest[A] =
		update(backingBehavior, extendingCatcher.orElse(baseMsgCatcher), extendingCatcher.orElse(baseSignalCatcher))

	def withMsgCatcher(extendingMsgCatcher: PartialFunction[Throwable, HandleResult[A]]): SupervisedNest[A] =
		update(backingBehavior, extendingMsgCatcher.orElse(baseMsgCatcher), baseSignalCatcher)

	def withSignalCatcher(extendingSignalCatcher: PartialFunction[Throwable, HandleResult[A]]): SupervisedNest[A] =
		update(backingBehavior, baseMsgCatcher, extendingSignalCatcher.orElse(baseSignalCatcher))
}

class RestartNest[A](var nestedBehavior: Behavior[A], initializer: () => Behavior[A], cleaner: () => Unit) extends Behavior[A] {
	private def update(newBehavior: Behavior[A]): this.type = {
		this.nestedBehavior = newBehavior
		this
	}

	override def handleMsg(message: A): HandleResult[A] = {
		nestedBehavior.handleMsg(message) match {
			case Restart =>
				cleaner()
				ContinueWith(update(initializer()))
			case rw: RestartWith[A] => rw
			case hmrA => hmrA.mapBehavior(bA => update(bA))
		}
	}

	override def handleSignal(signal: Signal): HandleResult[A] = {
		nestedBehavior.handleSignal(signal) match {
			case Restart =>
				cleaner()
				ContinueWith(update(initializer()))
			case rw: RestartWith[A] => rw
			case hmrA => hmrA.mapBehavior(bA => update(bA))
		}
	}

	// TODO overload all the methods to avoid escaping from the nest.
}

/** Nest both, the union of the message behaviors and the signal behavior. */
open class UnionAndSignalBehaviorNest[A, B](
	var mbA: MsgBehavior[A],
	var mbB: MsgBehavior[B],
	val sbAB: SignalBehavior[A | B]
)(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]) extends Behavior[A | B] { thisUnionOf =>

	private def update(nmbA: MsgBehavior[A], nmbB: MsgBehavior[B]): this.type = {
		this.mbA = nmbA
		this.mbB = nmbB
		this
	}

	private def handleMessageB(b: B): HandleResult[A | B] =
		mbB.handleMsg(b).mapMsg { nmbB =>
			update(mbA, nmbB)
		}

	override def handleMsg(message: A | B): HandleResult[A | B] = {
		message match {
			case ttA(a) =>
				val hmrA = mbA.handleMsg(a)
				if hmrA eq Unhandled then ttB.unapply(message) match {
					case Some(b) => handleMessageB(b)
					case None => Unhandled
				} else {
					hmrA.mapMsg { nbA => update(nbA, mbB) }
				}

			case ttB(b) => handleMessageB(b)
		}
	}

	override def handleSignal(signal: Signal): HandleResult[A | B] = sbAB.handleSignal(signal)

	// TODO overload all the methods to avoid escaping from the nest.
}


open class UnionNest[A, B](bA: Behavior[A], bB: Behavior[B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]) extends Behavior[A | B] { thisUnionBehavior =>

	private def handleMessageB(b: B): HandleResult[A | B] =
		bB.handleMsg(b).mapMsg { nbB =>
			if nbB eq bB then thisUnionBehavior else UnionNest(bA, nbB)
		}

	override def handleMsg(message: A | B): HandleResult[A | B] = {
		message match {
			case ttA(a) =>
				val hmrA = bA.handleMsg(a)
				if hmrA eq Unhandled then ttB.unapply(message) match {
					case Some(b) => handleMessageB(b)
					case None => Unhandled
				} else {
					hmrA.mapMsg { nbA =>
						if nbA eq bA then thisUnionBehavior else UnionNest(nbA, bB)
					}
				}

			case ttB(b) => handleMessageB(b)
		}
	}

	override def handleSignal(signal: Signal): HandleResult[A | B] = {
		if signal.isInitialization then { // TODO analyze if the order should be the opposite.
			val hsrA = bA.handleSignal(signal)
			val hsrB = bB.handleSignal(signal)
			combineSignals(hsrA, hsrB)
		} else {
			val hsrB = bB.handleSignal(signal)
			val hsrA = bA.handleSignal(signal)
			combineSignals(hsrA, hsrB)
		}
	}

	def combineSignals(hrA: HandleResult[A], hrB: HandleResult[B]): HandleResult[A | B] = {
		(hrA, hrB) match {
			case (Unhandled, Unhandled) => Unhandled
			case (Continue | Unhandled, Continue | Unhandled) => Continue
			case (Stop, _) | (_, Stop) => Stop
			case (Restart, _) | (_, Restart) => Restart
			case (RestartWith(nbA), hsrB) => hsrB match {
				case Continue | Unhandled => RestartWith(UnionNest(nbA, bB))
				case ContinueWith(nbB) => RestartWith(UnionNest(nbA, nbB))
				case RestartWith(nbB) => RestartWith(UnionNest(nbA, nbB))
				case _ => Behavior.unreachable
			}
			case (hsrA, RestartWith(nbB)) => hsrA match {
				case Continue | Unhandled => RestartWith(UnionNest(bA, nbB))
				case ContinueWith(nba) => RestartWith(UnionNest(nba, nbB))
				case _ => Behavior.unreachable
			}
			case (ContinueWith(nbA), hsrB) => hsrB match {
				case Continue | Unhandled => ContinueWith(UnionNest(nbA, bB))
				case ContinueWith(nbB) => ContinueWith(UnionNest(nbA, nbB))
				case _ => Behavior.unreachable
			}
			case (hsrA, ContinueWith(nbB)) => hsrA match {
				case Continue | Unhandled => ContinueWith(UnionNest(bA, nbB))
				case _ => Behavior.unreachable
			}
		}
	}
}




