package readren.matrix

import Behavior.defaultCatcher

import scala.reflect.TypeTest

trait MsgBehavior[-A] { thisMsgBehavior =>
	def handleMsg(message: A): HandleResult[A]

	def withSignalBehavior[B <: A](signalBehavior: SignalBehavior[B]): Behavior[B] = new Behavior[B] {
		override def handleMsg(message: B): HandleResult[B] = thisMsgBehavior.handleMsg(message)

		override def handleSignal(signal: Signal): HandleResult[B] = signalBehavior.handleSignal(signal)
	}

	def withMsgRecover[S <: A](pf: PartialFunction[Throwable, HandleResult[S]]): MsgBehavior[S] = (message: S) => {
		try thisMsgBehavior.handleMsg(message)
		catch {
			case e: Throwable if pf.isDefinedAt(e) => pf.apply(e)
		}
	}
}

trait SignalBehavior[-A] { thisSignalBehavior =>
	def handleSignal(signal: Signal): HandleResult[A]

	def withMsgBehavior[B <: A](msgBehavior: MsgBehavior[B]): Behavior[B] = new Behavior[B] {
		override def handleMsg(message: B): HandleResult[B] = msgBehavior.handleMsg(message)

		override def handleSignal(signal: Signal): HandleResult[B] = thisSignalBehavior.handleSignal(signal)
	}

	def withSignalRecover[S <: A](pf: PartialFunction[Throwable, HandleResult[S]]): SignalBehavior[S] = (signal: Signal) => {
		try thisSignalBehavior.handleSignal(signal)
		catch {
			case e: Throwable if pf.isDefinedAt(e) => pf.apply(e)
		}
	}
}

trait Behavior[-A] extends MsgBehavior[A], SignalBehavior[A] { thisBehavior =>
	def withRecover[S <: A](pf: PartialFunction[Throwable, HandleResult[S]]): Behavior[S] =
		new FromMsgAndSignalBehaviors[S](
			thisBehavior.withMsgRecover(pf),
			thisBehavior.withSignalRecover(pf)
			)

	def supervised[S <: A]: SupervisedBehavior[S] = new SupervisedBehavior[S](thisBehavior, defaultCatcher, defaultCatcher)
}


object Behavior {
	inline def ignore: Behavior[Any] = IgnoreAllBehavior

	inline def handleMsg[A](handler: MsgBehavior[A]): MsgBehavior[A] = handler

	inline def handleSignal[A](handler: SignalBehavior[A]): SignalBehavior[A] = handler

	inline def handleMsgAndSignal[A](mb: MsgBehavior[A])(sb: SignalBehavior[A]): Behavior[A] = new FromMsgAndSignalBehaviors[A](mb, sb)

	inline def supervise[A](behavior: Behavior[A]): SupervisedBehavior[A] = new SupervisedBehavior[A](behavior, defaultCatcher, defaultCatcher)

	inline def restartNest[A](initializer: () => Behavior[A])(cleaner: () => Unit): Behavior[A] = new RestartNest(initializer(), initializer, cleaner)

	inline def unionOf[A, B](mbA: MsgBehavior[A], mbB: MsgBehavior[B], sbAB: SignalBehavior[A | B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]): Behavior[A | B] =
		new UnionOf[A, B](mbA, mbB, sbAB)(using ttA, ttB)

	def united[A, B](bA: Behavior[A], bB: Behavior[B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]): Behavior[A | B] = {

		def combineSignals(hrA: HandleResult[A], hrB: HandleResult[B]): HandleResult[A | B] = {
			(hrA, hrB) match {
				case (Unhandled, Unhandled) => Unhandled
				case (Continue | Unhandled, Continue | Unhandled) => Continue
				case (Stop, _) | (_, Stop) => Stop
				case (Restart, _) | (_, Restart) => Restart
				case (RestartWith(nbA), hsrB) => hsrB match {
					case Continue | Unhandled => RestartWith(united(nbA, bB))
					case ContinueWith(nbB) => RestartWith(united(nbA, nbB))
					case RestartWith(nbB) => RestartWith(united(nbA, nbB))
					case _ => unreachable
				}
				case (hsrA, RestartWith(nbB)) => hsrA match {
					case Continue | Unhandled => RestartWith(united(bA, nbB))
					case ContinueWith(nba) => RestartWith(united(nba, nbB))
					case _ => unreachable
				}
				case (ContinueWith(nbA), hsrB) => hsrB match {
					case Continue | Unhandled => ContinueWith(united(nbA, bB))
					case ContinueWith(nbB) => ContinueWith(united(nbA, nbB))
					case _ => unreachable
				}
				case (hsrA, ContinueWith(nbB)) => hsrA match {
					case Continue | Unhandled => ContinueWith(united(bA, nbB))
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

		unionOf(bA, bB, signalBehavior)(using ttA, ttB)
	}

	def defaultCatcher[A]: PartialFunction[Throwable, HandleResult[A]] = {
		case scala.util.control.NonFatal(e) => Stop
	}

	private def unreachable: Nothing = throw new AssertionError("unreachable")
}

object IgnoreAllBehavior extends Behavior[Any] {
	override def handleMsg(message: Any): HandleResult[Any] = Continue

	override def handleSignal(signal: Signal): HandleResult[Any] = Continue
}


class FromMsgAndSignalBehaviors[A](mb: MsgBehavior[A], sb: SignalBehavior[A]) extends Behavior[A] {
	override def handleMsg(message: A): HandleResult[A] = mb.handleMsg(message)

	override def handleSignal(signal: Signal): HandleResult[A] = sb.handleSignal(signal)
}


class SupervisedBehavior[A](
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
		try backingBehavior.handleMsg(message).map(bA => update(bA, baseMsgCatcher, baseSignalCatcher))
		catch {
			case e if baseMsgCatcher.isDefinedAt(e) => baseMsgCatcher.apply(e)
		}
	}

	override def handleSignal(signal: Signal): HandleResult[A] = {
		try backingBehavior.handleSignal(signal).map(bA => update(bA, baseMsgCatcher, baseSignalCatcher))
		catch {
			case e if baseMsgCatcher.isDefinedAt(e) => baseMsgCatcher.apply(e)
		}
	}

	def withCatcher(extendingCatcher: PartialFunction[Throwable, HandleResult[A]]): SupervisedBehavior[A] =
		update(backingBehavior, extendingCatcher.orElse(baseMsgCatcher), extendingCatcher.orElse(baseSignalCatcher))

	def withMsgCatcher(extendingMsgCatcher: PartialFunction[Throwable, HandleResult[A]]): SupervisedBehavior[A] =
		update(backingBehavior, extendingMsgCatcher.orElse(baseMsgCatcher), baseSignalCatcher)

	def withSignalCatcher(extendingSignalCatcher: PartialFunction[Throwable, HandleResult[A]]): SupervisedBehavior[A] =
		update(backingBehavior, baseMsgCatcher, extendingSignalCatcher.orElse(baseSignalCatcher))
}

class RestartNest[A](var nestedBehavior: Behavior[A], initializer: () => Behavior[A], cleaner: () => Unit) extends Behavior[A]:
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
			case hmrA => hmrA.map(bA => update(bA))
		}
	}

	override def handleSignal(signal: Signal): HandleResult[A] = {
		nestedBehavior.handleSignal(signal) match {
			case Restart =>
				cleaner()
				ContinueWith(update(initializer()))
			case rw: RestartWith[A] => rw
			case hmrA => hmrA.map(bA => update(bA))
		}
	}


open class UnionOf[A, B](var mbA: MsgBehavior[A], var mbB: MsgBehavior[B], sbAB: SignalBehavior[A | B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]) extends Behavior[A | B] { thisUnionOf =>

	private def update(nmbA: MsgBehavior[A], nmbB: MsgBehavior[B]): this.type = {
		this.mbA = nmbA
		this.mbB = nmbB
		this
	}

	private def handleMessageB(b: B): HandleResult[A | B] =
		mbB.handleMsg(b).map { nmbB =>
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
					hmrA.map { nbA => update(nbA, mbB) }
				}

			case ttB(b) => handleMessageB(b)
		}
	}

	override def handleSignal(signal: Signal): HandleResult[A | B] = sbAB.handleSignal(signal)
}



//	open class Union[A, B](bA: Behavior[A], bB: Behavior[B])(using ttA: TypeTest[A | B, A], ttB: TypeTest[A | B, B]) extends Behavior[A | B] { thisUnionBehavior =>
//
//		private def handleMessageB(b: B): HandleResult[A | B] = bB.handleMessage(b).map { nbB =>
//			if nbB eq bB then thisUnionBehavior else union(bA, nbB)
//		}
//
//		override def handleMessage(message: A | B): HandleResult[A | B] = {
//			message match {
//				case ttA(a) =>
//					val hmrA = bA.handleMessage(a)
//					if hmrA eq Unhandled then ttB.unapply(message) match {
//						case Some(b) => handleMessageB(b)
//						case None => Unhandled
//					} else {
//						hmrA.map { nbA =>
//							if nbA eq bA then thisUnionBehavior else union(nbA, bB)
//						}
//					}
//
//				case ttB(b) => handleMessageB(b)
//			}
//		}
//
//		override def handleSignal(signal: Signal): HandleResult[A | B] = {
//			if signal.isInitialization then { // TODO analyze if the order should be the opposite.
//				val hsrB = bB.handleSignal(signal)
//				val hsrA = bA.handleSignal(signal)
//				combineSignals(hsrA, hsrB)
//			} else {
//				val hsrA = bA.handleSignal(signal)
//				val hsrB = bB.handleSignal(signal)
//				combineSignals(hsrA, hsrB)
//			}
//		}
//
//		def combineSignals(hrA: HandleResult[A], hrB: HandleResult[B]): HandleResult[A | B] = {
//			(hrA, hrB) match {
//				case (Unhandled, Unhandled) => Unhandled
//				case (Continue | Unhandled, Continue | Unhandled) => Continue
//				case (Stop, _) | (_, Stop) => Stop
//				case (eA: Error, _) => eA
//				case (_, eB: Error) => eB
//				case (Restart, _) | (_, Restart) => Restart
//				case (RestartWith(nbA), hsrB) => hsrB match {
//					case Continue | Unhandled => RestartWith(Union(nbA, bB))
//					case ContinueWith(nbB) => RestartWith(Union(nbA, nbB))
//					case RestartWith(nbB) => RestartWith(Union(nbA, nbB))
//					case _ => unreachable
//				}
//				case (hsrA, RestartWith(nbB)) => hsrA match {
//					case Continue | Unhandled => RestartWith(Union(bA, nbB))
//					case ContinueWith(nba) => RestartWith(Union(nba, nbB))
//					case _ => unreachable
//				}
//				case (ContinueWith(nbA), hsrB) => hsrB match {
//					case Continue | Unhandled => ContinueWith(Union(nbA, bB))
//					case ContinueWith(nbB) => ContinueWith(Union(nbA, nbB))
//					case _ => unreachable
//				}
//				case (hsrA, ContinueWith(nbB)) => hsrA match {
//					case Continue | Unhandled => ContinueWith(Union(bA, nbB))
//					case _ => unreachable
//				}
//			}
//		}
//
//		def unreachable: Nothing = throw new AssertionError("unreachable")
//
//		override def toString: String = deriveToString[Union[A, B]](this)
//	}




