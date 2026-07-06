package readren.sequencer

type OriginId = Int

/** TODO Make [[Doer.MonoOpbserver]] have the originId parameter and remove this trait. */
trait CompletionObserver[-A] {
	def onSuccess(a: A, originId: OriginId): Unit

	def onError(e: Throwable, originId: OriginId): Unit
}

object CompletionIgnorer extends CompletionObserver[Any] {
	override def onSuccess(a: Any, originId: OriginId): Unit = ()

	override def onError(e: Throwable, originId: OriginId): Unit = ()
}
